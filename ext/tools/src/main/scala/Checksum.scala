package fauna.tools

import fauna.atoms._
import fauna.lang.{ TimeBound, Timestamp }
import fauna.lang.syntax._
import fauna.model.Database
import fauna.repo.store._
import fauna.stats._
import fauna.storage.{ Cassandra, Cell, Tables }
import fauna.storage.cassandra.{ Hash, HashFilter }
import io.netty.buffer.{ ByteBuf, Unpooled }
import java.io.{ File, FileInputStream }
import java.time.format.DateTimeParseException
import java.util.concurrent.ConcurrentLinkedQueue
import org.apache.cassandra.db.{ Cell => CCell }
import org.apache.cassandra.io.sstable._
import org.apache.cassandra.utils.OutputHandler
import org.apache.commons.cli.Options
import scala.jdk.CollectionConverters._
import scala.util.{ Failure, Success }

/** The Checksum tool hashes data within a set of SSTables, producing
  * a checksum value which may be used to assert consistency between a
  * pair of snapshots.
  *
  * If --name is not provided, the Versions column family name is used.
  *
  * If --snapshot is not provided, the snapshot time is Timestamp.MaxMicros,
  *   i.e. all cells are live.
  *
  * The data within the snapshot may be filtered to a sub-tree
  * (inclusive) of the database hierarchy by providing the hierarchy
  * in "dump-tree" format, and the global_id of the sub-tree's root.
  */
object Checksum extends SSTableApp("Snapshot Checksum") {

  override def setPerAppCLIOptions(o: Options) = {
    o.addOption("n", "name", true, "Column family name.")
    o.addOption("s", "snapshot", true, "Snapshot time of live data.")
    o.addOption("d", "dump-tree", true, "Dump tree file.")
    o.addOption(
      null,
      "global-id",
      true,
      "Global ID of the root of database hierarchy.")

    o.addOption(null, "key-metadata", true, "Keys metadata file")
    o.addOption(null, "metadata", true, "Metadata file")
  }

  start {
    val cf = cli.getOptionValue("n", Tables.Versions.CFName)
    val snapshotTS = Option(cli.getOptionValue("s")) flatMap { str =>
      try {
        Some(Timestamp.parse(str))
      } catch {
        case _: DateTimeParseException =>
          System.err.println(s"Bad snapshot time: $str")
          sys.exit(1)
      }
    } getOrElse Timestamp.MaxMicros

    val dumpTree = Option(cli.getOptionValue("d")) map { path =>
      val fs = new FileInputStream(path)
      DumpEntry.fromStream(fs)
    }

    val globalID = Option(cli.getOptionValue("global-id")) map { str =>
      Database.decodeGlobalID(str) match {
        case Some(id) => id
        case None =>
          handler.warn(s"Invalid global ID: $str")
          sys.exit(1)
      }
    }

    if (dumpTree.nonEmpty ^ globalID.nonEmpty) {
      handler.warn(s"Both dump-tree and global-id required when one is provided.")
      sys.exit(1)
    }

    val schemaMetadata = Option(cli.getOptionValue("metadata")) orElse {
      Option(cli.getOptionValue("key-metadata"))
    } map { file =>
      SchemaMetadata.readFile(new File(file)) match {
        case Success(value) => value
        case Failure(exception) =>
          handler.warn("Error reading metadata file", exception)
          sys.exit(1)
      }
    }

    val treeAndID = dumpTree flatMap { tree =>
      globalID map { id => (tree, id) }
    }

    cf.split(',') foreach { cf =>
      val hash = run(cf, treeAndID, schemaMetadata, snapshotTS)

      System.out.println(s"$cf Checksum: ${hash.toHexString}")
      System.out.flush()
    }

    sys.exit(0)
  }

  def run(
    cf: String,
    treeAndID: Option[(Seq[DumpEntry], GlobalDatabaseID)],
    schemaMetadata: Option[SchemaMetadata],
    snapshotTS: Timestamp = Timestamp.MaxMicros): Array[Byte] = {

    val cfs = openColumnFamily(cf)

    stats.event(
      StatLevel.Info,
      "Checksum",
      s"Build ${Build.identifier}\n" +
        s"Column Family: $cf\n" +
        s"Snapshot Time: $snapshotTS",
      StatTags.Empty)

    val sstables = new ConcurrentLinkedQueue[SSTableReader]()

    forEachSSTable("Checksum", cfs) { sstable =>
      sstables.add(sstable)
    }

    handler.output(
      s"Found ${sstables.size} sstables for ${Cassandra.KeyspaceName}.$cf")

    val filter = (treeAndID, schemaMetadata) match {
      case (Some((tree, id)), Some(meta)) =>
        mkFilter(cf, tree, id, handler, meta)
      case (_, None) => HashFilter.All
      case _ => throw new IllegalStateException("dumpTree and globalID both empty.")
    }

    val hash = new Hash(
      cfs,
      sstables.asScala,
      Segment.All,
      snapshotTS,
      config.repair_max_hash_depth,
      TimeBound.Max,
      stats,
      filter)

    val tree = hash.get()

    tree.hash
  }

  private def mkFilter(
    cf: String,
    entries: Seq[DumpEntry],
    globalID: GlobalDatabaseID,
    handler: OutputHandler,
    schemaMetadata: SchemaMetadata): HashFilter = {
    val tree = DatabaseTree.build(entries)

    tree.forGlobalID(globalID) match {
      case None =>
        handler.warn(s"No dump-tree entry found for $globalID.")
        sys.exit(1)
      case Some(root) =>
        val filter = DatabaseFilter(root)

        cf match {
          case Tables.Versions.CFName =>
            val filterKey = { (buf: ByteBuf) =>
              val (scopeID, docID) = Tables.Versions.decodeRowKey(buf)

              filter.filterVersions(scopeID, docID)
            }

            HashFilter(filterKey)

          case Tables.HistoricalIndex.CFName | Tables.SortedIndex.CFName =>
            val filterKey = { (buf: ByteBuf) =>
              val (scopeID, indexID, _) = Tables.Indexes.decode(buf)

              filter.filterIndexes(scopeID, indexID)
            }

            HashFilter(filterKey)

          case Tables.Lookups.CFName =>
            val filterKey = { (buf: ByteBuf) =>
              // FIXME: need to find a way to avoid this double globalID decoding
              val globalID = Tables.Lookups.decode(buf)

              filter.filterLookupRow(globalID, schemaMetadata)
            }

            val filterCell = { (buf: ByteBuf, ccell: CCell) =>
              val cell = Cell(
                Unpooled.wrappedBuffer(ccell.name.toByteBuffer),
                Unpooled.wrappedBuffer(ccell.value),
                Timestamp.ofMicros(ccell.timestamp())
              )

              val globalID = Tables.Lookups.decode(buf)
              val (scope, id) = Tables.Lookups.decodeDatabase(buf, cell)

              filter.filterLookupCell(globalID, scope, id)
            }

            HashFilter(filterKey, filterCell)

          case _ =>
            handler.warn(s"Unsupported column family: $cf")
            sys.exit(1)
        }
    }
  }

}
