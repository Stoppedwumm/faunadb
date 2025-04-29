package fauna.tools

import fauna.atoms._
import fauna.lang.{ TimeBound, Timestamp }
import fauna.repo.doc.Version
import fauna.storage._
import fauna.storage.cassandra.{ CassandraKeyLocator, Hash, HashFilter }
import io.netty.buffer.ByteBuf
import java.lang.{ Long => JLong }
import java.time.format.DateTimeParseException
import java.util.concurrent.ConcurrentLinkedQueue
import org.apache.cassandra.db.{ Cell => CCell }
import org.apache.cassandra.io.sstable._
import org.apache.commons.cli.Options
import scala.jdk.CollectionConverters._

/** This tool processes a snapshot of the Versions column family,
  * printing out all versions for a ScopeID, (ScopeID, CollectionID)
  * pair, or (ScopeID, CollectionID, SubID) triple.
  *
  * If --snapshot is not provided, the snapshot time is Timestamp.MaxMicros,
  *   i.e. all cells are live.
  */
object VersionExporter extends SSTableApp("VersionExporter") {
  override def setPerAppCLIOptions(o: Options) = {
    o.addOption("s", "snapshot", true, "Snapshot time of live data.")
    o.addOption(null, "scope", true, "ScopeID for output.")
    o.addOption(null, "collection", true, "CollectionID for output. Optional.")
    o.addOption(null, "sub", true, "SubID for output. Optional. Requires --collection.")
  }

  start {
    val snapshotTS = Option(cli.getOptionValue("s")) flatMap { str =>
      try {
        Some(Timestamp.parse(str))
      } catch {
        case _: DateTimeParseException =>
          handler.warn(s"Bad snapshot time: $str")
          sys.exit(1)
      }
    } getOrElse Timestamp.MaxMicros

    val scope = Option(cli.getOptionValue("scope")) match {
      case Some(s) => longArg("--scope", s)
      case None    =>
        handler.warn("--scope is required.")
        sys.exit(1)
    }

    val collection = Option(cli.getOptionValue("collection")) map { str =>
      CollectionID(longArg("--collection", str))
    }

    val sub = Option(cli.getOptionValue("sub")) map { str =>
      SubID(longArg("--sub", str))
    }

    val docs = (collection, sub) match {
      case (None, None) => None
      case (Some(coll), None) => Some(Left(coll))
      case (Some(coll), Some(sub)) => Some(Right(DocID(sub, coll)))
      case (None, Some(_)) =>
        handler.warn("--sub requires --collection.")
        sys.exit(1)
    }

    run(ScopeID(scope), docs, snapshotTS)

    sys.exit(0)
  }

  private def longArg(arg: String, str: String): JLong =
    try {
      JLong.valueOf(str)
    } catch {
      case _: NumberFormatException =>
        handler.warn(s"$arg $str is not a valid Long.")
        sys.exit(1)
    }

  private def run(
    scope: ScopeID,
    docs: Option[Either[CollectionID, DocID]],
    snapshotTS: Timestamp) = {

    val cfs = openColumnFamily(Tables.Versions.CFName)

    val sstables = new ConcurrentLinkedQueue[SSTableReader]()

    forEachSSTable("VersionExporter", cfs) {
      sstables.add(_)
    }

    handler.output(
      s"Found ${sstables.size} sstables for ${Cassandra.KeyspaceName}.${Tables.Versions.CFName}")

    // Select only those rows of interest.
    val filterKey = { (buf: ByteBuf) =>
      val (scopeID, docID) = Tables.Versions.decodeRowKey(buf)

      if (scopeID == scope) {
        docs match {
          case None => true
          case Some(Left(coll)) => docID.collID == coll
          case Some(Right(doc)) => docID == doc
        }
      } else {
        false
      }
    }

    val filterCell = { (buf: ByteBuf, ccell: CCell) =>
      val version = Version.decodeCell(buf, Cell(ccell))
      handler.output(s"Version: $version")
      false // Don't compute any checksum.
    }

    val segment = docs match {
      case Some(Right(id)) =>
        // There is enough info. to constrain to a single token. Find
        // it.
        val bytes = Tables.Versions.rowKeyByteBuf(scope, id)
        val loc = CassandraKeyLocator.locate(bytes)
        Segment(loc, loc.next)

      case _ =>
        // Full scan. :'(
        Segment.All
    }

    // There are several ways to perform a table scan over a set of
    // SSTables without starting the storage engine. This is one. See
    // CassandraIterator and SSTableIterator for two others.
    val hash = new Hash(
      cfs,
      sstables.asScala,
      segment,
      snapshotTS,
      config.repair_max_hash_depth,
      TimeBound.Max,
      stats,
      HashFilter(filterKey, filterCell))

    hash.get() // Compute and discard the result.
  }
}
