package fauna.tools

import fauna.atoms._
import fauna.auth.JWTToken
import fauna.codex.cbor._
import fauna.codex.json2.{ JSON, JSONWriter }
import fauna.lang.{ Timestamp, Timing }
import fauna.model.{ AccessProvider, Database }
import fauna.repo.doc.Version
import fauna.repo.store._
import fauna.stats._
import fauna.storage.{ Cassandra, Cell => FCell, Tables, Value }
import fauna.storage.cassandra.{
  NeverPurgeController,
  RowBuffer,
  SSTableIterator,
  Transformer
}
import fauna.storage.index.IndexValue
import fauna.storage.ir.{ DocIDV, IRValue, LongV, MapV, NullV }
import fauna.storage.ops.VersionAdd
import io.netty.buffer._
import java.io.{ BufferedWriter, File, FileWriter, IOException }
import java.lang.{ Long => JLong }
import java.nio.channels.FileChannel
import java.nio.file.{ Files, Path, StandardOpenOption }
import java.nio.ByteBuffer
import java.time.format.DateTimeParseException
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.Collections
import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.db.{
  ArrayBackedSortedColumns,
  BufferCell,
  BufferDeletedCell,
  Cell,
  ColumnFamilyStore,
  DecoratedKey,
  Row
}
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator
import org.apache.cassandra.db.compaction.{ CompactionManager, LazilyCompactedRow }
import org.apache.cassandra.io.sstable._
import org.apache.cassandra.io.util.RandomAccessReader
import org.apache.cassandra.utils.OutputHandler
import org.apache.commons.cli.Options
import scala.collection.immutable.Queue
import scala.io.Source
import scala.util.control.NonFatal

/** The Rewriter tool processes SSTables in a snapshot, transforming
  * the data based on a file mapping ScopeIDs to a set of new
  * ScopeID/GlobalDatabaseID pairs.
  *
  * After this process completes successfully, the snapshot will no
  * longer contain any data associated with the input ScopeIDs. A new
  * set of SSTables will be left in the snapshot containing the
  * rewritten data and any data which did not require rewriting
  * (i.e. no mapping was present in the file).
  *
  * See MappingFile for the expected format of the file.
  *
  * Typical usage is like so:
  *   export FAUNADB_CONFIG=path/to/faunadb.yml
  *   java -cp path/to/faunadb.jar fauna.tools.Rewriter path/to/mapping.txt
  *        --snapshot 1970-01-01T00:00:00Z
  *        --name Versions
  *
  * If --name is not provided, the Versions column family name is used.
  *
  * If --snapshot is not provided, the snapshot time is Timestamp.MaxMicros,
  *   i.e. all cells are live.
  */
object Rewriter extends SSTableApp("SSTable Rewriter") {

  // This wart lets us skip rewriting a database with no mapping,
  // without needing to do a larger restructure for this weird
  // case that shouldn't happen anymore.
  private[tools] class IgnoreDB extends Throwable

  override def setPerAppCLIOptions(o: Options) = {
    o.addOption("n", "name", true, "Column family name")
    o.addOption("t", "threads", true, "Number of threads")
    o.addOption("s", "snapshot", true, "Snapshot time of live data.")
    o.addOption("k", "keys", false, "Include keys/tokens in output.")
  }

  start {
    val cf = cli.getOptionValue("n", Tables.Versions.CFName)
    val threads = Option(cli.getOptionValue("t")) flatMap {
      _.toIntOption
    } getOrElse 1
    val snapshotTS = Option(cli.getOptionValue("s")) flatMap { str =>
      try {
        Some(Timestamp.parse(str))
      } catch {
        case _: DateTimeParseException =>
          System.err.println(s"Bad snapshot time: $str")
          sys.exit(1)
      }
    } getOrElse Timestamp.MaxMicros

    val includeKeys = cli.hasOption("k")

    val args = cli.getArgs()
    require(args.lengthIs == 1, "A mapping file is required.")

    System.setProperty("cassandra.absc.parallel-sort", "true")

    val cfs = openColumnFamily(cf)

    stats.event(
      StatLevel.Info,
      "Rewriter",
      s"Build ${Build.identifier}\n" +
        s"Column Family: $cf\n" +
        s"Threads: $threads",
      StatTags.Empty)

    val sstables = new ConcurrentLinkedQueue[SSTableReader]()

    forEachSSTable("Rewriter", cfs) { sstable =>
      // Reset to L0 to protect against level inversion between
      // tombstones and covered cells.
      sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 0)
      sstable.reloadSSTableMetadata()

      sstables.add(sstable)
    }

    handler.output(
      s"Found ${sstables.size} sstables for ${Cassandra.KeyspaceName}.$cf")

    val mapping = MappingFile.read(args(0))

    var success = true

    val executor = new Executor("Rewriter", threads)
    val rewriterStats = new RewriterStats("Rewriter", stats)

    while (!sstables.isEmpty) {
      val sstable = sstables.poll()

      executor addWorker { () =>
        try {
          stats.incr("Rewriter.SSTables")
          val rewriter = new Rewriter(
            cfs,
            sstable,
            handler,
            mapping,
            snapshotTS,
            config.storage_memtable_size_mb,
            includeKeys,
            rewriterStats)

          try {
            rewriter.run()
          } finally {
            rewriter.close()
          }

          // NOTE: This will remove the input sstable from the
          // filesystem - there are no other live references to this
          // SSTableReader (hence, the DataTracker is null).
          require(sstable.markObsolete(null), s"$sstable already marked obsolete?")
        } catch {
          case NonFatal(ex) =>
            stats.incr("Rewriter.Error")
            success = false
            handler.warn(s"Error rewriting $sstable", ex)
        } finally {
          // Don't rely on the reference queue to do this; get rid of the
          // data. Now.
          val comps = SSTable.componentsFor(sstable.descriptor)
          SSTable.delete(sstable.descriptor, comps)
        }
      }
    }

    executor.waitWorkers()

    // Wait for deletion tasks to complete.
    SSTableDeletingTask.waitForDeletions()

    Files.createDirectories(Path.of(config.tempPath))
    val file = new File(config.tempPath, s"rewrite-$cf.json")
    val buf = ByteBufAllocator.DEFAULT.ioBuffer
    try {
      val out = JSONWriter(buf)

      out.writeObjectStart()
      out.writeObjectField(
        JSON.Escaped("column_family"), {
          out.writeString(cf)
        })

      out.writeObjectField(
        JSON.Escaped("stats"), {
          out.writeDelimiter()
          rewriterStats.toJson(out)
        })
      out.writeObjectEnd()

      val chan = FileChannel.open(
        file.toPath,
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE)

      try {
        buf.readBytes(chan, 0, buf.readableBytes)
      } finally {
        chan.close()
      }
    } finally {
      buf.release()
    }

    val rewritten = rewriterStats.rewrittenRows
    if (rewritten <= 0) {
      success = false
      handler.warn(s"Number of rows rewritten should be > 0, but found $rewritten.")
    }

    if (success) {
      handler.output("Rewrite completed successfully.")
      stats.incr("Rewriter.Success")
      sys.exit(0)
    } else {
      stats.incr("Rewriter.Failure")
      sys.exit(1)
    }
  }
}

/** A rewriter iterates over an SSTable, transforming Row and Cell
  * data based on a MappingFile, resulting in a new SSTable with the
  * transformed data.
  *
  * The caller is responsible for both the input and output SSTables
  * after `run()` returns.
  *
  * The input SSTable is assumed to be valid and consistent with its
  * primary index. If the data or the index component are corrupt,
  * this class will abort. See Scrubber for repairing corrupt
  * SSTables.
  */
final class Rewriter(
  cfs: ColumnFamilyStore,
  sstable: SSTableReader,
  output: OutputHandler,
  mapping: MappingFile,
  snapshotTS: Timestamp,
  maxSizeMB: Int,
  includeKeys: Boolean,
  stats: RewriterStats)
    extends SSTableIterator(sstable, output, stats) {

  private[this] val controller = new NeverPurgeController(cfs)

  private[this] val builder =
    RowTransformer.Builder(
      cfs,
      sstable,
      dataFile,
      mapping,
      snapshotTS,
      includeKeys,
      output,
      stats)

  private[this] val buffer = new RowBuffer(
    cfs,
    sstable.getSSTableLevel(),
    maxSizeMB,
    sstable.maxDataAge,
    sstable.getSSTableMetadata.repairedAt)

  override def run(): Unit = {
    output.output(s"Rewriting $sstable (${dataFile.length()} bytes)")
    stats.recordSSTableBytes(dataFile.length())

    // Disable bloom filters.
    cfs.metadata.bloomFilterFpChance(1.0)

    val start = Timing.start
    try {
      super.run()
    } finally {
      buffer.close()
      controller.close()
      stats.recordRuntime(start.elapsedMillis)
    }
  }

  def apply(oldKey: DecoratedKey, dataBytes: Long): Unit = try {
    val transform = builder.build(oldKey, dataBytes)
    val iter = transform.getRow()

    // Don't bother with empty rows. They can happen due to the
    // snapshot time filter.
    if (iter.hasNext()) {

      // Rows larger than the buffer size can be flushed directly to a
      // new SSTable, skipping the ABSC entirely. Because there is
      // only one row in this SSTable, there is no need for
      // buffering/sorting.
      if ((dataBytes / 1024 / 1024) > maxSizeMB) {
        writeRow(iter, dataBytes)

      } else {

        val factory = ArrayBackedSortedColumns.factory
        val cf = iter.getColumnFamily.cloneMeShallow(factory, false /* reversed */ )

        while (iter.hasNext()) {
          val atom = iter.next
          cf.addAtom(atom)
        }

        val row = new Row(iter.getKey, cf)

        buffer.add(row, dataBytes)
      }
    }
  } catch {
    case _: Rewriter.IgnoreDB =>
      // Don't rewrite this database. It's missing a mapping.
  }

  private def writeRow(iter: OnDiskAtomIterator, dataBytes: Long): Unit = {
    val repairedAt = sstable.getSSTableMetadata.repairedAt
    val dest = cfs.directories.getWriteableLocationAsFile(dataBytes)

    if (dest eq null) {
      throw new IOException("Disk full.")
    }

    val writer = CompactionManager.createWriter(
      cfs,
      dest,
      cfs.metadata.getMinIndexInterval(),
      repairedAt,
      sstable)

    try {
      writer.append(
        new LazilyCompactedRow(controller, Collections.singletonList(iter)))

      val reader =
        writer.finish(
          SSTableWriter.FinishType.NORMAL,
          sstable.maxDataAge,
          repairedAt)
      reader.selfRef.release()
    } catch {
      case NonFatal(ex) =>
        writer.abort(ex.getMessage)
        throw ex
    }
  }
}

/** Each line in the mapping file has the following format:
  *
  * oldScopeID newScopeID oldGlobalID newGlobalID\n
  *
  * On each line, the first column denotes the input ScopeID, and the
  * second column is the ScopeID which all versions of the input
  * ScopeID will be rewritten to. Likewise for the GlobalDatabaseIDs
  * in the third and fourth column.
  *
  * ScopeID column values are numbers in the range of an unsigned Java
  * long (63b).
  *
  * GlobalIDs are strings encoded as per Database.encodeGlobalID.
  *
  * Lines are seperated by newline characters.
  */
object MappingFile {

  val RootMapping = Mapping(
    ScopeID.RootID,
    ScopeID.RootID,
    GlobalDatabaseID.MinValue,
    GlobalDatabaseID.MinValue)

  /** Reads a mapping file into an in-memory structure for rewriting.
    *
    * If strict is true, attempts to read-and-rewrite ids not defined
    * in the file will fail.
    */
  def read(filename: String): MappingFile = {
    val file = new File(filename)

    require(file.exists(), s"Mapping file does not exist at $filename.")
    require(file.canRead(), s"Mapping file cannot be read at $filename.")

    val src = Source.fromFile(file)
    try {
      val mapping = src
        .getLines()
        .map { line =>
          val fields = line.split(' ')
          require(
            fields.lengthIs == 4,
            s"Incorrect number of fields. Expected 4, got ${fields.length}: $line")

          val Array(oldScope, newScope) = fields.take(2) map { JLong.valueOf(_) }
          val Array(oldGlobal, newGlobal) =
            fields.takeRight(2) flatMap { Database.decodeGlobalID(_) }

          val mapping =
            Mapping(ScopeID(oldScope), ScopeID(newScope), oldGlobal, newGlobal)

          mapping.oldScope -> mapping
        }
        .toMap

      new MappingFile(mapping)
    } finally {
      src.close()
    }
  }
}

case class Mapping(
  oldScope: ScopeID,
  newScope: ScopeID,
  oldGlobal: GlobalDatabaseID,
  newGlobal: GlobalDatabaseID)

final class MappingFile private (mapping: Map[ScopeID, Mapping]) {

  private[this] val globals = mapping map { case (_, mapping) =>
    mapping.oldGlobal -> mapping
  }

  def containsScope(src: ScopeID): Boolean =
    mapping.contains(src)

  def containsGlobalID(src: GlobalDatabaseID): Boolean =
    globals.contains(src)

  def scopeForScope(src: ScopeID): ScopeID =
    forScope(src) map { _.newScope } match {
      case None =>
        throw new IllegalArgumentException(s"No mapping found for $src.")
      case Some(id) => id
    }

  def globalIDForScope(src: ScopeID): Option[GlobalDatabaseID] =
    mapping.get(src) map { _.newGlobal }

  def globalIDForGlobalID(globalID: GlobalDatabaseID): GlobalDatabaseID =
    forGlobalID(globalID) map { _.newGlobal } match {
      case None =>
        throw new IllegalArgumentException(s"No mapping found for $globalID.")
      case Some(id) => id
    }

  private def forScope(src: ScopeID): Option[Mapping] =
    src match {
      case ScopeID.RootID => Some(MappingFile.RootMapping)
      case src            => mapping.get(src)
    }

  // FIXME: public for testing.
  def forGlobalID(globalID: GlobalDatabaseID): Option[Mapping] =
    globalID match {
      case GlobalDatabaseID.MinValue => Some(MappingFile.RootMapping)
      case id                        => globals.get(id)
    }

  // public for testing.
  def map(fn: Mapping => Mapping): MappingFile = {
    val mapped = mapping map { case (_, map) =>
      val m = fn(map)
      m.oldScope -> m
    }

    new MappingFile(mapped)
  }

  // public for testing.
  def save(file: String): Unit = {
    val writer = new BufferedWriter(new FileWriter(file))
    try {
      mapping foreach { case (_, map) =>
        val oldGlobal = Database.encodeGlobalID(map.oldGlobal)
        val newGlobal = Database.encodeGlobalID(map.newGlobal)
        writer.write(
          s"${map.oldScope.toLong} ${map.newScope.toLong} $oldGlobal $newGlobal")
        writer.newLine()
      }
    } finally {
      writer.close()
    }
  }
}

object RowTransformer {

  final case class Builder(
    cfs: ColumnFamilyStore,
    sstable: SSTableReader,
    dataFile: RandomAccessReader,
    mapping: MappingFile,
    snapshotTS: Timestamp,
    includeKeys: Boolean,
    output: OutputHandler,
    stats: RewriterStats) {

    def build(rowKey: DecoratedKey, dataSize: Long): Transformer[_] =
      cfs.name match {
        case Tables.Versions.CFName =>
          new VersionTransformer(
            rowKey,
            dataSize,
            sstable,
            dataFile,
            mapping,
            snapshotTS,
            includeKeys,
            output,
            stats)

        case Tables.Lookups.CFName =>
          new LookupsTransformer(
            rowKey,
            dataSize,
            sstable,
            dataFile,
            mapping,
            snapshotTS,
            includeKeys,
            cfs.metadata,
            output,
            stats)

        // Support for indexes is speculative: the characteristics of
        // rewriting index rows vs rebuilding them from Versions need to be
        // determined.
        case Tables.SortedIndex.CFName =>
          new IndexTransformer[Tables.SortedIndex.Key](
            SortedIndex,
            rowKey,
            dataSize,
            sstable,
            dataFile,
            mapping,
            snapshotTS,
            includeKeys,
            IndexValue.fromSortedValue,
            output,
            stats)

        case Tables.HistoricalIndex.CFName =>
          new IndexTransformer[Tables.HistoricalIndex.Key](
            HistoricalIndex,
            rowKey,
            dataSize,
            sstable,
            dataFile,
            mapping,
            snapshotTS,
            includeKeys,
            IndexValue.fromHistoricalValue,
            output,
            stats)

        case other =>
          throw new IllegalArgumentException(s"Unknown column family name $other.")
      }
  }

  /** Version row keys all contain a ScopeID, and need to be transformed.
    *
    * Version cell names _and values_ need to be transformed if
    * they are one of two collections:
    *   - Database
    *   - AccessProvider
    *
    * Each of these collections refers to a GlobalID somewhere in the
    * Version data. Database additionally refers to the ScopeID which
    * it defines.
    *
    * All other cell data remains unaltered.
    */
  final class VersionTransformer(
    rowKey: DecoratedKey,
    dataSize: Long,
    sstable: SSTableReader,
    dataFile: RandomAccessReader,
    mapping: MappingFile,
    snapshotTS: Timestamp,
    includeKeys: Boolean,
    output: OutputHandler,
    stats: RewriterStats)
      extends Transformer[(ScopeID, DocID)](
        rowKey,
        dataSize,
        sstable,
        dataFile,
        Transformer.VersionCodec,
        snapshotTS,
        output,
        stats) {

    override def includeRow(key: (ScopeID, DocID)) = key match {
      case (_, DocID(_, TokenID.collID)) | (_, DocID(_, KeyID.collID)) =>
        if (includeKeys) {
          stats.incrOutputRows()
        }

        includeKeys

      case _ =>
        stats.incrOutputRows()
        true
    }

    def transformKey(key: (ScopeID, DocID)) = {
      val (scope, id) = key
      (mapping.scopeForScope(scope), id)
    }

    override def transformCell(key: (ScopeID, DocID), cell: Cell): Cell = {
      val data = key match {
        case (_, DocID(_, DatabaseID.collID)) if cell.isLive =>
          transformVersion(cell, transformDatabase(key, _))

        case (scope, DocID(_, AccessProviderID.collID)) if cell.isLive =>
          transformVersion(cell, transformAccessProvider(scope, _))

        case _ =>
          // Leave any unknown cell types alone.
          return cell
      }

      new BufferCell(cell.name, data, cell.timestamp)
    }

    // This is called up to 2 times per version: Once for its data, and once for its
    // diff.
    private def transformDatabase(key: (ScopeID, DocID), data: MapV): IRValue = {
      val scope = data.get(Database.ScopeField.path) match {
        case Some(LongV(-1)) =>
          output.output(s"Null scope found in database $key.")
          return data
        case Some(LongV(l)) =>
          ScopeID(l)
        case _ =>
          output.debug(() => s"No scope found in database $key. $data")
          return data
      }

      val newScope =
        if (mapping.containsScope(scope)) {
          mapping.scopeForScope(scope)
        } else {
          // Some backups contain documents for deleted child databases.
          // Their scopes will not have mappings.
          output.warn(s"No mapping for database $key: skipping rewrite")
          throw new Rewriter.IgnoreDB
        }

      val newGlobalID =
        data.get(Database.GlobalIDField.path) map {
          case LongV(l) if l >= 0 && mapping.containsScope(scope) =>
            val prev = GlobalDatabaseID(l)
            mapping.globalIDForScope(scope).getOrElse(prev)
          case _ if !mapping.containsScope(scope) =>
            output.warn(s"No mapping for database $key: skipping rewrite")
            throw new Rewriter.IgnoreDB
          case ir =>
            throw new IllegalStateException(s"Invalid global ID type for $key: $ir.")
        }

      var result = data
      result = result.update(Database.ScopeField.path, LongV(newScope.toLong))
      newGlobalID foreach { g =>
        result = result.update(Database.GlobalIDField.path, LongV(g.toLong))
      }

      output.debug(() => s"Transformed:\n\t$data\n\t$result")
      stats.incrDatabasesRewritten()
      result
    }

    // This is called up to 2 times per version: Once for its data, and once for its
    // diff.
    private def transformAccessProvider(scope: ScopeID, data: MapV): IRValue = {
      def encodeGlobalID(prev: String, scope: ScopeID): Either[_, IRValue] = {
        val globalID = mapping.globalIDForScope(scope) map { id =>
          JWTToken.canonicalDBUrl(id)
        } getOrElse (prev)

        AccessProvider.AudienceField.ftype.encode(globalID).toRight {
          throw new IllegalStateException(s"Failed to encode $globalID.")
        }
      }

      if (
        data.contains(AccessProvider.AudienceField.path) &&
        mapping.containsScope(scope)
      ) {
        val result = data.modify(
          AccessProvider.AudienceField.path,
          AccessProvider.AudienceField.ftype.decode(_, Queue.empty) flatMap {
            encodeGlobalID(_, scope)
          } toOption)

        output.debug(() => s"Transformed:\n\t$data\n\t$result")
        stats.incrAccessProvidersRewritten()
        result
      } else {
        data
      }
    }

    // Given a Cell, decode the Data and Diff, pass each to fn, and
    // re-encode the result as a new Cell value.
    private def transformVersion(cell: Cell, fn: MapV => IRValue): ByteBuffer = {
      require(cell.isLive, s"Cell $cell is a tombstone!")

      val version =
        Version.decodeCell(Unpooled.wrappedBuffer(rowKey.getKey), FCell(cell))

      // Trigger decoding of the data, so accessing the TTL and then data
      // doesn't duplicate work.
      version.data
      val data = (
        version.schemaVersion,
        version.ttl,
        CBOR.encode(if (version.isDeleted) NullV else fn(version.data.fields)),
        version.diff map { d => fn(d.fields) } getOrElse NullV)

      val buf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer

      try {
        CBOR.gatheringEncode(buf, data)(VersionAdd.PartialTupleEncoder)

        // I'm so sorry. I hope there aren't very many Versions to
        // rewrite...
        // This copy must be done because the lifetime of the ByteBuf
        // is shorter than the lifetime of the ByteBuffer derived
        // therefrom.
        val bytes = new Array[Byte](buf.readableBytes())
        buf.getBytes(buf.readerIndex, bytes)

        ByteBuffer.wrap(bytes)
      } finally {
        buf.release()
      }
    }
  }

  /** Lookups may need either (or both) row key or cell name
    * transforms.
    *
    * Row keys contain one of the GlobalID variants: both ScopeID and
    * GlobalDatabaseID are relevant to us here. GlobalKeyIDs must
    * remain unaltered, because they are used for routing requests.
    *
    * Cell names contain ScopeIDs, which may need transformation for
    * any row.
    */
  final class LookupsTransformer(
    rowKey: DecoratedKey,
    dataSize: Long,
    sstable: SSTableReader,
    dataFile: RandomAccessReader,
    mapping: MappingFile,
    snapshotTS: Timestamp,
    includeKeys: Boolean,
    cfmeta: CFMetaData,
    output: OutputHandler,
    stats: RewriterStats)
      extends Transformer[GlobalID](
        rowKey,
        dataSize,
        sstable,
        dataFile,
        Transformer.LookupsCodec,
        snapshotTS,
        output,
        stats) {

    override def includeRow(key: GlobalID) = key match {
      case _: GlobalKeyID =>
        if (includeKeys) {
          stats.incrOutputRows()
        }

        includeKeys

      case id: ScopeID if mapping.containsScope(id) =>
        stats.incrOutputRows()
        true

      case id: GlobalDatabaseID if mapping.containsGlobalID(id) =>
        stats.incrOutputRows()
        true

      // This case is likely caused by a deleted child of a live
      // parent scope.
      case id =>
        output.warn(s"Dropping lookups for $id.")
        false
    }

    def transformKey(key: GlobalID) =
      key match {
        case id: ScopeID          => mapping.scopeForScope(id)
        case id: GlobalDatabaseID => mapping.globalIDForGlobalID(id)
        case id: GlobalKeyID      => id // Keys are left untouched.
      }

    override def transformCell(key: GlobalID, cell: Cell): Cell = {
      val entry =
        Tables.Lookups.decodeLookup(
          Unpooled.wrappedBuffer(rowKey.getKey),
          FCell(cell))

      // This lookup entry likely refers to a parent scope which once
      // contained this global ID, but no longer does. Leave the cell
      // untouched for now.
      if (!mapping.containsScope(entry.scope)) {
        return cell
      }

      val newScope = mapping.scopeForScope(entry.scope)

      // This is probably a key, but regardless it is not of
      // interest.
      if (entry.scope == newScope) {
        return cell
      }

      output.debug(() => s"$key ${entry.scope} transformed to $newScope")
      stats.incrLookupsRewritten()

      val newValue = entry.withScope(newScope).toValue
      val newName = newValue.keyPredicate.uncheckedColumnNameBytes

      if (cell.isLive) {
        new BufferCell(
          cfmeta.comparator.cellFromByteBuffer(newName.nioBuffer),
          cell.value,
          cell.timestamp)
      } else {
        // This should be equiv. to the live case - deletion time is
        // in the cell value, and lookups do not use cell values - but
        // Just In Case(TM)...
        new BufferDeletedCell(
          cfmeta.comparator.cellFromByteBuffer(newName.nioBuffer),
          cell.value,
          cell.timestamp)
      }
    }
  }

  /** Indexes would be trivially transformed - only the ScopeID in
    * each row key needs to be updated - however, support for
    * including/excluding Key and Token requires decoding index terms
    * and values to check for relevant documents.
    */
  final class IndexTransformer[K](
    index: AbstractIndex[K],
    rowKey: DecoratedKey,
    dataSize: Long,
    sstable: SSTableReader,
    dataFile: RandomAccessReader,
    mapping: MappingFile,
    snapshotTS: Timestamp,
    includeKeys: Boolean,
    decode: (ScopeID, Value[K]) => IndexValue,
    output: OutputHandler,
    stats: RewriterStats)
      extends Transformer[(ScopeID, IndexID, Vector[IRValue])](
        rowKey,
        dataSize,
        sstable,
        dataFile,
        Transformer.IndexCodec,
        snapshotTS,
        output,
        stats) {

    override def includeRow(key: (ScopeID, IndexID, Vector[IRValue])) =
      if (includeKeys) {
        stats.incrOutputRows()
        true
      } else {
        key match {
          case (_, _, terms) =>
            val hasKey = terms exists {
              case DocIDV(DocID(_, KeyID.collID))   => true
              case DocIDV(DocID(_, TokenID.collID)) => true
              case _                                => false
            }

            if (!hasKey) {
              stats.incrOutputRows()
              true
            } else {
              false
            }
        }
      }

    def transformKey(key: (ScopeID, IndexID, Vector[IRValue])) = {
      val (scope, id, terms) = key
      (mapping.scopeForScope(scope), id, terms)
    }

    override def transformCell(
      key: (ScopeID, IndexID, Vector[IRValue]),
      cell: Cell): Cell = {
      // No need for the decoding if keys/tokens are included.
      if (includeKeys) {
        return cell
      }

      val value =
        index.decodeIndex(Unpooled.wrappedBuffer(rowKey.getKey), FCell(cell))

      val (scope, _, _) = key
      val indexValue = decode(scope, value)

      indexValue.docID match {
        case DocID(_, KeyID.collID) | DocID(_, TokenID.collID) =>
          // At this point, we must emit a cell of some
          // type. Emitting a tombstone at the epoch indicates that
          // this cell has "always" been deleted.
          new BufferDeletedCell(
            cell.name,
            0 /* epoch seconds */,
            0 /* epoch micros */ )
        case _ => cell
      }
    }
  }
}
