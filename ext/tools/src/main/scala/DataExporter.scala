package fauna.tools

import fauna.atoms._
import fauna.codex.json.JSValue
import fauna.codex.json2.{ JSON, JSONWriter }
import fauna.lang.{ Timestamp, Timing }
import fauna.model.Database
import fauna.repo.doc.Version
import fauna.repo.store._
import fauna.stats.{ StatLevel, StatTags, StatsRecorder, StatsRequestBuffer }
import fauna.storage.{ Cassandra, Cell => FCell, Tables }
import fauna.storage.cassandra.{
  CellFilter,
  NeverPurgeController,
  SSTableIterator,
  SnapshotFilter
}
import fauna.storage.index.IndexTerm
import fauna.storage.ir.{ DocIDV, IRValue }
import io.netty.buffer.{ ByteBufAllocator, Unpooled }
import java.io.{ File, FileInputStream, FileOutputStream, InputStream }
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.time.format.DateTimeParseException
import java.util.{ Collections, Map => JMap, Set => JSet }
import java.util.concurrent.{ ConcurrentHashMap, ConcurrentLinkedQueue }
import java.util.concurrent.atomic.{ DoubleAdder, LongAdder }
import java.util.stream.Collectors
import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.db.{ Cell, ColumnFamilyStore, DecoratedKey }
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator
import org.apache.cassandra.db.compaction.{ CompactionManager, LazilyCompactedRow }
import org.apache.cassandra.io.sstable._
import org.apache.cassandra.io.sstable.metadata.MetadataCollector
import org.apache.cassandra.utils.{ ByteBufferUtil, OutputHandler }
import org.apache.cassandra.utils.concurrent.Refs
import org.apache.commons.cli.{ Option => CliOption, Options }
import scala.collection.mutable.HashMap
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

/** The data exporter tool. It will scan a snapshot data exporting multiple user backups based on filter file.
  *
  * Usage:
  *   export FAUNADB_CONFIG=path/to/faunadb.yml
  *   java -cp path/to/faunadb.jar fauna.tools.DataExporter \
  *       --cf Versions \
  *       --snapshot 1970-01-01T00:00:00Z \
  *       --dump-tree /path/to/dump/tree \
  *       --export /path/to/export/file \
  *       --threads N
  */
object DataExporter extends SSTableApp("Data exporter") {

  override def setPerAppCLIOptions(o: Options) = {
    val cf = new CliOption(null, "cf", true, "Column family name")
    cf.setRequired(true)
    o.addOption(cf)

    val dumpTree = new CliOption(null, "dump-tree", true, "Dump Tree file")
    dumpTree.setRequired(true)
    o.addOption(dumpTree)

    val exportFile = new CliOption(null, "export", true, "Export file")
    exportFile.setRequired(true)
    o.addOption(exportFile)

    o.addOption(null, "snapshot", true, "Snapshot time of live data")

    o.addOption(null, "threads", true, "Number of threads")

    // case cf == Versions: this file will be used to store keys metadata, ie: KeyID
    // -> ScopeID
    // case cf == LookupStore: this file will be use to read keys metadata
    o.addOption(null, "key-metadata", true, "Keys metadata file")
    o.addOption(null, "metadata", true, "Metadata file")
  }

  start {
    val cf = cli.getOptionValue("cf")
    val dumpTree = cli.getOptionValues("dump-tree")

    val metadataFile = Option(cli.getOptionValue("metadata")) orElse {
      Option(cli.getOptionValue("key-metadata"))
    } map {
      new File(_)
    }

    val exportPath = cli.getOptionValue("export")

    val snapshotTS = Option(cli.getOptionValue("snapshot")) flatMap { str =>
      try {
        Some(Timestamp.parse(str))
      } catch {
        case ex: DateTimeParseException =>
          handler.warn(s"Bad snapshot time: $str", ex)
          sys.exit(1)
      }
    } getOrElse Timestamp.MaxMicros

    val nThreads = Option(cli.getOptionValue("threads")) flatMap {
      _.toIntOption
    } getOrElse 0

    val exportStream = new FileInputStream(exportPath)
    val dumpTreeStream = dumpTree.toSeq map {
      new FileInputStream(_)
    }

    val entries = DumpEntry.fromStream(dumpTreeStream: _*)

    val root =
      DatabaseTree.build(entries, snapshotTS = snapshotTS, warn = handler.warn)

    val exporters =
      DBExport.read(root, exportStream, snapshotTS, handler, stats) match {
        case Success(r) => r
        case Failure(ex) =>
          handler.warn("Parsing input error", ex)
          sys.exit(1)
      }

    if (cf == Tables.Versions.CFName || cf == Tables.Lookups.CFName) {
      if (metadataFile.isEmpty) {
        handler.warn(
          "Metadata file is expected for Versions & Lookups column family.")
        sys.exit(1)
      }
    }

    val cfs = openColumnFamily(cf)

    stats.event(
      StatLevel.Info,
      "DataExporter",
      s"Build ${Build.identifier}\n" +
        s"Column Family: $cf\n" +
        s"Threads: $nThreads",
      StatTags.Empty)

    exporters foreach { exporter =>
      val dir = exporter.cfOutputDir(cfs.metadata)

      if (!dir.exists()) {
        require(dir.mkdirs(), s"Could not create column family directory '$dir'")
      }
    }

    val descriptors =
      new ConcurrentLinkedQueue[JMap.Entry[Descriptor, JSet[Component]]]()

    forEachSSTablePath("DataExporter", cfs) { descriptors.add(_) }

    handler.debug(() =>
      s"Found ${descriptors.size} sstables for ${Cassandra.KeyspaceName}.$cf")

    var success = true

    val compressionRatioSum = new DoubleAdder
    val compressionRatioCount = new LongAdder

    val metadataBuilder = new SchemaMetadataBuilder

    val schemaMetadata = if (cf == Tables.Lookups.CFName) {
      SchemaMetadata.readFile(metadataFile.get) match {
        case Success(metadata) => metadata
        case Failure(ex) =>
          handler.warn("Cannot read metadata", ex)
          sys.exit(1)
      }
    } else {
      SchemaMetadata.empty
    }

    val executor = new Executor("Data-Exporter", nThreads)

    // Track the number of rows read as a sanity check against missing
    // input files.
    val rows = new StatsRequestBuffer(Set("DataExporter.Rows"))

    def openSSTable(
      entry: JMap.Entry[Descriptor, JSet[Component]]): SSTableReader = {
      val sstable =
        SSTableReader.openNoValidation(entry.getKey, entry.getValue, cfs.metadata)

      // Reset to L0 to protect against level inversion between
      // tombstones and covered cells.
      sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 0)
      sstable.reloadSSTableMetadata()

      if (sstable.getCompressionRatio != MetadataCollector.NO_COMPRESSION_RATIO) {
        compressionRatioSum.add(sstable.getCompressionRatio)
        compressionRatioCount.increment()
      }

      stats.incr("DataExporter.SSTables")
      sstable
    }

    /** Process a single SSTable using several exporters. The caller is
      * responsible for releasing the SSTableReader Ref, as necessary.
      */
    def work(sstable: SSTableReader, exporters: Seq[DBExport]): Unit = {
      val counts = new HashMap[String, Long].withDefaultValue(0)

      try {

        val outerStats = stats
        val iteratorStats = new SSTableIteratorStats("DataExporter") {
          val stats = new StatsRecorder {
            def count(
              key: String,
              value: Long,
              tags: StatTags = StatTags.Empty): Unit =
              counts(key) += value

            def incr(key: String) = count(key, 1)
            def decr(key: String) = count(key, -1)

            def timing(key: String, value: Long): Unit =
              outerStats.timing(key, value)

            def distribution(key: String, value: Long, tags: StatTags): Unit =
              outerStats.distribution(key, value, tags)
            def event(
              level: StatLevel,
              title: String,
              text: String,
              tags: StatTags): Unit =
              outerStats.event(level, title, text, tags)
            def set(key: String, value: String): Unit = outerStats.set(key, value)
            def set(key: String, value: Double): Unit = outerStats.set(key, value)
          }
        }

        val dataExporter = cfs.name match {
          case Tables.Versions.CFName =>
            new VersionsDataExporter(
              cfs,
              sstable,
              snapshotTS,
              exporters,
              metadataBuilder,
              iteratorStats,
              handler)
          case Tables.HistoricalIndex.CFName =>
            new IndexesDataExporter(
              HistoricalIndex,
              cfs,
              sstable,
              snapshotTS,
              exporters,
              iteratorStats,
              handler)
          case Tables.SortedIndex.CFName =>
            new IndexesDataExporter(
              SortedIndex,
              cfs,
              sstable,
              snapshotTS,
              exporters,
              iteratorStats,
              handler)
          case Tables.Lookups.CFName =>
            new LookupsDataExporter(
              cfs,
              sstable,
              snapshotTS,
              exporters,
              schemaMetadata,
              iteratorStats,
              handler)
          case cf =>
            throw new IllegalStateException(s"Unsupported column family: $cf")
        }

        try {
          dataExporter.run()
        } finally {
          dataExporter.close()
        }
      } catch {
        case t: Throwable =>
          stats.incr("DataExporter.Error")
          success = false
          handler.warn(s"Error exporting $sstable", t)
      } finally {
        counts foreach { case (key, value) =>
          rows.count(key, value)
          stats.count(key, value)
        }
      }
    }

    /** Process a single Descriptor using several exporters in a single
      * thread.
      */
    def exportOne(
      descriptor: JMap.Entry[Descriptor, JSet[Component]],
      exporters: Seq[DBExport]): Unit =
      executor addWorker { () =>
        val sstable = openSSTable(descriptor)
        val ref = sstable.ref()

        try {
          work(sstable, exporters)
        } finally {
          ref.release()
        }
      }

    /** Process all SSTables using several exporters in a single
      * thread.
      */
    def exportAll(
      sstables: Iterable[SSTableReader],
      exporters: Seq[DBExport]): Unit =
      executor addWorker { () =>
        sstables foreach { sstable =>
          val ref = sstable.ref()

          try {
            work(sstable, exporters)
          } finally {
            ref.release()
          }
        }
      }

    // If there are more SSTables than threads, assign one SSTable per
    // thread, with all exporters in each thread.
    if (descriptors.size >= executor.getThreads) {
      while (!descriptors.isEmpty) {
        val desc = descriptors.poll()
        exportOne(desc, exporters)
      }

      executor.waitWorkers()

    } else {
      // There are fewer SSTables than threads, so assign all SSTables
      // to all threads, and partition by groups of exporters per
      // thread to maximize parallelism while reading the data
      // redundantly as few times as possible.
      val sstables = descriptors.stream
        .map { openSSTable(_) }
        .collect(Collectors.toList())

      val refs = Refs.ref(sstables)

      try {
        exporters.grouped(executor.getThreads) foreach { group =>
          exportAll(sstables.asScala, group)
        }

        executor.waitWorkers()
      } finally {
        refs.release()
      }
    }

    val read = rows.countOrZero("DataExporter.Rows")
    if (read <= 0) {
      success = false
      handler.warn(s"Number of rows read should be > 0, but found $read.")
    }

    if (cf == Tables.Versions.CFName) {
      SchemaMetadata
        .writeFile(metadataBuilder.build(), metadataFile.get)
        .failed foreach { ex =>
        success = false
        handler.warn("Cannot write metadata", ex)
      }
    }

    exporters foreach { exporter =>
      exporter.saveDumpTree()

      val count = compressionRatioCount.sum()
      val compressionRatio = if (count != 0) {
        compressionRatioSum.sum() / count
      } else {
        0.0
      }
      exporter.saveStats(cf, compressionRatio)
    }

    if (success) {
      stats.incr("DataExporter.Success")
      sys.exit(0)
    } else {
      stats.incr("DataExporter.Failure")
      sys.exit(1)
    }
  }
}

final class VersionsDataExporter(
  cfs: ColumnFamilyStore,
  sstable: SSTableReader,
  snapshotTS: Timestamp,
  dbExporters: Seq[DBExport],
  metadataBuilder: SchemaMetadataBuilder,
  stats: SSTableIterator.Stats,
  output: OutputHandler)
    extends DataExporter(cfs, sstable, snapshotTS, output, stats) {

  def apply(key: DecoratedKey, dataSize: Long): Unit = {
    val position = dataFile.getPosition

    val (parentScopeID, docID) =
      Tables.Versions.decodeRowKey(Unpooled.wrappedBuffer(key.getKey))

    var cellCount = 0

    val iter = dataIterator(key, dataSize)

    // This row is covered by a row tombstone. Register that fact with
    // the metadata builder.
    if (iter.getColumnFamily.isMarkedForDelete()) {
      metadataBuilder.markDeleted(parentScopeID, docID)
    } else {
      var dbScope = Option.empty[ScopeID]

      iter.asScala foreach { atom =>
        require(atom.isInstanceOf[Cell], "Range tombstones should not exist.")

        cellCount += 1

        val cell = atom.asInstanceOf[Cell]

        docID match {
          // if we are handling a database schema, we need to
          // grab its scope field to check later if that scope
          // should be exported or not.
          case DatabaseID(_) if cell.isLive() =>
            val version = decodeVersion(key, cell)
            metadataBuilder.handleVersion(version)

            if (dbScope.isEmpty) {
              dbScope = version.data.getOpt(Database.ScopeField)
            }

          case CollectionID(_) | IndexID(_) | KeyID(_) if cell.isLive =>
            metadataBuilder.handleVersion(decodeVersion(key, cell))

          case _ => ()
        }
      }

      dbExporters foreach { exporter =>
        if (exporter.filter.filterVersions(parentScopeID, docID)) {
          val shouldWriteRow =
            if (exporter.filter.isParentKey(parentScopeID, docID)) {
              val dbID = metadataBuilder.getDatabase(parentScopeID, docID.as[KeyID])

              dbID contains exporter.filter.tree.dbID
            } else if (docID.collID == DatabaseID.collID) {
              dbScope exists {
                exporter.filter.filterScope
              }
            } else {
              true
            }

          if (shouldWriteRow) {
            dataFile.seek(position)

            val row =
              new LazilyCompactedRow(
                controller,
                Collections.singletonList(dataIterator(key, dataSize)))

            addRow(exporter, row) { bytesWritten =>
              exporter.exportStats.recordRowBytes(bytesWritten)
              exporter.exportStats.incrOutputRows()
              exporter.exportStats.incrOutputCells(cellCount)
            }
          }
        }
      }
    }
  }

  private def decodeVersion(key: DecoratedKey, cell: Cell): Version =
    Version.decodeCell(Unpooled.wrappedBuffer(key.getKey), FCell(cell))
}

final class IndexesDataExporter[K](
  private[this] val index: AbstractIndex[K],
  cfs: ColumnFamilyStore,
  sstable: SSTableReader,
  snapshotTS: Timestamp,
  dbExporters: Seq[DBExport],
  stats: SSTableIterator.Stats,
  output: OutputHandler)
    extends DataExporter(cfs, sstable, snapshotTS, output, stats) {

  def apply(key: DecoratedKey, dataSize: Long): Unit = {
    val position = dataFile.getPosition

    val decodedKey = Tables.Indexes.decode(Unpooled.wrappedBuffer(key.getKey))
    val (scopeID, indexID, _) = decodedKey

    dbExporters foreach { exporter =>
      if (exporter.filter.filterIndexes(scopeID, indexID)) {
        dataFile.seek(position)

        val cellFilter =
          new IndexCellFilter(
            exporter.filter,
            decodedKey,
            dataIterator(key, dataSize))

        val row =
          new LazilyCompactedRow(controller, Collections.singletonList(cellFilter))

        addRow(exporter, row) { bytesWritten =>
          exporter.exportStats.recordRowBytes(bytesWritten)
          exporter.exportStats.incrOutputRows()
          exporter.exportStats.incrOutputCells(cellFilter.cellCount)
        }
      }
    }
  }

  private final class IndexCellFilter(
    private[this] val dbFilter: DatabaseFilter,
    private[this] val key: (ScopeID, IndexID, Vector[IRValue]),
    private[this] val iterator: OnDiskAtomIterator)
      extends CellFilter(iterator) {

    def filter(cell: Cell): Boolean = {
      val (scope, _, terms) = key

      if (scope == dbFilter.parentScopeID) {
        // if the scope is the parent container, check if the
        // entry index the user database

        val existOnTerms = terms exists {
          case DocIDV(dbFilter.dbID) => true
          case _                     => false
        }

        if (existOnTerms) {
          return true
        }

        val value =
          index.decodeIndex(Unpooled.wrappedBuffer(getKey.getKey), FCell(cell))
        val indexValue = index.fromValue(scope, value)
        val values = indexValue.tuple.values

        val existOnValues = values exists {
          case IndexTerm(DocIDV(dbFilter.dbID), _) => true
          case _                                   => false
        }

        if (existOnValues) {
          return true
        }

        indexValue.docID == dbFilter.dbID
      } else {
        dbFilter.scopeIDs contains scope.toLong
      }
    }
  }
}

final class LookupsDataExporter(
  cfs: ColumnFamilyStore,
  sstable: SSTableReader,
  snapshotTS: Timestamp,
  dbExporters: Seq[DBExport],
  schemaMetadata: SchemaMetadata,
  stats: SSTableIterator.Stats,
  output: OutputHandler)
    extends DataExporter(cfs, sstable, snapshotTS, output, stats) {

  def apply(key: DecoratedKey, dataSize: Long): Unit = {
    val position = dataFile.getPosition

    val globalID = Tables.Lookups.decode(Unpooled.wrappedBuffer(key.getKey))

    dbExporters foreach { exporter =>
      if (exporter.filter.filterLookupRow(globalID, schemaMetadata)) {
        dataFile.seek(position)

        val cellFilter =
          new LookupCellFilter(
            globalID,
            exporter.filter,
            key,
            dataIterator(key, dataSize))

        val row =
          new LazilyCompactedRow(controller, Collections.singletonList(cellFilter))

        addRow(exporter, row) { bytesWritten =>
          exporter.exportStats.recordRowBytes(bytesWritten)
          exporter.exportStats.incrOutputRows()
          exporter.exportStats.incrOutputCells(cellFilter.cellCount)
        }
      }
    }
  }

  def decodeLookup(rowKey: DecoratedKey, cell: Cell) =
    Tables.Lookups.decodeDatabase(Unpooled.wrappedBuffer(rowKey.getKey), FCell(cell))

  private final class LookupCellFilter(
    private[this] val globalID: GlobalID,
    private[this] val dbFilter: DatabaseFilter,
    private[this] val rowKey: DecoratedKey,
    private[this] val iterator: OnDiskAtomIterator)
      extends CellFilter(iterator) {

    def filter(cell: Cell): Boolean = {
      val (scope, id) = decodeLookup(rowKey, cell)
      dbFilter.filterLookupCell(globalID, scope, id)
    }
  }
}

/** Generic data exporter. Implementations should add logic to filter cells/rows based on its column family.
  *
  * @param cfs The column family store.
  * @param sstable The sstable to export.
  * @param snapshotTS Snapshot timestamp, any cell after this timestamp will be removed from the backup.
  * @param output The logger.
  * @param stats the StatsRecoder to emit stats.
  */
abstract class DataExporter(
  cfs: ColumnFamilyStore,
  sstable: SSTableReader,
  snapshotTS: Timestamp,
  output: OutputHandler,
  stats: SSTableIterator.Stats)
    extends SSTableIterator(sstable, output, stats) {

  protected[this] val controller = new NeverPurgeController(cfs)

  private[this] val expectedBloomFilterSize =
    Math.max(
      cfs.metadata.getMinIndexInterval(),
      SSTableReader.getApproximateKeyCount(Collections.singletonList(sstable)))

  private[this] val writers =
    new ConcurrentHashMap[GlobalDatabaseID, SSTableWriter]()

  override def run() = {
    output.debug(() => s"Exporting $sstable (${dataFile.length()} bytes)")
    stats.recordSSTableBytes(dataFile.length())

    // Disable bloom filters.
    cfs.metadata.bloomFilterFpChance(1.0)

    val start = Timing.start
    try {
      super.run()
      finish()
    } catch {
      case NonFatal(ex) =>
        abort(ex.getMessage)
        throw ex
    } finally {
      stats.recordRuntime(start.elapsedMillis)
      controller.close()
    }
  }

  def finish(): Unit = {
    output.debug(() => s"Finishing $sstable")

    val repairedAt = sstable.getSSTableMetadata.repairedAt

    writers forEach { (_, writer) =>
      if (writer.getFilePointer > 0) {
        val reader = writer.finish(
          SSTableWriter.FinishType.NORMAL,
          sstable.maxDataAge,
          repairedAt)

        reader.selfRef().release()
      } else {
        writer.abortQuietly("Empty file")
      }
    }
  }

  def abort(reason: String): Unit = {
    output.debug(() => s"Aborting $sstable")

    writers forEach { (_, writer) =>
      writer.abort(reason)
    }
  }

  protected def getWriterFor(exporter: DBExport): SSTableWriter = {
    def newSSTableWriter() = {
      CompactionManager.createWriter(
        cfs,
        exporter.cfOutputDir(sstable.metadata),
        expectedBloomFilterSize,
        sstable.getSSTableMetadata.repairedAt,
        sstable)
    }

    writers.computeIfAbsent(
      exporter.filter.tree.globalID,
      _ => newSSTableWriter()
    )
  }

  protected def addRow(exporter: DBExport, row: LazilyCompactedRow)(
    stats: Long => Unit): Unit = {
    val writer = getWriterFor(exporter)

    val position = writer.getFilePointer
    writer.append(row)
    val bytesWritten = writer.getFilePointer - position

    if (bytesWritten > 0) {
      stats(bytesWritten)
    }
  }

  // returns a OnDiskAtomIterator that filters cells based on snapshotTS
  protected def dataIterator(key: DecoratedKey, dataSize: Long) =
    new SnapshotFilter(sstable, dataFile, snapshotTS, key, dataSize)

}

/** Describes the database hierarchy to be exported. */
case class DBExport(filter: DatabaseFilter, stats: StatsRecorder, outputDir: File) {

  val exportStats = new DataExporterStats("Exporter", stats)

  def cfOutputDir(metadata: CFMetaData) = {
    val cfId =
      ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(metadata.cfId))
    val cfFolder = s"${metadata.cfName}-$cfId"

    new File(outputDir, cfFolder)
  }

  def saveDumpTree(): Unit = {
    val file = new File(outputDir, "dump-tree.json")
    val stream = new FileOutputStream(file)

    try {
      filter.tree.toJSON.writeTo(stream, pretty = true)
    } finally {
      stream.close()
    }
  }

  def saveStats(cf: String, compressionRatio: Double): Unit = {
    val file = new File(outputDir, s"stats-$cf.json")

    val buf = ByteBufAllocator.DEFAULT.ioBuffer
    try {
      val out = JSONWriter(buf)

      out.writeObjectStart()
      out.writeObjectField(
        JSON.Escaped("column_family"), {
          out.writeString(cf)
        })

      out.writeObjectField(
        JSON.Escaped("compression_ratio"), {
          out.writeNumber(compressionRatio)
        })

      out.writeObjectField(
        JSON.Escaped("stats"), {
          out.writeDelimiter()
          exportStats.toJson(out)
        })
      out.writeObjectEnd()

      val chan = FileChannel.open(
        file.toPath,
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE)
      try {
        buf.readBytes(chan, 0, buf.readableBytes())
      } finally {
        chan.close()
      }
    } finally {
      buf.release()
    }
  }
}

/** Export file represents a list of databases the export tool should create backups.
  *
  * {
  *   "export": arrays of Export objects, see below.
  * }
  *
  * Export:
  * {
  *   "global_id":   database global_id to export.
  *   "output_dir":  path where the snapshot files will be saved.
  *   "filter_auth": True filters out auth documents, False preserve auth documents.
  * }
  */
object DBExport {

  def read(
    databaseTree: DatabaseTree,
    stream: InputStream,
    snapshotTS: Timestamp,
    handler: OutputHandler,
    stats: StatsRecorder): Try[Seq[DBExport]] = {
    val source = Source.fromInputStream(stream).mkString

    JSON.tryParse[JSValue](source.getBytes) map { json =>
      val requests = (json / "export").as[Seq[JSValue]]

      requests flatMap { request =>
        try {
          val globalIDStr = (request / "global_id").as[String]

          val tree = Database.decodeGlobalID(globalIDStr) match {
            case Some(globalID) =>
              val entry = databaseTree.forGlobalID(globalID) orElse {
                databaseTree.forScopeID(ScopeID(globalID.toLong))
              }

              entry match {
                case None =>
                  throw new IllegalStateException(
                    s"Could not find id '$globalIDStr' in dump-tree file.")
                case Some(e) if e.deletedTS exists { _ < snapshotTS } =>
                  throw new IllegalStateException(
                    s"'$globalIDStr' is deleted (${e.deletedTS} < $snapshotTS)")
                case Some(e) => e
              }

            case None =>
              throw new IllegalStateException(
                s"'$globalIDStr' is not a valid global id.")
          }

          val outputDir = new File((request / "output_dir").as[String])

          if (outputDir.exists()) {
            require(outputDir.isDirectory, s"'$outputDir' is not a directory.")
            require(outputDir.canWrite, s"Cannot write to '$outputDir'.")
          } else {
            require(outputDir.mkdirs(), s"Could not create '$outputDir'")
          }

          Some(DBExport(DatabaseFilter(tree), stats, outputDir))
        } catch {
          case NonFatal(ex) =>
            handler.warn(s"Error while reading request: $request", ex)
            None
        }
      }
    }
  }
}
