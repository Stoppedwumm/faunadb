package fauna.tools

import fauna.atoms._
import fauna.codex.json._
import fauna.lang.clocks._
import fauna.lang.Timestamp
import fauna.model.util.CommonLogging
import fauna.repo.doc.Version
import fauna.stats.StatsRecorder
import fauna.storage.{ ScanBounds, Selector, Tables }
import fauna.storage.cassandra.{
  CassandraIterator,
  IteratorStats,
  IteratorStatsCounter
}
import fauna.util.ZBase32
import io.netty.buffer.Unpooled
import java.io.{ File, FileOutputStream }
import java.time.format.DateTimeParseException
import java.time.LocalDate
import java.util.concurrent.atomic.LongAdder
import java.util.UUID
import org.apache.commons.cli.Options
import scala.collection.mutable.{ Map => MMap }
import scala.concurrent.duration._
import scala.util._

/** Typical usage is like so:
  *   export FAUNADB_CONFIG=path/to/faunadb.yml
  *   java -cp path/to/faunadb.jar fauna.tools.SizeEstimator \
  *     --cf Versions \
  *     --metadata metadata.txt
  *     --segments 10
  */
object SizeEstimator extends SSTableApp("Size Estimator") {

  override def setPerAppCLIOptions(o: Options) = {
    o.addOption(null, "cf", true, "Column family name")
    o.addOption(null, "output", true, "Output file")
    o.addOption(null, "metadata", true, "Metadata file")
    o.addOption(null, "segments", true, "Number of segments (default: 1)")
    o.addOption(null, "snapshot", true, "Snapshot time of live data")
    o.addOption(null, "replica", true, "Replica name")
    o.addOption(null, "run-date", true, "Run date")
    o.addOption(null, "threads", true, "Number of threads (default: ncpu)")
    o.addOption(null, "multiplier", true, "Size multiplier")
  }

  start {
    val runId = UUID.randomUUID();
    val cfs = cli.getOptionValue("cf", Tables.Versions.CFName).split(',').toSeq

    val threads = Option(cli.getOptionValue("threads")) flatMap {
      _.toIntOption
    } getOrElse 0

    val segments = Option(cli.getOptionValue("segments")) map { str =>
      val segs = str.split(',').toSeq flatMap { _.toIntOption }

      if (cfs.size != segs.size) {
        handler.warn(
          s"Number of segments doesn't match the number of column families")
        sys.exit(1)
      }

      (cfs zip segs) toMap
    } getOrElse {
      handler.warn("Number of segments not specified, defaulting to 1")
      Map.empty[String, Int].withDefaultValue(1)
    }

    val multipliers = Option(cli.getOptionValue("multiplier")) map { str =>
      val values = str.split(',').toSeq flatMap { _.toIntOption }

      if (cfs.size != values.size) {
        handler.warn(
          s"Number of multipliers doesn't match the number of column families")
        sys.exit(1)
      }

      (cfs zip values) toMap
    } getOrElse {
      handler.warn(
        "Number of multipliers not specified, defaulting to 1 for versions and 2 for indexes")
      Map(
        Tables.Versions.CFName -> 1,
        Tables.HistoricalIndex.CFName -> 2,
        Tables.SortedIndex.CFName -> 2
      )
    }

    val snapshotTS = Option(cli.getOptionValue("snapshot")) flatMap { str =>
      try {
        Some(Timestamp.parse(str))
      } catch {
        case ex: DateTimeParseException =>
          handler.warn(s"Bad snapshot time: $str", ex)
          sys.exit(1)
      }
    } getOrElse {
      handler.warn(
        s"Snapshot timestamp not specified, defaulting to ${Timestamp.MaxMicros}")
      Timestamp.MaxMicros
    }

    val runDate = Option(cli.getOptionValue("run-date")) map {
      LocalDate.parse(_)
    }

    val replica = Option(cli.getOptionValue("replica"))

    val metadataFile = Option(cli.getOptionValue("metadata")) map {
      new File(_)
    } getOrElse {
      handler.warn("Metadata file not specified")
      sys.exit(1)
    }

    val output = Option(cli.getOptionValue("output")) map { str =>
      val out = str.split(',') map { new File(_) } toSeq

      if (out.size != cfs.size) {
        handler.warn(
          s"Number of outputs doesn't match the number of column families")
        sys.exit(1)
      }

      (cfs zip out) toMap
    } getOrElse {
      handler.warn("Output file not specified")
      sys.exit(1)
    }

    val storageType = Map(
      Tables.Versions.CFName -> "versions_v2",
      Tables.HistoricalIndex.CFName -> "indexes_v2",
      Tables.SortedIndex.CFName -> "indexes_v2"
    )

    val totalScanners = segments.values.sum
    val started = new LongAdder
    val completed = new LongAdder
    var reporter = Reporter.empty

    StatsRecorder.polling(10.seconds) {
      reporter(stats)
      stats.set("SizeEstimator.Scanners.Total", totalScanners)
      stats.set("SizeEstimator.Scanners.Started", started.sum())
      stats.set("SizeEstimator.Scanners.Completed", completed.sum())
    }

    def run(cf: String): Boolean = {
      val cfs = openColumnFamily(cf, withSSTables = true)

      val executor = new Executor("SizeEstimator", threads)
      val sizeMetadata = new SizeMetadata
      val metadataBuilder = new SchemaMetadataBuilder
      val counter = new IteratorStatsCounter
      reporter = Reporter.forColumnFamily(cf, counter)

      val multiplier = multipliers(cf)

      var success = true

      Segment.All.subSegments(segments(cf)) foreach { segment =>
        executor addWorker { () =>
          try {
            started.increment()

            stats.incr("SizeEstimator.Segment")

            var lastKey = Unpooled.EMPTY_BUFFER

            val iter =
              new CassandraIterator(
                counter,
                cfs,
                ScanBounds(segment),
                Selector.All,
                snapshotTS)
            while (iter.hasNext) {
              val (key, cell) = iter.next()

              cfs.name match {
                case Tables.Versions.CFName =>
                  val version = Version.decodeCell(key, cell)

                  if (lastKey != key) {
                    lastKey = key
                    sizeMetadata.incrVersionsSize(
                      version.parentScopeID,
                      version.id,
                      key.readableBytes * multiplier)
                  }

                  sizeMetadata.incrVersionsSize(
                    version.parentScopeID,
                    version.id,
                    cell.byteSize * multiplier)
                  metadataBuilder.handleVersion(version)

                case Tables.HistoricalIndex.CFName | Tables.SortedIndex.CFName =>
                  key.markReaderIndex()
                  val (scope, id, _) = Tables.Indexes.decode(key)
                  key.resetReaderIndex()

                  if (lastKey != key) {
                    lastKey = key
                    sizeMetadata.incrIndexesSize(
                      scope,
                      id,
                      key.readableBytes * multiplier)
                  }

                  sizeMetadata.incrIndexesSize(scope, id, cell.byteSize * multiplier)

                case _ =>
                  throw new IllegalArgumentException(s"Column family not supported")
              }
            }

            completed.increment()
          } catch {
            case t: Throwable =>
              stats.incr("SizeEstimator.Error")
              success = false
              handler.warn(s"Error estimating $segment", t)
              throw t
          }
        }
      }

      executor.waitWorkers()

      val metadataMissing = !metadataFile.exists() || metadataFile.length() == 0L

      val schemaMetadata = if (cf == Tables.Versions.CFName && metadataMissing) {
        // Only write metadata if it doesn't exist or if it's empty.
        // This is because we use SizeEstimator after DataExporter
        // on each exported database, given DataExporter creates a global
        // metadata, we should avoid overwrite this file.
        val schemaMetadata = metadataBuilder.build()

        SchemaMetadata.writeFile(schemaMetadata, metadataFile) match {
          case Failure(ex) =>
            handler.warn("Cannot write metadata", ex)
            sys.exit(1)
          case Success(_) => ()
        }

        schemaMetadata
      } else {
        // Loading this file can consume many GBs when there are a lot of DBs,
        // collections, and indexes. In the parallel enrichment case, it can easily
        // cause an OOME.
        SchemaMetadata.readFile(metadataFile) match {
          case Success(metadata) => metadata
          case Failure(ex) =>
            handler.warn("Cannot read metadata", ex)
            sys.exit(1)
        }
      }

      val sizes = sizeMetadata.calculateSize(schemaMetadata)

      val clock = runDate map { date =>
        new MonotonicClock(
          new TimestampClock { val time = Timestamp(date) },
          1.micro)
      } getOrElse {
        Clock
      }

      writeLog(
        output(cf),
        sizes,
        replica,
        storageType(cf),
        clock,
        cfs.getCompressionRatio,
        schemaMetadata,
        runId,
        snapshotTS
      ) match {
        case Failure(ex) =>
          success = false
          handler.warn("Cannot write output file", ex)

        case Success(_) => ()
      }

      success
    }

    handler.output(
      s"Size Estimator run starting, id=${runId} snapshot=${snapshotTS}")

    var success = true

    cfs foreach { cf =>
      if (success) {
        success = run(cf)
      }
    }

    if (success) {
      stats.incr("SizeEstimator.Success")
      handler.output(
        s"Size Estimator run succeeded, id=${runId} snapshot=${snapshotTS}")
      sys.exit(0)
    } else {
      stats.incr("SizeEstimator.Failure")
      handler.output(
        s"Size Estimator run failed, id=${runId} snapshot=${snapshotTS}")
      sys.exit(1)
    }
  }

  trait Reporter extends (StatsRecorder => Unit)

  object Reporter {
    def empty: Reporter = (_: StatsRecorder) => ()

    def forColumnFamily(name: String, counter: IteratorStatsCounter): Reporter =
      new CFReporter(name, counter)

    class CFReporter(cf: String, counter: IteratorStatsCounter) extends Reporter {
      private[this] val metricRows = s"SizeEstimator.$cf.Rows"
      private[this] val metricCells = s"SizeEstimator.$cf.Cells"
      private[this] val metricTombstones = s"SizeEstimator.$cf.Tombstones"
      private[this] val metricLiveCells = s"SizeEstimator.$cf.Cells.Live"
      private[this] val metricUnbornCells = s"SizeEstimator.$cf.Cells.Unborn"
      private[this] val metricDeletedCells = s"SizeEstimator.$cf.Cells.Deleted"
      private[this] val metricKiloBytesRead = s"SizeEstimator.$cf.KiloBytesRead"

      private[this] var statSnap = IteratorStats.zero

      def apply(stats: StatsRecorder): Unit = {
        val snap = counter.snapshot()
        val prev = statSnap
        statSnap = snap

        val diff = snap - prev

        stats.count(metricRows, diff.rows.toInt)
        stats.count(metricCells, diff.cells.toInt)
        stats.count(metricTombstones, diff.tombstones.toInt)
        stats.count(metricLiveCells, diff.liveCells.toInt)
        stats.count(metricUnbornCells, diff.unbornCells.toInt)
        stats.count(metricDeletedCells, diff.deletedCells.toInt)
        stats.count(metricKiloBytesRead, diff.kiloBytesRead.toInt)
      }
    }
  }

  private def writeLog(
    output: File,
    sizes: Map[ScopeID, Long],
    replica: Option[String],
    storageType: String,
    clock: Clock,
    compressionRatio: Double,
    schemaMetadata: SchemaMetadata,
    runId: UUID,
    snapshotTS: Timestamp
  ): Try[Unit] = {

    val refs = MMap.empty[ScopeID, Option[JSValue]]

    val collRef = JSObject("@ref" -> JSObject("id" -> "databases"))

    def getRef(scope: ScopeID, depth: Int): Option[JSValue] = {
      if (scope == ScopeID.RootID) {
        return if (depth > 0) None else Some("__root__ ")
      }

      refs.getOrElseUpdate(
        scope, {
          schemaMetadata.databaseForScope(scope) map {
            case SchemaMetadata.DB(_, parentScope, _, _, _, name, _, _, _, _) =>
              getRef(parentScope, depth + 1) match {
                case Some(parentRef) =>
                  JSObject(
                    "@ref" -> JSObject(
                      "id" -> name,
                      "collection" -> collRef,
                      "database" -> parentRef
                    )
                  )
                case None =>
                  JSObject(
                    "@ref" -> JSObject(
                      "id" -> name,
                      "collection" -> collRef
                    ))
              }
          }
        }
      )
    }

    Using(new FileOutputStream(output)) { stream =>
      sizes foreach { case (scope, uncompressed) =>
        // db will None for cases where we have data size reported under the scope
        // but we never encountered any Version with the database schema on it. In
        // this case lets emit a log with some information so we can try to cleanup
        // those cases in the future.
        val db = schemaMetadata.databaseForScope(scope)
        val compressed = (uncompressed * compressionRatio).toLong

        val log = JSObject(
          "run_id" -> runId.toString,
          "snapshot_ts" -> snapshotTS.toInstant.toString,
          "host" -> "not_used",
          "replica" -> replica,
          "ts" -> clock.time.toInstant.toString,
          "storage_period" -> 1.day.toSeconds,
          "storage_type" -> storageType,
          "database" -> getRef(scope, 0),
          "scope_id" -> scope.toLong,
          "global_id" -> db.map(db => ZBase32.encodeLong(db.globalID.toLong)),
          CommonLogging.GlobalIDFieldStr -> db.map(db =>
            db.globalIDPath.map(gid => ZBase32.encodeLong(gid.toLong))),
          "parent_id" -> db.map(_.parentScopeID.toLong),
          "db_id" -> db.map(_.id.toLong),
          "stats" -> JSObject(
            "storage_bytes" -> compressed,
            "storage_uncompressed_bytes" -> uncompressed
          )
        )

        log.writeTo(stream, pretty = false)
        stream.write('\n')
      }
    }
  }
}
