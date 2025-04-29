package fauna.tools

import fauna.api.FaunaApp
import fauna.atoms.{ CollectionID, ScopeID, Segment }
import fauna.codex.json.JSObject
import fauna.lang.Timestamp
import fauna.repo.doc.Version
import fauna.repo.RepoContext
import fauna.stats.StatsRecorder
import fauna.storage.{ ScanBounds, Selector, Tables }
import fauna.storage.cassandra.{ CassandraIterator, IteratorStatsCounter }
import fauna.storage.index.NativeIndexID
import fauna.tools.{ Executor, RepoTool, SchemaServiceCache }
import fauna.tools.SizeEstimator.Reporter
import io.netty.buffer.Unpooled
import java.io.{ File, FileOutputStream }
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.ConcurrentHashMap
import org.apache.cassandra.db.Keyspace
import org.apache.cassandra.utils.OutputHandler
import org.apache.commons.cli.Options
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala
import scala.util.{ Failure, Success, Try, Using }

object SnapshotSizeEstimator extends FaunaApp("Snapshot Size Estimator") {

  override def setupLogging() =
    Some(config.logging.configure(None, debugConsole = true))

  lazy val handler = new OutputHandler.SystemOutput(false, false)

  override def setPerAppCLIOptions(o: Options) = {
    o.addOption(null, "cf", true, "Column family name")
    o.addOption(null, "output", true, "Output file")
    o.addOption(null, "segments", true, "Number of segments (default: 1)")
    o.addOption(null, "threads", true, "Number of threads (default: ncpu)")
  }

  start {
    val cfs = cli.getOptionValue("cf", Tables.Versions.CFName).split(',').toSeq

    val threads = Option(cli.getOptionValue("threads")) flatMap {
      _.toIntOption
    } getOrElse 0

    val segments = Option(cli.getOptionValue("segments")) map { str =>
      val segs = str.split(',').toSeq flatMap { _.toIntOption }

      if (cfs.size != segs.size) {
        handler.warn(
          "Number of segments doesn't match the number of column families")
        sys.exit(1)
      }

      (cfs zip segs) toMap
    } getOrElse {
      handler.warn("Number of segments not specified, defaulting to 1")
      Map.empty[String, Int].withDefaultValue(1)
    }

    /** When we run this tool against customer snapshots, we run it for only HistoricalIndex and multiply that by
      * 2 to account for the size across both Historical and Sorted Index.
      */
    val multipliers = Map(
      Tables.Versions.CFName -> 1,
      Tables.HistoricalIndex.CFName -> 2,
      Tables.SortedIndex.CFName -> 2
    )

    val output = Option(cli.getOptionValue("output")) map { str =>
      val out = str.split(',') map { new File(_) } toSeq

      if (out.size != cfs.size) {
        handler.warn("Number of outputs doesn't match the number of column families")
        sys.exit(1)
      }

      (cfs zip out) toMap
    } getOrElse {
      handler.warn("Output file not specified")
      sys.exit(1)
    }

    val totalScanners = segments.values.sum
    val started = new LongAdder
    val completed = new LongAdder
    var reporter = Reporter.empty

    StatsRecorder.polling(10.seconds) {
      reporter(stats)
      stats.set("SnapshotSizeEstimator.Scanners.Total", totalScanners)
      stats.set("SnapshotSizeEstimator.Scanners.Started", started.sum())
      stats.set("SnapshotSizeEstimator.Scanners.Completed", completed.sum())
    }

    def run(cf: String, repo: RepoContext): Boolean = {
      val cfs = {
        val ks = Keyspace.open(repo.storage.keyspaceName)
        ks.getColumnFamilyStore(cf)
      }

      val executor = new Executor("SnapshotSizeEstimator", threads)

      val schemaServiceCache = new SchemaServiceCache(repo, handler)

      val counter = new IteratorStatsCounter
      reporter = Reporter.forColumnFamily(cf, counter)

      val multiplier = multipliers(cf)

      var success = true

      val scopeSizes = new ConcurrentHashMap[ScopeID, LongAdder]()

      Segment.All.subSegments(segments(cf)) foreach { segment =>
        executor addWorker { () =>
          try {
            started.increment()

            stats.incr("SnapshotSizeEstimator.Segment")

            var lastKey = Unpooled.EMPTY_BUFFER

            val iter =
              new CassandraIterator(
                counter,
                cfs,
                ScanBounds(segment),
                Selector.All,
                Timestamp.MaxMicros)
            while (iter.hasNext) {
              val (key, cell) = iter.next()

              cfs.name match {
                case Tables.Versions.CFName =>
                  val version = Version.decodeCell(key, cell)

                  val collID = version.id match {
                    case CollectionID(collID) =>
                      collID
                    case id => id.collID
                  }
                  if (lastKey != key) {
                    lastKey = key

                    if (
                      schemaServiceCache
                        .collectionExists(version.parentScopeID, collID)
                    ) {
                      scopeSizes
                        .computeIfAbsent(version.parentScopeID, _ => new LongAdder)
                        .add(key.readableBytes * multiplier)
                    }
                  }

                  if (
                    schemaServiceCache
                      .collectionExists(version.parentScopeID, collID)
                  ) {
                    scopeSizes
                      .computeIfAbsent(version.parentScopeID, _ => new LongAdder)
                      .add(cell.byteSize * multiplier)
                  }

                case Tables.HistoricalIndex.CFName | Tables.SortedIndex.CFName =>
                  key.markReaderIndex()
                  val (scope, id, _) = Tables.Indexes.decode(key)
                  key.resetReaderIndex()
                  // TODO: move changes by collection into a separate field in the
                  // output file.
                  if (id != NativeIndexID.ChangesByCollection.id) {
                    if (lastKey != key) {
                      lastKey = key
                      if (
                        schemaServiceCache
                          .indexExists(scope, id)
                      ) {
                        scopeSizes
                          .computeIfAbsent(scope, _ => new LongAdder)
                          .add(key.readableBytes * multiplier)
                      }
                    }

                    if (
                      schemaServiceCache
                        .indexExists(scope, id)
                    ) {
                      scopeSizes
                        .computeIfAbsent(scope, _ => new LongAdder)
                        .add(cell.byteSize * multiplier)
                    }

                  }

                case _ =>
                  throw new IllegalArgumentException("Column family not supported")
              }
            }

            completed.increment()
          } catch {
            case t: Throwable =>
              stats.incr("SnapshotSizeEstimator.Error")
              success = false
              handler.warn(s"Error estimating $segment", t)
              throw t
          }
        }
      }

      executor.waitWorkers()

      writeLog(output(cf), scopeSizes.asScala.toMap, cfs.getCompressionRatio) match {
        case Failure(ex) =>
          success = false
          handler.warn("Cannot write output file", ex)

        case Success(_) => ()
      }

      success
    }

    handler.output("Snapshot Size Estimator run starting")

    var success = true

    val repo = {
      handler.output("Setting up repo...")
      val repo = RepoTool.setupRepoWithoutCompactions(config, stats)
      handler.output("Repo is setup.")
      repo
    }

    cfs foreach { cf =>
      if (success) {
        success = run(cf, repo)
      }
    }

    // todo: Would it be useful to include the customer global id we are running for
    // so that we
    // could identify for which snapshot the size estimation failed?
    if (success) {
      stats.incr("SnapshotSizeEstimator.Success")
      handler.output("Snapshot Size Estimator run succeeded")
      sys.exit(0)
    } else {
      stats.incr("SnapshotSizeEstimator.Failure")
      handler.output("Snapshot Size Estimator run failed")
      sys.exit(1)
    }
  }

  private def writeLog(
    output: File,
    sizes: Map[ScopeID, LongAdder],
    compressionRatio: Double): Try[Unit] = {
    Using(new FileOutputStream(output)) { stream =>
      sizes foreach { case (scope, uncompressedSizeAddr) =>
        val uncompressedSize = uncompressedSizeAddr.sum()
        val compressedSize = (uncompressedSize * compressionRatio).toLong

        val log = JSObject(
          "scope_id" -> scope.toLong,
          "stats" -> JSObject(
            "storage_bytes" -> compressedSize,
            "storage_uncompressed_bytes" -> uncompressedSize
          )
        )

        log.writeTo(stream, pretty = false)
        stream.write('\n')
      }
    }
  }
}
