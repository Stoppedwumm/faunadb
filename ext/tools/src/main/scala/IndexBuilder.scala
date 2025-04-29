package fauna.tools

import fauna.atoms._
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.schema.NativeIndex
import fauna.repo.{ Executor => _, _ }
import fauna.repo.query._
import fauna.repo.service.rateLimits.PermissiveOpsLimiter
import fauna.scheduler.PriorityGroup
import fauna.storage._
import fauna.storage.cassandra._
import fauna.storage.ops._
import java.io.File
import java.time.format.DateTimeParseException
import org.apache.commons.cli.Options
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.util.{ Failure, Success }

object IndexBuilder extends SSTableApp("Index Builder") {

  override def setPerAppCLIOptions(o: Options) = {
    o.addOption(null, "index-id", true,
      "IndexID of the index to build. Must be a native index. Required.")
    o.addOption(null, "metadata", true, "Metadata file. Required.")
    o.addOption(
      null,
      "primary",
      false,
      "Write to the primary column families. (SortedIndex/HistoricalIndex)")
    o.addOption(null, "scope-id", true, "ScopeID of the index to build. Required.")
    o.addOption(
      null,
      "secondary",
      false,
      "Write to the secondary column families. (SortedIndex_2/HistoricalIndex_2)")
    o.addOption(null, "segments", true, "Number of segments (default: ncpu)")
    o.addOption(null, "snapshot", true, "Snapshot time")
    o.addOption(null, "threads", true, "Number of threads (default: ncpu)")
  }

  start {
    val metadata = Option(cli.getOptionValue("metadata")) match {
      case Some(path) =>
        val file = new File(path)

        SchemaMetadata.readFile(file) match {
          case Success(meta) => meta
          case Failure(ex) =>
            handler.warn("Cannot read metadata", ex)
            sys.exit(1)
        }

      case None =>
        handler.warn("--metadata is required.")
        sys.exit(1)
    }

    val scopeID = Option(cli.getOptionValue("scope-id")) flatMap {
      _.toLongOption
    } match {
      case Some(id) => ScopeID(id)
      case None =>
        handler.warn("--scope-id is required.")
        sys.exit(1)
    }

    val indexID = Option(cli.getOptionValue("index-id")) flatMap {
      _.toIntOption
    } match {
      case Some(id) => IndexID(id)
      case None     =>
        handler.warn("--index-id is required.")
        sys.exit(1)
    }

    val index = NativeIndex(scopeID, indexID) match {
      case None =>
        handler.warn("--index-id is not a native index.")
        sys.exit(1)
      case Some(idx) => idx
    }

    val snapshotTS = Option(cli.getOptionValue("snapshot")) flatMap { str =>
      try {
        Some(Timestamp.parse(str))
      } catch {
        case ex: DateTimeParseException =>
          handler.warn(s"Bad snapshot time: $str", ex)
          sys.exit(1)
      }
    } getOrElse Timestamp.MaxMicros

    val subsegments = Option(cli.getOptionValue("segments")) flatMap {
      _.toIntOption
    } getOrElse Runtime.getRuntime.availableProcessors()

    val threads = Option(cli.getOptionValue("threads")) flatMap {
      _.toIntOption
    } getOrElse Runtime.getRuntime.availableProcessors()

    val writePrimary = cli.hasOption("primary")
    val writeSecondary = cli.hasOption("secondary")

    if (!writePrimary && !writeSecondary) {
      handler.warn("At least one of --primary or --secondary must be provided.")
      sys.exit(1)
    }

    val versions = openColumnFamily(Tables.Versions.CFName, withSSTables = true)
    val executor = new Executor("Index Builder", threads)
    val counter = new IteratorStatsCounter
    val segments = Segment.All.subSegments(subsegments)
    val mvts = metadata.toMVTMap
    val deadline = 5.minutes // XXX: configure me.

    segments foreach { segment =>
      executor.addWorker { () =>
        val citer = new CassandraIterator(
          counter,
          versions,
          ScanBounds(segment),
          index.selector,
          snapshotTS)

        val iter = new VersionIterator(citer, mvts)

        while (iter.hasNext) {
          val version = iter.next()
          val query = index.indexer.rows(version) foreach { rows =>
            rows foreach { row =>
              val write = SetAdd(row.key, row.value)

              // FIXME: This duplicates
              // StorageEngine.applyMutationForKey().
              val mut = CassandraMutation(
                Cassandra.KeyspaceName,
                write.rowKey.nioBuffer(),
                index1CF = writePrimary,
                index2CF = writeSecondary
              )

              write.mutateAt(mut, version.ts.transactionTS)
              versions.keyspace.apply(mut.cmut, false /* writeCommitLog */)
            }
          }

          Await.result(eval(query, scopeID, snapshotTS, deadline), deadline)
        }
      }
    }

    executor.waitWorkers()
    // One final flush, in case any memtables are still around.
    versions.keyspace.getColumnFamilyStores forEach {
      _.forceBlockingFlush()
    }

    val stats = counter.snapshot()
    val msg = s"Rows: ${stats.rows}\n" +
      s"Cells: ${stats.cells} total, " +
      s"${stats.liveCells} live, " +
      s"${stats.unbornCells} unborn, " +
      s"${stats.deletedCells} deleted\n" +
      s"${stats.bytesRead} bytes read"

    handler.output(msg)

    sys.exit(0)
  }

  // This atrocity exists for the sole purpose of evaluating
  // `Indexer.rows()` without starting up the universe.
  private def eval(
    q: Query[_],
    scopeID: ScopeID,
    snapshotTS: Timestamp,
    deadline: FiniteDuration) = {
    val repo = RepoContext(
      service = null,
      storage = null,
      clock = Clock,
      priorityGroup = PriorityGroup.Default,
      stats = stats,
      limiters = null,
      retryOnContention = false,
      queryTimeout = deadline,
      readTimeout = deadline,
      newReadTimeout = deadline,
      rangeTimeout = deadline,
      writeTimeout = deadline,
      idSource = null,
      cacheContext = null,
      isLocalHealthyQ = Query.True,
      isClusterHealthyQ = Query.True,
      isStorageHealthyQ = Query.True,
      backupReadRatio = 0.0,
      repairTimeout = deadline,
      schemaRetentionDays = 0.days,
      txnSizeLimitBytes = 0,
      txnMaxOCCReads = 0,
      enforceIndexEntriesSizeLimit = false,
      reindexCancelLimit = 0,
      reindexCancelBytesLimit = Long.MaxValue,
      reindexDocsLimit = 0,
      taskReprioritizerLimit = 0
    )

    val ctx =
      new QueryEvalContext(
        repo,
        snapshotTS,
        PermissiveOpsLimiter,
        0, /* docsCacheSize - no I/O, no cache! */
        0, /* ioCacheSize - no I/O, no cache! */
        scopeID,
        new State.Metrics
      )

    ctx.eval(q, deadline.bound, repo.txnSizeLimitBytes)
  }
}
