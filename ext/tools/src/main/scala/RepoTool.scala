package fauna.tools

import fauna.config.CoreConfig
import fauna.exec.FaunaExecutionContext
import fauna.repo.{ CacheContext, RepoContext }
import fauna.repo.cache.{ Cache, SchemaCache }
import fauna.repo.cassandra.CassandraService
import fauna.repo.query.Query
import fauna.repo.service.rateLimits.PermissiveAccountOpsRateLimitsService
import fauna.scheduler.{ ConstantPriorityProvider, PriorityGroup }
import fauna.snowflake.IDSource
import fauna.stats.StatsRecorder
import fauna.storage.Storage
import fauna.storage.Tables.FullSchema
import java.nio.file.{ Path, Paths }
import org.apache.cassandra.db.compaction.CompactionManager
import org.apache.cassandra.db.Keyspace
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.DurationInt

object RepoTool {
  def setupRepoWithoutCompactions(config: CoreConfig, stats: StatsRecorder) = {
    val ffService = new fauna.flags.FileService(Path.of(config.flags_path), stats)
    ffService.start()

    val cassadraInitF = Future {
      CassandraService.initializeAndStart(
        "RepoTool",
        config = config.cassandra,
        encryption = config.encryption,
        stats = stats,
        prioritizer = ConstantPriorityProvider,
        tmpDirectory = Paths.get(config.tempPath),
        txnLogBackupPath = config.txnLogBackupPath,
        storagePath = Paths.get(config.storagePath),
        backupDir = Paths.get(config.snapshotPath),
        mkExecutor = None,
        flagsClient = ffService,
        arrivalsService = PermissiveAccountOpsRateLimitsService
      )
    }(FaunaExecutionContext.Implicits.global)

    Await.result(cassadraInitF, 5.minutes)

    Await.result(CassandraService.instance.initCluster(), 60.seconds)
    val cs = CassandraService.instance

    val caches = CacheContext(
      schema = Cache(
        name = "Schema",
        maxSizeMB = config.cache_schema_size_mb,
        refreshRate = config.cache_schema_ttl_seconds.seconds
      ),
      schema2 = SchemaCache(
        maxSchemaSizeMB = config.cache_schema_size_mb,
        refreshRate = config.cache_schema_ttl_seconds.seconds
      ),
      keys = Cache(
        name = "Keys",
        maxSizeMB = config.cache_key_size_mb,
        refreshRate = config.cache_key_ttl_seconds.seconds
      )
    )

    val storage = Storage(FullSchema)
    storage.init()

    val repo = RepoContext(
      cs,
      storage,
      cs.readClock,
      PriorityGroup.Default,
      stats,
      PermissiveAccountOpsRateLimitsService,
      false, // retry-on-contention
      RepoContext.DefaultQueryTimeout,
      config.network_read_timeout_ms.millis,
      config.storage_new_read_timeout_ms.millis,
      config.network_range_timeout_ms.millis,
      config.network_write_timeout_ms.millis,
      new IDSource(() => cs.workerID), // id source,
      caches,
      isLocalHealthyQ = Query.True,
      isClusterHealthyQ = Query.True,
      isStorageHealthyQ = Query.True,
      config.storage_backup_request_ratio,
      config.repairTimeout,
      config.schema_retention_days.days,
      config.transaction_max_size_bytes,
      config.transaction_max_occ_reads,
      config.index_enforce_entries_max_size,
      config.tasks_reindex_cancel_limit,
      config.reindexCancelBytesSize,
      config.tasks_reindex_docs_limit,
      config.task_reprioritizer_limit,
      config.health_check_timeout_ms.millis,
      config.health_check_healthy_period_ms.millis,
      config.health_check_unhealthy_period_ms.millis,
      internalJWK = config.internalJWK,
      jwkProvider = Some(config.jwkProvider),
      queryMaxConcurrentReads = config.queryMaxConcurrentReads,
      queryEvalStepsPerYield = config.queryEvalStepsPerYield,
      queryAccumulatePageWidth = config.queryAccumulatePageWidth,
      fqlxMaxStackFrames = config.fqlx_max_stack_frames
    )

    caches.setRepo(repo)
    disableCompaction(repo)
    repo
  }

  def disableCompaction(repo: RepoContext) = {
    val ks = Keyspace.open(repo.storage.keyspaceName)

    repo.storage.schema.foreach { cf =>
      val cfs = ks.getColumnFamilyStore(cf.name)
      val strategy = cfs.getCompactionStrategy
      strategy.disable()
    }

    CompactionManager.instance.forceShutdown()
  }
}
