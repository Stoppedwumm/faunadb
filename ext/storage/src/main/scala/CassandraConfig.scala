package fauna.storage

import java.io.{ File, FileWriter }
import java.nio.file.{ AccessDeniedException, Path }
import java.util.{ Arrays, List => JList, Map => JMap }
import org.yaml.snakeyaml.Yaml
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Random

object CassandraConfig {
  def mkRootDir(config: CassandraConfig) = (new File(config.rootPath)).mkdirs

  def writeConfiguration(config: CassandraConfig): Unit = {
    (new File(config.internalConfDirPath)).mkdirs()

    val file = new File(config.internalConfFilePath)

    // Nodes see themselves as seeds
    val seedProvider: JList[JMap[String, Object]] = Seq(Map(
      "class_name" -> "org.apache.cassandra.locator.SimpleSeedProvider",
      "parameters" -> Seq(Map("seeds" -> config.broadcastAddress).asJava).asJava.asInstanceOf[Object]).asJava).asJava

    val dataPath = s"${config.rootPath}/data"
    val commitLogPath = s"${config.rootPath}/commitlog"
    val cachesPath = s"${config.rootPath}/caches"

    // Choose a random commitlog segment size slightly lower than the configured
    // size to stagger out concurrent compactions across nodes servicing a uniform
    // write load
    val jitterFactor = (1.0 - Random.nextDouble() * 0.25)

    Seq(dataPath, commitLogPath, cachesPath) foreach { path =>
      val f = new File(path)
      if (
        (f.exists && !(f.canRead && f.canWrite && f.canExecute)) ||
        (!f.exists && !f.mkdir)
      ) {
          throw new AccessDeniedException(f.getAbsolutePath, null, "Insufficient permissions.")
      }
    }

    val conf = baseConfiguration ++ Map(
      "listen_address" -> config.listenAddress,
      "broadcast_address" -> config.broadcastAddress,
      "auto_bootstrap" -> true,
      "cluster_name" -> s"fauna-${config.cluster}",
      "num_tokens" -> 128, // Legacy number. could just be the default.
      "data_file_directories" -> Arrays.asList(dataPath),
      "commitlog_directory" -> commitLogPath,
      "saved_caches_directory" -> cachesPath,
      "storage_port" -> 7000,
      "ssl_storage_port" -> 7001,
      "seed_provider" -> seedProvider,
      "commitlog_total_space_in_mb" -> (config.commitLogSizeMB max 64),
      // Java is unhappy about random access files greater than ~2gb so limit to 1gb
      "commitlog_segment_size_in_mb" -> ((config.commitLogSizeMB / 4 max 16 min 1024) * jitterFactor).toInt,
      "index_summary_resize_interval_in_minutes" -> config.indexSummaryInterval.toMinutes,
      "index_summary_capacity_in_mb" -> config.indexSummarySizeMB,
      "key_cache_size_in_mb" -> config.keyCacheSizeMB,
      "memtable_heap_space_in_mb" -> (config.memtableSizeMB max 64),
      "memtable_offheap_space_in_mb" -> 0,
      "memtable_cleanup_threshold" -> 0.5,
      "memtable_flush_writers" -> (config.memtableFlushWriters max 1),
      "concurrent_reads" -> config.concurrentReads,
      "concurrent_writes" -> config.concurrentWrites,
      "concurrent_compactors" -> (config.concurrentCompactors max 1),
      "compaction_throughput_mb_per_sec" -> config.compactionThroughputMBPerSec,
      "disk_access_mode" -> config.sstableDiskAccessMode
    )

    val writer = new FileWriter(file)

    (new Yaml()).dump(conf.asJava, writer)
    writer.close()
  }

  private val baseConfiguration: Map[String, Any] = Map(
    "partitioner" -> "org.apache.cassandra.dht.Murmur3Partitioner",
    "hinted_handoff_enabled" -> false,
    "batchlog_replay_throttle_in_kb" -> 0,
    "endpoint_snitch" -> "SimpleSnitch",
    "dynamic_snitch" -> false,
    "inter_dc_tcp_nodelay" -> true,
    "internode_compression" -> "none",
    "start_rpc" -> false,
    "start_native_transport" -> false,
    "rpc_server_type" -> "hsha",
    "rpc_min_threads" -> 1,
    "rpc_max_threads" -> 1,

    "authenticator" -> "AllowAllAuthenticator",
    "authorizer" -> "AllowAllAuthorizer",
    "request_scheduler" -> "org.apache.cassandra.scheduler.NoScheduler",

    // Disable caches.
    "row_cache_size_in_mb" -> 0,
    "counter_cache_size_in_mb" -> 0,
    "file_cache_size_in_mb" -> 0,

    "key_cache_save_period" -> 0,
    "row_cache_save_period" -> 0,
    "counter_cache_save_period" -> 0,

    // Never fail a request because of tombstones
    "tombstone_warn_threshold" -> 20000,
    "tombstone_failure_threshold" -> Int.MaxValue,
    // Create a column index entry every N bytes in the row
    "column_index_size_in_kb" -> 256,

    // Don't let the kernel accumulate big dirty pages that might block
    "trickle_fsync" -> true,
    "trickle_fsync_interval_in_kb" -> 10240,

    // Actually commit to the commitlog
    "commitlog_sync" -> "batch",
    "commitlog_sync_batch_window_in_ms" -> 2,

    "disk_failure_policy" -> "stop")
}

final case class CassandraConfig(
  cluster: String,
  regionGroup: String,
  environment: String,
  rootPath: String,
  busPort: Int,
  busSSLPort: Int,
  busConnectTimeout: FiniteDuration,
  busHandshakeTimeout: FiniteDuration,
  busIdleTimeout: FiniteDuration,
  busKeepaliveInterval: FiniteDuration,
  busKeepaliveTimeout: FiniteDuration,
  connectionsPerHost: Int,
  busMaxMessageSize: Int,
  busMaxPendingMessages: Int,
  broadcastAddress: String,
  listenAddress: String,
  keyCacheSizeMB: Int,
  enableQoS: Boolean,
  processors: Int,
  concurrentReads: Int,
  concurrentTxnReads: Int,
  concurrentWrites: Int,
  concurrentCompactors: Int,
  compactionThroughputMBPerSec: Int,
  streamTimeout: FiniteDuration,
  indexSummaryInterval: FiniteDuration,
  indexSummarySizeMB: Int,
  memtableSizeMB: Int,
  memtableFlushWriters: Int,
  commitLogSizeMB: Int,
  concurrentApplyingTransactions: Int,
  roundTripTime: FiniteDuration,
  syncPeriod: FiniteDuration,
  syncOnShutdown: Boolean,
  backupReadRatio: Double,
  readTimeout: FiniteDuration,
  newReadTimeout: FiniteDuration,
  repairHashDepth: Byte,
  repairSyncParallelism: Int,
  ackDelayPercentile: Double,
  txnLogBackupPath: Option[Path],
  transactionLogBufferSize: Int,
  backupDir: String,
  dualWriteIndexCFs: Boolean,
  index2CFValidationRatio: Double,
  consensusStallRestartPeriod: Duration,
  minWorkerID: Int,
  maxWorkerID: Int,
  neighborReplica: Option[String],
  enableDataTransferDigests: Boolean,
  enableSnapshotTransferDigests: Boolean,
  streamMaxOpenStreams: Int,
  openFileReserve: Double,
  unleveledSSTableLimit: Int,
  transferChunkSize: Int,
  recvMaxBytesPerSecond: Double,
  recvMaxBurstSeconds: Double,
  streamPlanThreads: Int,
  latWaitThreshold: FiniteDuration,
  maxOCCReadsPerSecond: Int,
  occReadsBackoffThreshold: Int,
  minBackupReadDelay: FiniteDuration,
  limiterGossipInterval: FiniteDuration,
  sstableDiskAccessMode: String
) {

  def internalConfDirPath = s"$rootPath/conf"
  def internalConfFilePath = s"$internalConfDirPath/cassandra.yaml"
}
