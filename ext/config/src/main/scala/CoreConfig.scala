package fauna.config

import fauna.codex.json.JSArray
import fauna.logging.LoggingConfig
import fauna.net.bus.MessageBus.{ DefaultKeepaliveInterval, DefaultKeepaliveTimeout }
import fauna.net.security._
import fauna.storage.CassandraConfig
import java.io.File
import java.net.InetAddress
import java.nio.file._
import scala.concurrent.duration._

object CoreConfig {

  private val loader =
    new Loader[CoreConfig](new CoreConfig)

  def load(path: Option[Path], appDefaults: Map[String, Any] = Map.empty) = {
    path match {
      case Some(cfg) => loader.load(cfg, appDefaults)
      case None      => loader.collate(appDefaults)
    }
  }
}

class CoreConfig extends Configuration {

  /**  Setting the @Config will ensure that this config param will be displayed
    *  to the user. Potential modifications:
    *    * Required - The field must be set by the loader
    *    * Deprecated - The field is deprecated, setters and getters won't be generated
    *    * Renamed - The field is renamed, only private getters will be generated.
    */

  @Config(Renamed("dogstatsd_host")) var stats_host: String = null

  // A plaintext root key, used for tests and QA.
  @Config var auth_root_key: String = null

  // A hashed root key, used for non-testing environments.
  // XXX: deprecate once auth_root_key_hashes is used everywhere.
  @Config var auth_root_key_hash: String = null

  // A list of hashed root keys, used for non-testing
  // environments. The list allows for key rotation.
  @Config var auth_root_key_hashes: List[String] = null

  // BCrypt hash of a pre-shared-key to authenticate /export endpoints. The list
  // allows for key rotation
  @Config var export_shared_key_hashes: List[String] = null

  @Config var background_enable_task_execution: Boolean = true
  @Config var background_enable_task_bouncer: Boolean = false
  @Config(Deprecated) var background_enable_garbage_collection: Boolean = false

  @Config(Deprecated) var background_instance_scan_period_seconds: Int =
    2.days.toSeconds.toInt

  @Config(Deprecated) var background_enable_expired_version_scanner: Boolean = false
  @Config(Deprecated) var background_expired_scan_period_seconds: Int =
    2.days.toSeconds.toInt

  @Config var background_enable_sparse_document_scanner: Boolean = false
  @Config var background_document_scan_period_seconds: Int = 2.days.toSeconds.toInt

  @Config var background_enable_full_index_scanner: Boolean = false
  @Config var background_enable_sparse_index_scanner: Boolean = false
  @Config var background_index_scan_period_seconds: Int = 7.days.toSeconds.toInt


  // Controls how often DocGC logs progress information while
  // processing a segment.
  @Config(Deprecated) var background_garbage_collection_log_probability: Double = 0.0

  // Control whether DocGC checks and repairs incorrect versions diffs
  // while processing documents.
  @Config(Deprecated) var background_garbage_collection_diff_repair: Boolean = true

  // Control whether DocGC executes versions history cleanup while
  // processing documents.
  @Config(Deprecated) var background_garbage_collection_history_cleanup: Boolean =
    true

  // Controls the number of attempts DocGC will make to progress
  // before splitting a segment.
  @Config(Deprecated) var background_garbage_collection_attempts: Int = 5

  // Configures the initial and maximum backoff times for background
  // task execution.
  @Config var background_task_exec_sleep_time_seconds: Int = 10
  @Config var background_task_exec_backoff_time_seconds: Int =
    5.minutes.toSeconds.toInt

  // configures the 'interactivity' of background task
  // execution - i.e. how long blocking work is permitted to take
  // before yielding back to execution
  @Config var background_task_exec_yield_time_seconds: Int =
    5.minutes.toSeconds.toInt

  // The minimum backoff time for the task reprioritizer loop.
  @Config var background_task_reprioritize_sleep_time_seconds: Int = 10
  // The max backoff time for the task reprioritizer loop.
  @Config var background_task_reprioritize_backoff_time_seconds: Int =
    5.minutes.toSeconds.toInt

  // Controls the size of batch processed by the task executor in a
  // single iteration. Larger batches require fewer reads of the runQ,
  // but reduce work-stealign opportunities.
  @Config var background_task_exec_batch_size: Int = 32

  // Enables round-robin processing of the runQ, if true. If false,
  // tasks are processed in FIFO order, one batch at a time.
  @Config(Deprecated) var background_task_exec_round_robin: Boolean = true

  // Enables work-stealing on this host, which seeks unexecuted tasks
  // on other hosts and attempts to move them to this host's queue.
  @Config var background_task_stealing_enabled: Boolean = true

  // Controls the number of tasks the task stealer will try to steal
  // every attempt.
  @Config var background_task_steal_size: Int = 32

  // Controls the number of tasks the task stealer will try to steal
  // every unhealthy steal attempt.
  @Config var background_task_unhealthy_steal_size: Int = 1

  @Config var cache_key_size_mb: Int = 8
  @Config var cache_key_ttl_seconds: Int = 60
  @Config var cache_schema_size_mb: Int = 8
  @Config var cache_schema_ttl_seconds: Int = 60

  // Cloud environment description for associating with tracing and
  // stats
  @Config var cloud_account_id: String = null
  @Config var cloud_image_id: String = null
  @Config var cloud_image_name: String = null
  @Config var cloud_image_version: String = null
  @Config var cloud_instance_id: String = null
  @Config var cloud_instance_type: String = null
  @Config var cloud_provider: String = null
  @Config var cloud_region: String = null
  @Config var cloud_zone: String = null

  @Config var cluster_name: String = "fauna"
  @Config var cluster_region_group: String = ""
  @Config var cluster_environment: String = ""
  @Config var cluster_min_worker_id: Int = 0
  @Config var cluster_max_worker_id: Int = 1023

  @Config(Deprecated) var debug_enable_headers: Boolean = false
  @Config(Deprecated) var debug_enable_logs: Boolean = false

  @Config var peer_encryption_cipher_suites: List[String] =
    List("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA")
  @Config var peer_encryption_key_file: String = null
  @Config var peer_encryption_level: String = null
  @Config var peer_encryption_password: String = ""
  @Config var peer_encryption_trust_file: String = null

  @Config var admin_ssl_password: String = ""
  @Config var admin_ssl_key_file: String = null
  @Config var admin_ssl_trust_file: String = null

  @Config var http_ssl_password: String = ""
  @Config var http_ssl_key_file: String = null
  @Config var http_ssl_trust_file: String = null
  @Config var http_max_concurrent_streams: Int = Int.MaxValue

  @Config var log_level: String = "INFO"

  // Also used by src/main/scripts/fauna
  @Config var log_path: String = null
  @Config var log_pattern: String = "%d{ISO8601} [%c{2}] %level %msg%n"
  // When false, suppress writing to query.log. Mostly useful for performance
  // benchmarking.
  @Config var log_queries: Boolean = true
  @Config var log_streams: Boolean = true
  @Config var log_trace: Boolean = false
  @Config var log_rotate_count: Int = 0
  @Config var log_rotate_size_mb: Int = 128
  @Config var log_slow_query_ms: Int = 5000
  @Config var log_limiters: Boolean = false

  @Config var export_path: String = null

  // The maximum length of the x-fauna-tags header, in bytes.
  @Config var tags_max_bytes: Int = 3000
  // The maximum number of pairs in the x-fauna-tags header.
  @Config var tags_max_pairs: Int = 25
  // The maximum length of each key within the x-fauna-tags header, in bytes.
  @Config var tags_key_max_bytes: Int = 40
  // The maximum length of each value within the x-fauna-tags header, in bytes.
  @Config var tags_value_max_bytes: Int = 80

  @Config(Required) var network_broadcast_address: String = null
  // InetAddress.getLoopbackAddress()
  @Config var network_console_address: String = "127.0.0.1"
  @Config var network_console_port: Int = -1
  // InetAddress.anyLocalAddress()
  @Config var network_coordinator_http_address: String = null
  @Config var network_coordinator_http_port: Int = 8443
  @Config(Deprecated) var network_coordinator_http_port_nossl: Int = 8445
  // InetAddress.anyLocalAddress()
  @Config var network_admin_http_address: String = "127.0.0.1"
  @Config var network_admin_http_port: Int = 8444
  @Config var network_http_keepalive_timeout_ms: Int = 5000
  @Config var network_http_max_chunk_size: Int = 16 * 1024
  @Config var network_http_max_header_size: Int = 8 * 1024
  @Config var network_http_max_initial_line_length: Int = 16 * 1024
  @Config var network_http_read_timeout_ms: Int = 60000
  @Config var network_host_id: String = null
  @Config var network_listen_address: String = null
  @Config var network_read_timeout_ms: Int = 5000
  @Config var network_range_timeout_ms: Int = 30000
  @Config var network_round_trip_time_ms: Int = 300
  @Config var network_peer_port: Int = 7500
  @Config var network_peer_secure_port: Int = 7501
  @Config var network_connections_per_host: Int = 1
  @Config var network_stream_timeout_ms: Int = 2.hours.toMillis.toInt
  @Config var network_max_message_bytes: Int = 4096
  @Config var network_max_pending_messages: Int = 1024
  @Config var network_write_timeout_ms: Int = 30000
  @Config var network_connect_timeout_ms: Int = 5000
  @Config var network_handshake_timeout_ms: Int = 5000
  @Config var network_idle_timeout_ms: Int = 60000
  @Config var network_keepalive_interval_ms: Int =
    DefaultKeepaliveInterval.toMillis.toInt
  @Config var network_keepalive_timeout_ms: Int =
    DefaultKeepaliveTimeout.toMillis.toInt

  // Limits the throughput of file transfers. Unintuitively, this is
  // the throughput _net_ of framing (see
  // storage_transfer_chunk_size_bytes) and TCP overhead, so this
  // value is not calculated directly using the NIC throughput, i.e. a
  // host with a 10Gbps NIC should not use a limit calculated directly
  // from 10Gbps, as that would be much too high. The limit value is,
  // however, proportional to the NIC throughput, so a
  // higher-throughput NIC should have a higher recv limit.
  //
  // The default value of 1.0 was arrived at by empirically testing
  // file transfers from a host with a "big" NIC (50Gbps) to hosts
  // with "small" NICs (< 10Gbps) while generating traffic against the
  // "small" hosts. This value was adjusted downward until other flows
  // on the NIC were not contending against the file transfer.
  @Config var network_recv_max_gbit_per_second: Double = 1.0

  // The number of seconds of burst (i.e. unlimited) throughput a file
  // transfer is permitted after a period of inactivity. A non-zero
  // value for this setting alters the effect of the limiter to apply
  // an _average_ limit over the burst period by spilling permits from
  // idle periods forward into periods of transfer activity. A zero
  // value for this setting is the default to run apply the limiter as
  // a _max_ limit: i.e. there are no periods of unlimited throughput.
  @Config var network_recv_max_burst_seconds: Double = 0.0

  @Config var runtime_processors: Int = Runtime.getRuntime.availableProcessors

  @Config var runtime_heap_mb: Int =
    (Runtime.getRuntime.maxMemory / 1024 / 1024).toInt

  @Config var schema_retention_days: Int = 1

  @Config var stats_binary_protocol: Boolean = false
  @Config var stats_csv_path: String = null
  @Config var stats_poll_seconds: Int = 10
  @Config var stats_timings_per_second: Int = 100

  @Config var dogstatsd_host: String = null
  @Config var dogstatsd_port: Int = 0

  // Threshold number of documents scanned before falling back to a table-scan.
  @Config var tasks_reindex_cancel_limit: Int = 2.5e5.toInt

  // Threshold number of bytes read before falling back to a table-scan.
  @Config var tasks_reindex_cancel_size_limit_mb: Int = Int.MaxValue // no limit.

  // Limits the number of documents each ReindexDocs task will process.
  @Config var tasks_reindex_docs_limit: Int = 32_000 // ~2.5e5 / 8

  // Limits the number of tasks the TaskReprioritizer will count to. Any more
  // tasks will all have this number negated as the prioritiy.
  @Config var task_reprioritizer_limit: Int = 100

  @Config var trace_probability: Double = 0.0
  @Config var trace_probability_task_executor: Double = Double.NaN
  @Config var trace_buffer_size: Int = 1024

  // Allow upstream clients to dictate trace sampling if they provide this
  // value in the x-trace-secret header.
  @Config var trace_secret: String = null

  // Accept HTTP tracecontext headers from clients.
  // WARNING: this is an abuse vector
  @Config(Deprecated) var trace_accept_client: Boolean = false

  // Selects an Application Performance Monitoring (APM) system to
  // receive trace information. Currently, only "datadog" is supported.
  @Config var trace_apm: String = ""

  // DataDog APM
  @Config var datadog_host: String = "localhost"
  @Config var datadog_port: Int = 8126
  @Config var datadog_version: String = "v0.3"
  @Config var datadog_endpoint: String = "traces"
  @Config var datadog_connect_timeout_ms: Int = 10
  @Config var datadog_response_timeout_ms: Int = 200
  @Config var datadog_connections: Int = 10

  // The path to the feature flags JSON file.
  @Config var flags_path: String = "/etc/feature-flag-periodic.d/feature-flags.json"

  // Enable FQL2 features. Can be overridden per-account via Feature Flags.
  @Config(Deprecated) var fql2_enable_endpoint: Boolean = false
  @Config(Deprecated) var fql2_enable_schema: Boolean = true

  // v10 limits.
  @Config var fqlx_max_stack_frames: Int = 2048

  @Config var storage_commit_log_size_mb: Int = -1
  @Config var storage_compaction_throughput_mb: Int = 0

  @Config var storage_enable_qos: Boolean = false

  // We need enough threads per drive in order to allow for the OS and
  // drives themselves to reorder parallel IO ops. 128 is enough for
  // 16 threads per drive with 8 drives. More drives may require more
  // threads.
  // FIXME: scale default with # of drives.
  @Config var storage_concurrent_reads: Int = 128
  @Config var storage_concurrent_txn_reads: Int = 16

  // FIXME: writes value is totally off. the writeEC threadpool and
  // allowed concurrent transactions should be decoupled
  @Config var storage_concurrent_writes: Int = 256

  // The target ratio of backup requests to row requests
  @Config var storage_backup_request_ratio: Double = 0.1

  // The minimum amount of time to delay before sending a backup read
  // request.
  @Config var storage_backup_min_delay_millis: Int = 10

  @Config var storage_concurrent_compactors: Int = -1
  @Config var storage_data_path: String = null
  @Config var storage_index_summary_interval_mins: Int = 60
  @Config var storage_index_summary_size_mb: Int = -1
  @Config var storage_key_cache_size_mb: Int = -1
  @Config(Deprecated) var storage_should_migrate_key_cache: Boolean = true
  @Config var storage_memtable_flush_writers: Int = -1
  @Config var storage_memtable_size_mb: Int = 128

  // If per-key LAT wait exceeds this threshold, a warning will be
  // logged.
  @Config var storage_lat_wait_threshold_ms: Int = 1000

  // The maximum allowed timeout for new storage reads.
  @Config var storage_new_read_timeout_ms: Int = 600000

  @Config var storage_snapshot_path: String = null
  @Config var storage_sync_period_ms: Int = 2.minutes.toMillis.toInt
  @Config var storage_sync_on_shutdown: Boolean = true
  @Config var storage_temp_path: String = null
  @Config var storage_transfer_chunk_size_bytes: Int = 32768
  @Config var storage_enable_data_transfer_digests: Boolean = true
  @Config var storage_enable_snapshot_transfer_digests: Boolean = true

  @Config var storage_concurrent_applying_transactions: Int = 1024

  // A percentage of the maximum open file descriptor ulimit.
  // If the number of open file exceeds this percentage, file
  // transfers will be delayed until the number of open files falls
  // back below this percentage.
  @Config var storage_open_file_reserve_percent: Double = 1.0

  // A limit on the number of unleveled sstables, above which file
  // transfers will be delayed until the number of unleveled sstables
  // fall back below this limit.
  @Config var storage_unleveled_sstable_limit: Int = Int.MaxValue

  // Cassandra config that controls how mmap is used.
  // See
  // https://support.datastax.com/s/article/FAQ-Use-of-disk-access-mode-in-DSE-51-and-earlier
  @Config var sstableDiskAccessMode: String = "auto"

  @Config var storage_dual_write_index_cfs: Boolean = false
  @Config var index2cf_validation_ratio: Double = 0.0
  @Config var index2cf_disable_extended_gc_grace: Boolean = false

  @Config var storage_load_snapshot_threads: Int = 1
  @Config var storage_load_snapshot_timeout_seconds: Int = 4.hours.toSeconds.toInt

  // When constructing a stream plan on the coordinator of a
  // load-snapshot, computing each SSTable's stream plan may be
  // parallelized with up to the given number of threads.
  @Config var storage_transfer_stream_plan_threads: Int = 1

  @Config(Deprecated) var accelerate_indexes: Boolean = true

  @Config var shutdown_grace_period_seconds: Int = 30

  // a larger value for max hash depth will reduce sync transfer
  // sizes, but will use more memory to store the hash tree
  @Config var repair_max_hash_depth: Int = 16
  @Config var repair_timeout_mins: Int = 10

  // a larger value for sync parallelism will transfer more data
  // during the sync phase of repair, requiring more space in
  // storage_temp_path, but will require fewer streams.
  @Config var repair_sync_parallelism: Int = 16

  @Config var transaction_ack_delay_percentile: Double = 99

  // 500KB per 10ms (or ~50MB/sec) per log segment
  @Config var transaction_log_buffer_size_kb: Int = 512
  // Enables backing up of the raft transaction log files
  @Config var transaction_log_backup: Boolean = false
  // Where to backup the transaction log files
  @Config var transaction_log_backup_path: String = null

  @Config var transaction_max_size_bytes: Int = 16 * 1024 * 1024
  @Config var transaction_max_occ_reads: Int = Int.MaxValue

  /** The maximum allowed rate of OCC reads per second per log node. */
  @Config var transaction_max_occ_reads_per_second: Int = Int.MaxValue

  /** Threshold from which txns will be dropped when the OCCs/s limit is exceeded. */
  @Config var transaction_occ_reads_backoff_threshold: Int = Int.MaxValue

  // Trust file used to download JWKS public keys.
  @Config var jwks_ssl_trust_file: String = null

  @Config var jwks_max_cache_size_mb: Int = 16
  @Config var jwks_cache_refresh_rate_seconds: Int = 5.minutes.toSeconds.toInt
  @Config var jwks_cache_expire_rate_seconds: Int = 1.hour.toSeconds.toInt
  @Config var jwks_download_rate_millis: Int = 500.millis.toMillis.toInt

  @Config var jwk_encryption_key_store: String = null
  @Config var jwk_encryption_password: String = ""
  @Config var jwk_encryption_kid: String = ""
  @Config var jwk_encryption_alg: String = "RS256"

  @Config var index_enforce_entries_max_size: Boolean = true

  @Config var stream_max_open_streams: Int = 50_000

  /** Currently streams are created and started with
    * 2 separate requests.  To account for the delay
    * in the query that created the stream and the
    * start time, we replay stream events between
    * when it was created and started.  This could
    * also happen in the scenario of restarting a stream.
    * We use this config to limit how many events we will
    * replay, failing with an error of the number of historical
    * events exceeds this number.
    */
  @Config var stream_event_replay_limit: Int = 128
  @Config(Deprecated) var stream_idle_key_notification_enabled: Boolean = false

  @Config var query_min_compute_ops: Int = 1
  @Config var query_compute_overhead_ms: Int = 0
  @Config var query_read_overhead_ms: Int = 1
  @Config var query_write_overhead_ms: Int = 1
  // Should be configured per cluster based on the average apply latency.
  @Config var query_txn_overhead_ms: Int = 100
  @Config var query_max_per_query_parallelism: Int = 1024
  @Config var query_max_query_width: Int = 100_000
  @Config var query_width_log_ratio: Double = 0.90
  @Config var query_max_global_query_width: Int = 100_000_000
  @Config var query_global_width_log_ratio: Double = 0.90
  @Config var query_max_concurrent_reads: Int = 1024
  // This results in query eval yielding about once every 20-30ms, based on
  // cursory test observation.
  // TODO: It might be worth tuning this based on a larger variety of
  // compute-heavy workloads.
  @Config var query_eval_steps_per_yield: Int = 1024
  @Config var query_accumulate_page_width: Int = 1024

  // Max Rate Limits
  @Config var query_ops_rate_limits_enabled: Boolean = false
  @Config var query_ops_rate_limits_burst_seconds: Double = 2.0
  @Config var query_ops_rate_limits_cache_max_size: Int = 2000
  @Config var query_ops_rate_limits_cache_expire_after_seconds: Int =
    1.hour.toSeconds.toInt
  @Config var query_ops_rate_limits_gossip_interval_ms: Long = 1.second.toMillis
  @Config var query_max_read_ops_per_second: Double = 100_000
  @Config var query_max_write_ops_per_second: Double = 25_000
  @Config var query_max_compute_ops_per_second: Double = 100_000
  @Config var query_max_streams_per_second: Int = 10_000

  @Config var consensus_stall_restart_period_seconds: Int = 30
  @Config var consensus_stall_restart_period_enable: Boolean = true

  // ATTENTION: !!! DO NOT SET THIS INTO DATA+LOG REPLICAS !!!
  //
  // This configuration is meant for compute-only replicas. Setting a neighbor
  // replica to a data+log replica will cause its transaction coordinator to increase
  // its set of local writers, which results slower transaction completion. See
  // fauna.tx.transaction.Coordinator.Applies for details.
  //
  // FIXME: This configuration has been introduced to unblock the release of
  // compute-only replicas to production. We should move this
  // configuration to the admin tools for operational simplicity.
  @Config var coordinator_neighbor_replica: String = null

  // Forbid non-root users from importing or altering keys.
  // Non-root users can create and delete keys with POST and DELETE requests, and
  // update
  // the custom metadata on keys using the Update query function, but PUT and PATCH
  // requests to alter keys will be rejected.
  // This prevents users from breaking region group routing by having duplicate keys.
  @Config var key_forbid_user_import: Boolean = true

  @Config var health_check_timeout_ms: Int = 1.second.toMillis.toInt
  @Config var health_check_healthy_period_ms: Int = 20.seconds.toMillis.toInt
  @Config var health_check_unhealthy_period_ms: Int = 10.seconds.toMillis.toInt

  def asDuration(time: Long, unit: TimeUnit): Duration =
    time match {
      case Long.MaxValue => Duration.Inf
      case Long.MinValue => Duration.MinusInf
      case time          => Duration(time, unit)
    }

  lazy val exportSharedKeyHashes = Option(export_shared_key_hashes).getOrElse(Nil)

  def logPath = Option(log_path) getOrElse "/var/log/faunadb"

  def storagePath = Option(storage_data_path) getOrElse "/var/lib/faunadb"

  def snapshotPath =
    Option(
      storage_snapshot_path) getOrElse storagePath + File.separator + "snapshots"

  def tempPath =
    Option(storage_temp_path) getOrElse storagePath + File.separator + "tmp"

  def exportPath = Option(export_path) map { Paths.get(_) }

  def broadcastAddress =
    Option(
      network_broadcast_address) getOrElse InetAddress.getLocalHost.getHostAddress

  def listenAddress = Option(network_listen_address) getOrElse broadcastAddress

  def networkHostID = Option(network_host_id) getOrElse broadcastAddress

  def networkCoordinatorHttpAddress =
    Option(network_coordinator_http_address) getOrElse listenAddress

  def shutdownGracePeriod = asDuration(shutdown_grace_period_seconds, SECONDS)

  def transactionLogBufferSize: Int =
    1024 * ((transaction_log_buffer_size_kb max 10) min 100000)

  def txnLogBackupPath: Option[Path] = {
    if (transaction_log_backup) {
      val backupPath = Option(
        transaction_log_backup_path) getOrElse storagePath + File.separator + "txnbackup"
      Some(Paths.get(backupPath))
    } else {
      None
    }
  }

  def loadSnapshotThreads =
    storage_load_snapshot_threads

  def loadSnapshotTimeout =
    storage_load_snapshot_timeout_seconds.seconds

  def statsPollSeconds = stats_poll_seconds.seconds

  def indexScanLoopTime =
    background_index_scan_period_seconds.seconds

  def taskExecBackoffTime =
    background_task_exec_backoff_time_seconds.seconds

  def enableTaskBouncer =
    background_enable_task_bouncer

  def enableSparseDocumentScanner =
    background_enable_sparse_document_scanner

  def enableFullIndexScanner =
    background_enable_full_index_scanner

  def enableSparseIndexScanner =
    background_enable_sparse_index_scanner

  def documentScanLoopTime =
    background_document_scan_period_seconds.seconds

  def cacheSchemaTTL =
    cache_schema_ttl_seconds.seconds

  def logQueries =
    log_queries

  def logTraces =
    log_trace

  def commitLogSize =
    if (storage_commit_log_size_mb > 0) {
      storage_commit_log_size_mb
    } else {
      runtime_heap_mb / 7
    }

  def concurrentCompactors =
    if (storage_concurrent_compactors > 0) {
      storage_concurrent_compactors
    } else {
      (runtime_processors / 2) max 1
    }

  def indexSummaryInterval =
    storage_index_summary_interval_mins.minutes

  def indexSummarySize =
    if (storage_index_summary_size_mb > 0) {
      storage_index_summary_size_mb
    } else {
      // C* default
      (runtime_heap_mb * 0.05 / 1024 / 1024).toInt max 1
    }

  def reindexCancelBytesSize =
    tasks_reindex_cancel_size_limit_mb.toLong * 1024 * 1024

  def keyCacheSize =
    if (storage_key_cache_size_mb > 0) {
      storage_key_cache_size_mb
    } else {
      runtime_heap_mb / 8
    }

  def memtableSize =
    if (storage_memtable_size_mb > 0) {
      storage_memtable_size_mb
    } else {
      runtime_heap_mb / 4
    }

  def memtableFlushers =
    if (storage_memtable_flush_writers > 0) {
      storage_memtable_flush_writers
    } else {
      (runtime_processors / 8) max 1
    }

  def traceSecret =
    Option(trace_secret)

  def traceProbability =
    trace_probability max 0.0

  def traceProbabilityTaskExecutor =
    if (trace_probability_task_executor.isNaN) {
      traceProbability
    } else {
      trace_probability_task_executor max 0.0
    }

  def recvMaxBytesPerSecond =
    // Bytes are easier for the machine, Gibps is easier for the
    // humans.
    network_recv_max_gbit_per_second / 8 * 1024 * 1024 * 1024

  def cassandra =
    CassandraConfig(
      cluster = cluster_name,
      regionGroup = cluster_region_group,
      environment = cluster_environment,
      rootPath = storagePath,
      busPort = network_peer_port,
      busSSLPort = network_peer_secure_port,
      busConnectTimeout = network_connect_timeout_ms.millis,
      busHandshakeTimeout = network_handshake_timeout_ms.millis,
      busIdleTimeout = network_idle_timeout_ms.millis,
      busKeepaliveInterval = network_keepalive_interval_ms.millis,
      busKeepaliveTimeout = network_keepalive_timeout_ms.millis,
      connectionsPerHost = network_connections_per_host,
      busMaxMessageSize = network_max_message_bytes,
      busMaxPendingMessages = network_max_pending_messages,
      broadcastAddress = broadcastAddress,
      listenAddress = listenAddress,
      keyCacheSizeMB = keyCacheSize,
      enableQoS = storage_enable_qos,
      processors = runtime_processors,
      concurrentReads = storage_concurrent_reads,
      concurrentTxnReads = storage_concurrent_txn_reads,
      concurrentWrites = storage_concurrent_writes,
      concurrentCompactors = concurrentCompactors,
      compactionThroughputMBPerSec = storage_compaction_throughput_mb,
      streamTimeout = network_stream_timeout_ms.millis,
      indexSummaryInterval = indexSummaryInterval,
      indexSummarySizeMB = indexSummarySize,
      memtableSizeMB = memtableSize,
      memtableFlushWriters = memtableFlushers,
      commitLogSizeMB = commitLogSize,
      concurrentApplyingTransactions = storage_concurrent_applying_transactions,
      roundTripTime = network_round_trip_time_ms.millis,
      syncPeriod = storage_sync_period_ms.millis,
      syncOnShutdown = storage_sync_on_shutdown,
      backupReadRatio = storage_backup_request_ratio,
      readTimeout = network_read_timeout_ms.millis,
      newReadTimeout = storage_new_read_timeout_ms.millis,
      repairHashDepth = repair_max_hash_depth.toByte,
      repairSyncParallelism = repair_sync_parallelism,
      ackDelayPercentile = transaction_ack_delay_percentile,
      txnLogBackupPath = txnLogBackupPath,
      transactionLogBufferSize = transactionLogBufferSize,
      backupDir = snapshotPath,
      dualWriteIndexCFs = storage_dual_write_index_cfs,
      index2CFValidationRatio = index2cf_validation_ratio,
      consensusStallRestartPeriod = if (consensus_stall_restart_period_enable) {
        consensus_stall_restart_period_seconds.seconds
      } else { Duration.Inf },
      minWorkerID = cluster_min_worker_id,
      maxWorkerID = cluster_max_worker_id,
      neighborReplica = Option(coordinator_neighbor_replica),
      enableDataTransferDigests = storage_enable_data_transfer_digests,
      enableSnapshotTransferDigests = storage_enable_snapshot_transfer_digests,
      streamMaxOpenStreams = stream_max_open_streams,
      openFileReserve = storage_open_file_reserve_percent,
      unleveledSSTableLimit = storage_unleveled_sstable_limit,
      transferChunkSize = storage_transfer_chunk_size_bytes,
      recvMaxBytesPerSecond = recvMaxBytesPerSecond,
      recvMaxBurstSeconds = network_recv_max_burst_seconds,
      streamPlanThreads = storage_transfer_stream_plan_threads,
      latWaitThreshold = storage_lat_wait_threshold_ms.millis,
      maxOCCReadsPerSecond = transaction_max_occ_reads_per_second,
      occReadsBackoffThreshold = transaction_occ_reads_backoff_threshold,
      minBackupReadDelay = storage_backup_min_delay_millis.millis,
      limiterGossipInterval = query_ops_rate_limits_gossip_interval_ms.millis,
      sstableDiskAccessMode = sstableDiskAccessMode
    )

  def logging =
    new LoggingConfig(
      log_level,
      logPath,
      log_pattern,
      log_rotate_count,
      log_rotate_size_mb,
      log_slow_query_ms
    )

  def jkwsSSL: Option[SSLConfig] =
    Option(jwks_ssl_trust_file) map { file =>
      SSL(TrustSource(Paths.get(file)))
    }

  def httpSSL: SSLConfig =
    Option(http_ssl_key_file) match {
      case Some(keyFile) =>
        val pass = Password(http_ssl_password)
        val key = KeySource(Paths.get(keyFile), pass)

        val trust = Option(http_ssl_trust_file) map { f =>
          TrustSource(Paths.get(f))
        }

        SSLConfig(ServerSSL(key, trust, false), None)

      case None =>
        NoSSL
    }

  def adminSSL: SSLConfig =
    Option(admin_ssl_key_file) match {
      case Some(keyFile) =>
        val pass = Password(admin_ssl_password)
        val key = KeySource(Paths.get(keyFile), pass)

        val trust = Option(admin_ssl_trust_file) map { f =>
          TrustSource(Paths.get(f))
        }

        SSLConfig(ServerSSL(key, trust, true), ClientSSL(Some(key), trust, true))

      case None =>
        NoSSL
    }

  def encryption: Encryption.Level = {
    val ciphers = peer_encryption_cipher_suites
    val password = Password(peer_encryption_password)

    val kfOpt = Option(peer_encryption_key_file)
    val keySource = kfOpt match {
      case Some(kf) => Some(KeySource(Paths.get(kf), password))
      case _        => None
    }

    val tfOpt = Option(peer_encryption_trust_file)
    val trustSource = tfOpt match {
      case Some(tf) => Some(TrustSource(Paths.get(tf)))
      case _        => None
    }

    val levelOpt = Option(peer_encryption_level)

    (levelOpt, keySource) match {
      // No encryption configured
      case (Some("none"), _) =>
        Encryption.Cleartext

      // Use DC level encryption if we have a keysource but no level
      case (None, Some(_)) =>
        Encryption.Replica(keySource, trustSource, ciphers)

      // Ensure the encryption level is valid
      case (Some(l), _) if l != "dc" && l != "all" =>
        throw new IllegalArgumentException(
          s"Invalid encryption level $l. Must be one of: none, dc, all."
        )

      // Ensure a keysource exists given the encryption level
      case (Some(l), None) =>
        throw new IllegalArgumentException(
          s"Must provide an encryption key for level: $l"
        )

      // DC level encryption
      case (Some("dc"), _) =>
        Encryption.Replica(keySource, trustSource, ciphers)

      // Encrypt all traffic
      case (Some("all"), _) =>
        Encryption.All(keySource, trustSource, ciphers)

      case (Some(level), _) =>
        throw new IllegalArgumentException(s"Unknown level $level")

      // No encryption configured
      case (None, None) =>
        Encryption.Default
    }
  }

  def httpReadTimeout: FiniteDuration =
    network_http_read_timeout_ms.millis

  def httpKeepAliveTimeout: FiniteDuration =
    network_http_keepalive_timeout_ms.millis

  def internalJWK: Option[(JWK, String)] =
    Option(jwk_encryption_key_store) flatMap { keyFile =>
      val pw = Password(jwk_encryption_password)
      val pem = PEMFile(keyFile)

      pem.keyPairs(pw).headOption map { kp =>
        (JWK.rsa(jwk_encryption_kid, kp), jwk_encryption_alg)
      } orElse {
        val privateKeys = pem.privateKeys(pw)
        val publicKeys = pem.trustedKeys(pw)

        Option.when(privateKeys.lengthIs == 1 && publicKeys.lengthIs == 1) {
          (
            JWK.rsa(jwk_encryption_kid, publicKeys.head, privateKeys.head),
            jwk_encryption_alg)
        }
      }
    }

  val jwkProvider: JWKUrlProvider =
    JWKUrlProvider(
      JWKUrlProvider.safeAccessProviders,
      jkwsSSL,
      jwks_max_cache_size_mb,
      jwks_cache_expire_rate_seconds.seconds,
      jwks_cache_refresh_rate_seconds.seconds,
      jwks_download_rate_millis.millis
    )

  // This is a bit subtle: the repair timeout must be
  // less than the task timeout to prevent a thundering
  // herd. However, if the repair timeout is equal to
  // the task timeout, task execution will not update
  // this task's state, prefering instead to retry the
  // task's current state. To ensure that the segment
  // split technique works as intended, the repair
  // timeout must be less than the task timeout. 30
  // seconds is arbitrary.
  def repairTimeout: FiniteDuration = {
    val limit = background_task_exec_backoff_time_seconds.seconds - 30.seconds
    repair_timeout_mins.minutes min limit
  }

  def computeOverhead = query_compute_overhead_ms.millis
  def readOverhead = query_read_overhead_ms.millis
  def writeOverhead = query_write_overhead_ms.millis
  def txnOverhead = query_txn_overhead_ms.millis
  def maxPerQueryParallelism: Int = query_max_per_query_parallelism
  def queryMaxConcurrentReads = query_max_concurrent_reads
  def queryMaxWidth: Int = query_max_query_width
  def queryWidthLogRatio: Double = query_width_log_ratio
  def queryMaxGlobalWidth: Int = query_max_global_query_width
  def queryGlobalWidthLogRatio: Double = query_global_width_log_ratio
  def queryEvalStepsPerYield: Int = query_eval_steps_per_yield
  def queryAccumulatePageWidth: Short = {
    require(query_accumulate_page_width <= Short.MaxValue)
    require(query_accumulate_page_width > 0)
    query_accumulate_page_width.toShort
  }

  def queryOpsRateLimitsEnabled: Boolean = query_ops_rate_limits_enabled
  def opsLimitBurstSeconds: Double = query_ops_rate_limits_burst_seconds
  def opsLimitCacheMaxSize: Int = query_ops_rate_limits_cache_max_size
  def opsLimitCacheExpireAfter: FiniteDuration =
    query_ops_rate_limits_cache_expire_after_seconds.seconds
  def maxReadOpsPerSecond: Double = query_max_read_ops_per_second
  def maxWriteOpsPerSecond: Double = query_max_write_ops_per_second
  def maxComputeOpsPerSecond: Double = query_max_compute_ops_per_second
  def maxStreamsPerSecond: Int = query_max_streams_per_second

  def toJSON: JSArray =
    JSArray((fields.values map { _.toJSON } toSeq): _*)
}
