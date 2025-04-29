package fauna.flags

import fauna.atoms.{ AccountID, HostID }
import scala.concurrent.duration._

/** A Feature is a capability offered by the database whose state is
  * controlled by Properties and Flags, dictated by the Feature Flag
  * Service.
  */
abstract class Feature[ID, +V <: Value] {

  val key: String

  /** If this feature is not present in a set of flags, this value is
    * used.
    *
    * NB: Use `AccountFlags.forRoot.get()` instead, if you need the default value
    * of this flag.
    */
  private[flags] def default: V

  /** This is a type-safe convenience method for `default.value`.
    */
  private[flags] def defaultValue[T](implicit ev: V#T <:< T): T =
    ev(default.value)

  override def toString = s"""Feature($key, default = $defaultValue)"""
}

/** Permits an account to execute queries via the HTTP API when true,
  * denies execution when false.
  */
object RunQueries extends Feature[AccountID, BooleanValue] {
  val key = "run_queries"
  val default = true
}

/** Permits query execution for a maximum number of seconds,
  * irrespective of whether a higher per-query timeout is provided.
  *
  * NOTE: The default is an upper bound on this flag's value. See
  * CoreApplication.MaximumQueryTimeout.
  */
object MaxQueryTimeSeconds extends Feature[AccountID, LongValue] {
  val key = "max_query_time_secs"
  val default = 10.minutes.toSeconds
}

/** Permits an account to create and execute async tasks when true,
  * denies creation and execution when false.
  */
object RunTasks extends Feature[AccountID, BooleanValue] {
  val key = "run_tasks"
  val default = true
}

/** Permits processing a percentage of queries for an account.
  *
  * The value of this feature should be in the range [0.0, 1.0].
  */
object QueryLimit extends Feature[AccountID, DoubleValue] {
  val key = "query_limit"
  val default = 1.0
}

/** Enables inline index consistency detection on an account.
  */
object CheckIndexConsistency extends Feature[AccountID, BooleanValue] {
  val key = "check_index_consistency"
  val default = true
}

/** The number of async index builds a single account may concurrently create.
  */
object MaxConcurrentBuilds extends Feature[AccountID, LongValue] {
  val key = "max_concurrent_builds"
  val default = 500
}

/** Sets the percentage of ping write requests that have traces.
  *
  * The value of this feature should be in the range [0.0, 1.0].
  */
object PingTracePercentage extends Feature[HostID, DoubleValue] {
  val key = "enable_ping_traces"
  val default = 0.0
}

/** Permits a host to re-prioritize index builds.
  */
object EnableIndexBuildPriorities extends Feature[HostID, BooleanValue] {
  val key = "enable_index_build_priorities"
  val default = false
}

/** Feature flag to enable only considering compute hosts when looking for nodes to steal tasks from.
  * This is the desired behavior, adding this feature flag temporarily to test out the new code path.
  */
object EnableComputeOnlyTaskStealing extends Feature[HostID, BooleanValue] {
  val key = "enable_compute_only_task_stealing"
  val default = false
}

/** Limits the maximum number of OCC reads allowed within a single
  * transaction, yielding "transaction too large" when the limit is
  * exceeded.
  */
object ReadsPerTransaction extends Feature[HostID, LongValue] {
  val key = "reads_per_transaction"
  val default = Long.MaxValue
}

/** Enables filling the MVT cache from ExpiredVersionTask to collect
  * expired versions during compaction.
  */
object EnableMVTCache extends Feature[HostID, BooleanValue] {
  val key = "enable_mvt_cache"
  val default = false
}

/** Enables filling the MVT cache from DocHistory read ops to collect
  * expired versions during compaction.
  */
object EnableMVTReadHints extends Feature[HostID, BooleanValue] {
  val key = "enable_mvt_read_hints"
  val default = true
}

object EnableSparseDocumentScanner extends Feature[HostID, BooleanValue] {
  val key = "enable_sparse_document_scanner"
  val default = true
}

object EnableSparseHistoricalIndexScanner extends Feature[HostID, BooleanValue] {
  val key = "enable_sparse_historical_scanner"
  val default = true
}

object EnableSparseSortedIndexScanner extends Feature[HostID, BooleanValue] {
  val key = "enable_sparse_sorted_scanner"
  val default = true
}

object EnableFullHistoricalIndexScanner extends Feature[HostID, BooleanValue] {
  val key = "enable_full_historical_scanner"
  val default = false
}

object EnableFullSortedIndexScanner extends Feature[HostID, BooleanValue] {
  val key = "enable_full_sorted_scanner"
  val default = false
}

object EnableRingAvailabilityCheck extends Feature[HostID, BooleanValue] {
  val key = "enable_ring_availability_check"
  val default = true
}

object EnableRingAvailabilityCheckPeriodMinutes extends Feature[HostID, LongValue] {
  val key = "enable_ring_availability_check_period_minutes"
  val default = 1
}

object EnableRateLimits extends Feature[HostID, BooleanValue] {
  val key = "rate_limits_enabled"
  val default = true
}

class RateLimitsWrites(val default: DoubleValue)
    extends Feature[AccountID, DoubleValue] {
  val key = "rate_limits_writes_per_second"
}

class RateLimitsReads(val default: DoubleValue)
    extends Feature[AccountID, DoubleValue] {
  val key = "rate_limits_reads_per_second"
}

class RateLimitsCompute(val default: DoubleValue)
    extends Feature[AccountID, DoubleValue] {
  val key = "rate_limits_compute_per_second"
}

class RateLimitsStreams(val default: LongValue)
    extends Feature[AccountID, LongValue] {
  val key = "rate_limits_streams_per_second"
}

object DebugRateLimits extends Feature[AccountID, BooleanValue] {
  val key = "rate_limits_debug_logging"
  val default = false
}

/** The size of IO cache for the per-query `ReadCache`. */
object PerQueryIOCacheSize extends Feature[HostID, LongValue] {
  val key = "per_query_io_cache_size"
  val default = Int.MaxValue
}

/** The size of Docs cache for the per-query `ReadCache`. */
object PerQueryDocsCacheSize extends Feature[HostID, LongValue] {
  val key = "per_query_docs_cache_size"
  val default = Int.MaxValue
}

/** Permits an account to execute queries via the HTTP API when true,
  * denies execution when false.
  */
object AllowV4Queries extends Feature[AccountID, BooleanValue] {
  val key = "allow_v4_queries"
  val default = true
}

/** If true, allows calling the FQL.evalV4() function in v10 */
object EvalV4FromV10 extends Feature[AccountID, BooleanValue] {
  val key = "eval_v4_from_v10"
  val default = false
}
