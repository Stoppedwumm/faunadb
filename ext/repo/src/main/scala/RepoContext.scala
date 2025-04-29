package fauna.repo

import fauna.atoms._
import fauna.exec._
import fauna.flags.{
  AccountFlags,
  AccountProperties,
  Feature,
  HostFlags,
  Value => FFValue
}
import fauna.lang.{ ConsoleControl, TimeBound, Timestamp }
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.logging._
import fauna.net.security.{ JWK, JWKProvider }
import fauna.repo.cache.{ Cache, SchemaCache }
import fauna.repo.cassandra.{ CassandraService, Keyspace }
import fauna.repo.query.Query
import fauna.repo.service.rateLimits._
import fauna.scheduler.PriorityGroup
import fauna.snowflake._
import fauna.stats.StatsRecorder
import fauna.storage.Storage
import java.nio.file.Path
import java.util.concurrent.TimeoutException
import scala.annotation.unused
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.reflect.ClassTag

object RepoContext {
  val DefaultQueryTimeout = Duration.Inf
  val DefaultFQLXMaxStackFrames = 2048

  case class Result[T](transactionTS: Timestamp, value: T)
}

case class RepoContext protected (
  service: CassandraService,
  storage: Storage,
  clock: Clock,
  priorityGroup: PriorityGroup,
  stats: StatsRecorder,
  limiters: AccountOpsRateLimitsService,
  retryOnContention: Boolean,
  queryTimeout: Duration,
  readTimeout: FiniteDuration,
  newReadTimeout: FiniteDuration,
  rangeTimeout: FiniteDuration,
  writeTimeout: FiniteDuration,
  idSource: IDSource,
  cacheContext: CacheContext,
  isLocalHealthyQ: Query[Boolean],
  isClusterHealthyQ: Query[Boolean],
  isStorageHealthyQ: Query[Boolean],
  backupReadRatio: Double,
  repairTimeout: FiniteDuration,
  schemaRetentionDays: FiniteDuration,
  txnSizeLimitBytes: Int,
  txnMaxOCCReads: Int,
  enforceIndexEntriesSizeLimit: Boolean,
  reindexCancelLimit: Int,
  reindexCancelBytesLimit: Long,
  reindexDocsLimit: Int,
  taskReprioritizerLimit: Int,
  healthCheckTimeout: FiniteDuration = 1.second,
  healthCheckHealthyPeriod: FiniteDuration = 20.seconds,
  healthCheckUnhealthyPeriod: FiniteDuration = 10.seconds,
  backupReads: Boolean = true,
  replicaLocal: Boolean = false,
  maxAttempts: Int = 5,
  jwkProvider: Option[JWKProvider] = None,
  internalJWK: Option[(JWK, String)] = None,
  maxPerQueryParallelism: Int = Int.MaxValue,
  queryMaxConcurrentReads: Int = Int.MaxValue,
  queryMaxWidth: Int = Int.MaxValue,
  // Fraction of max local width at which we log queries.
  queryWidthLogRatio: Double = 1.0,
  queryMaxGlobalWidth: Int = Int.MaxValue,
  // Fraction of max global width at which we log queries.
  queryGlobalWidthLogRatio: Double = 1.0,
  // This results in query eval yielding about once every 20-30ms, based on
  // cursory test observation.
  // TODO: It might be worth tuning this based on a larger variety of
  // compute-heavy workloads.
  queryEvalStepsPerYield: Int = 1000,
  queryAccumulatePageWidth: Short = 1000,
  mvtProvider: MVTProvider = MVTProvider.Default,
  schemaOCCEnabled: Boolean = true,
  fqlxMaxStackFrames: Int = RepoContext.DefaultFQLXMaxStackFrames,
  exportPath: Option[Path] = None
) extends ExceptionLogging {

  import RepoContext._

  val keyspace = Keyspace(
    service,
    storage,
    clock,
    stats,
    backupReads,
    backupReadRatio,
    replicaLocal)

  def schema = storage.schema

  def forBackground =
    this.copy(priorityGroup = PriorityGroup.Background, backupReads = false)

  def forRepair: RepoContext = {
    this.copy(
      priorityGroup = PriorityGroup.Background,
      backupReads = false,
      replicaLocal = true
    )
  }

  def withStats(stats: StatsRecorder) = this.copy(stats = stats)
  def withPriority(pg: PriorityGroup) = this.copy(priorityGroup = pg)

  def withQueryTimeout(timeout: Duration) = copy(queryTimeout = timeout)

  def withSchemaOCCDisabled = copy(schemaOCCEnabled = false)
  def withRetryOnContention = copy(retryOnContention = true)
  def withRetryOnContention(maxAttempts: Int) =
    copy(retryOnContention = true, maxAttempts = maxAttempts)
  def withReindexDocsCancelLimit(limit: Int) = copy(reindexCancelLimit = limit)

  def nextID() = idSource.getID

  /** Returns true if the local host is among the healthy endpoints.
    */
  def isLocalHealthy: Boolean =
    runSynchronously(isLocalHealthyQ, healthCheckTimeout).value

  /** Returns true if all hosts in this cluster are among the healthy
    * endpoints.
    */
  def isClusterHealthy: Boolean =
    runSynchronously(isClusterHealthyQ, healthCheckTimeout).value

  /** Returns true if all hosts in a data replica are among the
    * healthy endpoints.
    */
  def isStorageHealthy: Boolean =
    runSynchronously(isStorageHealthyQ, healthCheckTimeout).value

  /** Given an account ID and a set of extra properties associated with the
    * account, returns a set of flags representing features available
    * to the account.
    */
  def accountFlags(
    account: AccountID,
    baseProps: Map[String, FFValue]): Query[AccountFlags] = {
    implicit val ec = ImmediateExecutionContext

    val req = service.accountProperties(account, baseProps)

    Query.future {
      service.flagsClient
        .getUncached[AccountID, AccountProperties, AccountFlags](req)
        .map { flags => flags.getOrElse(AccountFlags(account)) }
    }
  }

  def hostFlagsFut(): Future[HostFlags] = {
    // service is null in benchmarking
    if (service eq null) {
      return Future.successful(HostFlags(HostID.NullID))
    }

    implicit val ec = ImmediateExecutionContext
    service.cachedHostFlags() map { flags =>
      flags.getOrElse(HostFlags(service.localID.getOrElse(HostID.NullID)))
    }
  }

  def hostFlagFut[A <: FFValue: ClassTag](ff: Feature[HostID, A]): Future[A#T] = {
    implicit val ec = ImmediateExecutionContext
    hostFlagsFut() map { _.get(ff) }
  }

  def hostFlags(): Query[HostFlags] = Query.future(hostFlagsFut())

  def hostFlag[A <: FFValue: ClassTag](ff: Feature[HostID, A]): Query[A#T] =
    Query.future(hostFlagFut(ff))

  def run[T](
    query: Query[T],
    minSnapTime: Timestamp,
    accountID: AccountID,
    scopeID: ScopeID,
    limiter: OpsLimiter): Future[(Timestamp, T)] =
    // Protect against constructing a negative TimeBound
    if (queryTimeout <= Duration.Zero) {
      Future.failed(
        new TimeoutException(s"Timed out before deadline $queryTimeout."))
    } else {
      val deadline = queryTimeout match {
        case fd: FiniteDuration => fd.bound
        case _                  => TimeBound.Max
      }

      run(query, minSnapTime, deadline, accountID, scopeID, limiter)
    }

  def run[T](
    query: Query[T],
    minSnapTime: Timestamp,
    deadline: TimeBound,
    accountID: AccountID,
    scopeID: ScopeID,
    limiter: OpsLimiter): Future[(Timestamp, T)] =
    Query.execute(this, query, minSnapTime, deadline, accountID, scopeID, limiter)

  def runNow[T](query: Query[T]): Future[(Timestamp, T)] =
    run(query, clock.time, AccountID.Root, ScopeID.RootID, PermissiveOpsLimiter)

  def result[T](
    query: Query[T],
    minSnapTime: Timestamp = Timestamp.Epoch): Future[Result[T]] = {
    implicit val ec = ImmediateExecutionContext
    run(
      query,
      minSnapTime,
      AccountID.Root,
      ScopeID.RootID,
      PermissiveOpsLimiter) map { case (t, v) =>
      Result(t, v)
    }
  }

  def runSynchronously[T](
    query: Query[T],
    timeout: Duration,
    minSnapTime: Timestamp = Timestamp.Epoch): Result[T] = {
    val resultF =
      withQueryTimeout(timeout min queryTimeout)
        .result(query, minSnapTime)
    Await.result(resultF, timeout)
  }

  def !![T](query: Query[T])(implicit @unused ctrl: ConsoleControl): Result[T] =
    runSynchronously(query, Duration.Inf)

  def ![T](query: Query[T])(implicit ctrl: ConsoleControl): T =
    (this !! query).value
}

final case class CacheContext(
  schema: Cache,
  schema2: SchemaCache,
  keys: Cache
) {

  def setRepo(ctx: RepoContext): Unit = {
    schema.setRepo(ctx)
    schema2.setRepo(ctx)
    keys.setRepo(ctx)
  }

  def invalidateAll(): Unit = {
    schema.invalidate()
    schema2.invalidate()
    keys.invalidate()
  }

  def reportStats(stats: StatsRecorder): Unit = {
    schema.reportAndResetStats(stats)
    schema2.reportAndResetStats(stats)
    keys.reportAndResetStats(stats)
  }
}
