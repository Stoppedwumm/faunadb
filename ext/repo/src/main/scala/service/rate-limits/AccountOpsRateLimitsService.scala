package fauna.repo.service.rateLimits

import com.github.benmanes.caffeine.cache._
import fauna.atoms.AccountID
import fauna.flags._
import fauna.lang.clocks._
import fauna.limits._
import fauna.logging._
import fauna.net.{ ArrivalRateInfo, ArrivalRateService, LimiterStats }
import fauna.repo.query.Query
import fauna.stats.StatsRecorder
import org.apache.logging.log4j.LogManager
import scala.concurrent.duration._
import scala.jdk.DurationConverters._

trait AccountOpsRateLimitsService extends ArrivalRateService {
  def get(
    accountID: AccountID,
    accountFlags: AccountFlags,
    limits: AccountLimits): Query[OpsLimiter]
}

object PermissiveAccountOpsRateLimitsService extends AccountOpsRateLimitsService {
  private[this] val limiter: Query[OpsLimiter] = Query.value(PermissiveOpsLimiter)
  def get(
    accountID: AccountID,
    accountFlags: AccountFlags,
    limits: AccountLimits): Query[OpsLimiter] = limiter

  def poll(): Map[AccountID, LimiterStats] = Map.empty
  def register(arrivals: Map[AccountID, LimiterStats]): Unit = ()
}

private case class CacheKey(
  accountID: AccountID,
  softLimits: OpsLimits,
  hardLimits: OpsLimits,
  streamLimits: Int,
  debugLog: Boolean)

class CachingAccountOpsRateLimitsService(
  cacheMaxSize: Int,
  cacheExpireAfter: FiniteDuration,
  systemReadLimit: Double,
  systemWriteLimit: Double,
  systemComputeLimit: Double,
  systemStreamLimit: Long,
  burstSeconds: Double,
  statsRec: StatsRecorder,
  logLimiters: Boolean,
  clock: Clock = Clock)
    extends AccountOpsRateLimitsService {

  private[this] lazy val logger = LogManager.getLogger("limiter")

  private[this] val limiterClock: Clock = new MonotonicClock(clock)

  private[this] val cache: Cache[CacheKey, OpsLimiter] =
    Caffeine.newBuilder
      .maximumSize(cacheMaxSize)
      .expireAfterAccess(cacheExpireAfter.toJava)
      .recordStats()
      .build()

  @volatile private[this] var stats = cache.stats()

  private def reportStats() = {
    val snap = cache.stats()
    val prev = stats
    stats = snap

    val delta = snap.minus(prev)

    val prefix = "Cache.RateLimiters.AccountOps"
    statsRec.count(s"${prefix}.Hits", delta.hitCount.toInt)
    statsRec.count(s"${prefix}.Misses", delta.missCount.toInt)
    statsRec.count(s"${prefix}.Evictions", delta.evictionCount.toInt)
    statsRec.count(s"${prefix}.Loads", delta.loadCount.toInt)
    statsRec.count(s"${prefix}.Exceptions", delta.loadFailureCount.toInt)
    statsRec.count(s"${prefix}.Objects", cache.estimatedSize.toInt)
  }

  StatsRecorder.polling(10.seconds) {
    reportStats()
  }

  // Don't bother starting the polling loop if logging is disabled.
  if (logLimiters) {
    StatsRecorder.polling(1.second) {
      val snaps = Map.newBuilder[AccountID, LimiterStats]

      cache.asMap() forEach { (key, limiter) =>
        val stats = limiter.snapshot() // NB. don't reset the limiter!

        if (stats.nonEmpty) {
          snaps += (key.accountID -> stats)
        }
      }

      val loggable = snaps.result()
      if (loggable.nonEmpty) {
        logger.info(JSONMessage(ArrivalRateInfo(loggable)))
      }
    }
  }

  private[this] val accountReadsFlag = new RateLimitsReads(systemReadLimit)
  private[this] val accountWritesFlag = new RateLimitsWrites(systemWriteLimit)
  private[this] val accountComputeFlag = new RateLimitsCompute(systemComputeLimit)
  private[this] val accountStreamsFlag = new RateLimitsStreams(systemStreamLimit)

  def get(
    accountID: AccountID,
    accountFlags: AccountFlags,
    limits: AccountLimits): Query[OpsLimiter] =
    Query.repo flatMap { repo =>
      repo.hostFlag(EnableRateLimits) map { enabled =>
        if (!enabled) {
          PermissiveOpsLimiter
        } else {
          val hardLimits = limits.getHardLimits(
            accountFlags,
            accountReadsFlag,
            accountWritesFlag,
            accountComputeFlag,
            systemReadLimit,
            systemWriteLimit,
            systemComputeLimit,
            burstSeconds)

          val softLimits = limits.getSoftLimits(
            accountFlags,
            accountReadsFlag,
            accountWritesFlag,
            accountComputeFlag,
            hardLimits)

          val streamLimits = accountFlags.get(accountStreamsFlag).intValue

          val debugLog = accountFlags.get(DebugRateLimits)

          cache.get(
            CacheKey(accountID, softLimits, hardLimits, streamLimits, debugLog),
            { _ =>
              OpsRateLimiter(
                accountID,
                limiterClock,
                softLimits,
                hardLimits,
                streamLimits,
                debugLog
              )
            }
          )
        }
      }
    }

  /** Returns arrivals at all limiters since the last call to `poll()`.
    */
  def poll(): Map[AccountID, LimiterStats] = {
    var arrivals = 0
    val snaps = Map.newBuilder[AccountID, LimiterStats]

    cache.asMap() forEach { (key, limiter) =>
      val stats = limiter.snapshotAndReset()

      if (stats.nonEmpty) {
        snaps += (key.accountID -> stats)
        arrivals += 1
      }
    }

    statsRec.count("RateLimiter.Arrivals.Sent", arrivals)
    snaps.result()
  }

  /** Syncs all local limiters with the remote arrival rates discovered
    * via gossip.
    */
  def register(arrivals: Map[AccountID, LimiterStats]): Unit = {
    val snapshot = cache.asMap()

    snapshot forEach { (key, _) =>
      arrivals.get(key.accountID) foreach { stats =>
        snapshot.compute(
          key,
          { (_, v) =>
            if (v ne null) {
              statsRec.incr("RateLimiter.Arrivals.Received")
              v.sync(stats)
            } else {
              // Limiter fell out of cache. So be it.
              statsRec.incr("RateLimiter.Arrivals.Dropped")
              v
            }
          }
        )
      }
    }
  }
}
