package fauna.repo.service.rateLimits

import fauna.atoms.AccountID
import fauna.codex.json._
import fauna.lang.clocks._
import fauna.limits._
import fauna.logging.ExceptionLogging
import fauna.net.{ BurstLimiter, LimiterCounter, LimiterStats, RateLimiter }
import fauna.repo.query.State
import fauna.stats.StatsRequestBuffer
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import scala.util.control.{ NoStackTrace, NonFatal }

sealed trait OpsLimitException {
  def op: String
}

final case class OpsLimitExceptionWithMetrics(op: String, metrics: State.Metrics)
    extends Exception(s"Rate limit for ${op} exceeded")
    with NoStackTrace
    with OpsLimitException

final case class OpsLimitExceptionForRendering(
  op: String,
  buffer: StatsRequestBuffer)
    extends Exception(s"Rate limit for ${op} exceeded")
    with NoStackTrace
    with OpsLimitException

trait OpsLimiter {
  def tryAcquireReads(n: Int): RateLimiter.Result
  def tryAcquireWrites(n: Int): RateLimiter.Result
  def tryAcquireCompute(n: Int): RateLimiter.Result
  def tryAcquireStream(): RateLimiter.Result

  // Arrival rate measurement.
  def snapshot(): LimiterStats
  def snapshotAndReset(): LimiterStats

  // Sync remote arrivals with this limiter.
  def sync(stats: LimiterStats): OpsLimiter

  def toJSON: JSObject
}

final object PermissiveOpsLimiter extends OpsLimiter {
  private[this] val Acquired = RateLimiter.Acquired(false)
  def tryAcquireReads(n: Int): RateLimiter.Result = Acquired
  def tryAcquireWrites(n: Int): RateLimiter.Result = Acquired
  def tryAcquireCompute(n: Int): RateLimiter.Result = Acquired
  def tryAcquireStream(): RateLimiter.Result = Acquired
  def snapshot(): LimiterStats = LimiterStats.empty
  def snapshotAndReset(): LimiterStats = LimiterStats.empty
  def sync(stats: LimiterStats): OpsLimiter = this
  def toJSON: JSObject = AccountLimits.empty.toJSON
}

// Base class for test limiters that allow a fixed amount of ops
// and then reject further ops.
abstract class FixedOpsLimiter(max: Int) extends OpsLimiter {
  val count = new AtomicInteger(0)
  val Acquired = RateLimiter.Acquired(false)
  val Rejected = RateLimiter.DelayUntil(1000.hours)
  def tryAcquire(n: Int): RateLimiter.Result =
    if (count.addAndGet(n) > max) Rejected else Acquired
  def tryAcquireReads(n: Int): RateLimiter.Result = Acquired
  def tryAcquireWrites(n: Int): RateLimiter.Result = Acquired
  def tryAcquireCompute(n: Int): RateLimiter.Result = Acquired
  def tryAcquireStream(): RateLimiter.Result = Acquired
  def snapshot(): LimiterStats = LimiterStats.empty
  def snapshotAndReset(): LimiterStats = LimiterStats.empty
  def sync(stats: LimiterStats): OpsLimiter = this
  def toJSON: JSObject = AccountLimits.empty.toJSON
}

final case class FixedWriteOpsLimiter(max: Int) extends FixedOpsLimiter(max: Int) {
  override def tryAcquireWrites(n: Int): RateLimiter.Result = tryAcquire(n)
}

final case class FixedReadOpsLimiter(max: Int) extends FixedOpsLimiter(max: Int) {
  override def tryAcquireReads(n: Int): RateLimiter.Result = tryAcquire(n)
}

final case class FixedComputeOpsLimiter(max: Int) extends FixedOpsLimiter(max: Int) {
  override def tryAcquireCompute(n: Int): RateLimiter.Result = tryAcquire(n)
}

object OpsRateLimiter {
  def apply(
    account: AccountID,
    clock: Clock,
    softLimits: OpsLimits,
    hardLimits: OpsLimits,
    streamsPerSecond: Int,
    debugLog: Boolean = false): OpsRateLimiter = {
    require(
      streamsPerSecond >= 0,
      s"streamsPerSecond must be >= 0, but was $streamsPerSecond")

    checkLimits(softLimits, hardLimits)

    new OpsRateLimiter(
      account,
      clock,
      softLimits,
      hardLimits,
      streamsPerSecond,
      debugLog)
  }

  private def checkLimits(softLimits: OpsLimits, hardLimits: OpsLimits) = {
    def checkLimits0(soft: OpLimit, hard: OpLimit) =
      if (soft.permitsPerSecond > hard.permitsPerSecond) {
        throw new IllegalArgumentException(
          s"soft limits must not be greater than hard limits. soft=$soft hard=$hard")
      }

    checkLimits0(softLimits.read, hardLimits.read)
    checkLimits0(softLimits.write, hardLimits.write)
    checkLimits0(softLimits.compute, hardLimits.compute)
  }
}

final class OpsRateLimiter private (
  account: AccountID,
  clock: Clock,
  softLimits: OpsLimits,
  hardLimits: OpsLimits,
  streamsPerSecond: Int,
  debugLog: Boolean)
    extends OpsLimiter
    with ExceptionLogging {

  private[this] val counter = new LimiterCounter()

  private[this] val readsSoft = BurstLimiter
    .newBuilder()
    .withName(s"$account Read Ops - Soft")
    .withClock(clock)
    .withPermitsPerSecond(softLimits.read.permitsPerSecond)
    .withMaxBurstSeconds(0.0)
    .withDebugLog(debugLog)
    .build()

  private[this] val readsHard = BurstLimiter
    .newBuilder()
    .withName(s"$account Read Ops - Hard")
    .withClock(clock)
    .withPermitsPerSecond(hardLimits.read.permitsPerSecond)
    .withMaxBurstSeconds(hardLimits.read.burstSeconds)
    .withDebugLog(debugLog)
    .build()

  private[this] val writesSoft = BurstLimiter
    .newBuilder()
    .withName(s"$account Write Ops - Soft")
    .withClock(clock)
    .withPermitsPerSecond(softLimits.write.permitsPerSecond)
    .withMaxBurstSeconds(0.0)
    .withDebugLog(debugLog)
    .build()

  private[this] val writesHard = BurstLimiter
    .newBuilder()
    .withName(s"$account Write Ops - Hard")
    .withClock(clock)
    .withPermitsPerSecond(hardLimits.write.permitsPerSecond)
    .withMaxBurstSeconds(hardLimits.write.burstSeconds)
    .withDebugLog(debugLog)
    .build()

  private[this] val computeSoft = BurstLimiter
    .newBuilder()
    .withName(s"$account Compute Ops - Soft")
    .withClock(clock)
    .withPermitsPerSecond(softLimits.compute.permitsPerSecond)
    .withMaxBurstSeconds(0.0)
    .withDebugLog(debugLog)
    .build()

  private[this] val computeHard = BurstLimiter
    .newBuilder()
    .withName(s"$account Compute Ops - Hard")
    .withClock(clock)
    .withPermitsPerSecond(hardLimits.compute.permitsPerSecond)
    .withMaxBurstSeconds(hardLimits.compute.burstSeconds)
    .withDebugLog(debugLog)
    .build()

  private[this] val streams = BurstLimiter
    .newBuilder()
    .withName(s"$account Streams")
    .withClock(clock)
    .withPermitsPerSecond(streamsPerSecond)
    .withMaxBurstSeconds(0.0)
    .withDebugLog(debugLog)
    .build()

  // TODO: pull batches of compute ops so we're not always checking the limiter

  def tryAcquireReads(n: Int): RateLimiter.Result =
    acquireAndCount(readsSoft, readsHard, n, counter.recordRead)
  def tryAcquireWrites(n: Int): RateLimiter.Result =
    acquireAndCount(writesSoft, writesHard, n, counter.recordWrite)
  def tryAcquireCompute(n: Int): RateLimiter.Result =
    acquireAndCount(computeSoft, computeHard, n, counter.recordCompute)
  def tryAcquireStream(): RateLimiter.Result =
    acquireAndCount(RateLimiter.NoLimit, streams, 1, counter.recordStream)

  def sync(stats: LimiterStats): OpsLimiter = {
    def checkAndSync(limiter: RateLimiter, permits: Int) =
      // Drop zeroes and overflows.
      if (permits > 0) {
        limiter.sync(permits)
      }

    checkAndSync(readsSoft, stats.readCount.toInt)
    checkAndSync(readsHard, stats.readCount.toInt)
    checkAndSync(writesSoft, stats.writeCount.toInt)
    checkAndSync(writesHard, stats.writeCount.toInt)
    checkAndSync(computeSoft, stats.computeCount.toInt)
    checkAndSync(computeHard, stats.computeCount.toInt)

    this
  }

  def snapshot(): LimiterStats =
    // NB. gossip will drop empty snapshots.
    counter.snapshot()

  def snapshotAndReset(): LimiterStats =
    // NB. gossip will drop empty snapshots.
    counter.snapshotAndReset()

  def toJSON: JSObject = {
    val limits = AccountLimits(
      hardRead = Some(hardLimits.read.permitsPerSecond),
      hardWrite = Some(hardLimits.write.permitsPerSecond),
      hardCompute = Some(hardLimits.compute.permitsPerSecond),
      softRead = Some(softLimits.read.permitsPerSecond),
      softWrite = Some(softLimits.write.permitsPerSecond),
      softCompute = Some(softLimits.compute.permitsPerSecond)
    )

    limits.toJSON
  }

  override def toString(): String =
    s"OpsRateLimiter(soft=$softLimits,hard=$hardLimits)"

  private def acquireAndCount(
    softLimiter: RateLimiter,
    hardLimiter: RateLimiter,
    permits: Int,
    incr: Int => Unit): RateLimiter.Result =
    try {
      val softResult = softLimiter.tryAcquire(permits)
      val hardResult = hardLimiter.tryAcquire(permits)

      // NB. This variant of tryAcquire() will not reserve permits
      // unless it can successfully acquire without delay. Count only
      // successes here.
      if (softResult.isSuccess || hardResult.isSuccess) {
        incr(permits)
      }

      // Rules:
      //   * hard has precedence over soft.
      //   * soft has zero burst.
      //   * exceeding soft = warning; exceeding hard = error.
      //   * soft <= hard, hard burst = exceeding soft = warning.
      hardResult match {
        case r: RateLimiter.DelayUntil => r
        case _: RateLimiter.Acquired =>
          softResult match {
            case _: RateLimiter.DelayUntil =>
              // Issue a warning.
              RateLimiter.Acquired(inBurst = true)

            case _: RateLimiter.Acquired =>
              RateLimiter.Acquired(inBurst = false)
          }
      }
    } catch {
      case NonFatal(ex) =>
        logException(ex)
        RateLimiter.Acquired(inBurst = false)
    }

}
