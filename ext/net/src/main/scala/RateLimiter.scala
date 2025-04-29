package fauna.net

import fauna.exec.Timer
import fauna.lang.Timestamp
import fauna.lang.clocks._
import fauna.lang.syntax._
import scala.concurrent.duration._
import scala.concurrent.Future

object RateLimiter {

  sealed trait Result {
    def isSuccess: Boolean
  }

  /** Returned by tryAcquire() when a permit reservation succeeds. */
  case class Acquired(inBurst: Boolean) extends Result {
    def isSuccess = true
  }

  /** Returned by tryAcquire() when a permit reservation fails. Callers
    * should delay before retrying.
    */
  case class DelayUntil(delay: FiniteDuration) extends Result {
    def isSuccess = false
  }

  val NoLimit = new RateLimiter {
    val name = "NoLimit"
    val isDebugEnabled = false
    val clock = Clock
    def reserve(permits: Int, nowMicros: Long): (Long, Boolean) = (0, false)
    def canAcquire(permits: Int, nowMicros: Long, withinMicros: Long): Boolean = true
    def nextAvailable(nowMicros: Long): Long = 0
  }

  val logger = getLogger()
}

trait RateLimiter {

  /** An identifier used to differentiate between rate limiters in log output. */
  def name: String

  /** Enable debug logging when this method is true. */
  def isDebugEnabled: Boolean

  /** Acquires a number of permits from the rate limiter, blocking until
    * all permits have been acquired.
    *
    * Returns the time spent blocking.
    */
  def acquire(permits: Int = 1): FiniteDuration = {
    checkPermits(permits)
    val (wait, _) = reserve(permits)

    RateLimiter.logger.debug(
      s"RateLimiter: waiting ${wait.toCoarsest} for $permits permits.")

    clock.sleepUninterruptibly(wait)

    wait
  }

  /** Non-blocking variant of acquire().
    *
    * NOTE: The precision of this method will vary based on the
    * precision of the provided Timer.
    */
  def acquireAsync(
    permits: Int = 1,
    timer: Timer = Timer.Global): Future[FiniteDuration] = {
    checkPermits(permits)
    val (wait, _) = reserve(permits)

    if (isDebugEnabled) {
      RateLimiter.logger.info(
        s"RateLimiter($name): waiting ${wait.toCoarsest} for $permits permits.")
    }

    timer.delay(wait) { Future.successful(wait) }
  }

  /** Returns Acquired if it can acquire the requested permits without
    * waiting. Returns DelayUntil otherwise in addition to not
    * changing the limiter's stored permits.
    */
  def tryAcquire(permits: Int = 1): RateLimiter.Result = {
    checkPermits(permits)
    tryAcquire(permits, Duration.Zero)
  }

  /** Returns Acquired if it can acquire the requested permits within
    * the deadline, blocking as necessary until the permits become
    * available. Returns DelayUntil otherwise in addition to not
    * changing the limiter's stored permits.
    */
  def tryAcquire(permits: Int, deadline: FiniteDuration): RateLimiter.Result = {
    checkPermits(permits)
    val timeoutMicros = deadline.toMicros max 0

    val (wait, inBurst) = synchronized {
      val nowMicros = clock.micros
      if (!canAcquire(permits, nowMicros, timeoutMicros)) {
        if (isDebugEnabled) {
          RateLimiter.logger.info(
            s"RateLimiter($name): cannot acquire $permits permits within $deadline.")
        }

        return RateLimiter.DelayUntil((nextAvailable(nowMicros) - nowMicros).micros)
      } else {
        val res = reserve(permits, nowMicros)

        // Important: assert under the lock to capture the limiter
        // state consistently.
        require(res._1 <= timeoutMicros,
          s"RateLimiter($name): waiting ${res._1} usecs. for $permits permits " +
            s"would exceed timeout $timeoutMicros usecs. " +
            s"(limiter=$this)")

        res
      }
    }

    clock.sleepUninterruptibly(wait.micros)
    RateLimiter.Acquired(inBurst)
  }

  /** Syncs this limiter with others by applying a deficit to the
    * currently available permit supply.
    */
  def sync(deficit: Int): Unit = {
    checkPermits(deficit)
    synchronized {
      forceReserve(deficit)
    }
  }

  protected def reserve(permits: Int): (FiniteDuration, Boolean) =
    synchronized {
      val (wait, inBurst) = reserve(permits, clock.micros)
      (wait.micros, inBurst)
    }

  protected def clock: Clock

  /** Reserves a number of permits and returns the number of micros from
    * `nowMicros` when they become available and a flag indicating
    * whether part of the reservation was fulfilled from the burst
    * bucket.
    */
  protected def reserve(permits: Int, nowMicros: Long): (Long, Boolean)

  /** Reserves a number of permits without delay. If the number of
    * permits exceeds the currently-available supply, delay any future
    * reservation accordingly.
    */
  protected def forceReserve(permits: Int): Unit = synchronized {
    reserve(permits, nextAvailable(clock.micros))
  }

  /** Returns true if permits may be acquired within a limited amount of time from now. */
  protected def canAcquire(
    permits: Int,
    nowMicros: Long,
    withinMicros: Long): Boolean

  /** Returns the time at which the next reservation becomes available
    * in UNIX epoch micros.
    *
    * NOTE: callers must hold a mutex on `this`!
    */
  protected def nextAvailable(nowMicros: Long): Long

  private def checkPermits(permits: Int) =
    require(permits > 0, "May not request less than 1 permit.")

}

object BurstLimiter {
  def newBuilder(): Builder = new Builder()

  final class Builder {

    private[this] var name: String = ""
    private[this] var clock: Clock = new MonotonicClock(Clock)
    private[this] var permitsPerSecond = 1.0d
    private[this] var maxBurstSeconds = 1.0d
    private[this] var debugLog: Boolean = false

    def withPermitsPerSecond(permits: Double): Builder = {
      require(
        permits > 0.0,
        s"Permits per second must be greater than zero, received $permits.")
      this.permitsPerSecond = permits
      this
    }

    def withMaxBurstSeconds(seconds: Double): Builder = {
      require(
        seconds >= 0.0,
        s"Max burst seconds must be non-negative, received $seconds.")
      this.maxBurstSeconds = seconds
      this
    }

    def withClock(clock: Clock): Builder = {
      this.clock = clock
      this
    }

    def withName(name: String): Builder = {
      this.name = name
      this
    }

    def withDebugLog(value: Boolean): Builder = {
      this.debugLog = value
      this
    }

    def build(): BurstLimiter =
      new BurstLimiter(
        name,
        1.second.toMicros / permitsPerSecond,
        clock,
        maxBurstSeconds,
        debugLog)
  }
}

/** Limits permits to a frequency given by intervalMicros, relative to
  * the Clock.
  *
  * After a period of inactivity (up to maxBurstSeconds), reserve()
  * will issue permits without throttling.
  */
final class BurstLimiter private (
  val name: String,
  intervalMicros: Double,
  val clock: Clock,
  maxBurstSeconds: Double,
  val isDebugEnabled: Boolean)
    extends RateLimiter {

  /** The max number of permits per second. */
  private[this] val maxBasePermits =
    (1.second.toMicros / intervalMicros).toInt

  /** The max number of permits saved during periods of inactivity. */
  private[this] val maxPermits =
    Math.floor(maxBurstSeconds * maxBasePermits).toInt

  /** The number of permits in the current window at the base rate. */
  private[this] var basePermits = maxBasePermits

  /** The number of permits saved for the next burst. */
  private[this] var burstPermits = 0

  /** The time when the next request (no matter its size) will be
    * granted. After granting a request, this is pushed further in the
    * future. Large requests push this further than small requests.
    */
  private[this] var nextFreeMicros = clock.micros

  override def toString =
    s"BurstLimiter($name, base=$basePermits, burst=$burstPermits, " +
      s"next=${Timestamp.ofMicros(nextFreeMicros)})"

  protected def nextAvailable(nowMicros: Long): Long =
    nextFreeMicros

  protected def canAcquire(
    permits: Int,
    nowMicros: Long,
    withinMicros: Long): Boolean = {
    if (isDebugEnabled) {
      RateLimiter.logger.info(
        s"BurstLimiter($name): check $permits within $withinMicros of $nowMicros " +
          s"(next: $nextFreeMicros base: $basePermits burst: $burstPermits)")
    }

    (maxBasePermits + maxPermits) >= permits && ((basePermits + burstPermits) >= permits ||
      nextFreeMicros - withinMicros <= nowMicros)
  }

  protected def reserve(permits: Int, nowMicros: Long): (Long, Boolean) = {
    maybeRefill(nowMicros)

    require(basePermits >= 0, s"base permits must be >= 0 ($this)")
    require(burstPermits >= 0, s"burst permits must be >= 0 ($this)")

    val moment = nextFreeMicros

    // Take as many as necessary from accumulated base permits.
    val base = permits min basePermits

    // Take the remainder from burst.
    val burst = ((permits - basePermits) max 0) min burstPermits

    // Generate any necessary fresh permits.
    val fresh = permits - (base + burst)

    // Push the limiter out by the distance of the fresh permits.
    val wait = fresh * intervalMicros

    nextFreeMicros = nextFreeMicros.saturatedAdd(wait.toLong)
    basePermits = basePermits - base
    burstPermits = burstPermits - burst

    if (isDebugEnabled) {
      RateLimiter.logger.info(
      s"BurstLimiter($name): reserved permits until $nextFreeMicros " +
        s"(wait: $wait base: $basePermits burst: $burstPermits)")
    }

    ((moment - nowMicros) max 0, burst != 0)
  }

  private def maybeRefill(nowMicros: Long): Unit =
    // Sync with the new "now".
    if (nowMicros > nextFreeMicros) {
      // Make it a Long to guard against overflows.
      val newPermits =
        Math.floor((nowMicros - nextFreeMicros) / intervalMicros).toLong

      // Increment base by the number of intervals that have passed, up to max.
      if (newPermits > 0) {
        basePermits = ((newPermits + basePermits) min maxBasePermits).toInt
      }

      // The remainder of new permits may be accumulated in burst.
      val newBurst = (newPermits - basePermits) max 0

      // Promote sum to a Long to avoid overflowing Int.
      val burst = burstPermits + newBurst.toLong

      burstPermits = (burst min maxPermits).toInt

      if (isDebugEnabled) {
        RateLimiter.logger.info(s"BurstLimiter($name): syncing time to $nowMicros " +
          s"(next: $nextFreeMicros, new: $newPermits base: $basePermits burst: $burstPermits)")
      }

      nextFreeMicros = nowMicros
    }
}
