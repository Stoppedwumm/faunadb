package fauna.lang

import fauna.lang.clocks.DeadlineClock
import java.util.concurrent.TimeoutException
import scala.concurrent.duration._

/**
  * TimeBound is our version of Scala's Deadline. It's been re-written here so we can
  * pass in our own DeadlineClock allowing for stable tests. DeadlineClock must be
  * monotonic to ensure TimeBounds can be adhered to regardless of what's happening to
  * a system's wall clock.
  */
final case class TimeBound private (startNanos: Long, expireNanos: Long) {

  require(expireNanos - startNanos >= 0, "negative time bound")

  /**
    * Calculate time difference between this duration and now; the result is negative if the deadline has passed.
    */
  def timeLeft(implicit clock: DeadlineClock): FiniteDuration =
    (expireNanos - clock.nanos).nanos

  /**
    * Determine whether the deadline still lies in the future at the point where this method is called.
    */
  def hasTimeLeft(implicit clock: DeadlineClock): Boolean = !isOverdue

  /**
    * Determine whether the deadline lies in the past at the point where this method is called.
    */
  def isOverdue(implicit clock: DeadlineClock): Boolean =
    (expireNanos - clock.nanos) < 0

  /**
    * Throws a TimeoutException if this TimeBound is overdue.
    */
  def checkOrThrow()(implicit clock: DeadlineClock): Unit =
    if (isOverdue) {
      throw new TimeoutException(s"$toString expired")
    }

  def duration: FiniteDuration = (expireNanos - startNanos).nanos

  override def toString = s"TimeBound(${duration.toCoarsest})"

  def min(other: TimeBound) =
    // Using subtraction maintains correctness in presence of overflows
    if (expireNanos - other.expireNanos <= 0) {
      this
    } else {
      other
    }

  def max(other: TimeBound) =
    if (expireNanos - other.expireNanos >= 0) {
      this
    } else {
      other
    }
}

object TimeBound {

  def apply(d: FiniteDuration)(implicit clock: DeadlineClock): TimeBound = {
    val snap = clock.nanos
    TimeBound(snap, snap + d.toNanos)
  }

  val Min = TimeBound(0, 0)
  val Max = TimeBound(0, Long.MaxValue)
}
