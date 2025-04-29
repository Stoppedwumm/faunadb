package fauna.lang.clocks

import fauna.lang.{ TimeBound, Timestamp, WeightedAverage }
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration._

/** Various clock strategies.
  *
  * FIXME: once our internal timestamps are up to par, base clocks on
  * nano precision.
  */
trait Clock {
  def micros: Long
  def millis: Long
  def time: Timestamp

  def fromNow(d: Duration) = time + d
  def expireTime(d: TimeBound)(implicit clock: DeadlineClock) = time + d.timeLeft

  /** Sleeps for the given duration without allowing InterruptedExceptions.
    *
    * If an interrupt does occur during the sleep, the calling thread
    * will be interrupted after the sleep has passed.
    */
  def sleepUninterruptibly(duration: FiniteDuration): Unit = {
    var interrupted = false
    try {
      var remainingNanos = duration.toNanos
      val finishNanos = (micros * 1000) + remainingNanos

      while (true) {
        try {
          // TimeUnit.sleep() treats negative timeouts just like zero.
          NANOSECONDS.sleep(remainingNanos)
          return
        } catch {
          case _: InterruptedException =>
            interrupted = true
            remainingNanos = finishNanos - (micros * 1000)
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread.interrupt()
      }
    }
  }
}

abstract class TimestampClock extends Clock {
  def micros = time.micros
  def millis = time.millis
}

abstract class MillisClock extends Clock {
  def micros = millis * 1000
  def time = Timestamp.ofMillis(millis)
}

abstract class MicrosClock extends Clock {
  def millis = micros / 1000
  def time = Timestamp.ofMicros(micros)
}

class SystemClock extends MillisClock {
  def millis = System.currentTimeMillis
}

object Clock extends SystemClock

class TestClock(private var now: Timestamp = Timestamp.Epoch)
    extends TimestampClock {
  def time = now
  def set(ts: Timestamp) = now = ts
  def advance(d: Duration) = now += d
  def tick() = advance(1.micro)

  override def sleepUninterruptibly(duration: FiniteDuration): Unit =
    advance(duration)
}

class MonotonicClock(clock: Clock, minAdvance: FiniteDuration = Duration.Zero)
    extends MicrosClock {
  private[this] val counter = new AtomicLong(clock.micros)
  private[this] val minAdvanceMicros = minAdvance.toMicros

  @annotation.tailrec
  final def micros = {
    val last = counter.get
    val current = clock.micros
    val next = if (current <= last) last + minAdvanceMicros else current

    if ((next > last) && !counter.compareAndSet(last, next)) {
      micros
    } else {
      next
    }
  }

  /** Forcibly resets to the underlying clock. This operation can violate
    * monotonicity. Be sure to know what you are doing.
    */
  def reset(): Unit =
    counter.set(clock.micros)
}

abstract class DelayTrackingClock extends MicrosClock {
  def mark(time: Timestamp): Unit

  def mark(delay: Duration): Unit

  def delay: Int
}

class WeightedDelayTrackingClock(clock: Clock) extends DelayTrackingClock {

  /** This is set to the minimal allowable weight to force a more stable
    * evolution of the read clock. This trades accuracy in snapshot selection
    * for write latency (we wait proportional to the range of readclocks we
    * observe).
    */
  private[this] val averageDelayMicros = new WeightedAverage(1)

  private[this] val mono = new MonotonicClock(
    new MicrosClock {
      def micros = clock.micros - delay
    },
    Duration.Zero)

  def mark(time: Timestamp) =
    averageDelayMicros.sample((clock.micros - time.micros) max 0)

  def mark(delay: Duration) = averageDelayMicros.sample(delay.toMicros max 0)

  def micros = mono.micros

  def delay = averageDelayMicros.value.toInt

  def reset() = mono.reset()
}

class DirectDelayTrackingClock(clock: Clock, maxDelay: FiniteDuration)
    extends DelayTrackingClock {
  private[this] val maxDelayMicros = maxDelay.toMicros.toInt

  @volatile
  private[this] var _delay = 0

  protected val mono = new MonotonicClock(
    new MicrosClock {
      def micros = clock.micros - delay
    },
    Duration.Zero)

  def mark(time: Timestamp) = _delay = (clock.micros - time.micros).toInt max 0

  def mark(delay: Duration) = _delay = delay.toMicros.toInt max 0

  def micros = mono.micros

  def delay = _delay min maxDelayMicros

  def reset() = mono.reset()
}

trait DeadlineClock {
  def nanos: Long
  def millis: Long = nanos / 1_000_000
  def seconds: Long = millis / 1_000
}

object SystemDeadlineClock extends DeadlineClock {
  def nanos: Long = System.nanoTime()
}

object DeadlineClock {
  implicit val global: DeadlineClock = SystemDeadlineClock
}
