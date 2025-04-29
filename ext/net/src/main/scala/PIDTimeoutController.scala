package fauna.net

import fauna.exec.Timer
import fauna.lang.clocks.DeadlineClock
import fauna.lang.{ PID, TimeBound }
import fauna.stats.StatsRecorder
import io.netty.util.Timeout
import java.io.Closeable
import java.util.concurrent.atomic.LongAdder
import scala.concurrent.duration._

/**
  * PID to control timeout delay based on a target percentage of timeouts.
  *
  * Dynamically learn  (100 - X)th percentile read latency s.t by triggering a
  * second read after waiting out this latency we only issue backup reads X
  * percent of the time.
  *
  * How?
  *
  * We use a PID with a setpoint of the ratio of reads we want to achieve and
  * control variable that represents the delay we'd like to apply to issuing
  * the second read. The control variable will be managed by the PID by sampling
  * the state of system.
  *
  * Note: This means that as the latency of reads goes down, the applied delay
  * goes down as well. The converse is also true: reads taking longer will mean
  * increased delays before issuing a second read.
  */
class PIDTimeoutController(
  readRatio: Double,
  readTimeout: FiniteDuration,
  minDelay: FiniteDuration,
  name: String,
  stats: StatsRecorder)
    extends Closeable {

  require(readRatio >= 0 && readRatio <= 1, "Read ratio must be in [0,1]")
  require(minDelay <= readTimeout)

  // NB. Windup of 1 produces an increase/decrease rate of 1 millisecond per sample.
  // Sampling at 100 milliseconds interval will ensure an increase/decrease of at
  // most 600 milliseconds per minute. Increasing the windup window will increase the
  // integral gain quickly during spikes in latency, although recovery time is known
  // to be slow. Limiting the PID output helps recovery time during long periods of
  // instability.
  //
  // TODO: should these be configurable?
  // Seems counter to the point of the thing.
  private val pid =
    new PID(p = -20,
            i = -1,
            d = -1,
            windup = PID.window(1),
            limit = PID.limit(minDelay.toMillis.toDouble, readTimeout.toMillis.toDouble))

  pid.setPoint = readRatio

  // Chances are the system is a bit slow at start.
  // Will begin half way through the limit and adjust from there.
  @volatile private var _delay = readTimeout / 2

  private val samples = new LongAdder
  private val timeouts = new LongAdder

  private val heartbeat = Heartbeat(100.millis, name) {
    val timeoutsNow = timeouts.sumThenReset
    val samplesNow = samples.sumThenReset
    if (samplesNow != 0) {
      val ratio = timeoutsNow.toDouble / samplesNow.toDouble
      val rawDelay = pid(ratio)
      _delay = rawDelay.millis
      stats.set(s"$name.Percentage", ratio)
      stats.set(s"$name.Delay", rawDelay)
    }
  }

  def scheduleTimeout(timer: Timer, deadline: TimeBound)(onTimeout: => Any)(onExceeded: => Any)(
    implicit clock: DeadlineClock): Timeout = {

    timer.scheduleTimeout(_delay) {
      if (deadline.hasTimeLeft) {
        onTimeout
      } else {
        onExceeded
      }
    }
  }

  def incrSample(): Unit = {
    stats.incr(s"$name.Samples")
    samples.increment()
  }

  def incrTimeout(): Unit = {
    stats.incr(s"$name.Timeouts")
    timeouts.increment()
  }

  def close(): Unit =
    heartbeat.stop()
}
