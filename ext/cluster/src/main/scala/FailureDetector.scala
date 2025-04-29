package fauna.cluster

import fauna.atoms.HostID
import fauna.lang.{ RunningAverage, Timestamp }
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.stats.StatsRecorder
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

object FailureDetector {
  // The expected (mean) frequency of samples recorded in the FD. Used
  // to schedule the heartbeat interval in the FailureService.
  //
  // XXX: C* check interval
  val CheckInterval = 1.second

  // The maximum number of samples to keep for computing the mean
  // inter-arrival time of samples.
  //
  // XXX: C* sample size
  val SampleSize = 1000

  // Maximum allowable window of missing samples due to 'normal'
  // events like GC, network connection flapping, &c.
  //
  // XXX: C* pause threshold
  val MaxLocalPause = 5.seconds

  // A high threshold for phi may incorrectly suspect a host is down,
  // but will react quickly to changes in liveness.
  //
  // see `phi()` more information on how this is computed.
  //
  // XXX: The constant 8.0 is C*'s default.
  val PhiThreshold = 8.0 / (1.0 / Math.log(10.0)) // 18.420...
}

/**
  * A failure detector based on "The Phi Accrual Failure Detector" by
  * Hayashibara.
  */
case class FailureDetector(
  membership: Membership,
  stats: StatsRecorder,
  clock: Clock = Clock) {

  import FailureDetector._

  private[this] val log = getLogger
  private[this] val samples = new ConcurrentHashMap[HostID, Arrivals]
  private[this] val dead = new ConcurrentHashMap[HostID, Timestamp]
  private[this] val subscribers = ListBuffer[HostID => Unit]()

  private[this] var lastInterpretation = clock.time
  private[this] var lastPause = Timestamp.Epoch

  /**
    * Returns whether we believe `host` to be unreachable or not.
    */
  def isAlive(host: HostID) =
    if (host == membership.self) {
      true
    } else {
      if (membership.hosts contains host) {
        samples.containsKey(host) && !dead.containsKey(host)
      } else {
        // We inherited this semantics from C*. (see CASSANDRA-1463)
        false
      }
    }

  def subscribe(f: HostID => Unit): Unit = synchronized {
    subscribers += f
  }

  /**
    * Marks an event indicating to the FD that `host` is still
    * reachable.
    *
    * This method is concurrent with `interpret`: if `mark` is
    * published first, `interpret` will observe the new arrival
    * window. If `mark` happens after `interpret`, the new arrival
    * window will be safely observed on the next iteration of
    * `interpret`.
    */
  def mark(host: HostID): Unit =
    samples.compute(host, { (host, arrivals) =>
      if (arrivals eq null) {
        val arr = new Arrivals(host, SampleSize)
        arr.add(clock.time.millis)
      } else {
        arrivals.add(clock.time.millis)
      }
    })

  /**
    * Removes all samples for `host`, and removes it from the dead
    * list.
    *
    * This method is exclusive with `interpret`, but is concurrent
    * with `mark`.
    */
  def remove(host: HostID): Unit =
    synchronized {
      dead.remove(host)
      samples.remove(host)
    }

  /**
    * Interprets the accrued samples for `host`, and marking the host
    * unreachable if it seems to have failed.
    *
    * This method is concurrent with `mark`, but must be exclusive
    * with `remove`. If `remove` and `interpret` interleave, the dead
    * list and arrival windows for a host may diverge.
    */
  def interpret(host: HostID): Unit =
    synchronized {
      val now = clock.time
      val diff = now.millis - lastInterpretation.millis
      lastInterpretation = now

      if (diff > MaxLocalPause.toMillis) {
        log.warn(s"Not marking nodes down due to local pause ${diff}ms > $MaxLocalPause")
        lastPause = now
      } else if (now.millis - lastPause.millis < MaxLocalPause.toMillis) {
        log.info(s"Not marking nodes down due to recent pause ${now.millis - lastPause.millis}ms ago.")
      } else {
        Option(samples.get(host)) foreach { arrivals =>
          val phi = arrivals.phi(now.millis)

          log.debug(s"Phi $phi for $host (mean latency ${arrivals.mean}ms).")
          if (phi > PhiThreshold) {
            stats.incr("FailureDetection.Unreachable")
            log.warn(s"$host is unreachable ($phi).")
            dead.put(host, now)
          } else if (dead.containsKey(host)) {
            stats.incr("FailureDetection.Recovery")
            log.info(s"$host has recovered ($phi).")
            dead.remove(host)
            subscribers foreach { _.apply(host) }
          }
        }
      }
    }
}

private[cluster] class Arrivals(host: HostID, samples: Int) {
  // XXX: C* default
  val MaxInterval = (2 * FailureDetector.CheckInterval).toMillis

  private[this] val log = getLogger
  private[this] val average = new RunningAverage(samples)
  private[this] var last = 0L

  def add(value: Long): Arrivals =
    synchronized {
      require(last >= 0)
      if (last > 0) {
        val diff = value - last
        if (diff <= MaxInterval) {
          average.sample(diff)
        } else {
          log.debug(s"Ignoring interval $value for $host (${diff}ms > ${MaxInterval}ms).")
        }
      } else {
        // we use a very large initial interval, since the mean
        // depends on cluster size, and erring low would cause
        // flapping.
        average.sample(MaxInterval)
      }
      last = value
      this
    }

  // This method of computing phi is based on the observation (due to
  // the original authors of Cassandra) that the true value of phi is
  // approximated by an exponential distribution, not Gaussian as in
  // the original Hayashibara paper. The original paper assumed
  // regular messages with typical jitter following a normal
  // distribution. Random message intervals (such as gossip) make up a
  // Poisson process. Here is the 'new math':
  //
  // L = 1 / mean // the rate of sample observations; mean is the arith. mean.
  // F = 1 - e^(-Lt) // the CDF of an exp. distribution
  // P_later = 1 - F(t) // the prob. of failure since time t
  //         = e^(-t/mean) // simplification of the above
  //
  // phi(t) = -log10(P_later(t))
  //        = -log10(e^(-t/mean))
  //        = (t/mean) / log(10)
  //        = (1 / log(10)) * t/mean
  //
  // C* defaults the phi threshold to 8.0; we have maintained
  // continuity with them.
  def phi(now: Long): Double =
    synchronized {
      if (average.value > 0.0 && last > 0) {
        (now - last) / average.value
      } else {
        log.debug("No samples have arrived. Assuming everything is OK.")
        0.0
      }
    }

  def mean: Double = average.value
}
