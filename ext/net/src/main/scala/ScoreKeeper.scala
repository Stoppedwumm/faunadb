package fauna.net

import fauna.atoms.HostID
import fauna.exec.Timer
import fauna.lang.{ ReservoirSample, Service }
import fauna.lang.clocks._
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.stats.StatsRecorder
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration._
import scala.util.Random
import scala.util.control.NonFatal

object ScoreKeeper {
  // XXX: C* update interval
  val UpdateInterval = 100.millis

  /**
    * A percentage of the sampling window, before which scores are not
    * computed. This allows time for collecting a representative
    * sample after reboot/reset.
    */
  val FullnessThreshold = 0.1
}

/**
  * A ScoreKeeper receives round-trip times, which are used to
  * maintain a dynamic score for each peer in a cluster. The resulting
  * score may be used to order hosts by preference for e.g. planning
  * remote reads.
  *
  * This process is analagous to the dynamic snitch in Cassandra.
  */
final class ScoreKeeper(
  name: String,
  window: Int = 100,
  decay: Double = 0.75,
  clock: DeadlineClock = SystemDeadlineClock,
  random: Random = Random,
  stats: StatsRecorder = StatsRecorder.Null)
    extends Service
    with ExceptionLogging {

  import ScoreKeeper._

  private[this] val fullnessLimit = window * FullnessThreshold

  private[this] val log = getLogger

  private[this] val isShutdown = new AtomicBoolean(true)

  private[this] val samples = new ConcurrentHashMap[HostID, ReservoirSample]

  @volatile private[this] var scores = Map.empty[HostID, Double]

  private[this] val GoodIntervalStatname = s"ScoreKeeper.$name.GoodInterval"
  private[this] val BadIntervalStatname = s"ScoreKeeper.$name.BadInterval"

  def start(): Unit =
    if (isShutdown.compareAndSet(true, false)) {
      Timer.Global.scheduleRepeatedly(UpdateInterval, isRunning) {
        try {
          computeScores()
        } catch {
          case NonFatal(e) => logException(e)
        }
      }
    }

  def isRunning: Boolean = !isShutdown.get

  def stop(graceful: Boolean): Unit =
    isShutdown.set(true)

  /**
    * Returns the score for the provided host (lower is
    * better). Returns 0.0 if the host has not yet been scored.
    */
  def score(id: HostID): Double =
    scores.getOrElse(id, { receiveTiming(id, 0); 0.0 })

  /**
    * Registers observed round-trip latency of a message to `host` in
    * milliseconds.
    */
  def receiveTiming(host: HostID, latency: Long): Unit = {
    val avg = samples.computeIfAbsent(host, { _ =>
      new ReservoirSample(window, decay, clock)
    })

    if (latency >= 0) {
      avg.sample(latency)
    }
  }

  /**
    * (Re-)Computes host preference scores. This process is expensive;
    * call periodically.
    */
  def computeScores(): Unit = {
    var max = 1L

    val b = Map.newBuilder[HostID, Double]

    var snaps = Seq.empty[(HostID, ReservoirSample.Snapshot)]

    samples forEach { (host, sample) =>
      val snap = sample.snapshot()

      val median = snap.median
      // prevent inadequate sampling from pulling the max up
      if (snap.size >= fullnessLimit && median > max) {
        max = median
      }

      log.trace(
        s"ScoreKeeper ($name): $host has median latency ${median}ms " +
          s"(N: ${snap.size} stddev: ${snap.stddev} MAD: ${snap.medianAbsoluteDeviation}.")
      snaps :+= ((host, snap))
    }

    var bestScore = 1.0
    var goodInterval = true

    snaps foreach {
      case (host, snap) =>
        // if the snapshot doesn't have enough samples, force sampling
        // by scoring low
        val score = if (snap.size < fullnessLimit) {
          0.0
        } else {
          snap.median.toDouble / max
        }

        // provide an opportunity for the slowest hosts to improve
        val adj = if (score == 1.0) {
          1 - random.nextDouble()
        } else {
          score
        }

        if (adj < bestScore) {
          bestScore = adj
          goodInterval = (snap.size >= fullnessLimit) && (adj == score)
        }

        log.trace(s"ScoreKeeper ($name): $host has score $adj ($score)")
        b += host -> adj
    }

    if (goodInterval) {
      stats.incr(GoodIntervalStatname)
    } else {
      stats.incr(BadIntervalStatname)
    }

    scores = b.result()
  }

  /**
    * Resets measured latencies for the provided host, giving a bad
    * host a chance to recover.
    */
  def reset(host: HostID): Unit = {
    log.trace(s"ScoreKeeper ($name): $host score reset.")
    samples.remove(host)
  }

  /**
    * Resets all measured latencies, giving bad hosts a chance to
    * recover.
    */
  def reset(): Unit = {
    log.trace(s"ScoreKeeper ($name): all scores reset.")
    samples.clear()
  }

}
