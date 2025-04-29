package fauna.net.gossip

import fauna.atoms.HostID
import fauna.codex.cbor.CBOR
import fauna.exec.Timer
import fauna.lang.WeightedMoments
import fauna.lang.clocks.DelayTrackingClock
import fauna.lang.syntax.future._
import fauna.net.HostInfo
import fauna.net.bus.SignalID
import fauna.stats.StatsRecorder
import org.apache.commons.math3.distribution.NormalDistribution
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration.{ DurationInt, DurationLong, FiniteDuration }
import scala.util.Random

object ReadClockDelay {

  object Sample {
    implicit val codec: CBOR.Codec[Sample] = CBOR.TupleCodec
  }

  case class Sample(host: HostID, meanMicros: Double, varianceMicros: Double)
}

/**
  * Gossip service that
  * 1. Propagates local read clock delay via gossip
  * 2. Maintains a clusterwide sampled read clock delay at a configured percentile
  * 3. Updates a listener with this delay.
  */
class ReadClockDelay(
  hostID: HostID,
  signalID: SignalID,
  gossip: GossipAdaptor,
  isLocal: HostID => Boolean,
  percentile: Double,
  clock: DelayTrackingClock,
  stats: StatsRecorder
)(listener: FiniteDuration => Unit) extends AutoCloseable {

  import ReadClockDelay._

  private val SendRate = 50.millis
  private val UpdateRate = 500.millis
  private val timer = Timer.Global
  // This "alpha" has been chosen because it seems to work. The goal is to
  // evolve slowly enough that we are stable without lagging so hard we are
  // irrelevant
  private val DelayDecay = 0.01
  private val avg = new WeightedMoments(DelayDecay)

  @volatile
  private var running = true


  //XXX:: calculate z score from p value, also does this make sense.. if readclocks are power law
  // then centrl limit says we go to stable distro with heavier tails. Also can I apply central limit?
  // are our samples iid / meet other conditions?
  private val ZScore = (new NormalDistribution).inverseCumulativeProbability(percentile / 100)
  private val SampleSize = 1000
  private val samples = Array.fill[Double](SampleSize)(0)
  private var sampled = 0

  private val stopit: Promise[Unit] = Promise[Unit]()

  @volatile
  private var _delay: FiniteDuration = 0.micros

  def delay = _delay

  /**
    * Handler inserts cluster wide samples of read clock delay into a reservoir that
    * can overflow, but will retain samples over the interval with uniform
    * probability.
    */
  private val ctx = gossip.handler[Sample](signalID)(receiveSample)

  /**
    * Propagate weighted average local delay throughout the cluster.
    */
  gossip.scheduledSend(signalID, sendSample(), SendRate, stopit.future)

  /**
    * (Re-)Compute the maximum weighted average delay observed via gossip.
    */
  timer.scheduleRepeatedly(UpdateRate, running) { computeDelay() }

  def sendSample(): Option[Sample] = {
    avg.sample(clock.delay)
    val sample = Sample(hostID, avg.mean, avg.variance)
    stats.set("WriteAck.Local.Wait", waitMillis(sample))
    Some(sample)
  }

  /**
    * Restrict samples to local replica. We want to offer soft strict
    * serializability to clients that remain in the same replica, but a
    * global guarantee is too costly: diverse non-log replicas adversely
    * affect cluster write latency.
    */
  def receiveSample(host: HostInfo, sample: Sample): Future[Unit] = {
    val oldCondition = sample.host == HostID.NullID && isLocal(host.id)
    if (oldCondition || isLocal(sample.host)) {
      val estimate = waitMicros(sample)
      samples.synchronized {
        if (sampled < SampleSize) {
          samples(sampled) = estimate
        } else {
          val idx = Random.nextInt(sampled)
          if (idx < SampleSize) {
            samples(idx) = estimate
          }
        }
        sampled += 1
      }
    }
    Future.unit
  }

  def computeDelay(): Unit = {
    samples.synchronized {
      if (sampled != 0) {
        val estimate = samples.iterator.take(sampled).max(Ordering.Double.TotalOrdering)
        sampled = 0
        _delay = math.round(estimate).micros
      }
    }
    stats.set("WriteAck.Replica.Wait", _delay.toMillis)
    listener(_delay)
  }

  def close(): Unit = {
    ctx.synchronized {
      if (running) {
        running = false
        ctx.close()
        stopit.setDone()
      }
    }
  }

  private def waitMicros(sample: Sample): Double =
    sample.meanMicros + ZScore * math.sqrt(sample.varianceMicros)

  private def waitMillis(sample: Sample): Double =
    waitMicros(sample) / 1000
}
