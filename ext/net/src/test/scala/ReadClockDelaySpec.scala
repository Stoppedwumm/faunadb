package fauna.net.test

import fauna.atoms._
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.clocks._
import fauna.net._
import fauna.net.bus._
import fauna.net.gossip._
import fauna.stats.StatsRecorder
import scala.concurrent.duration._

class ReadClockDelaySpec extends Spec {
  val MaxBytes = 4096

  val id = HostID.randomID
  val signal = SignalID.temp
  val bus = MessageBus("test", id, "localhost", port = 7000)
  val gossip = new GossipAdaptor(
    SignalID.temp,
    SignalID.temp,
    bus,
    Clock,
    MaxBytes,
    Seq(HostDest(Right(id))),
    StatsRecorder.Null)

  "ReadClockDelay" - {
    "non-local samples do not affect delay" in {
      val remote = HostInfo(HostID.randomID, HostAddress("localhost", 7001))
      val local = HostInfo(HostID.randomID, HostAddress("localhost", 7002))

      val clock = new DirectDelayTrackingClock(Clock, 30.seconds)
      val rcd = new ReadClockDelay(id, signal, gossip, Set(id, local.id), 0.75, clock, StatsRecorder.Null)({ _ => () })

      rcd.delay should equal (0.micros)

      rcd.receiveSample(remote, ReadClockDelay.Sample(remote.id, 100.0, 10.0))
      rcd.computeDelay()

      rcd.delay should equal (0.micros)

      rcd.receiveSample(local, ReadClockDelay.Sample(local.id, 100.0, 10.0))
      rcd.computeDelay()

      rcd.delay should equal (92.micros)
    }
  }
}
