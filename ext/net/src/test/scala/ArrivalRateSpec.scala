package fauna.net.test

import fauna.atoms._
import fauna.codex.cbor._
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.net._
import fauna.net.bus._
import fauna.net.gossip._
import fauna.stats.StatsRecorder
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class ArrivalRateSpec extends Spec {
  val ArrivalInterval = 500.millis
  val ArrivalRateSignal = SignalID(100)
  val GossipSignal = SignalID(101)
  val MaxBytes = 4096
  val PingSignal = SignalID(102)

  "ArrivalRateSpec" - {
    "encoding" in {
      val alice = AccountID(1)
      val bob = AccountID(2)
      val charlie = AccountID(Long.MaxValue)

      val nobody = ArrivalRateInfo(Map.empty)
      CBOR.encode(nobody).toHexString should equal("[0x00]")

      val lightweight = ArrivalRateInfo(Map(alice -> LimiterStats(1, 1, 1, 1)))

      val light = CBOR.encode(lightweight)
      light.toHexString should equal("[0x01 0x01 0x01 0x01 0x01 0x01]")
      CBOR.decode[ArrivalRateInfo](light) should equal(lightweight)

      val multiple = ArrivalRateInfo(
        Map(
          alice -> LimiterStats(1024, 0, 256, 0),
          bob -> LimiterStats(256, 1024, 0, 0)))

      val multi = CBOR.encode(multiple)
      multi.toHexString should equal(
        "[0x02 " + // length
          "0x01 0x19 0x04 0x00 0x00 0x19 0x01 0x00 0x00 " + // uint16s
          "0x02 0x19 0x01 0x00 0x19 0x04 0x00 0x00 0x00]")
      CBOR.decode[ArrivalRateInfo](multi) should equal(multiple)

      // This encoding isn't practically possible, as permits are
      // Ints, but it's the largest encodable arrivals for a single
      // user.
      val heavyweight = ArrivalRateInfo(
        Map(charlie -> LimiterStats(Long.MaxValue, Long.MaxValue, Long.MaxValue, Long.MaxValue)))

      val heavy = CBOR.encode(heavyweight)
      heavy.toHexString should equal(
        "[0x01 " + // length
          "0x1B 0x7F 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF " + // uint64
          "0x1B 0x7F 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF " +
          "0x1B 0x7F 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF " +
          "0x1B 0x7F 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF " +
          "0x1B 0x7F 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF]")
      CBOR.decode[ArrivalRateInfo](heavy) should equal(heavyweight)
    }

    "propagates" in {
      withBuses(3) { buses =>
        val size = buses.size
        val custom =
          buses.zipWithIndex.map { case (bus, i) =>
            val topo = () => Seq(buses((i + 1) % size).hostID)
            (bus.hostID, (true, MaxBytes, topo))
          }.toMap

        withGossip(buses, custom) { adaptors =>
          val svcs = adaptors map { _ => new MockArrivals() }
          val rates = adaptors.zipWithIndex map { case (adaptor, idx) =>
            new ArrivalRate(ArrivalRateSignal, ArrivalInterval, svcs(idx), adaptor)
          }

          rates foreach { _.start() }

          svcs.zipWithIndex foreach { case (svc, idx) =>
            svc.arrive(AccountID(idx), LimiterStats(idx, idx, idx, idx))
          }

          eventually {
            val accounts = adaptors.indices map { AccountID(_) }
            svcs foreach { _.poll().keys should contain allElementsOf (accounts) }
          }

          rates foreach { _.stop() }
        }
      }
    }

    final class MockArrivals extends ArrivalRateService {
      private[this] val _arrivals = new ConcurrentHashMap[AccountID, LimiterStats]

      def arrive(account: AccountID, stats: LimiterStats): Unit =
        _arrivals.computeIfAbsent(account, { _ => stats })

      def poll(): Map[AccountID, LimiterStats] =
        _arrivals.asScala.toMap

      def register(arrivals: Map[AccountID, LimiterStats]): Unit =
        arrivals foreach { case (id, stats) => arrive(id, stats) }
    }

    def withBuses(count: Int)(f: Seq[MessageBus] => Unit): Unit = {
      val buses = Seq.tabulate[MessageBus](count) { i =>
        val id = HostID(UUID.randomUUID)
        MessageBus("test", id, "localhost", port = 7000 + i)
      }

      buses foreach { me =>
        buses foreach { you =>
          if (me != you) {
            val info = me.hostInfo
            you.registerHostID(info.id, info.address)
          }
        }
      }

      try {
        buses foreach { _.start() }
        f(buses)
      } finally {
        buses foreach { _.stop() }
      }
    }

    def withGossip(
      buses: Seq[MessageBus],
      custom: Map[HostID, (Boolean, Int, () => Seq[HostID])])(
      f: Seq[GossipAdaptor] => Unit): Unit = {
      val ids = buses.map(_.hostID)
      val gossips = buses
        .map { bus =>
          custom
            .get(bus.hostID)
            .map { case (manage, maxBytes, topology) =>
              val gossip =
                new GossipAdaptor(
                  PingSignal,
                  GossipSignal,
                  bus,
                  Clock,
                  maxBytes, {
                    topology().map(id => HostDest(Right(id)))
                  },
                  StatsRecorder.Null)
              if (manage) {
                gossip.start()
              }

              gossip
            }
            .getOrElse {
              val gossip = new GossipAdaptor(
                PingSignal,
                GossipSignal,
                bus,
                Clock,
                MaxBytes,
                ids.filter(id => id != bus.hostID).map(id => HostDest(Right(id))),
                StatsRecorder.Null)
              gossip.start()
              gossip
            }
        }

      try {
        f(gossips)
      } finally {
        gossips foreach { _.stop() }
      }
    }
  }
}
