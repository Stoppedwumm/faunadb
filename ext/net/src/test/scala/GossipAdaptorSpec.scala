package fauna.net.test

import fauna.atoms.HostID
import fauna.codex.cbor.CBOR
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.clocks.Clock
import fauna.net.HostDest
import fauna.net.bus.{ MessageBus, SignalID }
import fauna.net.gossip.GossipAdaptor.DupTracker
import fauna.net.gossip.{ GossipAdaptor, Lamport }
import fauna.stats.StatsRecorder
import io.netty.buffer.Unpooled
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import java.util.concurrent.{ Phaser, TimeoutException }
import scala.collection.{ mutable => mut }
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import org.scalatest.PrivateMethodTester

class GossipAdaptorSpec extends Spec with PrivateMethodTester {
  val PingSignal = SignalID(100)
  val GossipSignal = SignalID(101)
  val TestSignal = SignalID(102)
  val MaxBytes = 4096
  val GossipStepWait = 500.millis

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
          .map {
            case (manage, maxBytes, topology) =>
              val gossip =
                new GossipAdaptor(PingSignal, GossipSignal, bus, Clock, maxBytes, {
                  topology().map(id => HostDest(Right(id)))
                }, StatsRecorder.Null)
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

  "DupTrackerSpec" - {

    "track duplicated elements" in {
      val ltime = Lamport()
      val stats = StatsRecorder.Null
      val tracker = DupTracker(3, ltime, stats)

      val buf1 = Unpooled.wrappedBuffer("1".getBytes(UTF_8))
      val buf2 = Unpooled.wrappedBuffer("2".getBytes(UTF_8))
      val buf3 = Unpooled.wrappedBuffer("3".getBytes(UTF_8))
      val buf4 = Unpooled.wrappedBuffer("4".getBytes(UTF_8))
      val buf5 = Unpooled.wrappedBuffer("5".getBytes(UTF_8))

      val lts1 = ltime()
      tracker.seen(lts1, buf1) should equal(false)
      assert(buf1.refCnt == 2, "Tracker must retain buffers it holds")

      val lts2 = ltime.increment()
      tracker.seen(lts2, buf2) should equal(false)

      val lts3 = ltime.increment()
      tracker.seen(lts3, buf3) should equal(false)

      val lts4 = ltime.increment()
      tracker.seen(lts4, buf4) should equal(false)
      assert(buf1.refCnt == 1, "Tracker must release buffers it drops")

      // dropped so we claim we saw it
      tracker.seen(lts1, buf1) should equal(true)

      // these timestamps are still tracked
      tracker.seen(lts2, buf2) should equal(true)
      tracker.seen(lts3, buf3) should equal(true)
      tracker.seen(lts4, buf4) should equal(true)

      // mismatched, but now tracked
      tracker.seen(lts2, buf1) should equal(false)
      // tracked
      tracker.seen(lts2, buf1) should equal(true)

      tracker.seen(ltime.increment(), buf5) should equal(false)

      // counts as seen because too old.
      tracker.seen(lts2, buf4) should equal(true)

      tracker.close()
      assert(buf1.refCnt == 1)
      assert(buf2.refCnt == 1)
      assert(buf3.refCnt == 1)
      assert(buf4.refCnt == 1)
      assert(buf5.refCnt == 1)

      buf1.release
      buf2.release
      buf3.release
      buf4.release
      buf5.release

    }

    /**
      * Double check that we retain / release properly in the loop body.
      * We can't test to destruction as the dup tracker in the receiver prevents
      * us from reusing the barrier in the handler.
      *
      * FIXME: go the whole hog and have n hosts for retrans and force the
      *             hosts to line up
      */
    "transmit maintains reference counts" in {
      withBuses(2) { buses =>
        val buf = Unpooled.compositeBuffer
        CBOR.gatheringEncode[String](buf, "hello")

        val custom = Map(
          (buses(0).hostID, (false, buf.readableBytes, () => Seq(buses(1).hostID))),
          (buses(1).hostID, (true, MaxBytes, () => Seq.empty))
        )

        withGossip(buses, custom) { adaptors =>
          val gossip1 = adaptors(0)
          val gossip2 = adaptors(1)

          val barrier = new java.util.concurrent.CountDownLatch(1)
          gossip2.handler[String](TestSignal) { (_, _) =>
            barrier.countDown; Future.unit
          }

          val clock = Lamport()
          val seg = GossipAdaptor.Segment(clock(), TestSignal, buf)

          gossip1 invokePrivate PrivateMethod[Unit](Symbol("push"))(seg)
          gossip1 invokePrivate PrivateMethod[Unit](Symbol("transmit"))()
          barrier.await(GossipStepWait.length, GossipStepWait.unit)
          assert(seg.buf.refCnt() == 1, "must remain one after single round")
        }
      }
    }

    /**
      * Create artifically constrained topology ring
      * which shows messages being recv'd and rebroadcast
      */
    "propagates gossip" in {
      withBuses(3) { buses =>
        val size = buses.size
        val custom =
          buses.zipWithIndex.map {
            case (bus, i) =>
              val topo = () => Seq(buses((i + 1) % size).hostID)
              (bus.hostID, (true, MaxBytes, topo))
          }.toMap

        withGossip(buses, custom) { adaptors =>
          val parent = new Phaser
          val sets = adaptors.indices map { mut.Set(_) }
          val ctxs =
            sets.zipWithIndex.map {
              case (set, i) =>
                val child = new Phaser(parent, 1)
                adaptors(i).handler[Int](TestSignal) { (_, id) =>
                  set.synchronized { set += id }
                  child.arriveAndAwaitAdvance
                  Future.unit
                }
            }

          adaptors.zipWithIndex foreach {
            case (adaptor, i) =>
              adaptor.scheduledSend(TestSignal, Some(i), 100.millis, Future.never)
          }

          //FIXME: write testing friendly scheduler
          // Let three rounds of gossip occur.
          (0 to 2) foreach { i =>
            val length = GossipStepWait.length
            val unit = GossipStepWait.unit
            parent.awaitAdvanceInterruptibly(i, length, unit)
          }

          ctxs foreach { _.close() }
          sets foreach { set =>
            set should equal (Set(0, 1, 2))
          }
        }
      }
    }

    "track membership changes" in {

      withBuses(2) { buses =>
        val hostSets = Seq.fill[mut.Set[HostID]](2)(mut.Set.empty)
        val custom =
          buses.zipWithIndex.map {
            case (bus, i) =>
              val hosts = hostSets(i)
              val topo = () => hosts.synchronized { hosts.toSeq }
              (bus.hostID, (true, MaxBytes, topo))
          }.toMap

        withGossip(buses, custom) { adaptors =>
          val sets = adaptors.indices map { mut.Set(_) }
          val parent = new Phaser
          val ctxs =
            sets.zipWithIndex.map {
              case (set, i) =>
                val child = new Phaser(parent, 1)
                adaptors(i).handler[Int](TestSignal) { (_, id) =>
                  set.synchronized { set += id }
                  child.arriveAndAwaitAdvance
                  Future.unit
                }
            }

          adaptors.zipWithIndex foreach {
            case (adaptor, i) =>
              adaptor.scheduledSend(TestSignal, Some(i), 100.millis, Future.never)
          }

          val wait = 1.second

          an[TimeoutException] should be thrownBy {
            parent.awaitAdvanceInterruptibly(0, wait.length, wait.unit)
          }
          sets.zipWithIndex foreach { case (set, i) => set should contain only (i) }
          hostSets.zipWithIndex foreach {
            case (set, i) =>
              set.synchronized { set += buses((i + 1) % buses.size).hostID }
          }
          parent.awaitAdvanceInterruptibly(0, wait.length, wait.unit)
          ctxs foreach { _.close() }
          sets foreach { set =>
            set should equal (Set(0, 1))
          }
        }
      }
    }
  }
}
