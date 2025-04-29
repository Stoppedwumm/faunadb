package fauna.tx.test

import fauna.atoms._
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.Timestamp
import fauna.lang.clocks.Clock
import fauna.net.bus._
import java.util.concurrent.atomic.AtomicLong
import fauna.stats.StatsRecorder
import fauna.tx.transaction._
import java.util.concurrent.CountDownLatch
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.Future
import scala.concurrent.duration._
import org.scalatest.time._
import scala.util.Random

import Batch._

class ScopeTxnQueueSpec extends Spec with ScalaFutures {
  // The noTimeout test flakes a lot with the default 20 second timeout.
  // Raise the timeout and see if this reduces flakes. Maybe the problem is a
  // bona fide deadlock...
  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(60, Seconds), interval = Span(100, Millis))

  // stub ctx, only stats and clock are needed.
  val stats = new TxnPipeline.Stats(StatsRecorder.Null)

  def segInfo(start: Long, end: Long) = SegmentInfo(Vector.empty, Closed(Epoch(start), Epoch(end)), SegmentInfo.NoInitRound)

  "ScopeTxnQueue" - {

    "new" in {
      val q = new ScopeTxnQueue(Clock, stats, 100, 100)
      whenReady(q.lastEpochs) { _.toVector should equal (Vector.empty) }
      whenReady(q.poll(0.seconds)) { _ should equal (None) }
    }

    "lastEpochs" in {
      val q = new ScopeTxnQueue(Clock, stats, 100, 100)

      val work = for {
        _ <- q.updateSegments(Map(SegmentID(1) -> segInfo(1, 2), SegmentID(2) -> segInfo(1, 2)), Epoch(1))
        epochs <- q.lastEpochs
      } yield epochs

      whenReady(work) { _.toVector should equal (Vector(SegmentID(1) -> Epoch(0), SegmentID(2) -> Epoch(0))) }

      val more = for {
        _ <- q.add(SegmentID(1), Epoch(0), List(Batch(Epoch(1), ScopeID.RootID, Vector.empty, 0)))
        epochs <- q.lastEpochs
      } yield epochs

      whenReady(more) { _.toVector should equal (Vector(SegmentID(1) -> Epoch(1), SegmentID(2) -> Epoch(0))) }
    }

    "feeds epochs in order" in {
      val q = new ScopeTxnQueue(Clock, stats, 100, 100)

      val origin = HandlerID(HostID.NullID, SignalID.temp)
      val txns = 0 to 9 map { i => Txn(Array(i.toByte), origin, Timestamp.Max) }
      val bad = Txn(Array(100), origin, Timestamp.Max)

      def deal(s: Vector[(SegmentID, Epoch, Batch)]): Future[Option[Seq[Batch]]] = {
        for {
          _ <- Future.traverse(Random.shuffle(s)) { case (id, p, b) => q.add(id, p, Vector(b)) }
          polled <- q.poll(10.millis)
        } yield polled
      }

      val s1 = Vector(
        (SegmentID(1), Epoch(0), Batch(Epoch(1), ScopeID.RootID, Vector(txns(0)), 1)),
        (SegmentID(1), Epoch(1), Batch(Epoch(2), ScopeID.RootID, Vector(txns(1)), 1)),
        (SegmentID(1), Epoch(2), Batch(Epoch(3), ScopeID.RootID, Vector(txns(3)), 1)),
        (SegmentID(1), Epoch(3), Batch(Epoch(4), ScopeID.RootID, Vector(txns(5)), 1)),
        (SegmentID(1), Epoch(4), Batch(Epoch(5), ScopeID.RootID, Vector(bad), 1)),
        (SegmentID(1), Epoch(5), Batch(Epoch(6), ScopeID.RootID, Vector(bad), 1))
      )

      val s2 = Vector(
        (SegmentID(2), Epoch(0), Batch(Epoch(1), ScopeID.RootID, Vector(bad), 1)),
        (SegmentID(2), Epoch(1), Batch(Epoch(2), ScopeID.RootID, Vector(txns(2)), 1)),
        (SegmentID(2), Epoch(2), Batch(Epoch(3), ScopeID.RootID, Vector(txns(4)), 1)),
        (SegmentID(2), Epoch(3), Batch(Epoch(4), ScopeID.RootID, Vector(txns(6)), 1)),
        (SegmentID(2), Epoch(4), Batch(Epoch(5), ScopeID.RootID, Vector(txns(8)), 1)),
        (SegmentID(1), Epoch(5), Batch(Epoch(6), ScopeID.RootID, Vector(bad), 1)),
        (SegmentID(1), Epoch(6), Batch(Epoch(7), ScopeID.RootID, Vector(bad), 1))
      )

      val s3 = Vector(
        (SegmentID(3), Epoch(0), Batch(Epoch(1), ScopeID.RootID, Vector(bad), 1)),
        (SegmentID(3), Epoch(1), Batch(Epoch(2), ScopeID.RootID, Vector(bad), 1)),
        (SegmentID(3), Epoch(2), Batch(Epoch(3), ScopeID.RootID, Vector(bad), 1)),
        (SegmentID(3), Epoch(3), Batch(Epoch(4), ScopeID.RootID, Vector(txns(7)), 1)),
        (SegmentID(3), Epoch(4), Batch(Epoch(5), ScopeID.RootID, Vector(bad), 1)),
      )

      val s4 = Vector(
        (SegmentID(4), Epoch(0), Batch(Epoch(1), ScopeID.RootID, Vector(bad), 1)),
        (SegmentID(4), Epoch(1), Batch(Epoch(2), ScopeID.RootID, Vector(bad), 1)),
        (SegmentID(4), Epoch(2), Batch(Epoch(3), ScopeID.RootID, Vector(bad), 1)),
        (SegmentID(4), Epoch(3), Batch(Epoch(4), ScopeID.RootID, Vector(bad), 1)))

      val updating = q.updateSegments(Map(
        SegmentID(1) -> segInfo(1, 5),
        SegmentID(2) -> segInfo(2, 6),
        SegmentID(3) -> segInfo(4, 5)
      ), Epoch(0))

      whenReady(updating) { _ should equal (()) }
      whenReady(deal(s1)) { _.get.flatMap { _.txns map { _.expr(0) } } should equal (Vector(0)) }
      whenReady(deal(s2)) { _.get.flatMap { _.txns map { _.expr(0) } } should equal (1 to 4) }
      whenReady(deal(s3)) { _.get.flatMap { _.txns map { _.expr(0) } } should equal (5 to 8) }
      whenReady(deal(s1 appendedAll s2 appendedAll s3 appendedAll s4)) { _ should equal (None) }
    }

    "noTimeout doesn't timeout ops" in {
      val q = new ScopeTxnQueue(Clock, stats, 100, 100)

      // Fill the queue with tasks that wait for the countdown.
      // They crowd out the task that we want to timeout.
      val latch = new CountDownLatch(1)
      val maxWait = 10.seconds
      val started = new AtomicLong
      val finished = new AtomicLong

      (0 until 2 * ScopeTxnQueue.QueueDepth) foreach { _ =>
        Future {
          started.getAndIncrement()
          q.work {
            latch.await(maxWait.length, maxWait.unit)
          } onComplete { _ =>
            finished.getAndIncrement()
          }
        }
      }

      def wait(atomic: AtomicLong) = {
        while (atomic.get < ScopeTxnQueue.QueueDepth * 2)
          Thread.sleep(10)
      }

      // Make sure all tasks have started before starting this work.
      wait(started)

      // Submit some work that we want to timeout.
      @volatile var timeouts = 0
      val processing = q.noTimeout {
        q.work(0).transform(identity, { e => timeouts += 1; e })
      }

      // Wait for it to timeout because everything ahead of it is blocked.
      eventually(timeout(15.seconds), interval(150.millis)) {
        (timeouts > 0) should be (true)
      }

      // Unblock the tasks and wait for work to complete.
      latch.countDown()
      wait(finished)
      whenReady(processing) { _ should equal (0) }
    }

  }
}
