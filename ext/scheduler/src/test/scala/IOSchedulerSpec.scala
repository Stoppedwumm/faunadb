package fauna.scheduler.test

import fauna.atoms._
import fauna.lang.{ PriorityTreeGroup, TimeBound }
import fauna.lang.clocks.DeadlineClock
import fauna.lang.syntax._
import fauna.scheduler._
import fauna.stats.StatsRecorder
import java.util.concurrent.CountDownLatch
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ blocking, Await, Future }

class IOSchedulerSpec extends Spec {
  val AwaitTimeout = 1 second
  val RootDB = DatabaseID(0)

  implicit class CountDownLatchOps(l: CountDownLatch) {

    def await(d: Duration): Boolean =
      l.await(d.length, d.unit)

    def toFuture: Future[Unit] = Future { blocking { l.await() } }
  }

  class TestClock extends DeadlineClock {
    @volatile private[this] var _nanos: Long = 0

    def tick(amount: Duration): Unit = synchronized {
      _nanos += amount.toNanos
    }

    def nanos = _nanos
  }

  val successF = Future.successful(())

  "IOScheduler" - {
    "limits concurrency" in {
      val max = 2
      val forever = new CountDownLatch(1)
      val latch = new CountDownLatch(max + 1)
      val scheduler = new IOScheduler("", 2, enableQoS = true, initialPermits = max)

      (0 until (max + 1)) foreach { _ =>
        scheduler(PriorityGroup.Default, TimeBound.Max) {
          latch.countDown()
          forever.toFuture
        }
      }

      try {
        latch.await(AwaitTimeout) should be(false)
        latch.getCount should be(1)
      } finally forever.countDown()
    }

    "is accurate during concurrent access" in {
      var counter = 0
      val prioList = List.newBuilder[PriorityTreeGroup]
      def mkTree(width: Int, depth: Int, ancestors: List[PriorityTreeGroup]): Unit =
        (1 until width) foreach { w =>
          val group = PriorityTreeGroup(counter, ancestors, 1)
          prioList += group
          counter += 1
          (1 until depth) foreach { d =>
            mkTree(w, d - 1, ancestors :+ group)
          }
        }

      mkTree(2, 5, Nil)
      val prios = prioList.result() map { PriorityGroup(GlobalKeyID.MinValue, _) } toSet

      val threads = 10
      val work = 5000

      val scheduler = new IOScheduler("", 2, enableQoS = true, initialPermits = 200)
      val latch = new CountDownLatch(threads * work * prios.size)

      (0 until threads) foreach { _ =>
        Future {
          for {
            _ <- (0 until work)
            p <- prios
          } {
            scheduler(p, TimeBound.Max) {
              latch.countDown()
              successF
            }
          }
        }
      }

      latch.await(AwaitTimeout * 100) should be(true)
    }

    "fairly schedules work according to PriorityGroup" in {
      val forever = new CountDownLatch(1)
      val scheduler = new IOScheduler("", 2, enableQoS = true, initialPermits = 2)

      val p1 = new CountDownLatch(1)
      val p2 = new CountDownLatch(1)

      scheduler(PriorityGroup.Default, TimeBound.Max)(p1.toFuture)
      scheduler(PriorityGroup.Default, TimeBound.Max)(p2.toFuture)

      val prio1 = PriorityGroup(
        GlobalKeyID.MinValue,
        PriorityTreeGroup((ScopeID.RootID, DatabaseID(1)), Nil, 1)
      )
      val prio2 = PriorityGroup(
        GlobalKeyID.MinValue,
        PriorityTreeGroup((ScopeID.RootID, DatabaseID(2)), Nil, 1)
      )

      val wrk1 = new CountDownLatch(2)
      val wrk2 = new CountDownLatch(1)

      scheduler(prio1, TimeBound.Max) {
        wrk1.countDown()
        forever.toFuture
      }
      scheduler(prio1, TimeBound.Max) {
        wrk1.countDown()
        forever.toFuture
      }
      scheduler(prio2, TimeBound.Max) {
        wrk2.countDown()
        forever.toFuture
      }

      try {
        p1.countDown()
        wrk1.await(AwaitTimeout) should be(false)
        wrk1.getCount should equal(1)

        p2.countDown()
        wrk1.await(AwaitTimeout) should be(false)
        wrk1.getCount should equal(1)

        wrk2.await(AwaitTimeout) should be(true)
      } finally {
        forever.countDown()
      }
    }

    "schedulers high priority work first" in {
      val HighPri = PriorityGroup(
        GlobalKeyID.MinValue,
        PriorityTreeGroup((ScopeID.RootID, RootDB), Nil, Priority.MaxValue)
      )

      val forever = new CountDownLatch(1)
      val scheduler = new IOScheduler("", 2, enableQoS = true, initialPermits = 2)

      val p1 = new CountDownLatch(1)
      val p2 = new CountDownLatch(1)

      scheduler(HighPri, TimeBound.Max)(p1.toFuture)
      scheduler(HighPri, TimeBound.Max)(p2.toFuture)

      val wrk1 = new CountDownLatch(2)
      val wrk2 = new CountDownLatch(1)

      scheduler(HighPri, TimeBound.Max) {
        wrk1.countDown()
        forever.toFuture
      }
      scheduler(HighPri, TimeBound.Max) {
        wrk1.countDown()
        forever.toFuture
      }
      scheduler(PriorityGroup.Background, TimeBound.Max) {
        wrk2.countDown()
        forever.toFuture
      }

      p1.countDown()
      p2.countDown()

      try {
        wrk1.await(AwaitTimeout) should be(true)
        wrk2.await(AwaitTimeout) should be(false)
      } finally forever.countDown()
    }

    "continuously processes a single queue if it's the only one with work" in {
      val count = new CountDownLatch(1)
      val scheduler = new IOScheduler("", 2, enableQoS = true, initialPermits = 1)
      val forever = new CountDownLatch(1)
      scheduler(PriorityGroup.Default, TimeBound.Max)(forever.toFuture)

      (0 until 5) foreach { _ =>
        scheduler(PriorityGroup.Default, TimeBound.Max) {
          count.countDown(); successF
        }
      }
      forever.countDown()

      count.await(AwaitTimeout) should equal(true)
    }

    "will reject work if the wait is too long" in {
      implicit val clock = new TestClock
      def deadline = 1.millisecond.bound

      val scheduler = new IOScheduler("", 2, true, StatsRecorder.Null, 1, clock)
      val p1 = new CountDownLatch(1)
      val p2 = new CountDownLatch(1)
      scheduler(PriorityGroup.Default, deadline)(p1.toFuture)

      // Pump the scheduler in order to drive the wait time up, so
      // that rejection actually happens.
      (0 until 100) foreach { _ =>
        scheduler(PriorityGroup.Default, deadline)(successF)
      }
      val f1 = scheduler(PriorityGroup.Default, deadline)(successF)
      scheduler(PriorityGroup.Default, deadline)(p2.toFuture)

      clock.tick(100 seconds)

      p1.countDown()
      Await.ready(f1, AwaitTimeout)

      var ranWork: Boolean = false
      val work = scheduler(PriorityGroup.Default, deadline) {
        Future { ranWork = true }
      }

      Await.ready(work, AwaitTimeout * 20)
      work.value.get.isFailure should equal(true)

      p2.countDown()
      ranWork should equal(false)
    }

    "will not get stuck nacking forever" in {
      implicit val clock = new TestClock
      def deadline = 1.millisecond.bound

      val scheduler = new IOScheduler("", 2, enableQoS = true, initialPermits = 1, clock = clock)
      val p1 = new CountDownLatch(1)
      scheduler(PriorityGroup.Default, deadline)(p1.toFuture)

      // drive the wait time up
      clock.tick(2 milliseconds)
      p1.countDown()

      // drive the wait time back down (some of these will have been nack'd
      val fs = (0 until 100) map { _ =>
        scheduler(PriorityGroup.Default, deadline)(successF)
      }
      Await.ready(Future.sequence(fs), AwaitTimeout)

      // fill the queue
      val p2 = new CountDownLatch(1)
      scheduler(PriorityGroup.Default, deadline)(p2.toFuture)

      // insert the test task
      var ranWork: Boolean = false
      val work = scheduler(PriorityGroup.Default, deadline) {
        Future { ranWork = true }
      }

      ranWork should equal(false)
      work.value should be(None)

      p2.countDown()

      Await.result(work, AwaitTimeout)
      ranWork should equal(true)
    }

    "protects itself from bad thunks" in {
      val scheduler = new IOScheduler("", 2, enableQoS = true, initialPermits = 1)
      val work = scheduler(PriorityGroup.Default, TimeBound.Max) {
        throw new Exception("boom")
      }
      Await.ready(work, AwaitTimeout)
      work.value.get.isFailure should equal(true)
    }
  }
}
