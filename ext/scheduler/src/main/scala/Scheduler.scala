package fauna.scheduler

import fauna.exec.NamedForkJoinExecutionContext
import fauna.lang._
import fauna.lang.clocks.{ DeadlineClock, SystemDeadlineClock }
import fauna.stats.StatsRecorder
import java.util.ArrayList
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration._
import scala.concurrent._
import scala.util.control.NonFatal

/**
  * A work scheduler for recursive priorities.
  *
  * Priority for any particular work item is set by that item's priority group and
  * ancestor priority groups.
  *
  * Work is stored in a tree of WorkQueues. A WorkQueue contains a
  * prioritized queue of work items for its PriorityGroup as well as links to
  * WorkQueues for any children of its PriorityGroup. Each WorkQueue tracks
  * its overall size - the number of work items it has plus the sum of all its
  * child queues.
  *
  * WorkItems are inserted via their PriorityGroup's ancestry starting at the
  * root. A WorkQueue's size is not updated until the item has been stored
  * ensuring counters can under-report, but never over-report queued work.
  *
  * Retrieving work is done by providing the root WorkQueue a buffer and max
  * number of items to retrieve. Given these, each WorkQueue in the tree is free
  * to decide how best to fill the buffer - either with its own WorkItems or with
  * those of its children. A WorkQueue will atomically decrement its counter by
  * the total number of WorkItems it will provide (possibly fewer than
  * requested). The total number of items provided will be returned. Given that,
  * the WorkQueue can retrieve more items from a different child if more remain.
  *
  * The scheduler itself uses a permit system and will try to run the maximum
  * number of WorkItems allowed after each WorkItem completes.
 **/

class TooBusyException(group: PriorityGroup, deadline: TimeBound, time: Long)
    extends Exception(
      s"Scheduler for $group is too busy. Current wait time: ${time}ms. TimeBound: $deadline"
    )

class IOScheduler(
  name: String,
  parallelism: Int,
  enableQoS: Boolean = false,
  stats: StatsRecorder = StatsRecorder.Null,
  initialPermits: Int = Int.MaxValue,
  implicit private val clock: DeadlineClock = SystemDeadlineClock
) {
  private[this] val rootWorkGroup =
    PriorityTreeQueueGroup[WorkItem[_]](PriorityGroup.Root.treeGroup)
  private[this] val permits = new AtomicInteger(initialPermits)

  implicit private[this] val ec =
    NamedForkJoinExecutionContext(
      s"IO.Scheduler.$name",
      parallelism = parallelism,
      daemons = true,
      reporter = new FailureReporter {
        def report(thread: Option[Thread], ex: Throwable): Unit =
          ex match {
            case _: TooBusyException =>
              () // Silence rejected work items from QoS.
            case _ => FailureReporter.Default.report(thread, ex)
          }
      }
    )

  object StatNames {
    def n(stat: String) = s"Scheduler.$name.$stat"
    val Permits = n("Permits")
    val Queued = n("Queued")
    val TTS = n("TimeToStart")
    val Latency = n("Latency")
    val WorkAttempted = n("WorkAttempted")
    val WorkDone = n("WorkDone")
    val Put = n("Put")
    val Reject = n("Reject")
    val Take = n("Take")
  }

  StatsRecorder.polling(10 seconds) {
    stats.set(StatNames.Permits, permits.get)
    stats.set(StatNames.Queued, rootWorkGroup.size)
  }

  private final class Tracker {
    private[this] val waitAvg = new WeightedAverage(50)
    def avgWaitTime = waitAvg.value

    def waitTime(millis: Long): Unit = {
      waitAvg.sample(millis)
      stats.timing(StatNames.TTS, millis)
    }

    def runTime(millis: Long): Unit =
      stats.timing(StatNames.Latency, millis)
  }

  private[this] val trackers = new ConcurrentHashMap[PriorityTreeGroup, Tracker]()

  // A single, prioritized unit of work.
  private final class WorkItem[T](
    work: => Future[T],
    tracker: Tracker,
    start: Long
  ) {

    private[this] val underlyingP = Promise[T]()

    def future = underlyingP.future

    def fail(ex: Throwable) =
      underlyingP.failure(ex)

    def start(): Unit = {
      stats.incr(StatNames.WorkDone)
      tracker.waitTime(clock.millis - start)

      val workStart = clock.millis
      val workF =
        try {
          work
        } catch {
          case NonFatal(e) => Future.failed(e)
        }
      workF onComplete { ret =>
        underlyingP.tryComplete(ret)
        tracker.runTime(clock.millis - workStart)
        releaseAndRun()
      }
    }
  }

  def apply[T](group: PriorityGroup, deadline: TimeBound)(
    work: => Future[T]
  ): Future[T] =
    if (enableQoS) {
      applyQoS(group, deadline)(work)
    } else {
      work
    }

  def applyQoS[T](group: PriorityGroup, deadline: TimeBound)(
    work: => Future[T]
  ): Future[T] = {
    val tracker = trackers.computeIfAbsent(group.treeGroup, { _ =>
      new Tracker
    })
    val workItem = new WorkItem(work, tracker, clock.millis)

    stats.incr(StatNames.WorkAttempted)

    // Attempt to run the work immediately. If that fails, queue it according
    // to its priority hierarchy. In case permits become available during
    // queueing, attempt to start another round of work.
    if (rootWorkGroup.size == 0 && getPermit()) {
      workItem.start()

    } else {
      val waitTime = tracker.avgWaitTime

      if (deadline.hasTimeLeft && waitTime < deadline.timeLeft.toMillis) {
        rootWorkGroup.put(group.treeGroup, workItem)
        stats.incr(StatNames.Put)
        runNext()

      } else {
        stats.incr(StatNames.Reject)
        workItem.fail(new TooBusyException(group, deadline, waitTime.toLong))
      }
    }

    workItem.future
  }

  private def releaseAndRun(): Unit = {
    permits.incrementAndGet()
    workRunner.run()
  }

  private[this] val workRunner = new Runnable {

    def run() = {
      // Get as many available permits. Collect as much work as either is available
      // or permits will allow. Any unused permits are immediately put back. Then
      // collected work items are started.
      var available = getPermits()
      if (available > 0) {
        val buf = new ArrayList[WorkItem[_]](available min rootWorkGroup.size)
        val got = rootWorkGroup.drainTo(buf, available)

        require(got <= available)
        require(buf.size == got)
        available -= got

        if (available > 0) {
          permits.addAndGet(available)

          // ensure we attempt to run any work that snuck into the queues between
          // when we drained them and when we put the permits back
          if (rootWorkGroup.size > 0) runNext()
        }

        stats.count(StatNames.Take, buf.size)
        buf forEach { _.start() }
      }
    }
  }

  private def runNext(): Unit =
    ec.execute(workRunner)

  @annotation.tailrec
  private def getPermit(): Boolean = {
    val available = permits.get()
    if (available <= 0) {
      false
    } else if (permits.compareAndSet(available, available - 1)) {
      true
    } else {
      getPermit()
    }
  }

  @annotation.tailrec
  private def getPermits(): Int = {
    val available = permits.get()
    if (available <= 0) {
      0
    } else if (permits.compareAndSet(available, 0)) {
      available
    } else {
      getPermits()
    }
  }
}
