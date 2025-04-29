package fauna.exec

import fauna.lang.NamedPoolThreadFactory
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import io.netty.util.{ HashedWheelTimer, Timer => NTimer, TimerTask, Timeout }
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.concurrent.{ Future, Promise }
import scala.util.control.NonFatal

object Timer {
  // Timeouts longer than one day are sus.
  val LongTimeoutThreshold = 1.day

  lazy val Global = new Timer

  private[Timer] final class NeverTimeout(_timer: NTimer, task: TimerTask)
      extends Timeout {
    @volatile private[this] var cancelled = false

    def timer() = _timer
    def task() = task
    def isExpired() = false
    def isCancelled() = cancelled

    def cancel() = synchronized {
      val prev = cancelled
      cancelled = true
      !prev
    }
  }

  private final class NoWaitTimeout(_timer: NTimer, task: TimerTask) extends Timeout {
    def timer() = _timer
    def task() = task
    def isExpired() = false
    def isCancelled() = false
    def cancel() = false
  }

  private[Timer] def NoWait(timer: NTimer, task: TimerTask): Timeout = {
    val t = new NoWaitTimeout(timer, task)
    task.run(t)
    t
  }
}

class Timer(tickMillis: Long = 50, ticksPerWheel: Int = 512) extends ExceptionLogging {

  private val timer = {
    val tf = new NamedPoolThreadFactory("fauna-timer", true)
    new HashedWheelTimer(tf, tickMillis, TimeUnit.MILLISECONDS, ticksPerWheel)
  }

  timer.start()

  private final class RunnerTask(f: => Any) extends TimerTask {
    def run(timeout: Timeout) =
      try {
        f
      } catch {
        case NonFatal(e) =>
          logException(e)
          throw e
      }
  }

  def scheduleTimeout(d: Duration)(f: => Any): Timeout =
    d match {
      case Duration(l, _) if l <= 0 => Timer.NoWait(timer, new RunnerTask(f))
      case Duration(l, u)           => {
        // Log an exception for absurdly long timeouts
        // because they cause memory leaks.
        if (d > Timer.LongTimeoutThreshold)
          logException(new Exception(s"long timeout scheduled: ${d.toCoarsest}"))
        timer.newTimeout(new RunnerTask(f), l, u)
      }
      case _                        => new Timer.NeverTimeout(timer, new RunnerTask(f))
    }

  /** Schedule a function to be invoked repeatedly while a predicate holds.
    * If initialDelay is set to false (the default value), the predicate is
    * immediately evaluated, and if it evaluates to true, the function is
    * immediately executed.
    * If initialDelay is set to true, the checking of the predicate and function
    * evaluation is scheduled after the specified delay.
    * This is repeated while the predicate evaluates to true.
    * @param delay the delay between end of an invocation and start of next one.
    * @param p a predicate that is evaluated before every function invocation. If it
    *          evaluates to true, the function is invoked and its next invocation is
    *          scheduled. If it evaluates to false, repetitive invocation ends.
    * @param f the function to invoke.
    * @param initialDelay whether or not the first invocation of the method should be delayed.
    *                     When set to true, the first invocation of the predicate/function will be delayed.
    *                     When set to false, the first invocation of the predicate/function will be executed immediately.
    */
  def scheduleRepeatedly(
    delay: Duration,
    p: => Boolean,
    initialDelay: Boolean = false)(f: => Any): Unit = {
    def f0(): Unit =
      if (p) {
        f
        scheduleTimeout(delay)(f0())
      }

    if (!initialDelay) {
      f0()
    } else {
      scheduleTimeout(delay)(f0())
    }
  }

  def completeAfter[T](promise: Promise[T], duration: Duration)(f: => Future[T]): Option[Timeout] =
    if (!promise.isCompleted && duration.isFinite) {
      val timeout = scheduleTimeout(duration)(promise.completeWith(try f catch { case NonFatal(e) => Future.failed(e) }))
      promise.future.ensure(timeout.cancel())(ImmediateExecutionContext)
      Some(timeout)
    } else {
      None
    }

  def completedAfter[T](future: Future[T], duration: Duration)(f: => Future[T]): Future[T] =
    if (!future.isCompleted && duration.isFinite) {
      val p = Promise[T]()
      p.completeWith(future)
      completeAfter(p, duration)(f)
      p.future
    } else {
      future
    }

  def timeoutPromise[T](promise: Promise[T], duration: Duration, ex: => Throwable): Option[Timeout] =
    completeAfter(promise, duration)(Future.failed(ex))

  def timeoutFuture[T](future: Future[T], duration: Duration, ex: => Throwable): Future[T] =
    completedAfter(future, duration)(Future.failed(ex))

  def delay[T](duration: Duration)(f: => Future[T]): Future[T] =
    completedAfter(Future.never, duration)(f)
}
