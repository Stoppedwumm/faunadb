package fauna.repo.query

import fauna.exec.ImmediateExecutionContext
import fauna.lang.Local
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.LinkedList
import scala.concurrent.{ Future, Promise }
import scala.util.Try
import QueryEvalContext._

trait QueryEvalWait { self: QueryEvalContext =>

  private def maxConcurrentWaiters = repo.queryMaxConcurrentReads

  private[this] var waitersPending = 0
  private[this] val queuedWaiters = new LinkedList[() => Unit]()
  private[this] val waitersCompleted = new ConcurrentLinkedQueue[Wait]()
  @volatile private[this] var wakeP = Promise[Unit]()

  protected def wakeFut = wakeP.future
  protected def hasPendingWaiters = waitersPending > 0

  protected def mapWait(w: Wait)(f: EvalStep => EvalStep): Unit = {
    val thnk = w.thunk
    w.thunk = () => f(thnk())
  }

  protected def waitFuture[A](fut: => Future[A])(f: Try[A] => EvalStep): Wait = {
    @volatile var res: Try[A] = null
    val w = Wait(() => f(res), Local.save())

    def thnk() = {
      Local.restore(w.locals)
      registerWaiter(w)
      implicit val ec = ImmediateExecutionContext
      fut.onComplete { t =>
        res = t
        wakeWaiter(w)
      }
    }

    if (waitersPending >= maxConcurrentWaiters) {
      queuedWaiters.add(thnk _)
    } else {
      thnk()
    }
    w
  }

  protected def waitImmediate(w: Wait): Wait = {
    registerWaiter(w)
    wakeWaiter(w)
    w
  }

  @annotation.nowarn("cat=unused")
  private def registerWaiter(w: Wait): Unit = {
    waitersPending += 1
  }

  private def wakeWaiter(w: Wait): Unit = {
    waitersCompleted.offer(w)
    wakeP.trySuccess(())
  }

  private def completeWaiter(w: Wait): Unit = {
    waitersPending -= 1

    while (!queuedWaiters.isEmpty && waitersPending < maxConcurrentWaiters) {
      val thnk = queuedWaiters.poll()
      thnk()
    }

    Local.restore(w.locals)
    val res = w.thunk()
    w.thunk = () => res
  }

  final protected def stepWaiters(): Unit =
    if (hasPendingWaiters) {
      var w = waitersCompleted.poll()
      while (w ne null) {
        completeWaiter(w)
        w = if (stepCount < repo.queryEvalStepsPerYield) {
          waitersCompleted.poll()
        } else {
          null
        }
      }

      wakeP = Promise[Unit]()

      // If there are still outstanding branches, then it is possible resetting
      // the waker will race with threads trying to complete it. If so, check
      // state and pre-trigger the wake promise so that we immediately get
      // scheduled to process again.
      if (hasPendingWaiters && !waitersCompleted.isEmpty) {
        wakeP.trySuccess(())
      }
    }
}
