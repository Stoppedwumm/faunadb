package fauna.exec

import scala.concurrent.{ ExecutionContext, Future, Promise }
import java.util.concurrent.atomic.{ AtomicInteger, AtomicMarkableReference }
import scala.util.control.NonFatal

/**
  * A promise-based latch that supports 1 or more waiters.
  */
final class AsyncLatch[T] extends AtomicMarkableReference[Promise[T]](Promise[T](), false) {

  /**
    * Returns a future that will be completed when signal is
    * called. If the latch had previously been signaled with no pending
    * waiters, this will reset the latch and return a completed
    * future.
    *
    * If the signal has not been called, this and subsequent calls to
    * await will return a future that will be completed by the next
    * call to signal.
    */
  def await: Future[T] = {
    val p = getReference

    attemptMark(p, true)

    if (p.isCompleted) {
      compareAndSet(p, Promise[T](), true, false)
    }

    p.future
  }

  /**
    * Notifies any waiters with a value and resets the latch. If there
    * are no waiters, the next call to await will immediately succeed.
    */
  def signal(value: T): Boolean = {
    val p = getReference
    val rv = p.trySuccess(value)

    // reset if there were any waiters
    compareAndSet(p, Promise[T](), true, false)
    rv
  }
}

final class AsyncEventSource {
  private[this] val latch = new AsyncLatch[Unit]
  @volatile private[this] var running = true

  def subscribe(cb: => Future[Boolean])(implicit ec: ExecutionContext): Future[Unit]= {
    def callAndWait: Future[Unit] = {
      if (running) {
        val cbfut = try {
          cb
        } catch {
          case NonFatal(e) => Future.failed(e)
        }
        cbfut flatMap {
          case true  => latch.await flatMap { _ => callAndWait }
          case false => Future.unit
        }
      } else {
        Future.unit
      }
    }
    callAndWait
  }

  def signal(): Unit =
    latch.signal(())

  def stop(): Unit = {
    running = false
    signal()
  }
}

object AsyncSemaphore {
  def apply(permits: Int = 1) = {
    require(permits >= 1)
    new AsyncSemaphore(new AtomicInteger(permits))
  }
}

final class AsyncSemaphore private (private[this] val permits: AtomicInteger) {
  private[this] val latch = new AsyncLatch[Unit]

  /**
    * Returns a future that completes when the semaphore has acquired some
    * permits. The future will contain the number of permits that were acquired.
    * @param n The maximum number of permits to acquire. Must be non-negative.
    * @return a future that will complete with the number of permits acquired.
    *         The method allows partial acquisition, so it is possible that the
    *         future will complete with a lesser number of permits, but at least
    *         1 if n > 0. For n == 0, it immediately completes with 0.
    */
  def acquire(n: Int = 1): Future[Int] = {
    require(n >= 0, s"n == $n, must be at least 0.")
    if (n == 0) {
      Future.successful(0)
    } else {
      val taken = permits.getAndAccumulate(n, (x, y) => (x - y) max 0) min n

      if (taken == 0) {
        implicit val ec = ImmediateExecutionContext

        latch.await flatMap { _ => acquire(n) }
      } else {
        Future.successful(taken)
      }
    }
  }

  def availablePermits: Int = permits.get

  def release(count: Int = 1): Unit =
    if (permits.getAndAdd(count) == 0) {
      latch.signal(())
    }
}
