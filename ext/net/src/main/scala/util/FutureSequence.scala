package fauna.net.util

import fauna.lang.syntax._
import scala.concurrent._

/**
  * An instance of FutureSequence acts as an asynchronous
  * serialization mechanism for computation. The only method is
  * apply[T], which takes a closure to create a future, and passes
  * back a future that will be complete once all previously submitted
  * futures, and finally the created one is complete.
  *
  * It maintains a sequential execution of submitted closures by using
  * Future#onComplete to chain execution of each subsequent closure
  * off of the previous one.
  */
object FutureSequence {
  def apply(maxPending: Int = Int.MaxValue)(implicit ec: ExecutionContext) =
    new FutureSequence(maxPending)
}

class FutureSequence(maxPending: Int)(implicit ec: ExecutionContext) {

  if (maxPending < 1) throw new IllegalArgumentException("maxPending must be greater than 0")

  // counts the one currently executing if it's there.
  private[this] var numPending = 1
  private[this] var tail = Future.successful[Any](null)

  // FIXME: This could probably be made more efficient with better
  // concurrency primitives. This works by always appending the new
  // work to the current tail and updating the tail to the new
  // composed future.
  def apply[T](run: => Future[T]): Future[T] =
    synchronized {
      if (numPending >= maxPending) {
        Future.failed(new IllegalStateException(s"Max of $maxPending futures currently pending."))
      } else {
        val fut = if (tail.isCompleted) {
          GuardFuture(run)
        } else {
          numPending += 1
          val p = Promise[T]()

          tail onComplete { _ =>
            decrPending()
            val fut = synchronized { GuardFuture(run) }
            p completeWith fut
          }

          p.future
        }

        tail = fut
        fut
      }
    }

  private def decrPending() = synchronized { numPending -= 1 }
}
