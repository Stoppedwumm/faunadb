package fauna.exec

import io.netty.util.{ AbstractReferenceCounted, IllegalReferenceCountException }
import java.util.concurrent.ConcurrentMap
import scala.concurrent.duration._
import scala.concurrent.{ Future, Promise }

/**
  * Generic class for registering waiters for a future value.
  */
abstract class AsyncWaitersMap[K, V] {

  @volatile private var shutdownEx: Exception = _

  def shutdown(ex: Exception) =
    synchronized {
      if (shutdownEx eq null) {
        shutdownEx = ex
        pendingPromises forEach { (_, p) => p.tryFailure(shutdownEx) }
      }
    }

  /**
    * Map to hold pending promises. On state change, subclasses should
    * use this map directly to complete waiting promises.
    */
  protected val pendingPromises: ConcurrentMap[K, PromiseRef]

  /**
    * Timer used to time out waiting promises
    */
  protected def timer: Timer

  /**
    * The result for a key on timeout.
    */
  protected def timeoutForKey(key: K): Future[V]

  /**
    * An existing result for a key, if present.
    */
  protected def existingForKey(key: K): Option[Future[V]]

  protected class PromiseRef(key: K) extends AbstractReferenceCounted {
    def trySuccess(v: V) = promise.trySuccess(v)
    def tryFailure(t: Throwable) = promise.tryFailure(t)

    def completeWith(f: Future[V]) = promise.completeWith(f)

    val promise = {
      implicit val ec = ImmediateExecutionContext
      val p = Promise[V]()
      p.future onComplete { _ => pendingPromises.remove(key, this) }
      p
    }

    protected def deallocate() = completeWith(timeoutForKey(key))

    def touch(hint: Any) = this
  }

  protected def newPromiseRef(key: K): PromiseRef = new PromiseRef(key)

  def isEmpty = pendingPromises.isEmpty
  def nonEmpty = !isEmpty

  def get(key: K, within: Duration): Future[V] =
    existingForKey(key) match {
      // waiters should go away
      case _ if shutdownEx ne null => Future.failed(shutdownEx)

      // Key is already associated with a result.
      case Some(succ) => succ

      // Immediately fail on zero duration.
      case None if within == Duration.Zero => timeoutForKey(key)

      case None =>
        val entry = pendingPromises.compute(key, {
          case (_, null) =>
            newPromiseRef(key)
          case (_, prev) =>
            try {
              prev.retain()
              prev
            } catch {
              case _: IllegalReferenceCountException =>
                newPromiseRef(key)
            }
        })

        // Our key may already have gotten a result before we grabbed
        // our map entry. If that is the case, then the entry may not
        // have been notified, so re-check the key here.
        existingForKey(key) match {
          case Some(succ) =>
            entry.release()
            succ

          case None =>
            // We have an entry in the map, and our key is still not
            // satisfied, so schedule our timeout if necessary.
            if (within.isFinite) {
              timer.completedAfter(entry.promise.future, within) {
                entry.release()
                timeoutForKey(key)
              }
            } else {
              entry.promise.future
            }
        }
    }
}
