package fauna.exec

import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.{ Failure, Success }

object AsyncRingBuffer {
  private val ClosedSequence = -1L
  private val CanceledSequence = -2L
}

/** A single producer, multiple consumer ring buffer implementation -- semantically
  * equivalent to `Publisher`.
  *
  * This ring buffer synchronizes publishers while allowing for uncoordinated
  * consumers to follow the ring at their own pace. If the ring outruns any
  * consumer, the consumer detects the race and closes itself.
  *
  * Note that the lack of synchronization between producers and consumers prevents
  * the ring buffer from nullifying its entries once all consumers have handled them.
  * Therefore, this ring buffer is only viable in scenarios where values are pushed
  * periodically. The rate of publishing events plus the ring buffer size determines
  * for how long an entry will be held before being available for GC.
  *
  * XXX: A future improvement to this data-structure might include tracking consumers
  * so that publishers periodically or collaboratively scan them looking for the
  * maximum processed entry from which prior slots can be nullified to aid with GC.
  * This would make the ring suitable for low throughput or mostly idle use-cases.
  *
  * == Synchronization and thread-safety semantics ==
  *
  * This ring buffer is inspired by the LMAX Disruptor design. Reads and writes to
  * the ring array are not synchronized to prevent lock waits. Synchronization
  * between reads and writes occurs based on a volatile sequence increment. The
  * thread safety assumptions are:
  *
  * 1. Concurrent writes are synchronized at publishing time;
  *
  * 2. Reads only occur after writes guarded by the volatile sequence increment;
  *
  * 3. Contention to read and write to the same slot is detected by the consumer's
  * outrun check prior to using a value from the ring, thus discarding a possibly
  * stale read instead of pushing it down to the observer;
  *
  * See: https://github.com/LMAX-Exchange/disruptor/
  */
final class AsyncRingBuffer[A: ClassTag](maxBufferSize: Int) extends Observable[A] {
  import AsyncRingBuffer._

  private val ring = new Array[A](maxBufferSize)
  private val globalLatch = new AsyncLatch[Unit]

  @volatile private var sequence = 0L
  @volatile private var error: Throwable = _

  @inline def isClosed: Boolean =
    sequence == ClosedSequence

  def publish(value: A): Boolean = {
    val accepted =
      synchronized {
        if (isClosed) {
          false
        } else {
          val idx = sequence % maxBufferSize
          ring(idx.toInt) = value
          sequence += 1
          require(sequence > 0, "ring buffer sequence overflow")
          true
        }
      }
    if (accepted) {
      globalLatch.signal(())
    }
    accepted
  }

  def close(err: Throwable = null): Unit = {
    val closedNow =
      synchronized {
        if (!isClosed) {
          sequence = ClosedSequence
          error = err
          true
        } else {
          false
        }
      }
    if (closedNow) {
      globalLatch.signal(())
    }
  }

  def subscribe(observer: Observer[A]): Cancelable =
    new Consumer(observer)

  private final class Consumer(observer: Observer[A])
      extends Cancelable
      with ExceptionLogging {
    import observer.ec

    private val localLatch = new AsyncLatch[Unit]
    @volatile private var cursor = sequence // start from next published value.

    logException(Future.delegate(dispatch()))

    @inline def isCanceled: Boolean =
      cursor == CanceledSequence

    def cancel(): Unit = {
      synchronized { cursor = CanceledSequence }
      localLatch.signal(())
    }

    private def dispatch(): Future[Unit] = {
      // RACE! take a snapshot of the cursor *HERE* so that a race with `cancel()`
      // don't impact the dispatch item position midrun. It's okay to dispatch a
      // value upon a race given that the next iteration of of `dispatch()` will
      // catch the consumer being canceled.
      val cursor0 = cursor
      if (isCanceled) {
        // Semantically equivalent to a `Publisher` buffer overlow.
        observer.onError(Cancelable.Canceled)
        Future.unit
      } else if (cursor0 < sequence) {
        val idx = cursor0 % maxBufferSize
        val value = ring(idx.toInt)

        // RACE! Check if the consumer wasn't outrun by the ring *AFTER* reading the
        // next value, thus ensuring the value read is valid before pushing it down
        // to the observer.
        if (sequence - cursor0 > maxBufferSize) {
          // Semantically equivalent to a `Publisher` buffer overlow.
          Future(
            observer.onError(
              Publisher.QueueFull(maxBufferSize)
            ))
        } else {
          GuardFuture(observer.onNext(value)) transformWith {
            case Success(Observer.Continue) =>
              synchronized {
                // If was cancelled mid dispatch, let the next iteration detect it.
                if (!isCanceled) cursor += 1
              }
              dispatch()

            case Success(Observer.Stop) =>
              observer.onComplete()
              Future.unit

            case Failure(err) =>
              observer.onError(err)
              Future.unit
          }
        }
      } else if (isClosed) { // Ring closed?
        if (error ne null) {
          observer.onError(error)
        } else {
          observer.onComplete()
        }
        Future.unit
      } else {
        val globalSignal = globalLatch.await
        // RACE! Check if there are more values to publish *BEFORE* waiting on the
        // global latch, thus ensuring that the consumer either observes the sequence
        // increment or else catches a continue signal on the global latch.
        if (cursor0 < sequence) {
          dispatch()
        } else {
          // NB: `Future.firstCompletedOf(Seq(globalSignal.await, localLatch.await))`
          // retains a the future chain on all given futures. Considering the local
          // latch won't receive any signals until closing the consumer, the retained
          // future chain causes an OOM under high throughput. Feeding the global
          // signal into the local one prevents the retention of a long future chain.
          globalSignal foreach { localLatch.signal(_) }
          localLatch.await flatMap { _ => dispatch() }
        }
      }
    }
  }
}
