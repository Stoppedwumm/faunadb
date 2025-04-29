package fauna.exec

import fauna.lang.syntax._
import java.util.concurrent.{ BlockingQueue, LinkedBlockingQueue }
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.{ Failure, Success }
import scala.util.control.NoStackTrace

/** Cancelable is used by subscription holders to cancel values' flow. */
trait Cancelable {

  /** Cancels the current subscription. */
  def cancel(): Unit

  /** Cancels the subscription after `fut` completes. */
  final def cancelAfter[A](fut: Future[A])(implicit
    ec: ExecutionContext): Future[A] =
    fut ensure { cancel() }
}

object Cancelable {

  type Canceled = Canceled.type

  /**
    * Although not required by the Cancelable semantics, the Canceled exception is
    * useful to mark a value flow as prematurely closed due to cancellation.
    */
  case object Canceled extends NoStackTrace

  /** An noop `Cancelable` instance. */
  val empty =
    new Cancelable {
      def cancel(): Unit = ()
    }
}

/**
  * An asynchronous observer.
  *
  * Semantics:
  *
  * * The `onNext` method will be called for every value observed. Implementations
  *   must ensure the value has been processed before completing the returned future,
  *   allowing the upstream to accurately account for back-pressure caused by slow
  *   value processing. Subsequent callbacks (including `onError` and `onComplete`)
  *   are serialized after value processing, hence, implementations must ensure the
  *   returned future is always completed or failed.
  *
  * * The `onError` method will be called if an error has happened on the publisher
  *   side or during value processing. After called, no more calls to any observer's
  *   callbacks will be made.
  *
  * * The `onComplete` method will be called if no more values are available, or when
  *   returning the `Observer.Stop` signal. After called, no more calls to the
  *   observer's callback will be made.
  *
  * Calls to any of the callback functions in the contract are made asynchronously
  * within the execution context provided. Make sure to select the appropriate ec to
  * handle the observed values. Be aware that parasitic ecs may block the value's
  * publisher, effectively making the value flow synchronous.
  */
trait Observer[-A] {
  implicit val ec: ExecutionContext
  def onNext(value: A): Future[Observer.Ack]
  def onError(cause: Throwable): Unit
  def onComplete(): Unit
}

object Observer {

  /**
    * Acknowledges the completion of a observer callback: `Continue` signals the
    * observer wishes to continue receiving new values; `Stop` signals the observer
    * no longer wishes to receive new values.
    */
  sealed trait Ack
  case object Continue extends Ack
  case object Stop extends Ack

  final val ContinueF: Future[Ack] = Future.successful(Continue)
  final val StopF: Future[Ack] = Future.successful(Stop)

  /** Default implementation for syntax sugar. Primarily used for testing. */
  abstract class Default[A](implicit val ec: ExecutionContext) extends Observer[A] {
    def onNext(value: A): Future[Ack] = Future.successful(Continue: Ack)
    def onError(cause: Throwable): Unit = throw cause
    def onComplete(): Unit = ()
  }

  /**
    * An observer with an embedded future used to compute the final result after all
    * values have been processed.
    */
  abstract class ObsPromise[A, B](implicit val ec: ExecutionContext)
      extends Observer[A] {
    protected val promise = Promise[B]()
    def onError(cause: Throwable) = promise.tryFailure(cause)
    def future = promise.future
  }
}

/**
  * An observable type. Observables are lazily transformed and only initiate the
  * value flow when the `subscribe` method is invoked. The `subscribe` method returns
  * a `Cancelable` instance that allows the value flow to be canceled by the
  * subscriber.
  *
  * The observable API is composed by transformation and consumer methods.
  * Transformations are lazy, meaning that they do not initiate the value flow, and
  * that they return a new observable type when called. Consumer functions have the
  * `F` suffix and they initiate the value flow as soon as called, returning a
  * `Future` type that is fulfilled once the consumer completes.
  */
trait Observable[+A] {
  import Observer._

  /** Initiates the value flow. */
  def subscribe(observer: Observer[A]): Cancelable

  /** Given a predicate `p`, returns an observable of the values selected by `p`. */
  final def select(p: A => Boolean): Observable[A] =
    transform { (observer, value) =>
      if (p(value)) {
        observer.onNext(value)
      } else {
        ContinueF
      }
    }

  /** Same as `select`, except its predicate `p` runs asynchronously. */
  final def selectAsync(p: A => Future[Boolean]): Observable[A] =
    transform { (observer, value) =>
      import observer.ec
      p(value) flatMap { predicate =>
        if (predicate) {
          observer.onNext(value)
        } else {
          ContinueF
        }
      }
    }

  /**
    * Given a mapping function `fn: A => B`, returns an observable of type B obtained
    * by calling `fn` on each value processed by this observable.
    */
  final def map[B](fn: A => B): Observable[B] =
    transform { (observer, value) =>
      observer.onNext(fn(value))
    }

  /** Same as `map`, except its mapping function runs asynchronously. */
  final def mapAsync[B](fn: A => Future[B]): Observable[B] =
    transform { (observer, value) =>
      import observer.ec
      fn(value) flatMap {
        observer.onNext(_)
      }
    }

  /**
    * Given a mapping function `fn: A => Observable[B]`, returns an observable of
    * type B obtained by calling `fn` on each value observed by this observable and
    * then subscribing to its result.
    */
  final def flatMap[B](fn: A => Observable[B]): Observable[B] =
    transform { (observer, value) =>
      import observer.ec
      @volatile var lastAckF = ContinueF
      val obsP =
        new ObsPromise[B, Ack] {
          def onNext(value: B) = { lastAckF = observer.onNext(value); lastAckF }
          def onComplete() = promise.completeWith(lastAckF)
        }
      fn(value).subscribe(obsP)
      obsP.future
    }

  /** Same as `flatMap`, except its mapping function runs asynchronously. */
  final def flatMapAsync[B](fn: A => Future[Observable[B]]): Observable[B] =
    transform { (observer, value) =>
      import observer.ec
      fn(value) flatMap { nextObs =>
        @volatile var lastAckF = ContinueF
        val obsP =
          new ObsPromise[B, Ack] {
            def onNext(value: B) = { lastAckF = observer.onNext(value); lastAckF }
            def onComplete() = promise.completeWith(lastAckF)
          }
        nextObs.subscribe(obsP)
        obsP.future
      }
    }

  /**
    * Given a `fn: (Observer[B], A) => Future[Observer.Ack]`, returns a new
    * observable of type B obtained by piping each value processed by this observable
    * to `fn`. The function `fn` is responsible for pushing the transformed values
    * into the given observer.
    */
  final def transform[B](
    fn: (Observer[B], A) => Future[Observer.Ack]): Observable[B] =
    observer =>
      subscribe(new Observer[A] {
        implicit val ec = observer.ec
        def onNext(value: A) = GuardFuture(fn(observer, value))
        def onError(cause: Throwable) = observer.onError(cause)
        def onComplete() = observer.onComplete()
      })

  /** Returns a new observable that continues with the given `other` values after
    * `this` observable is completed.
    */
  final def concat[B >: A](other: Observable[B]): Observable[B] =
    observer =>
      subscribe(new Observer[A] {
        implicit val ec = observer.ec
        def onNext(value: A) = observer.onNext(value)
        def onError(cause: Throwable) = observer.onError(cause)
        def onComplete() = other.subscribe(observer)
      })

  /** Returns a new observable that batches at most `batchSize` values while the
    * downstream observer is processing previous batches.
    *
    * NOTE: this method will retain up to `2*batchSize` entries under high throughput
    * to ensure that batches have the same size.
    */
  final def batched(batchSize: Int): Observable[Seq[A]] =
    observer => {
      import observer.ec
      val bufSize = 2 * batchSize
      val batch = ArrayBuffer.empty[A]
      batch.sizeHint(bufSize)

      def processBatch(): Future[Ack] = {
        val next =
          batch.synchronized {
            val next = batch.view.take(batchSize).toSeq
            batch.dropInPlace(batchSize)
            next
          }
        if (next.isEmpty) {
          ContinueF
        } else {
          GuardFuture(observer.onNext(next)) flatMap {
            case Continue => processBatch()
            case Stop     => StopF
          }
        }
      }

      @volatile var lastBatchF = ContinueF
      @inline def maybeContinue(size: Int) =
        if (size < bufSize) ContinueF else lastBatchF

      subscribe(new Observer[A] {
        implicit val ec = observer.ec
        def onNext(value: A) = {
          val size =
            batch.synchronized {
              batch += value
              batch.size
            }
          if (lastBatchF.isCompleted) {
            lastBatchF flatMap {
              case Stop => StopF
              case Continue =>
                lastBatchF = processBatch()
                maybeContinue(size)
            }
          } else {
            maybeContinue(size)
          }
        }
        def onError(cause: Throwable) =
          lastBatchF onComplete { _ =>
            observer.onError(cause)
          }
        def onComplete() =
          lastBatchF onComplete { _ =>
            observer.onComplete()
          }

      })
    }

  /**
    * Given a partial function `pf: Throwable => Future[Observable[B]`, attempt to
    * recover from a failed observable by switching to the one returned by `pf`.
    */
  final def recoverAsync[B >: A](
    pf: PartialFunction[Throwable, Future[Observable[B]]]): Observable[B] = {
    observer =>
      subscribe(new Observer[A] {
        implicit val ec = observer.ec
        def onNext(value: A) = observer.onNext(value)
        def onComplete() = observer.onComplete()
        def onError(cause: Throwable) =
          if (pf.isDefinedAt(cause)) {
            GuardFuture(pf(cause)) onComplete {
              case Success(next) => next.subscribe(observer)
              case Failure(err)  => observer.onError(err)
            }
          } else {
            observer.onError(cause)
          }
      })
  }

  /**
    * Given a callback function `fn: => Any`, returns an observable that calls `fn`
    * if it either completes or fails. Useful to release resources held due to value
    * processing.
    */
  final def ensure(fn: => Any): Observable[A] =
    observer =>
      subscribe(new Observer[A] {
        implicit val ec = observer.ec
        def onNext(value: A) = observer.onNext(value)
        def onError(cause: Throwable) = wrap(observer.onError(cause))
        def onComplete() = wrap(observer.onComplete())

        @inline def wrap(thunk: => Any) =
          try thunk
          finally fn
      })

  /** Consumes the observable by folding its values with the provided `seed`. */
  final def foldLeftF[B](seed: B)(fn: (B, A) => B)(implicit
    ec: ExecutionContext): Future[B] = {
    var last = seed
    val obsP =
      new ObsPromise[A, B] {
        def onComplete() = promise.trySuccess(last)
        def onNext(value: A): Future[Ack] =
          GuardFuture {
            last = fn(last, value)
            ContinueF
          }
      }
    subscribe(obsP) cancelAfter { obsP.future }
  }

  /** Consumes the observable by calling `fn` on each observed value. */
  final def foreachF(fn: A => Unit)(implicit ec: ExecutionContext): Future[Unit] = {
    val obsP =
      new ObsPromise[A, Unit] {
        def onComplete() = promise.trySuccess(())
        def onNext(value: A): Future[Ack] =
          GuardFuture {
            fn(value)
            ContinueF
          }
      }
    subscribe(obsP) cancelAfter { obsP.future }
  }

  /** Same as `foreach`, except `fn` can run asynchronously. */
  final def foreachAsyncF(fn: A => Future[Unit])(implicit
    ec: ExecutionContext): Future[Unit] = {
    val obsP =
      new ObsPromise[A, Unit] {
        def onComplete() = promise.trySuccess(())
        def onNext(value: A): Future[Ack] =
          GuardFuture { fn(value) before ContinueF }
      }
    subscribe(obsP) cancelAfter { obsP.future }
  }

  /** Consumes the observable by taking its first value. */
  final def firstF(implicit ec: ExecutionContext): Future[Option[A]] = {
    val obsP =
      new ObsPromise[A, Option[A]] {
        def onComplete() = promise.trySuccess(None)
        def onNext(value: A) = {
          promise.trySuccess(Some(value))
          StopF
        }
      }
    subscribe(obsP) cancelAfter { obsP.future }
  }

  /** Consumes the observable by taking its first `count` values. */
  final def takeF(count: Int)(implicit ec: ExecutionContext): Future[Seq[A]] = {
    val values = Seq.newBuilder[A]
    val obsP =
      new ObsPromise[A, Seq[A]] {
        def onComplete() = promise.trySuccess(values.result())
        def onNext(value: A): Future[Ack] = {
          values += value
          if (values.knownSize < count) {
            ContinueF
          } else {
            promise.trySuccess(values.result())
            StopF
          }
        }
      }
    subscribe(obsP) cancelAfter { obsP.future }
  }

  /** Consumes the observable by accumulating all observed values. */
  final def sequenceF(implicit ec: ExecutionContext): Future[Seq[A]] = {
    val values = Seq.newBuilder[A]
    val obsP =
      new ObsPromise[A, Seq[A]] {
        def onComplete() = promise.trySuccess(values.result())
        def onNext(value: A) = {
          values += value
          ContinueF
        }
      }
    subscribe(obsP) cancelAfter { obsP.future }
  }
}

object Observable {
  import Cancelable._
  import Observer._

  /** Returns an observable that never emits any values. */
  def never[A]: Observable[A] =
    Observable.create { _: Publisher[A] => Cancelable.empty }

  /** Returns an observable with no values. */
  def empty[A]: Observable[A] =
    Observable.from(Iterable.empty)

  /** Returns an observable with a single value. */
  def single[A](single: A): Observable[A] =
    Observable.from(Iterable.single(single))

  /** Returns a future wrapping an observable with a single value. */
  def singleF[A](single: A): Future[Observable[A]] =
    Future.successful(Observable.from(Iterable.single(single)))

  /** Converts an iterable into an observable. */
  def from[A](iterable: Iterable[A]): Observable[A] =
    observer => {
      import observer.ec
      val iter = iterable.iterator
      @volatile var canceled = false

      def consume(): Future[Unit] =
        if (canceled) {
          observer.onError(Canceled)
          Future.unit
        } else {
          if (iter.hasNext) {
            GuardFuture(observer.onNext(iter.next())) transformWith {
              case Success(Continue) => consume()
              case Success(Stop)     => Future.successful(observer.onComplete())
              case Failure(err)      => Future.successful(observer.onError(err))
            }
          } else {
            observer.onComplete()
            Future.unit
          }
        }

      consume()
      () => canceled = true
    }

  /**
    * Returns an observable that immediately calls for the `onError` callback on the
    * subscribing observer.
    */
  def failed[A](cause: Throwable): Observable[A] =
    observer => {
      observer.onError(cause)
      Cancelable.empty
    }

  /**
    * Creates an unbounded observable for the values published into the provided
    * `Publisher` instance.
    */
  def create[A](fn: Publisher[A] => Cancelable): Observable[A] =
    create(OverflowStrategy.unbounded[A])(fn)

  /**
    * Creates an observable for the values published into the `Publisher` instance
    * as long as allowed by the overflow strategy provided.
    */
  def create[A](overflow: OverflowStrategy[A])(
    fn: Publisher[A] => Cancelable): Observable[A] =
    observer => {
      val pub = new Publisher[A](overflow, observer)
      val sub = fn(pub)
      () => {
        pub.cancel()
        sub.cancel()
      }
    }

  /** Creates an observable for the values published into the `Publisher` instance
    * while buffering events until the `Observable.subscribe` method is called, as
    * long as allowed by the overflow strategy provided.
    */
  def gathering[A](overflow: OverflowStrategy[A] = OverflowStrategy.unbounded[A])(
    implicit _ec: ExecutionContext): (Publisher[A], Observable[A]) = {

    val obsP = Promise[Observer[A]]()

    val pub =
      new Publisher[A](
        overflow,
        new Observer[A] {
          val ec = _ec
          def onNext(value: A) = obsP.future flatMap { _.onNext(value) }
          def onError(err: Throwable) = obsP.future foreach { _.onError(err) }
          def onComplete() = obsP.future foreach { _.onComplete() }
        })

    val obs: Observable[A] =
      observer => {
        obsP.trySuccess(observer)
        () => pub.fail(Canceled)
      }

    (pub, obs)
  }
}

/**
  * An overflow strategy controls the `Publisher` buffer size while handling
  * leftovers caused by overflow and/or closing.
  */
abstract class OverflowStrategy[A](val maxBufferSize: Int) {
  private[exec] def handleOverflow(queue: BlockingQueue[A]): Unit
  private[exec] def handleOverflow(value: A): Unit
}

object OverflowStrategy {

  /** Allows for an unbounded publishing queue. */
  def unbounded[A] =
    new OverflowStrategy[A](Int.MaxValue) {
      def handleOverflow(queue: BlockingQueue[A]) = queue.clear()
      def handleOverflow(value: A) = ()
    }

  /** Restrict a publishing queue of at most `maxBufferSize` values. */
  def bounded[A](maxBufferSize: Int) =
    new OverflowStrategy[A](maxBufferSize) {
      def handleOverflow(queue: BlockingQueue[A]) = queue.clear()
      def handleOverflow(value: A) = ()
    }

  /**
    * Allows for an unbounded publishing queue. Leftovers are due to closing or
    * cancellation are handed over to the `callback` function for resource release.
    */
  def callbackOnOverflow[A](callback: A => Unit): OverflowStrategy[A] =
    callbackOnOverflow(maxBufferSize = Int.MaxValue)(callback)

  /**
    * Restrict a publishing queue of at most `maxBufferSize` values. Leftovers are
    * handed over to the `callback` function for resource release.
    */
  def callbackOnOverflow[A](maxBufferSize: Int)(callback: A => Unit) =
    new OverflowStrategy[A](maxBufferSize) {

      def handleOverflow(queue: BlockingQueue[A]) =
        while (!queue.isEmpty) {
          handleOverflow(queue.poll())
        }

      def handleOverflow(value: A) =
        callback(value)
    }
}

object Publisher {

  /** QueueFull is thrown by the `Publisher` if its max queue size is reached. */
  final case class QueueFull(max: Int)
      extends Exception(s"Publisher queue is full (max=$max).", null, false, false)
}

/**
  * A value publisher. It buffers events according to the provided overflow strategy,
  * handling values asynchronously withing the execution context provided by its
  * observer type.
  */
final class Publisher[A] private[exec] (
  overflow: OverflowStrategy[A],
  observer: Observer[A]) {
  import Cancelable._
  import Observer._
  import Publisher._
  import observer.ec

  private[this] val queue = new LinkedBlockingQueue[A](overflow.maxBufferSize)
  private[this] var lastProcess = Future.unit

  @volatile private[this] var processing = false
  @volatile private[this] var interrupt = false
  @volatile private[this] var closed = false

  /** Publish a value and returns true if accepted. Returns false otherwise. */
  def publish(value: A): Boolean =
    synchronized {
      if (!closed && queue.offer(value)) {
        dispatch()
        true
      } else {
        // if already closed/canceled, it just handles the overflow
        doClose(Some(QueueFull(overflow.maxBufferSize))) ensure {
          overflow.handleOverflow(value)
        }
        false
      }
    }

  /** Closes a publisher without errors. */
  def close(): Unit =
    doClose(None)

  /** Closes a publisher with the given error. */
  def fail(err: Throwable): Unit =
    doClose(Some(err))

  /** Closes a publisher with an `Cancelable.Canceled` error. */
  private[exec] def cancel(): Unit =
    doClose(Some(Canceled))

  private def doClose(err: Option[Throwable]): Future[Unit] = {
    synchronized {
      if (!closed) {
        closed = true
        interrupt = err.isDefined
        lastProcess = lastProcess andThen { _ =>
          try {
            err match {
              case None      => observer.onComplete()
              case Some(err) => observer.onError(err)
            }
          } finally {
            overflow.handleOverflow(queue)
          }
        }
      }
      lastProcess
    }
  }

  private def dispatch(): Unit = {
    def dispatch0(): Future[Unit] =
      if (closed && interrupt) {
        Future.unit
      } else {
        val next = queue.poll
        if (next == null) {
          processing = false
          Future.successful(dispatch())
        } else {
          GuardFuture(observer.onNext(next)) transformWith {
            case Success(Continue) => dispatch0()
            case Success(Stop)     => Future.successful(close())
            case Failure(err)      => Future.successful(fail(err))
          }
        }
      }

    synchronized {
      if (!queue.isEmpty && !processing && !closed) {
        processing = true
        lastProcess = Future.delegate(dispatch0())
      }
    }
  }
}
