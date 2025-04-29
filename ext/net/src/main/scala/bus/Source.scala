package fauna.net.bus

import fauna.codex.cbor.CBOR
import fauna.exec._
import fauna.lang.syntax._
import fauna.lang.TimeBound
import fauna.net.HostInfo
import io.netty.buffer.ByteBuf
import io.netty.util.ReferenceCountUtil
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

/**
  * Source is a convenient API for responding to incoming messages in
  * a more coordinated fashion. Central to the interface is a
  * pull-based, queue API. Because implementations must statefully
  * manage received messages, these are going to be more costly than
  * basic Handlers if the use-case makes no difference.
  */
trait Source[+T] extends HandlerCtx {

  /**
    * Close the source. A closed source will not receive any more
    * messages, however currently pending messages must still be consumed.
    */
  def close(): Unit

  /**
    * Returns true if the source has been closed.
    */
  def isClosed: Boolean

  /**
    * Asynchronously poll the state of the source, waiting for up to
    * `within` if there are no pending messages.
    */
  def poll(within: FiniteDuration): Future[Source.Event[T]]

  /**
    * Subscribe to the source. The source is repeatedly serially polled, the
    * results of are applied to the provided function, `f`, which is
    * expected to return an Option which may contain a final result
    * value. If the result Option is non-empty, the source is closed
    * and the returned Future[R] is completed with the result.
    */
  def subscribe[R](idle: FiniteDuration = Source.DefaultIdle)
    (f: Source.Event[T] => Future[Option[R]])(implicit ec: ExecutionContext): Future[R]

  /**
    * Similar to subscribe, with the difference that a state value is
    * tracked. `f` must return a Left containing the next state value,
    * or a Right containing a final result. If a result is returned,
    * the source is closed and the returned future is completed with
    * the result.
    */
  def fold[S, R](seed: S, idle: FiniteDuration = Source.DefaultIdle)
    (f: (S, Source.Event[T]) => Future[Either[S, R]])(implicit ec: ExecutionContext): Future[R]

  def map[R](f: T => R): Source[R]
}

object Source {
  final val DefaultMaxPending = 100000
  val DefaultIdle = 10.seconds

  final class Message[+T](val from: HostInfo, val deadline: TimeBound, val value: T, retained: Any) {
    def map[R](f: T => R): Message[R] = new Message(from, deadline, f(value), retained)
    def retain() = { ReferenceCountUtil.retain(retained); this }
    def release() = { ReferenceCountUtil.release(retained); this }
    override def toString = s"Message($from, $deadline, $value, $retained)"
  }

  object Message {
    def unapply[T](m: Message[T]) = Some((m.from, m.deadline, m.value))
  }

  sealed trait Event[+T] {
    def map[R](f: T => R): Event[R]
  }
  final case class Messages[T](messages: Seq[Message[T]]) extends Event[T] {
    override def map[R](f: T => R): Event[R] = Messages(messages map { _ map f })
  }
  final case object Idle extends Event[Nothing] {
    override def map[R](f: Nothing => R): Event[R] = Idle
  }
  final case object Done extends Event[Nothing] {
    override def map[R](f: Nothing => R): Event[R] = Done
  }

  final class LimitExceeded(signalID: SignalID) extends Exception(
    s"Queue limit exceeded for $signalID")
}

final class QueueSource[T](
  private[this] val bus: MessageBus,
  private[this] val protocol: Protocol[T],
  final val id: HandlerID,
  private[this] val maxPending: Int) extends Source[T] with Handler {

  private[this] val queue = new AsyncQueue[Source.Message[T]]
  private[this] var _isClosed = false

  val name = protocol.name

  def isClosed = queue.synchronized { _isClosed }

  def close() = {
    bus.unbind(id.signalID, this)
    queue.synchronized { _isClosed = true }
  }

  private def translateEvent(ev: Option[Seq[Source.Message[T]]]) =
    ev match {
      case Some(vs) => Source.Messages(vs)
      case None     => if (isClosed) Source.Done else Source.Idle
    }

  private def releaseMessages(ev: Option[Seq[Source.Message[T]]]) =
    ev.getOrElse(Nil) foreach { _.release() }

  private def cleanupSubscribe() = {
    implicit val ec = ImmediateExecutionContext
    close()
    queue.poll(0.seconds) foreach releaseMessages
  }

  def poll(idle: FiniteDuration) = {
    implicit val ec = ImmediateExecutionContext
    queue.poll(idle) map translateEvent
  }

  def subscribe[R](idle: FiniteDuration)(f: Source.Event[T] => Future[Option[R]])(implicit ec: ExecutionContext): Future[R] =
    queue.subscribe(idle) { ev =>
      GuardFuture(f(translateEvent(ev))) ensure { releaseMessages(ev) }
    } ensure {
      cleanupSubscribe()
    }

  def fold[S, R](seed: S, idle: FiniteDuration)(f: (S, Source.Event[T]) => Future[Either[S, R]])(implicit ec: ExecutionContext): Future[R] =
    queue.fold(seed, idle) { (s, ev) =>
      GuardFuture(f(s, translateEvent(ev))) ensure { releaseMessages(ev) }
    } ensure {
      cleanupSubscribe()
    }

  // Handler

  private def enqueue(from: HostInfo, t: T, retained: Any, deadline: TimeBound) = {
    val msg = new Source.Message(from, deadline, t, retained)

    queue.synchronized {
      if (queue.size > maxPending) {
        msg.release()
        throw new Source.LimitExceeded(id.signalID)
      } else if (isClosed) {
        msg.release()
      } else {
        queue.add(Seq(msg))
      }
    }

    Future.unit
  }

  def recvBytes(from: HostInfo, bytes: Future[ByteBuf], deadline: TimeBound): Future[Unit] = {
    implicit val ec = ImmediateExecutionContext
    bytes flatMap { b => enqueue(from, CBOR.decode(b)(protocol.codec), b, deadline) }
  }

  def recvExpire() = {
    close()
    Future.unit
  }

  def map[R](f: T => R): Source[R] =
    new MappedQueueSource(this, f)
}

final class MappedQueueSource[T, A](
  private[this] val queueSource: QueueSource[A],
  private[this] val f: A => T)
    extends Source[T] {

  val id: HandlerID = queueSource.id

  /**
    * Close the source. A closed source will not receive any more
    * messages, however currently pending messages must still be consumed.
    */
  def close(): Unit = queueSource.close()

  /**
    * Returns true if the source has been closed.
    */
  def isClosed: Boolean = queueSource.isClosed

  /**
    * Asynchronously poll the state of the source, waiting for up to
    * `within` if there are no pending messages.
    */
  def poll(within: FiniteDuration): Future[Source.Event[T]] = {
    implicit val ec = ImmediateExecutionContext
    queueSource.poll(within) map { _ map f }
  }

  /**
    * Subscribe to the source. The source is repeatedly serially polled, the
    * results of are applied to the provided function, `f`, which is
    * expected to return an Option which may contain a final result
    * value. If the result Option is non-empty, the source is closed
    * and the returned Future[R] is completed with the result.
    */
  def subscribe[R](idle: FiniteDuration)(f2: Source.Event[T] => Future[Option[R]])(implicit ec: ExecutionContext): Future[R] =
    queueSource.subscribe(idle) { st => f2(st map { t => f(t) }) }

  /**
    * Similar to subscribe, with the difference that a state value is
    * tracked. `f` must return a Left containing the next state value,
    * or a Right containing a final result. If a result is returned,
    * the source is closed and the returned future is completed with
    * the result.
    */
  def fold[S, R](seed: S, idle: FiniteDuration)(f2: (S, Source.Event[T]) => Future[Either[S, R]])(implicit ec: ExecutionContext): Future[R] =
    queueSource.fold(seed, idle) { (s, st) =>
      f2(s, st map f)
    }

  def map[R](f2: T => R): Source[R] =
    new MappedQueueSource[R, A](queueSource, { a => f2(f(a)) })
}
