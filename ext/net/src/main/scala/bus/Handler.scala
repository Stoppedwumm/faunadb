package fauna.net.bus

import fauna.codex.cbor.CBOR
import fauna.exec._
import fauna.lang.syntax._
import fauna.lang.TimeBound
import fauna.logging.ExceptionLogging
import fauna.net.HostInfo
import fauna.trace.GlobalTracer
import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.util.Timeout
import scala.concurrent.Future

trait Handler {
  @volatile private[bus] var isIdle = false

  // NB: Only access `timeout` and `isDone` through syncronized blocks.
  private[bus] var timeout: Timeout = null
  private[bus] var isDone = false

  def isTCPStreamHandler: Boolean = false
  @annotation.nowarn("cat=unused-params")
  def recvTCPStream(from: HostInfo, ch: Channel): Future[Unit] = Future.unit

  def recvBytes(from: HostInfo, bytes: Future[ByteBuf], deadline: TimeBound): Future[Unit]
  def recvExpire(): Future[Unit]

  // This is called from the message bus when unbinding a handler. A similarly
  // named `close`, on `Source`, which extends handler, calls unbind, which then
  // calls `setDone`.
  private[bus] def setDone() = synchronized {
    if (!isDone) {
      isDone = true
      if (timeout ne null) {
        timeout.cancel()
        timeout = null
      }
    }
  }
}

object Handler extends ExceptionLogging {
  type HandlerF[T] = (HostInfo, T, TimeBound) => Future[Unit]

  private def decodeCBOR[T: CBOR.Decoder](
    buf: ByteBuf,
    retPolicy: CBOR.BufRetention) =
    if (retPolicy == CBOR.BufRetention.Borrow) {
      // handler will release
      CBOR.decode[T](buf, retPolicy)
    } else {
      // handler must release, so retain message components and
      // release underlying buffer.
      try {
        CBOR.decode[T](buf, retPolicy)
      } finally {
        buf.release()
      }
    }

  protected final class SyncHandler[T](
    private[this] val name: String,
    private[this] val f: HandlerF[T],
    private[this] val expired: => Future[Unit],
    private[this] val decode: ByteBuf => T,
    private[this] val autoRelease: Boolean) extends Handler {

    def recvBytes(from: HostInfo, bytes: Future[ByteBuf], deadline: TimeBound) = {
      implicit val ec = ImmediateExecutionContext
      val tracer = GlobalTracer.instance

      tracer.withSpan(name) {
        val fut = bytes flatMap { b => f(from, decode(b), deadline) }

        logException(fut)

        if (autoRelease) {
          fut ensure { bytes foreach { _.release() } }
        } else {
          fut
        }
      }
    }

    def recvExpire() = expired
  }

  def apply[T: CBOR.Decoder](
    name: String,
    f: HandlerF[T],
    expired: => Future[Unit] = Future.unit,
    bufRetention: CBOR.BufRetention = CBOR.BufRetention.Borrow): Handler =
    new SyncHandler(
      name,
      f,
      expired,
      decodeCBOR(_, bufRetention),
      bufRetention == CBOR.BufRetention.Borrow)

  def bytes(
    name: String,
    f: HandlerF[ByteBuf],
    expired: => Future[Unit] = Future.unit,
    autoRelease: Boolean = true): Handler =
    new SyncHandler(name, f, expired, buf => buf, autoRelease)

  protected final class AsyncHandler[T](
    private[this] val name: String,
    private[this] val f: HandlerF[Future[T]],
    private[this] val expired: => Future[Unit],
    private[this] val decode: ByteBuf => T,
    private[this] val autoRelease: Boolean) extends Handler {

    def recvBytes(from: HostInfo, bytes: Future[ByteBuf], deadline: TimeBound) = {
      implicit val ec = ImmediateExecutionContext
      val tracer = GlobalTracer.instance

      tracer.withSpan(name) {
        val fut = f(from, bytes map decode, deadline)

        logException(fut)

        if (autoRelease) {
          fut ensure { bytes foreach { _.release() } }
        } else {
          fut
        }
      }
    }

    def recvExpire() = expired
  }

  def async[T: CBOR.Decoder](
    name: String,
    f: HandlerF[Future[T]],
    expired: => Future[Unit] = Future.unit,
    bufRetention: CBOR.BufRetention = CBOR.BufRetention.Borrow): Handler =
    new AsyncHandler(
      name,
      f,
      expired,
      decodeCBOR(_, bufRetention),
      bufRetention == CBOR.BufRetention.Borrow)

  protected final class TCPStreamHandler(
    f: (HostInfo, Channel) => Future[Unit],
    expired: => Future[Unit],
    enableChannelSyntax: Boolean) extends Handler {

    override def isTCPStreamHandler = true
    override def recvTCPStream(from: HostInfo, ch: Channel) = {
      if (enableChannelSyntax) {
        ch.enableChannelSyntax()
      }
      f(from, ch)
    }

    def recvBytes(from: HostInfo, bytes: Future[ByteBuf], deadline: TimeBound) = Future.unit
    def recvExpire() = expired
  }

  def stream(f: (HostInfo, Channel) => Future[Unit], expired: => Future[Unit], enableChannelSyntax: Boolean) =
    new TCPStreamHandler(f, expired, enableChannelSyntax)
}
