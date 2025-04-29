package fauna.net.util

import fauna.trace.{ InternalError, Scope }
import io.netty.buffer.{ ByteBuf, ByteBufAllocator }
import java.util.concurrent.TimeoutException
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.control.NoStackTrace

object ByteBufStream {
  object StreamClosed extends Exception("Stream closed.") with NoStackTrace
}

sealed trait ByteBufStream {
  def bytes: Future[ByteBuf]
  def onChunk(f: () => Unit): Unit
  def close(): Unit
}

final class SinglePacketStream(buf: ByteBuf) extends ByteBufStream {
  val bytes = Future.successful(buf)
  def onChunk(f: () => Unit): Unit = ()
  def close(): Unit = buf.release()
}

final class MultiPacketStream(msgCount: Int, alloc: ByteBufAllocator, scope: Option[Scope])
    extends ByteBufStream {

  private[this] val p = Promise[ByteBuf]()
  private[this] val cBuf = alloc.compositeBuffer(msgCount)
  private[this] var cb: List[() => Unit] = Nil

  val bytes = p.future

  scope foreach { _.retain() }

  def onChunk(f: () => Unit) = synchronized { cb = f :: cb }

  def close() = fail(ByteBufStream.StreamClosed)

  def addChunk(num: Int, b: ByteBuf, ec: ExecutionContext) = synchronized {
    if (!p.isCompleted) {
      // don't allow out of order packets
      if (num != (cBuf.numComponents + 1)) {
        fail(new TimeoutException("Invalid stream."))
        true
      } else {
        cBuf.addComponent(true, b)
        cb foreach { _() }
        if (cBuf.numComponents == msgCount) {
          ec.execute { () =>
            // cBuf ownership goes to the caller
            if (p.trySuccess(cBuf)) {
              // scope ownership is mine.
              scope foreach { _.close() }
            }
          }
          true
        } else {
          false
        }
      }
    } else {
      // cBuf and scope both owned by others, release the input
      // immediately
      b.release()
      true
    }
  }

  def fail(ex: Exception) = synchronized {
    if (p.tryFailure(ex)) {
      // cBuf and scope ownership are mine
      cBuf.release()
      scope foreach { s =>
        s.span foreach { _.setStatus(InternalError(ex.getMessage)) }
        s.close()
      }
    }
  }
}
