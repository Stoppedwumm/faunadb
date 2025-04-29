package fauna.lang

import io.netty.buffer._
import io.netty.channel._
import io.netty.handler.timeout.{ IdleState, IdleStateEvent, IdleStateHandler }
import java.io.IOException
import java.util.concurrent.{ LinkedBlockingQueue, TimeoutException }
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration._
import scala.language.implicitConversions

trait ChannelSyntax {
  implicit def asRichChannel(c: Channel): ChannelSyntax.RichChannel =
    ChannelSyntax.RichChannel(c)
}

object ChannelSyntax {

  final case class WriteThunk(buf: ByteBuf, p: Promise[Unit], flush: Boolean)

  final class RichChannelHandler(ch: Channel) extends ChannelDuplexHandler {

    // Thread safety: This handler assumes a dedicated instance will be created
    // and assigned to a single channel. Additionally, it does not support
    // concurrent users of send or recv. Only one thread/future coroutine should
    // use send, write, or recv at a time.

    var toSend: WriteThunk = null

    // The size of this queue is softly bounded by the fact that channel
    // autoread is turned off.
    val readQ = new LinkedBlockingQueue[ByteBuf]()
    val readP = new AtomicReference[Promise[ByteBuf]]()

    def send(buf: ByteBuf, flush: Boolean): Future[Unit] = {
      val p = Promise[Unit]()
      ch.eventLoop.submit { () =>
        if (toSend ne null) {
          // notify caller
          p.failure(new IllegalStateException("Existing pending write!"))
          buf.release()
        } else {
          toSend = WriteThunk(buf, p, flush)
          tryWrite()
        }
      }
      p.future
    }

    private def tryWrite() =
      if ((toSend ne null) && ch.isWritable) {
        val WriteThunk(buf, p, flush) = toSend
        toSend = null
        if (flush) ch.writeAndFlush(buf) else ch.write(buf)
        p.success(())
      }
 
    override def channelWritabilityChanged(ctx: ChannelHandlerContext) = 
      tryWrite()

    def isReadable = !readQ.isEmpty

    def recv(): Future[ByteBuf] =
      readQ.poll() match {
        case null =>
          val p = Promise[ByteBuf]()
          if (!readP.compareAndSet(null, p)) {
            // notify caller
            p.failure(new IllegalStateException("Existing pending read!"))
          } else {
            if (readQ.peek() ne null) {
              readP.getAndSet(null) match {
                case null => ()
                case p0 => p0.success(readQ.poll())
              }
            } else {
              ch.read()
            }
          }

          p.future

        case buf => Future.successful(buf)
      }

    override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef) = {
      readQ.put(msg.asInstanceOf[ByteBuf])

      readP.getAndSet(null) match {
        case null => ()
        case p =>
          readQ.poll() match {
            case null => 
              if (!readP.compareAndSet(null, p)) {
                throw new IllegalStateException("Unexpected pending read!")
              }
            case buf0 => p.success(buf0)
          }
      }
    }

    def idleTimeout(timeout: Duration): Unit = {
      try {
        ch.pipeline.remove("channel syntax idle trigger")
      } catch {
        case _: NoSuchElementException => ()
      }

      if (timeout != Duration.Inf) {
        val trigger = new IdleStateHandler(0, 0, timeout.length, timeout.unit)
        ch.pipeline.addBefore(
          "channel syntax handler",
          "channel syntax idle trigger",
          trigger
        )
      }
    }

    override def userEventTriggered(ctx: ChannelHandlerContext, ev: AnyRef) =
      ev match {
        case ev: IdleStateEvent if ev.state eq IdleState.ALL_IDLE =>
          closePendingState(ctx, new TimeoutException("Channel idle timeout"))
        case _ => ()
      }


    override def exceptionCaught(ctx: ChannelHandlerContext, ex: Throwable) = 
      closePendingState(ctx, ex)

    override def channelInactive(ctx: ChannelHandlerContext) = 
      closePendingState(ctx, new IOException("Channel closed"))

    private def closePendingState(ctx: ChannelHandlerContext, cause: => Throwable) = {
      lazy val ex = cause
      Option(toSend) foreach { _.p.tryFailure(ex) }
      Option(readP.get()) foreach { _.tryFailure(ex) }

      var buf = readQ.poll()
      while (buf ne null) {
        buf.release()
        buf = readQ.poll()
      }

      // ctx.channel.closeQuietly()
      try {
        ctx.channel.close()
      } catch {
        case _: IOException => ()
      }
    }
  }

  final case class RichChannel(ch: Channel) extends AnyVal {
    private def richHandler =
      ch.pipeline.last match {
        case h: ChannelSyntax.RichChannelHandler => h
        case _ =>
          val h = new RichChannelHandler(ch)
          ch.config.setAutoRead(false)
          ch.pipeline.addLast("channel syntax handler", h)
          h
      }

    /**
     * Setup the channel for async read and write.
     */
    def enableChannelSyntax(): Unit = richHandler

    /**
     * Write `buf` to the channel when the channel is writable. 
     * 
     * Does not support concurrent calls to send or sendAndFlush from different
     * threads/future-based coroutines. Concurrent calls to send and write also
     * do not provide any delivery order guarantees.
     * 
     * @param buf The buffer to write
     * @return    A future which is completed when `buf` is written to the channel.
     */
    def send(buf: ByteBuf): Future[Unit] = 
      richHandler.send(buf, false)

    /**
     * Write and flush `buf` to the channel when the channel is writable.
     * 
     * Does not support concurrent calls to send or sendAndFlush from different
     * threads/future-based coroutines. Concurrent calls to send and write also
     * do not provide any delivery order guarantees.
     * 
     * @param buf The buffer to write
     * @return    A future which is completed when `buf` is written to the channel.
     */
    def sendAndFlush(buf: ByteBuf): Future[Unit] =
      richHandler.send(buf, true)

    /**
     * Returns whether or not the channel has any data available to read. A
     * subsequent call to recv will not block.
     */
    def isReadable: Boolean =
      richHandler.isReadable

    /**
     * Asynchronously read one message from the channel.
     * 
     * Does not support concurrent calls to recv from different
     * threads/future-based coroutines.
     * 
     * @return A future which is completed with the read buffer.
     */
    def recv(): Future[ByteBuf] =
      richHandler.recv()

    /**
     * configure the channel to time out and close after no activity for
     * `timeout` duration
     */
    def idleTimeout(timeout: Duration) =
      richHandler.idleTimeout(timeout)

    /** Close the channe and squelch any IOExceptions */
    def closeQuietly(): Unit =
      try {
        ch.close()
      } catch {
        case _: IOException => ()
      }
  }
}
