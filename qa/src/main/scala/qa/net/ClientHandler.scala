package fauna.qa.net

import fauna.exec.Timer
import io.netty.channel._
import java.nio.channels.ClosedChannelException
import java.util.concurrent.TimeoutException
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

/**
  * A SimpleChannelInboundHandler could not be used here as it was unable to
  * determine the class it should match on. We have to do that here.
  */
private class ClientHandler[T: ClassTag] extends ChannelInboundHandlerAdapter {
  private[this] val respP = Promise[T]()
  def future: Future[T] = respP.future

  def timeoutAfter(timeout: Duration)(implicit timer: Timer): Unit =
    timer.timeoutPromise(respP, timeout, new TimeoutException())

  override def channelRead(ctx: ChannelHandlerContext, obj: Object): Unit =
    obj match {
      case msg: T => respP.trySuccess(msg)
      case _      => ctx.fireChannelRead(obj)
    }

  override def exceptionCaught(
    ctx: ChannelHandlerContext,
    cause: Throwable
  ): Unit = {
    respP.tryFailure(cause)
    ctx.fireExceptionCaught(cause)
  }

  override def channelWritabilityChanged(ctx: ChannelHandlerContext): Unit = {
    if (!ctx.channel.isWritable) respP.tryFailure(new ClosedChannelException())
    ctx.fireChannelWritabilityChanged()
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    respP.tryFailure(new ClosedChannelException())
    ctx.fireChannelInactive()
  }
}
