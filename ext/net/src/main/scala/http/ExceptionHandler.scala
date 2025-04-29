package fauna.net.http

import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import io.netty.channel._
import io.netty.channel.unix.Errors$NativeIoException
import java.io.IOException

/** This default exception handler is the first/last handler in the
  * pipeline, and serves only to log uncaught exceptions and close
  * failed channels.
  *
  * If exceptions are logged by this handler, they should be caught
  * and handled by another handler - they are bugs.
  */
@ChannelHandler.Sharable
final class ExceptionHandler extends ChannelDuplexHandler with ExceptionLogging {

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit =
    try {
      super.channelRead(ctx, msg)
    } catch {
      case e: Throwable =>
        // Netty doesn't seem to call exceptionCaught() reliably when
        // a read error occurs. Do it here instead.
        exceptionCaught(ctx, e)
    }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable): Unit = {
    e match {
      case native: Errors$NativeIoException =>
        // Attempt to add additional socket info. to a native I/O
        // exception for correlation with other systems.
        val local = ctx.channel.localAddress()
        val remote = ctx.channel.remoteAddress()

        logException(new IOException(s"$local -> $remote", native))

      case _ => logException(e)
    }

    ctx.channel.closeQuietly()
  }
}
