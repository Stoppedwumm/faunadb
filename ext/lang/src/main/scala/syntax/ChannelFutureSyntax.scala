package fauna.lang

import io.netty.channel.{ Channel, ChannelFuture, ChannelFutureListener }
import java.util.concurrent.CancellationException
import scala.concurrent.{ Future, Promise }
import scala.language.implicitConversions

trait ChannelFutureSyntax {
  implicit def asRichChannel(cf: ChannelFuture): ChannelFutureSyntax.RichChannelFuture =
    ChannelFutureSyntax.RichChannelFuture(cf)
}

object ChannelFutureSyntax {
  case class RichChannelFuture(cf: ChannelFuture) extends AnyVal {
    def toFuture: Future[Channel] = {
      val rv = Promise[Channel]()

      //rv.setInterruptHandler {
        //case e: CancellationException => cf.cancel(true) // interrupt if running
      //}

      cf.addListener(new ChannelFutureListener {
        def operationComplete(self: ChannelFuture): Unit = {
          if (self.isSuccess) {
            rv.success(self.channel)
          } else if (self.isCancelled) {
            rv.failure(new CancellationException)
          } else {
            rv.failure(self.cause)
          }
        }
      })

      rv.future
    }
  }
}
