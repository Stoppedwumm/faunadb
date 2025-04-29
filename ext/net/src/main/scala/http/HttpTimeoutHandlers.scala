package fauna.net.http

import fauna.stats.StatsRecorder
import io.netty.channel.{ ChannelDuplexHandler, ChannelHandlerContext, ChannelPromise }
import io.netty.handler.codec.http.{ FullHttpMessage, LastHttpContent }
import io.netty.handler.codec.http.{ HttpMessage => NettyHttpMessage }
import io.netty.handler.timeout.{ IdleState, IdleStateEvent, IdleStateHandler }
import io.netty.handler.timeout.{ ReadTimeoutException => NettyReadTimeout }
import scala.concurrent.duration._

class HttpClientTimeoutHandler(responseTimeout: Duration) extends ChannelDuplexHandler {
  @volatile private var waiting: Boolean = false

  private val trigger =
    new IdleStateHandler(responseTimeout.length, 0, 0, responseTimeout.unit)

  override def handlerAdded(ctx: ChannelHandlerContext) =
    ctx.pipeline.addBefore(ctx.name, "idle trigger", trigger)

  override def handlerRemoved(ctx: ChannelHandlerContext) =
    ctx.pipeline.remove(trigger)

  override def write(ctx: ChannelHandlerContext, e: AnyRef, p: ChannelPromise) = {
    e match {
      case _: FullHttpMessage | _: LastHttpContent => waiting = true
      case _ => ()
    }

    ctx.write(e, p)
  }

  override def channelRead(ctx: ChannelHandlerContext, e: AnyRef) = {
    e match {
      case _: FullHttpMessage | _: LastHttpContent => waiting = false
      case _ => ()
    }

    ctx.fireChannelRead(e)
  }

  /**
    * We receive idle state events from the IdleStateHandler
    * configured on HttpClient construction. Interpret READER_IDLE as
    * a read timeout.
    */
  override def userEventTriggered(ctx: ChannelHandlerContext, e: AnyRef): Unit =
    e match {
      case e: IdleStateEvent =>
        e.state match {
          case IdleState.READER_IDLE if waiting => throw NettyReadTimeout.INSTANCE
          case _ => ()
        }
      case _ => ()
    }
}

class HttpServerTimeoutHandler(
  readTimeout: Duration,
  keepAliveTimeout: Duration,
  stats: StatsRecorder
) extends ChannelDuplexHandler {

  @volatile private var waiting: Boolean = false
  @volatile private var idle: Boolean = false

  private val trigger =
    new IdleStateHandler(readTimeout.toMillis, 0, keepAliveTimeout.toMillis, MILLISECONDS)

  override def handlerAdded(ctx: ChannelHandlerContext) =
    ctx.pipeline.addBefore(ctx.name, "idle trigger", trigger)

  override def handlerRemoved(ctx: ChannelHandlerContext) =
    ctx.pipeline.remove(trigger)

  override def channelRead(ctx: ChannelHandlerContext, e: AnyRef) = {
    e match {
      case _: FullHttpMessage => idle = false
      case _: NettyHttpMessage => idle = false; waiting = true
      case _: LastHttpContent => waiting = false
      case _ => ()
    }

    ctx.fireChannelRead(e)
  }

  override def write(ctx: ChannelHandlerContext, e: AnyRef, p: ChannelPromise) = {
    e match {
      case _: FullHttpMessage | _: LastHttpContent => idle = true
      case _ => ()
    }

    ctx.write(e, p)
  }

  /**
    * We receive idle state events from the IdleStateHandler
    * configured on HttpServer construction. Interpret READER_IDLE as
    * a read timeout, and ALL_IDLE as keepalive disconnect.
    */
  override def userEventTriggered(ctx: ChannelHandlerContext, e: AnyRef): Unit =
    e match {
      case e: IdleStateEvent =>
        e.state match {
          case IdleState.READER_IDLE if waiting =>
            throw NettyReadTimeout.INSTANCE

          case IdleState.ALL_IDLE if idle =>
            stats.incr("HTTP.Connections.Dropped")
            ctx.channel.close()

          case _ => ()
        }
      case _ => ()
    }
}
