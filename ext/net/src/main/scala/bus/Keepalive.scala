package fauna.net.bus.transport

import fauna.lang.syntax._
import fauna.net.bus.{ MessageBus, SignalID }
import io.netty.buffer.{ ByteBuf, Unpooled }
import io.netty.channel._
import io.netty.handler.timeout.{ IdleState, IdleStateEvent, IdleStateHandler }
import scala.concurrent.duration._

class KeepalivePingSend(pingInterval: Duration)
    extends ChannelInboundHandlerAdapter {

  private val trigger =
    new IdleStateHandler(0, pingInterval.length, 0, pingInterval.unit)
  private val log = getLogger

  override def handlerAdded(ctx: ChannelHandlerContext) =
    ctx.pipeline.addBefore(ctx.name, "keepalive ping send trigger", trigger)

  override def handlerRemoved(ctx: ChannelHandlerContext) =
    ctx.pipeline.remove(trigger)

  override def userEventTriggered(ctx: ChannelHandlerContext, e: AnyRef) =
    e match {
      case e: IdleStateEvent if e.state == IdleState.WRITER_IDLE =>
        def remote = ctx.channel.remoteAddress
        if (ctx.channel.isWritable) {
          log.trace(s"Sending keepalive ping to $remote")
          ctx.channel.writeAndFlush(Unpooled.wrappedBuffer(new Array[Byte](1)))
        } else {
          log.trace(
            s"Cannot send keepalive ping to $remote: Channel is not writable")
        }
      case _ =>
        super.userEventTriggered(ctx, e)
    }
}

class KeepalivePingReceive(
  pingTimeout: Duration,
  signalID: Option[SignalID],
  stats: MessageBus.Stats)
    extends ChannelInboundHandlerAdapter {

  private val trigger =
    new IdleStateHandler(pingTimeout.length, 0, 0, pingTimeout.unit)
  private val log = getLogger

  override def handlerAdded(ctx: ChannelHandlerContext) =
    ctx.pipeline.addBefore(ctx.name, "keepalive ping receive trigger", trigger)

  override def handlerRemoved(ctx: ChannelHandlerContext) =
    ctx.pipeline.remove(trigger)

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef) =
    msg match {
      case b: ByteBuf =>
        def remote = ctx.channel.remoteAddress
        while (b.isReadable) {
          if (b.readByte != 0) {
            log.error(s"Unexpected non-zero keepalive read from $remote")
          } else {
            log.trace(s"Received keepalive ping from $remote")
          }
        }
        b.release()
      case o =>
        log.error(s"Unexpected non-bytebuf read from keepalive receiver")
        super.channelRead(ctx, o)
    }

  override def userEventTriggered(ctx: ChannelHandlerContext, e: AnyRef) =
    e match {
      case e: IdleStateEvent if e.state == IdleState.READER_IDLE =>
        def connDesc =
          signalID.fold("connection")(id => s"dedicated connection ${id.toInt}")
        stats.incrKeepaliveTimeouts()
        log.warn(
          s"Closing $connDesc with ${ctx.channel.remoteAddress} due to missed keepalive pings")
        ctx.channel.close()
      case _ =>
        super.userEventTriggered(ctx, e)
    }
}
