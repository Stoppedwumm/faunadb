package fauna.net.http

import fauna.lang.clocks.Clock
import fauna.lang.Timestamp
import fauna.stats.{ QueryMetrics, StatsRecorder }
import io.netty.buffer.ByteBuf
import io.netty.channel.{
  ChannelDuplexHandler,
  ChannelHandler,
  ChannelHandlerContext,
  ChannelPromise
}
import io.netty.util.AttributeKey
import java.util.concurrent.atomic.LongAdder
import scala.concurrent.duration._

@ChannelHandler.Sharable
class HttpServerStatsHandler(stats: StatsRecorder) extends ChannelDuplexHandler {
  private[this] val connectionCount = new LongAdder
  private[this] val ConnectionDurationKey =
    AttributeKey.valueOf[Timestamp]("conn_duration")

  StatsRecorder.polling(10.seconds) {
    stats.set("HTTP.Connections", connectionCount.sum)
  }

  def incrConnects() = {
    stats.incr("HTTP.Connections.Opened")
    connectionCount.increment()
  }

  def incrCloses() = {
    stats.incr("HTTP.Connections.Closed")
    connectionCount.decrement()
  }

  def connDuration(time: Long) = stats.set("HTTP.Connections.Duration", time)
  def sentBytes(bytes: Int) = stats.count(QueryMetrics.BytesOut, bytes)
  def recvdBytes(bytes: Int) = stats.count(QueryMetrics.BytesIn, bytes)

  // StatsHandler gets added after the connection has been established
  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    incrConnects()
    ctx.channel.attr(ConnectionDurationKey).set(Clock.time)
    super.handlerAdded(ctx)
  }

  override def write(
    ctx: ChannelHandlerContext,
    msg: Object,
    p: ChannelPromise): Unit = {
    msg match {
      case buf: ByteBuf => sentBytes(buf.readableBytes)
      case _            =>
    }

    super.write(ctx, msg, p)
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
    msg match {
      case buf: ByteBuf => recvdBytes(buf.readableBytes)
      case _            =>
    }

    super.channelRead(ctx, msg)
  }

  // consider the connection to have been closed after it's de-registered from the
  // event loop
  override def channelUnregistered(ctx: ChannelHandlerContext): Unit = {
    incrCloses()
    val startAt = ctx.channel.attr(ConnectionDurationKey).get
    connDuration(Clock.micros - startAt.micros)
    super.channelUnregistered(ctx)
  }
}
