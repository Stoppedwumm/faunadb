package fauna.net.bus.transport

import fauna.lang.syntax._
import fauna.net.bus.SignalID
import io.netty.channel._
import io.netty.handler.timeout.{ IdleState, IdleStateEvent, IdleStateHandler }
import scala.concurrent.duration._

object CloseOnIdleHandler {
  def Read(timeout: FiniteDuration, signalID: Option[SignalID]) = {
    val ev = IdleState.READER_IDLE
    val trigger = new IdleStateHandler(timeout.length, 0, 0, timeout.unit)
    new CloseOnIdleHandler(trigger, ev, timeout, signalID)
  }

  def Write(timeout: FiniteDuration, signalID: Option[SignalID]) = {
    val ev = IdleState.WRITER_IDLE
    val trigger = new IdleStateHandler(0, timeout.length, 0, timeout.unit)
    new CloseOnIdleHandler(trigger, ev, timeout, signalID)
  }

  def All(timeout: FiniteDuration, signalID: Option[SignalID]) = {
    val ev = IdleState.ALL_IDLE
    val trigger = new IdleStateHandler(0, 0, timeout.length, timeout.unit)
    new CloseOnIdleHandler(trigger, ev, timeout, signalID)
  }
}

class CloseOnIdleHandler(
  trigger: IdleStateHandler,
  eventState: IdleState,
  timeout: FiniteDuration,
  signalID: Option[SignalID])
    extends ChannelInboundHandlerAdapter {

  private def activityString = eventState match {
    case IdleState.READER_IDLE => "read"
    case IdleState.WRITER_IDLE => "write"
    case IdleState.ALL_IDLE    => "read/write"
  }

  override def handlerAdded(ctx: ChannelHandlerContext) =
    ctx.pipeline.addBefore(ctx.name, "close on idle trigger", trigger)

  override def handlerRemoved(ctx: ChannelHandlerContext) =
    ctx.pipeline.remove(trigger)

  override def userEventTriggered(ctx: ChannelHandlerContext, e: AnyRef) =
    e match {
      case e: IdleStateEvent if e.state == eventState =>
        def connDesc =
          signalID.fold("connection")(id => s"dedicated connection ${id.toInt}")
        getLogger.info(
          s"Closing $connDesc with ${ctx.channel.remoteAddress} after $timeout of $activityString inactivity")
        ctx.channel.close()
      case _ =>
        super.userEventTriggered(ctx, e)
    }
}
