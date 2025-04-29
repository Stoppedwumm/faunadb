package fauna.net.bus.transport

import io.netty.channel.ChannelPipeline
import io.netty.handler.codec._

object FrameCodec {
  val MaxMessageBytes = 512 * 1024 * 1024

  // See netty's documentation for the meaning of these fields.
  private val LengthFieldOffset = 0
  private val LengthFieldLength = 4
  private val LengthAdjustment = 0
  private val BytesToStrip = 4

  def newDecoder =
    new LengthFieldBasedFrameDecoder(
      MaxMessageBytes,
      LengthFieldOffset,
      LengthFieldLength,
      LengthAdjustment,
      BytesToStrip
    )

  def newEncoder =
    new LengthFieldPrepender(
      LengthFieldLength,
      LengthAdjustment
    )

  def addLast(pipeline: ChannelPipeline): Unit = {
    pipeline.addLast("frame decoder", newDecoder)
    pipeline.addLast("frame encoder", newEncoder)
  }
}
