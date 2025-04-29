package fauna.qa.net

import fauna.codex.cbor._
import io.netty.handler.codec.{ LengthFieldBasedFrameDecoder, LengthFieldPrepender }
import io.netty.buffer.ByteBuf
import io.netty.channel.{ ChannelHandlerContext, ChannelPipeline }
import io.netty.handler.codec.ByteToMessageCodec
import java.util.{ List => JList }
import scala.reflect.ClassTag
import scala.util.{ Failure, Success, Try }

private class CBORHandler[T](implicit tag: ClassTag[T], codec: CBOR.Codec[T])
    extends ByteToMessageCodec[T](tag.runtimeClass.asInstanceOf[Class[_ <: T]]) {

  override def encode(ctx: ChannelHandlerContext, msg: T, out: ByteBuf): Unit =
    Try(CBOR.encode(out, msg)) match {
      case Success(_)   => ()
      case Failure(err) => ctx.fireExceptionCaught(err)
    }

  override def decode(
    ctx: ChannelHandlerContext,
    in: ByteBuf,
    out: JList[Object]
  ): Unit =
    Try(CBOR.decode[T](in).asInstanceOf[Object]) match {
      case Failure(err) => ctx.fireExceptionCaught(err)
      case Success(msg) => out.add(msg)
    }
}

object Codec {
  val FrameLengthSize = 8
  val MaxFrameLength = Int.MaxValue

  def server[Req: CBOR.Codec: ClassTag, Rep: CBOR.Codec: ClassTag](
    pipeline: ChannelPipeline
  ): ChannelPipeline =
    init[Req, Rep](pipeline)

  def client[Req: CBOR.Codec: ClassTag, Rep: CBOR.Codec: ClassTag](
    pipeline: ChannelPipeline
  ): ChannelPipeline =
    init[Rep, Req](pipeline)

  private def init[IN: CBOR.Codec: ClassTag, OUT: CBOR.Codec: ClassTag](
    pipeline: ChannelPipeline
  ): ChannelPipeline =
    pipeline
      .addLast("frame-encoder", new LengthFieldPrepender(FrameLengthSize))
      .addLast(
        "frame-decoder",
        new LengthFieldBasedFrameDecoder(
          MaxFrameLength,
          0,
          FrameLengthSize,
          0,
          FrameLengthSize
        )
      )
      .addLast("decoder", new CBORHandler[IN])
      .addLast("encoder", new CBORHandler[OUT])
}
