package fauna.codex

import fauna.codex.builder._
import java.io.{ ByteArrayOutputStream, OutputStream }
import io.netty.buffer.{ ByteBuf, Unpooled }
import scala.util.Try

class EncodingException(msg: String) extends Exception(msg)
class DecodingException(msg: String) extends Exception(msg)

trait CodecCompanion[In, Out] {

  val base: CodecType[In, Out]

  def encode[T](stream: OutputStream, t: T)(implicit enc: base.Encoder[T]): Unit

  def decode[T: base.Decoder](buf: ByteBuf): Try[T]

  def decode[T: base.Decoder](bytes: Array[Byte]): Try[T] =
    decode(Unpooled.wrappedBuffer(bytes))

  def encode[T: base.Encoder](t: T): Array[Byte] = {
    val out = new ByteArrayOutputStream
    encode(out, t)
    out.toByteArray
  }
}
