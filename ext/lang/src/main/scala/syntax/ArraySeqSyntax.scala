package fauna.lang

import fauna.lang.syntax.array._
import io.netty.buffer.{ ByteBuf, Unpooled }
import scala.collection.immutable.ArraySeq
import scala.language.implicitConversions

trait ArraySeqSyntax {
  implicit def asRichArrayOfBytes(
    a: ArraySeq[Byte]): ArraySeqSyntax.RichArraySeqOfBytes =
    ArraySeqSyntax.RichArraySeqOfBytes(a)
}

object ArraySeqSyntax {
  case class RichArraySeqOfBytes(a: ArraySeq[Byte]) extends AnyVal {

    private def arr(a: ArraySeq[Byte]) =
      a.unsafeArray.asInstanceOf[Array[Byte]]

    def unsafeByteArray: Array[Byte] = arr(a)

    def unsafeByteBuf: ByteBuf = Unpooled.wrappedBuffer(arr(a))

    def xor(b: ArraySeq[Byte]): ArraySeq[Byte] =
      ArraySeq.unsafeWrapArray(arr(a) xor arr(b))

    def toHexString: String = arr(a).toHexString
  }
}
