package fauna.lang

import java.nio.charset.StandardCharsets.UTF_8
import io.netty.buffer._

trait StringSyntax {
  implicit class StringOps(s: String) {

    /**
      * No short circuit on character inequality which should defeat
      * a timing oracle looking to figure out a string a character at a time.
      */
    def secureEquals(other: String): Boolean = {
      val sizecmp = s.length ^ other.length
      val cmp = s.iterator.zip(other).foldLeft(sizecmp) { case (acc, (a, b)) => (a ^ b) | acc }

      cmp == 0
    }

    def quote = s"'${s.escapeQuote}'"

    def escapeQuote = s.replace("'", """\'""")

    def escapeDot = s.replace(".", """\.""")

    def jsPathComponents: List[String] =
      (s split """(?<!\\)\.""").toList map { _.replace("""\.""", ".") }

    def toUTF8Bytes: Array[Byte] = s.getBytes(UTF_8)
  }

  private[this] val MaxUTF8Bytes = UTF_8.newEncoder.maxBytesPerChar.toInt

  implicit class CharSequenceOps(s: CharSequence) {

    /**
      * Return an unpooled ByteBuf of the UTF-8 encoded bytes of the
      * string.
      */
    def toUTF8Buf: ByteBuf = {
      val len = s.length
      val buf = UnpooledByteBufAllocator.DEFAULT.heapBuffer(len, len * MaxUTF8Bytes)
      ByteBufUtil.writeUtf8(buf, s)
      buf
    }

    /**
      * Return a ByteBuf of the UTF-8 encoded bytes of the string,
      * using the provided allocator.
      */
    def toUTF8Buf(alloc: ByteBufAllocator): ByteBuf = ByteBufUtil.writeUtf8(alloc, s)
  }
}
