package fauna.lang

import fauna.lang.syntax.array._
import fauna.lang.syntax.filechannel._
import io.netty.buffer.ByteBuf
import java.nio.channels.{ FileChannel, GatheringByteChannel }
import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.immutable.ArraySeq
import scala.language.implicitConversions

trait ByteBufSyntax {
  implicit def asRichByteBuf(buf: ByteBuf): ByteBufSyntax.RichByteBuf =
    ByteBufSyntax.RichByteBuf(buf)
}

object ByteBufSyntax {
  case class RichByteBuf(buf: ByteBuf) extends AnyVal {
    def toByteArray = {
      val arr = new Array[Byte](buf.readableBytes)
      buf.getBytes(buf.readerIndex, arr)
      arr
    }

    def toByteArraySeq = ArraySeq.unsafeWrapArray(toByteArray)

    def toUTF8String = buf.toString(UTF_8)

    def readAllBytes(c: GatheringByteChannel): ByteBuf = {
      while (buf.isReadable) buf.readBytes(c, buf.readableBytes)
      buf
    }

    def writeAllBytes(c: FileChannel): ByteBuf = {
      require(c.remaining <= Int.MaxValue)
      while (c.remaining > 0) buf.writeBytes(c, c.remaining.toInt)
      buf
    }

    def insertPadding(index: Int, size: Int): ByteBuf = {
      assert(size >= 0)

      buf.ensureWritable(size)

      // make some space
      var i = buf.writerIndex - 1
      while (i >= index) {
        buf.setByte(i + size, buf.getByte(i))
        i -= 1
      }

      buf.writerIndex(buf.writerIndex + size)
    }

    // Returns the index of the first mismatched byte in the inputs up to len, or
    // len if the bytebufs match. Obeys readerIndex position.
    def mismatchIndex(buf2: ByteBuf): Int =
      mismatchIndex(buf2, Math.min(buf.readableBytes, buf2.readableBytes))

    def mismatchIndex(buf2: ByteBuf, len: Int): Int = {
      var i = 0
      while (i < (len & ~7) && buf.getLong(buf.readerIndex + i) == buf2.getLong(buf2.readerIndex + i)) {
        i += 8
      }
      while (i < len && buf.getByte(buf.readerIndex + i) == buf2.getByte(buf2.readerIndex + i)) {
        i += 1
      }
      i
    }

    def insertBytes(index: Int, bytes: Array[Byte]): ByteBuf = {
      insertPadding(index, bytes.length)
      buf.setBytes(index, bytes)
    }

    def prependLength(f: ByteBuf => Unit, extra: Int = 0): ByteBuf = {
      val idx = buf.writerIndex
      buf.writerIndex(idx + 4)
      f(buf)
      buf.setInt(idx, buf.readableBytes - 4 + extra)
    }

    def toHexString = {
      val len = buf.readableBytes
      val bytes = new Array[Byte](len)
      buf.duplicate.readBytes(bytes, 0, len)
      bytes.toHexString
    }
  }
}
