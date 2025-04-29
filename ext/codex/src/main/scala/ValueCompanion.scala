package fauna.codex

import java.io.{ ByteArrayOutputStream, OutputStream }
import io.netty.buffer.{ ByteBuf, ByteBufOutputStream, Unpooled }

trait ValueParser[T] {
  def parse(buf: ByteBuf): T

  def parse(bytes: Array[Byte]): T =
    parse(Unpooled.wrappedBuffer(bytes))
}

trait ValueWriter {
  def writeTo(out: OutputStream): Unit

  def writeTo(buf: ByteBuf): Unit =
    writeTo(new ByteBufOutputStream(buf))

  def byteBuf: ByteBuf = {
    val buf = Unpooled.buffer
    writeTo(buf)
    buf
  }

  def bytes: Array[Byte] = {
    val out = new ByteArrayOutputStream
    writeTo(out)
    out.toByteArray
  }
}
