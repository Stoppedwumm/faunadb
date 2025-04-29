package fauna.storage.cassandra.comparators

import fauna.codex.builder._
import fauna.lang.syntax._
import fauna.storage.ComponentTooLargeException
import fauna.storage.cassandra.{ CValue, CReader, CBuilder }
import io.netty.buffer.{ ByteBuf, Unpooled }
import java.util.Date

object CodecFormat extends BinaryCodecFormat[CReader, CBuilder]
    with TupleCodecFormat[CReader, CBuilder] {

  // TODO: readers should check CValue's type, if possible.

  private def readFromBuf[T](data: CReader)(f: (ByteBuf, Int) => T): T = {
    val buf = data.readNext().bytes
    f(buf, buf.readerIndex())
  }

  def readByte(data: CReader) = readFromBuf(data) { _.getByte(_) }
  def readShort(data: CReader) = readFromBuf(data) { _.getInt(_).toShort }
  def readInt(data: CReader) = readFromBuf(data) { _.getInt(_) }
  def readLong(data: CReader) = readFromBuf(data) { _.getLong(_) }
  def readFloat(data: CReader) = readFromBuf(data) { _.getFloat(_) }
  def readDouble(data: CReader) = readFromBuf(data) { _.getDouble(_) }
  def readBoolean(data: CReader) = readFromBuf(data) { _.getBoolean(_) }

  def readChar(data: CReader) = data.readNext().bytes.toUTF8String.apply(0)
  def readString(data: CReader) = data.readNext().bytes.toUTF8String

  def readBytes(data: CReader) = data.readNext().bytes.slice()

  def readTuple[T](data: CReader, count: Int)(f: (Int, CReader) => Unit) = {
    var i = 0
    while (i < count) { f(i, data); i += 1 }
  }

  def skipField(data: CReader): Unit = data.readNext()

  def readDate(data: CReader) = new Date(readLong(data))
  def readBigInt(data: CReader) = BigInt(readBytes(data).toByteArray)
  def readBigDecimal(data: CReader) = {
    val buf = readBytes(data)
    val scale = buf.readInt
    BigDecimal(BigInt(buf.toByteArray), scale)
  }

  // Write

  def writeByte(builder: CBuilder, t: Byte) = builder.add(CValue(BytesType, Unpooled.wrappedBuffer(Array(t))))
  def writeShort(builder: CBuilder, t: Short) = builder.add(CValue(Int32Type, Unpooled.copyInt(t)))
  def writeInt(builder: CBuilder, t: Int) = builder.add(CValue(Int32Type, Unpooled.copyInt(t)))
  def writeLong(builder: CBuilder, t: Long) = builder.add(CValue(LongType, Unpooled.copyLong(t)))
  def writeFloat(builder: CBuilder, t: Float) = builder.add(CValue(FloatType, Unpooled.copyFloat(t)))
  def writeDouble(builder: CBuilder, t: Double) = builder.add(CValue(DoubleType, Unpooled.copyDouble(t)))
  def writeBoolean(builder: CBuilder, t: Boolean) = builder.add(CValue(BooleanType, Unpooled.copyBoolean(t)))
  def writeChar(builder: CBuilder, t: Char) = builder.add(CValue(UTF8Type, t.toString.toUTF8Buf))
  def writeString(builder: CBuilder, t: String) = builder.add(CValue(UTF8Type, checkLength(t.toUTF8Buf)))

  def writeBytes(builder: CBuilder, t: ByteBuf) = builder.add(CValue(BytesType, checkLength(t.slice)))

  def writeTuple(builder: CBuilder, size: Int)(f: CBuilder => Unit) = f(builder)

  private def checkLength(buf: ByteBuf): ByteBuf = {
    if (buf.readableBytes > Short.MaxValue) {
        throw ComponentTooLargeException(s"buf length ${buf.readableBytes} exceeds ${Short.MaxValue}")
    }
    buf
  }
}
