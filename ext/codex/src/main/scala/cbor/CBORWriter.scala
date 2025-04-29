package fauna.codex.cbor

import fauna.lang.Timestamp
import io.netty.buffer.{ ByteBuf, CompositeByteBuf, Unpooled }
import scala.math.BigInt

case class CopyingCBORWriter(buf: ByteBuf) extends CBORWriter {

  def unsafeWriteBytesRaw(bytes: ByteBuf, length: Int): CBORWriter = {
    buf.writeBytes(bytes, bytes.readerIndex, length)
    this
  }

  /**
    * Copies contents of str to this writer's buffer. Does not advance
    * the readerIndex of str.
    *
    * @param str the utf8 encoded string to write.
    */
  def writeString(str: ByteBuf): CBORWriter = {
    val length = str.readableBytes
    writeStringStart(length)
    // does not increase bytes.readerIndex
    buf.writeBytes(str, str.readerIndex, length)
    this
  }
}

final case class GatheringCBORWriter(composite: CompositeByteBuf) extends CBORWriter {
  var _buf: ByteBuf = null

  def buf = {
    if (_buf eq null) {
      _buf = if (composite.isDirect) {
        composite.alloc.directBuffer()
      } else {
        composite.alloc.heapBuffer()
      }
    }
    _buf
  }

  def flush() =
    if (_buf ne null) {
      composite.addComponent(true, _buf)
      _buf = null
    }

  /**
    * Adds the contents of bytes this writer's composite buffer. Does not advance
    * the readerIndex of bytes. Release ownership is transferred to this writer.
    *
    * @param bytes the buffer to write.
    */
  def unsafeWriteBytesRaw(bytes: ByteBuf, length: Int): CBORWriter = {
    if (length > 0) {
      flush()
      composite.addComponent(true, bytes)
    }

    this
  }

  /**
    * Adds the contents contents of str to this writer's buffer. Does not advance
    * the readerIndex of str. Release ownership is transferred to this writer.
    *
    * @param str the utf8 encoded string to write.
    */
  def writeString(str: ByteBuf): CBORWriter = {
    val length = str.readableBytes
    writeStringStart(length)

    if (length > 0) {
      flush()
      composite.addComponent(true, str)
    }

    this
  }
}

sealed abstract class CBORWriter {
  protected def buf: ByteBuf

  def writeInt(int: Long): CBORWriter = {
    if (int < 0) {
      writeTypeAndInt(int.abs - 1, InitialByte.NegIntMin)
    } else {
      writeTypeAndInt(int, InitialByte.UIntMin)
    }
    this
  }

  def writeBoolean(bool: Boolean): CBORWriter = {
    if (bool) buf.writeByte(InitialByte.True)
    else buf.writeByte(InitialByte.False)
    this
  }

  def writeNil(): CBORWriter = {
    buf.writeByte(InitialByte.Nil)
    this
  }

  def writeFloat(float: Float): CBORWriter = {
    buf.writeByte(InitialByte.Float)
    buf.writeFloat(float)
    this
  }

  def writeDouble(double: Double): CBORWriter = {
    buf.writeByte(InitialByte.Double)
    buf.writeDouble(double)
    this
  }

  def writeBigNum(num: BigInt): CBORWriter = {
    if (num >= 0) {
      buf.writeByte(InitialByte.PositiveBigNumTag)
      writeBytes(Unpooled.wrappedBuffer(num.toByteArray))
    } else {
      buf.writeByte(InitialByte.NegativeBigNumTag)
      writeBytes(Unpooled.wrappedBuffer((num.abs - 1).toByteArray))
    }
    this
  }

  def writeTimestamp(ts: Timestamp): CBORWriter = {
    buf.writeByte(InitialByte.EpochTimestampTag)

    if (ts.nanoOffset == 0) {
      writeInt(ts.seconds)
    } else {
      writeArrayStart(2)
      writeInt(ts.seconds)
      writeInt(ts.nanoOffset)
    }
  }

  def writeBytesStart(length: Int): CBORWriter = writeTypeAndInt(length, InitialByte.ByteStrMin)

  /**
   * Wildly unsafe, do not use, unless you need to inspect the serialized state
   * of a CBOR message subcomponent while maintaining backwards compatibility.
   */
  def unsafeWriteBytesRaw(bytes: ByteBuf, length: Int): CBORWriter

  /**
    * Copies contents of bytes this writer's buffer. Does not advance
    * the readerIndex of bytes.
    *
    * @param bytes the buffer to write.
    */
  def writeBytes(bytes: ByteBuf): CBORWriter = {
    val length = bytes.readableBytes
    writeBytesStart(length)
    unsafeWriteBytesRaw(bytes, length)
  }

  def writeStringStart(length: Int): CBORWriter = writeTypeAndInt(length, InitialByte.StrMin)

  def writeString(str: ByteBuf): CBORWriter

  def writeArrayStart(length: Long): CBORWriter = writeTypeAndInt(length, InitialByte.ArrayMin)

  def writeMapStart(length: Long): CBORWriter = writeTypeAndInt(length, InitialByte.MapMin)

  def writeTag(tag: Long): CBORWriter = writeTypeAndInt(tag, InitialByte.TagMin)

  private def writeTypeAndInt(i: Long, typeMin: Int): CBORWriter = {
    i match {
      case l if l < InitialByte.UInt8 =>
        buf.writeByte(typeMin + l.toInt)
      case l if l < Byte.MaxValue =>
        buf.writeByte(typeMin + InitialByte.UInt8)
        buf.writeByte(l.toInt)
      case l if l < Short.MaxValue =>
        buf.writeByte(typeMin + InitialByte.UInt16)
        buf.writeShort(l.toInt)
      case l if l < Int.MaxValue =>
        buf.writeByte(typeMin + InitialByte.UInt32)
        buf.writeInt(l.toInt)
      case l =>
        buf.writeByte(typeMin + InitialByte.UInt64)
        buf.writeLong(l)
    }
    this
  }
}
