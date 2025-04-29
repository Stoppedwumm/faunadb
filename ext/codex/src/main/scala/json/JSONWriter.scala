package fauna.codex.json2

import fauna.lang.syntax._
import io.netty.buffer.{ ByteBuf, ByteBufAllocator, ByteBufUtil }
import io.netty.util.AsciiString

object JSONWriter {
  val CommaByte = ','.toByte
  val ColonByte = ':'.toByte
  val BackslashByte = '\\'.toByte
  val QuoteByte = '"'.toByte
  val ArrayStartByte = '['.toByte
  val ArrayEndByte = ']'.toByte
  val ObjectStartByte = '{'.toByte
  val ObjectEndByte = '}'.toByte

  val TrueBytes = "true".toUTF8Bytes
  val FalseBytes = "false".toUTF8Bytes
  val NullBytes = "null".toUTF8Bytes

  val ControlEscapes: Array[Array[Byte]] = {
    val arr = ((0 to 0x1f) map { i =>
      ("\\u%04x" format i).toUTF8Bytes
    }).toArray

    arr(0x08) = "\\b".toUTF8Bytes
    arr(0x09) = "\\t".toUTF8Bytes
    arr(0x0c) = "\\f".toUTF8Bytes
    arr(0x0a) = "\\n".toUTF8Bytes
    arr(0x0d) = "\\r".toUTF8Bytes
    arr
  }

  val BackslashEscape = "\\\\".toUTF8Bytes
  val QuoteEscape = "\\\"".toUTF8Bytes

  private[JSONWriter] sealed trait State
  private[JSONWriter] final object ArrayStart extends State
  private[JSONWriter] final object ArrayElem extends State
  private[JSONWriter] final object ObjectStart extends State
  private[JSONWriter] final object ObjectField extends State
  private[JSONWriter] final object ObjectElem extends State

  final case class Marker(stack: List[State], index: Int)

  private[JSONWriter] val alloc = ByteBufAllocator.DEFAULT
}

final case class JSONWriter(buf: ByteBuf) {
  import JSONWriter._

  private[this] var stack: List[State] = Nil

  def marker: Marker = Marker(stack, buf.writerIndex)

  def isChanged(marker: Marker) = buf.writerIndex != marker.index

  def reset(marker: Marker) = {
    stack = marker.stack
    buf.writerIndex(marker.index)
  }

  def writeDelimiter(): Unit =
    stack match {
      case Nil                 => ()
      case ArrayStart :: rest  => stack = ArrayElem :: rest
      case ArrayElem :: _      => buf.writeByte(CommaByte)
      case ObjectStart :: rest => stack = ObjectField :: rest
      case ObjectField :: rest =>
        buf.writeByte(ColonByte)
        stack = ObjectElem :: rest
      case ObjectElem :: rest =>
        buf.writeByte(CommaByte)
        stack = ObjectField :: rest
    }

  def writeNumber(num: Long): JSONWriter = {
    writeDelimiter()
    ByteBufUtil.writeAscii(buf, num.toString)
    this
  }

  def writeUnsignedNumber(num: Long): JSONWriter = {
    writeDelimiter()
    ByteBufUtil.writeAscii(buf, num.toUnsignedString)
    this
  }

  def writeNumber(num: Double): JSONWriter = {
    writeDelimiter()
    val outputStr =
      if (num.isNaN) {
        "\"NaN\""
      } else if (num.isPosInfinity) {
        "\"Infinity\""
      } else if (num.isNegInfinity) {
        "\"-Infinity\""
      } else {
        num.toString
      }
    ByteBufUtil.writeAscii(buf, outputStr)
    this
  }

  def writeBoolean(bool: Boolean): JSONWriter = {
    writeDelimiter()
    buf.writeBytes(if (bool) TrueBytes else FalseBytes)
    this
  }

  def writeNull(): JSONWriter = {
    writeDelimiter()
    buf.writeBytes(NullBytes)
    this
  }

  def writeString(str: String): JSONWriter = {
    val buf = str.toUTF8Buf(JSONWriter.alloc)
    writeString(buf)
    buf.release()
    this
  }

  def writeString(str: AsciiString): JSONWriter = {
    writeDelimiter()
    buf.writeByte(QuoteByte)
    ByteBufUtil.copy(str, buf)
    buf.writeByte(QuoteByte)
    this
  }

  def writeString(str: ByteBuf): JSONWriter = {
    writeDelimiter()
    buf.writeByte(QuoteByte)

    var ridx = str.readerIndex

    def flush(negoff: Int) = {
      val len = str.readerIndex - ridx - negoff
      if (len > 0) buf.writeBytes(str, ridx, len)
      ridx = str.readerIndex
    }

    // scan the buffer up to the point where we run into an escape character.
    // flush read bytes up to that point, write the escape, and continue.
    while (str.isReadable) {
      str.readByte match {
        case ch if ch < 0 => // high bit is set
        case ch if ch < 0x20 =>
          flush(1)
          buf.writeBytes(ControlEscapes(ch))
        case QuoteByte =>
          flush(1)
          buf.writeBytes(QuoteEscape)
        case BackslashByte =>
          flush(1)
          buf.writeBytes(BackslashEscape)
        case _ =>
      }
    }

    flush(0)

    buf.writeByte(QuoteByte)
    this
  }

  def writeString(str: JSON.Escaped): JSONWriter = {
    writeDelimiter()
    buf.writeBytes(str.bytes)
    this
  }

  def writeArrayStart(): JSONWriter = {
    writeDelimiter()
    buf.writeByte(ArrayStartByte)
    stack = ArrayStart :: stack
    this
  }

  def writeArrayEnd(): JSONWriter = {
    buf.writeByte(ArrayEndByte)
    stack = stack.tail
    this
  }

  def writeObjectStart(): JSONWriter = {
    writeDelimiter()
    buf.writeByte(ObjectStartByte)
    stack = ObjectStart :: stack
    this
  }

  def writeObjectEnd(): JSONWriter = {
    buf.writeByte(ObjectEndByte)
    stack = stack.tail
    this
  }

  // other helpers

  // ensure null is written if thunk encodes nothing.
  def orWriteNull(thunk: => Any): JSONWriter = {
    val mk = marker
    thunk
    if (!isChanged(mk)) writeNull()
    this
  }

  // writes null if thunk encodes nothing.
  def writeObjectField(key: ByteBuf, thunk: => Any): JSONWriter = {
    writeString(key)
    orWriteNull(thunk)
  }

  def writeObjectField(key: JSON.Escaped, thunk: => Any): JSONWriter = {
    writeString(key)
    orWriteNull(thunk)
  }

  def writeObjectField(key: AsciiString, thunk: => Any): JSONWriter = {
    writeString(key)
    orWriteNull(thunk)
  }

  // rewinds the field write if thunk encodes nothing.
  def writeNonNullObjectField(key: ByteBuf, thunk: => Any): JSONWriter = {
    val mk1 = marker
    writeString(key)

    val mk2 = marker
    thunk
    if (!isChanged(mk2)) reset(mk1)
    this
  }

  def writeNonNullObjectField(key: JSON.Escaped, thunk: => Any): JSONWriter = {
    val mk1 = marker
    writeString(key)

    val mk2 = marker
    thunk
    if (!isChanged(mk2)) reset(mk1)
    this
  }
}
