package fauna.codex.cbor

import fauna.lang.syntax._
import fauna.lang.Timestamp
import io.netty.buffer.{ ByteBuf, Unpooled }
import scala.math.BigInt
import CBORParser._
import TypeLabels._

object CBORParser {
  // number of high bits containing the "major type" in the leading
  // byte of a CBOR value
  val MajorTypeBits = 5

  // mask for bits containing "additional info" in the leading byte of
  // a CBOR value
  val InfoMask = 0x1f

  def apply(bytes: Array[Byte]): CBORParser =
    CBORParser(Unpooled.wrappedBuffer(bytes))

  final object BytesSwitch extends PartialCBORSwitch[ByteBuf](ByteStringLabel) {
    override def readBytes(b: ByteBuf, stream: CBORParser) =
      stream.bufRetentionPolicy.read(b)
  }

  final object TagSwitch extends PartialCBORSwitch[Long](TagLabel) {
    override def readTag(tag: Long, stream: CBORParser) = tag
  }

  final object ArrayStartSwitch extends PartialCBORSwitch[Long](ArrayLabel) {
    override def readArrayStart(len: Long, stream: CBORParser) = len
  }

  final object MapStartSwitch extends PartialCBORSwitch[Long](MapLabel) {
    override def readMapStart(len: Long, stream: CBORParser) = len
  }

  final case class ShowSwitch(str: StringBuilder) extends CBORSwitch[Unit] {
    def readInt(int: Long, stream: CBORParser) = str.append(int)
    def readBoolean(bool: Boolean, stream: CBORParser) = str.append(bool)
    def readNil(stream: CBORParser) = str.append("nil")
    def readFloat(float: Float, stream: CBORParser) = str.append(float)
    def readDouble(double: Double, stream: CBORParser) = str.append(double)
    def readBigNum(num: BigInt, stream: CBORParser) = str.append(num)
    def readTimestamp(ts: Timestamp, stream: CBORParser) = str.append(ts)

    def readBytes(buf: ByteBuf, stream: CBORParser) = {
      str.append("h'")
      while (buf.isReadable) {
        str.append(buf.readByte match {
          case 0x08                   => "\\b"
          case 0x09                   => "\\t"
          case 0x0c                   => "\\f"
          case 0x0a                   => "\\n"
          case 0x0d                   => "\\r"
          case 0x27                   => "\\'"
          case 0x5c                   => "\\\\"
          case b if b > 31 && b < 127 => b.toChar.toString
          case b =>
            val hex = (b.toInt & 0xff).toHexString
            if (hex.length == 1) s"\\x0$hex" else s"\\x$hex"
        })
      }
      str.append("'")
    }

    def readString(buf: ByteBuf, stream: CBORParser) = {
      str.append("\"")
      str.append(buf.toUTF8String)
      str.append("\"")
    }

    def readArrayStart(length: Long, stream: CBORParser) = {
      str.append("[")

      var i = 0L
      while (i < length) {
        stream.read(this)
        if (i < length - 1) str.append(", ")
        i += 1
      }

      str.append("]")
    }

    def readMapStart(length: Long, stream: CBORParser) = {
      str.append("{")

      var i = 0L
      while (i < length) {
        stream.read(this)
        str.append(" : ")
        stream.read(this)
        if (i < length - 1) str.append(", ")
        i += 1
      }
      str.append("}")
    }

    def readTag(tag: Long, stream: CBORParser) = {
      def showTag() = {
        str.append(tag)
        str.append("(")
        stream.read(this)
        str.append(")")
      }
      tag match {
        case CBOR.UUIDTag =>
          // try interpreting tag 37 as a uuid, else fall back to raw repr
          val pos = stream.buf.readerIndex()
          try {
            str.append(stream.read(CBOR.UUIDCodec))
          } catch {
            case _: CBORUnexpectedTypeException =>
              stream.buf.readerIndex(pos)
              showTag()
          }
        case _ => showTag()
      }
    }
  }

  final object SkipSwitch extends CBORSwitch[Unit] {
    def readInt(int: Long, stream: CBORParser) = ()
    def readBoolean(bool: Boolean, stream: CBORParser) = ()
    def readNil(stream: CBORParser) = ()
    def readFloat(float: Float, stream: CBORParser) = ()
    def readDouble(double: Double, stream: CBORParser) = ()
    def readBigNum(num: BigInt, stream: CBORParser) = ()
    def readTimestamp(ts: Timestamp, stream: CBORParser) = ()

    def readBytes(bytes: ByteBuf, stream: CBORParser) = ()
    def readString(str: ByteBuf, stream: CBORParser) = ()

    def readArrayStart(length: Long, stream: CBORParser) = {
      var i = 0

      while (i < length) {
        stream.read(this)
        i += 1
      }
    }

    def readMapStart(length: Long, stream: CBORParser) = {
      var i = 0

      while (i < length) {
        stream.read(this)
        stream.read(this)
        i += 1
      }
    }

    def readTag(tag: Long, stream: CBORParser) =
      stream.read(this)
  }

  // Epoch-based timestamps use tag 1. RFC 7049 describes integer and
  // floating point timestamps. In order to support precise nanosecond
  // timestamps, we support integer timestamps and timestamps of a
  // tuple of (seconds, nanosecond offset), with the same semantics as
  // java Instant.
  final object TimestampSwitch extends PartialCBORSwitch[Timestamp](TypeLabels.TimestampLabel) {
    private final object IntComponentSwitch extends PartialCBORSwitch[Long](TypeLabels.IntLabel) {
      override def readInt(int: Long, stream: CBORParser) = int
    }

    override def readInt(int: Long, stream: CBORParser) = Timestamp.ofSeconds(int)

    override def readArrayStart(len: Long, stream: CBORParser) = {
      assert(len == 2)

      val seconds = stream.read(IntComponentSwitch)
      val nanos = stream.read(IntComponentSwitch)

      Timestamp(seconds, nanos)
    }
  }
}

trait CBORSwitch[+R] {
  def readInt(int: Long, stream: CBORParser): R
  def readBoolean(bool: Boolean, stream: CBORParser): R
  def readNil(stream: CBORParser): R
  def readFloat(float: Float, stream: CBORParser): R
  def readDouble(double: Double, stream: CBORParser): R
  def readBigNum(num: BigInt, stream: CBORParser): R
  def readTimestamp(ts: Timestamp, stream: CBORParser): R
  def readArrayStart(length: Long, stream: CBORParser): R
  def readMapStart(length: Long, stream: CBORParser): R
  def readTag(tag: Long, stream: CBORParser): R

  // The caller is responsible for applying stream.bufRetentionPolicy if `bytes`
  // escapes the decode call in order to ensure it is properly retained/copied.
  def readBytes(bytes: ByteBuf, stream: CBORParser): R
  // The caller is responsible for applying stream.bufRetentionPolicy if `str`
  // escapes the decode call in order to ensure it is properly retained/copied.
  def readString(str: ByteBuf, stream: CBORParser): R

  def readEndOfStream(stream: CBORParser): R = throw new NoSuchElementException("End of stream.")
}

final case class FailingCBORSwitch(expected: String) extends CBORSwitch[Nothing] {
  private def error(provided: String) = throw CBORUnexpectedTypeException(provided, expected)

  def readInt(int: Long, stream: CBORParser) = error(IntLabel)
  def readBoolean(bool: Boolean, stream: CBORParser) = error(BooleanLabel)
  def readNil(stream: CBORParser) = error(NilLabel)
  def readFloat(float: Float, stream: CBORParser) = error(FloatLabel)
  def readDouble(double: Double, stream: CBORParser) = error(DoubleLabel)
  def readBigNum(num: BigInt, stream: CBORParser) = error(BigNumLabel)
  def readTimestamp(ts: Timestamp, stream: CBORParser) = error(TimestampLabel)
  def readBytes(bytes: ByteBuf, stream: CBORParser) = error(ByteStringLabel)
  def readString(str: ByteBuf, stream: CBORParser) = error(UTF8StringLabel)
  def readArrayStart(length: Long, stream: CBORParser) = error(ArrayLabel)
  def readMapStart(length: Long, stream: CBORParser) = error(MapLabel)
  def readTag(tag: Long, stream: CBORParser) = error(TagLabel)
}

trait DelegatingCBORSwitch[I, R] extends CBORSwitch[R] {

  val delegate: CBORSwitch[I]

  def transform(i: I): R

  def readInt(int: Long, stream: CBORParser) = transform(
    delegate.readInt(int, stream))
  def readBoolean(bool: Boolean, stream: CBORParser) = transform(
    delegate.readBoolean(bool, stream))
  def readNil(stream: CBORParser) = transform(delegate.readNil(stream))
  def readFloat(float: Float, stream: CBORParser) = transform(
    delegate.readFloat(float, stream))
  def readDouble(double: Double, stream: CBORParser) = transform(
    delegate.readDouble(double, stream))
  def readBigNum(num: BigInt, stream: CBORParser) = transform(
    delegate.readBigNum(num, stream))
  def readTimestamp(ts: Timestamp, stream: CBORParser) = transform(
    delegate.readTimestamp(ts, stream))
  def readBytes(bytes: ByteBuf, stream: CBORParser) = transform(
    delegate.readBytes(bytes, stream))
  def readString(str: ByteBuf, stream: CBORParser) = transform(
    delegate.readString(str, stream))
  def readArrayStart(length: Long, stream: CBORParser) = transform(
    delegate.readArrayStart(length, stream))
  def readMapStart(length: Long, stream: CBORParser) = transform(
    delegate.readMapStart(length, stream))
  def readTag(tag: Long, stream: CBORParser) = transform(
    delegate.readTag(tag, stream))
}

abstract class PartialCBORSwitch[R](expected: String = null)
    extends DelegatingCBORSwitch[R, R] {
  val delegate = FailingCBORSwitch(expected)
  def transform(r: R) = r
}

final case class CBORParser(
  buf: ByteBuf,
  bufRetentionPolicy: CBOR.BufRetention = CBOR.BufRetention.Borrow) {
  def currentTypeByte = buf.getUnsignedByte(buf.readerIndex)

  def currentMajorType = currentTypeByte >> MajorTypeBits

  def skip(): Unit = read(SkipSwitch)

  def read[R](switch: CBORSwitch[R]): R = try {
    val byte = readTypeByte
    val majorType = byte >> MajorTypeBits
    val info = byte & InfoMask

    (majorType: @annotation.switch) match {
      case MajorType.PositiveInt =>
        switch.readInt(readIntData(info), this)

      case MajorType.NegativeInt =>
        switch.readInt(-1 - readIntData(info), this)

      case MajorType.ByteString =>
        switch.readBytes(readBytesData(readIntData(info)), this)

      case MajorType.UTF8String =>
        switch.readString(readBytesData(readIntData(info)), this)

      case MajorType.Array =>
        switch.readArrayStart(readIntData(info), this)

      case MajorType.Map =>
        switch.readMapStart(readIntData(info), this)

      case MajorType.Tag =>
        import TypeInfos._

        readIntData(info) match {
          case EpochTimestampTag =>
            switch.readTimestamp(read(CBORParser.TimestampSwitch), this)
          case PositiveBigNumTag =>
            switch.readBigNum(BigInt(read(CBORParser.BytesSwitch).toByteArray), this)
          case NegativeBigNumTag =>
            switch.readBigNum(
              -1 - BigInt(read(CBORParser.BytesSwitch).toByteArray),
              this)
          case tag => switch.readTag(tag, this)
        }

      case MajorType.Misc =>
        import TypeInfos._

        (info: @annotation.switch) match {
          case FalseInfo  => switch.readBoolean(false, this)
          case TrueInfo   => switch.readBoolean(true, this)
          case NilInfo    => switch.readNil(this)
          case FloatInfo  => switch.readFloat(buf.readFloat, this)
          case DoubleInfo => switch.readDouble(buf.readDouble, this)
        }
    }
  } catch {
    case _: IndexOutOfBoundsException => switch.readEndOfStream(this)
  }

  private def readTypeByte = buf.readUnsignedByte

  // See RFC 7049 Section 2.1 Major Type 0
  private def readIntData(info: Int): Long =
    if (info < InitialByte.UInt8) info else {
      (info: @annotation.switch) match {
        case InitialByte.UInt8 => buf.readUnsignedByte
        case InitialByte.UInt16 => buf.readUnsignedShort
        case InitialByte.UInt32 => buf.readUnsignedInt
        case InitialByte.UInt64 => buf.readLong
        case 28 | 29 | 30 => // reserved for future expansion
          throw CBORValidationException
        case 31 => // TODO: indefinite length items
          throw CBORValidationException
      }
    }

  private def readBytesData(length: Long): ByteBuf = {
    require(length <= Int.MaxValue)
    buf.readSlice(length.toInt)
  }
}
