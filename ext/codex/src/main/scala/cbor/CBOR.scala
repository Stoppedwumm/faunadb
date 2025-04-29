package fauna.codex.cbor

import fauna.codex.builder._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import io.netty.buffer.{ ByteBuf, CompositeByteBuf, Unpooled }
import java.nio.ByteBuffer
import java.util.UUID
import scala.util.control.NonFatal
import TypeLabels._

/**
  * A partial implementation of RFC 7049, "Concise Binary Object
  * Representation (CBOR)". Notably absent features are:
  *
  *   * Indefinite-length data items ("streaming")
  *   * 64-bit length containers (byte strings, UTF-8 strings, arrays and maps)
  *   * 64-bit tags
  *   * IANA tag reservations
  *
  * The completeness of this implementation is contingent upon Core's
  * needs over time.
  */
object CBOR extends CodecType2[CBORParser, CBORWriter] with CodecByteHelpers {

  /** Used to inform CBORParser and switch implementations on how to handle
    * ByteBufs
    */
  sealed trait BufRetention {
    def read(buf: ByteBuf): ByteBuf =
      this match {
        case CBOR.BufRetention.Borrow =>
          buf
        case CBOR.BufRetention.Retain =>
          buf.retain()
          buf
        case CBOR.BufRetention.Copy =>
          Unpooled.copiedBuffer(buf)
      }
  }

  object BufRetention {
    case object Borrow extends BufRetention
    case object Retain extends BufRetention
    case object Copy extends BufRetention
  }

  // Custom Tags
  final val ReverseTag = 6

  // The tags [0, 5] are reserved by the CBOR specification for
  // date/times, bignums, and decimal fractions. However, starting sum
  // types at 6 would prevent scalac from optimizing low-cardinality
  // switch statements.
  //
  // This offset is added (subtracted) from all sum type tags as they
  // are encoded (decoded) to fulfill both needs.
  final val SumTypePrivateTagOffset = 6

  // The maximum allowed variants using the SumCodec macro.
  final val SumTypeVariantsLimit = 256 - SumTypePrivateTagOffset

  // FIXME: need a more elegant way to handle sum types
  final val EitherLeftTag = 6
  final val EitherRightTag = 7
  final val DocIDTag = 13
  final val EpochNanosTag = 16
  final val EpochDaysTag = 17
  final val QueryTag = 18
  final val TransactionTimeTag = 19
  final val RangeInclusiveTag = 20
  final val RangeExclusiveTag = 21
  final val UUIDTag = 37

  def encode[T: Encoder](buf: ByteBuf, t: T): ByteBuf = {
    encode(CopyingCBORWriter(buf), t)
    buf
  }

  def gatheringEncode[T: Encoder](c: CompositeByteBuf, t: T): CompositeByteBuf = {
    val writer = GatheringCBORWriter(c)
    encode(writer, t)
    writer.flush()
    c
  }

  def decode[T: Decoder](buf: ByteBuf): T =
    decode(CBORParser(buf))

  def decode[T: Decoder](buf: ByteBuf, bufRetentionPolicy: BufRetention): T =
    decode(CBORParser(buf, bufRetentionPolicy))

  /** Retains buffers returned as part of T. */
  def retainedDecode[T: Decoder](buf: ByteBuf): T =
    decode(CBORParser(buf, bufRetentionPolicy = BufRetention.Retain))

  /** Creates unpooled copies of buffers returned as part of T. */
  def copiedDecode[T: Decoder](buf: ByteBuf): T =
    decode(CBORParser(buf, bufRetentionPolicy = BufRetention.Copy))

  // helpers

  def show(buf: ByteBuf): String = {
    val str = new StringBuilder
    try {
      CBORParser(buf.duplicate).read(CBORParser.ShowSwitch(str))
    } catch {
      case _: NoSuchElementException => str.append(" <EOS>")
    }
    str.result()
  }

  /**
    * Similar to {@link #show(ByteBuf)} but in case the buffer contents are
    * not a valid CBOR-encoded value it will attempt to render them as a single
    * 64-bit signed integer (if the buffer length is exactly 8 bytes), or
    * otherwise as a generic hexadecimal listing of bytes. This method is only
    * useful in diagnostic output.
    */
  def showBuffer(buf: ByteBuf): String =
    try show(buf) catch {
      case NonFatal(_) =>
        val dup = buf.duplicate
        if (dup.readableBytes == 8) {
          dup.readLong.toString
        } else {
          s"'${dup.toHexString}'"
        }
    }

  def showBuffer(buf: Array[Byte]): String =
    showBuffer(Unpooled.wrappedBuffer(buf))


  // implements decoding in terms of CBORSwitch

  trait SwitchDecoder[T] extends Decoder[T] with CBORSwitch[T] {
    def decode(stream: CBORParser) = stream.read(this)
  }

  trait SwitchCodec[T] extends Codec[T] with SwitchDecoder[T]

  abstract class PartialDecoder[T](label: String)
      extends PartialCBORSwitch[T](label)
      with Decoder[T] {
    def decode(stream: In) = stream.read(this)
  }

  abstract class PartialCodec[T](label: String)
      extends PartialDecoder[T](label)
      with Codec[T]

  // codecs for builtins

  implicit object ByteCodec extends PartialCodec[Byte](ByteLabel) {
    override def readInt(l: Long, stream: CBORParser) = (l & 0xff).toByte
    def encode(stream: Out, b: Byte) = stream.writeInt(b.toInt)
  }

  implicit object IntCodec extends PartialCodec[Int](IntLabel) {
    override def readInt(l: Long, stream: CBORParser) = l.toInt
    def encode(stream: Out, i: Int) = stream.writeInt(i)
  }

  implicit object LongCodec extends PartialCodec[Long](IntLabel) {
    override def readInt(l: Long, stream: CBORParser) = l
    def encode(stream: Out, l: Long) = stream.writeInt(l)
  }

  implicit object BooleanCodec extends PartialCodec[Boolean](BooleanLabel) {
    override def readBoolean(b: Boolean, stream: CBORParser) = b
    def encode(stream: Out, b: Boolean) = stream.writeBoolean(b)
  }

  implicit object UnitCodec extends PartialCodec[Unit](NilLabel) {
    override def readNil(stream: CBORParser) = ()
    def encode(stream: Out, u: Unit) = stream.writeNil()
  }

  implicit object FloatCodec extends PartialCodec[Float](FloatLabel) {
    override def readFloat(f: Float, stream: CBORParser) = f
    def encode(stream: Out, f: Float) = stream.writeFloat(f)
  }

  implicit object DoubleCodec extends PartialCodec[Double](DoubleLabel) {
    override def readDouble(d: Double, stream: CBORParser) = d
    override def readFloat(f: Float, stream: CBORParser) = f
    def encode(stream: Out, d: Double) = stream.writeDouble(d)
  }

  implicit object BigIntCodec extends PartialCodec[BigInt](BigNumLabel) {
    override def readBigNum(n: BigInt, stream: CBORParser) = n
    def encode(stream: Out, n: BigInt) = stream.writeBigNum(n)
  }

  implicit object TimestampCodec extends PartialCodec[Timestamp](TimestampLabel) {
    override def readTimestamp(ts: Timestamp, stream: CBORParser) = ts
    def encode(stream: Out, ts: Timestamp) = stream.writeTimestamp(ts)
  }

  /** UUIDs used to be handled as first-class values in our CBOR codec library as
    * a tagged value, using the CBOR-standard UUID tag 37. However, they have
    * been demoted so that SumCodecs can have more variants. The tagged version
    * of the UUID codec remains the implicit, however, in order to remain
    * compatible with the previous implementation's encoding.
    */
  implicit object TaggedUUIDCodec extends PartialCodec[UUID]("UUID") {
    override def readTag(tag: Long, stream: CBORParser) =
      tag match {
        case CBOR.UUIDTag => stream.read(UUIDCodec)
        case _            => throw CBORUnexpectedTypeException(TagLabel, "UUID")
      }
    def encode(stream: Out, uuid: UUID) = {
      stream.writeTag(CBOR.UUIDTag)
      UUIDCodec.encode(stream, uuid)
    }
  }

  object UUIDCodec extends PartialCodec[UUID]("UUID") {
    override def readBytes(bytes: ByteBuf, stream: CBORParser) = {
      if (bytes.readableBytes() != 16) {
        throw CBORUnexpectedTypeException("malformed UUID bytes", "UUID")
      }

      new UUID(bytes.readLong, bytes.readLong)
    }
    def encode(stream: Out, uuid: UUID) = {
      val b = Unpooled.buffer(16)
      b.writeLong(uuid.getMostSignificantBits)
      b.writeLong(uuid.getLeastSignificantBits)
      stream.writeBytes(b)
    }
  }

  implicit object ByteBufCodec extends PartialCodec[ByteBuf](ByteStringLabel) {
    override def readBytes(b: ByteBuf, stream: CBORParser) =
      stream.bufRetentionPolicy.read(b)
    def encode(stream: Out, b: ByteBuf) = stream.writeBytes(b)
  }

  implicit object ByteBufferCodec extends PartialCodec[ByteBuffer](ByteStringLabel) {
    override def readBytes(b: ByteBuf, stream: CBORParser) =
      stream.bufRetentionPolicy.read(b).nioBuffer

    def encode(stream: Out, b: ByteBuffer) =
      stream.writeBytes(Unpooled.wrappedBuffer(b))
  }

  implicit object ByteArrayCodec extends PartialCodec[Array[Byte]](ByteStringLabel) {
    override def readBytes(b: ByteBuf, stream: CBORParser) = b.toByteArray

    def encode(stream: Out, a: Array[Byte]) =
      stream.writeBytes(Unpooled.wrappedBuffer(a))
  }

  implicit object StringCodec extends PartialCodec[String](UTF8StringLabel) {
    override def readString(str: ByteBuf, stream: CBORParser) =
      str.toUTF8String

    def encode(stream: Out, str: String) =
      stream.writeString(Unpooled.wrappedBuffer(str.getBytes("UTF-8")))
  }

  implicit object RangeEncoder extends Encoder[Range] {
    def encode(stream: CBOR.Out, r: Range) = {
      stream.writeTag(if (r.isInclusive) RangeInclusiveTag else RangeExclusiveTag)
      stream.writeInt(r.start)
      stream.writeInt(r.end)
      stream.writeInt(r.step)
    }
  }

  implicit object RangeDecoder extends PartialDecoder[Range](TagLabel) {
    override def readTag(tag: Long, stream: CBORParser): Range = {
      val intSwitch = implicitly[CBORSwitch[Int]]
      val start = stream.read(intSwitch)
      val end = stream.read(intSwitch)
      val step = stream.read(intSwitch)

      tag match {
        case RangeInclusiveTag => start to end by step
        case RangeExclusiveTag => start until end by step
        case tag => throw new IllegalStateException(s"Unknown range tag $tag.")
      }
    }
  }

  implicit def OptEncoder[T: Encoder] = new OptEncoder[T]

  class OptEncoder[T](implicit enc: Encoder[T]) extends Encoder[Option[T]] {

    def encode(stream: Out, opt: Option[T]) =
      opt match {
        case Some(t) => enc.encode(stream, t)
        case None    => stream.writeNil()
      }
  }

  implicit def OptDecoder[T: Decoder] = new OptDecoder[T]

  class OptDecoder[T](implicit dec: Decoder[T]) extends Decoder[Option[T]] {

    def decode(stream: In) =
      if (stream.currentTypeByte == InitialByte.Nil) {
        stream.buf.readByte
        None
      } else {
        Some(dec.decode(stream))
      }
  }

  implicit def EitherEncoder[A: Encoder, B: Encoder] = new EitherEncoder[A, B]

  class EitherEncoder[A, B](implicit encA: Encoder[A], encB: Encoder[B])
      extends Encoder[Either[A, B]] {

    def encode(stream: Out, e: Either[A, B]) =
      e match {
        case Left(a) =>
          stream.writeTag(EitherLeftTag)
          encA.encode(stream, a)
        case Right(b) =>
          stream.writeTag(EitherRightTag)
          encB.encode(stream, b)
      }
  }

  implicit def EitherDecoder[A: Decoder, B: Decoder] = new EitherDecoder[A, B]

  class EitherDecoder[A, B](implicit decA: Decoder[A], decB: Decoder[B])
      extends PartialDecoder[Either[A, B]](TagLabel) {
    override def readTag(tag: Long, stream: CBORParser) =
      tag match {
        case EitherLeftTag  => Left(decA.decode(stream))
        case EitherRightTag => Right(decB.decode(stream))
        case tag => throw new IllegalStateException(s"Unknown either tag $tag.")
      }
  }

  implicit def VectorEncoder[T: Encoder] = new VectorEncoder[T]

  class VectorEncoder[T](implicit enc: Encoder[T]) extends Encoder[Vector[T]] {

    def encode(stream: Out, ts: Vector[T]) = {
      stream.writeArrayStart(ts.length)
      val size = ts.size
      var i = 0
      while (i < size) {
        enc.encode(stream, ts(i))
        i += 1
      }
      stream
    }
  }

  implicit def VectorDecoder[T: Decoder] = new VectorDecoder[T]

  class VectorDecoder[T](implicit dec: Decoder[T])
      extends PartialDecoder[Vector[T]](ArrayLabel) {
    override def readArrayStart(length: Long, stream: CBORParser) = {
      // this contraption allows building the vector with a known length
      // which is more efficient in case length <= 32;
      // see implementation of Vector.from and VectorBuilder for details about that.
      Iterator.fill(length.toInt)(dec.decode(stream)).toVector
    }
  }

  implicit def SetEncoder[T: Encoder] = new SetEncoder[T]

  class SetEncoder[T](implicit enc: Encoder[T]) extends Encoder[Set[T]] {

    def encode(stream: Out, ts: Set[T]) = {
      stream.writeArrayStart(ts.size)
      val iter = ts.iterator
      while (iter.hasNext) enc.encode(stream, iter.next())
      stream
    }
  }

  implicit def SetDecoder[T: Decoder] = new SetDecoder[T]

  class SetDecoder[T](implicit dec: Decoder[T])
    extends PartialDecoder[Set[T]](ArrayLabel) {

    override def readArrayStart(length: Long, stream: CBORParser) = {
      val b = Set.newBuilder[T]
      b.sizeHint(length.toInt)
      var i = 0
      while (i < length) {
        b += dec.decode(stream)
        i += 1
      }
      b.result()
    }
  }

  implicit def MapEncoder[K: Encoder, V: Encoder] = new MapEncoder[K, V]

  class MapEncoder[K, V](implicit keyEnc: Encoder[K], valEnc: Encoder[V])
      extends Encoder[Map[K, V]] {

    def encode(stream: Out, m: Map[K, V]) = {
      stream.writeMapStart(m.size)
      val iter = m.iterator
      while (iter.hasNext) {
        val t = iter.next()
        keyEnc.encode(stream, t._1)
        valEnc.encode(stream, t._2)
      }
      stream
    }
  }

  implicit def MapDecoder[K: Decoder, V: Decoder] = new MapDecoder[K, V]

  class MapDecoder[K, V](implicit keyDec: Decoder[K], valDec: Decoder[V])
      extends PartialDecoder[Map[K, V]](MapLabel) {
    override def readMapStart(length: Long, stream: CBORParser) = {
      val b = Map.newBuilder[K, V]
      b.sizeHint(length.toInt)
      var i = 0
      while (i < length) {
        b += (keyDec.decode(stream) -> valDec.decode(stream))
        i += 1
      }
      b.result()
    }
  }

  // Impls for tuple, record macros

  trait TupleDecoder {

    def decodeStart(stream: In, expected: Int) = {
      val len = stream.read(CBORParser.ArrayStartSwitch)
      if (len != expected) throw CBORInvalidLengthException(len, expected)
    }

    def decodeEnd(stream: In) = ()
  }

  trait TupleEncoder {
    def encodeStart(stream: Out, arity: Int) = stream.writeArrayStart(arity)

    def encodeEnd(stream: Out) = ()
  }

  trait SumTypeValidator {
    // FIXME: WTF not compile checked???
    def validateTags(tags: List[Int]) =
      require(
        tags.size <= SumTypeVariantsLimit,
        s"Number of SumCodec variants exceeds limit of $SumTypeVariantsLimit")
  }

  trait SumTypeDecoder {

    def decodeStart(stream: In): Long =
      stream.read(CBORParser.TagSwitch) - SumTypePrivateTagOffset

    def decodeEnd(stream: In) = ()
  }

  trait SumTypeEncoder {

    def encodeStart(stream: Out, tag: Long) =
      stream.writeTag(tag + SumTypePrivateTagOffset)

    def encodeEnd(stream: Out) = ()
  }

  type RecordFieldNameFormat = FieldNameFormat.Underscore

  trait RecordDecoder {
    private class Iter(var i: Long) extends Iterator[Unit] {
      def hasNext = i > 0
      def next() = i -= 1
    }

    def decodeStart(stream: In): Iterator[Unit] =
      new Iter(stream.read(CBORParser.MapStartSwitch))

    // FIXME: do not convert field names back and forth between ByteBuf and String
    def decodeFieldName(stream: In) = StringCodec.decode(stream)

    def decodeEnd(stream: In) = ()
  }

  trait RecordEncoder {
    def encodeStart(stream: Out, arity: Int) = stream.writeMapStart(arity)
    def encodeFieldName(stream: Out, name: String) = StringCodec.encode(stream, name)

    def encodeEnd(stream: Out) = ()
  }
}
