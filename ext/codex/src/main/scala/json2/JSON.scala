package fauna.codex.json2

import com.fasterxml.jackson.core.{ JsonParseException => JacksonParseException, io => _, _ }
import fauna.codex.builder._
import fauna.lang.syntax._
import io.netty.buffer.{ ByteBuf, Unpooled }
import scala.annotation.unused

import JSONTypes._

// FIXME: Decode JSON bytebufs to Query[Result]. Encode needs to
//        somehow return a Query[ByteBuf]

// FIXME: Lazily decode a Version's ByteBuf into CBOR, rather than
//        always. Version render can transform a CBORParser into a
//        JSON token stream.

// FIXME: Version wrappers (database, task, etc.) should decode their
//        tree on construction and discard it.

object JSON extends CodecType2[JSONParser, JSONWriter] with CodecByteHelpers {

  val jacksonFactory = new JsonFactory

  def encode[T: Encoder](buf: ByteBuf, t: T): ByteBuf = {
    encode(JSONWriter(buf), t)
    buf
  }

  override def decode[T](stream: In)(implicit dec: Decoder[T]): T =
    try super.decode(stream)(dec) catch {
      case e: JacksonParseException => throw JSONParseException(e.getMessage)
    }

  def decode[T](buf: ByteBuf)(implicit dec: Decoder[T]): T =
    decode(JSONParser(buf))(dec)

  // A pre-escaped string

  class Escaped(val bytes: Array[Byte]) extends AnyVal

  object Escaped {
    def apply(str: String): Escaped =
      new Escaped(JSONWriter(Unpooled.buffer)
        .writeString(str)
        .buf.toByteArray)

    implicit object JSONEncoder extends Encoder[Escaped] {
      def encode(stream: Out, e: Escaped) = stream.writeString(e)
    }
  }

  // implements decoding in terms of JSONSwitch

  trait SwitchDecoder[T] extends Decoder[T] with JSONSwitch[T] {
    def decode(stream: JSONParser) = stream.read(this)

    // these should be handled in the start callbacks

    def readArrayEnd(stream: JSON.In) =
      throw JSONUnexpectedTypeException(JSONTypes.ArrayEndLabel)

    def readObjectEnd(stream: JSON.In) =
      throw JSONUnexpectedTypeException(JSONTypes.ObjectEndLabel)

    def readObjectFieldName(name: String, stream: JSON.In) =
      throw JSONUnexpectedTypeException(JSONTypes.ObjectFieldNameLabel)
  }

  trait SwitchCodec[T] extends Codec[T] with SwitchDecoder[T]


  abstract class PartialDecoder[T](label: String) extends PartialJSONSwitch[T](label) with Decoder[T] {
    def decode(stream: In) = stream.read(this)
  }

  abstract class PartialCodec[T](label: String) extends PartialDecoder[T](label) with Codec[T]


  // codecs for builtins

  implicit object IntCodec extends PartialCodec[Int](IntLabel) {
    override def readInt(l: Long, stream: JSONParser) = l.toInt
    def encode(stream: Out, i: Int) = stream.writeNumber(i)
  }

  implicit object LongCodec extends PartialCodec[Long](IntLabel) {
    override def readInt(l: Long, stream: JSONParser) = l
    def encode(stream: Out, l: Long) = stream.writeNumber(l)
  }

  implicit object BooleanCodec extends PartialCodec[Boolean](BooleanLabel) {
    override def readBoolean(b: Boolean, stream: JSONParser) = b
    def encode(stream: Out, b: Boolean) = stream.writeBoolean(b)
  }

  implicit object UnitCodec extends PartialCodec[Unit](NullLabel) {
    override def readNull(stream: JSONParser) = ()
    def encode(stream: Out, n: Unit) = stream.writeNull()
  }

  implicit object FloatCodec extends PartialCodec[Float](DoubleLabel) {
    override def readDouble(d: Double, stream: JSONParser) = d.toFloat
    def encode(stream: Out, f: Float) = stream.writeNumber(f)
  }

  implicit object DoubleCodec extends PartialCodec[Double](DoubleLabel) {
    override def readDouble(d: Double, stream: JSONParser) = d
    def encode(stream: Out, d: Double) = stream.writeNumber(d)
  }

  implicit object StringCodec extends PartialCodec[String](StringLabel) {
    override def readString(s: String, stream: JSONParser) = s
    def encode(stream: Out, s: String) = stream.writeString(s)
  }

  implicit def OptEncoder[T: Encoder] = new OptEncoder[T]
  class OptEncoder[T](implicit enc: Encoder[T]) extends Encoder[Option[T]] {
    def encode(stream: Out, opt: Option[T]) = opt match {
      case Some(t) => enc.encode(stream, t)
      case None    => stream.writeNull()
    }
  }

  implicit def OptDecoder[T: Decoder] = new OptDecoder[T]
  class OptDecoder[T](implicit dec: Decoder[T]) extends Decoder[Option[T]] {
    def decode(stream: In) =
      if (stream.skipNull) None else Some(dec.decode(stream))
  }

  implicit def SeqEncoder[T: Encoder] = new SeqEncoder[T]
  class SeqEncoder[T](implicit enc: Encoder[T]) extends Encoder[Seq[T]] {
    def encode(stream: Out, ts: Seq[T]) = {
      stream.writeArrayStart()
      val iter = ts.iterator
      while (iter.hasNext) enc.encode(stream, iter.next())
      stream.writeArrayEnd()
      stream
    }
  }

  implicit def SeqDecoder[T: Decoder] = new SeqDecoder[T]
  class SeqDecoder[T](implicit dec: Decoder[T]) extends PartialDecoder[Seq[T]](ArrayStartLabel) {
    override def readArrayStart(stream: In) = {
      val b = Seq.newBuilder[T]
      while (!stream.skipArrayEnd) b += dec.decode(stream)
      b.result()
    }
  }

  implicit def StringMapEncoder[T: Encoder] = new StringMapEncoder[T]
  class StringMapEncoder[T](implicit enc: Encoder[T]) extends Encoder[Map[String, T]] {
    def encode(stream: Out, ts: Map[String, T]) = {
      stream.writeObjectStart()
      val iter = ts.iterator
      while (iter.hasNext) {
        val (k, v) = iter.next()
        stream.writeString(k)
        enc.encode(stream, v)
      }
      stream.writeObjectEnd()
      stream
    }
  }

  implicit def StringMapDecoder[T: Decoder] = new StringMapDecoder[T]
  class StringMapDecoder[T](implicit dec: Decoder[T]) extends PartialDecoder[Map[String, T]](ObjectStartLabel) {
    override def readObjectStart(stream: JSONParser) = {
      val b = Map.newBuilder[String, T]
      while (!stream.skipObjectEnd) b += (stream.read(JSONParser.ObjectFieldNameSwitch) -> dec.decode(stream))
      b.result()
    }
  }

  // Impls for tuple, record macros

  trait TupleDecoder {
    def decodeStart(stream: In, @unused expected: Int) = {
      stream.read(JSONParser.ArrayStartSwitch)
    }

    def decodeEnd(stream: In) = {
      stream.read(JSONParser.ArrayEndSwitch)
    }
  }

  trait TupleEncoder {
    def encodeStart(stream: Out, @unused arity: Int) = stream.writeArrayStart()

    def encodeEnd(stream: Out) = stream.writeArrayEnd()
  }

  type RecordFieldNameFormat = FieldNameFormat.Underscore

  private final class RecordIter(stream: In) extends Iterator[Unit] {
    def hasNext = !stream.skipObjectEnd
    def next() = ()
  }

  trait RecordDecoder {
    def decodeStart(stream: In): Iterator[Unit] = {
      stream.read(JSONParser.ObjectStartSwitch)
      new RecordIter(stream)
    }

    def decodeFieldName(stream: In) = stream.read(JSONParser.ObjectFieldNameSwitch)

    def decodeEnd(@unused stream: In) = () // object end consumed in Iter
  }

  trait RecordEncoder {
    def encodeStart(stream: Out, @unused arity: Int) = stream.writeObjectStart()

    def encodeFieldName(stream: Out, name: String) = stream.writeString(name)
    def encodeEnd(stream: Out) = stream.writeObjectEnd()
  }
}
