package fauna.codex

import fauna.codex.builder._
import fauna.codex.json2.JSON
import io.netty.buffer.ByteBuf
import java.io.OutputStream
import language.implicitConversions

package json {
  case class JsonFieldAccessException(field: Any) extends NoSuchElementException(s"No field $field.")

  case class JsonRangeAccessException(idx: Int) extends IndexOutOfBoundsException(s"Index $idx beyond array bounds.")

  case class JsonInvalidAccessException(msg: String) extends NoSuchElementException(msg)

  case class JsonUnexpectedValue(msg: String) extends NoSuchElementException(msg)

  case class JsonParseException(msg: String) extends Exception(msg)

  package object conversions {
    implicit def valueAsT[T: JsonDecoder](value: JSValue): T = value.as[T]
  }
}

package object json {
  def JsonExceptionRescue[T](f: Throwable => T): PartialFunction[Throwable, T] = ({
    case e: JsonFieldAccessException   => f(e)
    case e: JsonRangeAccessException   => f(e)
    case e: JsonInvalidAccessException => f(e)
    case e: JsonUnexpectedValue        => f(e)
    case e: JsonParseException         => f(e)
  })

  final object JsonCodec extends NullCodecType[JSValue, JsonGenerator]
      with BinaryCodecType[JSValue, JsonGenerator]
      with TupleCodecType[JSValue, JsonGenerator]
      with SeqCodecType[JSValue, JsonGenerator]
      with RecordCodecType[JSValue, JsonGenerator]
      with UnionCodecType[JSValue, JsonGenerator]
      with CodecBuilder[JSValue, JsonGenerator]
      with RecordCodecBuilder[JSValue, JsonGenerator]
      with UnionCodecBuilder[JSValue, JsonGenerator]
      with CodecCompanion[JSValue, JsonGenerator] {

    // satisfy Codec

    val format = JsonCodecFormat

    def decode[T: Decoder](buf: ByteBuf) = JSON.tryParse[JSValue](buf) flatMap { _.tryAs[T] }

    def encode[T: Encoder](stream: OutputStream, t: T) = JS(t).writeTo(stream, false)

    // JSON can't encode arbitrary maps, so Map[String, T] will have to do.

    final class StringMapDecoder[+T](val innerDecoder: Decoder[T]) extends Decoder[Map[String, T]] {
      def decode(stream: JSValue) = {
        val b = Map.newBuilder[String, T]
        format.readRecord(stream, -1) { (key, s) => b += (key -> innerDecoder.decode(s)) }
        b.result()
      }
    }

    implicit def stringMapDecoder[T](implicit dec: Decoder[T]): Decoder[Map[String, T]] = new StringMapDecoder(dec)

    final class StringMapEncoder[T](val innerEncoder: Encoder[T]) extends Encoder[Map[String, T]] {
      def encodeTo(stream: JsonGenerator, map: Map[String, T]) =
        format.writeRecord(stream, map.size) { s =>
          map foreach { case (k, v) => format.writeField(s, k) { s => innerEncoder.encodeTo(s, v) } }
        }
    }

    implicit def stringMapEncoder[T](implicit enc: Encoder[T]): Encoder[Map[String, T]] = new StringMapEncoder(enc)
  }

  type JsonDecoder[+T] = JsonCodec.Decoder[T]
  type JsonEncoder[-T] = JsonCodec.Encoder[T]
  type JsonCodec[T] = JsonCodec.Codec[T]

  object JsonDecoder extends DecoderBuilder[JSValue]
      with RecordDecoderBuilder[JSValue]
      with UnionDecoderBuilder[JSValue] {
    val base = JsonCodec
  }

  object JsonEncoder extends EncoderBuilder[JsonGenerator]
      with RecordEncoderBuilder[JsonGenerator]
      with UnionEncoderBuilder[JsonGenerator] {
    val base = JsonCodec
  }
}
