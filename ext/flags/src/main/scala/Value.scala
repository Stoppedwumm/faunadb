package fauna.flags

import fauna.codex.cbor._
import fauna.codex.json.JsonCodec
import fauna.lang.syntax._
import fauna.lang.Timestamp
import io.netty.buffer.ByteBuf
import scala.language.implicitConversions
import scala.math.BigInt

object Value {
  implicit object JSONCodec extends JsonCodec[Value] {
    override def decode(stream: JsonCodec.In): Value = {
      if (stream.is[Double]) {
        DoubleValue(stream.as)
      } else if (stream.is[Long]) {
        LongValue(stream.as)
      } else if (stream.is[Boolean]) {
        BooleanValue(stream.as)
      } else if (stream.is[String]) {
        StringValue(stream.as)
      } else {
        throw new IllegalStateException("invalid stream: " + stream)
      }
    }

    def encodeTo(stream: JsonCodec.Out, value: Value): Unit =
      value match {
        case DoubleValue(v)  => stream.put(v)
        case LongValue(v)    => stream.put(v)
        case BooleanValue(v) => stream.put(v)
        case StringValue(v)  => stream.put(v)
      }
  }

  // Convenience conversions from plain types to Values
  implicit def longToLongValue(l: Long) = LongValue(l)
  implicit def doubleToDoubleValue(d: Double) = DoubleValue(d)
  implicit def stringToStringValue(s: String) = StringValue(s)
  implicit def booleanToBooleanValue(b: Boolean) = BooleanValue(b)

  implicit def CBORCodec: CBOR.Codec[Value] = new ValueCodec

  def fromString(s: String) = s match {
    case "true"  => BooleanValue(true)
    case "false" => BooleanValue(false)
    case _ =>
      s.toLongOption match {
        case Some(v) => LongValue(v)
        case None =>
          s.toDoubleOption match {
            case Some(v) => DoubleValue(v)
            case None    => StringValue(s)
          }
      }
  }

  /** This codec permits transparent encoding between "plain" types -
    * String, Long, Double, &c. - and the "value" type constructors.
    *
    * This allows for the wire protocol to be uncluttered with CBOR
    * type tags, while preserving the safety of the Value ADT.
    */
  final class ValueCodec extends CBOR.Codec[Value] {

    object ValueSwitch extends CBORSwitch[Value] {
      import TypeLabels._

      private def error(provided: String) =
        throw CBORUnexpectedTypeException(provided, "Value")

      def readInt(value: Long, stream: CBORParser): Value =
        LongValue(value)

      def readDouble(value: Double, stream: CBORParser): Value =
        DoubleValue(value)

      def readString(value: ByteBuf, stream: CBORParser): Value =
        StringValue(value.toUTF8String)

      def readBoolean(value: Boolean, stream: CBORParser): Value =
        BooleanValue(value)

      def readNil(stream: CBORParser) = error(NilLabel)
      def readFloat(float: Float, stream: CBORParser) = error(FloatLabel)
      def readBigNum(num: BigInt, stream: CBORParser) = error(BigNumLabel)
      def readTimestamp(ts: Timestamp, stream: CBORParser) = error(TimestampLabel)
      def readBytes(bytes: ByteBuf, stream: CBORParser) = error(ByteStringLabel)
      def readArrayStart(length: Long, stream: CBORParser) = error(ArrayLabel)
      def readMapStart(length: Long, stream: CBORParser) = error(MapLabel)
      def readTag(tag: Long, stream: CBORParser) = error(TagLabel)
    }

    def encode(stream: CBOR.Out, value: Value): CBOR.Out =
      value match {
        case StringValue(s)  => CBOR.encode(stream, s)
        case LongValue(l)    => CBOR.encode(stream, l)
        case DoubleValue(d)  => CBOR.encode(stream, d)
        case BooleanValue(b) => CBOR.encode(stream, b)
      }

    def decode(stream: CBOR.In): Value =
      stream.read(ValueSwitch)
  }
}

/** Values are typed attributes associated with a key in a Properties
  * object.
  */
sealed trait Value extends Any {
  type T
  def value: T
}

case class StringValue(value: String) extends AnyVal with Value {
  type T = String
}

case class LongValue(value: Long) extends AnyVal with Value {
  type T = Long
}

case class DoubleValue(value: Double) extends AnyVal with Value {
  type T = Double
}

case class BooleanValue(value: Boolean) extends AnyVal with Value {
  type T = Boolean
}
