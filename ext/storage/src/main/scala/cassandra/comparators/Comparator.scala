package fauna.storage.cassandra.comparators

import fauna.codex.cbor.{ CBOR, CBOROrdering }
import fauna.codex.cbor.CBOR.showBuffer
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.storage._
import fauna.storage.cassandra.CValue
import io.netty.buffer.{ ByteBuf, Unpooled }
import java.nio.ByteBuffer
import org.apache.cassandra.db.marshal.{ AbstractType, TypeParser }
import org.apache.cassandra.serializers.{
  BytesSerializer,
  MarshalException,
  TypeSerializer
}
import org.apache.cassandra.utils.Hex

class ComparatorException(a: ByteBuf, b: ByteBuf, cause: Throwable)
    extends Exception(
      s"Exception caught comparing:\n\t${a.toHexString}\n\t${b.toHexString}",
      cause
    )

trait Comparator {
  def comparator: String

  def matches(other: Comparator): Boolean

  protected def compare0(a: ByteBuf, b: ByteBuf): Int

  def canEncodeValue(cs: Seq[CValue], compareOp: Byte): Boolean

  def canEncodeValue(cs: Seq[CValue]): Boolean = canEncodeValue(cs, Predicate.EQ)

  // Encode a Seq of CValues that matches or is a prefix of the comparator schema
  def cvaluesToBytes(c: Seq[CValue], compareOp: Byte): ByteBuf

  def cvaluesToBytes(cs: Seq[CValue]): ByteBuf = cvaluesToBytes(cs, Predicate.EQ)

  // Decode an exact match of the schema
  def bytesToCValues(b: ByteBuf): Seq[CValue]

  def show(b: ByteBuf): String

  override def toString = comparator

  def compare(a: ByteBuf, b: ByteBuf): Int =
    try {
      compare0(a, b)
    } catch {
      case t: Throwable =>
        throw new ComparatorException(a, b, t)
    }
}

object BasicComparator {

  implicit val codec: CBOR.Codec[BasicComparator] = CBOR.SumCodec[BasicComparator](
    CBOR.SingletonCodec(BooleanType),
    CBOR.SingletonCodec(BytesType),
    CBOR.SingletonCodec(Int32Type),
    CBOR.SingletonCodec(LongType),
    CBOR.SingletonCodec(FloatType),
    CBOR.SingletonCodec(DoubleType),
    CBOR.SingletonCodec(UTF8Type),
    CBOR.TupleCodec[ReversedType],
    AbstractCBORType.codec
  )
}

sealed trait BasicComparator extends Comparator {

  def matches(other: Comparator) = this == other || this.reverse == other

  def canEncodeValue(cs: Seq[CValue], compareOp: Byte) =
    cs.sizeCompare(1) == 0 && (this matches cs.head.comparator)

  def canEncodeValue(c: CValue) =
    matches(c.comparator)

  def cvaluesToBytes(cs: Seq[CValue], compareOp: Byte) =
    if (canEncodeValue(cs, compareOp)) {
      cs.head.bytes
    } else {
      throw SchemaViolationException(s"Value does not match comparator $comparator")
    }

  def bytesToCValues(b: ByteBuf) = Seq(CValue(this, b))

  def reverse = this match {
    case ReversedType(underlying) => underlying
    case dataType                 => ReversedType(dataType)
  }
}

case class ReversedType(underlying: BasicComparator) extends BasicComparator {

  val comparator = underlying match {
    case ReversedType(u) => u.comparator
    case u               => u.comparator + "(reversed=true)"
  }

  protected def compare0(a: ByteBuf, b: ByteBuf) = underlying.compare(b, a)

  def show(b: ByteBuf) = underlying show b

  override def matches(other: Comparator) = underlying matches other
}

object BytesType extends BasicComparator {
  val comparator = "BytesType"
  def show(b: ByteBuf) = showBuffer(b)

  @inline final override def compare(a: ByteBuf, b: ByteBuf): Int =
    a compareTo b

  // fulfill the Comparator trait's interface
  protected def compare0(a: ByteBuf, b: ByteBuf): Int = compare(a, b)

  override def matches(other: Comparator) =
    other match {
      case _: AbstractCBORType => true
      case _ =>
        this == other || this.reverse == other
    }
}

object UTF8Type extends BasicComparator {
  val comparator = "UTF8Type"
  def show(b: ByteBuf) = b.toUTF8String

  def compare0(a: ByteBuf, b: ByteBuf) = BytesType.compare(a, b)
}

object LongType extends BasicComparator {
  val comparator = "LongType"
  def show(b: ByteBuf) = b.slice.readLong.toString

  def compare0(a: ByteBuf, b: ByteBuf) = BytesType.compare(a, b)
}

object Int32Type extends BasicComparator {
  val comparator = "Int32Type"
  def show(b: ByteBuf) = b.slice.readInt.toString

  def compare0(a: ByteBuf, b: ByteBuf) = BytesType.compare(a, b)
}

object BooleanType extends BasicComparator {
  val comparator = "BooleanType"
  def show(b: ByteBuf) = b.slice.readBoolean.toString

  def compare0(a: ByteBuf, b: ByteBuf) = a compareTo b
}

object FloatType extends BasicComparator {
  val comparator = "FloatType"
  def show(b: ByteBuf) = b.slice.readFloat.toString

  def compare0(a: ByteBuf, b: ByteBuf) =
    a.duplicate.readFloat compare b.duplicate.readFloat
}

object DoubleType extends BasicComparator {
  val comparator = "DoubleType"
  def show(b: ByteBuf) = b.slice.readDouble.toString

  @inline final def compare0(a: ByteBuf, b: ByteBuf) =
    a.duplicate.readDouble compare b.duplicate.readDouble
}

object AbstractCBORType {

  implicit val codec = CBOR.SumCodec[AbstractCBORType](
    CBOR.SingletonCodec(CBORType)
  )
}

abstract class AbstractCBORType
    extends AbstractType[ByteBuffer]
    with BasicComparator
    with ExceptionLogging {
  val comparator = "fauna.storage.cassandra.comparators.CBORType"
  def show(b: ByteBuf) = CBOR.show(b)

  override def matches(other: Comparator) =
    other match {
      case BytesType           => true
      case _: AbstractCBORType => true
      case _ =>
        this == other || this.reverse == other
    }

  def compare(a: ByteBuffer, b: ByteBuffer): Int =
    compare(Unpooled.wrappedBuffer(a), Unpooled.wrappedBuffer(b))

  protected def compare0(a: ByteBuf, b: ByteBuf) =
    try {
      CBOROrdering.compare(a, b)
    } catch {
      case e: Throwable =>
        throw new ComparatorException(a, b, e)
    }

  def fromString(source: String): ByteBuffer =
    try {
      ByteBuffer.wrap(Hex.hexToBytes(source))
    } catch {
      case e: NumberFormatException =>
        throw new MarshalException(s"cannot parse '$source' as hex bytes", e)
    }

  final def getInstance(parser: TypeParser) =
    AbstractType.parseDefaultParameters(this, parser)

  def getSerializer(): TypeSerializer[ByteBuffer] = BytesSerializer.instance

  // N.B. must be parseable by TypeParser.parse
  override def toString = CBORType.comparator
}

object CBORType extends AbstractCBORType {
  override def isCompatibleWith(other: AbstractType[_ <: Object]): Boolean =
    this == other || other.isCompatibleWith(this) // Avoid dep. loop.
}
