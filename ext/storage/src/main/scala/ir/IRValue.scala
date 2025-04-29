package fauna.storage.ir

import fauna.atoms._
import fauna.codex.cbor._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.storage.Resolved
import io.netty.buffer.ByteBuf
import java.time.LocalDate
import java.util.UUID
import scala.util.hashing.MurmurHash3

sealed abstract class IRType(override val toString: String)

object IRType {
  case class Custom(vtype: String) extends IRType(vtype)
  case class Array(vtype: IRType) extends IRType(s"$vtype Array")
  case class Map(vtype: IRType) extends IRType(s"$vtype Map")
}

sealed trait IRValue extends Any {
  def vtype: IRType
  def transformValues(pf: PartialFunction[ScalarV, IRValue]): IRValue

  // Equivalence of values by representation.
  // Basically, equality but NaN == NaN for floating point values.
  def equiv(other: IRValue): Boolean = this == other
}

object IRValue {
  import language.implicitConversions

  implicit def byteToV(l: Byte) = LongV(l)
  implicit def byteFieldToV(t: (String, Byte)) = t._1 -> LongV(t._2)
  implicit def shortToV(l: Short) = LongV(l)
  implicit def shortFieldToV(t: (String, Short)) = t._1 -> LongV(t._2)
  implicit def intToV(l: Int) = LongV(l)
  implicit def intFieldToV(t: (String, Int)) = t._1 -> LongV(t._2)
  implicit def longToV(l: Long) = LongV(l)
  implicit def longFieldToV(t: (String, Long)) = t._1 -> LongV(t._2)
  implicit def doubleToV(l: Double) = DoubleV(l)
  implicit def doubleFieldToV(t: (String, Double)) = t._1 -> DoubleV(t._2)
  implicit def booleanToV(l: Boolean) = BooleanV(l)
  implicit def booleanFieldToV(t: (String, Boolean)) = t._1 -> BooleanV(t._2)
  implicit def stringToV(l: String) = StringV(l)
  implicit def stringFieldToV(t: (String, String)) = t._1 -> StringV(t._2)
  implicit def collectionIDToV(c: CollectionID) = DocIDV(c.toDocID)
  implicit def collectionIDFieldToV(t: (String, CollectionID)) =
    t._1 -> DocIDV(t._2.toDocID)
  implicit def instanceIDToV(l: DocID) = DocIDV(l)
  implicit def instanceIDFieldToV(t: (String, DocID)) = t._1 -> DocIDV(t._2)

  // A lower bound estimate for the serialized size of the IR value.
  // Values are based on CBORCodex.encode. See IRValueSpec.
  def serializedSizeLowerBound(s: IRValue): Long = s match {
    case _: TransactionTimeV => 1 + 1
    case _: LongV            => 1 + 1
    case _: DoubleV          => 1 + 8
    case _: BooleanV         => 1
    case _: NullV            => 1
    case StringV(value)      => 1 + value.length
    case BytesV(value)       => 1 + value.readableBytes()
    case _: DocIDV           => 1 + 1 + 10
    case _: TimeV            => 1 + 1 + 1 + 1
    case _: DateV            => 1 + 1
    case _: UUIDV            => 1 + 1 + 16

    case ArrayV(elems) =>
      var size = 1L // array tag + size
      elems.foreach { e => size += serializedSizeLowerBound(e) }
      size

    case MapV(fields) =>
      var size = 1L // map tag + size
      fields.foreach { case (n, v) =>
        size += serializedSizeLowerBound(StringV(n))
        size += serializedSizeLowerBound(v)
      }
      size

    case QueryV(_, value) => 1 + serializedSizeLowerBound(value)
  }

  sealed abstract class AbstractCBORCodec extends CBOR.SwitchCodec[IRValue] {

    protected def encodeTxnTime(stream: CBORWriter, txnTS: TransactionTimeV): Unit

    def encode(stream: CBORWriter, value: IRValue) =
      value match {
        case TrueV      => stream.writeBoolean(true)
        case FalseV     => stream.writeBoolean(false)
        case NullV      => stream.writeNil()
        case LongV(v)   => stream.writeInt(v)
        case DoubleV(v) => stream.writeDouble(v)
        case BytesV(v)  => stream.writeBytes(v)
        case UUIDV(v)   => CBOR.TaggedUUIDCodec.encode(stream, v)
        case StringV(v) => CBOR.StringCodec.encode(stream, v)

        case txnTS: TransactionTimeV =>
          encodeTxnTime(stream, txnTS)
          stream

        case DocIDV(v) =>
          stream.writeTag(CBOR.DocIDTag)
          CBOR.ByteArrayCodec.encode(stream, DocID.toBytes(v))

        // FIXME: we should be able to replace this with
        // stream.writeTimestamp(ts), except that index data is moved
        // around as a result. See ticket ###
        case TimeV(ts) =>
          stream.writeTag(CBOR.EpochNanosTag)
          stream.writeArrayStart(2)
          stream.writeInt(ts.seconds)
          stream.writeInt(ts.nanoOffset)

        case DateV(date) =>
          stream.writeTag(CBOR.EpochDaysTag)
          stream.writeInt(date.toEpochDay)

        case QueryV(vers, value) =>
          stream.writeTag(CBOR.QueryTag)

          if (vers > APIVersion.LambdaDefaultVersion) {
            stream.writeArrayStart(2)
            stream.writeInt(vers.ordinal)
          }

          encode(stream, value)

        case ArrayV(v) =>
          stream.writeArrayStart(v.size)
          v foreach { encode(stream, _) }
          stream

        case MapV(v) =>
          stream.writeMapStart(v.size)
          v foreach { case (k, v) =>
            CBOR.StringCodec.encode(stream, k)
            encode(stream, v)
          }
          stream
      }

    // CBORSwitch

    def readInt(int: Long, stream: CBORParser) = LongV(int)

    def readBoolean(bool: Boolean, stream: CBORParser) = BooleanV(bool)

    def readNil(stream: CBORParser) = NullV

    def readFloat(float: Float, stream: CBORParser) = DoubleV(float)

    def readDouble(double: Double, stream: CBORParser) = DoubleV(double)

    def readBigNum(num: BigInt, stream: CBORParser) = LongV(num.toLong)

    def readTimestamp(ts: Timestamp, stream: CBORParser) = TimeV(ts)

    def readBytes(bytes: ByteBuf, stream: CBORParser) =
      BytesV(stream.bufRetentionPolicy.read(bytes))

    def readString(str: ByteBuf, stream: CBORParser) =
      StringV(CBOR.StringCodec.readString(str, stream))

    def readArrayStart(length: Long, stream: CBORParser) = {
      val b = Vector.newBuilder[IRValue]
      b.sizeHint(length.toInt)
      var i = 0L
      while (i < length) {
        b += stream.read(this)
        i += 1
      }
      new ArrayV(b.result())
    }

    def readMapStart(length: Long, stream: CBORParser) = {
      val b = List.newBuilder[(String, IRValue)]
      b.sizeHint(length.toInt)
      var i = 0L
      while (i < length) {
        b += (CBOR.StringCodec.decode(stream) -> stream.read(this))
        i += 1
      }
      new MapV(b.result())
    }

    def readTag(tag: Long, stream: CBORParser) =
      (tag.toInt: @annotation.switch) match {
        case CBOR.UUIDTag =>
          UUIDV(CBOR.UUIDCodec.decode(stream))

        case CBOR.DocIDTag =>
          DocIDV(DocID.fromBytes(CBOR.ByteArrayCodec.decode(stream)))

        // FIXME: deprecate. See note in encode above.
        case CBOR.EpochNanosTag =>
          val len = stream.read(CBORParser.ArrayStartSwitch)
          assert(len == 2)
          val secs = CBOR.LongCodec.decode(stream)
          val nanos = CBOR.LongCodec.decode(stream)
          TimeV(Timestamp(secs, nanos))

        case CBOR.EpochDaysTag =>
          val days = CBOR.LongCodec.decode(stream)
          DateV(LocalDate.ofEpochDay(days))

        case CBOR.QueryTag =>
          decodeQuery(stream)

        case CBOR.TransactionTimeTag =>
          val isMicros = CBOR.BooleanCodec.decode(stream)
          if (isMicros) {
            TransactionTimeV.Micros
          } else {
            TransactionTimeV.Timestamp
          }

        case _ =>
          error(TypeLabels.TagLabel)
      }

    protected def error(provided: String) =
      throw CBORUnexpectedTypeException(provided)

    // helpers

    def decodeQuery(stream: CBORParser): QueryV = {
      val vers = if (stream.currentMajorType == MajorType.Array) {
        val len = stream.read(CBORParser.ArrayStartSwitch)
        assert(len == 2)
        APIVersion(CBOR.IntCodec.decode(stream))
      } else {
        APIVersion.LambdaDefaultVersion
      }

      decode(stream) match {
        case expr: MapV => QueryV(vers, expr)
        case _          => QueryV.Null
      }
    }
  }

  implicit final object CBORCodec extends AbstractCBORCodec {
    protected def encodeTxnTime(stream: CBORWriter, txnTS: TransactionTimeV) = {
      stream.writeTag(CBOR.TransactionTimeTag)
      stream.writeBoolean(txnTS.isMicros)
    }
  }

  /** Resolves pending txn times when **encoding** IRValues into CBOR format. */
  final case class ResolvingCodec(txnTS: Resolved) extends AbstractCBORCodec {
    protected def encodeTxnTime(stream: CBORWriter, txnTSV: TransactionTimeV) = {
      encode(stream, txnTSV.resolve(txnTS.transactionTS))
    }
  }
}

/** PartialIRCodec partially decodes CBOR-encoded IR values.
  *
  * If the encoded value is not a map, the decoder behaves like the standard
  * decoder.
  *
  * If the encoded value is a map, the decoder looks for the key at the head
  * of `path`, then recursively partially decodes the key's value. For
  * example, decoding { "a" : { "b" : 0 } } with path [ "a", "b" ] produces
  * 0 as the result.
  *
  * This decoder is useful when the value of just one field is needed,
  * especially when the field is stored near the front of the top-level map.
  *
  * NB: The behavior may be surprising in some cases involving arrays:
  *
  * [ { "a" : 0 } ] decodes to [ { "a" : 0 } ], regardless of `path`.
  *
  * { "a" : [ { "b" : 0 } ] } decodes to [ { "b" : 0 } ] for _any_ path starting
  * with "a".
  */
final class PartialIRCodec(path: List[String])
    extends DelegatingCBORSwitch[IRValue, Option[IRValue]] {

  require(path.nonEmpty, "empty path")

  val delegate = IRValue.CBORCodec
  def transform(ir: IRValue) = Some(ir)
  override def readEndOfStream(stream: CBORParser) = None

  override def readMapStart(length: Long, stream: CBORParser): Option[IRValue] = {
    var i = 0
    while (i < length) {
      val key = stream.read(CBOR.StringCodec)
      path match {
        case `key` :: Nil  => return Some(stream.read(delegate))
        case `key` :: rest => return stream.read(new PartialIRCodec(rest))
        case _             => stream.read(CBORParser.SkipSwitch)
      }
      i += 1
    }
    None
  }
}

// Differentiates value vs non-value types.
sealed trait ScalarV extends Any with IRValue {
  def transformValues(pf: PartialFunction[ScalarV, IRValue]): IRValue =
    pf.applyOrElse(this, identity[IRValue])
}

object ScalarV {
  case object Type extends IRType("Scalar")
}

/** A placeholder which is rewritten by the storage engine to the
  * current transaction's time when a mutation containing this value
  * is applied to storage.
  *
  * These values should never be written to data storage, nor rendered.
  */
object TransactionTimeV {
  case object Type extends IRType("TransactionTime")

  final val Micros = TransactionTimeV(true)
  final val Timestamp = TransactionTimeV(false)
}

case class TransactionTimeV(isMicros: Boolean) extends AnyVal with ScalarV {
  def vtype = TransactionTimeV.Type
  def resolve(ts: Timestamp) = if (isMicros) LongV(ts.micros) else TimeV(ts)
}

object LongV {
  case object Type extends IRType("Long")
}

case class LongV(value: Long) extends AnyVal with ScalarV {
  def vtype = LongV.Type
  override def toString = value.toString
}

object DoubleV {
  case object Type extends IRType("Double")
}

case class DoubleV(value: Double) extends AnyVal with ScalarV {
  def vtype = DoubleV.Type
  override def toString = value.toString

  // Storage-wise and index-tuple-wise, NaN == NaN for IRValue.
  override def equiv(other: IRValue): Boolean = other match {
    case DoubleV(ov) => (value.isNaN() && ov.isNaN()) || value == ov
    case _ => false
  }
}

object BooleanV {
  case object Type extends IRType("Boolean")

  def apply(value: Boolean) = if (value) TrueV else FalseV

  def unapply(v: IRValue) =
    v match {
      case TrueV  => SomeTrue
      case FalseV => SomeFalse
      case _      => None
    }
}

sealed trait BooleanV extends ScalarV {
  def value: Boolean
  def vtype = BooleanV.Type
  override def toString = value.toString
}
case object TrueV extends BooleanV { val value = true }
case object FalseV extends BooleanV { val value = false }

sealed trait NullV extends ScalarV

case object NullV extends NullV {
  case object Type extends IRType("Null")
  def vtype = Type
  override def toString = "null"
}

object StringV {
  case object Type extends IRType("String")
}

case class StringV(value: String) extends AnyVal with ScalarV {
  def vtype = StringV.Type
  override def toString = "\"" + value + "\""
}

object BytesV {
  case object Type extends IRType("Bytes")
}

case class BytesV(value: ByteBuf) extends AnyVal with ScalarV {
  def vtype = BytesV.Type
  override def toString = s"""Bytes(\"${value.toHexString}\")"""
}

object DocIDV {
  case object Type extends IRType("Ref")

  implicit val CBORDecoder =
    CBOR.AliasDecoder[DocIDV, IRValue](_.asInstanceOf[DocIDV])
}

case class DocIDV(value: DocID) extends AnyVal with ScalarV {
  def vtype = DocIDV.Type
  override def toString = value.toString
}

object TimeV {
  case object Type extends IRType("Time")
}

case class TimeV(value: Timestamp) extends AnyVal with ScalarV {
  def vtype = TimeV.Type
  override def toString = value.toString
}

object DateV {
  case object Type extends IRType("Date")
}

case class DateV(value: LocalDate) extends AnyVal with ScalarV {
  def vtype = DateV.Type
  override def toString = value.toString
}

object QueryV {
  case object Type extends IRType("Query")

  final val Null = QueryV(
    APIVersion.LambdaDefaultVersion,
    MapV("lambda" -> ArrayV(), "expr" -> NullV))

  // Used for value construction in tests
  def apply(params: IRValue, expr: IRValue): QueryV =
    QueryV(APIVersion.LambdaDefaultVersion, MapV("lambda" -> params, "expr" -> expr))
}

case class QueryV(apiVersion: APIVersion, expr: MapV) extends IRValue {
  def vtype = QueryV.Type

  def transformValues(pf: PartialFunction[ScalarV, IRValue]): QueryV =
    QueryV(apiVersion, expr transformValues pf)
}

object UUIDV {
  case object Type extends IRType("UUID")
}

case class UUIDV(value: UUID) extends AnyVal with ScalarV {
  def vtype = UUIDV.Type
  override def toString = value.toString
}

object ArrayV {
  case object Type extends IRType("Array")

  private[this] val _empty = ArrayV(Vector.empty)

  def empty: ArrayV = _empty

  def apply(es: IRValue*): ArrayV = apply(es.toVector)

  def lift(a: ArrayV): Option[ArrayV] = if (a.isEmpty) None else Some(a)
}

case class ArrayV(elems: Vector[IRValue]) extends AnyVal with IRValue {
  def vtype = ArrayV.Type

  override def toString = s"[ ${elems mkString ", "} ]"

  def isEmpty = elems.isEmpty

  def transformValues(pf: PartialFunction[ScalarV, IRValue]): ArrayV =
    ArrayV(elems map { _ transformValues pf })
}

object MapV {
  case object Type extends IRType("Map")

  implicit val CBORDecoder = CBOR.AliasDecoder[MapV, IRValue](_.asInstanceOf[MapV])

  def empty: MapV = _empty; private val _empty = MapV(Nil)

  def apply(ps: (String, IRValue)*): MapV = apply(ps.toList)

  def lift(m: MapV): Option[MapV] = if (m.isEmpty) None else Some(m)
}

case class MapV(elems: List[(String, IRValue)]) extends IRValue {
  def vtype = MapV.Type

  override def toString = {
    val fields = elems.iterator map { case (k, v) =>
      "\"" + k + "\" : " + v
    } mkString ", "
    s"{ $fields }"
  }

  override def equals(other: Any) = other match {
    case other: MapV =>
      (this eq other) || ((other canEqual this) && (AList(
        elems) hasSameElementsAs AList(other.elems)))
    case _ => false
  }

  override def hashCode = MurmurHash3.unorderedHash(elems, 2203)

  def isEmpty = elems.isEmpty

  // transformations, iterations

  private def foreachValue[U](f: ((List[String], ScalarV)) => U): Unit = {
    def foreachValue0(
      k: String,
      v: IRValue,
      f: ((List[String], ScalarV)) => U): Unit = v match {
      case v: ScalarV => f((k :: Nil) -> v)
      case _: QueryV =>
        () // FIXME QueryV's are not scalars, but cannot be walked into.
      case a: ArrayV => a.elems foreach { foreachValue0(k, _, f) }
      case m: MapV   => m foreachValue { case (p, v) => f((k :: p) -> v) }
    }

    elems foreach { case (k, v) => foreachValue0(k, v, f) }
  }

  def collectValues[B](pf: PartialFunction[(List[String], ScalarV), B]): List[B] = {
    val rv = List.newBuilder[B]
    foreachValue(pf.runWith { rv += _ })
    rv.result()
  }

  def selectValues(path: List[String]): List[ScalarV] =
    collectValues { case (p, v) if p == path => v }

  def transformValues(pf: PartialFunction[ScalarV, IRValue]): MapV =
    MapV(elems map { case (k, v) => (k, v transformValues pf) })

  // aggregate

  def combine(other: MapV)(
    f: (Option[IRValue], Option[IRValue]) => Option[IRValue]): MapV =
    MapV((AList(elems) combine AList(other.elems))(f).elems)

  def merge(other: MapV): MapV =
    if (isEmpty) {
      other
    } else if (other.isEmpty) {
      this
    } else {
      (this combine other) {
        case (Some(l: MapV), Some(r: MapV)) => Some(l merge r)
        case (_, sr @ Some(_))              => sr
        case (l, None)                      => l
      }
    }

  def subtract(other: MapV): MapV =
    if (isEmpty) this
    else if (other.isEmpty) this
    else {
      (this combine other) {
        case (Some(l: MapV), Some(r: MapV)) => MapV.lift(l subtract r)
        case (Some(l), Some(r)) if l == r   => None
        case (l, _)                         => l
      }
    }

  // field reads, updates

  def contains(path: List[String]): Boolean = get(path).isDefined

  def get(path: List[String]): Option[IRValue] = path match {
    case Nil        => Some(this)
    case key :: Nil => AList(elems).get(key)
    case key :: rest =>
      AList(elems).get(key) match {
        case Some(m @ MapV(_)) => m.get(rest)
        case Some(n: NullV)    => Some(n)
        case _                 => None
      }
  }

  def modify(path: List[String], f: Option[IRValue] => Option[IRValue]): MapV = {
    val rv = path match {
      case Nil =>
        AList(elems)

      case key :: Nil =>
        AList(elems).modify(key, f)

      case key :: rest =>
        AList(elems).modify(
          key,
          { e =>
            val m = e match {
              case Some(m: MapV) => m
              case _             => MapV.empty
            }

            val m0 = m.modify(rest, f)
            if (m0.isEmpty) {
              None
            } else {
              Some(m0)
            }
          })
    }

    MapV(rv.elems)
  }

  def update(path: List[String], value: IRValue): MapV =
    modify(path, { _ => Some(value) })

  /** Renames a key in this map or any nested map at the provided path.
    * Returns the original unmodified map if the path does not correspond
    * to a field with a key that can be renamed.
    *
    * > MapV("a" -> MapV("b" -> "c")).rename(List("a"), "A")
    * > MapV("A" -> MapV("b" -> "c"))
    *
    * > MapV("a" -> MapV("b" -> "c")).rename(List("a", "b"), "B")
    * > MapV("a" -> MapV("B" -> "c"))
    */
  def rename(path: List[String], name: String): MapV = {
    def go(p: List[String], m: MapV): Option[MapV] = p match {
      case Nil =>
        None

      case old :: Nil =>
        if (m.elems.exists { _._1 == old }) {
          Some(MapV(m.elems.map { case (k, v) =>
            if (k == old) (name, v) else (k, v)
          }))
        } else {
          None
        }

      case h :: t =>
        m.get(h :: Nil) flatMap {
          case _: ArrayV  => None
          case _: ScalarV => None
          case _: QueryV  => None
          case m: MapV    => go(t, m)
        }
    }

    go(path, this) map { renamed =>
      path.init match {
        case Nil  => renamed
        case init => update(init, renamed)
      }
    } getOrElse this
  }

  def remove(path: List[String]): MapV = modify(path, { _ => None })
}
