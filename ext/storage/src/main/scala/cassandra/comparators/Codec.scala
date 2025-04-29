package fauna.storage.cassandra.comparators

import fauna.codex.builder._
import fauna.lang.Timestamp
import fauna.storage.Predicate
import fauna.storage.cassandra._

abstract class CassandraCodecImpl extends BinaryCodecType[CReader, CBuilder]
    with TupleCodecType[CReader, CBuilder]
    with CodecBuilder[CReader, CBuilder] {

  val format = CodecFormat

  implicit lazy val TimestampCodec: CassandraCodec[Timestamp] =
    CassandraCodec.Alias[Timestamp, Long](Timestamp.ofMicros, ts => Some(ts.micros))

  // helpers

  def decode[T](p: Predicate)(implicit dec: Decoder[T]) = {
    dec.decode(CReader(Iterator.single(p.rowKey) ++ p.columnName.iterator))
  }

  def encode[T](t: T)(implicit enc: Encoder[T]) = {
    val b = new CBuilder
    enc.encodeTo(b, t)

    val cvalues = b.result
    Predicate(cvalues.head, cvalues.tail)
  }

  def comparator[T: Decoder] = {
    val cs = components[T]
    if (cs.size > 1) CompositeType(cs) else cs.head
  }

  def components[T](implicit decoder: Decoder[T]): Vector[BasicComparator] =
    _components(decoder)

  private def _components[T](enc: Decoder[T]): Vector[BasicComparator] =
    (enc: @unchecked) match {
      case e: SynthesizedCodec[_] => _components(e.innerDecoder)
      case e: AliasDecoder[_, _]  => _components(e.innerDecoder)
      case e: TupleDecoder[_]     => e.innerDecoders flatMap { _components }
      case ByteDecoder            => Vector(BytesType)
      case ShortDecoder           => Vector(Int32Type)
      case IntDecoder             => Vector(Int32Type)
      case LongDecoder            => Vector(LongType)
      case FloatDecoder           => Vector(FloatType)
      case DoubleDecoder          => Vector(DoubleType)
      case BooleanDecoder         => Vector(BooleanType)
      case CharDecoder            => Vector(UTF8Type)
      case StringDecoder          => Vector(UTF8Type)
      case BytesDecoder           => Vector(BytesType)
    }
}
