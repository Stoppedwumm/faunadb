package fauna.storage.index

import fauna.codex.cbor._
import fauna.storage.ir._

object IndexTerm {
  // Min/Max values viz. the CBOR comparator.
  val MinValue = IndexTerm(LongV(Long.MinValue))
  val MaxValue = IndexTerm(NullV)

  implicit object CBORCodec
      extends CBOR.SwitchCodec[IndexTerm]
      with DelegatingCBORSwitch[IRValue, IndexTerm] {
    val delegate = IRValue.CBORCodec

    def transform(ir: IRValue) = IndexTerm(ir, false)

    override def readTag(tag: Long, stream: CBORParser) = tag match {
      case CBOR.ReverseTag => IndexTerm.unsafe(delegate.decode(stream), true)
      case tag             => IndexTerm.unsafe(delegate.readTag(tag, stream), false)
    }

    def encode(stream: CBORWriter, t: IndexTerm) = {
      if (t.reverse) stream.writeTag(CBOR.ReverseTag)
      IRValue.CBORCodec.encode(stream, t.value)
    }
  }

  def apply(value: IRValue, reverse: Boolean): IndexTerm =
    unsafe(normalizedValue(value), reverse)

  def apply(value: IRValue): IndexTerm =
    unsafe(normalizedValue(value), false)

  // Assumes that normalization has been done previously
  def unsafe(value: IRValue, reverse: Boolean): IndexTerm =
    new IndexTerm(value, reverse)

  // Normalizes values in order for Maps to sort by sorted field names then values.
  private def normalizedValue(value: IRValue): IRValue = value match {
    case MapV(fs) =>
      MapV(fs.sortWith { case ((a, _), (b, _)) => a.compare(b) < 0 }.map {
        case (n, v) => n -> normalizedValue(v)
      })
    case ArrayV(es) => ArrayV(es.map(normalizedValue))
    case value      => value
  }
}

/** The terms of an index entry are the values at a given path (a
  * "term path") in an instance (e.g. the term "fred" might be at the
  * term path "data.name"), and may be either covered (in the entry's
  * value) or compound (in the entry's key).
  */
case class IndexTerm(value: IRValue, reverse: Boolean) extends Ordered[IndexTerm] {

  // PageMerger merges multiple sequences of IndexValue objects which in turn
  // do lots of comparisons on their IndexTuple instances and through them
  // the IndexTerm instances. Memoizing the encoded representation used for
  // comparisons is a win in both CPU and memory allocations.
  private lazy val encoded = CBOR.encode(this)

  def compare(that: IndexTerm): Int =
    CBOROrdering.compare(encoded, that.encoded)

  // A lower bound estimate for the serialized size of the term.
  def serializedSizeLowerBound(): Long =
    IRValue.serializedSizeLowerBound(value) + (if (reverse) 1 else 0)

  def equiv(b: IndexTerm): Boolean = this.reverse == b.reverse && this.value.equiv(b.value)
}
