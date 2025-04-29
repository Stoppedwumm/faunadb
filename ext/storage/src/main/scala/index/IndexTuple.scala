package fauna.storage.index

import fauna.atoms._
import fauna.codex.cbor._
import fauna.lang.Timestamp
import fauna.storage.ir._

object IndexTuple {
  val MinValue = IndexTuple(ScopeID.MinValue, DocID.MinValue)
  val MaxValue = IndexTuple(ScopeID.MaxValue, DocID.MaxValue)

  implicit val codec = CBOR.TupleCodec[IndexTuple]

  /** Equivalence of index tuples, which differs from 'regular' equality.
    * 1. TTL is not part of cell name, therefore, tuples are equivalent without it.
    * 2. For floating point values, NaN is equivalent to NaN.
    */
  object StorageEquiv extends Equiv[IndexTuple] {
    def equiv(a: IndexTuple, b: IndexTuple): Boolean =
      a.scopeID == b.scopeID && a.docID == b.docID && a.values.size == b.values.size && a.values.view
        .zip(b.values)
        .forall { case (ta, tb) => ta.equiv(tb) }
  }
}

/** All index tuples cover, at minimum, the ID of the relevant document. Optionally,
  * they will also cover any additional terms specified in the index configuration.
  *
  * Note that tuples can be equal or equivalent: tuples are equal when all elements
  * are equal; tuples are equivalent if all elements except TTL are equal. TTL is
  * left out of equivalence check since it's not part of the cell name in storage.
  */
final case class IndexTuple(
  scopeID: ScopeID,
  docID: DocID,
  values: Vector[IndexTerm] = Vector.empty,
  ttl: Option[Timestamp] = None)
    extends Ordered[IndexTuple] {

  // Returns the segment of this value to be used as a pagination
  // cursor in the query API.
  def cursor: Vector[IndexTerm] =
    docID match {
      case DocID.MinValue | DocID.MaxValue => values
      case id                              => values :+ IndexTerm(DocIDV(id))
    }

  def coveredValues: Vector[IndexTerm] =
    if (values.isEmpty) {
      Vector(IndexTerm(DocIDV(docID)))
    } else {
      values
    }

  def compareValues(that: IndexTuple): Int = {
    val thisSize = values.size
    val thatSize = that.values.size
    val minSize = math.min(thisSize, thatSize)
    var i = 0
    while (i < minSize) {
      val cmp = values(i) compare that.values(i)
      if (cmp != 0) return cmp
      i += 1
    }
    thisSize - thatSize
  }

  def compare(that: IndexTuple): Int = {
    def compare0(c: Int, next: => Int) =
      if (c == 0) {
        next
      } else {
        c
      }

    compare0(
      compareValues(that),
      compare0(docID compare that.docID, scopeID compare that.scopeID))
  }

  @inline def equiv(other: IndexTuple) =
    IndexTuple.StorageEquiv.equiv(this, other)
}
