package fauna.storage.api.set

import fauna.atoms._
import fauna.codex.cbor.CBOR
import fauna.lang.Timestamp
import fauna.storage.{ Add, AtValid, BiTimestamp, Remove, Unresolved }
import fauna.storage.index.{ IndexTerm, IndexTuple, IndexValue }

/** An event in the history of a set, representing the addition or removal of an
  * element from a set as of a valid time. It's analogous to a Version.
  */
sealed trait Element {
  def scopeID: ScopeID
  def docID: DocID
  def values: Vector[IndexTerm]
  def validTS: Option[Timestamp]
  def ttl: Option[Timestamp]
  import Element._

  def toTuple =
    IndexTuple(scopeID, docID, values, ttl)

  def toIndexValue =
    this match {
      case _: Live =>
        IndexValue(toTuple, validTS.fold(Unresolved: BiTimestamp)(AtValid(_)), Add)
      case _: Deleted =>
        IndexValue(
          toTuple,
          validTS.fold(Unresolved: BiTimestamp)(AtValid(_)),
          Remove)
    }
}

object Element {

  def apply(value: IndexValue) =
    if (value.isCreate) {
      Element.Live(value)
    } else {
      Element.Deleted(value)
    }

  object ByEventOrdering extends Ordering[Element] {

    override def equiv(a: Element, b: Element): Boolean =
      IndexValue.ByEventOrdering.equiv(a.toIndexValue, b.toIndexValue)

    def compare(a: Element, b: Element) =
      IndexValue.ByEventOrdering.compare(a.toIndexValue, b.toIndexValue)
  }

  object ByValueOrdering extends Ordering[Element] {

    override def equiv(a: Element, b: Element): Boolean =
      IndexValue.ByValueOrdering.equiv(a.toIndexValue, b.toIndexValue)

    def compare(a: Element, b: Element) =
      IndexValue.ByValueOrdering.compare(a.toIndexValue, b.toIndexValue)
  }

  // A live element, the addition of an element to a set at a valid time.
  final case class Live(
    scopeID: ScopeID,
    docID: DocID,
    values: Vector[IndexTerm],
    validTS: Option[Timestamp],
    ttl: Option[Timestamp] = None)
      extends Element

  object Live {
    implicit val Codec =
      CBOR.TupleCodec[Live]

    // Scala.
    val ByEventOrdering = {
      implicit val ordering = Element.ByEventOrdering
      Ordering.by[Element.Live, Element](identity)
    }

    val ByValueOrdering = {
      implicit val ordering = Element.ByValueOrdering
      Ordering.by[Element.Live, Element](identity)
    }

    def apply(value: IndexValue) =
      new Live(
        value.tuple.scopeID,
        value.tuple.docID,
        value.tuple.values,
        value.ts.validTSOpt,
        value.tuple.ttl
      )
  }

  // A deleted element, the removal of an element from a set at a valid time.
  final case class Deleted(
    scopeID: ScopeID,
    docID: DocID,
    values: Vector[IndexTerm],
    validTS: Option[Timestamp],
    ttl: Option[Timestamp] = None)
      extends Element

  object Deleted {
    implicit val Codec =
      CBOR.TupleCodec[Deleted]

    def apply(value: IndexValue) =
      new Deleted(
        value.tuple.scopeID,
        value.tuple.docID,
        value.tuple.values,
        value.ts.validTSOpt,
        value.tuple.ttl
      )
  }

  implicit val Codec =
    CBOR.SumCodec[Element](
      Live.Codec,
      Deleted.Codec
    )
}
