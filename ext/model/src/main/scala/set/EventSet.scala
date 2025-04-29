package fauna.model

import fauna.ast.{ EvalContext, EventL, Literal }
import fauna.atoms.{ DocID, ScopeID }
import fauna.auth.Auth
import fauna.lang.Page
import fauna.lang.syntax._
import fauna.repo._
import fauna.repo.query.Query
import fauna.storage._
import scala.math.Ordering

object EventSet {

  val MaxInternalPageSize = 4096

  // FIXME: while in transition to fully limiting which sets have
  // historical events views, new sets which do not support events out
  // of the gate will throw this exception from their implementation of
  // `history` and `sortedEvents`.
  // The exception is caught and turned into a FQL error in PaginateFunction.
  case class InvalidHistoricalSetException(set: EventSet) extends IllegalArgumentException(
    s"Set $set does not support historical events reads.")

  case class Elem[+T](value: T, sources: List[EventSet] = Nil) {
    def equiv[T0 >: T](other: Elem[T0]) = value == other.value
  }

  class ElemOrdering[T](inner: Ordering[T]) extends Ordering[Elem[T]] {
    def compare(a: Elem[T], b: Elem[T]) = inner.compare(a.value, b.value)
  }

  case class Shape(values: Int, reversed: List[Boolean], isHistorical: Boolean) {
    def compatible(other: Shape) = {
      @annotation.tailrec
      def compat0(a: List[Boolean], b: List[Boolean]): Boolean =
        (a, b) match {
          case (a :: as, b :: bs) => if (a == b) compat0(as, bs) else false
          case _                  => true
        }

      isHistorical == other.isHistorical && compat0(reversed, other.reversed)
    }
  }

  object Shape {
    val Zero = Shape(0, Nil, false)
    val Single = Shape(1, List(false), false)

    def apply(idx: IndexConfig): Shape = {
      val b = List.newBuilder[Boolean]
      b ++= idx.reverseFlags
      b += false

      Shape(idx.values.size + 1, b.result(), false)
    }
  }

  val historicalOrdering = new ElemOrdering(Event.HistoricalOrd)
  val snapshotOrdering = new ElemOrdering(Event.SnapshotOrd)

  def HistoricalOrdering(ascending: Boolean) =
    if (ascending) historicalOrdering else historicalOrdering.reverse

  def SnapshotOrdering(ascending: Boolean) =
    if (ascending) snapshotOrdering else snapshotOrdering.reverse

  def MergeElems[T](streams: Seq[PagedQuery[Iterable[Elem[T]]]], ord: Ordering[Elem[T]]) =
    MergeElemsIfPresent(streams, ord) { ps =>
      List(Elem(ps.head._1.value, ps flatMap { _._1.sources } distinct))
    }

  def MergeElemsIfPresent[T](streams: Seq[PagedQuery[Iterable[Elem[T]]]], ord: Ordering[Elem[T]])(select: List[(Elem[T], Int)] => List[Elem[T]]): PagedQuery[Iterable[Elem[T]]] =
    Page.mergeReduce(streams, Nil: List[(Elem[T], Int)]) {
      case (None, Nil)         => (Nil, Nil)
      case (Some((e, i)), Nil) => (Nil, List((e, i)))
      case (Some((e, i)), ps) if ord.equiv(ps.head._1, e) =>
        (Nil, (e, i) :: ps)
      case (next, ps) => (select(ps), next.toList)
    } (Query.MonadInstance, ord)
}

import EventSet._

trait EventSet {
  // Widen this interface only as a last resort. It is very difficult to
  // correctly and performantly implement the query functions for
  // new index views, especially on Events.

  val MinPageSize = 1

  def count: Int

  // default min/max values for most sets
  def minValue: Event = Event.MinValue
  def maxValue: Event = Event.MaxValue

  // Returns true if the event set should be filtered based
  // on read permissions, or false otherwise.
  def isFiltered: Boolean

  // Returns true if the event set is composed of other sets
  def isComposite: Boolean

  // Returns an event set that is filtered based on read permissions,
  // or none if the set would have been empty.
  def filteredForRead(auth: Auth): Query[Option[EventSet]]

  // Returns a page of entries in the set.
  def snapshot(ec: EvalContext, from: Event, to: Event, size: Int, ascending: Boolean): PagedQuery[Iterable[Elem[Event]]]

  // Returns a page of events from the set, ordered by Timestamp and Action.
  def history(ec: EvalContext, from: Event, to: Event, size: Int, ascending: Boolean): PagedQuery[Iterable[Elem[Event]]]

  def sparseSnapshot(ec: EvalContext, keys: Vector[Event], ascending: Boolean): PagedQuery[Iterable[Elem[Event]]]

  // Returns the instance with the "minimal" value, if any
  def snapshotHead(ec: EvalContext): Query[Option[(ScopeID, DocID)]] =
    snapshot(ec, Event.MinValue, Event.MaxValue, 1, true).headValueT mapT { e =>
      e.value.scopeID -> e.value.docID
    }

  // Return a sequence of events for a given entry in the set.
  def sortedValues(ec: EvalContext, from: Event, to: Event, size: Int, ascending: Boolean): PagedQuery[Iterable[Elem[Event]]]

  // Returns the shape of the set.
  def shape: Shape

  def getLiteral(event: Event): Literal = if (shape.isHistorical) {
    EventL(event)
  } else {
    Literal.fromIndexTerms(event.scopeID, event.toSetEvent.tuple.coveredValues)
  }

  // produces an Elem with the specified event and a singleton list with only
  // this EventSet in it.
  protected final def selfElem[E <: Event](ev: E) =
    Elem(ev, List(this))
}
