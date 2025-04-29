package fauna.model

import fauna.ast.EvalContext
import fauna.atoms._
import fauna.auth.Auth
import fauna.lang.syntax._
import fauna.repo._
import fauna.repo.query.Query
import fauna.storage._
import fauna.storage.index.IndexTerm
import fauna.storage.ir._

/** Reverses the given set's order by flipping its boundaries before reading from it.
  * This set re-shape the source set so that it's only compatible with sets also
  * defined or queried in reverse order. Note that term-less indexes can't be defined
  * in reverse order, therefore the underlying set is re-shaped so that it works as
  * if the indexed document ID is given as a prefix cursor to the paginate function
  * (see `shape`).
  */
final case class ReverseSet(set: EventSet) extends EventSet {
  import EventSet._

  def count: Int = set.count

  def isFiltered: Boolean = set.isFiltered

  def isComposite: Boolean = true

  // Term-less indexes contain a single covered value: its document ID. However, they
  // can't be defined in reverse order. The following workaround forces the cursor
  // parser and the paginate function to interpret the user provided cursors as
  // prefix cursors. This approach makes reversed term-less indexes only compatible
  // with reversed indexes with the same shape. However, it requires special handling
  // of given cursors so that they become valid predicates to the storage engine. See
  // `PaginateImpl.dropCmp` and `CursorParser.parseArray`.
  lazy val shape: Shape =
    if (set.shape.values == 1) {
      EventSet.Shape(2, List(true, false), set.shape.isHistorical)
    } else {
      set.shape.copy(
        reversed = set.shape.reversed.view
          .dropRight(1)
          .map { !_ }
          .appended(false)
          .toList
      )
    }

  def filteredForRead(auth: Auth): Query[Option[EventSet]] =
    set.filteredForRead(auth) mapT { ReverseSet(_) }

  def snapshot(
    ec: EvalContext,
    from: Event,
    to: Event,
    size: Int,
    ascending: Boolean
  ): PagedQuery[Iterable[Elem[Event]]] = {
    // Users can only influence pagination by providing cursors with document IDs and
    // covered values. Thus, we only swap those components while re-shaping the event
    // cursors. Remaining relevant components are re-shaped by the `reshapeEvent`
    // function.
    @inline def swapBounds(a: SetEvent, b: SetEvent) = {
      val a0 = a.copy(docID = b.docID, values = b.values)
      val b0 = b.copy(docID = a.docID, values = a.values)
      (a0, b0)
    }

    val (from0, to0) = {
      val (from0, to0) = swapBounds(from.toSetEvent, to.toSetEvent)
      (reshapeEvent(ec, from0, ascending), reshapeEvent(ec, to0, !ascending))
    }

    set.snapshot(ec, to0, from0, size, !ascending) mapValuesT { reshapeElem(_) }
  }

  def sparseSnapshot(
    ec: EvalContext,
    keys: Vector[Event],
    ascending: Boolean
  ): PagedQuery[Iterable[Elem[Event]]] = {
    val keys0 = keys map { ev => reshapeEvent(ec, ev.toSetEvent, floor = ascending) }
    set.sparseSnapshot(ec, keys0, !ascending) mapValuesT { reshapeElem(_) }
  }

  def sortedValues(
    ec: EvalContext,
    from: Event,
    to: Event,
    size: Int,
    ascending: Boolean) =
    throw InvalidHistoricalSetException(this)

  def history(
    ec: EvalContext,
    from: Event,
    to: Event,
    size: Int,
    ascending: Boolean) =
    throw InvalidHistoricalSetException(this)

  private def reshapeEvent(ec: EvalContext, event: SetEvent, floor: Boolean) = {
    def event0 = {
      val event0 = if (floor) set.minValue.toSetEvent else set.maxValue.toSetEvent
      event0.copy(scopeID = ec.scopeID)
    }

    // Special case to term-less indexes: move document ID from the prefix cursor
    // to the document ID in the event boundary in order to provide a valid cursor
    // to storage engine (see `shape`).
    event.values match {
      case Vector(IndexTerm(DocIDV(docID), _)) => event0.copy(docID = docID)
      case Vector(IndexTerm(LongV(0), _))      => event0.copy(docID = DocID.MinValue)
      case Vector(IndexTerm(NullV, _))         => event0.copy(docID = DocID.MaxValue)
      case other =>
        event.docID match {
          case DocID.MaxValue | DocID.MinValue =>
            event0.copy(
              values = reverse(event.values)
            )
          case docID =>
            event0.copy(
              docID = docID,
              values = reverse(other)
            )
        }
    }
  }

  private def reshapeElem(elem: Elem[Event]) = {
    val setEvent = elem.value.toSetEvent
    if (setEvent.values.isEmpty) {
      // Special case for term-less indexes: move document ID into the value portion
      // of the result event so that higher layer components holding a prefix cursors
      // can use its held cursor to filter events returned by this set. See
      // `PaginateImpl.dropCmp`.
      selfElem(
        setEvent.copy(
          values = Vector(IndexTerm(setEvent.docID, true))
        ))
    } else {
      selfElem(
        setEvent.copy(
          values = reverse(setEvent.values)
        ))
    }
  }

  private def reverse(terms: Vector[IndexTerm]) =
    terms map { term => term.copy(reverse = !term.reverse) }
}
