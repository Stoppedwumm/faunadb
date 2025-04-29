package fauna.model

import fauna.ast.EvalContext
import fauna.atoms._
import fauna.repo._
import fauna.repo.query.Query
import fauna.storage._
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.{ List => SList }

import EventSet._

sealed trait EventsFilter {
  def isEmpty: Boolean
}

object EventsFilter {

  case object All extends EventsFilter { val isEmpty = false }
  case object None extends EventsFilter { val isEmpty = true }
  case class List(src: SList[Elem[Event]], from: Event, to: Event) extends EventsFilter { def isEmpty = false }

  def apply(elems: Seq[Elem[Event]], from: Event, to: Event, ascending: Boolean): EventsFilter =
    buildFilter(ListBuffer(elems: _*), from, to, ascending)

  private type MkStreamF = (Event, EventsFilter) => PagedQuery[Iterable[Elem[Event]]]

  def mergeSources(ec: EvalContext, set: EventSet, from: Event, to: Event, ascending: Boolean)(mk: MkStreamF): PagedQuery[Iterable[Elem[Event]]] =
    // FIXME: Allow horizontal pagination across the source in the
    // case where this does not retrieve the entire source set.
    set.sortedValues(ec, Event.MaxValue, Event.MinValue, Everything, false) flatMap { page =>
      val streams = ListBuffer.empty[PagedQuery[Iterable[Elem[Event]]]]
      val buf = ListBuffer.empty[Elem[Event]]

      page.value foreach { e =>

        // next ID. add the stream and reset the buffer.
        if (buf.nonEmpty && e.value.tuple != buf.head.value.tuple) {
          streams += mk(buf.head.value, buildFilter(buf, from, to, ascending))
          buf.clear()
        }

        buf += e
      }


      // if the source page has a next, then the last batch is
      // possibly partial, so drop the last batch.
      if (!page.hasNext && buf.nonEmpty) {
        streams += mk(buf.head.value, buildFilter(buf, from, to, ascending))
      }

      Query.MonadInstance.sequence(streams.result()) flatMap { pages =>
        MergeElems(pages collect { case p if p.value.nonEmpty || p.hasNext => Query(p) }, HistoricalOrdering(ascending))
      }
    }

  // mutates the provided buffer.
  private def buildFilter(buf: ListBuffer[Elem[Event]], from: Event, to: Event, ascending: Boolean): EventsFilter = {
    val (min, max) = if (ascending) (from, to) else (to, from)

    // normalize buffer:

    // strip out repeat events.
    var i = 0
    while (buf.size > (i + 1)) {
      if (buf(i).value.action == buf(i + 1).value.action) {

        // if the event is a create, remove the later one. if it is a
        // delete, remove the earlier one. This allows for presence in
        // Unions with repeat events.
        buf.remove(if (buf(i).value.action.isCreate) i else i + 1)
      } else {
        i += 1
      }
    }

    // remove earliest element if it is a delete.
    if (buf.nonEmpty && buf.last.value.action.isDelete) buf.dropRightInPlace(1)

    // trim the buffer so that there are no events after the range
    // requested, and at most one event beforehand, that can be used
    // to determine initial presence.
    while (buf.nonEmpty && Event.HistoricalOrd.gt(buf.head.value, max))
      buf.dropInPlace(1)
    while ((buf.size > 1) && Event.HistoricalOrd.lt(buf(buf.size - 2).value, min))
      buf.dropRightInPlace(1)

    if (buf.isEmpty) {
      // the src does not exist within the range requested
      None

    } else if (Event.HistoricalOrd.lt(buf.head.value, min)) {

      // if the head is a create, the source exists in the entire
      // range requested, so no need to filter. If the head is a
      // delete, then the source was deleted before the range
      // requested.
      if (buf.head.value.action.isCreate) All else None

    } else {

      // filter the target based on the changing existence of the
      // source.

      // Bounds for the target, based on start and end presence
      // NB. There is a fence post here: if the event at either end of
      // the buffer is not the most inclusive cursor, we must expand
      // the cursor's inclusivity in order to capture events sitting
      // on the fence.
      val bMax = if (buf.head.value.action.isCreate) {
        buf.head.value.at(max.ts.validTS, max.action)
      } else {
        buf.head.value.withDocID(DocID.MaxValue)
      }
      val bMin = if (buf.last.value.action.isDelete) {
        buf.head.value.at(min.ts.validTS, min.action)
      } else {
        buf.last.value.withDocID(DocID.MinValue)
      }

      val (bFrom, bTo) = if (ascending) (bMin, bMax) else (bMax, bMin)

      // flip the source events around to match the requested order.
      List(if (ascending) buf.reverseIterator.toList else buf.toList, bFrom, bTo)
    }
  }
}
