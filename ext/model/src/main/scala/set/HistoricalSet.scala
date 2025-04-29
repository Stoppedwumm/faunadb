package fauna.model

import fauna.ast.EvalContext
import fauna.auth.Auth
import fauna.lang.syntax._
import fauna.repo._
import fauna.storage.Event

case class HistoricalSet(set: EventSet) extends EventSet {
  import EventSet.Elem

  def count = set.count

  val shape = set.shape.copy(isHistorical = true)

  override def minValue: Event = set.minValue
  override def maxValue: Event = set.maxValue

  def isFiltered: Boolean = set.isFiltered

  def isComposite: Boolean = true

  def filteredForRead(auth: Auth) =
    set.filteredForRead(auth) mapT { HistoricalSet(_) }

  def sortedValues(ec: EvalContext, from: Event, to: Event, size: Int, asc: Boolean) =
    rewrite(set.sortedValues(ec, from, to, size, asc))

  def history(ec: EvalContext, from: Event, to: Event, size: Int, asc: Boolean) =
    rewrite(set.history(ec, from, to, size, asc))

  def snapshot(ec: EvalContext, from: Event, to: Event, size: Int, asc: Boolean) =
    rewrite(set.snapshot(ec, from, to, size, asc))

  def sparseSnapshot(ec: EvalContext, keys: Vector[Event], asc: Boolean) =
    rewrite(set.sparseSnapshot(ec, keys, asc))

  private def rewrite(events: PagedQuery[Iterable[Elem[Event]]]): PagedQuery[Iterable[Elem[Event]]] =
    if (set.isComposite) {
      events
    } else {
      events mapValuesT { _.copy(sources = List(this)) }
    }
}
