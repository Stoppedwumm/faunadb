package  fauna.model

import fauna.ast.EvalContext
import fauna.auth.Auth
import fauna.lang.syntax._
import fauna.repo.RangeArgumentException
import fauna.repo.query.Query
import fauna.storage._
import Event.SnapshotOrd
import EventSet._

case class RangeSet(source: EventSet, lower: SetEvent, upper: SetEvent) extends EventSet {

  def count = source.count

  def shape = source.shape

  def isFiltered = source.isFiltered

  def isComposite: Boolean = true

  def filteredForRead(auth: Auth) =
    source.filteredForRead(auth) mapT { src => this.copy(source = src) }

  private def compareCursors(a: Event, b: Event, floor: Boolean): Int = {
    val as = a.toSetEvent.valuesWithDocID.iterator
    val bs = b.toSetEvent.valuesWithDocID.iterator

    while (as.hasNext && bs.hasNext) {
      val cmp = as.next() compare bs.next()
      if (cmp != 0) return cmp
    }

    if (as.hasNext) {
      if (floor) 1 else -1
    } else if (bs.hasNext) {
      if (floor) -1 else 1
    } else {
      0
    }
  }

  def snapshot(ec: EvalContext, from: Event, to: Event, size: Int, ascending: Boolean) = {
    val (from0, to0) = if (ascending) {
      (if (compareCursors(from, lower, true) < 0) lower else from,
       if (compareCursors(to, upper, false) > 0) upper else to)
    } else {
      (if (compareCursors(from, upper, false) > 0) upper else from,
       if (compareCursors(to, lower, true) < 0) lower else to)
    }
    source.snapshot(ec, from0, to0, size, ascending) recoverWith {
      case e: RangeArgumentException =>
        // log and propagate error
        RangeSet.log.error(s"RangeSet from:$from0 to:$to0 lower:$lower upper:$upper ascending:$ascending")
        Query.fail(e)
    }
  }

  def sparseSnapshot(ec: EvalContext, keys: Vector[Event], ascending: Boolean) = {
    val keys0 = keys filter { k => SnapshotOrd.gteq(k, lower) && SnapshotOrd.lteq(k, upper) }
    source.sparseSnapshot(ec, keys0, ascending)
  }

  def sortedValues(ec: EvalContext, from: Event, to: Event, size: Int, ascending: Boolean) =
    throw InvalidHistoricalSetException(this)

  def history(ec: EvalContext, from: Event, to: Event, size: Int, ascending: Boolean) =
    throw InvalidHistoricalSetException(this)
}

object RangeSet {
  private val log = getLogger
}
