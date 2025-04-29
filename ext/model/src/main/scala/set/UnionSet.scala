package fauna.model

import fauna.ast.EvalContext
import fauna.auth.Auth
import fauna.lang.syntax._
import fauna.repo._
import fauna.storage.Event

import EventSet._

case class Union(sets: Seq[EventSet]) extends EventSet {

  def count = sets.iterator map { _.count } sum

  def isFiltered: Boolean = sets.exists { _.isFiltered }

  def isComposite: Boolean = true

  def filteredForRead(auth: Auth) =
    (sets map { _.filteredForRead(auth) } sequence) map {
      _.flatten.toList match {
        case Nil  => None
        case sets => Some(Union(sets))
      }
    }

  def sortedValues(ec: EvalContext, from: Event, to: Event, size: Int, ascending: Boolean) = {
    val streams = sets map { _.sortedValues(ec, from, to, size, ascending) }
    MergeElems(streams, SnapshotOrdering(ascending))
  }

  def history(ec: EvalContext, from: Event, to: Event, size: Int, ascending: Boolean) =
    MergeElems(sets map { _.history(ec, from, to, size, ascending) }, HistoricalOrdering(ascending))

  private def merge(streams: Seq[PagedQuery[Iterable[Elem[Event]]]], ascending: Boolean) =
    MergeElemsIfPresent(streams, SnapshotOrdering(ascending)) { ps =>
      val sources = ps flatMap { _._1.sources } distinct
      val byStream = ps groupBy { _._2 }
      val maxStream = byStream.foldLeft(List.empty[Elem[Event]]) {
        case (Nil, (_, es)) => es map { _._1 }
        case (as, (_, bs)) if as.size < bs.size => bs map { _._1 }
        case (as, _) => as
      }
      maxStream map { _.copy(sources = sources) }
    }

  def snapshot(ec: EvalContext, from: Event, to: Event, size: Int, ascending: Boolean) =
    merge(sets map { _.snapshot(ec, from, to, size, ascending) }, ascending)

  def sparseSnapshot(ec: EvalContext, keys: Vector[Event], ascending: Boolean) =
    merge(sets map { _.sparseSnapshot(ec, keys, ascending) }, ascending)

  // assumes uniform shape of sets
  def shape = sets.head.shape
}
