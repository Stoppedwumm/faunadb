package fauna.model

import fauna.ast.EvalContext
import fauna.lang.{ Page, Timestamp }
import fauna.lang.syntax._
import fauna.repo._
import fauna.repo.query.Query
import fauna.storage._

import EventSet._

/**
  * Abstract class which contains implementation shared by Intersection and Difference sets.
  */
abstract class AbstractAlgebraicSet extends EventSet {
  val sets: List[EventSet]

  def count = sets.iterator map { _.count } sum

  override val MinPageSize = 8

  // assumes uniform shape of sets
  def shape = sets.head.shape

  protected def isPresent(memberships: Set[Int]): Boolean

  private def merge(streams: List[PagedQuery[Iterable[Elem[Event]]]], ascending: Boolean) = {
    val ord = SnapshotOrdering(ascending)

    MergeElemsIfPresent(streams, ord) { ps =>
      val byStreamBuffer = ps groupBy { _._2 }

      if (isPresent(byStreamBuffer.keys.toSet)) {
        val minStreamBuffer = byStreamBuffer.foldLeft(List.empty[Elem[Event]]) {
          case (Nil, (_, es))                     => es map { _._1 }
          case (as, (_, bs)) if as.size > bs.size => bs map { _._1 }
          case (as, _)                            => as
        }
        val sources = ps.flatMap { _._1.sources } distinct

        minStreamBuffer map { _.copy(sources = sources) }
      } else {
        Nil
      }
    }
  }

  def isComposite: Boolean = true

  def sparseSnapshot(ec: EvalContext, keys: Vector[Event], ascending: Boolean) =
    merge(sets map { _.sparseSnapshot(ec, keys, ascending) }, ascending)

  def snapshot(ec: EvalContext, from: Event, to: Event, size: Int, ascending: Boolean) =
    merge(sets map { _.snapshot(ec, from, to, size, ascending) }, ascending)

  def sortedValues(ec: EvalContext, from: Event, to: Event, size: Int, ascending: Boolean) =
    sets match {
      case Nil       => Query(Page(Nil))
      case List(set) => set.sortedValues(ec, from, to, size, ascending)
      case sets =>
        val ord = SnapshotOrdering(ascending)
        val streams = sets map { _.sortedValues(ec, from, to, size, ascending) }

        Page.mergeReduce(streams, Nil: List[(Elem[Event], Int)]) {
          case (Some(t), Nil)                                    => (Nil, List(t))

          // This ends up reversing the order of the events, so we'll
          // need to un-reverse them. We need to iterate to find the
          // initial memberships, however.
          case (Some(t), buf) if buf.head._1.value == t._1.value => (Nil, t :: buf)

          case (next, buf) =>
            var initMems = Set.empty[Int]
            var elems: List[(Elem[Event], Int)] = Nil

            buf foreach { t =>
              elems = t :: elems
              // If we're ascending, then prior presence is indicated
              //by Delete. If descending, then Create.
              if (ascending ^ t._1.value.isCreate) initMems += t._2 else initMems -= t._2
            }

            (filterValues(initMems, elems.iterator, ascending), next.toList)

        } (Query.MonadInstance, ord)
    }

  def history(ec: EvalContext, from: Event, to: Event, size: Int, ascending: Boolean) =
    sets match {
      case Nil       => Query(Page(Nil))
      case List(set) => set.history(ec, from, to, size, ascending)
      case src :: targets =>
        val adjFrom = from.at(if (ascending) SetAction.MinValue else SetAction.MaxValue)
        EventsFilter.mergeSources(ec, src, adjFrom, to, ascending) {

          case (_, EventsFilter.None) => Query(Page(Nil))

          case (t, srcFilter) =>
            val max = t.at(Timestamp.MaxMicros, SetAction.MaxValue)
            val min = t.at(Timestamp.Epoch, SetAction.MinValue)

            (targets map { _.sortedValues(ec, max, min, Everything, false).initValuesT } sequence) mapT { evs =>
              EventsFilter(evs.toSeq, adjFrom, to, ascending)
            } flatMap { filters =>
              var initMems = Set.empty[Int]

              val elemsQ = (srcFilter +: filters.toSeq).zipWithIndex map {
                case (EventsFilter.None, i) =>
                  Query.value((Nil, i))

                case (EventsFilter.All, i) =>
                  initMems += i
                  val sv = sets(i).sortedValues(ec, min, max, Everything, ascending)
                  sv.initValuesT map { (_, i) }

                case (EventsFilter.List(elems, _, _), i) =>
                  // If we're ascending, then if the first event in the future
                  // we see is a create, the assume the id was previously not in
                  // the set. conversely, if we're descending, and the first
                  // event in the past is a delete, then the id is not in the
                  // set afterwards.
                  if (ascending ^ elems.head.value.isCreate) initMems += i
                  Query.value((elems, i))
              }

              elemsQ.sequence map { elems =>
                val merged = elems.mergedOrderedIterator(HistoricalOrdering(ascending))
                Page(filterValues(initMems, merged, ascending))
              }
            }
        }
    }

  private def filterValues(initMems: Set[Int], iter: Iterator[(Elem[Event], Int)], ascending: Boolean): List[Elem[Event]] = {
    val (enterAct, leaveAct) = if (ascending) (Add, Remove) else (Remove, Add)
    val buf = List.newBuilder[Elem[Event]]

    var memberships = initMems
    var presence = isPresent(memberships)

    iter foreach {
      case (e, i) =>

        // If we're ascending and see a create, or if we're descending
        // and see a delete, then the id is now in the set.
        val newMems = if (e.value.action == enterAct) memberships + i else memberships - i
        val newPres = isPresent(newMems)

        if (!presence && newPres) { // entering
          buf += Elem(e.value.at(enterAct), e.sources)
        } else if (presence && !newPres) { // leaving
          buf += Elem(e.value.at(leaveAct), e.sources)
        }

        // reset
        memberships = newMems
        presence = newPres
    }

    buf.result()
  }
}
