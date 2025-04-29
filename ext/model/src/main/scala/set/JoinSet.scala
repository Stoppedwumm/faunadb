package fauna.model

import fauna.ast._
import fauna.auth.Auth
import fauna.lang.Page
import fauna.lang.syntax._
import fauna.repo._
import fauna.repo.query.Query
import fauna.storage._
import fauna.trace._

import EventSet._

case class LambdaJoin(source: EventSet, lambda: LambdaL, pos: Position) extends AbstractJoin {

  def count = source.count

  def isFiltered: Boolean = source.isFiltered

  def filteredForRead(auth: Auth) =
    source.filteredForRead(auth) mapT { src => this.copy(source = src) }

  protected def targetSet(ec: EvalContext, evt: Event) = {
    val arg = Literal.fromIndexTerms(evt.scopeID, evt.tuple.coveredValues)

    ec.evalLambdaApply(lambda, arg, pos at "with") flatMapT { setR =>
      Query(Casts.Set(setR, pos at "with"))
    } flatMap {
      case Right(set) => set.filteredForRead(ec.auth)
      case Left(errs) => throw EvalErrorException(errs)
    }
  }

  // TODO: derive shape from the lambda expression
  def shape = Shape.Zero
}

case class IndexJoin(source: EventSet, idx: IndexConfig) extends AbstractJoin {

  def count = source.count

  def isFiltered: Boolean = source.isFiltered

  def filteredForRead(auth: Auth) =
    source.filteredForRead(auth) mapT { src => this.copy(source = src) }

  protected def targetSet(ec: EvalContext, event: Event) = {
    val values = event match {
      case _: DocEvent  => Vector.empty
      case se: SetEvent => se.tuple.coveredValues
    }
    IndexSet(idx, values).filteredForRead(ec.auth)
  }

  def shape = Shape(idx)
}

abstract class AbstractJoin extends EventSet {

  def source: EventSet

  protected def targetSet(ec: EvalContext, value: Event): Query[Option[EventSet]]

  def isComposite: Boolean = true

  // This is "correct" in the sense that it filters events the same
  // way as events() does. (target events are emitted based on the
  // presence of the source, but no synthetic events are emitted as
  // the source changes)
  //
  // However, this isn't good enough for a join within a join, since
  // proper window bounds per-id are not emitted.
  //
  // This is also woefully inefficient.

  def sortedValues(ec: EvalContext, from: Event, to: Event, size: Int, ascending: Boolean) = {
    val (min, max) = if (ascending) (Event.MinValue, Event.MaxValue) else (Event.MaxValue, Event.MinValue)

    EventsFilter.mergeSources(ec, source, min, max, ascending) {
      case (_, EventsFilter.None) => Query(Page(Nil))

      case (t, EventsFilter.All) =>
        targetSet(ec, t) flatMap {
          case Some(s) => s.sortedValues(ec, from, to, size, ascending)
          case None    => Query(Page(Nil))
        }

      case (t, EventsFilter.List(src, _, _)) =>
        targetSet(ec, t) flatMap {
          case None => Query(Page(Nil))
          case Some(target) =>
            target.sortedValues(ec, from, to, Everything, ascending).initValuesT map { targ =>
              val ord = HistoricalOrdering(ascending)
              val iter = Seq(src, targ.toSeq.sorted(ord)).zipWithIndex.mergedOrderedIterator(ord)
              val enterAct = if (ascending) Add else Remove

              val buf = List.newBuilder[Elem[Event]]

              var isPresent = src.head.value.action != enterAct

              iter foreach {
                case (e, 0) => isPresent = e.value.action == enterAct
                case (e, _) => if (isPresent) buf += e
              }

              Page(buf.result().sorted(SnapshotOrdering(ascending)))
            }
        }
    }
  }

  def snapshot(ec: EvalContext, from: Event, to: Event, size: Int, ascending: Boolean) = {
    val (f, t) = if (ascending) {
      (Event.MinValue, Event.MaxValue)
    } else {
      (Event.MaxValue, Event.MinValue)
    }
    source.snapshot(ec, f, t, Everything, ascending).initValuesT map { srcs =>
      traceMsg(s"  JOIN: Read ${srcs.size} IDs for source set ${source}")
      srcs map { e => targetSet(ec, e.value) }
    } flatMap { targets =>
      val idQs = targets map {
        _ flatMap {
          case Some(s) => s.snapshot(ec, from, to, size, ascending)
          case None    => PagedQuery.empty
        }
      }

      MergeElems(idQs.toSeq, SnapshotOrdering(ascending))
    } recoverWith {
      case e: RangeArgumentException =>
        // log and propagate error
        AbstractJoin.log.error(s"AbstractJoin f:$f t:$t ascending:$ascending")
        Query.fail(e)
    }
  }

  def sparseSnapshot(ec: EvalContext, keys: Vector[Event], ascending: Boolean) = {
    val (f, t) = if (ascending) {
      (Event.MinValue, Event.MaxValue)
    } else {
      (Event.MaxValue, Event.MinValue)
    }

    source.snapshot(ec, f, t, Everything, ascending).initValuesT map { srcs =>
      traceMsg(s"  JOIN(MultiGet): Read ${srcs.size} IDs for source set ${source}")
      srcs map { e => targetSet(ec, e.value) }
    } flatMap { targets =>
      val idQs = targets map {
        _ flatMap {
          case Some(s) => s.sparseSnapshot(ec, keys, ascending)
          case None    => PagedQuery.empty
        }
      }

      MergeElems(idQs.toSeq, SnapshotOrdering(ascending))
    }
  }

  def history(ec: EvalContext, from: Event, to: Event, size: Int, ascending: Boolean) =
    EventsFilter.mergeSources(ec, source, from, to, ascending) {
      case (_, EventsFilter.None) => Query(Page(Nil))

      case (t, EventsFilter.All) =>
        targetSet(ec, t) flatMap {
          case Some(s) => s.history(ec, from, to, size, ascending)
          case None    => Query(Page(Nil))
        }

      case (t, EventsFilter.List(src, from, to)) =>
        targetSet(ec, t) flatMap {
          case None => Query(Page(Nil))
          case Some(target) =>
            val stream = target.history(ec, from, to, size, ascending)
            val enterAct = if (ascending) Add else Remove

            // If we're ascending and the first event in the future we
            // see is a create, then assume the instance was
            // previously not in the set.
            //
            // Conversely, if we're descending and the first event in
            // the past is a delete, then the instance is not in the
            // set afterwards.
            val isPresent = src.head.value.action != enterAct

            Page.mergeReduce(Seq(Query(Page[Query](src)), stream), isPresent) {
              case (Some((e, 0)), _)     => (Nil, e.value.action == enterAct)
              case (Some((e, _)), true)  => (List(e), true)
              case (Some((_, _)), false) => (Nil, false)
              case (None, p)             => (Nil, p)
            } (Query.MonadInstance, HistoricalOrdering(ascending))
        }
    }
}

object AbstractJoin {
  private val log = getLogger
}
