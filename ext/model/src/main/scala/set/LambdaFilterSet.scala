package fauna.model

import fauna.ast._
import fauna.auth.Auth
import fauna.lang.syntax._
import fauna.repo.query.Query
import fauna.storage._

import EventSet._

case class LambdaFilterSet(source: EventSet, lambda: LambdaL, pos: Position) extends EventSet {

  def count = source.count

  override def canEqual(o: Any) = o.isInstanceOf[LambdaFilterSet]

  override def equals(o: Any) =
    o match {
      case o: LambdaFilterSet => source == o.source && lambda == o.lambda
      case _                  => false
    }

  def shape = source.shape

  def isFiltered = source.isFiltered

  def isComposite: Boolean = true

  def filteredForRead(auth: Auth) =
    source.filteredForRead(auth) mapT { src => this.copy(source = src) }

  def snapshot(ec: EvalContext, from: Event, to: Event, size: Int, ascending: Boolean) =
    source.snapshot(ec, from, to, size, ascending) selectMT { elem =>
      filterPredicate(ec, elem.value)
    }

  def sparseSnapshot(ec: EvalContext, keys: Vector[Event], ascending: Boolean) =
    source.sparseSnapshot(ec, keys, ascending) selectMT { elem =>
      filterPredicate(ec, elem.value)
    }

  private def filterPredicate(ec: EvalContext, ev: Event) = {
    val arg = Literal.fromIndexTerms(ev.scopeID, ev.tuple.coveredValues)

    ec.evalLambdaApply(lambda, arg, pos at "filter") flatMapT { res =>
      Query.value(Casts.Boolean(res, pos at "filter"))
    } map {
      case Right(bool) => bool
      case Left(errs)  => throw EvalErrorException(errs)
    }
  }

  def sortedValues(ec: EvalContext, from: Event, to: Event, size: Int, ascending: Boolean) =
    throw InvalidHistoricalSetException(this)

  def history(ec: EvalContext, from: Event, to: Event, size: Int, ascending: Boolean) =
    throw InvalidHistoricalSetException(this)
}
