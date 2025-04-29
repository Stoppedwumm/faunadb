package fauna.ast

import fauna.codex.cbor.{ CBOR, CBOROrdering }
import fauna.model.runtime.Effect
import fauna.repo.query.Query
import scala.annotation.unused

sealed abstract class ComparisonFunction(name: String, operator: Int => Boolean)
    extends QFunction {
  val effect = Effect.Pure

  def apply(
    elems: List[Literal],
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    def compareAll(elems: List[Literal]): Boolean = {
      val encodedElems = elems map { e => CBOR.encode(Literal.toIndexTerm(e)) }
      encodedElems.sliding(2).forall {
        case x :: y :: Nil => operator(CBOROrdering.compare(x, y))
        case _             => true // one or zero operands total
      }
    }

    val errors = elems collect { case UnresolvedRefL(ref) =>
      UnresolvedRefError(ref, pos at name)
    }

    if (errors.nonEmpty) {
      Query.value(Left(errors))
    } else {
      Query.value(Right(BoolL(compareAll(elems))))
    }
  }
}

object LessThanFunction extends ComparisonFunction("lt", _ < 0)
object LessThanOrEqualsFunction extends ComparisonFunction("lte", _ <= 0)
object GreaterThanFunction extends ComparisonFunction("gt", _ > 0)
object GreaterThanOrEqualsFunction extends ComparisonFunction("gte", _ >= 0)
