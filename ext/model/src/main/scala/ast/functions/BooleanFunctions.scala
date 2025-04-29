package fauna.ast

import fauna.model.runtime.Effect
import fauna.repo.query.Query
import scala.annotation.unused

/** This function is NOT short-circuit
  *
  * @deprecated 3
  */
object AndFunction extends QFunction {
  val effect = Effect.Pure

  @annotation.tailrec
  private def fold(elems: List[Boolean]): Literal =
    elems match {
      case true :: rest => fold(rest)
      case false :: _   => FalseL
      case _            => TrueL
    }

  def apply(
    bools: List[Boolean],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] =
    Query(Right(fold(bools)))
}

/** This function is NOT short-circuit
  *
  * @deprecated 3
  */
object OrFunction extends QFunction {
  val effect = Effect.Pure

  @annotation.tailrec
  private def fold(elems: List[Boolean]): Literal =
    elems match {
      case true :: _     => TrueL
      case false :: rest => fold(rest)
      case _             => FalseL
    }

  def apply(
    bools: List[Boolean],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] =
    Query(Right(fold(bools)))
}

object NotFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    bool: Boolean,
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] =
    Query(Right(BoolL(!bool)))
}
