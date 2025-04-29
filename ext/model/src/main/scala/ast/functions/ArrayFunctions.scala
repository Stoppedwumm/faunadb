package fauna.ast

import fauna.auth.Auth
import fauna.lang.syntax._
import fauna.model.{ EventSet, LambdaFilterSet, ReverseSet }
import fauna.model.runtime.Effect
import fauna.repo.query.Query
import scala.annotation.unused

sealed abstract class AbstractMapFunction(name: String) extends QFunction {
  val effect = Effect.Pure

  def apply(
    lambda: LambdaL,
    coll: Literal,
    ec: EvalContext,
    pos: Position): Query[R[Literal]]

  protected def mapValues(
    vs: List[Literal],
    lambda: LambdaL,
    ec: EvalContext,
    pos: Position) =
    vs map { v => ec.evalLambdaApply(lambda, v, pos at name) } sequenceT

  protected def filterValues(
    vs: List[Literal],
    uvs: List[Literal],
    lambda: LambdaL,
    ec: EvalContext,
    pos: Position) = {
    val bools = vs map { ec.evalLambdaApply(lambda, _, pos at name) }
    Parser.sequenceT(bools) flatMapT { boolRs =>
      Query(Casts.ZeroOrMore(Casts.Boolean)(ArrayL(boolRs), pos)) mapT { bools =>
        (vs zip uvs) zip bools collect { case (t, true) => t } unzip
      }
    }
  }
}

object MapFunction extends AbstractMapFunction("map") {
  def apply(
    lambda: LambdaL,
    coll: Literal,
    ec: EvalContext,
    pos: Position): Query[R[Literal]] =
    coll match {
      case ArrayL(vs) => mapValues(vs, lambda, ec, pos) mapT { ArrayL(_) }
      case PageL(vs, uvs, b, a) =>
        mapValues(vs, lambda, ec, pos) mapT { PageL(_, uvs, b, a) }
      case _ =>
        Query(
          Left(
            List(
              InvalidArgument(
                List(Type.AbstractArray),
                coll.rtype,
                pos at "collection"))))
    }
}

object ForeachFunction extends AbstractMapFunction("foreach") {
  def apply(
    lambda: LambdaL,
    coll: Literal,
    ec: EvalContext,
    pos: Position): Query[R[Literal]] =
    coll match {
      case ArrayL(vs)         => mapValues(vs, lambda, ec, pos) mapT { _ => coll }
      case PageL(vs, _, _, _) => mapValues(vs, lambda, ec, pos) mapT { _ => coll }
      case _ =>
        Query(
          Left(
            List(
              InvalidArgument(
                List(Type.AbstractArray),
                coll.rtype,
                pos at "collection"))))
    }
}

object FilterFunction extends AbstractMapFunction("filter") {
  def apply(
    lambda: LambdaL,
    coll: Literal,
    ec: EvalContext,
    pos: Position): Query[R[Literal]] =
    Casts.Iterable(coll, pos at "collection") match {
      case Right(ArrayL(vs)) =>
        filterValues(vs, vs, lambda, ec, pos) mapT { t => ArrayL(t._1) }
      case Right(PageL(vs, uvs, b, a)) =>
        filterValues(vs, uvs, lambda, ec, pos) mapT { t => PageL(t._1, t._2, b, a) }
      case Right(SetL(set)) => filterSet(lambda, set, pos)
      case l @ Left(_)      => Query.value(l)
    }

  def apply(
    lambda: LambdaL,
    set: EventSet,
    @unused auth: Auth,
    pos: Position): Query[R[SetL]] =
    filterSet(lambda, set, pos)

  private def filterSet(lambda: LambdaL, set: EventSet, pos: Position) =
    lambda.callMaxEffect map {
      case Effect.Write =>
        Left(List(InvalidLambdaEffect(Effect.Read, pos at "filter")))
      case _ => Right(SetL(LambdaFilterSet(set, lambda, pos)))
    }
}

object PrependFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    elems: List[Literal],
    coll: ArrayL,
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] =
    Query(Right(ArrayL((elems ++ coll.elems))))
}

object AppendFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    elems: List[Literal],
    coll: ArrayL,
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] =
    Query(Right(ArrayL(coll.elems ++ elems)))
}

object TakeFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    num: Long,
    coll: Literal,
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    val inum = num.toInt
    coll match {
      case ArrayL(vs) => Query(Right(ArrayL(vs take inum)))
      case PageL(vs, uvs, b, a) => {
        val updatedAfter = if (inum < 0) b else uvs lift inum orElse a
        Query(Right(PageL(vs take inum, uvs take inum, b, updatedAfter)))
      }
      case _ =>
        Query(
          Left(
            List(
              InvalidArgument(
                List(Type.AbstractArray),
                coll.rtype,
                pos at "collection"))))
    }
  }
}

object TakeFunction2 extends QFunction {
  val effect = Effect.Read

  def apply(
    num: Long,
    coll: IterableL,
    ec: EvalContext,
    pos: Position): Query[R[IterableL]] = {
    val inum = num.toInt
    coll match {
      case ArrayL(vs) =>
        val elems = if (inum < 0) {
          vs drop (vs.length + inum)
        } else {
          vs take inum
        }

        Query.value(Right(ArrayL(elems)))

      case PageL(vs, uvs, b, a) =>
        if (inum < 0) {
          val len = vs.length + inum
          Query.value(Right(PageL(vs drop len, uvs drop len, b, a)))
        } else {
          val updatedAfter = if (inum < 0) b else uvs lift inum orElse a
          Query.value(Right(PageL(vs take inum, uvs take inum, b, updatedAfter)))
        }

      case SetL(set) if inum.abs <= ReadAdaptor.MaxPageSize =>
        val len = inum.abs min ReadAdaptor.MaxPageSize

        val sset = if (inum < 0) {
          ReverseSet(set)
        } else {
          set
        }

        ReadAdaptor.paginate(
          set = sset,
          cursorOpt = None,
          tsOpt = None,
          sizeOpt = Some(len),
          sourcesOpt = None,
          ascending = true,
          ecRaw = ec,
          pos = pos
        ) mapT { page =>
          if (inum < 0) {
            ArrayL(page.elems.reverse)
          } else {
            ArrayL(page.elems)
          }
        }

      case SetL(_) =>
        Query.value(Left(List(
          BoundsError("Number of elements", s"<= ${ReadAdaptor.MaxPageSize}", pos))))

      case _ =>
        Query.value(
          Left(
            List(
              InvalidArgument(
                List(Type.AbstractIterable),
                coll.rtype,
                pos at "collection"))))
    }
  }
}

object DropFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    num: Long,
    coll: Literal,
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    val inum = num.toInt
    coll match {
      case ArrayL(vs) => Query(Right(ArrayL(vs drop inum)))
      case PageL(vs, uvs, b, a) => {
        val droppedUvs = uvs drop inum
        val updatedBefore = if (inum < 0) b else droppedUvs.headOption orElse a
        Query(Right(PageL(vs drop inum, droppedUvs, updatedBefore, a)))
      }
      case _ =>
        Query(
          Left(
            List(
              InvalidArgument(
                List(Type.AbstractArray),
                coll.rtype,
                pos at "collection"))))
    }
  }
}

object IsEmptyFunction extends QFunction {
  val effect = Effect.Pure

  def apply(coll: IterableL, ec: EvalContext, pos: Position): Query[R[Literal]] =
    coll match {
      case ArrayL(vs) =>
        Query.value(Right(BoolL(vs.isEmpty)))

      case PageL(vs, _, _, _) =>
        Query.value(Right(BoolL(vs.isEmpty)))

      case SetL(set) =>
        ReadAdaptor.exists(ec, set, ec.getValidTime(None), pos) mapT {
          case TrueL  => FalseL
          case FalseL => TrueL
        }
    }
}

object IsNonEmptyFunction extends QFunction {
  val effect = Effect.Pure

  def apply(coll: IterableL, ec: EvalContext, pos: Position): Query[R[Literal]] =
    coll match {
      case ArrayL(vs) =>
        Query.value(Right(BoolL(vs.nonEmpty)))

      case PageL(vs, _, _, _) =>
        Query.value(Right(BoolL(vs.nonEmpty)))

      case SetL(set) =>
        ReadAdaptor.exists(ec, set, ec.getValidTime(None), pos)
    }
}
