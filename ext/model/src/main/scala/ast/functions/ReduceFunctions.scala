package fauna.ast

import fauna.codex.cbor.{ CBOR, CBOROrdering }
import fauna.lang.syntax._
import fauna.model.runtime.Effect
import fauna.repo.query.Query

case class ReduceFunction(ec: EvalContext, lambda: LambdaL)
    extends AbstractFoldFunction[Literal] {

  val functionName = "reduce"
  val collectionName = "collection"

  def apply(
    initial: Literal,
    collection: Literal,
    pos: Position): Query[R[Literal]] =
    fold(initial, collection, ec, pos)

  def toLiteral(value: Literal, scanned: Long, pos: Position): Query[R[Literal]] =
    Query.value(Right(value))

  def accumulate(
    initial: Literal,
    elems: Iterable[Literal],
    pos: Position): Query[R[Literal]] = {
    val seed: Query[R[Literal]] = Query(Right(initial))

    elems.foldLeft(seed) { (accQ, value) =>
      accQ flatMapT { acc =>
        ec.evalLambdaApply(lambda, ArrayL(List(acc, value)), pos at functionName)
      }
    } map {
      case Right(value) => Right(value)
      case Left(errs)   => throw EvalErrorException(errs)
    }
  }
}

object ReduceFunction extends QFunction {
  val effect = Effect.Read

  def apply(
    lambda: LambdaL,
    initial: Expression,
    collection: Expression,
    ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    (ec.eval(initial), ec.eval(collection)) parT { (ini, coll) =>
      val reduce = ReduceFunction(ec, lambda)

      reduce(ini, coll, pos)
    }
  }
}

object CountFunction extends AbstractFoldFunction[Literal] {
  val functionName = "count"
  val collectionName = "count"

  def apply(
    collection: IterableL,
    ec: EvalContext,
    pos: Position): Query[R[Literal]] =
    fold(NullL, collection, ec, pos)

  def toLiteral(value: Literal, scanned: Long, pos: Position): Query[R[Literal]] =
    Query.addCompute(scanned.toInt) map { _ =>
      Right(LongL(scanned))
    }

  def accumulate(
    initial: Literal,
    elems: Iterable[Literal],
    pos: Position): Query[R[Literal]] = {
    Query.value(Right(NullL))
  }
}

abstract class AbstractSumFunction(name: String)
    extends AbstractFoldFunction[NumericL] {
  val functionName = name
  val collectionName = name

  def apply(
    collection: IterableL,
    ec: EvalContext,
    pos: Position): Query[R[Literal]] =
    fold(LongL(0), collection, ec, pos)

  def accumulate(
    initial: NumericL,
    elems: Iterable[Literal],
    pos: Position): Query[R[NumericL]] = {

    var acc: R[NumericL] = Right(initial)
    var cost = 0
    val iter = elems.iterator

    while (iter.hasNext && acc.isRight) {
      acc = Casts.Number(iter.next(), pos at functionName).map {
        cost += 1
        add(acc.toOption.get, _)
      }
    }

    Query.addCompute(cost) map { _ => acc }
  }

  private def add(a: NumericL, b: Either[Double, Long]): NumericL = (a, b) match {
    case (LongL(a), Right(b))   => LongL(a + b)
    case (LongL(a), Left(b))    => DoubleL(a + b)
    case (DoubleL(a), Right(b)) => DoubleL(a + b)
    case (DoubleL(a), Left(b))  => DoubleL(a + b)
  }
}

object MeanFunction extends AbstractSumFunction("mean") {

  def toLiteral(value: NumericL, scanned: Long, pos: Position): Query[R[Literal]] =
    value match {
      case LongL(acc) if scanned > 0 =>
        Query.value(Right(DoubleL(acc.toDouble / scanned)))
      case DoubleL(acc) if scanned > 0 =>
        Query.value(Right(DoubleL(acc / scanned)))
      case _ => Query.value(Left(List(EmptyArrayArgument(pos at functionName))))
    }
}

object SumFunction extends AbstractSumFunction("sum") {

  def toLiteral(value: NumericL, scanned: Long, pos: Position): Query[R[Literal]] =
    Query.value(Right(value))
}

abstract class AbstractBooleanFunction(
  name: String,
  op: (Boolean, Boolean) => Boolean)
    extends AbstractFoldFunction[BoolL] {

  val functionName = name
  val collectionName = name

  def toLiteral(value: BoolL, scanned: Long, pos: Position): Query[R[Literal]] =
    Query.value(Right(value))

  def accumulate(
    initial: BoolL,
    elems: Iterable[Literal],
    pos: Position): Query[R[BoolL]] = {

    var acc: R[BoolL] = Right(initial)
    var cost = 0
    val iter = elems.iterator

    while (iter.hasNext && acc.isRight) {
      acc = Casts.Boolean(iter.next(), pos at functionName).map { value =>
        cost += 1
        BoolL(op(acc.toOption.get.value, value))
      }
    }

    Query.addCompute(cost) map { _ => acc }
  }
}

object AnyFunction extends AbstractBooleanFunction("any", _ || _) {

  def apply(
    collection: IterableL,
    ec: EvalContext,
    pos: Position): Query[R[Literal]] =
    fold(FalseL, collection, ec, pos)
}

object AllFunction extends AbstractBooleanFunction("all", _ && _) {

  def apply(
    collection: IterableL,
    ec: EvalContext,
    pos: Position): Query[R[Literal]] =
    fold(TrueL, collection, ec, pos)
}

abstract class AbstractCBORFunction(name: String, op: Int => Boolean)
    extends AbstractFoldFunction[Literal] {

  val functionName = name

  val collectionName = name

  def apply(coll: IterableL, ec: EvalContext, pos: Position): Query[R[Literal]] = {
    coll match {
      case ArrayL(List(x: IterableL)) =>
        apply(x, ec, pos)

      case _ =>
        fold(null, coll, ec, pos)
    }
  }

  def toLiteral(value: Literal, scanned: Long, pos: Position): Query[R[Literal]] =
    if (value == null) {
      Query.value(Left(List(EmptyArrayArgument(pos at functionName))))
    } else {
      Query.value(Right(value))
    }

  def accumulate(
    initial: Literal,
    elems: Iterable[Literal],
    pos: Position): Query[R[Literal]] = {

    val errors = elems collect { case UnresolvedRefL(ref) =>
      UnresolvedRefError(ref, pos at name)
    }

    if (errors.nonEmpty) {
      Query.value(Left(errors.toList))
    } else {
      val reduced = elems.foldLeft(initial) { (acc, value) =>
        if (acc == null) {
          value
        } else {
          val cmp = CBOROrdering.compare(
            CBOR.encode(Literal.toIndexTerm(acc)),
            CBOR.encode(Literal.toIndexTerm(value)))
          if (op(cmp)) acc else value
        }
      }

      Query.value(Right(reduced))
    }
  }
}

object MaxFunction extends AbstractCBORFunction("max", _ > 0)
object MinFunction extends AbstractCBORFunction("min", _ < 0)
