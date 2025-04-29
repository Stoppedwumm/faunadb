package fauna.ast

import fauna.model.runtime.Effect
import fauna.repo.query.Query
import scala.annotation.{ tailrec, unused }
import scala.math._
import scala.util.Either

sealed trait NumberFolds extends QFunction {

  protected def combine(a: Long, b: Long, pos: Position): R[Long]
  protected def combine(a: Long, b: Double, pos: Position): R[Double]
  protected def combine(a: Double, b: Long, pos: Position): R[Double]
  protected def combine(a: Double, b: Double, pos: Position): R[Double]

  @tailrec
  private def fold(
    seed: Long,
    pos: Position,
    elems: List[Either[Double, Long]]): R[Literal] =
    elems match {
      case Nil              => Right(LongL(seed))
      case Right(l) :: rest =>
        // Either.flatMap inlined for tail recursion
        combine(seed, l, pos) match {
          case Right(l1) => fold(l1, pos, rest)
          case e @ _     => e.asInstanceOf[R[Literal]]
        }
      case Left(d) :: rest =>
        combine(seed, d, pos).flatMap { fold(_, pos, rest) }
    }

  @tailrec
  private def fold(
    seed: Double,
    pos: Position,
    elems: List[Either[Double, Long]]): R[Literal] =
    elems match {
      case Nil              => Right(DoubleL(seed))
      case Right(l) :: rest =>
        // Either.flatMap inlined for tail recursion
        combine(seed, l, pos) match {
          case Right(l1) => fold(l1, pos, rest)
          case e @ _     => e.asInstanceOf[R[Literal]]
        }
      case Left(d) :: rest =>
        // Either.flatMap inlined for tail recursion
        combine(seed, d, pos) match {
          case Right(d1) => fold(d1, pos, rest)
          case e @ _     => e.asInstanceOf[R[Literal]]
        }
    }

  def apply(
    args: List[Either[Double, Long]],
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] =
    Query(args.head.fold(fold(_, pos, args.tail), fold(_, pos, args.tail)))
}

object AbsFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    num: Either[Double, Long],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    Query(num match {
      case Right(l) => Right(LongL(abs(l)))
      case Left(d)  => Right(DoubleL(abs(d)))
    })
  }
}

object AddFunction extends NumberFolds {
  val effect = Effect.Pure

  protected def combine(a: Long, b: Long, pos: Position) = Right(a + b)
  protected def combine(a: Long, b: Double, pos: Position) = Right(a + b)
  protected def combine(a: Double, b: Long, pos: Position) = Right(a + b)
  protected def combine(a: Double, b: Double, pos: Position) = Right(a + b)
}

object BitAndFunction extends NumberFolds {
  val effect = Effect.Pure

  protected def combine(a: Long, b: Long, pos: Position) = Right(a & b)
  protected def combine(a: Long, b: Double, pos: Position) = Right(
    (a & b.toLong).toDouble)
  protected def combine(a: Double, b: Long, pos: Position) = Right(
    (a.toLong & b).toDouble)
  protected def combine(a: Double, b: Double, pos: Position) =
    Right((a.toLong & b.toLong).toDouble)
}

object BitNotFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    num: Either[Double, Long],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    Query(num match {
      case Right(l) => Right(LongL(~l))
      case Left(d)  => Right(LongL(~(d.toLong)))
    })
  }
}

object BitOrFunction extends NumberFolds {
  val effect = Effect.Pure

  protected def combine(a: Long, b: Long, pos: Position) = Right(a | b)
  protected def combine(a: Long, b: Double, pos: Position) = Right(
    (a | b.toLong).toDouble)
  protected def combine(a: Double, b: Long, pos: Position) = Right(
    (a.toLong | b).toDouble)
  protected def combine(a: Double, b: Double, pos: Position) =
    Right((a.toLong | b.toLong).toDouble)
}

object BitXorFunction extends NumberFolds {
  val effect = Effect.Pure

  protected def combine(a: Long, b: Long, pos: Position) = Right(a ^ b)
  protected def combine(a: Long, b: Double, pos: Position) = Right(
    (a ^ b.toLong).toDouble)
  protected def combine(a: Double, b: Long, pos: Position) = Right(
    (a.toLong ^ b).toDouble)
  protected def combine(a: Double, b: Double, pos: Position) =
    Right((a.toLong ^ b.toLong).toDouble)
}

object CeilFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    num: Either[Double, Long],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    Query(num match {
      case Right(l) => Right(LongL(l))
      case Left(d)  => Right(DoubleL(ceil(d)))
    })
  }
}

trait DivideLikeFunction extends NumberFolds {
  val effect = Effect.Pure

  private def divZero(pos: Position) = Left(List(DivideByZero(pos)))

  protected def combine(a: Long, b: Long, pos: Position) =
    if (b == 0) divZero(pos) else Right(divFun(a, b))
  protected def combine(a: Long, b: Double, pos: Position) =
    if (b == 0) divZero(pos) else Right(divFun(a, b))
  protected def combine(a: Double, b: Long, pos: Position) =
    if (b == 0) divZero(pos) else Right(divFun(a, b))
  protected def combine(a: Double, b: Double, pos: Position) =
    if (b == 0) divZero(pos) else Right(divFun(a, b))

  protected def divFun(a: Long, b: Long): Long
  protected def divFun(a: Long, b: Double): Double
  protected def divFun(a: Double, b: Long): Double
  protected def divFun(a: Double, b: Double): Double
}

object DivideFunction extends DivideLikeFunction {
  protected def divFun(a: Long, b: Long): Long = a / b
  protected def divFun(a: Long, b: Double): Double = a / b
  protected def divFun(a: Double, b: Long): Double = a / b
  protected def divFun(a: Double, b: Double): Double = a / b
}

object FloorFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    num: Either[Double, Long],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    Query(num match {
      case Right(l) => Right(LongL(l))
      case Left(d)  => Right(DoubleL(floor(d)))
    })
  }
}

object ModuloFunction extends DivideLikeFunction {
  protected def divFun(a: Long, b: Long): Long = a % b
  protected def divFun(a: Long, b: Double): Double = a % b
  protected def divFun(a: Double, b: Long): Double = a % b
  protected def divFun(a: Double, b: Double): Double = a % b
}

object MultiplyFunction extends NumberFolds {
  val effect = Effect.Pure

  protected def combine(a: Long, b: Long, pos: Position) = Right(a * b)
  protected def combine(a: Long, b: Double, pos: Position) = Right(a * b)
  protected def combine(a: Double, b: Long, pos: Position) = Right(a * b)
  protected def combine(a: Double, b: Double, pos: Position) = Right(a * b)
}

object RoundFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    num: Either[Double, Long],
    dp: Option[Long],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    Query(
      Right(
        (num, dp) match {
          case (Left(d), Some(p))  => DoubleL(roundOff(d, p))
          case (Right(l), Some(p)) => LongL(roundOff(l, p))
          case (Left(d), None)     => DoubleL(roundOff(d, 2))
          case (Right(l), None)    => LongL(l)
        }
      ))
  }

  def roundOff(num: Long, dp: Long): Long = {
    if (dp > 0) {
      // No fractional part to round
      num
    } else {
      val tmp = pow(10, dp.toDouble)
      (((num * tmp) + 0.5).toLong / tmp).toLong
    }
  }

  /* We must always compute the fractional part as rounnd can cause us to carry */
  def roundOff(num: Double, dp: Long): Double = {
    if (dp > 0) {
      // Avoid overflows, break the number appart
      val tmp = pow(10, dp.toDouble).toInt
      val wnum = num.toLong
      // db >=0 adjust the fraction part
      val fract = ((num - wnum + 0.5 / tmp) * tmp).toLong
      // Put the 2 parts back together
      wnum + fract.toDouble / tmp.toDouble
    } else {
      val tmp = pow(10, dp.toDouble)
      (((num * tmp) + 0.5).toLong / tmp).toLong.toDouble
    }
  }
}

object SignFunction extends QFunction {
  val effect = Effect.Pure
  /* For doubles infinite is comparable Double.NegativeInfinity < -1
   * Double.PositiveInfinity > 1 */
  def apply(
    num: Either[Double, Long],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    Query.value((num: @unchecked) match {
      case Right(l) if (l == 0)           => Right(LongL(0))
      case Right(l) if (l > 0)            => Right(LongL(1))
      case Right(l) if (l < 0)            => Right(LongL(-1))
      case Left(d) if (d < 0)             => Right(LongL(-1))
      case Left(d) if (d >= 0 || d.isNaN) => Right(LongL(1))
    })
  }
}

object SubtractFunction extends NumberFolds {
  val effect = Effect.Pure

  protected def combine(a: Long, b: Long, pos: Position) = Right(a - b)
  protected def combine(a: Long, b: Double, pos: Position) = Right(a - b)
  protected def combine(a: Double, b: Long, pos: Position) = Right(a - b)
  protected def combine(a: Double, b: Double, pos: Position) = Right(a - b)
}

object SqrtFunction extends QFunction {
  val effect = Effect.Pure
  private[this] val msg = "positive"

  def apply(
    num: Either[Double, Long],
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    Query(num match {
      case Right(l) if (l >= 0) => Right(DoubleL(sqrt(l.toDouble)))
      case Left(d) if (d >= 0)  => Right(DoubleL(sqrt(d)))
      case Right(l)             => Left(List(BoundsError(l.toString(), msg, pos)))
      case Left(d)              => Left(List(BoundsError(d.toString(), msg, pos)))
    })
  }
}

object TruncFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    num: Either[Double, Long],
    dp: Option[Long],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    Query(Right((num, dp) match {
      case (Left(d), Some(p))  => DoubleL(truncDouble(d, p))
      case (Right(l), Some(p)) => LongL(trunc(l, p))
      case (Left(d), None)     => DoubleL(truncDouble(d, 2))
      case (Right(l), None)    => LongL(l)
    }))
  }

  /* dp: if dp > 0 THEN the number to the right of the decimal places to remove dp:
   * if dp < 0 THEN the number to the left of the decimal places to replace with
   * zeros */
  def trunc(num: Long, dp: Long): Long = {
    if (dp > 0) {
      // No decimal to deal with
      num
    } else {
      val tmp = pow(10, dp.toDouble)
      ((num * tmp).toLong / tmp).toLong
    }
  }

  def truncDouble(num: Double, dp: Long): Double = {
    if (dp > 0) {
      // Avoid overflows, break the number appart
      val tmp = pow(10, dp.toDouble).toInt
      val wnum = num.toLong
      // db >=0 adjust the fraction part
      val fract = ((num - wnum) * tmp).toLong
      // Put the 2 parts back together
      wnum + fract.toDouble / tmp.toDouble
    } else {
      val tmp = pow(10, dp.toDouble)
      (num * tmp).toLong / tmp
    }
  }

  def truncLong(num: Double, dp: Long): Long = {
    if (dp > 0) {
      // We are only returning a long, the fractional part can be ignored
      num.toLong
    } else {
      val tmp = pow(10, dp.toDouble)
      ((num * tmp).toLong / tmp).toLong
    }
  }
}
