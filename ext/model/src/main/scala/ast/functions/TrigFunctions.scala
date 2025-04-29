package fauna.ast

import fauna.model.runtime.Effect
import fauna.repo.query.Query
import scala.annotation.unused
import scala.math._

object ACosFunction extends QFunction {
  val effect = Effect.Pure
  val msg = "between -1 and 1 inclusive"

  def apply(
    num: Either[Double, Long],
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    Query(num match {
      case Right(l) if (l >= -1 && l <= 1) => Right(DoubleL(acos(l.toDouble)))
      case Left(d) if (d >= -1 && d <= 1)  => Right(DoubleL(acos(d)))
      case Right(l) => Left(List(BoundsError(l.toString(), msg, pos)))
      case Left(d)  => Left(List(BoundsError(d.toString(), msg, pos)))
    })
  }
}

object ASinFunction extends QFunction {
  val effect = Effect.Pure
  val msg = "between -1 and 1 inclusive"

  def apply(
    num: Either[Double, Long],
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    Query(num match {
      case Right(l) if (l >= -1 && l <= 1) => Right(DoubleL(asin(l.toDouble)))
      case Left(d) if (d >= -1 && d <= 1)  => Right(DoubleL(asin(d)))
      case Right(l) => Left(List(BoundsError(l.toString(), msg, pos)))
      case Left(d)  => Left(List(BoundsError(d.toString(), msg, pos)))
    })
  }
}

object ATanFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    num: Either[Double, Long],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    Query(num match {
      case Right(l) => Right(DoubleL(atan(l.toDouble)))
      case Left(d)  => Right(DoubleL(atan(d)))
    })
  }
}

object CosFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    num: Either[Double, Long],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    Query(num match {
      case Right(l) => Right(DoubleL(cos(l.toDouble)))
      case Left(d)  => Right(DoubleL(cos(d)))
    })
  }
}

object CoshFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    num: Either[Double, Long],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    Query(num match {
      case Right(l) => Right(DoubleL(cosh(l.toDouble)))
      case Left(d)  => Right(DoubleL(cosh(d)))
    })
  }
}

object DegreesFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    num: Either[Double, Long],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    Query(num match {
      case Right(l) => Right(DoubleL(toDegrees(l.toDouble)))
      case Left(d)  => Right(DoubleL(toDegrees(d)))
    })
  }
}

object ExpFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    num: Either[Double, Long],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    Query(num match {
      case Right(l) => Right(DoubleL(exp(l.toDouble)))
      case Left(d)  => Right(DoubleL(exp(d)))
    })
  }
}

object HypotFunction extends QFunction {
  val effect = Effect.Pure
  val msg = "greater than 0"

  def apply(
    a: Either[Double, Long],
    b: Option[Either[Double, Long]],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    val s2 = b.getOrElse(a)

    Query(Right(DoubleL((a, s2) match {
      case (Left(d1), Left(d2))   => hypot(d1, d2)
      case (Left(d1), Right(l2))  => hypot(d1, l2.toDouble)
      case (Right(l1), Left(d2))  => hypot(l1.toDouble, d2)
      case (Right(l1), Right(l2)) => hypot(l1.toDouble, l2.toDouble)
    })))
  }
}

object LnFunction extends QFunction {
  val effect = Effect.Pure
  val msg = "greater than 0"

  def apply(
    num: Either[Double, Long],
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    Query(num match {
      case Right(l) if (l > 0) => Right(DoubleL(log(l.toDouble)))
      case Left(d) if (d > 0)  => Right(DoubleL(log(d)))
      case Right(l)            => Left(List(BoundsError(l.toString(), msg, pos)))
      case Left(d)             => Left(List(BoundsError(d.toString(), msg, pos)))
    })
  }
}

object LogFunction extends QFunction {
  val effect = Effect.Pure
  private[this] val msg = "greater than 0"

  def apply(
    num: Either[Double, Long],
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    Query(num match {
      case Right(l) if (l > 0) => Right(DoubleL(log10(l.toDouble)))
      case Left(d) if (d > 0)  => Right(DoubleL(log10(d)))
      case Right(l)            => Left(List(BoundsError(l.toString(), msg, pos)))
      case Left(d)             => Left(List(BoundsError(d.toString(), msg, pos)))
    })
  }
}

object PowFunction extends QFunction {
  val effect = Effect.Pure
  /* Default to expon of 2 */

  def apply(
    num: Either[Double, Long],
    exp: Option[Either[Double, Long]],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    val e = exp match {
      case Some(Left(d))  => d
      case Some(Right(l)) => l.toDouble
      case _              => 2.0
    }

    Query(num match {
      case Left(d)  => Right(DoubleL(pow(d, e)))
      case Right(l) => Right(DoubleL(pow(l.toDouble, e)))
    })
  }
}

object RadiansFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    num: Either[Double, Long],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    Query(num match {
      case Right(l) => Right(DoubleL(toRadians(l.toDouble)))
      case Left(d)  => Right(DoubleL(toRadians(d)))
    })
  }
}

object SinFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    num: Either[Double, Long],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    Query(num match {
      case Right(l) => Right(DoubleL(sin(l.toDouble)))
      case Left(d)  => Right(DoubleL(sin(d)))
    })
  }
}

object SinhFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    num: Either[Double, Long],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    Query(num match {
      case Right(l) => Right(DoubleL(sinh(l.toDouble)))
      case Left(d)  => Right(DoubleL(sinh(d)))
    })
  }
}

object TanFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    num: Either[Double, Long],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    Query(num match {
      case Right(l) => Right(DoubleL(tan(l.toDouble)))
      case Left(d)  => Right(DoubleL(tan(d)))
    })
  }
}

object TanhFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    num: Either[Double, Long],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    Query(num match {
      case Right(l) => Right(DoubleL(tanh(l.toDouble)))
      case Left(d)  => Right(DoubleL(tanh(d)))
    })
  }
}
