package fql.typer

import fql.error.TypeError

/** Typecheck is the Typer's internal representation of an intermediate or final
  * typecheck result. (A final result is one which contains a Value constraint,
  * whereas an intermediate result may contain an arbitrary value).
  *
  * Typecheck is a monad, which allows it to isolate tracking of free variables
  * and errors away from control flow logic in the Typer itself.
  */
case class Typecheck[+A](value: A, failures: List[TypeError]) {

  def isFailed = failures.nonEmpty

  def flatMap[B](f: A => Typecheck[B]): Typecheck[B] = {
    val tc0 = f(value)
    new Typecheck(tc0.value, failures ++ tc0.failures)
  }

  def map[B](f: A => B): Typecheck[B] =
    new Typecheck(f(value), failures)
}

object Typecheck {

  /** Represents a "final" Typecheck result. */
  type Final = Typecheck[Constraint.Value]
  type TypeFinal = Typecheck[Type]

  def apply[A](v: A) =
    new Typecheck(v, Nil)

  def apply[A](v: A, errs: List[TypeError]) =
    new Typecheck(v, errs)

  def unapply[A](tc: Typecheck[A]): Option[(A, List[TypeError])] =
    Some((tc.value, tc.failures))

  def fail(failure: TypeError): Typecheck.TypeFinal =
    new Typecheck(Type.Any, List(failure))

  def lift(failures: Iterable[TypeError]): Typecheck.TypeFinal =
    new Typecheck(Type.Any, failures.toList)

  def sequence[A](tcs: IterableOnce[Typecheck[A]]): Typecheck[List[A]] = {
    val vs = List.newBuilder[A]
    val failures = List.newBuilder[TypeError]

    tcs.iterator.foreach { tc =>
      vs += tc.value
      failures ++= tc.failures
    }

    new Typecheck(vs.result(), failures.result())
  }

  def option[A](tc: Option[Typecheck[A]]): Typecheck[Option[A]] =
    tc match {
      case Some(tc) => new Typecheck(Some(tc.value), tc.failures)
      case None     => new Typecheck(None, Nil)
    }
}
