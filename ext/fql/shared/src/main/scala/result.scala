package fql

import fql.error.Error
import fql.result.macros._
import scala.language.experimental.macros
import scala.util.control._

/** This module duplicates the API in ext/lang/Result. We cannot reuse that
  * dependency here, however in order to be able to target JS.
  */
sealed trait Result[+A] {
  import Result.{ Err, Ok }
  private type E = List[Error]

  def isOk: Boolean = this.isInstanceOf[Ok[_]]
  def isErr = !isOk

  def toOption: Option[A] =
    this match {
      case Ok(v) => Some(v)
      case _     => None
    }
  def errOption: Option[E] =
    this match {
      case Err(es) => Some(es)
      case _       => None
    }

  def fold[B](f1: E => B, f2: A => B): B =
    this match {
      case Ok(v)   => f2(v)
      case Err(es) => f1(es)
    }

  def getOr[A1 >: A](withErr: E => A1): A1 = fold(withErr, identity)

  def errOr[E1 >: E](withOk: A => E1): E1 = fold(identity, withOk)

  def getOrElse[A1 >: A](alt: => A1): A1 = fold(_ => alt, identity)

  def errOrElse[E1 >: E](alt: => E1): E1 = fold(identity, _ => alt)

  def map[B](f: A => B): Result[B] =
    this match {
      case Ok(v)  => Ok(f(v))
      case e: Err => e.asInstanceOf[Result[B]]
    }

  def mapErr(f: E => E): Result[A] =
    this match {
      case Err(es) => Err(f(es))
      case _       => this
    }

  def flatMap[B](f: A => Result[B]): Result[B] =
    this match {
      case Ok(v)  => f(v)
      case e: Err => e.asInstanceOf[Result[B]]
    }

  /** Short-circuit accessor to be called within Result.guard or guardM */
  @annotation.compileTimeOnly(
    "Must be used within Result.guard or Result.guardM block")
  def getOrFail: A = sys.error("compile time only!")
}

object Result {

  final case class Ok[+A](value: A) extends Result[A]
  final case class Err(errs: List[Error]) extends Result[Nothing]
  object Err {
    def apply(err: Error): Result.Err = new Err(List(err))
  }

  // FIXME: extend ControlThrowable, but check NonFatal usage in monads.
  final case class ThrownFailure private (errs: List[Error]) extends NoStackTrace {
    override def getMessage = errs.mkString(", ")
  }

  object ThrownFailure {
    private def apply(errs: List[Error]): ThrownFailure = new ThrownFailure(errs)
    // This name is chosen in order to clearly discourage manual use.
    def _unsafeApply(errs: List[Error]): ThrownFailure = apply(errs)
  }

  def guard[A](body: => Result[A]): Result[A] = macro ResultMacroImpl.guard

  /** Short-circuit failed return to be called within Result.guard or guardM */
  @annotation.compileTimeOnly(
    "Must be used within Result.guard or Result.guardM block")
  def fail(err: Error): Nothing =
    sys.error("compile time only!")

  @annotation.compileTimeOnly(
    "Must be used within Result.guard or Result.guardM block")
  def fail(err: List[Error]): Nothing =
    sys.error("compile time only!")

  /** Short-circuit failed return. This _must_ be called within a guard block, but
    * is not checked like fail().
    */
  def unsafeFail(err: Error): Nothing =
    throw ThrownFailure._unsafeApply(List(err))
  def unsafeFail(errs: List[Error]): Nothing =
    throw ThrownFailure._unsafeApply(errs)
}
