package fauna.storage.doc

import fauna.lang._
import fauna.lang.syntax._
import scala.annotation.unused

/**
 * Validators are composable, reusable logic that verifies the
 * correctness of a proposed modification to a version. They:
 *
 *   - filter input to only allowed values
 *   - normalize and verify correctness of allowed values
 *
 * The API should be flexible enough to write validators that modify
 * their input, and check arbitrary pre and post state of the
 * modification.
 */
object Validator {
  def collectErrors(es: Either[List[ValidationException], _]*): Either[List[ValidationException], Nothing] =
    Left((es collect { case Left(es) => es }).flatten.toList)
}

abstract class Validator[M[+_]](implicit M: Monad[M]) {

  protected def filterMask: MaskTree

  private[doc] def _filterMask = filterMask

  /**
   * Validate a version.
   */
  def validate(data: Data): M[List[ValidationException]] =
    M.map(patch(data, Diff.empty)) { _.fold(identity, _ => Nil) }

  /**
   * Internal data validation method. Usually validators override
   * either this or validatePatch().
   */
  protected def validateData(@unused data: Data): M[List[ValidationException]] =
    M.pure(Nil)

  private[doc] def _validateData(data: Data): M[List[ValidationException]] =
    validateData(data)

  /**
   * Internal patch validation/revision method. Usually validators
   * override either this or validateData().
   *
   * Validators that override validatePatch() modify input MUST
   * conform to the IR patch logic: They must allow fields that may
   * be cleared to be so via a NullV. E.g.: A password validator is
   * responsible for shadowing an ephemeral `password` field with a
   * persisted `hashed_password` field. If `password` is updated,
   * `hashed_password` is as well via a transformation. If `password`
   * is removed via NullV, then `hashed_password` should be as well.
   */
  protected def validatePatch(@unused current: Data, diff: Diff): M[Either[List[ValidationException], Diff]] =
    M.pure(Right(diff))

  private[doc] def _validatePatch(current: Data, diff: Diff): M[Either[List[ValidationException], Diff]] =
    validatePatch(current, diff)

  /**
   * Select only fields specified by the filterMask
   */
  def select(data: Data) = Data(filterMask.select(data.fields))

  def selectDiff(diff: Diff) = Diff(filterMask.select(diff.fields))

  /**
   * Patch, validate, and filter a version.
   */
  def patch(data: Data, diff: Diff): M[Either[List[ValidationException], Data]] =
    M.flatMap(validatePatch(data, diff)) { pv =>
      val (patchErrs, pending) = pv match {
        case Right(diff) => (Nil, select(data patch diff))
        case Left(errs)  => (errs, select(data patch diff))
      }

      M.map(validateData(pending)) { dataErrs =>
        if (patchErrs.isEmpty && dataErrs.isEmpty) {
          Right(pending)
        } else {
          Left((patchErrs ++ dataErrs).distinct)
        }
      }
    }

  def append(rhs: Validator[M]): Validator[M] = (this, rhs) match {
    case (AggregateValidator(lhs), AggregateValidator(rhs)) => AggregateValidator(lhs ++ rhs)
    case (lhs, AggregateValidator(rhs))                     => AggregateValidator(lhs +: rhs)
    case (AggregateValidator(lhs), rhs)                     => AggregateValidator(lhs :+ rhs)
    case (lhs, rhs)                                         => AggregateValidator(List(lhs, rhs))
  }

  def +(rhs: Validator[M]): Validator[M] = this append rhs
}

case class AggregateValidator[M[+_]](subfilters: List[Validator[M]])(implicit M: Monad[M]) extends Validator[M] {
  protected lazy val filterMask = subfilters map { _._filterMask } reduceLeft { _ merge _ }

  // iterates over all subfilters, returning a cumulative revised
  //patch, or else a list of all patch-related errors
  override protected def validatePatch(current: Data, diff: Diff) =
    validate0(current, diff, subfilters, Right(diff))

  private def validate0(
    data: Data,
    diff: Diff,
    rest: List[Validator[M]],
    acc: Either[List[ValidationException], Diff]): M[Either[List[ValidationException], Diff]] =
    rest match {
      case Nil => M.pure(acc)
      case v :: rest =>
        val accQ = acc match {
          case Right(diff) => v._validatePatch(data, diff)
          case Left(errs)  =>
            M.map(v._validatePatch(data, diff)) { e =>
              Left(e.fold(errs ++ _, _ => errs))
            }
        }

        M.flatMap(accQ) { validate0(data, diff, rest, _) }
    }

  override protected def validateData(data: Data) =
    M.map((subfilters map { _._validateData(data) }).sequence) { _.flatten.toList }
}
