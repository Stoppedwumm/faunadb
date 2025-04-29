package fauna.ast

import fauna.atoms._
import fauna.codex.json._
import fauna.lang.Timestamp
import fauna.model.RefParser.RefScope
import fauna.model.{ EventSet, RenderError }
import fauna.model.runtime.Effect
import fauna.storage.doc.ValidationException
import fauna.repo.schema.ConstraintFailure

object Error {

  def statusCode(errs: List[Error]) =
    (errs maxBy {
      case _: ParseError           => 3
      case _: InvalidArgumentError => 2
      case _: PermissionDenied     => 1
      case _                       => 0
    }) match {
      case _: SchemaNotFound   => 404
      case _: ValueNotFound    => 404
      case _: InstanceNotFound => 404
      case _: PermissionDenied => 403
      case _                   => 400
    }

  /**
    * toJSValue encodes an Error as a JSON value. It mimics how an Error
    * is encoded in an HTTP response body, so Error values can be included
    * in the query log.
    */
  def toJSValue(version: APIVersion, err: Error): JSValue = {
    val ob = JSObject.newBuilder

    ob += "code" -> err.code
    ob += "description" -> JSString(RenderError.encodeErrors(err, version))
    ob += "position" -> err.position.toElems.map {
      _.fold(JSLong(_), JSString(_))
    }

    if (err.validationFailures.nonEmpty) {
      ob += "failures" -> JSArray(err.validationFailures map { vf =>
        JSObject(
          "field" -> vf.path.map { JSString(_) },
          "code" -> JSString(vf.code),
          "description" -> JSString(RenderError.encodeErrors(vf, version)))
      })
    }

    if (err.cause.nonEmpty) {
      ob += "cause" -> err.cause.map { toJSValue(version, _) }
    }

    ob.result()
  }
}

/*
 * Send the default version of the error to the exception log
 */
case class EvalErrorException(error: List[EvalError])
    extends Exception(error.headOption map { RenderError.encodeErrors(_, APIVersion.Default)  } getOrElse null)

sealed trait Error {
  def code: String
  def position: Position
  def validationFailures: List[ValidationException] = Nil
  def cause: List[Error] = Nil
}

// Parse Errors

sealed trait ParseError extends Error {
  def code = "invalid expression"
}

case object QueryNotFound extends Error {
  def code = "invalid expression"
  def position = RootPosition
}

case class InvalidJSON(msg: String) extends Error {
  def code = "invalid expression"
  def position = RootPosition
}

case class InvalidURLParam(name: String, description: String) extends Error {
  def code: String = "invalid url parameter"
  def position: Position = RootPosition
}

case class InvalidExprType(expected: List[Type], provided: Type, position: Position)
    extends ParseError

case class InvalidObjectExpr(position: Position) extends ParseError
case class InvalidRefExpr(position: Position) extends ParseError
case class InvalidSetExpr(position: Position) extends ParseError
case class InvalidTimeExpr(position: Position) extends ParseError
case class InvalidDateExpr(position: Position) extends ParseError
case class InvalidBytesExpr(position: Position) extends ParseError
case class InvalidUUIDExpr(position: Position) extends ParseError
case class InvalidFormExpr(keys: List[String], position: Position)
    extends ParseError
case class InvalidLambdaExpr(position: Position) extends ParseError
case class EmptyExpr(position: Position) extends ParseError
case class UnboundVariable(name: String, position: Position) extends ParseError

// NB. Only used by legacy endpoints
case class SchemaNotFound(name: Option[String], collectionID: CollectionID, position: Position)
    extends ParseError {
  override def code = "schema not found"
}

// used to wrap errors when constructing @set values.
case class ParseEvalError(err: EvalError) extends ParseError {
  def position = err.position
  val wrappedError: Error = err
}

// Eval Errors

sealed trait EvalError extends Error

case class TransactionAbort(description: String, position: Position)
    extends EvalError {
  def code = "transaction aborted"
}

case class InvalidEffect(limit: Effect.Limit, eff: Effect, position: Position)
    extends EvalError {
  def code = "invalid expression"
}

case class InvalidWriteTime(position: Position) extends EvalError {
  def code = "invalid write time"
}

sealed trait InvalidArgumentError extends EvalError {
  def code = "invalid argument"
}

case class UnresolvedRefError(orig: RefScope.Ref, position: Position)
    extends EvalError {
  def code = "invalid ref"
}

case class InvalidNativeClassRefError(name: String, position: Position)
    extends EvalError {
  def code = "invalid ref"
}

case class MissingIdentityError(position: Position) extends EvalError {
  def code = "missing identity"
}

case class InvalidJWTScopeError(position: Position) extends EvalError {
  def code = "invalid scope"
}

case class InvalidTokenError(position: Position) extends EvalError {
  def code = "invalid token"
}

case class EmptyArrayArgument(position: Position) extends InvalidArgumentError
case class EmptySetArgument(position: Position) extends InvalidArgumentError
case class InvalidArgument(expected: List[Type], provided: Type, position: Position)
    extends InvalidArgumentError
case class InvalidLambdaArity(expected: Int, provided: Int, position: Position)
    extends InvalidArgumentError
case class InvalidLambdaEffect(maxEffect: Effect, position: Position)
    extends InvalidArgumentError
case class BoundsError(argument: String, predicate: String, position: Position)
    extends InvalidArgumentError
case class InvalidRegex(argument: String, position: Position)
    extends InvalidArgumentError
case class InvalidMatchTermExpr(provided: Type, position: Position)
    extends ParseError
case class InvalidScopedRef(position: Position) extends InvalidArgumentError
case class InvalidMatchTermArgument(provided: Type, position: Position)
    extends InvalidArgumentError
case class InvalidSetArgument(right: EventSet, left: EventSet, position: Position)
    extends InvalidArgumentError
case class InvalidHistoricalSet(set: EventSet, position: Position)
    extends InvalidArgumentError
case class InvalidCursorObject(position: Position) extends InvalidArgumentError
case class InvalidEventCursorForm(keys: List[String], position: Position)
    extends InvalidArgumentError
case class InvalidDocAction(provided: Type, position: Position)
    extends InvalidArgumentError
case class InvalidSetAction(provided: Type, position: Position)
    extends InvalidArgumentError
case class InvalidNormalizer(provided: Type, position: Position)
    extends InvalidArgumentError
case class InvalidCreateClassArgument(provided: CollectionID, position: Position)
    extends InvalidArgumentError
case class InvalidUpdateRefArgument(provided: DocID, position: Position)
    extends InvalidArgumentError
case class InvalidDeleteRefArgument(provided: DocID, position: Position)
    extends InvalidArgumentError
case class InvalidInsertRefArgument(position: Position)
    extends InvalidArgumentError
case class InvalidRemoveRefArgument(position: Position)
    extends InvalidArgumentError
case class InvalidValidReadTime(ts: Timestamp, position: Position)
    extends InvalidArgumentError
case class InvalidTimeArgument(str: String, position: Position)
    extends InvalidArgumentError
case class InvalidTimeUnit(unit: String, position: Position)
    extends InvalidArgumentError
case class InvalidDateArgument(str: String, position: Position)
    extends InvalidArgumentError
case class InvalidSchemaClassArgument(provided: CollectionID, position: Position)
    extends InvalidArgumentError
case class NonNumericSubIDArgument(idStr: String, position: Position)
    extends InvalidArgumentError
case class SubIDArgumentTooLarge(idStr: String, position: Position)
   extends InvalidArgumentError
case class DivideByZero(position: Position) extends InvalidArgumentError
case class InvalidCast(lit: Literal, typ: Type, position: Position)
    extends InvalidArgumentError
case class InvalidConversionFormatError(conversion: String, position: Position)
    extends InvalidArgumentError
case class InvalidDateTimeSuffixError(suffix: String, position: Position)
    extends InvalidArgumentError
case class IncompatibleTimeArguments(position: Position)
    extends InvalidArgumentError
case class MixedArguments(expected: List[Type], position: Position)
    extends InvalidArgumentError
case class NativeCollectionModificationProhibited(clID: CollectionID, position: Position)
    extends InvalidArgumentError
/**
  * Returned when the document index does not exist for a user-defined
  * collection but a document needs to be added to it, which happens when a
  * document and its collection are created in the same transaction.
  */
case class CollectionAndDocumentCreatedInSameTx(collID: CollectionID, position: Position)
    extends InvalidArgumentError
case class NonStreamableType(provided: Type, position: Position)
    extends InvalidArgumentError

case class InvalidFQL4Value(provided: String, position: Position)
    extends InvalidArgumentError

case class FunctionCallError(
  id: UserFunctionID,
  position: Position,
  override val cause: List[Error])
    extends EvalError {
  def code = "call error"
}

sealed trait StackOverflowError extends EvalError {
  def code = "stack overflow"
}

case class LambdaStackOverflowError(lambda: LambdaL, position: Position)
    extends StackOverflowError
case class FunctionStackOverflowError(id: UserFunctionID, position: Position)
    extends StackOverflowError

case class PermissionDenied(resource: Either[EventSet, RefL], position: Position)
    extends EvalError {
  def code = "permission denied"
}

case class AuthenticationFailed(position: Position) extends EvalError {
  def code = "authentication failed"
}

case class ValueNotFound(path: List[Either[Long, String]], position: Position)
    extends EvalError {
  def code = "value not found"
}

case class InstanceNotFound(resource: Either[EventSet, RefL], position: Position)
    extends EvalError {
  def code = "instance not found"
}

case class IndexInactive(name: String, position: Position) extends EvalError {
  def code = "instance not found"
}

case class KeyNotFound(secret: String, position: Position) extends EvalError {
  def code = "instance not found"
}

case class InstanceAlreadyExists(provided: DocID, position: Position)
    extends EvalError {
  def code = "instance already exists"
}

case class ValidationError(
  override val validationFailures: List[ValidationException],
  position: Position)
    extends EvalError {
  def code = "validation failed"
}

case class CheckConstraintEvalError(
  inner: ConstraintFailure.CheckConstraintFailure,
  position: Position)
    extends EvalError {
  def code = "check constraint failure"
}

case class SchemaValidationError(msg: String, position: Position) extends EvalError {
  def code = "schema validation failed"
}

case class InstanceNotUnique(
  indexes: List[(RefL, Vector[Literal])],
  position: Position)
    extends EvalError {
  def code = "instance not unique"
}

case class InvalidObjectInContainer(position: Position) extends EvalError {
  def code = "invalid object in container"
}

case class MoveDatabaseError(
  src: String,
  dst: String,
  reason: String,
  position: Position)
    extends EvalError {
  def code = "move database error"
}

case class FeatureNotAvailable(feature: String, position: Position)
    extends EvalError {

  def code = "feature not available"
}
