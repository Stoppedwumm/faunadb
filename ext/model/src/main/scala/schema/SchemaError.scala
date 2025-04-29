package fauna.model.schema

import fauna.model.runtime.fql2.{ QueryFailure => FQLQueryFailure }
import fauna.model.schema.fsl.SourceFile
import fql.error.Error
import scala.util.control.NoStackTrace

final class SchemaErrorsException(val errors: List[SchemaError])
    extends Exception
    with NoStackTrace

sealed trait SchemaError {
  import Result.Err
  def toResult = Err(this)
  def toQuery = toResult.toQuery
}

object SchemaError {
  case object Forbidden extends SchemaError
  case object NotFound extends SchemaError
  case object InvalidStoredSchema extends SchemaError

  final case class Validation(message: String) extends SchemaError
  final case class Conflict(message: String) extends SchemaError
  final case class QueryFailure(failure: FQLQueryFailure) extends SchemaError

  final case class Unexpected(cause: Throwable) extends SchemaError
  object Unexpected {
    def apply(msg: String) =
      new Unexpected(new IllegalStateException(msg))
  }

  final case class SchemaSourceErrors(
    errors: Iterable[SchemaError],
    files: Iterable[SourceFile])
      extends SchemaError

  final case class FQLError(toFQL: Error) extends SchemaError with Error {
    def message = toFQL.message
    def span = toFQL.span
    override def annotation = toFQL.annotation
    override def hints = toFQL.hints
  }
}
