package fauna.model.runtime.fql2

import fauna.atoms.DocID
import fauna.model.runtime.fql2.serialization.MaterializedValue
import fauna.model.runtime.fql2.ToString._
import fauna.model.runtime.Effect
import fauna.model.schema.NamedCollectionID
import fauna.repo.query.Query
import fauna.repo.schema.ConstraintFailure
import fauna.repo.values._
import fql.ast.Span
import fql.error.{ Error, Hint, ParseError }

// FIXME: we end up unifying QueryCheckFailure and QueryRuntimeFailure in
// Result, even though they are processed in separate stages. As a result the
// methods below are implemented on both, even though this doesn't makes
// sense.
trait CallSiteTracing {
  type Self <: QueryFailure

  /** We always want to filter out error spans that come from set
    * pagination tokens.  This is because we won't ever have the source
    * for these errors.  This method is used to filter them out.
    */
  def filterDecodedErrors(): Self
  // apply only to QueryRuntimeFailure. Doesn't really make sense for QCF.
  def filterErrors(): Self
}

sealed trait QueryFailure extends CallSiteTracing {
  def code: String

  /** used for source lookup in FQLEndpoint */
  def errors: Seq[Error]

  /** The value of the `message` field
    */
  def failureMessage: String

  def toQuery: Query[Result[Nothing]] = toResult.toQuery
  def toResult: Result[Nothing] = Result.Err(this)
}

final case class QueryCheckFailure(
  errors: Seq[Error]
) extends QueryFailure {
  type Self = QueryCheckFailure

  require(
    errors.nonEmpty,
    "can't create a query check failure with no errors"
  )

  def code = "invalid_query"
  def filterErrors() = copy(errors = errors.filter { e =>
    e.span.src.isQuery || e.span.src.isNull
  })

  def filterDecodedErrors() = this

  def merge(o: QueryCheckFailure) = QueryCheckFailure(errors ++ o.errors)
  def srcs = errors.map(_.span.src)

  def failureMessage: String = {
    val messageEnding = if (errors.length > 1) "checks" else "check"
    s"The query failed ${errors.length} validation $messageEnding"
  }
}

object QueryCheckFailure {

  // Variant constructors

  def Unimplemented(message: String, span: Span) =
    QueryCheckFailure(Seq(CheckError(message, None, span)))

  def InvalidLambdaDefinition(fieldName: String, span: Span, suggestArrow: Boolean) =
    QueryCheckFailure(
      Seq(
        CheckError(
          "Body must be a lambda expression.",
          Some(s"`$fieldName` is not a lambda expression."),
          span,
          hints = if (suggestArrow) {
            Seq(
              Hint(
                "Declare a lambda.",
                span.copy(end = span.start),
                suggestion = Some("() => ")))
          } else {
            Seq.empty
          }
        )))

  def InvalidShortLambda(span: Span) =
    QueryCheckFailure(
      Seq(ParseError("Short form lambdas are not allowed here", span)))

  def TypecheckStackOverflow(span: Span) =
    QueryCheckFailure(
      Seq(CheckError("Typechecking failed due to query complexity.", None, span)))

  final case class CheckError(
    message: String,
    override val annotation: Option[String],
    span: Span,
    override val hints: Seq[Hint] = Seq.empty
  ) extends Error
}

final case class QueryRuntimeFailure(
  code: String,
  failureMessage: String,
  trace: FQLInterpreter.StackTrace,
  // These hints will surround the original error. The trace will follow after
  // these hints.
  errorHints: Seq[Hint] = Seq.empty,
  constraintFailures: Seq[ConstraintFailure] = Nil,
  abortReturn: Option[MaterializedValue] = None)
    extends QueryFailure
    with Error {
  type Self = QueryRuntimeFailure

  // Runtime failures only have a single error, so we make `this` be the single
  // error.
  override def errors = Seq(this)
  override def span = trace.currentStackFrame
  override def hints = errorHints ++ trace.trace.drop(1).zipWithIndex.map {
    case (span, 0) if errorHints.nonEmpty => Hint.TraceWithName(span)
    case (span, _)                        => Hint.Trace(span)
  }

  def srcs = trace.trace.map(_.src)

  // This is the error message in `summary`, not the error message in the
  // `error` blob. The message at `error.message` is the `failureMessage`.
  def message = if (constraintFailures.isEmpty) {
    failureMessage
  } else {
    val errMsg = new StringBuilder(s"${failureMessage}\nconstraint failures:")
    constraintFailures.map(constraintFailureToMessage(_)) foreach { ve =>
      errMsg.append(s"\n  ${ve.replace("\n", "\n      ")}")
    }
    errMsg.result()
  }

  def filterDecodedErrors() =
    copy(trace = trace.filter(_ != Span.DecodedSet))
  def filterErrors() = copy(trace = trace.filter(_.src.isQuery))

  def withHint(hint: Hint) = copy(errorHints = errorHints.appended(hint))
  def withHints(hints: Seq[Hint]) = copy(errorHints = errorHints ++ hints)

  private def constraintFailureToMessage(failure: ConstraintFailure): String = {
    if (failure.label.nonEmpty) {
      s"${failure.label}: ${failure.message}"
    } else {
      failure.message
    }
  }
}

object QueryRuntimeFailure {
  object Simple {
    def unapply(v: QueryRuntimeFailure)
      : Option[(String, String, Span, Seq[ConstraintFailure])] =
      Some((v.code, v.failureMessage, v.span, v.constraintFailures))
  }

  def apply(code: String, message: String, span: Span): QueryRuntimeFailure =
    QueryRuntimeFailure(code, message, FQLInterpreter.StackTrace(Seq(span)))

  def apply(
    code: String,
    message: String,
    span: Span,
    hints: Seq[Hint]): QueryRuntimeFailure =
    QueryRuntimeFailure(code, message, FQLInterpreter.StackTrace(Seq(span)), hints)

  def UnboundVariable(name: String, stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure("unbound_variable", s"Unbound variable `$name`", stackTrace)

  def IndexOutOfBounds(
    idx: Int,
    length: Int,
    stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "index_out_of_bounds",
      s"index ${idx} out of bounds for length ${length}",
      stackTrace)

  def InvalidType(
    expectedType: ValueType,
    v: Value,
    stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "type_mismatch",
      s"expected type: ${expectedType.displayString}, received ${v.dynamicType.displayString}",
      stackTrace
    )

  // NB: Use the type error for an argument of invalid type, not this generic error.
  def InvalidArgument(
    argName: String,
    reason: String,
    stackTrace: FQLInterpreter.StackTrace,
    hints: Seq[Hint] = Seq.empty) =
    QueryRuntimeFailure(
      "invalid_argument",
      s"invalid argument `$argName`: $reason",
      stackTrace,
      hints
    )

  def InvalidArgumentType(
    argName: String,
    expected: ValueType,
    actual: ValueType,
    stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "invalid_argument",
      s"expected value for `$argName` of type ${expected.displayString}, received ${actual.displayString}",
      stackTrace)

  def InvalidArgumentTypes(
    funcName: String,
    expected: String,
    actual: String,
    stackTrace: FQLInterpreter.StackTrace) = {
    QueryRuntimeFailure(
      "invalid_argument",
      s"expected arguments to `$funcName` of type $expected, received $actual",
      stackTrace)
  }

  def InvalidBounds(
    ctx: FQLInterpCtx,
    argName: String,
    predicate: String,
    actual: Value,
    stackTrace: FQLInterpreter.StackTrace) =
    actual.toDebugString(ctx).map { str =>
      QueryRuntimeFailure(
        "invalid_bounds",
        s"expected `$argName` to be $predicate, received $str",
        stackTrace)
    }

  def InvalidRegex(
    description: String,
    index: Int,
    stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "invalid_regex",
      s"Invalid regular expression. $description at index $index",
      stackTrace
    )

  // TODO adding this so we can cover the different write failures but this one isn't
  // actually implemented yet.
  // When it is we will need to do work to translate the collection ids
  // for each inbound document into collection names to return to the user.
  def DeleteConstraintViolation(
    collectionName: String,
    id: DocID,
    // inboundIds: Iterable[DocID],
    stackTrace: FQLInterpreter.StackTrace) =
    SchemaConstraintViolation(
      s"Unable to delete document with id ${id.subID.toLong} in collection $collectionName because it is referenced by other documents.",
      Nil,
      stackTrace
    )

  def SchemaConstraintViolation(
    message: String,
    failures: Iterable[ConstraintFailure],
    stackTrace: FQLInterpreter.StackTrace
  ) =
    QueryRuntimeFailure(
      "constraint_failure",
      message,
      stackTrace,
      constraintFailures = failures.toSeq
    )

  def DatabaseSchemaEnvViolation(
    message: String,
    stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure("invalid_schema", message, stackTrace)

  def InvalidDocumentID(
    collectionName: String,
    id: DocID,
    stackTrace: FQLInterpreter.StackTrace
  ) =
    QueryRuntimeFailure(
      "invalid_document_id",
      s"Create in collection $collectionName failed due to invalid document id ${id.subID.toLong}",
      stackTrace
    )

  def DocumentIDExists(
    collectionName: String,
    id: DocID,
    stackTrace: FQLInterpreter.StackTrace
  ) = {
    assert(NamedCollectionID.unapply(id.collID).isEmpty)
    QueryRuntimeFailure(
      "document_id_exists",
      s"Collection `$collectionName` already contains document with id ${id.subID.toLong}.",
      stackTrace
    )
  }

  def DocumentNotFound(
    collectionName: String,
    doc: Value.Doc,
    stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "document_not_found",
      doc.name match {
        case Some(name) =>
          s"$collectionName `$name` not found."
        case None =>
          s"Collection `$collectionName` does not contain document with id ${doc.id.subID.toLong}."
      },
      stackTrace
    )

  def DocumentDeleted(
    collectionName: String,
    doc: Value.Doc,
    stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "document_deleted",
      doc.name match {
        case Some(name) =>
          s"$collectionName `$name` was deleted."
        case None =>
          s"Document in `$collectionName` with id ${doc.id.subID.toLong} was deleted."
      },
      stackTrace
    )

  def CollectionDeleted(doc: Value.Doc, stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "document_not_found",
      s"Document with id ${doc.id.subID.toLong} does not exist because the collection was deleted.",
      stackTrace
    )

  def InvalidFuncParamArity(
    argName: String,
    expected: Int,
    actualArity: Value.Func.Arity,
    stackTrace: FQLInterpreter.StackTrace) = {
    def s(num: Int) = if (num == 1) "" else "s"

    QueryRuntimeFailure(
      "invalid_argument",
      s"`$argName` should accept $expected parameter${s(expected)}, " +
        s"provided function requires ${actualArity.displayString("parameter")}.",
      stackTrace
    )
  }

  def InvalidFuncArity(
    expectedArity: Value.Func.Arity,
    actual: Int,
    stackTrace: FQLInterpreter.StackTrace) = {
    def was(num: Int) = if (num == 1) "was" else "were"

    QueryRuntimeFailure(
      "invalid_function_invocation",
      s"callee function expects ${expectedArity.displayString()}, but $actual ${was(actual)} provided.",
      stackTrace)
  }

  def IndexNotQueryable(
    collName: String,
    indexName: String,
    stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "invalid_index_invocation",
      s"The index `$collName.$indexName` is not queryable.",
      stackTrace
    )

  def FunctionDoesNotExist(
    field: String,
    callee: ValueType,
    stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "invalid_function_invocation",
      s"The function `$field` does not exist on `${callee.displayString}`",
      stackTrace
    )

  def V4EvalError(msgs: List[String], stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "eval_error",
      msgs mkString "\n",
      stackTrace
    )

  def V4InvalidArgument(
    name: String,
    v: Value,
    stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "invalid_argument",
      s"Invalid argument for `$name`. FQL v4 does not support `${v.dynamicType.displayString}` values.",
      stackTrace
    )

  def NullValue(
    message: String,
    opStackTrace: FQLInterpreter.StackTrace,
    nullSpan: Span) =
    QueryRuntimeFailure(
      "null_value",
      message,
      opStackTrace,
      errorHints = if (!opStackTrace.currentStackFrame.overlaps(nullSpan)) {
        Seq(Hint("Null value created here", nullSpan))
      } else {
        Seq.empty
      }
    )

  def InvalidFieldFunctionInvalidType(
    field: String,
    actualType: ValueType,
    stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "invalid_function_invocation",
      s"`$field`: Expected a function, found `${actualType.displayString}`",
      stackTrace
    )

  def InvalidNullAccess(
    field: String,
    nullSpan: Span,
    stackTrace: FQLInterpreter.StackTrace) = {
    QueryRuntimeFailure(
      "invalid_null_access",
      s"Cannot access `$field` on null.",
      trace = stackTrace,
      errorHints = Seq(Hint("Null value created here", nullSpan))
    )
  }

  def InvalidCursor(stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "invalid_cursor",
      "Cursor cursor is invalid or expired.",
      stackTrace)

  def PermissionDenied(
    stackTrace: FQLInterpreter.StackTrace,
    msg: String = "Insufficient privileges to perform the action.") =
    QueryRuntimeFailure("permission_denied", msg, stackTrace)

  def InvalidEffect(
    limit: Effect.Limit,
    action: Effect.Action,
    eff: Effect,
    stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure("invalid_effect", action.message(eff, limit), stackTrace)

  def MustWriteNow(stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "invalid_write",
      "Cannot write at a snapshot time in the past.",
      stackTrace)

  def InternalFailure(stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "internal_failure",
      "An internal failure was encountered during the query",
      stackTrace
    )

  def DivideByZero(stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "divide_by_zero",
      "Attempted integer division by zero.",
      stackTrace)

  def InvalidID(str: String, stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure("invalid_id", s"`$str` is not a valid ID.", stackTrace)

  def InvalidSecret(stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "invalid_secret",
      "Provided secret did not match.",
      stackTrace)

  def InvalidTime(str: String, stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure("invalid_time", s"`$str` is not a valid time.", stackTrace)

  def InvalidTimeUnit(str: String, stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "invalid_unit",
      s"`$str` is not a valid time unit.",
      stackTrace)

  def MinimumTimeOverflow(stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "invalid_time",
      "Time exceeds the minimum value.",
      stackTrace)

  def MaximumTimeOverflow(stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "invalid_time",
      "Time exceeds the maximum value.",
      stackTrace)

  def InvalidDate(str: String, stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure("invalid_date", s"`$str` is not a valid date.", stackTrace)

  def InvalidDateUnit(str: String, stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "invalid_unit",
      s"`$str` is not a valid date unit.",
      stackTrace)

  def ValueTooLarge(reason: String, stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure("value_too_large", s"Value too large: $reason.", stackTrace)

  def Aborted(v: MaterializedValue, stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "abort",
      s"Query aborted.",
      stackTrace,
      Seq.empty,
      abortReturn = Some(v))

  def StackOverflow(limit: Int, stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "stack_overflow",
      s"number of stack frames exceeded limit of $limit",
      stackTrace)

  def InvalidComputedFieldAccess(stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "invalid_computed_field_access",
      s"invalid access of computed field",
      stackTrace)

  def DisabledFeature(name: String, stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "disabled_feature",
      s"$name are disabled at the moment.",
      stackTrace)

  def InvalidReceiver(
    method: String,
    reason: String,
    stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "invalid_receiver",
      s"can't call `$method` because $reason",
      stackTrace)

  def CollectionDocModify(stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "invalid_write",
      "Cannot modify a document and its collection in the same query.",
      stackTrace
    )

  def InvalidTimestampFieldAccess(stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure(
      "invalid_timestamp_field_access",
      s"invalid access of timestamp field",
      stackTrace)
}
