package fql.parser

import fql.ast.{ Name, Span }
import fql.error.{ Hint, ParseError }

object TemplateSigil {
  // The \u0000 is replaced by '<' when rendering source. See
  // ext/fql/shared/src/main/scala/ast/Span.scala
  val Nul = '\u0000'
  val LeftAngle = '<'

  val Value = "\u0000value>"
  val Expr = "\u0000expr>"
}

// msg style:
//   * The span will be displayed following the message, as in this example:
//
//       error: The widget foobarred the frobulator
//       at *Query*3:8:
//         |
//       3 | widget.foobar(frobulator)
//         |        ^^^^^^^^^^^^^^^^^^
//         |
//
//   * msg should start with a capital letter.
//   * msg should end without a period.
//     - unless there is no span to follow the message.
//     - the span will follow the message with a lowercase `at ...:`
//   * msg does not need to contain details that are already in the span.

object Failure {
  import Hint.HintType

  def MissingReturn(block: Span, stmt: Span) = {
    val msg = "Missing return expression"
    val causeMsg = "Statement does not return a value"
    val cause = Hint(causeMsg, stmt, None, HintType.Cause)
    ParseError(msg, block, hints = List(cause))
  }

  object Unexpected {
    def apply(
      actual: String,
      expected: String,
      span: Span,
      hints: List[Hint] = Nil) =
      ParseError(s"Unexpected $actual. Expected $expected", span, hints = hints)

    def EndOfQuery(span: Span) =
      apply("end of query", "statement or expression", span)
    def EndOfBlock(span: Span) =
      apply("end of block", "statement or expression", span)
  }

  def InvalidShortLambda(span: Span) = {
    // FIXME: hint to wrap the expr in parens
    val msg = "Invalid anonymous field access"
    ParseError(msg, span)
  }
  def DuplicateLambdaArgument(original: Span, duplicate: Span) = {
    ParseError(
      "Duplicate lambda argument",
      duplicate,
      hints = Seq(Hint("Argument originally defined here", original)))
  }
  def DuplicateObjectKey(original: Span, duplicate: Span) = {
    ParseError(
      "Duplicate key",
      duplicate,
      hints = Seq(Hint("Key originally used here", original)))
  }
  def InvalidIdent(name: Name) = {
    val msg = "Invalid identifier"
    ParseError(msg, name.span, annotation = Some(s"`${name.str}` is a keyword"))
  }
  def InvalidEscape(span: Span, annotation: String) = {
    val msg = "Invalid escape"
    ParseError(msg, span, annotation = Some(annotation))
  }
  def InvalidVariadic(span: Span) = ParseError("Invalid variadic argument", span)

  def InvalidNamedType(span: Span) =
    ParseError("Only lambda argument types may be named", span)

  def InvalidNamedCount(span: Span) =
    ParseError("All parameters must be named, or none", span)

  object InvalidLambdaDefinition {
    def BodyMustBeALambda(span: Span) =
      ParseError(
        "Body must be a lambda expression.",
        span,
        Some("Expression is not a lambda."))

    def DeclareLambda(span: Span) =
      ParseError(
        "Body must be a lambda expression.",
        span,
        Some("Expression is not a lambda."),
        Seq(
          Hint(
            "Declare a lambda.",
            span.copy(end = span.start),
            suggestion = Some("() => ")))
      )
  }

  def InvalidField(name: Name) =
    ParseError(s"Invalid field `${name.str}`", name.span)

  // Fallback case for uncaught fastparse errors.
  def InternalError(msg: String, span: Span) = ParseError(msg, span)
}
