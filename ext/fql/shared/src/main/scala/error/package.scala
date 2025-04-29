package fql.error

import fql.ast.{ Expr, Span }
import fql.error.Hint

final case class ParseError(
  message: String,
  span: Span,
  override val annotation: Option[String] = None,
  override val hints: Seq[Hint] = Nil)
    extends Error

final case class TypeError private (
  message: String,
  span: Span,
  causes: Seq[TypeError],
  _hints: Seq[Hint]
) extends Error {
  import Hint.HintType

  override def hints =
    _hints ++ causes.flatMap { cause =>
      Hint(cause.message, cause.span, hintType = HintType.Cause) +: cause.hints
    }
}

trait DiagnosticEmitter[-D <: Diagnostic] {
  def emit(error: D): Unit
}

object EmptyEmitter extends DiagnosticEmitter[Diagnostic] {
  def emit(error: Diagnostic): Unit = ()
}

object TypeError {
  def apply(
    message: String,
    span: Span,
    _causes: Seq[TypeError] = Nil,
    hints: Seq[Hint] = Nil
  ): TypeError = {
    val causes = _causes.toList.sortWith { (a, b) =>
      a.span.compare(b.span) match {
        case 0   => a.message < b.message
        case cmp => cmp < 0
      }
    }.distinct

    new TypeError(message, span, causes, hints)
  }

  def InvalidHoleType(span: Span) =
    TypeError(s"Invalid placeholder type `_`.", span)

  def InvalidProjection(span: Span) =
    TypeError(
      "Invalid projection on a unknown type. Use function signatures to fix the issue.",
      span)

  def InvalidRecursiveType(span: Span) =
    TypeError(s"Invalid recursive type.", span)

  def InvalidSkolemConstraint(
    skol: String,
    skolSpan: Span,
    closureSpan: Span,
    span: Span) =
    TypeError(
      s"Value cannot be constrained by inner generic type `$skol`",
      span,
      Nil,
      Seq(
        Hint("Generic type definition", skolSpan),
        Hint("Outer value originates here", closureSpan))
    )

  def InvalidTypeArity(span: Span, name: String, expected: Int, provided: Int) =
    TypeError(
      if (expected == 0) {
        s"Type `$name` does not accept any parameters."
      } else {
        s"Type constructor `$name<>` accepts $expected parameters, received $provided"
      },
      span
    )

  def RecursiveVariable(span: Span, name: String) =
    TypeError(s"Recursively defined `$name` needs explicit type.", span)

  def RefToNonDoc(span: Span) =
    TypeError("Reference type must refer to a doc type", span)

  def TimeLimitExceeded(span: Span) =
    TypeError("Exceeded static typing time limit", span)

  def StackOverflow(span: Span) =
    TypeError("Typechecking failed due to query complexity", span)

  def UnboundTypeVariable(span: Span, name: String) =
    TypeError(s"Unknown type `$name`", span)

  def UnboundVariable(span: Span, name: String) =
    TypeError(s"Unbound variable `$name`", span)

  def UnimplementedExpr(span: Span, ex: Expr) =
    TypeError(s"Unimplemented typecheck for ${ex.getClass}", span)

  def UnknownInternalError(span: Span) =
    TypeError(
      "Internal typechecking error. Please create a ticket at https://support.fauna.com",
      span)
}
