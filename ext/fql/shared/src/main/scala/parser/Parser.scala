package fql.parser

import fastparse._
import fastparse.internal.{ Lazy, Msgs }
import fql.ast._
import fql.error.ParseError
import fql.Result

object Parser {

  // "Top-level" entry points for FQL and FSL

  def query(str: String): Result[Expr] =
    query(str, Src.Query(str), isTemplate = false, completions = false)

  /** Parsers the query, allowing template placeholders. */
  def queryTemplate(str: String): Result[Expr] =
    query(str, Src.Query(str), isTemplate = true, completions = false)

  /** Parses the query, but ignores errors like lets at the end of blocks. This
    * parsed expr should only be used for auto-completion. Evaling it will throw
    * illegal state exceptions.
    */
  def queryForCompletions(str: String): Result[Expr] =
    query(str, Src.Query(str), isTemplate = false, completions = true)

  private def query(
    str: String,
    src: Src,
    isTemplate: Boolean,
    completions: Boolean): Result[Expr] = {
    val parser = p(src, 0, isTemplate, completions)
    mapParseResult(src, parse(str, parser.parseQuery(_)), parser)
  }

  def lambdaExpr(str: String, src: Src, srcOffset: Int = 0): Result[Expr.Lambda] = {
    val parser = p(src, srcOffset)
    mapParseResult(src, parse(str, parser.parseLambdaExpr(_)), parser)
  }

  def schemaItems(str: String, src: Src = Src.FSL("")): Result[Seq[SchemaItem]] =
    fslNodes(str, src).flatMap(SchemaItemConverter(_))

  def fslNodes(str: String, src: Src = Src.FSL("")): Result[Seq[FSL.Node]] = {
    val parser = p(src, 0)
    mapParseResult(src, parse(str, parser.parseRawSchemaItems(_)), parser)
  }

  def fslNodesForCompletions(
    str: String,
    src: Src = Src.FSL("")): Result[Seq[FSL.Node]] = {
    val parser = p(src, 0, completions = true)
    mapParseResult(src, parse(str, parser.parseRawSchemaItems(_)), parser)
  }

  // specialized parser for field paths

  def path(str: String, src: Src = Src.Null): Result[Path] = {
    val parser = p(src, 0)
    mapParseResult(src, parse(str, parser.parseFieldPathExpr(_)), parser)
  }

  // variant parsers

  def expr(
    str: String,
    src: Src = Src.Query(""),
    allowShortLambda: Boolean = false,
    srcOffset: Int = 0): Result[Expr] = {
    val parser = p(src, srcOffset)
    mapParseResult(
      src,
      parse(
        str,
        if (allowShortLambda) {
          parser.parseExprAllowShortLambda(_)
        } else {
          parser.parseExpr(_)
        }),
      parser)
  }

  def typeExpr(
    str: String,
    src: Src = Src.Query(""),
    srcOffset: Int = 0): Result[TypeExpr] = {
    val parser = p(src, srcOffset)
    mapParseResult(src, parse(str, parser.parseTypeExpr(_)), parser)
  }

  def schemaTypeExpr(
    str: String,
    src: Src = Src.Query(""),
    srcOffset: Int = 0): Result[SchemaTypeExpr] = {
    val parser = p(src, srcOffset)
    mapParseResult(src, parse(str, parser.parseSchemaTypeExpr(_)), parser)
  }

  def typeSchemeExpr(
    str: String,
    src: Src = Src.Query(""),
    srcOffset: Int = 0): Result[TypeExpr.Scheme] = {
    val parser = p(src, srcOffset)
    mapParseResult(src, parse(str, parser.parseTypeSchemeExpr(_)), parser)
  }

  def patExpr(
    str: String,
    src: Src = Src.Query(""),
    srcOffset: Int = 0): Result[PatExpr] = {
    val parser = p(src, srcOffset)
    mapParseResult(src, parse(str, parser.parsePatExpr(_)), parser)
  }

  private def p(
    src: Src,
    srcOffset: Int,
    isTemplate: Boolean = false,
    completions: Boolean = false) =
    new Parser(src, srcOffset, isTemplate, completions)

  private def mapParseResult[A](src: Src, pr: Parsed[A], parser: Parser): Result[A] =
    // matches on type, because using unapply breaks exhaustiveness check.
    pr match {
      case _ if !parser.errs.isEmpty => Result.Err(parser.errs)
      case s: Parsed.Success[A]      => Result.Ok(s.value)
      case f: Parsed.Failure =>
        val names = f
          .trace()
          .groups
          .value
          .map { lazyName => lazyName() }
          .toSet
          .toSeq
          .sorted
        val msg = if (names.sizeIs == 0) {
          // Internal failure, but we pretend everything worked.
          "expression"
        } else if (names.sizeIs == 1) {
          names(0)
        } else if (names.sizeIs == 2) {
          s"${names(0)} or ${names(1)}"
        } else {
          val prefix = names.slice(0, names.length - 1).mkString(", ")
          s"${prefix}, or ${names.last}"
        }
        val span = Span(f.index, f.index + 1, src)
        Result.Err(ParseError(s"Expected $msg", span))
    }
}

/** The `completions` argument controls the errors that parsing produces. For
  * example, if a block ends with a `let` statement, it will be allowed when
  * `completiosn` is true. `completions` should only be enabled in fql-analyzer,
  * as it breaks assumptions made in model about the resulting AST.
  */
final class Parser(
  val src: Src,
  val srcOffset: Int = 0,
  val isTemplate: Boolean = false,
  val completions: Boolean = false)
    extends ExprParser
    with PatExprParser
    with SchemaItemParser
    with PartialTypeExprParser
    with TypeExprParser
    with Errors
    with Identifiers
    with Numbers
    with Spans
    with Strings
    with Tokens
    with Whitespace {

  var nextTemplate = 0
  def nextTemplateValue(span: Span): Expr = {
    val res = Expr.Id("$" + nextTemplate, span)
    nextTemplate += 1
    res
  }

  // HACK: Skips failures. This probably breaks fastparse. Only use when parsing for
  // completions.
  def skipFails[T](value: P[T]): P[Option[T]] = {
    if (!value.isSuccess) {
      value.freshSuccess(None)
    } else {
      value.map(Some(_))
    }
  }

  /** This is a reimplementation of: fastparse.P(t)(name, ctx)
    *
    * Specifically, when we're in the error pass, this will aggregate an empty
    * message. This will essentially remove this parser and all child parsers
    * from the error output.
    *
    * TODO: Benchmark this, and see if we need a macro.
    */
  def P[T](t: P[T])(implicit ctx: P[_]): P[T] = {
    // Prevent this rule, or any child rules from creating a message in the
    // error.
    if (t.verboseFailures) {
      val startIndex = ctx.index
      t.aggregateMsg(
        startIndex,
        // For debugging, replace Msgs(List.empty) with this:
        // Msgs(List(new Lazy(() => "UNNAMED-" + name.value))),
        Msgs(List.empty),
        t.failureGroupAggregate,
        startIndex < t.traceIndex
      )
    }
    t
  }

  /** This is a copy of `fastparse.P(t)(name, ctx)`, except the name is passed
    * in via a string literal. In fastparse, they use an `implicit sourcecode.Name`
    * to get the parser's name.
    */
  def P[T](t: P[T], name: String)(implicit ctx: P[_]): P[T] = {
    if (t.verboseFailures) {
      val startIndex = ctx.index
      t.aggregateMsg(
        startIndex,
        Msgs(List(new Lazy(() => name))),
        t.failureGroupAggregate,
        startIndex < t.traceIndex
      )
      if (!t.isSuccess) {
        t.failureStack = (name -> startIndex) :: t.failureStack
      }
    }
    t
  }
}
