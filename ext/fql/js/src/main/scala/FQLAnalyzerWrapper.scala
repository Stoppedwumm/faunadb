package fql.js

import fql.analyzer
import scala.scalajs.js
import scala.scalajs.js.annotation._
import scala.scalajs.js.JSConverters._

/** Wraps language library types an exposes them to javascript.
  */
object FQLAnalyzerWrapper {

  @JSExportTopLevel("Environment", "fql")
  class Environment(
    val globals: js.Map[String, TypeScheme],
    val shapes: js.Map[String, TypeShape])
      extends js.Object

  @JSExportTopLevel("TypeShape", "fql")
  class TypeShape(
    val self: TypeScheme,
    val fields: js.Map[String, TypeScheme],
    val ops: js.Map[String, TypeScheme],
    @JSName("apply")
    val apply: js.UndefOr[TypeScheme],
    val access: js.UndefOr[TypeScheme],
    val alias: js.UndefOr[TypeScheme])
      extends js.Object

  @JSExportTopLevel("TypeScheme", "fql")
  class TypeScheme(val scala: fql.typer.TypeScheme) extends js.Object

  @JSExportTopLevel("typeSchemeFromString", "fql")
  def typeSchemeFromString(str: String): TypeScheme = {
    fql.parser.Parser.typeSchemeExpr(str, fql.ast.Src.Null) match {
      case fql.Result.Ok(sch) =>
        new TypeScheme(fql.typer.Typer.typeTSchemeUnchecked(sch))
      case fql.Result.Err(e) =>
        logException(
          new IllegalStateException(s"error parsing type from the environment: $e"))
        new TypeScheme(fql.typer.Type.Any.typescheme)
    }
  }

  def parseEnv(env: Environment) = {
    // Converts all the js types into fql types. This will explode if you pass in
    // invalid types, as per the norm in javascript.
    //
    // The given `env` object _must_ match the format of `Environment`.
    //
    // If any types fail to parse, those fields will not show up in the environment.

    val globals = env.globals.view.mapValues { _.scala }.toMap

    val shapes =
      env.shapes.view.mapValues { shape =>
        fql.typer.TypeShape(
          self = shape.self.scala,
          fields = shape.fields.view.mapValues { _.scala }.toMap,
          ops = shape.ops.view.mapValues { _.scala }.toMap,
          apply = shape.apply.toOption.map(_.scala),
          access = shape.access.toOption.map(_.scala),
          alias = shape.alias.toOption.map(_.scala)
        )
      }.toMap

    (globals, shapes)
  }

  private def logException(e: Throwable): Unit = {
    val _ = js.Dynamic.global.console.log(s"internal typechecking error: $e")
  }

  @JSExportTopLevel("newQueryContext", "fql")
  def newQueryContext(env: Environment): Context =
    Context(
      env,
      env => {
        val (globals, types) = parseEnv(env)
        analyzer.QueryContext(globals, types, logException)
      })

  @JSExportTopLevel("newSchemaItemContext", "fql")
  def newSchemaItemContext(
    env: Environment,
    kind: js.UndefOr[String],
    name: js.UndefOr[String]): Context = {
    val _ = (kind, name)
    Context(
      env,
      env => {
        val (globals, types) = parseEnv(env)
        analyzer.SchemaContext(globals, types, logException)
      })
  }

  @JSExportTopLevel("newSchemaFileContext", "fql")
  def newSchemaFileContext(env: Environment, path: js.UndefOr[String]): Context = {
    val _ = path
    Context(
      env,
      env => {
        val (globals, types) = parseEnv(env)
        analyzer.SchemaContext(globals, types, logException)
      })
  }

  /** Implements the QueryContext interface in index.ts.
    */
  @JSExportTopLevel("Context", "fql")
  @JSExportAll
  class Context(
    var ctx: analyzer.Context,
    val newCtx: (Environment) => analyzer.Context) {

    def refresh(env: Environment): Unit = {
      val query = ctx.query
      ctx = newCtx(env)
      ctx.update(query)
    }

    def onUpdate(query: String): Unit = ctx.update(query)

    def errors(): js.Array[Diagnostic] = ctx.diagnostics.map(Diagnostic(_)).toJSArray

    // TODO: Implement
    def completionsAt(cursor: Int): js.Array[CompletionItem] = {
      ctx.completions(cursor).map(CompletionItem(_)).toJSArray
    }

    def hoverOn(cursor: Int): js.UndefOr[String] = {
      val _ = cursor
      js.undefined
    }

    def gotoDefinitionAt(cursor: Int): js.UndefOr[Int] = {
      val _ = cursor
      js.undefined
    }

    def highlightAt(cursor: Int): js.Array[HighlightSpan] = {
      val _ = cursor
      js.Array()
    }
  }

  object Context {
    def apply(env: Environment, newCtx: Environment => analyzer.Context) =
      new Context(newCtx(env), newCtx)
  }

  /** Implements the Span interface in index.ts.
    */
  @JSExportTopLevel("Span", "fql")
  @JSExportAll
  case class Span(start: Int, end: Int, src: Src)

  /** Implements the Src interface in index.ts.
    */
  @JSExportTopLevel("Src", "fql")
  @JSExportAll
  case class Src(name: String)

  /** Implements the Diagnostic interface in index.ts.
    */
  @JSExportTopLevel("Diagnostic", "fql")
  @JSExportAll
  case class Diagnostic(
    span: Span,
    severity: String,
    message: String,
    relatedInfo: js.Array[DiagnosticInfo])

  /** Implements the DiagnosticInfo interface in index.ts.
    */
  @JSExportTopLevel("DiagnosticInfo", "fql")
  @JSExportAll
  case class DiagnosticInfo(span: Span, message: String)

  /** Mirrors the Severity enum in index.ts.
    */
  object Severity {
    val Error = "error"
    val Warn = "warn"
    val Hint = "hint"
    val Cause = "cause"
  }

  /** Implements the CompletionItem interface in index.ts.
    */
  @JSExportTopLevel("CompletionItem", "fql")
  @JSExportAll
  case class CompletionItem(
    label: String,
    detail: String,
    span: Span,
    replaceText: String,
    newCursor: Int,
    kind: String,
    retrigger: Boolean,
    snippet: js.UndefOr[String])

  /** Mirrors the CompletionItemKind enum in index.ts.
    */
  object CompletionItemKind {
    val Module = "module"
    val Keyword = "keyword"
    val Type = "type"
    val Variable = "variable"
    val Field = "field"
    val Property = "property"
    val Function = "function"

    def apply(kind: fql.analyzer.CompletionKind): String = {
      import fql.analyzer.CompletionKind
      kind match {
        case CompletionKind.Keyword  => Keyword
        case CompletionKind.Module   => Module
        case CompletionKind.Type     => Type
        case CompletionKind.Variable => Variable
        case CompletionKind.Field    => Field
        case CompletionKind.Property => Property
        case CompletionKind.Function => Function
      }
    }
  }

  /** Implements the HighlightSpan interface in index.ts.
    */
  @JSExportTopLevel("HighlightSpan", "fql")
  @JSExportAll
  case class HighlightSpan(span: Span, kind: String)

  /** Mirrors the HighlightKind enum in index.ts.
    */
  object HighlightKind {
    val Read = "read"
    val Write = "write"
  }

  // conversion functions

  object Src {
    val Null = Src(fql.ast.Src.Null)

    def apply(src: fql.ast.Src): Src = Src(
      name = src.name
    )
  }

  object Span {
    val Null = Span(fql.ast.Span.Null)

    def apply(span: fql.ast.Span): Span = Span(
      start = span.start,
      end = span.end,
      src = Src(span.src)
    )
  }

  object Diagnostic {
    def apply(d: fql.error.Diagnostic): Diagnostic = Diagnostic(
      span = Span(d.span),
      severity = Severity.Error,
      message = d.message,
      relatedInfo = d.hints.map(DiagnosticInfo(_)).toJSArray
    )
  }

  object DiagnosticInfo {
    def apply(hint: fql.error.Hint): DiagnosticInfo = DiagnosticInfo(
      span = Span(hint.span),
      message = hint.message
    )
  }

  object CompletionItem {
    def apply(compl: fql.analyzer.CompletionItem): CompletionItem = CompletionItem(
      label = compl.label,
      detail = compl.detail,
      span = Span(compl.span),
      replaceText = compl.replaceText,
      newCursor = compl.newCursor,
      kind = CompletionItemKind(compl.kind),
      retrigger = compl.retrigger,
      snippet = compl.snippet.orUndefined
    )
  }
}
