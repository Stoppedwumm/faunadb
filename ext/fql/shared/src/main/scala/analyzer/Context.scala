package fql.analyzer

import fql.ast.{ Span, Src }
import fql.error.Diagnostic
import fql.typer.{ TypeScheme, TypeShape }

trait Context extends ContextUtil {
  val globals: Map[String, TypeScheme]
  val types: Map[String, TypeShape]

  var query: String
  var diagnostics: Seq[Diagnostic]
  val src: Src

  def update(source: String): Unit

  def completionsImpl(cursor: Int, identSpan: Span): Seq[CompletionItem]
  def completions(cursor: Int): Seq[CompletionItem] = {
    val ident = findIdent(cursor)

    val span = ident match {
      case Some(name) => name.span
      case None       => Span(cursor, cursor, src)
    }

    completionsImpl(cursor, span)
  }
}
