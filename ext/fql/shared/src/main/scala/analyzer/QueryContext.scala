package fql.analyzer

import fql.ast.{ Expr, Src }
import fql.error.Diagnostic
import fql.parser.Parser
import fql.typer.{ TypeScheme, TypeShape, Typer }
import fql.Result

case class QueryContext(
  globals: Map[String, TypeScheme],
  types: Map[String, TypeShape],
  logException: Throwable => Unit = _ => ())
    extends Context
    with QueryContextCompletions {
  var typer: Typer = Typer(globals, types)
  typer.logException = logException

  var query: String = ""
  var ast: Option[Expr] = None
  var diagnostics: Seq[Diagnostic] = Seq.empty

  val src = Src.Id(Src.QueryName)

  def update(query: String): Unit = {
    this.query = query

    Parser.query(query) match {
      case Result.Ok(ast) =>
        this.ast = Some(ast)
        val typer = Typer(globals, types)
        typer.logException = logException
        typer.recordingSpans = true
        this.diagnostics = typer.typeExpr(ast) match {
          case Result.Ok(_)     => Seq.empty
          case Result.Err(errs) => errs
        }
        this.typer = typer
      case Result.Err(errs) =>
        this.ast = None
        this.diagnostics = errs
    }
  }
}
