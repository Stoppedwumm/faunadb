package fql.analyzer

import fql.ast.{ FSL, Src }
import fql.error.Diagnostic
import fql.parser.{ Parser, SchemaItemConverter }
import fql.typer.{ TypeScheme, TypeShape }
import fql.Result

case class SchemaContext(
  globals: Map[String, TypeScheme],
  types: Map[String, TypeShape],
  logException: Throwable => Unit = _ => ())
    extends Context
    with SchemaContextCompletions {

  var query: String = ""
  var ast: Option[Seq[FSL.Node]] = None
  var diagnostics: Seq[Diagnostic] = Seq.empty

  val src = Src.Id(Src.QueryName)

  def update(query: String): Unit = {
    this.query = query

    Parser.fslNodes(query, Src.Query(query)) match {
      case Result.Ok(ast) =>
        this.ast = Some(ast)
        this.diagnostics = SchemaItemConverter(ast) match {
          case Result.Ok(_)     => Seq.empty
          case Result.Err(errs) => errs
        }
      case Result.Err(errs) =>
        this.ast = None
        this.diagnostics = errs
    }
  }
}
