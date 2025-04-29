package fauna.model.schema

import fauna.atoms._
import fauna.auth.EvalAuth
import fauna.model.runtime.fql2._
import fauna.model.runtime.Effect
import fauna.model.Collection
import fauna.repo.query.Query
import fauna.repo.schema.{ ConstraintFailure, Path, SchemaResult }
import fauna.repo.values.Value
import fauna.storage.doc.{ Data, FieldType }
import fauna.storage.ir._
import fql.ast.{ Expr, Src, TypeExpr }
import fql.parser.Parser
import fql.typer.Typer

object DefinedField {

  def fromData(coll: String, data: Data): Map[String, DefinedField] =
    data(Collection.DefinedFields) match {
      case Some(v) =>
        v.map { case (name, data) => name -> DefinedField(coll, name, data) }.toMap
      case None => Map.empty
    }

  def evalDefault(
    scope: ScopeID,
    name: String,
    src: String,
    srcName: String): Query[SchemaResult[IRValue]] =
    Parser.expr(src, Src.Inline(srcName, src)) match {
      case fql.Result.Ok(expr) =>
        val intp =
          new FQLInterpreter(
            EvalAuth(scope),
            Effect.Limit(Effect.Observation, "default values"))
        intp
          .evalWithTypecheck(expr, Map.empty, FQLInterpreter.TypeMode.Disabled)
          .map {
            case Result.Ok((v, _)) =>
              Value.toIR(v) match {
                case Right(ir) =>
                  SchemaResult.Ok(ir)
                case Left(_) =>
                  // The error is missing some info and looks bad.
                  // Let's cook our own.
                  SchemaResult.Err(Seq(ConstraintFailure.DefaultValueFailure(
                    Path(Right(name)),
                    s"Value has type ${v.dynamicType.displayString}, which cannot be persisted")))
              }

            case Result.Err(err) =>
              SchemaResult.Err(
                err.errors.map(e =>
                  ConstraintFailure
                    .DefaultValueFailure(
                      Path(Right(name)),
                      e.renderWithSource(Map.empty))))
          }

      case fql.Result.Err(e) => throw new IllegalStateException(e.toString)
    }

  def evalDefault(
    scope: ScopeID,
    path: Path,
    body: Expr): Query[SchemaResult[IRValue]] = {
    val intp =
      new FQLInterpreter(
        EvalAuth(scope),
        Effect.Limit(Effect.Observation, "default values"))

    intp
      .evalWithTypecheck(body, Map.empty, FQLInterpreter.TypeMode.Disabled)
      .map {
        case Result.Ok((v, _)) =>
          Value.toIR(v) match {
            case Right(ir) =>
              SchemaResult.Ok(ir)
            case Left(_) =>
              // The error is missing some info and looks bad.
              // Let's cook our own.
              SchemaResult.Err(Seq(ConstraintFailure.DefaultValueFailure(
                path,
                s"Value has type ${v.dynamicType.displayString}, which cannot be persisted")))
          }

        case Result.Err(err) =>
          SchemaResult.Err(
            err.errors.map(e =>
              ConstraintFailure
                .DefaultValueFailure(path, e.renderWithSource(Map.empty))))
      }
  }
}

object DefinedFieldData {
  implicit val DefinedFieldType =
    FieldType.RecordCodec[DefinedFieldData]
}

final case class DefinedFieldData(signature: String, default: Option[String])

final case class DefinedField(coll: String, name: String, data: DefinedFieldData) {

  def signature = data.signature
  def default = data.default
  def hasDefault = data.default.isDefined
  def src = s"*field:$coll:$name*"

  lazy val expectedTypeExpr = {
    Parser.schemaTypeExpr(signature, Src.Inline(src, signature)) match {
      case fql.Result.Ok(te) => te
      case fql.Result.Err(e) => throw new IllegalStateException(e.toString)
    }
  }

  // Signatures cannot have generics, so we make a type scheme with no params, and
  // convert that. This makes named types instead of skolems.
  lazy val expectedType =
    Typer().typeTSchemeUncheckedType(
      TypeExpr.Scheme(Seq.empty, expectedTypeExpr.asTypeExpr))

  def defaultQ(src: String)(scope: ScopeID): Query[SchemaResult[IRValue]] =
    DefinedField.evalDefault(scope, name, src, this.src)
}
