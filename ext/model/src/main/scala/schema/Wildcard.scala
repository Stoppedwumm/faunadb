package fauna.model.schema

import fauna.model.Collection
import fauna.repo.schema.{ ScalarType, SchemaType }
import fauna.storage.doc.{ Data, FieldType }
import fql.ast.{ Src, TypeExpr }
import fql.parser.Parser
import fql.typer.{ Type, Typer }

object Wildcard {

  def fromData(coll: String, data: Data): Option[Wildcard] =
    data(Collection.WildcardField) map { wc => Wildcard(coll, WildcardData(wc)) }

  def expectedType(wildcard: Option[Type], hasDefinedFields: Boolean): Option[Type] =
    if (wildcard.isEmpty && !hasDefinedFields) {
      Some(Type.Any)
    } else {
      wildcard
    }

  def expectedSchemaType(
    wildcard: Option[SchemaType],
    hasDefinedFields: Boolean): Option[SchemaType] =
    if (wildcard.isEmpty && !hasDefinedFields) {
      Some(ScalarType.Any)
    } else {
      wildcard
    }

  // Nice for a couple of tests.
  def any(col: String) = Wildcard(col, WildcardData("Any"))
}

object WildcardData {
  implicit val WildcardType =
    FieldType.RecordCodec[WildcardData]
}

final case class WildcardData(signature: String)

final case class Wildcard(coll: String, data: WildcardData) {

  def signature = data.signature
  def src = s"*field:$coll:<wildcard>*"

  // Re-use defined field parsing.
  lazy val expectedTypeExpr = {
    Parser.typeExpr(signature, Src.Inline(src, signature)) match {
      case fql.Result.Ok(te) => te
      case fql.Result.Err(e) => throw new IllegalStateException(e.toString)
    }
  }
  lazy val expectedType =
    Typer().typeTSchemeUncheckedType(TypeExpr.Scheme(Seq.empty, expectedTypeExpr))
}
