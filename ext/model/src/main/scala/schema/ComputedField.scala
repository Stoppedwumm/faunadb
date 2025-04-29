package fauna.model.schema

import fauna.lang.syntax._
import fauna.model.runtime.fql2.{ FQLInterpCtx, Result }
import fauna.model.runtime.Effect
import fauna.model.Collection
import fauna.repo.query.Query
import fauna.repo.values.Value
import fauna.storage.doc.{ Data, FieldType }
import fql.ast.Src
import fql.parser.Parser
import scala.collection.immutable.ArraySeq

object ComputedField {

  // Computed fields need to be constructed in index bindings, which don't know the
  // collection name. So we insert this placeholder. This is fine because we don't
  // emit errors in indexing.
  val UnknownCollectionName = "<unknown>"

  def fromData(collName: String, data: Data): Map[String, ComputedField] =
    data(Collection.ComputeField)
      .getOrElse(List.empty)
      .map { case (name, data) => name -> ComputedField(collName, name, data) }
      .toMap

  private val MaxEffect = Effect.Limit(Effect.Read, "computed fields")
}

object ComputedFieldData {
  implicit val ComputedFieldType =
    FieldType.RecordCodec[ComputedFieldData]
}

final case class ComputedFieldData(body: String, signature: Option[String])

final case class ComputedField(coll: String, name: String, data: ComputedFieldData) {
  import ComputedField._
  import Result._

  def body = data.body
  def signature = data.signature

  private[this] lazy val src = Src.Inline(s"*computed_field:$coll:$name*", data.body)

  // Parsing is relatively expensive, and the name of this computed field won't
  // change, so we just parse lazily and cache the result.
  private[this] lazy val parsed: Result[Value.Lambda] =
    Parser.lambdaExpr(data.body, src) map { lambda =>
      val params = lambda.params.view.map { _ map { _.str } }.to(ArraySeq)
      Value.Lambda(params, vari = None, lambda.body, closure = Map.empty)
    }

  def eval(ctx: FQLInterpCtx, doc: Value.Doc): Query[Result[Value]] =
    Query
      .value(parsed)
      .flatMapT { lambda =>
        ctx
          .withEffectLimit(MaxEffect.min(ctx.effectLimit))
          .evalApply(lambda, IndexedSeq(doc))
      }
}
