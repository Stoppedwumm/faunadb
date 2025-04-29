package fauna.model.schema

import fauna.ast.EvalContext
import fauna.atoms.DocID
import fauna.lang.syntax._
import fauna.model.runtime.fql2.{ FQLInterpCtx, Result, UnholyEval }
import fauna.model.runtime.Effect
import fauna.model.Collection
import fauna.repo.query.Query
import fauna.repo.values.Value
import fauna.storage.doc.Data
import fauna.storage.ir.{ MapV, StringV }
import fql.ast.Src
import fql.parser.Parser
import scala.collection.immutable.ArraySeq

object CheckConstraint {

  private val MaxEffect = Effect.Limit(Effect.Read, "check constraints")

  def fromData(data: Data): List[CheckConstraint] = {
    data(Collection.ConstraintsField)
      .getOrElse(Vector.empty)
      .flatMap(cs => {
        cs.fields.get("check" :: Nil) match {
          case Some(fields: MapV) =>
            val name = fields.get("name" :: Nil) match {
              case Some(StringV(n)) => n
              case _ =>
                throw new IllegalStateException(
                  s"Check constraint $cs has missing or non-string name")
            }
            val body = fields.get("body" :: Nil) match {
              case Some(StringV(b)) => b
              case _ =>
                throw new IllegalStateException(
                  s"Check constraint $cs has missing or non-string body")
            }
            Some(CheckConstraint(name, body))
          case _ => None
        }
      })
      .toList
  }
}

final case class CheckConstraint(name: String, body: String) {
  import CheckConstraint._
  import Result._

  private[this] lazy val parsed: Result[Value.Lambda] =
    Parser.lambdaExpr(body, Src.Inline(s"*check_constraint:$name*", body)) map {
      lambda =>
        val params = lambda.params.view.map { _ map { _.str } }.to(ArraySeq)
        Value.Lambda(params, vari = None, lambda.body, closure = Map.empty)
    }

  def toData = MapV("check" -> MapV("name" -> name, "body" -> body))

  def eval(ctx: FQLInterpCtx, doc: Value.Doc): Query[Result[Value]] =
    Query
      .value(parsed)
      .flatMapT { lambda =>
        ctx
          .withEffectLimit(MaxEffect)
          .evalApply(lambda, IndexedSeq(doc))
      }

  def evalV4(ec: EvalContext, id: DocID): Query[Result[Value]] =
    Query.value(parsed).flatMapT { lambda =>
      val ec0 = ec.copy(effectLimit = MaxEffect)
      UnholyEval.ctxV4ToV10(ec0).flatMap { intp =>
        intp.evalLambda(lambda, None, name, IndexedSeq(Value.Doc(id)))
      }
    }
}
