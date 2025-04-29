package fauna.model.runtime.fql2.stdlib

import fauna.model.runtime.fql2._
import fauna.model.runtime.fql2.ToString._
import fauna.repo.values.Value

object IDCompanion extends CompanionObject("ID") {

  def contains(v: Value): Boolean = v match {
    case _: Value.ID => true
    case _           => false
  }

  private def applyImpl(ctx: FQLInterpCtx, v: Value) =
    TypeTag.ID.cast(v) match {
      case Some(v) => v.toQuery
      case None =>
        v.toDebugString(ctx)
          .map(QueryRuntimeFailure.InvalidID(_, ctx.stackTrace).toResult)
    }

  defApply(tt.ID)("id" -> tt.Str)(applyImpl)

  defApply(tt.ID)("id" -> tt.Number)(applyImpl)
}

object IDPrototype extends Prototype(TypeTag.ID, isPersistable = false)
