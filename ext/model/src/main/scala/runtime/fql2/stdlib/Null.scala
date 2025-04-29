package fauna.model.runtime.fql2.stdlib

import fauna.model.runtime.fql2._
import fauna.repo.values.Value
import fql.ast.Name

object NullCompanion extends CompanionObject("Null") {
  def contains(v: Value): Boolean = v match {
    case _: Value.Null => true
    case _             => false
  }
}

object NullPrototype extends Prototype(TypeTag.Null, isPersistable = true) {
  import FieldTable.R

  override def getField(ctx: FQLInterpCtx, self: Value.Null, name: Name) =
    getResolver(name.str) match {
      case Some(resolver) => resolver.impl(ctx, self)
      case None           => R.Null(self.cause).toQuery
    }

  override def getMethod(ctx: FQLInterpCtx, self: Value.Null, name: Name) =
    methods.get(name.str) match {
      case Some(m) => R.Val(Some(m)).toQuery
      case _       => R.Null(self.cause).toQuery
    }

  defAccess(tt.Never)("key" -> tt.Any) { (_, self, _, _) =>
    R.Null(self.cause).toResultQ
  }
}
