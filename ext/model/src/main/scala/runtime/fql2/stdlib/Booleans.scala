package fauna.model.runtime.fql2.stdlib

import fauna.model.runtime.fql2._
import fauna.repo.values._

object BooleanCompanion extends CompanionObject("Boolean") {

  def contains(v: Value): Boolean = v match {
    case _: Value.Boolean => true
    case _                => false
  }
}

object BooleanPrototype extends Prototype(TypeTag.Boolean, isPersistable = true) {
  defOp("||" -> tt.Boolean)("other" -> tt.Boolean) { (_, self, other) =>
    Value.Boolean(self.value || other.value).toQuery
  }

  defOp("&&" -> tt.Boolean)("other" -> tt.Boolean) { (_, self, other) =>
    Value.Boolean(self.value && other.value).toQuery
  }

  defOp("!" -> tt.Boolean)() { (_, self) =>
    Value.Boolean(!self.value).toQuery
  }
}
