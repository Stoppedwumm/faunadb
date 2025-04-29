package fauna.model.runtime.fql2.stdlib

import fauna.lang.syntax._
import fauna.model.runtime.fql2._
import fauna.repo.values.Value

object EventSourceCompanion extends CompanionObject("EventSource") {

  def contains(v: Value): Boolean = v match {
    case _: Value.EventSource => true
    case _                    => false
  }
}

object EventSourcePrototype
    extends Prototype(TypeTag.EventSource(TypeTag.A), isPersistable = false) {

  defMethod("where" -> tt.EventSource(tt.A))("predicate" -> tt.Predicate(tt.A)) {
    (ctx, self, pred) =>
      SetPrototype.whereImpl(ctx, self.set, pred) mapT { set0 =>
        self.copy(set = set0)
      }
  }

  defMethod("map" -> tt.EventSource(tt.B))(
    "mapper" -> tt.Function(Seq(tt.A), tt.B)) { (ctx, self, func) =>
    SetPrototype.mapImpl(ctx, self.set, func) mapT { set0 =>
      self.copy(set = set0)
    }
  }
}
