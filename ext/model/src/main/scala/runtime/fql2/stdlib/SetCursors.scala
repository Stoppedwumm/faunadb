package fauna.model.runtime.fql2.stdlib

import fauna.model.runtime.fql2._
import fauna.repo.values.Value

object SetCursorCompanion extends CompanionObject("SetCursor") {

  def contains(v: Value): Boolean = v match {
    case _: Value.SetCursor => true
    case _                  => false
  }
}

object SetCursorPrototype extends Prototype(TypeTag.SetCursor, isPersistable = false)
