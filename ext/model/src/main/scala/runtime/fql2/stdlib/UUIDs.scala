package fauna.model.runtime.fql2.stdlib

import fauna.model.runtime.fql2._
import fauna.repo.values.Value

object UUIDCompanion extends CompanionObject("UUID") {

  def contains(v: Value): Boolean = v match {
    case _: Value.UUID => true
    case _             => false
  }
}

object UUIDPrototype extends Prototype(TypeTag.UUID, isPersistable = true)
