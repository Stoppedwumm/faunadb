package fauna.model.runtime.fql2.stdlib

import fauna.model.runtime.fql2._
import fauna.repo.values.Value

object TransactionTimeCompanion extends CompanionObject("TransactionTime") {

  def contains(v: Value): Boolean = v match {
    case Value.TransactionTime => true
    case _                     => false
  }

  defApply(tt.TransactionTime)() { (_) =>
    Value.TransactionTime.toQuery
  }
}

object TransactionTimePrototype
    extends Prototype(TypeTag.TransactionTime, isPersistable = false)
