package fauna.model.runtime.fql2.stdlib

import fauna.model.runtime.fql2._
import fauna.repo.values.Value

object BytesCompanion extends CompanionObject("Bytes") {

  def contains(v: Value): Boolean = v match {
    case _: Value.Bytes => true
    case _              => false
  }

  defApply(tt.Bytes)("encoded" -> tt.Str) { (_, encoded) =>
    Value.Bytes.fromBase64(encoded.value).toQuery
  }

  defStaticFunction("fromBase64" -> tt.Bytes)("encoded" -> tt.Str) { (_, encoded) =>
    Value.Bytes.fromBase64(encoded.value).toQuery
  }
}

object BytesPrototype extends Prototype(TypeTag.Bytes, isPersistable = true) {
  defMethod("toBase64" -> tt.Str)() { (_, self) =>
    Value.Str(self.toBase64.toString).toQuery
  }
}
