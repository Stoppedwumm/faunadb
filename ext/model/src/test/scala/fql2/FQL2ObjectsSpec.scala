package fauna.model.test

import fauna.repo.values.Value

class FQL2ObjectsSpec extends FQL2Spec {
  "FQL2Objects" - {
    val auth = newDB

    "literals" - {
      "can create an object with an empty field name" in {
        evalOk(auth, "let x = { \"\": 0 }; x") shouldEqual
          Value.Struct("" -> Value.Int(0))
      }
    }
  }
}
