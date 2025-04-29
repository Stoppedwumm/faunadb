package fauna.model.test

import fauna.model.runtime.fql2.{ stdlib, FQLInterpreter, QueryRuntimeFailure }
import fauna.model.runtime.Effect
import fauna.repo.values.Value
import fql.ast.{ Span, Src }

class FQL2IDSpec extends FQL2StdlibHelperSpec("ID", stdlib.IDPrototype) {
  val auth = newDB

  "apply" - {
    "can construct from numbers and strings" in {
      checkOk(auth, "ID(123)", Value.ID(123))
      checkOk(auth, "ID('123')", Value.ID(123))
    }

    "rejects invalid strings" in {
      checkErr(
        auth,
        "ID('foo')",
        QueryRuntimeFailure.InvalidID(
          "\"foo\"",
          FQLInterpreter.StackTrace(Seq(Span(2, 9, Src.Query(""))))))
    }

    "rejects doubles" in {
      checkErr(
        auth,
        "ID(12.3)",
        QueryRuntimeFailure.InvalidID(
          "12.3",
          FQLInterpreter.StackTrace(Seq(Span(2, 8, Src.Query(""))))))

      checkErr(
        auth,
        "ID(12333333333333333333333333333333333333333333333333)",
        QueryRuntimeFailure.InvalidID(
          "1.2333333333333334E49",
          FQLInterpreter.StackTrace(Seq(Span(2, 54, Src.Query("")))))
      )
    }
  }

  "toString" - {
    testSig("toString() => String")

    "works" in {
      checkOk(auth, "ID(123).toString()", Value.Str("123"))
    }
  }

  "comparison" - {
    "compares with numbers" in {
      checkOk(auth, "ID(123) == 123", Value.True)
      checkOk(auth, "ID(123) < 124", Value.True)
      checkOk(auth, "ID(123) < 122", Value.False)
    }

    "compares with strings" in {
      checkOk(auth, "ID(123) == '123'", Value.True)
      checkOk(auth, "ID(123) < '124'", Value.True)
      checkOk(auth, "ID(123) < '122'", Value.False)
    }
  }

  "newId()" - {
    "counts as a read" in {
      evalOk(auth, "newId()", effect = Effect.Read)
      val err = evalErr(auth, "newId()", effect = Effect.Pure)

      err.code shouldEqual "invalid_effect"
      err.errors.head.message shouldEqual "`newId` performs an observation, which is not allowed in model tests."
    }
  }
}
