package fauna.model.test

import fauna.model.runtime.fql2.{ FQLInterpreter, QueryRuntimeFailure }
import fauna.repo.values.Value
import fql.ast.{ Span, Src }

class FQL2BooleanSpec extends FQL2Spec {

  "&&" - {
    "types as boolean operator" in {
      val auth = newDB
      evalRes(
        auth,
        "(a, b) => a && b").typeStr shouldEqual "(Boolean, Boolean) => Boolean"
    }

    "short-circuits" in {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: \"Thing\" })")

      evalOk(auth, "false && { Thing.create({}); true }") shouldEqual Value.False

      evalOk(auth, "Thing.all().first()") shouldEqual Value.Null(Span.Null)
    }

    "checks runtime args" in {
      val auth = newDB
      evalErr(auth, "null && true", typecheck = false) shouldEqual
        QueryRuntimeFailure(
          "type_mismatch",
          "expected type: Boolean, received Null",
          FQLInterpreter.StackTrace(List(Span(0, 4, Src.Query("")))))

      evalErr(auth, "true && null", typecheck = false) shouldEqual
        QueryRuntimeFailure(
          "type_mismatch",
          "expected type: Boolean, received Null",
          FQLInterpreter.StackTrace(List(Span(8, 12, Src.Query("")))))
    }
  }

  "||" - {
    "types as boolean operator" in {
      val auth = newDB
      evalRes(
        auth,
        "(a, b) => a || b").typeStr shouldEqual "(Boolean, Boolean) => Boolean"
    }

    "short-circuits" in {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: \"Thing\" })")

      evalOk(auth, "true || { Thing.create({}); false }") shouldEqual Value.True

      evalOk(auth, "Thing.all().first()") shouldEqual Value.Null(Span.Null)
    }

    "checks runtime args" in {
      val auth = newDB
      evalErr(auth, "null || false", typecheck = false) shouldEqual
        QueryRuntimeFailure(
          "type_mismatch",
          "expected type: Boolean, received Null",
          FQLInterpreter.StackTrace(List(Span(0, 4, Src.Query("")))))

      evalErr(auth, "false || null", typecheck = false) shouldEqual
        QueryRuntimeFailure(
          "type_mismatch",
          "expected type: Boolean, received Null",
          FQLInterpreter.StackTrace(List(Span(9, 13, Src.Query("")))))
    }
  }
}
