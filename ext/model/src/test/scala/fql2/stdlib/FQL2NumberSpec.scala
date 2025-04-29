package fauna.model.test

import fauna.model.runtime.fql2.{ FQLInterpreter, QueryRuntimeFailure }
import fauna.repo.values.Value
import fql.ast.{ Span, Src }

class FQL2NumberSpec extends FQL2Spec {
  val auth = newDB

  "unary -" in {
    evalOk(auth, "-5") shouldBe Value.Number(-5)
    evalOk(auth, "-(5)") shouldBe Value.Number(-5)
    evalOk(auth, "-(-5)") shouldBe Value.Number(5)
    evalOk(auth, "let a = 5; -a") shouldBe Value.Number(-5)
    // weird, but valid
    evalOk(auth, "--5") shouldBe Value.Number(5)
    evalOk(auth, "---5") shouldBe Value.Number(-5)
  }

  "bitwise ops" - {
    "work" in {
      // Standard.
      evalOk(auth, "(1 + 2 + 4) & (1 + 4)") shouldBe Value.Number(1 + 4)
      evalOk(auth, "(1 + 2 + 4) | (1 + 4)") shouldBe Value.Number(1 + 2 + 4)
      evalOk(auth, "(1 + 2 + 4) ^ (1 + 4)") shouldBe Value.Number(2)
      evalOk(auth, "~(1 + 2 + 4)") shouldBe Value.Number(-8)

      // Double truncation.
      evalOk(auth, "(1 + 2 + 4.1) & (1.2 + 4)") shouldBe Value.Number(1 + 4)
      evalOk(auth, "(1.4 + 2 + 4) | (1 + 4.2)") shouldBe Value.Number(1 + 2 + 4)
      evalOk(auth, "(1 + 2.5 + 4) ^ (1.1 + 4)") shouldBe Value.Number(2)
      evalOk(auth, "~(1.3 + 2 + 4.2)") shouldBe Value.Number(-8)
    }
  }

  "modulo operation" - {
    "works" in {
      // Standard.
      evalOk(auth, "1 % 2") shouldBe Value.Number(1)
      evalOk(auth, "2 % 1") shouldBe Value.Number(0)
      evalOk(auth, "3 % 4") shouldBe Value.Number(3)
      evalOk(auth, "4 % 3") shouldBe Value.Number(1)

      // Doubles.
      evalOk(auth, "0.5 % 0.25") shouldBe Value.Number(0)
    }

    def isNaN(v: Value) = v match {
      case Value.Double(d) => d.isNaN()
      case _               => false
    }

    "rejects zero modulus for integers and returns NaNs for doubles" in {
      evalErr(auth, "1 % 0") shouldBe (QueryRuntimeFailure.DivideByZero(
        FQLInterpreter.StackTrace(Seq(Span(4, 5, Src.Query(""))))))
      isNaN(evalOk(auth, "1 % 0.0")) shouldBe (true)
      isNaN(evalOk(auth, "1.0 % 0")) shouldBe (true)
      isNaN(evalOk(auth, "1.0 % 0.0")) shouldBe (true)
    }
  }

  "exponentiation operation" - {
    "works" in {
      evalOk(auth, "2 ** 2") shouldBe Value.Int(4)
      evalOk(auth, "2 ** 1") shouldBe Value.Int(2)
      evalOk(auth, "2 ** 0") shouldBe Value.Int(1)
      evalOk(auth, s"2 ** ${Int.MaxValue - 1}") shouldBe (Value.Double(
        Double.PositiveInfinity))
    }

    "is right associative" in {
      // this evals to 64 if ** is left associative
      evalOk(auth, "2 ** 3 ** 2") shouldBe Value.Int(512)
    }
  }
}
