package fauna.model.test

import fauna.model.runtime.fql2.{ stdlib, FQLInterpreter, QueryRuntimeFailure }
import fauna.repo.values.Value
import fql.ast.{ Span, Src }

class FQL2MathSpec extends FQL2StdlibHelperSpec("MathModule", stdlib.Math) {
  val auth = newDB

  // Tolerance for floating point equality.
  val tol = 0.01

  def toD(x: Value) = {
    x match {
      case Value.Double(v) => v
      case _               => fail()
    }
  }

  "NaN" - {
    testSig("Number")
  }

  "Infinity" - {
    testSig("Number")
  }

  "E" - {
    testSig("Number")
  }

  "PI" - {
    testSig("Number")
  }

  "abs" - {
    testSig("abs(x: Number) => Number")

    "works" in {
      // Basics.
      checkOk(auth, "Math.abs(-1)", Value.Number(1))
      checkOk(auth, "Math.abs(0)", Value.Number(0))
      checkOk(auth, "Math.abs(1)", Value.Number(1))

      checkOk(auth, "Math.abs(-1.0)", Value.Number(1.0))
      checkOk(auth, "Math.abs(0.0)", Value.Number(0.0))
      checkOk(auth, "Math.abs(1.0)", Value.Number(1.0))

      // Weird cases.
      checkOk(auth, "Math.abs(-2147483648)", Value.Number(-2147483648L))
    }
  }

  "sign" - {
    testSig("sign(x: Number) => Number")

    "works" in {
      checkOk(auth, "Math.sign(-101)", Value.Number(-1))
      checkOk(auth, "Math.sign(0)", Value.Number(0))
      checkOk(auth, "Math.sign(2.98)", Value.Number(1))
    }
  }

  "sqrt" - {
    testSig("sqrt(x: Number) => Number")

    "works" in {
      checkOkApprox(auth, "Math.sqrt(0)", 0.0, tol)
      checkOkApprox(auth, "Math.sqrt(2) * Math.sqrt(2)", 2.0, tol)
    }

    "rejects invalid inputs" in {
      // No negatives allowed.
      checkErr(
        auth,
        "Math.sqrt(-4)",
        QueryRuntimeFailure
          .InvalidArgument(
            "x",
            "x must be nonnegative",
            FQLInterpreter.StackTrace(Seq(Span(9, 13, Src.Query(""))))))
    }
  }

  "sum" - {
    testSig(
      "sum(numbers: Array<Number>) => Number",
      "sum(numbers: Set<Number>) => Number")

    "works" in {
      checkOk(auth, "Math.sum([1, 2, 3, 4, 5])", Value.Number(15))

      checkOk(auth, "Math.sum([1, 2, 3, 4, 5].toSet())", Value.Number(15))
    }
  }

  "mean" - {
    testSig(
      "mean(numbers: Array<Number>) => Number",
      "mean(numbers: Set<Number>) => Number")

    "works" in {
      checkOkApprox(auth, "Math.mean([1, 2, 3, 4])", 2.5, tol)

      checkOk(auth, "Math.sum([1, 2, 3, 4, 5].toSet())", Value.Number(15))
    }
  }

  "ceil" - {
    testSig("ceil(x: Number) => Number")

    "works" in {
      checkOk(auth, "Math.ceil(12.345)", Value.Number(13))
    }
  }

  "floor" - {
    testSig("floor(x: Number) => Number")

    "works" in {
      checkOk(auth, "Math.floor(12.345)", Value.Number(12))
    }
  }

  "round" - {
    testSig("round(x: Number, precision: Number) => Number")

    "works" in {
      checkOk(auth, "Math.round(12.345, 0)", Value.Number(12))
      checkOk(auth, "Math.round(12.345, 2)", Value.Number(12.35))
      checkOk(auth, "Math.round(12.345, 5)", Value.Number(12.345))
      checkOk(auth, "Math.round(12.345, -1)", Value.Number(10))
      checkOk(auth, "Math.round(12.345, -3)", Value.Number(0))
    }
  }

  "trunc" - {
    testSig("trunc(x: Number, precision: Number) => Number")

    "works" in {
      checkOk(auth, "Math.trunc(12.345, 0)", Value.Number(12))
      checkOk(auth, "Math.trunc(12.345, 2)", Value.Number(12.34))
      checkOk(auth, "Math.trunc(12.345, 5)", Value.Number(12.345))
      checkOk(auth, "Math.trunc(12.345, -1)", Value.Number(10))
      checkOk(auth, "Math.trunc(12.345, -3)", Value.Number(0))
    }
  }

  "min" - {
    testSig("min(x: Number, y: Number) => Number")

    "works" in {
      checkOk(auth, "Math.min(1.0, 2)", Value.Number(1.0))
    }
  }

  "max" - {
    testSig("max(x: Number, y: Number) => Number")

    "works" in {
      checkOk(auth, "Math.max(1.0, 2)", Value.Number(2))
    }
  }

  "cos" - {
    testSig("cos(x: Number) => Number")

    "works" in {
      checkOkApprox(auth, "Math.cos(0)", 1.0, tol)
      checkOkApprox(auth, "Math.cos(Math.PI / 2)", 0.0, tol)
      checkOkApprox(auth, "Math.cos(Math.PI)", -1.0, tol)
    }
  }

  "sin" - {
    testSig("sin(x: Number) => Number")

    "works" in {
      checkOkApprox(auth, "Math.sin(0)", 0.0, tol)
      checkOkApprox(auth, "Math.sin(Math.PI / 2)", 1.0, tol)
      checkOkApprox(auth, "Math.sin(Math.PI)", 0.0, tol)
    }
  }

  "tan" - {
    testSig("tan(x: Number) => Number")

    "works" in {
      checkOkApprox(auth, "Math.tan(0)", 0.0, tol)
      checkOkApprox(auth, "Math.tan(Math.PI / 4)", 1.0, tol)
      checkOkApprox(auth, "Math.tan(Math.PI)", 0.0, tol)

      // Tangent blow-up. This agrees with Scala and Javascript.
      val badTan = 16331239353195370.0
      checkOkApprox(auth, "Math.tan(Math.PI / 2)", badTan, tol)
    }
  }

  "acos" - {
    testSig("acos(x: Number) => Number")

    "works" in {
      checkOkApprox(auth, "Math.acos(0)", math.Pi / 2, tol)
      checkOkApprox(auth, "Math.acos(1)", 0.0, tol)
      checkOkApprox(auth, "Math.acos(-1)", math.Pi, tol)

      checkErr(
        auth,
        "Math.acos(2)",
        QueryRuntimeFailure.InvalidArgument(
          "x",
          "argument must lie in [-1, 1]",
          FQLInterpreter.StackTrace(Seq(Span(9, 12, Src.Query(""))))
        ))
    }
  }

  "asin" - {
    testSig("asin(x: Number) => Number")

    "works" in {
      checkOkApprox(auth, "Math.asin(0)", 0.0, tol)
      checkOkApprox(auth, "Math.asin(1)", math.Pi / 2, tol)
      checkOkApprox(auth, "Math.asin(-1)", -1 * math.Pi / 2, tol)
    }

    "rejects invalid inputs" in {
      checkErr(
        auth,
        "Math.asin(-2)",
        QueryRuntimeFailure.InvalidArgument(
          "x",
          "argument must lie in [-1, 1]",
          FQLInterpreter.StackTrace(Seq(Span(9, 13, Src.Query(""))))))
    }
  }

  "atan" - {
    testSig("atan(x: Number) => Number")

    "works" in {
      checkOkApprox(auth, "Math.atan(0)", 0.0, tol)
      checkOkApprox(auth, "Math.atan(1)", math.Pi / 4, tol)
      checkOkApprox(auth, "Math.atan(-1)", -1 * math.Pi / 4, tol)
    }
  }

  "degrees" - {
    testSig("degrees(x: Number) => Number")

    "works" in {
      checkOkApprox(auth, "Math.degrees(Math.PI / 2)", 90.0, tol)
    }
  }

  "radians" - {
    testSig("radians(x: Number) => Number")

    "works" in {
      checkOkApprox(auth, "Math.radians(90)", math.Pi / 2, tol)
    }
  }

  "hypot" - {
    testSig("hypot(x: Number, y: Number) => Number")

    "works" in {
      checkOkApprox(auth, "Math.hypot(3, 4.0)", 5.0, tol)
    }
  }

  "exp" - {
    testSig("exp(x: Number) => Number")

    "works" in {
      checkOkApprox(auth, "Math.exp(0)", 1.0, tol)
      checkOkApprox(auth, "Math.exp(1)", math.E, tol)
      checkOkApprox(auth, "Math.exp(-1)", 1 / math.E, tol)
    }
  }

  "pow" - {
    testSig("pow(x: Number, power: Number) => Number")

    "works" in {
      checkOkApprox(auth, "Math.pow(1, 10)", 1.0, tol)
      checkOkApprox(auth, "Math.pow(2, 5)", 32.0, tol)
      checkOkApprox(auth, "Math.pow(-3, 3)", -27.0, tol)
    }
  }

  "log" - {
    testSig("log(x: Number) => Number")

    "works" in {
      checkOkApprox(auth, "Math.log(1)", 0.0, tol)
      checkOkApprox(auth, "Math.log(Math.E)", 1.0, tol)

      checkOkApprox(auth, "Math.log(Math.exp(2))", 2.0, tol)
      checkOkApprox(auth, "Math.pow(100, Math.log10(8))", 64.0, tol)
    }

    "rejects invalid inputs" in {
      checkErr(
        auth,
        "Math.log(0)",
        QueryRuntimeFailure.InvalidArgument(
          "x",
          "x must be positive",
          FQLInterpreter.StackTrace(Seq(Span(8, 11, Src.Query(""))))))
      checkErr(
        auth,
        "Math.log(-1)",
        QueryRuntimeFailure.InvalidArgument(
          "x",
          "x must be positive",
          FQLInterpreter.StackTrace(Seq(Span(8, 12, Src.Query(""))))))
    }
  }

  "log10" - {
    testSig("log10(x: Number) => Number")

    "works" in {
      checkOkApprox(auth, "Math.log10(1)", 0.0, tol)
      checkOkApprox(auth, "Math.log10(10)", 1.0, tol)
    }
  }

  val cosh1 = (math.E + 1 / Math.E) / 2
  val sinh1 = (math.E - 1 / Math.E) / 2

  "cosh" - {
    testSig("cosh(x: Number) => Number")

    "works" in {
      checkOkApprox(auth, "Math.cosh(1)", cosh1, tol)
    }
  }

  "sinh" - {
    testSig("sinh(x: Number) => Number")

    "works" in {
      checkOkApprox(auth, "Math.sinh(1)", sinh1, tol)
    }
  }

  "tanh" - {
    testSig("tanh(x: Number) => Number")

    "works" in {
      checkOkApprox(auth, "Math.tanh(1)", sinh1 / cosh1, tol)
    }
  }
}
