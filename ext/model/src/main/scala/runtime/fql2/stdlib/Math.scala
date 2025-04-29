package fauna.model.runtime.fql2.stdlib

import fauna.ast.{ RoundFunction, TruncFunction }
import fauna.lang.syntax._
import fauna.model.runtime.fql2._
import fauna.repo.query.Query
import fauna.repo.values.Value

object Math extends ModuleObject("Math") with NumberHelpers {

  defField("NaN" -> tt.Number) { (_, _) =>
    Query.value(Value.Number(Double.NaN))
  }

  defField("Infinity" -> tt.Number) { (_, _) =>
    Query.value(Value.Number(Double.PositiveInfinity))
  }

  defField("E" -> tt.Number) { (_, _) =>
    Query.value(Value.Number(math.E))
  }

  defField("PI" -> tt.Number) { (_, _) =>
    Query.value(Value.Number(math.Pi))
  }

  // Returns the absolute value of x.
  defStaticFunction("abs" -> tt.Number)("x" -> tt.Number) { (_, x) =>
    val res = x match {
      case Value.Int(x)    => Value.Int(if (x < 0) -x else x)
      case Value.Long(x)   => Value.Long(if (x < 0) -x else x)
      case Value.Double(x) => Value.Double(x.abs)
    }
    res.toQuery
  }

  // Given x in [-1, 1], returns the unique value t in [0, PI] such that cos t = x.
  defStaticFunction("acos" -> tt.Number)("x" -> tt.Number) { (ctx, x) =>
    val v = x.toDouble
    if (v < -1 || v > 1) {
      QueryRuntimeFailure
        .InvalidArgument(
          "x",
          "argument must lie in [-1, 1]",
          ctx.stackTrace
        )
        .toQuery
    } else {
      Value.Number(math.acos(v)).toQuery
    }
  }

  // Given x in [-1, 1], returns the unique value t in [-PI/2, PI/2] such that sin t
  // = x.
  defStaticFunction("asin" -> tt.Number)("x" -> tt.Number) { (ctx, x) =>
    val v = x.toDouble
    if (v < -1 || v > 1) {
      QueryRuntimeFailure
        .InvalidArgument(
          "x",
          "argument must lie in [-1, 1]",
          ctx.stackTrace
        )
        .toQuery
    } else {
      Value.Number(math.asin(v)).toQuery
    }
  }

  // Given x, returns the unique t in [-PI/2, PI/2] such that tan t = x.
  defStaticFunction("atan" -> tt.Number)("x" -> tt.Number) { (_, x) =>
    Value.Number(math.atan(x.toDouble)).toQuery
  }

  // Returns the smallest integer >= x.
  defStaticFunction("ceil" -> tt.Number)("x" -> tt.Number) { (_, x) =>
    {
      val res = x match {
        case v: Value.Int    => v
        case v: Value.Long   => v
        case Value.Double(v) => Value.Number(math.ceil(v))
      }
      res.toQuery
    }
  }

  // Returns the cosine of x.
  defStaticFunction("cos" -> tt.Number)("x" -> tt.Number) { (_, x) =>
    Value.Number(math.cos(x.toDouble)).toQuery
  }

  // Returns the hyperbolic cosine of x.
  defStaticFunction("cosh" -> tt.Number)("x" -> tt.Number) { (_, x) =>
    Value.Number(math.cosh(x.toDouble)).toQuery
  }

  // Returns the angle in degrees equal to the angle x measured in radians.
  defStaticFunction("degrees" -> tt.Number)("x" -> tt.Number) { (_, x) =>
    Value.Number(java.lang.Math.toDegrees(x.toDouble)).toQuery
  }

  // Returns e^x.
  defStaticFunction("exp" -> tt.Number)("x" -> tt.Number) { (_, x) =>
    Value.Number(math.exp(x.toDouble)).toQuery
  }

  // Returns the largest integer <= x.
  defStaticFunction("floor" -> tt.Number)("x" -> tt.Number) { (_, x) =>
    (x match {
      case v: Value.Int    => v
      case v: Value.Long   => v
      case Value.Double(d) => Value.Number(math.floor(d))
    }).toQuery
  }

  // Returns the length of the hypotenuse of a triangle with legs of length x and y.
  defStaticFunction("hypot" -> tt.Number)("x" -> tt.Number, "y" -> tt.Number) {
    (_, x, y) => Value.Number(math.hypot(x.toDouble, y.toDouble)).toQuery
  }

  // Returns the natural logarithm of x, for x > 0.
  defStaticFunction("log" -> tt.Number)("x" -> tt.Number) { (ctx, x) =>
    if (x.compareNumbers(Value.Number(0)) > 0) {
      Value.Number(math.log(x.toDouble)).toQuery
    } else {
      QueryRuntimeFailure
        .InvalidArgument("x", "x must be positive", ctx.stackTrace)
        .toQuery
    }
  }

  // Returns the base-10 logarithm of x, for x > 0.
  defStaticFunction("log10" -> tt.Number)("x" -> tt.Number) { (ctx, x) =>
    if (x.compareNumbers(Value.Number(0)) > 0) {
      Value.Number(math.log10(x.toDouble)).toQuery
    } else {
      QueryRuntimeFailure
        .InvalidArgument("x", "x must be positive", ctx.stackTrace)
        .toQuery
    }
  }

  // Returns the larger of the two numbers.
  defStaticFunction("max" -> tt.Number)("x" -> tt.Number, "y" -> tt.Number) {
    (_, x, y) =>
      val res = if (x.compareNumbers(y) < 0) y else x
      res.toQuery
  }

  // Returns the mean of an array of numbers.
  defStaticFunction("mean" -> tt.Number)("numbers" -> tt.Array(tt.Number)) {
    case (ctx, Value.Array(vs)) =>
      Result.guardM {
        val sum = vs.foldLeft(Value.Number(0)) {
          case (acc, n: Value.Number) => Math.add(acc, n)
          case _ =>
            Result.fail(
              QueryRuntimeFailure.InvalidArgument(
                "numbers",
                "all array elements must be numbers",
                ctx.stackTrace
              ))
        }
        divide(sum, Value.Number(vs.size.toDouble), ctx.stackTrace)
      }
  }

  // Returns the mean of a set of numbers, eagerly evaluating the set.
  defStaticFunction("mean" -> tt.Number)("numbers" -> tt.Set(tt.Number)) {
    case (ctx, vs: Value.Set) =>
      Result.guardM {
        val seed = Value.Struct("count" -> Value.Number(0), "sum" -> Value.Number(0))
        vs.foldLeft(ctx, seed) {
          case (acc: Value.Struct.Full, n: Value.Number) =>
            // This'll be kinda slow. Improve if needed.
            val count = Math.add(
              acc.fields("count").asInstanceOf[Value.Number],
              Value.Number(1))
            val sum = Math.add(acc.fields("sum").asInstanceOf[Value.Number], n)
            Value.Struct("count" -> count, "sum" -> sum).toQuery
          case _ =>
            Result.fail(
              QueryRuntimeFailure.InvalidArgument(
                "numbers",
                "all array elements must be numbers",
                ctx.stackTrace
              ))
        }.flatMapT { v =>
          val res = v.asInstanceOf[Value.Struct.Full]
          divide(
            res.fields("sum").asInstanceOf[Value.Number],
            res.fields("count").asInstanceOf[Value.Number],
            ctx.stackTrace)
        }
      }
  }

  // Returns the smaller of the two numbers.
  defStaticFunction("min" -> tt.Number)("x" -> tt.Number, "y" -> tt.Number) {
    (_, x, y) =>
      val res = if (x.compareNumbers(y) <= 0) x else y
      res.toQuery
  }

  // Returns x^power.
  defStaticFunction("pow" -> tt.Number)("x" -> tt.Number, "power" -> tt.Number) {
    (_, x, y) => Value.Number(math.pow(x.toDouble, y.toDouble)).toQuery
  }

  // Returns the angle in degrees equal to the angle x measured in radians.
  defStaticFunction("radians" -> tt.Number)("x" -> tt.Number) { (_, x) =>
    Value.Number(math.toRadians(x.toDouble)).toQuery
  }

  // Returns x rounded to the given precision. The precision identifies the decimal
  // place to which x is rounded, with 0 set as the units place, counting forward
  // past the decimal point and backward to the tens, hundreds, etc. places.
  defStaticFunction("round" -> tt.Number)("x" -> tt.Number, "precision" -> tt.Int) {
    case (_, x, Value.Int(p)) =>
      val res = x match {
        case Value.Int(v)    => Value.Long(RoundFunction.roundOff(v, p))
        case Value.Long(v)   => Value.Long(RoundFunction.roundOff(v, p))
        case Value.Double(v) => Value.Double(RoundFunction.roundOff(v, p))
      }
      res.toQuery
  }

  // Returns x/|x|, or 0 if x is 0.
  defStaticFunction("sign" -> tt.Int)("x" -> tt.Number) { (_, x) =>
    Value.Int(x.compareNumbers(Value.Int(0))).toQuery
  }

  // Returns the sine of x.
  defStaticFunction("sin" -> tt.Number)("x" -> tt.Number) { (_, x) =>
    Value.Number(math.sin(x.toDouble)).toQuery
  }

  // Returns the hyperbolic cosine of x.
  defStaticFunction("sinh" -> tt.Number)("x" -> tt.Number) { (_, x) =>
    Value.Number(math.sinh(x.toDouble)).toQuery
  }

  // Returns the (positive) square root of x.
  defStaticFunction("sqrt" -> tt.Number)("x" -> tt.Number) { (ctx, x) =>
    if (x.compareNumbers(Value.Number(0)) < 0) {
      QueryRuntimeFailure
        .InvalidArgument("x", "x must be nonnegative", ctx.stackTrace)
        .toQuery
    } else {
      Value.Number(math.sqrt(x.toDouble)).toQuery
    }
  }

  // Returns the sum of an array of numbers.
  defStaticFunction("sum" -> tt.Number)("numbers" -> tt.Array(tt.Number)) {
    case (ctx, Value.Array(vs)) =>
      Result.guardM {
        val sum = vs.foldLeft(Value.Number(0)) {
          case (acc, n: Value.Number) => Math.add(acc, n)
          case _ =>
            Result.fail(
              QueryRuntimeFailure.InvalidArgument(
                "numbers",
                "all array elements must be numbers",
                ctx.stackTrace
              ))
        }
        sum.toQuery
      }
  }

  // Returns the sum of a set of numbers, eagerly evaluating the set.
  defStaticFunction("sum" -> tt.Number)("numbers" -> tt.Set(tt.Number)) {
    case (ctx, vs: Value.Set) =>
      Result.guardM {
        vs.foldLeft(ctx, Value.Number(0)) {
          case (acc: Value.Number, n: Value.Number) => Math.add(acc, n).toQuery
          case _ =>
            Result.fail(
              QueryRuntimeFailure.InvalidArgument(
                "numbers",
                "all array elements must be numbers",
                ctx.stackTrace
              ))
        }.mapT { _.asInstanceOf[Value.Number] }
      }
  }

  // Tan.
  defStaticFunction("tan" -> tt.Number)("x" -> tt.Number) { (_, x) =>
    Value.Number(math.tan(x.toDouble)).toQuery
  }

  // Tanh.
  defStaticFunction("tanh" -> tt.Number)("x" -> tt.Number) { (_, x) =>
    Value.Number(math.tanh(x.toDouble)).toQuery
  }

  // Works like round, but always returns a number <= x.
  defStaticFunction("trunc" -> tt.Number)("x" -> tt.Number, "precision" -> tt.Int) {
    case (_, x, Value.Int(p)) =>
      val res = x match {
        case Value.Int(v)    => Value.Long(TruncFunction.trunc(v, p))
        case Value.Long(v)   => Value.Long(TruncFunction.trunc(v, p))
        case Value.Double(v) => Value.Double(TruncFunction.truncDouble(v, p))
      }
      res.toQuery
  }
}
