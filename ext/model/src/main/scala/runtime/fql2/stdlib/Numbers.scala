package fauna.model.runtime.fql2.stdlib

import fauna.model.runtime.fql2._
import fauna.repo.values._
import scala.math._

object NumberCompanion extends CompanionObject("Number") {

  def contains(v: Value): Boolean = v match {
    case _: Value.Number => true
    case _               => false
  }
}
object NumberPrototype extends BaseNumberPrototype(TypeTag.Number)

object IntCompanion extends CompanionObject("Int") {

  def contains(v: Value): Boolean = v match {
    case _: Value.Int => true
    case _            => false
  }
}
object IntPrototype extends BaseNumberPrototype(TypeTag.Int)

object LongCompanion extends CompanionObject("Long") {

  def contains(v: Value): Boolean = v match {
    case _: Value.Long => true
    case _             => false
  }
}
object LongPrototype extends BaseNumberPrototype(TypeTag.Long)

object DoubleCompanion extends CompanionObject("Double") {

  def contains(v: Value): Boolean = v match {
    case _: Value.Double => true
    case _               => false
  }
}
object DoublePrototype extends BaseNumberPrototype(TypeTag.Double)

object FloatCompanion extends CompanionObject("Float") {
  def contains(v: Value): Boolean = v match {
    case _: Value.Double => true
    case _               => false
  }
}
object FloatPrototype extends BaseNumberPrototype(TypeTag.Float)

trait NumberHelpers {
  protected def addIntegers(v1: Long, v2: Long) = try {
    Value.Number(addExact(v1, v2))
  } catch {
    // Fall back to Double arithmetic if there is an integer overflow.
    case (_: ArithmeticException) => Value.Number(v1.toDouble + v2.toDouble)
  }

  protected def add(a: Value.Number, b: Value.Number) =
    (a, b) match {
      case (Value.Int(x), Value.Int(y))       => addIntegers(x, y)
      case (Value.Int(x), Value.Long(y))      => addIntegers(x, y)
      case (Value.Int(x), Value.Double(y))    => Value.Number(x + y)
      case (Value.Long(x), Value.Int(y))      => addIntegers(x, y)
      case (Value.Long(x), Value.Long(y))     => addIntegers(x, y)
      case (Value.Long(x), Value.Double(y))   => Value.Number(x + y)
      case (Value.Double(x), Value.Int(y))    => Value.Number(x + y)
      case (Value.Double(x), Value.Long(y))   => Value.Number(x + y)
      case (Value.Double(x), Value.Double(y)) => Value.Number(x + y)
    }

  protected def subtractIntegers(v1: Long, v2: Long) = try {
    Value.Number(subtractExact(v1, v2))
  } catch {
    // Fall back to Double arithmetic if there is an integer underflow.
    case (_: ArithmeticException) => Value.Number(v1.toDouble - v2.toDouble)
  }

  protected def subtract(a: Value.Number, b: Value.Number) =
    (a, b) match {
      case (Value.Int(x), Value.Int(y))       => subtractIntegers(x, y)
      case (Value.Int(x), Value.Long(y))      => subtractIntegers(x, y)
      case (Value.Int(x), Value.Double(y))    => Value.Number(x - y)
      case (Value.Long(x), Value.Int(y))      => subtractIntegers(x, y)
      case (Value.Long(x), Value.Long(y))     => subtractIntegers(x, y)
      case (Value.Long(x), Value.Double(y))   => Value.Number(x - y)
      case (Value.Double(x), Value.Int(y))    => Value.Number(x - y)
      case (Value.Double(x), Value.Long(y))   => Value.Number(x - y)
      case (Value.Double(x), Value.Double(y)) => Value.Number(x - y)
    }

  protected def multiplyIntegers(v1: Long, v2: Long) = try {
    Value.Number(multiplyExact(v1, v2))
  } catch {
    // Fall back to Double arithmetic if there is an integer overflow.
    case (_: ArithmeticException) => Value.Number(v1.toDouble * v2.toDouble)
  }

  protected def multiply(a: Value.Number, b: Value.Number) =
    (a, b) match {
      case (Value.Int(x), Value.Int(y))       => multiplyIntegers(x, y)
      case (Value.Int(x), Value.Long(y))      => multiplyIntegers(x, y)
      case (Value.Int(x), Value.Double(y))    => Value.Number(x * y)
      case (Value.Long(x), Value.Int(y))      => multiplyIntegers(x, y)
      case (Value.Long(x), Value.Long(y))     => multiplyIntegers(x, y)
      case (Value.Long(x), Value.Double(y))   => Value.Number(x * y)
      case (Value.Double(x), Value.Int(y))    => Value.Number(x * y)
      case (Value.Double(x), Value.Long(y))   => Value.Number(x * y)
      case (Value.Double(x), Value.Double(y)) => Value.Number(x * y)
    }

  protected def divideIntegers(
    v1: Long,
    v2: Long,
    stackTrace: FQLInterpreter.StackTrace) =
    if (v2 == 0) {
      QueryRuntimeFailure.DivideByZero(stackTrace).toQuery
    } else {
      Value.Number(v1 / v2).toQuery
    }

  protected def divide(
    a: Value.Number,
    b: Value.Number,
    stackTrace: FQLInterpreter.StackTrace) =
    (a, b) match {
      case (Value.Int(x), Value.Int(y))       => divideIntegers(x, y, stackTrace)
      case (Value.Int(x), Value.Long(y))      => divideIntegers(x, y, stackTrace)
      case (Value.Int(x), Value.Double(y))    => Value.Number(x / y).toQuery
      case (Value.Long(x), Value.Int(y))      => divideIntegers(x, y, stackTrace)
      case (Value.Long(x), Value.Long(y))     => divideIntegers(x, y, stackTrace)
      case (Value.Long(x), Value.Double(y))   => Value.Number(x / y).toQuery
      case (Value.Double(x), Value.Int(y))    => Value.Number(x / y).toQuery
      case (Value.Double(x), Value.Long(y))   => Value.Number(x / y).toQuery
      case (Value.Double(x), Value.Double(y)) => Value.Number(x / y).toQuery
    }

  protected def modulusIntegers(
    v1: Long,
    v2: Long,
    stackTrace: FQLInterpreter.StackTrace) =
    if (v2 == 0) {
      QueryRuntimeFailure.DivideByZero(stackTrace).toQuery
    } else {
      Value.Number(v1 % v2).toQuery
    }

  protected def modulus(
    a: Value.Number,
    b: Value.Number,
    stackTrace: FQLInterpreter.StackTrace) =
    (a, b) match {
      case (Value.Int(x), Value.Int(y))       => modulusIntegers(x, y, stackTrace)
      case (Value.Int(x), Value.Long(y))      => modulusIntegers(x, y, stackTrace)
      case (Value.Int(x), Value.Double(y))    => Value.Number(x % y).toQuery
      case (Value.Long(x), Value.Int(y))      => modulusIntegers(x, y, stackTrace)
      case (Value.Long(x), Value.Long(y))     => modulusIntegers(x, y, stackTrace)
      case (Value.Long(x), Value.Double(y))   => Value.Number(x % y).toQuery
      case (Value.Double(x), Value.Int(y))    => Value.Number(x % y).toQuery
      case (Value.Double(x), Value.Long(y))   => Value.Number(x % y).toQuery
      case (Value.Double(x), Value.Double(y)) => Value.Number(x % y).toQuery
    }

  protected def expIntegers(x: Long, y: Long) =
    x match {
      case _ if y == 0 => Value.Int(1)
      case 1           => Value.Int(1)
      case -1          => Value.Int(if (y % 2 == 0) 1 else -1)
      case 0 if y > 0  => Value.Int(0)
      case x if y > 0 && y <= Int.MaxValue - 1 =>
        Value.Number(BigInt(x).pow(y.toInt))
      case _ => Value.Number(math.pow(x.toDouble, y.toDouble))
    }

  protected def exponent(a: Value.Number, b: Value.Number) =
    (a, b) match {
      case (Value.Int(x), Value.Int(y))     => expIntegers(x, y)
      case (Value.Int(x), Value.Long(y))    => expIntegers(x, y)
      case (Value.Int(x), Value.Double(y))  => Value.Number(math.pow(x.toDouble, y))
      case (Value.Long(x), Value.Int(y))    => expIntegers(x, y)
      case (Value.Long(x), Value.Long(y))   => expIntegers(x, y)
      case (Value.Long(x), Value.Double(y)) => Value.Number(math.pow(x.toDouble, y))
      case (Value.Double(x), Value.Int(y))  => Value.Number(math.pow(x, y.toDouble))
      case (Value.Double(x), Value.Long(y)) => Value.Number(math.pow(x, y.toDouble))
      case (Value.Double(x), Value.Double(y)) => Value.Number(math.pow(x, y))
    }
}

sealed class BaseNumberPrototype[V <: Value.Number](selfType: TypeTag[V])
    extends Prototype(selfType, isPersistable = true)
    with NumberHelpers {

  defOp("+" -> tt.Number)("other" -> tt.Number) { (_, self, other) =>
    add(self, other).toQuery
  }

  defOp("-" -> tt.Number)("other" -> tt.Number) { (_, self, other) =>
    subtract(self, other).toQuery
  }

  // unary - operator
  defOp("-" -> tt.Number)() { (_, self: Value.Number) =>
    self match {
      case Value.Int(x)    => Value.Int(-x).toQuery
      case Value.Long(x)   => Value.Long(-x).toQuery
      case Value.Double(x) => Value.Double(-x).toQuery
    }
  }

  defOp("*" -> tt.Number)("other" -> tt.Number) { (_, self, other) =>
    multiply(self, other).toQuery
  }

  defOp("/" -> tt.Number)("other" -> tt.Number) { (ctx, self, other) =>
    divide(self, other, ctx.stackTrace)
  }

  defOp("%" -> tt.Number)("other" -> tt.Number) { (ctx, self, other) =>
    modulus(self, other, ctx.stackTrace)
  }

  defOp("**" -> tt.Number)("exponent" -> tt.Number) { (_, self, exp) =>
    exponent(self, exp).toQuery
  }

  private def toLong(v: Value.Number): Long = v match {
    case Value.Int(x)    => x
    case Value.Long(x)   => x
    case Value.Double(x) => x.toLong
  }

  defOp("&" -> tt.Number)("other" -> tt.Number) { (_, self, other) =>
    Value.Number(toLong(self) & toLong(other)).toQuery
  }

  defOp("|" -> tt.Number)("other" -> tt.Number) { (_, self, other) =>
    Value.Number(toLong(self) | toLong(other)).toQuery
  }

  defOp("^" -> tt.Number)("other" -> tt.Number) { (_, self, other) =>
    Value.Number(toLong(self) ^ toLong(other)).toQuery
  }

  defOp("~" -> tt.Number)() { (_, self: Value.Number) =>
    self match {
      case Value.Int(x)    => Value.Number(~x).toQuery
      case Value.Long(x)   => Value.Number(~x).toQuery
      case Value.Double(x) => Value.Number(~(x.toLong)).toQuery
    }
  }
}
