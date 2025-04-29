package fauna.model.test

import fauna.atoms.DocID
import fauna.lang.Timestamp
import fauna.model.runtime.fql2.TypeTag
import fauna.repo.values._
import fql.ast.{ Expr, Span }
import java.time.LocalDate
import java.util.UUID
import scala.collection.immutable.ArraySeq

class FQL2TypecastSpec extends Spec {
  object TestFunc extends Value.NativeFunc {
    def arity = Value.Func.Arity(1)
    def callee = Value.Null(Span.Null)
    def name = "test"
    // We cheat for tests
    protected def `Only fauna.model.runtime.fql2.NativeFunction may extend Value.NativeFunc`() = {}
  }

  object TestSet extends Value.Set {
    def reify(ctx: ValueReification.ReifyCtx) = Expr.Id("Test", Span.Null)
    // We cheat for tests
    protected def `Only fauna.model.runtime.fql2.ValueSet may extend Value.Set`() = {}
  }

  val aNull: Value = Value.Null(Span.Null)
  val aBool: Value = Value.Boolean(true)
  val anInt: Value = Value.Int(1)
  val intLong: Value = Value.Long(2L)
  val aLong: Value = Value.Long(50000000000L)
  val aDouble: Value = Value.Double(3.14)
  val intDouble: Value = Value.Double(7.0)
  val longDouble: Value = Value.Double(50000000000.00)
  val aString: Value = Value.Str("hello, world")
  val aByteStr: Value = Value.Bytes(ArraySeq[Byte](1, 2, 3))
  val aTime: Value = Value.Time(Timestamp.Epoch)
  val aDate: Value = Value.Date(LocalDate.EPOCH)
  val aUUID: Value = Value.UUID(UUID.randomUUID())
  val aStruct: Value = Value.Struct.empty
  val aDoc: Value = Value.Doc(DocID.MinValue)
  val anArray: Value = Value.Array.empty
  val aLambda: Value =
    Value.Lambda(ArraySeq(Some("x")), None, Expr.Id("x", Span.Null), Map.empty)

  "TypeTag cast" - {
    "null casts" in {
      TypeTag.Null.cast(aNull) shouldEqual Some(aNull)
      TypeTag.Null.cast(aBool) shouldEqual None
    }

    "boolean casts" in {
      TypeTag.Boolean.cast(aBool) shouldEqual Some(aBool)
      TypeTag.Boolean.cast(aNull) shouldEqual None
    }

    "number casts" in {
      TypeTag.Number.cast(aBool) shouldEqual None
      TypeTag.Number.cast(anInt) shouldEqual Some(anInt)
      TypeTag.Number.cast(aLong) shouldEqual Some(aLong)
      TypeTag.Number.cast(intLong) shouldEqual Some(intLong)
      TypeTag.Number.cast(aDouble) shouldEqual Some(aDouble)
      TypeTag.Number.cast(intDouble) shouldEqual Some(intDouble)
      TypeTag.Number.cast(longDouble) shouldEqual Some(longDouble)
    }

    "int casts" in {
      TypeTag.Int.cast(aBool) shouldEqual None
      TypeTag.Int.cast(anInt) shouldEqual Some(anInt)
      TypeTag.Int.cast(aLong) shouldEqual None
      TypeTag.Int.cast(intLong) shouldEqual Some(Value.Int(2))
      TypeTag.Int.cast(aDouble) shouldEqual None
      TypeTag.Int.cast(intDouble) shouldEqual Some(Value.Int(7))
      TypeTag.Int.cast(longDouble) shouldEqual None
    }

    "long casts" in {
      TypeTag.Long.cast(aBool) shouldEqual None
      TypeTag.Long.cast(anInt) shouldEqual Some(Value.Long(1L))
      TypeTag.Long.cast(aLong) shouldEqual Some(aLong)
      TypeTag.Long.cast(intLong) shouldEqual Some(intLong)
      TypeTag.Long.cast(aDouble) shouldEqual None
      TypeTag.Long.cast(intDouble) shouldEqual Some(Value.Long(7L))
      TypeTag.Long.cast(longDouble) shouldEqual Some(Value.Long(50000000000L))
    }

    "double casts" in {
      TypeTag.Double.cast(aBool) shouldEqual None
      TypeTag.Double.cast(anInt) shouldEqual Some(Value.Double(1.0))
      TypeTag.Double.cast(aLong) shouldEqual Some(Value.Double(50000000000.00))
      TypeTag.Double.cast(intLong) shouldEqual Some(Value.Double(2.0))
      TypeTag.Double.cast(aDouble) shouldEqual Some(aDouble)
      TypeTag.Double.cast(intDouble) shouldEqual Some(intDouble)
      TypeTag.Double.cast(longDouble) shouldEqual Some(longDouble)
    }

    "float casts" in {
      TypeTag.Float.cast(aBool) shouldEqual None
      TypeTag.Float.cast(anInt) shouldEqual Some(Value.Double(1.0))
      TypeTag.Float.cast(aLong) shouldEqual Some(Value.Double(50000000000.00))
      TypeTag.Float.cast(intLong) shouldEqual Some(Value.Double(2.0))
      TypeTag.Float.cast(aDouble) shouldEqual Some(aDouble)
      TypeTag.Float.cast(intDouble) shouldEqual Some(intDouble)
      TypeTag.Float.cast(longDouble) shouldEqual Some(longDouble)
    }

    "string casts" in {
      TypeTag.Str.cast(aString) shouldEqual Some(aString)
      TypeTag.Str.cast(aNull) shouldEqual None
    }

    "bytes casts" in {
      TypeTag.Bytes.cast(aByteStr) shouldEqual Some(aByteStr)
      TypeTag.Bytes.cast(aNull) shouldEqual None
    }

    "time casts" in {
      TypeTag.Time.cast(aTime) shouldEqual Some(aTime)
      TypeTag.Time.cast(aNull) shouldEqual None
    }

    "date casts" in {
      TypeTag.Date.cast(aDate) shouldEqual Some(aDate)
      TypeTag.Date.cast(aNull) shouldEqual None
    }

    "uuid casts" in {
      TypeTag.UUID.cast(aUUID) shouldEqual Some(aUUID)
      TypeTag.UUID.cast(aNull) shouldEqual None
    }

    "object casts" in {
      TypeTag.AnyObject.cast(aStruct) shouldEqual Some(aStruct)
      TypeTag.AnyObject.cast(aDoc) shouldEqual Some(aDoc)
      TypeTag.AnyObject.cast(aNull) shouldEqual None
    }

    "struct casts" in {
      TypeTag.AnyStruct.cast(aStruct) shouldEqual Some(aStruct)
      TypeTag.AnyStruct.cast(aDoc) shouldEqual None
      TypeTag.AnyStruct.cast(aNull) shouldEqual None
    }

    "doc casts" in {
      TypeTag.AnyDoc.cast(aStruct) shouldEqual None
      TypeTag.AnyDoc.cast(aDoc) shouldEqual Some(aDoc)
      TypeTag.AnyDoc.cast(aNull) shouldEqual None
    }

    "array casts" in {
      TypeTag.AnyArray.cast(anArray) shouldEqual Some(anArray)
      TypeTag.AnyArray.cast(TestSet) shouldEqual None
      TypeTag.AnyArray.cast(aNull) shouldEqual None
    }

    "set casts" in {
      TypeTag.AnySet.cast(TestSet) shouldEqual Some(TestSet)
      TypeTag.AnySet.cast(anArray) shouldEqual None
      TypeTag.AnySet.cast(aNull) shouldEqual None
    }

    "function casts" in {
      TypeTag.AnyFunction.cast(aLambda) shouldEqual Some(aLambda)
      TypeTag.AnyFunction.cast(TestFunc) shouldEqual Some(TestFunc)
      TypeTag.AnyFunction.cast(aNull) shouldEqual None
    }
  }
}
