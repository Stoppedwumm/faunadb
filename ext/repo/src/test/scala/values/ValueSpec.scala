package fauna.repo.test

import fauna.atoms._
import fauna.codex.cbor.{ CBOR, CBOROrdering }
import fauna.lang.Timestamp
import fauna.repo.values._
import fql.ast.{ Expr, Literal, Name, Span }
import java.time.LocalDate
import java.util.UUID
import scala.collection.immutable.{ ArraySeq, SeqMap }

class ValueSpec extends Spec {
  object TestNativeFunction extends Value.NativeFunc {
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

  "Value" - {
    "matches number values" in {
      def matcher(v: Value.Number) =
        v match {
          case Value.Int(_)    =>
          case Value.Long(_)   =>
          case Value.Double(_) =>
        }

      matcher(Value.Int(0))
    }

    "matches scalar values" in {
      def matcher(v: Value.Scalar) =
        v match {
          case Value.ID(_)           =>
          case Value.Int(_)          =>
          case Value.Long(_)         =>
          case Value.Double(_)       =>
          case Value.True            =>
          case Value.False           =>
          case Value.Str(_)          =>
          case Value.Bytes(_)        =>
          case Value.Time(_)         =>
          case Value.TransactionTime =>
          case Value.Date(_)         =>
          case Value.UUID(_)         =>
          case Value.Null(_)         =>
        }

      matcher(Value.Int(0))
    }

    "matches persistable values" in {
      def matcher(v: Value.Persistable) =
        v match {
          case Value.Scalar(_)          =>
          case Value.Doc(_, _, _, _, _) =>
        }

      matcher(Value.Int(0))
    }

    "matches all values" in {
      def matcher(v: Value) =
        v match {
          case Value.Scalar(_)                     =>
          case Value.Array(_)                      =>
          case Value.Doc(_, _, _, _, _)            =>
          case Value.Struct.Full(_, _, _, _)       =>
          case Value.Struct.Partial(_, _, _, _, _) =>
          case Value.SingletonObject((_, _))       =>

          case _: Value.SetCursor   =>
          case _: Value.Func        =>
          case _: Value.EventSource =>
          case _: Value.Set         =>
        }

      matcher(Value.Int(0))
    }

    "field accessor" in {
      val struct = Value.Struct("foo" -> Value.Str("bar"))

      (struct / "foo") shouldBe Value.Str("bar")
      (struct / "bar") shouldBe Value.Null.missingField(
        struct,
        Name("bar", Span.Null))

      the[UnexpectedValue] thrownBy {
        Value.Str("str") / "foo"
      } should have message "Unexpected value Str(str)"
    }
  }

  "Value ordering" - {
    "values order across types equivalent to index order" in {
      val values = Seq(
        Value.Double(1),
        Value.True,
        Value.Str(""),
        Value.Bytes(ArraySeq(1)),
        Value.Time(Timestamp.Epoch),
        Value.Date(LocalDate.EPOCH),
        Value.UUID(UUID.randomUUID),
        Value.Null(Span.Null),
        Value.Doc(DocID.MinValue),
        Value.Array(Value.Long(1)),
        Value.Struct("foo" -> Value.Long(1))
      )

      def toTerm(v: Value) =
        CBOR.encode(Value.toIR(v).toOption.get)

      for {
        a <- values
        b <- values
      } {
        val cmp1 = a.compare(b)
        val cmp2 = CBOROrdering.compare(toTerm(a), toTerm(b))
        if (cmp1 != cmp2) {
          fail(s"$a compare $b == $cmp1, $a cborcompare $b == $cmp2")
        }
      }
    }

    "arrays sort lexically" in {
      val a = Value.Array(Value.Long(1), Value.Long(2))
      val b = Value.Array(Value.Long(1), Value.Long(1))

      (a > b) shouldEqual true

      val c = Value.Array(Value.Long(2))
      val d = Value.Array(Value.Long(1), Value.Long(1))

      (c > d) shouldEqual true

      val e = Value.Array(Value.Long(1), Value.Long(1))
      val f = Value.Array(Value.Long(1))

      (e > f) shouldEqual true
    }

    "structs sort in alphabetical key order then length" in {
      val a = Value.Struct("a" -> Value.Long(1), "b" -> Value.Long(2))
      val b = Value.Struct("b" -> Value.Long(1), "a" -> Value.Long(1))

      (a > b) shouldEqual true

      val c = Value.Struct(
        "a" -> Value.Long(1),
        "c" -> Value.Long(1),
        "b" -> Value.Long(1))
      val d = Value.Struct("b" -> Value.Long(1), "a" -> Value.Long(1))

      (c > d) shouldEqual true
    }
  }

  "Persistable values" - {
    "scalar values are persistable" in {
      Value.Int(0).isPersistable shouldEqual true
      Value.Long(0).isPersistable shouldEqual true
      Value.Double(0).isPersistable shouldEqual true
      Value.True.isPersistable shouldEqual true
      Value.False.isPersistable shouldEqual true
      Value.Str("").isPersistable shouldEqual true
      Value.Time(Timestamp.Epoch).isPersistable shouldEqual true
      Value.Date(LocalDate.EPOCH).isPersistable shouldEqual true
      Value.UUID(UUID.randomUUID).isPersistable shouldEqual true
      Value.Null(Span.Null).isPersistable shouldEqual true
    }

    "doc refs are persistable" in {
      Value.Doc(DatabaseID.RootID.toDocID).isPersistable shouldEqual true
    }

    "arrays are persistable if they contain persistable elements" in {
      Value.Array.empty.isPersistable shouldEqual true
      Value.Array(ArraySeq(Value.Long(0))).isPersistable shouldEqual true

      Value.Array(ArraySeq(TestSet)).isPersistable shouldEqual false
    }

    "objects are persistable if they contain persistable fields" in {
      Value.Struct.empty.isPersistable shouldEqual true
      Value.Struct(SeqMap("foo" -> Value.Long(0))).isPersistable shouldEqual true

      Value.Struct(SeqMap("foo" -> TestSet)).isPersistable shouldEqual false
    }
  }

  "Value runtime types" - {
    "scalar values have correct erased runtime types" in {
      Value.Int(0).dynamicType shouldEqual ValueType.IntType
      Value.Long(0).dynamicType shouldEqual ValueType.LongType
      Value.Double(0).dynamicType shouldEqual ValueType.DoubleType
      Value.True.dynamicType shouldEqual ValueType.BooleanType
      Value.False.dynamicType shouldEqual ValueType.BooleanType
      Value.Str("").dynamicType shouldEqual ValueType.StringType
      Value.Time(Timestamp.Epoch).dynamicType shouldEqual ValueType.TimeType
      Value.Date(LocalDate.EPOCH).dynamicType shouldEqual ValueType.DateType
      Value.UUID(UUID.randomUUID).dynamicType shouldEqual ValueType.UUIDType
      Value.Null(Span.Null).dynamicType shouldEqual ValueType.NullType
    }

    "non-scalar values have correct erased types" in {
      Value.Array.empty.dynamicType shouldEqual ValueType.AnyArrayType
      TestSet.dynamicType shouldEqual ValueType.AnySetType

      Value.Struct.empty.dynamicType shouldEqual ValueType.AnyStructType
      Value
        .Doc(DatabaseID.RootID.toDocID)
        .dynamicType shouldEqual ValueType.AnyDocType

      Value
        .Lambda(ArraySeq.empty, None, Expr.Lit(Literal.Null, Span.Null), Map.empty)
        .dynamicType shouldEqual ValueType.AnyLambdaType
      TestNativeFunction.dynamicType shouldEqual ValueType.AnyFunctionType
    }
  }

  "Function Arity" - {
    "Fixed arity accepts right number of arguments" in {
      val arity = Value.Func.Arity(2, 4)
      arity.accepts(0) shouldEqual false
      arity.accepts(1) shouldEqual false
      arity.accepts(2) shouldEqual true
      arity.accepts(3) shouldEqual false
      arity.accepts(4) shouldEqual true
    }
    "Variable arity accepts right number of arguments" in {
      val arity = Value.Func.Arity.Variable(2)
      arity.accepts(0) shouldEqual false
      arity.accepts(1) shouldEqual false
      arity.accepts(2) shouldEqual true
      arity.accepts(3) shouldEqual true
      arity.accepts(4) shouldEqual true
    }
    "Fixed arity renders to string" in {
      Value.Func.Arity(0).displayString() shouldEqual "no arguments"
      Value.Func.Arity(1).displayString() shouldEqual "exactly 1 argument"
      Value.Func.Arity(3).displayString() shouldEqual "exactly 3 arguments"
      Value.Func.Arity(1, 3).displayString() shouldEqual "1 or 3 arguments"
      Value.Func.Arity(1, 2, 3).displayString() shouldEqual "1, 2 or 3 arguments"
    }
    "Variable arity renders to string" in {
      Value.Func.Arity
        .Variable(0)
        .displayString() shouldEqual "any number of arguments"
      Value.Func.Arity.Variable(1).displayString() shouldEqual "at least 1 argument"
      Value.Func.Arity.Variable(4).displayString() shouldEqual "at least 4 arguments"
    }
  }

  "collection factory" - {
    "Can build a Value.Array" in {
      val a1 = (1 to 3).map(Value.Int(_)).to(Value.Array)

      a1 shouldEqual Value.Array(Value.Int(1), Value.Int(2), Value.Int(3))

      val a2 = {
        val b = Value.Array.newBuilder
        (1 to 3) foreach { i => b += Value.Int(i) }
        b.result()
      }

      a2 shouldEqual Value.Array(Value.Int(1), Value.Int(2), Value.Int(3))
    }

    "Can build a Value.Struct" in {
      val s1 = Seq("a", "b").map(k => (k, Value.Int(1))).to(Value.Struct)

      s1 shouldEqual Value.Struct("a" -> Value.Int(1), "b" -> Value.Int(1))

      val s2 = {
        val b = Value.Struct.newBuilder
        b += ("a" -> Value.Int(1))
        b += ("b" -> Value.Int(1))
        b.result()
      }

      s2 shouldEqual Value.Struct("a" -> Value.Int(1), "b" -> Value.Int(1))
    }
  }
}
