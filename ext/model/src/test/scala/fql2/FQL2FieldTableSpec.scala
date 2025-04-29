package fauna.model.test

import fauna.model.runtime.fql2._
import fauna.repo.values.Value
import fql.ast.{ Span, Src }
import fql.error.ParseError
import scala.collection.immutable.Map

class FQL2FieldTableSpec extends FQL2Spec {
  val auth = newAuth

  def evalInCtx(ctx: Map[String, Value], query: String): Result[Value] =
    eval(auth, query, typecheck = false, ctx).res.map(_.value)

  def evalErrInCtx(ctx: Map[String, Value], query: String): QueryFailure =
    evalInCtx(ctx, query) match {
      case Result.Ok(v)  => fail(s"unexpected eval success: $v")
      case Result.Err(e) => e
    }

  def evalOkInCtx(ctx: Map[String, Value], query: String): Value =
    evalInCtx(ctx, query) match {
      case Result.Ok(v) => v
      case Result.Err(e) =>
        fail(
          s"unexpected eval error: ${e.errors.map(_.renderWithSource(Map.empty)).mkString("\n\n")}")
    }

  "FieldTable singleton object" - {
    object TestSingleton extends SingletonObject("Foo", None) {

      def contains(v: Value): Boolean = ???

      defStaticFunction("func")() { (_) =>
        Value.Int(0).toQuery
      }
      defStaticFunction("func")("one") { (_, _) =>
        Value.Int(1).toQuery
      }
      defStaticFunction("func")("one", "two", "three") { (_, _, _, _) =>
        Value.Int(3).toQuery
      }

      defApply(tt.Int)() { (_) =>
        Value.Int(0).toQuery
      }
      defApply(tt.Int)("one") { (_, _) =>
        Value.Int(1).toQuery
      }
      defApply(tt.Int)("one", "two", "three") { (_, _, _, _) =>
        Value.Int(3).toQuery
      }
    }

    val ctx = Map[String, Value]("Foo" -> TestSingleton)

    "Static functions" - {
      "Can call a static function on a singleton object" in {
        evalOkInCtx(ctx, "Foo.func()") shouldEqual Value.Int(0)
        evalOkInCtx(ctx, "Foo.func('foo')") shouldEqual Value.Int(1)
        evalOkInCtx(ctx, "Foo.func('foo', 'bar', 'baz')") shouldEqual Value.Int(3)
      }

      "Cannot call a static function with wrong number of arguments" in {
        evalErrInCtx(ctx, "Foo.func('foo', 'bar')") shouldEqual QueryRuntimeFailure
          .InvalidFuncArity(
            Value.Func.Arity(0, 1, 3),
            2,
            FQLInterpreter.StackTrace(Seq(Span(8, 22, Src.Query("")))))
      }
    }

    "Constructor" - {
      "Can call a constructor on a singleton object" in {
        evalOkInCtx(ctx, "Foo()") shouldEqual Value.Int(0)
        evalOkInCtx(ctx, "Foo('foo')") shouldEqual Value.Int(1)
        evalOkInCtx(ctx, "Foo('foo', 'bar', 'baz')") shouldEqual Value.Int(3)
      }
      "Cannot call a constructor with wrong number of arguments" in {
        evalErrInCtx(ctx, "Foo('foo', 'bar')") shouldEqual QueryRuntimeFailure
          .InvalidFuncArity(
            Value.Func.Arity(0, 1, 3),
            2,
            FQLInterpreter.StackTrace(Seq(Span(3, 17, Src.Query("")))))
      }
    }
  }

  "NativeMethod" - {
    "Arity-based function/method overloads" - {
      val m0 = NativeMethod(TypeTag.Any, "func") { (_, _) =>
        Value.Str("func0").toQuery
      }
      val m1 = NativeMethod(TypeTag.Any, "func", "one") { (_, _, _) =>
        Value.Str("func1").toQuery
      }
      val m3 = NativeMethod(TypeTag.Any, "func", "one", "two", "three") {
        (_, _, _, _, _) => Value.Str("func3").toQuery
      }
      val overloaded = m0.overload(m1).overload(m3)
      val ctx = Map("func" -> overloaded.bind(Value.Null(Span.Null)))

      "Calling an overloaded function delegates to the impl with matching arity" in {
        // FIXME: Cannot handle overloaded functions with a different amount of
        // arguments when typechecking.
        evalOkInCtx(ctx, "func()") shouldEqual Value.Str("func0")
        evalOkInCtx(ctx, "func('foo')") shouldEqual Value.Str("func1")
        evalOkInCtx(ctx, "func('foo', 'bar', 'baz')") shouldEqual Value.Str("func3")
      }

      "Calling an overloaded function with invalid arity throws an error" in {
        evalErrInCtx(ctx, "func('foo', 'bar')") shouldEqual QueryRuntimeFailure
          .InvalidFuncArity(
            Value.Func.Arity(0, 1, 3),
            2,
            FQLInterpreter.StackTrace(Seq(Span(4, 18, Src.Query("")))))
      }
    }

    "Type-based function/method overloads" - {
      val m_int =
        NativeMethod(TypeTag.Any, "func", "intParam" -> TypeTag.Int) { (_, _, _) =>
          Value.Str("int").toQuery
        }
      val m_str =
        NativeMethod(TypeTag.Any, "func", "strParam" -> TypeTag.Str) { (_, _, _) =>
          Value.Str("str").toQuery
        }
      val overloaded = m_int.overload(m_str)
      val ctx = Map("func" -> overloaded.bind(Value.Null(Span.Null)))

      "Calling an overloaded function delegates to the impl with matching param types" in {
        evalOkInCtx(ctx, "func(3)") shouldEqual Value.Str("int")
        evalOkInCtx(ctx, "func('foo')") shouldEqual Value.Str("str")
      }

      "Calling an overloaded function with invalid param type throws an error" in {
        evalErrInCtx(ctx, "func(7.21)") shouldEqual QueryRuntimeFailure
          .InvalidArgumentTypes(
            "func",
            "(Int) | (String)",
            "(Double)",
            FQLInterpreter.StackTrace(Seq(Span(4, 10, Src.Query(""))))
          )
      }

      "Calling an overloaded function with both type- and arity- based overloads" - {
        val m_2 =
          NativeMethod(
            TypeTag.Any,
            "func",
            "i1" -> TypeTag.Int,
            "i2" -> TypeTag.Int) { (_, _, _, _) =>
            Value.Str("2 params").toQuery
          }
        val overloaded2 = overloaded.overload(m_2)
        val ctx = Map("func" -> overloaded2.bind(Value.Null(Span.Null)))

        "with correct params" in {
          evalOkInCtx(ctx, "func(3)") shouldEqual Value.Str("int")
          evalOkInCtx(ctx, "func('foo')") shouldEqual Value.Str("str")
          evalOkInCtx(ctx, "func(2, 3)") shouldEqual Value.Str("2 params")
        }

        "with incorrect params" in {
          evalErrInCtx(ctx, "func(3, 7.21)") shouldEqual QueryRuntimeFailure
            .InvalidArgumentTypes(
              "func",
              "(Int, Int)",
              "(Int, Double)",
              FQLInterpreter.StackTrace(Seq(Span(4, 13, Src.Query(""))))
            )
        }
      }
    }
  }

  "isa" - {
    "works for basic types" in {
      // Basic.
      evalOk(auth, "1 isa Int") shouldBe Value.Boolean(true)
      evalOk(auth, "'1' isa Int") shouldBe Value.Boolean(false)
      evalOk(auth, "'1' isa String") shouldBe Value.Boolean(true)
      evalOk(auth, "1 isa String") shouldBe Value.Boolean(false)

      // Hierarchy.
      evalOk(auth, "1.0 isa Double") shouldBe Value.Boolean(true)
      evalOk(auth, "1 isa Double") shouldBe Value.Boolean(false)
      evalOk(auth, "1.0 isa Float") shouldBe Value.Boolean(true)
      evalOk(auth, "1 isa Float") shouldBe Value.Boolean(false)
      evalOk(auth, "1.0 isa Number") shouldBe Value.Boolean(true)

      evalOk(auth, "[1] isa Array") shouldBe Value.Boolean(true)

      // Operator precedence.
      evalOk(auth, "1.0 + 1 isa Number == true") shouldBe Value.Boolean(true)

      // Invalid argument.
      evalErr(auth, "1 isa 1") shouldBe QueryRuntimeFailure.InvalidArgument(
        "type_object",
        "expected a module object",
        FQLInterpreter.StackTrace(Seq(Span(6, 7, Src.Query(""))))
      )
    }

    "requires trailing whitespace" in {
      evalErr(auth, "1 isaInt") shouldBe QueryCheckFailure(
        Seq(
          ParseError(
            "Expected end-of-input",
            Span(2, 3, Src.Query("")),
            None,
            List.empty)))
      evalErr(auth, "1isaInt") shouldBe QueryCheckFailure(
        Seq(
          ParseError(
            "Expected end-of-input",
            Span(1, 2, Src.Query("")),
            None,
            List.empty)))
      evalErr(
        auth,
        """|1
           |isa Int
         """.stripMargin
      ) shouldBe QueryCheckFailure(
        Seq(
          ParseError(
            "Invalid identifier",
            Span(2, 5, Src.Query("")),
            Some("`isa` is a keyword"),
            List.empty)))

      // This is OK.
      evalOk(
        auth,
        """|1 isa
           |Int
         """.stripMargin
      ) shouldBe Value.True

      // This is weird but OK.
      evalOk(auth, "1isa Int") shouldBe Value.True
    }
  }
}
