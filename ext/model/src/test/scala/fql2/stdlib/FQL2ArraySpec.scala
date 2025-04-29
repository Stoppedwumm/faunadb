package fauna.model.test

import fauna.model.runtime.fql2.{
  stdlib,
  FQLInterpreter,
  QueryCheckFailure,
  QueryRuntimeFailure
}
import fauna.repo.values.Value
import fql.ast.{ Span, Src }
import fql.error.TypeError

class FQL2ArraySpec extends FQL2StdlibHelperSpec("Array", stdlib.ArrayPrototype) {
  val auth = newDB

  def span(start: Int, end: Int) = Span(start, end, Src.Query(""))

  def stackTrace(start: Int, end: Int) =
    FQLInterpreter.StackTrace(Seq(span(start, end)))

  def testStaticSig(sig: String*) =
    s"has signature $sig" inWithTest { test =>
      test.signatures = sig
      lookupSig(stdlib.ArrayCompanion, test.function) shouldBe sig
    }

  override def checkAllTested() = {
    val arrFields = typer.typeShapes("Array").fields.keySet
    val setFields = typer.typeShapes("Set").fields.keySet.filter(!_.startsWith("$"))

    // Fields on both are iterable functions, so we don't test those here.
    val allFields = arrFields.filter { !setFields.contains(_) }
    allFields.foreach { field =>
      if (!skipFields.contains(field) && !tests.contains(field)) {
        fail(s"function Array.$field is not tested")
      }
    }
  }

  "at" - {
    testSig("at(index: Number) => A")

    "selects an element out of an array" in {
      checkOk(auth, "[1, 2].at(0)", Value.Number(1))
      checkOk(auth, "[1, 2].at(1)", Value.Number(2))

      checkOk(auth, "[1, 2][0]", Value.Number(1))
      checkOk(auth, "[1, 2][1]", Value.Number(2))
    }

    "indexes that are too large cause an error" in {
      checkErr(
        auth,
        "[].at(0)",
        QueryRuntimeFailure.IndexOutOfBounds(0, 0, stackTrace(5, 8)))

      // FIXME: this could be a QueryCheckFailure: see TyperConstrain
      checkErr(
        auth,
        "[][0]",
        QueryRuntimeFailure.IndexOutOfBounds(0, 0, stackTrace(2, 5)))

      // force dynamic access
      checkErr(
        auth,
        "{ let i = 0; [][i] }",
        QueryRuntimeFailure.IndexOutOfBounds(0, 0, stackTrace(15, 18)))

      checkErr(
        auth,
        "[1, 2].at(2)",
        QueryRuntimeFailure.IndexOutOfBounds(2, 2, stackTrace(9, 12)))

      // FIXME: this could be a QueryCheckFailure: see TyperConstrain
      checkErr(
        auth,
        "[1, 2][2]",
        QueryRuntimeFailure.IndexOutOfBounds(2, 2, stackTrace(6, 9)))

      // force dynamic access
      checkErr(
        auth,
        "{ let i = 2; [1, 2][i] }",
        QueryRuntimeFailure.IndexOutOfBounds(2, 2, stackTrace(19, 22)))
    }

    "negative indexes cause an error" in {
      checkErr(
        auth,
        "[].at(-1)",
        QueryRuntimeFailure.IndexOutOfBounds(-1, 0, stackTrace(5, 9)))

      checkErr(
        auth,
        "[][-1]",
        QueryCheckFailure(
          Seq(TypeError("Index -1 out of bounds for length 0", span(3, 5)))))

      // force dynamic access
      checkErr(
        auth,
        "{ let i = -1; [][i] }",
        QueryRuntimeFailure.IndexOutOfBounds(-1, 0, stackTrace(16, 19)))

      checkErr(
        auth,
        "[1, 2].at(-3)",
        QueryRuntimeFailure.IndexOutOfBounds(-3, 2, stackTrace(9, 13)))

      checkErr(
        auth,
        "[1, 2][-3]",
        QueryCheckFailure(
          Seq(TypeError("Index -3 out of bounds for length 2", span(7, 9)))))

      // force dynamic access
      checkErr(
        auth,
        "{ let i = -3; [1, 2][i] }",
        QueryRuntimeFailure.IndexOutOfBounds(-3, 2, stackTrace(20, 23)))
    }

    "indexes at the integer limit don't cause problems" in {
      checkErr(
        auth,
        "[].at(2147483647)",
        QueryRuntimeFailure
          .IndexOutOfBounds(2147483647, 0, stackTrace(5, 17)))
      checkErr(
        auth,
        "[].at(2147483648)",
        QueryRuntimeFailure(
          "invalid_argument",
          "expected value for `index` of type Int, received Long",
          stackTrace(5, 17)))

      checkErr(
        auth,
        "[].at(-2147483648)",
        QueryRuntimeFailure
          .IndexOutOfBounds(-2147483648, 0, stackTrace(5, 18)))
      checkErr(
        auth,
        "[].at(-2147483649)",
        QueryRuntimeFailure(
          "invalid_argument",
          "expected value for `index` of type Int, received Long",
          stackTrace(5, 18)))

      checkErr(
        auth,
        "[1, 2].at(-2147483648)",
        QueryRuntimeFailure
          .IndexOutOfBounds(-2147483648, 2, stackTrace(9, 22)))
    }
  }

  "append" - {
    testSig("append(element: A) => Array<A>")

    "appends an element to the end of the array" in {
      checkOk(auth, "[].append(5)", Value.Array(Value.Number(5)))
      checkOk(
        auth,
        "[1, 2].append(3)",
        Value.Array(Value.Number(1), Value.Number(2), Value.Number(3)))
    }
  }

  "entries" - {
    testSig("entries() => Array<[Number, A]>")

    "adds the index to every element in an iterable" in {
      checkOk(
        auth,
        "['a', 'b', 'c'].entries()",
        Value.Array(
          Value.Array(Value.Number(0), Value.Str("a")),
          Value.Array(Value.Number(1), Value.Str("b")),
          Value.Array(Value.Number(2), Value.Str("c")))
      )
    }
  }

  "filter" - {
    testSig("filter(predicate: A => Boolean | Null) => Array<A>")

    "filters by the given condition" in {
      checkOk(
        auth,
        "[1, 2, 3, 4].filter(v => v >= 3)",
        Value.Array(Value.Number(3), Value.Number(4)))
    }

    "null acts like false in the predicate" in {
      checkOk(
        auth,
        "[true, false, null].filter(v => v)",
        Value.Array(Value.Boolean(true)))
    }
  }

  "flatten" - {
    testSig("flatten() => Array<Any>")

    "flattens an array" in {
      checkOk(
        auth,
        "[[1, 2], [3, 4]].flatten()",
        Value.Array(
          Value.Number(1),
          Value.Number(2),
          Value.Number(3),
          Value.Number(4)))
    }

    "works for empty arrays" in {
      checkOk(auth, "[].flatten()", Value.Array())
      checkOk(auth, "[[], []].flatten()", Value.Array())
      checkOk(
        auth,
        "[[1, 2], [], []].flatten()",
        Value.Array(Value.Number(1), Value.Number(2)))
      checkOk(
        auth,
        "[[], [3, 4]].flatten()",
        Value.Array(Value.Number(3), Value.Number(4)))
    }
  }

  "indexOf" - {
    testSig(
      "indexOf(element: A) => Number | Null",
      "indexOf(element: A, start: Number) => Number | Null")

    "returns the index of an element if the element was found" in {
      checkOk(auth, "['a', 'b', 'c', 'b'].indexOf('a')", Value.Int(0))
      checkOk(auth, "['a', 'b', 'c', 'b'].indexOf('b')", Value.Int(1))
      checkOk(auth, "['a', 'b', 'c', 'b'].indexOf('c')", Value.Int(2))
    }

    "returns null if no element was found" in {
      checkOk(auth, "['a', 'b', 'c'].indexOf('d')", Value.Null(Span.Null))
    }

    "starts searching at the given index" in {
      checkOk(auth, "['a', 'b', 'b', 'd'].indexOf('b', 0)", Value.Int(1))
      checkOk(auth, "['a', 'b', 'b', 'd'].indexOf('b', 1)", Value.Int(1))
      checkOk(auth, "['a', 'b', 'b', 'd'].indexOf('b', 2)", Value.Int(2))
      checkOk(auth, "['a', 'b', 'b', 'd'].indexOf('b', 3)", Value.Null(Span.Null))
    }

    "rejects negative indexes" in {
      checkErr(
        auth,
        "['a', 'b', 'b', 'd'].indexOf('b', -1)",
        QueryRuntimeFailure
          .IndexOutOfBounds(-1, 4, stackTrace(28, 37)))
    }

    "errors for indexes out of bounds" in {
      checkErr(
        auth,
        "['a', 'b', 'b', 'd'].indexOf('b', 100)",
        QueryRuntimeFailure
          .IndexOutOfBounds(100, 4, stackTrace(28, 38)))
      checkErr(
        auth,
        "['a', 'b', 'b', 'd'].indexOf('b', -50)",
        QueryRuntimeFailure
          .IndexOutOfBounds(-50, 4, stackTrace(28, 38)))
    }
  }

  "indexWhere" - {
    testSig(
      "indexWhere(predicate: A => Boolean | Null) => Number | Null",
      "indexWhere(predicate: A => Boolean | Null, start: Number) => Number | Null")

    "returns the index of an element if the element was found" in {
      // This is separate so that the tests fit on one line
      val arr = "['a', 'b', 'c', 'b']"
      checkOk(auth, s"$arr.indexWhere(v => v == 'a')", Value.Int(0))
      checkOk(auth, s"$arr.indexWhere(v => v == 'b')", Value.Int(1))
      checkOk(auth, s"$arr.indexWhere(v => v == 'c')", Value.Int(2))
    }

    "returns null if no element was found" in {
      checkOk(auth, "['a', 'b', 'c'].indexWhere(v => false)", Value.Null(Span.Null))
      // null is falsey
      checkOk(auth, "['a', 'b', 'c'].indexWhere(v => null)", Value.Null(Span.Null))
    }

    "starts searching at the given index" in {
      val arr = "['a', 'b', 'b', 'd']"
      checkOk(auth, s"$arr.indexWhere(v => v == 'b', 0)", Value.Int(1))
      checkOk(auth, s"$arr.indexWhere(v => v == 'b', 1)", Value.Int(1))
      checkOk(auth, s"$arr.indexWhere(v => v == 'b', 2)", Value.Int(2))
      checkOk(auth, s"$arr.indexWhere(v => v == 'b', 3)", Value.Null(Span.Null))
    }

    "rejects negative indexes" in {
      checkErr(
        auth,
        s"['a', 'b', 'b', 'd'].indexWhere(v => v == 'b', -1)",
        QueryRuntimeFailure
          .IndexOutOfBounds(-1, 4, stackTrace(31, 50)))
    }

    "only creates side affects until the lambda returns true" in {
      val auth = newAuth
      evalOk(auth, "Collection.create({ name: 'User' })")

      checkOk(
        auth,
        """|['a', 'b', 'b', 'd'].indexWhere(v => {
           |  User.create({ value: v })
           |  v == 'b'
           |})
           |""".stripMargin,
        Value.Int(1)
      )

      checkOk(
        auth,
        "User.all().map(.value).toArray()",
        Value.Array(Value.Str("a"), Value.Str("b")))
    }

    "errors for indexes out of bounds" in {
      checkErr(
        auth,
        "['a', 'b', 'b', 'd'].indexWhere(v => v == 'b', 100)",
        QueryRuntimeFailure
          .IndexOutOfBounds(100, 4, stackTrace(31, 51)))
      checkErr(
        auth,
        "['a', 'b', 'b', 'd'].indexWhere(v => v == 'b', -50)",
        QueryRuntimeFailure
          .IndexOutOfBounds(-50, 4, stackTrace(31, 51)))
    }
  }

  "lastIndexOf" - {
    testSig(
      "lastIndexOf(element: A) => Number | Null",
      "lastIndexOf(element: A, end: Number) => Number | Null")

    "returns the index of an element if the element was found" in {
      checkOk(auth, "['a', 'b', 'c', 'b'].lastIndexOf('a')", Value.Int(0))
      checkOk(auth, "['a', 'b', 'c', 'b'].lastIndexOf('b')", Value.Int(3))
      checkOk(auth, "['a', 'b', 'c', 'b'].lastIndexOf('c')", Value.Int(2))
    }

    "returns null if no element was found" in {
      checkOk(auth, "['a', 'b', 'c'].lastIndexOf('d')", Value.Null(Span.Null))
    }

    "starts searching backwards at the given index" in {
      checkOk(
        auth,
        "['a', 'b', 'b', 'd'].lastIndexOf('b', 0)",
        Value.Null(Span.Null))
      checkOk(auth, "['a', 'b', 'b', 'd'].lastIndexOf('b', 1)", Value.Int(1))
      checkOk(auth, "['a', 'b', 'b', 'd'].lastIndexOf('b', 2)", Value.Int(2))
      checkOk(auth, "['a', 'b', 'b', 'd'].lastIndexOf('b', 3)", Value.Int(2))
    }

    "rejects negative indexes" in {
      checkErr(
        auth,
        "['a', 'b', 'b', 'd'].lastIndexOf('b', -1)",
        QueryRuntimeFailure
          .IndexOutOfBounds(-1, 4, stackTrace(32, 41)))
    }

    "errors for indexes out of bounds" in {
      checkErr(
        auth,
        "['a', 'b', 'b', 'd'].lastIndexOf('b', 100)",
        QueryRuntimeFailure
          .IndexOutOfBounds(100, 4, stackTrace(32, 42)))
      checkErr(
        auth,
        "['a', 'b', 'b', 'd'].lastIndexOf('b', -50)",
        QueryRuntimeFailure
          .IndexOutOfBounds(-50, 4, stackTrace(32, 42)))
    }
  }

  "lastIndexWhere" - {
    testSig(
      "lastIndexWhere(predicate: A => Boolean | Null) => Number | Null",
      "lastIndexWhere(predicate: A => Boolean | Null, end: Number) => Number | Null")

    "returns the index of an element if the element was found" in {
      val arr = "['a', 'b', 'c', 'b']"
      checkOk(auth, s"$arr.lastIndexWhere(v => v == 'a')", Value.Int(0))
      checkOk(auth, s"$arr.lastIndexWhere(v => v == 'b')", Value.Int(3))
      checkOk(auth, s"$arr.lastIndexWhere(v => v == 'c')", Value.Int(2))
    }

    "returns null if no element was found" in {
      checkOk(
        auth,
        "['a', 'b', 'c'].lastIndexWhere(v => false)",
        Value.Null(Span.Null))
      // null is falsey
      checkOk(
        auth,
        "['a', 'b', 'c'].lastIndexWhere(v => null)",
        Value.Null(Span.Null))
    }

    "starts searching backwards at the given index" in {
      val arr = "['a', 'b', 'b', 'd']"
      checkOk(auth, s"$arr.lastIndexWhere(v => v == 'b', 0)", Value.Null(Span.Null))
      checkOk(auth, s"$arr.lastIndexWhere(v => v == 'b', 1)", Value.Int(1))
      checkOk(auth, s"$arr.lastIndexWhere(v => v == 'b', 2)", Value.Int(2))
      checkOk(auth, s"$arr.lastIndexWhere(v => v == 'b', 3)", Value.Int(2))
    }

    "rejects negative indexes" in {
      checkErr(
        auth,
        s"['a', 'b', 'b', 'd'].lastIndexWhere(v => v == 'b', -1)",
        QueryRuntimeFailure
          .IndexOutOfBounds(-1, 4, stackTrace(35, 54)))
    }

    "only creates side affects until the lambda returns true" in {
      val auth = newAuth
      evalOk(auth, "Collection.create({ name: 'User' })")

      checkOk(
        auth,
        """|['a', 'b', 'b', 'd'].lastIndexWhere(v => {
           |  User.create({ value: v })
           |  v == 'b'
           |})
           |""".stripMargin,
        Value.Int(2)
      )

      // Note this is in reverse order from the original array.
      checkOk(
        auth,
        "User.all().map(.value).toArray()",
        Value.Array(Value.Str("d"), Value.Str("b")))
    }

    "errors for indexes out of bounds" in {
      checkErr(
        auth,
        "['a', 'b', 'b', 'd'].lastIndexWhere(v => v == 'b', 100)",
        QueryRuntimeFailure
          .IndexOutOfBounds(100, 4, stackTrace(35, 55)))
      checkErr(
        auth,
        "['a', 'b', 'b', 'd'].lastIndexWhere(v => v == 'b', -50)",
        QueryRuntimeFailure
          .IndexOutOfBounds(-50, 4, stackTrace(35, 55)))
    }
  }

  "length" - {
    testSig("Number")

    "returns the length of the array" in {
      checkOk(auth, "[].length", Value.Number(0))
      checkOk(auth, "[1].length", Value.Number(1))
      checkOk(auth, "[1, 2].length", Value.Number(2))
    }
  }

  "prepend" - {
    testSig("prepend(element: A) => Array<A>")

    "prepends an element at the start of the array" in {
      checkOk(auth, "[].prepend(5)", Value.Array(Value.Number(5)))
      checkOk(
        auth,
        "[2, 3].prepend(1)",
        Value.Array(Value.Number(1), Value.Number(2), Value.Number(3)))
    }
  }

  "slice" - {
    testSig(
      "slice(from: Number) => Array<A>",
      "slice(from: Number, until: Number) => Array<A>")

    "returns a sliced portion of the array" in {
      checkOk(auth, "[1, 2, 3].slice(0, 0)", Value.Array())
      checkOk(auth, "[1, 2, 3].slice(0, 1)", Value.Array(Value.Number(1)))
      checkOk(
        auth,
        "[1, 2, 3].slice(0, 2)",
        Value.Array(Value.Number(1), Value.Number(2)))
      checkOk(
        auth,
        "[1, 2, 3].slice(0, 3)",
        Value.Array(Value.Number(1), Value.Number(2), Value.Number(3)))
      checkOk(
        auth,
        "[1, 2, 3].slice(0, 4)",
        Value.Array(Value.Number(1), Value.Number(2), Value.Number(3)))
    }

    "calling with one argument slices to the end of the array" in {
      checkOk(
        auth,
        "[1, 2, 3].slice(0)",
        Value.Array(Value.Number(1), Value.Number(2), Value.Number(3)))
      checkOk(
        auth,
        "[1, 2, 3].slice(1)",
        Value.Array(Value.Number(2), Value.Number(3)))
      checkOk(auth, "[1, 2, 3].slice(2)", Value.Array(Value.Number(3)))
      checkOk(auth, "[1, 2, 3].slice(3)", Value.Array())
    }

    "indexes out of bounds will not error" in {
      checkOk(
        auth,
        "[1, 2].slice(0, 100)",
        Value.Array(Value.Number(1), Value.Number(2)))
      checkOk(auth, "[1, 2].slice(100, 200)", Value.Array())

      // even negative indexes will work
      checkOk(
        auth,
        "[1, 2].slice(-50, 100)",
        Value.Array(Value.Number(1), Value.Number(2)))
    }
  }

  "sequence" - {
    testStaticSig("sequence(from: Number, until: Number) => Array<Number>")

    "returns a set with the expected contents" in {
      checkOk(
        auth,
        "Array.sequence(0, 4)",
        Value.Array(Value.Int(0), Value.Int(1), Value.Int(2), Value.Int(3)))
    }

    "out of order args" in {
      checkOk(auth, "Array.sequence(4, 0)", Value.Array())
    }

    "too big" in {
      checkErr(
        auth,
        "Array.sequence(0, 16001)",
        QueryRuntimeFailure
          .ValueTooLarge("range size 16001 exceeds limit 16000", stackTrace(14, 24)))
    }
  }

  "toSet" - {
    testSig("toSet() => Set<A>")

    "returns a set containing the elements of the array" in {
      checkOk(auth, "[].toSet().paginate()", Value.Struct("data" -> Value.Array()))
      checkOk(
        auth,
        "[1, 2].toSet().paginate()",
        Value.Struct("data" -> Value.Array(Value.Number(1), Value.Number(2))))

      val page = evalOk(
        auth,
        "[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17].toSet().paginate()")
        .asInstanceOf[Value.Struct.Full]
      page.fields("data") shouldBe Value.Array(
        Value.Number(0),
        Value.Number(1),
        Value.Number(2),
        Value.Number(3),
        Value.Number(4),
        Value.Number(5),
        Value.Number(6),
        Value.Number(7),
        Value.Number(8),
        Value.Number(9),
        Value.Number(10),
        Value.Number(11),
        Value.Number(12),
        Value.Number(13),
        Value.Number(14),
        Value.Number(15)
      )
      page.fields("after") should matchPattern { case _: Value.SetCursor => }
    }
  }
}
