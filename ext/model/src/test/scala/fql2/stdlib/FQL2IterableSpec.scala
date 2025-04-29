package fauna.model.test

import fauna.model.runtime.fql2.stdlib
import fauna.model.runtime.fql2.QueryCheckFailure
import fauna.model.runtime.fql2.ToString._
import fauna.repo.values.Value
import fql.ast.{ Span, Src }
import fql.error.TypeError
import org.scalactic.source.Position

class FQL2IterableSpec extends FQL2StdlibSpec("Iterable") {
  val auth = newDB

  def testArrSig(sig: String*)(implicit pos: Position) =
    s"Array has signature $sig" inWithTest { test =>
      test.signatures = sig
      lookupSig(stdlib.ArrayPrototype, test.function) shouldBe sig
    }

  def testSetSig(sig: String*)(implicit pos: Position) =
    s"Set has signature $sig" inWithTest { test =>
      test.signatures = sig
      lookupSig(stdlib.SetPrototype, test.function) shouldBe sig
    }

  def testSig(sig: String)(implicit pos: Position) = {
    testArrSig(sig)
    testSetSig(sig)
  }
  def testSigPending(sig: String*)(implicit pos: Position) = {
    s"Array has signature $sig" inWithTest { test =>
      pendingUntilFixed {
        test.signatures = sig
        lookupSig(stdlib.ArrayPrototype, test.function) shouldBe sig
      }
    }
    s"Set has signature $sig" inWithTest { test =>
      pendingUntilFixed {
        test.signatures = sig
        lookupSig(stdlib.SetPrototype, test.function) shouldBe sig
      }
    }
  }

  def buildArrQuery(arr: Seq[Value], query: String): String = {
    // NOTE: Lets hope the value doesn't depend on the interpreter existing
    val iter =
      arr.map { v => ctx ! v.toDisplayString(null) }.mkString("[", ", ", "]")
    s"let iter = $iter; $query"
  }
  def buildSetQuery(arr: Seq[Value], query: String): String = {
    // NOTE: Lets hope the value doesn't depend on the interpreter existing
    val iter =
      arr.map { v => ctx ! v.toDisplayString(null) }.mkString("[", ", ", "]")
    s"let iter = $iter.toSet(); $query"
  }

  /** Runs the given query twice. The first run will bind the variable `iter` to an array, and the second one will bind `iter` to a set.
    */
  def checkIter(arr: Seq[Value], query: String, v: Value)(implicit pos: Position) = {
    checkOk(auth, buildArrQuery(arr, query), v)(pos)
    checkOk(auth, buildSetQuery(arr, query), v)(pos)
  }

  override def checkAllTested() = {
    val arrFields = typer.typeShapes("Array").fields.keySet
    val setFields = typer.typeShapes("Set").fields.keySet.filter(!_.startsWith("$"))

    // Iterable functions are fields on both set and array, so this is just getting
    // all fields that overlap
    val allFields = arrFields.filter(setFields.contains) ++
      setFields.filter(arrFields.contains)

    allFields.foreach { field =>
      if (!skipFields.contains(field) && !tests.contains(field)) {
        fail(s"function Iterable.$field is not tested")
      }
    }
  }

  "aggregate" - {
    testSig("aggregate(seed: A, combiner: (A, A) => A) => A")

    "aggregates all the elements in the iterable" in {
      checkIter(
        Seq(Value.Number(1), Value.Number(2), Value.Number(3), Value.Number(4)),
        "iter.aggregate(0, (a, b) => a + b)",
        Value.Int(10))
    }

    "returns the seed for an empty iterable" in {
      checkIter(Seq.empty, "iter.aggregate(0, (a, b) => a + b)", Value.Int(0))
    }

    "typechecks with intermediate tuple" in {
      checkOk(
        auth,
        "Set.sequence(1, 10).map(i => [i]).aggregate([], (a, b) => a.concat(b))[0]",
        Value.Int(1))
    }
  }

  "any" - {
    testSig("any(predicate: A => Boolean | Null) => Boolean")

    "returns true if any closures returned true" in {
      checkIter(
        Seq(Value.Number(1), Value.Number(2), Value.Number(3)),
        "iter.any(v => v == 2)",
        Value.True)
    }

    "returns false of all of the closures return false" in {
      checkIter(
        Seq(Value.Number(1), Value.Number(2), Value.Number(3)),
        "iter.any(v => v == 100)",
        Value.False)
    }

    "null acts like false" in {
      checkIter(
        Seq(Value.Number(1), Value.Number(2), Value.Number(3)),
        "iter.any(v => null)",
        Value.False)
    }

    "creates side effects up to the first true" in {
      val auth = newAuth
      evalOk(auth, "Collection.create({ name: 'User1' })")
      evalOk(auth, "Collection.create({ name: 'User2' })")

      checkOk(
        auth,
        """|[1, 2, 3].any(v => {
           |  User1.create({ foo: v })
           |  v == 2
           |})""".stripMargin,
        Value.True)
      checkOk(
        auth,
        "User1.all().map(.foo).toArray()",
        Value.Array(Value.Number(1), Value.Number(2)))

      checkOk(
        auth,
        """|[1, 2, 3].toSet().any(v => {
           |  User2.create({ foo: v })
           |  v == 2
           |})""".stripMargin,
        Value.True)
      checkOk(
        auth,
        "User2.all().map(.foo).toArray()",
        Value.Array(Value.Number(1), Value.Number(2)))
    }

    "creates side effects for all closures if nothing returns true" in {
      val auth = newAuth
      evalOk(auth, "Collection.create({ name: 'User1' })")
      evalOk(auth, "Collection.create({ name: 'User2' })")

      checkOk(
        auth,
        """|[1, 2, 3].any(v => {
           |  User1.create({ foo: v })
           |  v == 100
           |})""".stripMargin,
        Value.False)
      checkOk(
        auth,
        "User1.all().map(.foo).toArray()",
        Value.Array(Value.Number(1), Value.Number(2), Value.Number(3)))

      checkOk(
        auth,
        """|[1, 2, 3].toSet().any(v => {
           |  User2.create({ foo: v })
           |  v == 100
           |})""".stripMargin,
        Value.False)
      checkOk(
        auth,
        "User2.all().map(.foo).toArray()",
        Value.Array(Value.Number(1), Value.Number(2), Value.Number(3)))
    }
  }

  "concat" - {
    testArrSig("concat(other: Array<B>) => Array<A | B>")
    testSetSig("concat(other: Set<B>) => Set<A | B>")

    "<array>.concat(<array>) works" in {
      checkOk(
        auth,
        "[1, 2].concat([3, 4])",
        Value.Array(
          Value.Number(1),
          Value.Number(2),
          Value.Number(3),
          Value.Number(4)
        ))
    }
    "<array>.concat(<set>) does not work" in {
      checkErr(
        auth,
        "[1, 2].concat([3, 4].toSet())",
        QueryCheckFailure(
          List(
            TypeError(
              "Type `Set<3 | 4>` is not a subtype of `Array<Any>`",
              Span(14, 28, Src.Query(""))
            )))
      )
    }
    "<set>.concat(<array>) does not work" in {
      checkErr(
        auth,
        "[1, 2].toSet().concat([3, 4]).paginate()",
        QueryCheckFailure(
          List(
            TypeError(
              "Type `[3, 4]` is not a subtype of `Set<Any>`",
              Span(22, 28, Src.Query(""))
            )))
      )
    }
    "<set>.concat(<set>) works" in {
      checkOk(
        auth,
        "[1, 2].toSet().concat([3, 4].toSet()).paginate()",
        page(
          Seq(
            Value.Number(1),
            Value.Number(2),
            Value.Number(3),
            Value.Number(4)
          )))
    }

    "works for empty sets" in {
      checkOk(auth, "[].toSet().concat([].toSet()).paginate()", page(Seq()))
      checkOk(
        auth,
        "[1, 2].toSet().concat([].toSet()).paginate()",
        page(
          Seq(
            Value.Number(1),
            Value.Number(2)
          )))
      checkOk(
        auth,
        "[].toSet().concat([3, 4].toSet()).paginate()",
        page(
          Seq(
            Value.Number(3),
            Value.Number(4)
          )))
    }
  }

  "distinct" - {
    testArrSig("distinct() => Array<A>")
    testSetSig("distinct() => Set<A>")

    "returns unique elements of an ordered iterable" in {
      checkOk(
        auth,
        "[1, 1, 2, 3, 3].distinct()",
        Value.Array(Value.Number(1), Value.Number(2), Value.Number(3)))

      checkOk(
        auth,
        "[1, 1, 2, 3, 3].toSet().distinct().paginate()",
        Value
          .Struct(
            "data" -> Value
              .Array(Value.Number(1), Value.Number(2), Value.Number(3))))
    }
  }

  "drop" - {
    testArrSig("drop(amount: Number) => Array<A>")
    testSetSig("drop(amount: Number) => Set<A>")

    "skips the given number of elements" in {
      checkOk(
        auth,
        "[1, 2, 3, 4, 5].drop(2)",
        Value.Array(Value.Number(3), Value.Number(4), Value.Number(5)))
      checkOk(
        auth,
        "[1, 2, 3, 4, 5].toSet().drop(2).toArray()",
        Value
          .Array(Value.Number(3), Value.Number(4), Value.Number(5)))
    }

    "skips all elements if count is larger than the iterable" in {
      checkOk(auth, "[1, 2, 3, 4, 5].drop(100)", Value.Array())
      checkOk(
        auth,
        "[1, 2, 3, 4, 5].toSet().drop(100).toArray()",
        Value
          .Array())
    }
  }

  "every" - {
    testSig("every(predicate: A => Boolean | Null) => Boolean")

    "returns false if any closures returned false" in {
      checkIter(
        Seq(Value.Number(1), Value.Number(-2), Value.Number(3)),
        "iter.every(v => v > 0)",
        Value.False)
    }

    "returns true if all of the closures return true" in {
      checkIter(
        Seq(Value.Number(1), Value.Number(2), Value.Number(3)),
        "iter.every(v => v > 0)",
        Value.True)
    }

    "null acts like false" in {
      checkIter(
        Seq(Value.Number(1), Value.Number(2), Value.Number(3)),
        "iter.every(v => null)",
        Value.False)
    }

    "creates side effects up to the first false" in {
      val auth = newAuth
      evalOk(auth, "Collection.create({ name: 'User1' })")
      evalOk(auth, "Collection.create({ name: 'User2' })")

      checkOk(
        auth,
        """|[1, -2, 3].every(v => {
           |  User1.create({ foo: v })
           |  v > 0
           |})""".stripMargin,
        Value.False)
      checkOk(
        auth,
        "User1.all().map(.foo).toArray()",
        Value.Array(Value.Number(1), Value.Number(-2)))

      checkOk(
        auth,
        """|[1, -2, 3].toSet().every(v => {
           |  User2.create({ foo: v })
           |  v > 0
           |})""".stripMargin,
        Value.False)
      checkOk(
        auth,
        "User2.all().map(.foo).toArray()",
        Value.Array(Value.Number(1), Value.Number(-2)))
    }

    "creates side effects for all closures if nothing returns false" in {
      val auth = newAuth
      evalOk(auth, "Collection.create({ name: 'User1' })")
      evalOk(auth, "Collection.create({ name: 'User2' })")

      checkOk(
        auth,
        """|[1, 2, 3].every(v => {
           |  User1.create({ foo: v })
           |  v > 0
           |})""".stripMargin,
        Value.True)
      checkOk(
        auth,
        "User1.all().map(.foo).toArray()",
        Value.Array(Value.Number(1), Value.Number(2), Value.Number(3)))

      checkOk(
        auth,
        """|[1, 2, 3].toSet().every(v => {
           |  User2.create({ foo: v })
           |  v > 0
           |})""".stripMargin,
        Value.True)
      checkOk(
        auth,
        "User2.all().map(.foo).toArray()",
        Value.Array(Value.Number(1), Value.Number(2), Value.Number(3)))
    }
  }

  "first" - {
    testSig("first() => A | Null")

    "gets the first element" in {
      checkIter(Seq(Value.Number(1)), "iter.first()", Value.Number(1))
      checkIter(
        Seq(Value.Number(1), Value.Number(2)),
        "iter.first()",
        Value.Number(1))
    }

    "returns null for an empty iterable" in {
      checkIter(Seq(), "iter.first()", Value.Null(Span.Null))
    }
  }

  "firstWhere" - {
    testSig("firstWhere(predicate: A => Boolean | Null) => A | Null")

    "gets the first element of an iterable that matches the given predicate" in {
      checkIter(
        Seq(Value.Number(1), Value.Number(2), Value.Number(3), Value.Number(4)),
        "iter.firstWhere(v => v > 2)",
        Value.Number(3))
    }
    "returns null for an empty iterable" in {
      checkIter(Seq(), "iter.firstWhere(v => v > 2)", Value.Null(Span.Null))
    }
    "returns null when no elements in the iterable match the predicate" in {
      checkIter(
        Seq(Value.Number(1), Value.Number(2), Value.Number(3), Value.Number(4)),
        "iter.firstWhere(v => v > 10)",
        Value.Null(Span.Null))
    }
    "nulls are falsey" in {
      checkIter(
        Seq(Value.Number(1), Value.Number(2), Value.Number(3), Value.Number(4)),
        "iter.firstWhere(v => null)",
        Value.Null(Span.Null))
    }
  }

  "flatMap" - {
    testArrSig("flatMap(mapper: A => Array<B>) => Array<B>")
    testSetSig("flatMap(mapper: A => Set<B>) => Set<B>")

    "[].flatMap([]) converts every element using the closure" in {
      checkOk(
        auth,
        "[1, 2, 3, 4].flatMap(v => [v, v + 10])",
        Value.Array(
          Value.Number(1),
          Value.Number(11),
          Value.Number(2),
          Value.Number(12),
          Value.Number(3),
          Value.Number(13),
          Value.Number(4),
          Value.Number(14))
      )
    }
    "[].flatMap(Set<>) does not work" in {
      evalErr(
        auth,
        "[1, 2, 3, 4].flatMap(v => [v, v + 10].toSet())") shouldBe QueryCheckFailure(
        Seq(
          TypeError(
            "Type `Set<Number>` is not a subtype of `Array<Any>`",
            Span(21, 45, Src.Query("")),
            Nil)))
    }
    "Set<>.flatMap([]) does not work" in {
      evalErr(
        auth,
        "[1, 2, 3, 4].toSet().flatMap(v => [v, v + 10]).paginate()") shouldBe QueryCheckFailure(
        Seq(
          // FIXME: get rid of Never
          TypeError(
            "Type `[Never, Number]` is not a subtype of `Set<Any>`",
            Span(34, 45, Src.Query("")))))
    }
    "Set<>.flatMap(Set<>) converts every element using the closure" in {
      checkOk(
        auth,
        "[1, 2, 3, 4].toSet().flatMap(v => [v, v + 10].toSet()).paginate()",
        page(
          Seq(
            Value.Number(1),
            Value.Number(11),
            Value.Number(2),
            Value.Number(12),
            Value.Number(3),
            Value.Number(13),
            Value.Number(4),
            Value.Number(14)))
      )
    }
  }

  "fold" - {
    testSig("fold(seed: B, reducer: (B, A) => B) => B")

    "calls the reducer for every element in the set" in {
      checkIter(
        Seq(Value.Number(1), Value.Number(2), Value.Number(3)),
        "iter.fold(100, (value, elem) => value + elem)",
        Value.Number(106))
    }
    "calls the reducer in order from the start to the end of the set" in {
      checkIter(
        Seq(Value.Number(1), Value.Number(2), Value.Number(3)),
        "iter.fold([], (value, elem) => value.concat([elem]))",
        Value.Array(Value.Number(1), Value.Number(2), Value.Number(3))
      )
    }
    "returns the seed for an empty set" in {
      checkIter(Seq(), "iter.fold(0, (a, b) => 100)", Value.Number(0))
    }

    "typechecks with intermediate tuple" in {
      checkOk(
        auth,
        "Set.sequence(1, 10).map(i => [i]).fold([], (a, b) => a.concat(b))[0]",
        Value.Int(1))

      checkOk(
        auth,
        "Set.sequence(1, 10).fold([], (a, b) => a.append(b))[0]",
        Value.Int(1))
    }
  }

  "foldRight" - {
    testSig("foldRight(seed: B, reducer: (B, A) => B) => B")

    "calls the reducer for every element in the set" in {
      checkIter(
        Seq(Value.Number(1), Value.Number(2), Value.Number(3)),
        "iter.foldRight(100, (value, elem) => value + elem)",
        Value.Number(106))
    }
    "calls the reducer in order from the end of the set to the start of the set" in {
      checkIter(
        Seq(Value.Number(1), Value.Number(2), Value.Number(3)),
        "iter.foldRight([], (value, elem) => value.concat([elem]))",
        Value.Array(Value.Number(3), Value.Number(2), Value.Number(1))
      )
    }
    "returns the seed for an empty set" in {
      checkIter(Seq(), "iter.foldRight(0, (a, b) => 100)", Value.Number(0))
    }
  }

  "forEach" - {
    testSig("forEach(callback: A => Any)")

    "runs the closure for every element in the iterable" in {
      evalOk(
        auth,
        "Collection.create({ name: 'User1' }); Collection.create({ name: 'User2' })")

      // I would use logs here, but as of writing, logs can be duplicated on
      // contention, because it uses mutable state in the interpreter. In order to
      // avoid a flaky test, this just creates documents in the `forEach` instead.

      // check arrays
      evalOk(
        auth,
        """|[1, 2, 3].forEach(v => {
           |  User1.create({ foo: v })
           |})
           |""".stripMargin)
      // .{} please!
      val elems1 = (evalOk(auth, "(User1.all() { foo }).paginate()") / "data")
        .asInstanceOf[Value.Array]
      elems1.elems.length shouldBe 3
      elems1.elems should contain(Value.Struct("foo" -> Value.Number(1)))
      elems1.elems should contain(Value.Struct("foo" -> Value.Number(2)))
      elems1.elems should contain(Value.Struct("foo" -> Value.Number(3)))

      // check sets
      evalOk(
        auth,
        """|[1, 2, 3].toSet().forEach(v => {
           |  User2.create({ foo: v })
           |})
           |""".stripMargin
      )
      val elems2 = (evalOk(auth, "(User2.all() { foo }).paginate()") / "data")
        .asInstanceOf[Value.Array]
      elems2.elems.length shouldBe 3
      elems2.elems should contain(Value.Struct("foo" -> Value.Number(1)))
      elems2.elems should contain(Value.Struct("foo" -> Value.Number(2)))
      elems2.elems should contain(Value.Struct("foo" -> Value.Number(3)))
    }

    "array.forEach will cause side effects, even if the result is ignored" in {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: 'User' })")
      evalOk(
        auth,
        """|[1, 2, 3, 4].forEach(v => {
           |  User.create({ foo: v })
           |})
           |
           |"some result"
           |""".stripMargin
      ) shouldBe Value.Str("some result")

      val elems1 = (evalOk(auth, "(User.all() { foo }).paginate()") / "data")
        .asInstanceOf[Value.Array]
      elems1.elems.length shouldBe 4
      elems1.elems should contain(Value.Struct("foo" -> Value.Number(1)))
      elems1.elems should contain(Value.Struct("foo" -> Value.Number(2)))
      elems1.elems should contain(Value.Struct("foo" -> Value.Number(3)))
      elems1.elems should contain(Value.Struct("foo" -> Value.Number(4)))
    }
    "set.forEach will cause side effects, even if the result is ignored" in {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: 'User' })")
      evalOk(
        auth,
        """|[1, 2, 3, 4].toSet().forEach(v => {
           |  User.create({ foo: v })
           |})
           |
           |"some result"
           |""".stripMargin
      ) shouldBe Value.Str("some result")

      val elems1 = (evalOk(auth, "(User.all() { foo }).paginate()") / "data")
        .asInstanceOf[Value.Array]
      elems1.elems.length shouldBe 4
      elems1.elems should contain(Value.Struct("foo" -> Value.Number(1)))
      elems1.elems should contain(Value.Struct("foo" -> Value.Number(2)))
      elems1.elems should contain(Value.Struct("foo" -> Value.Number(3)))
      elems1.elems should contain(Value.Struct("foo" -> Value.Number(4)))
    }
  }

  "isEmpty" - {
    testSig("isEmpty() => Boolean")

    "empty iterables return true" in {
      checkIter(Seq.empty, "iter.isEmpty()", Value.True)
      checkIter(
        Seq(Value.Number(1), Value.Number(2)),
        "iter.take(0).isEmpty()",
        Value.True)
    }
    "non-empty iterables return false" in {
      checkIter(Seq(Value.Number(1)), "iter.isEmpty()", Value.False)
      checkIter(Seq(Value.Number(1), Value.Number(2)), "iter.isEmpty()", Value.False)
    }
  }

  "includes" - {
    testSig("includes(element: A) => Boolean")

    "returns true if the value exists in the iterable" in {
      checkIter(
        Seq(Value.Number(1), Value.Number(2), Value.Number(3)),
        "iter.includes(2)",
        Value.True)
    }

    "returns false of the value doesn't exist in the iterable" in {
      checkIter(
        Seq(Value.Number(1), Value.Number(2), Value.Number(3)),
        "iter.includes(100)",
        Value.False)
    }
  }

  "last" - {
    testSig("last() => A | Null")

    "gets the last element in an iterable" in {
      checkIter(Seq(Value.Number(1)), "iter.last()", Value.Number(1))
      checkIter(
        Seq(Value.Number(1), Value.Number(2)),
        "iter.last()",
        Value.Number(2))
    }
    "returns null for an empty iterable" in {
      checkIter(Seq(), "iter.last()", Value.Null(Span.Null))
    }
  }

  "lastWhere" - {
    testSig("lastWhere(predicate: A => Boolean | Null) => A | Null")

    "gets the last element of an iterable that matches the given predicate" in {
      checkIter(
        Seq(Value.Number(1), Value.Number(2), Value.Number(3), Value.Number(4)),
        "iter.lastWhere(v => v > 2)",
        Value.Number(4))
    }
    "returns null for an empty iterable" in {
      checkIter(Seq(), "iter.lastWhere(v => v > 2)", Value.Null(Span.Null))
    }
    "returns null when no elements in the iterable match the predicate" in {
      checkIter(
        Seq(Value.Number(1), Value.Number(2), Value.Number(3), Value.Number(4)),
        "iter.lastWhere(v => v > 10)",
        Value.Null(Span.Null))
    }
  }

  "map" - {
    testArrSig("map(mapper: A => B) => Array<B>")
    testSetSig("map(mapper: A => B) => Set<B>")

    "converts every element using the closure" in {
      checkOk(
        auth,
        "[1, 2, 3, 4].map(v => v + 2)",
        Value.Array(
          Value.Number(3),
          Value.Number(4),
          Value.Number(5),
          Value.Number(6)))
      checkOk(
        auth,
        "[1, 2, 3, 4].toSet().map(v => v + 2).paginate()",
        page(
          Seq(Value.Number(3), Value.Number(4), Value.Number(5), Value.Number(6))))

      checkOk(auth, "[].map(v => v + 2)", Value.Array())
      checkOk(auth, "[].toSet().map(v => v + 2).paginate()", page(Seq()))
    }

    "array.map will cause side effects, even if the result is ignored" in {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: 'User' })")
      evalOk(
        auth,
        """|[1, 2, 3, 4].map(v => {
           |  User.create({ foo: v })
           |})
           |
           |"some result"
           |""".stripMargin
      ) shouldBe Value.Str("some result")

      val elems1 = (evalOk(auth, "(User.all() { foo }).paginate()") / "data")
        .asInstanceOf[Value.Array]
      elems1.elems.length shouldBe 4
      elems1.elems should contain(Value.Struct("foo" -> Value.Number(1)))
      elems1.elems should contain(Value.Struct("foo" -> Value.Number(2)))
      elems1.elems should contain(Value.Struct("foo" -> Value.Number(3)))
      elems1.elems should contain(Value.Struct("foo" -> Value.Number(4)))
    }
    "set.map doesn't cause side effects if the result is ignored" in {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: 'User' })")
      // This won't have any side effects
      evalOk(
        auth,
        """|[1, 2, 3, 4].toSet().map(v => {
           |  User.create({ foo: v })
           |})
           |
           |"some result"
           |""".stripMargin
      ) shouldBe Value.Str("some result")

      val elems1 = (evalOk(auth, "(User.all() { foo }).paginate()") / "data")
        .asInstanceOf[Value.Array]
      elems1.elems.length shouldBe 0
    }

    "changing the page size of a mapped set will change how many side effects it has" in {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: 'User' })")
      evalOk(
        auth,
        """|[1, 2, 3, 4].toSet().forEach(v => {
           |  User.create({ foo: v })
           |})
           |""".stripMargin
      )

      val elems1 = (evalOk(auth, "(User.all() { foo }).paginate()") / "data")
        .asInstanceOf[Value.Array]
      elems1.elems.length shouldBe 4
      elems1.elems should contain(Value.Struct("foo" -> Value.Number(1)))
      elems1.elems should contain(Value.Struct("foo" -> Value.Number(2)))
      elems1.elems should contain(Value.Struct("foo" -> Value.Number(3)))
      elems1.elems should contain(Value.Struct("foo" -> Value.Number(4)))
    }
  }

  "nonEmpty" - {
    testSig("nonEmpty() => Boolean")

    "empty iterables return false" in {
      checkIter(Seq.empty, "iter.nonEmpty()", Value.False)
      checkIter(
        Seq(Value.Number(1), Value.Number(2)),
        "iter.take(0).nonEmpty()",
        Value.False)
    }
    "non-empty iterables return true" in {
      checkIter(Seq(Value.Number(1)), "iter.nonEmpty()", Value.True)
      checkIter(Seq(Value.Number(1), Value.Number(2)), "iter.nonEmpty()", Value.True)
    }
  }

  "reduce" - {
    testSig("reduce(reducer: (A, A) => A) => A | Null")

    "calls the reducer for every element in the set" in {
      checkIter(
        Seq(Value.Number(1), Value.Number(2), Value.Number(3)),
        "iter.reduce((value, elem) => value + elem)",
        Value.Number(6))
    }
    "calls the reducer in order from the start to the end of the set" in {
      checkIter(
        Seq(Value.Number(1), Value.Number(2), Value.Number(3)),
        "iter.map(v => [v]).reduce((acc, elem) => acc.concat(elem))",
        Value.Array(Value.Number(1), Value.Number(2), Value.Number(3))
      )
    }
    "does not call the reducer for a set of one element" in {
      checkIter(
        Seq(Value.Number(1)),
        "iter.reduce((acc, elem) => 500)",
        Value.Number(1))
    }
    "returns null for an empty set" in {
      checkIter(Seq(), "iter.reduce((acc, elem) => 100)", Value.Null(Span.Null))
    }

    "typechecks with intermediate tuple" in {
      checkOk(
        auth,
        "Set.sequence(1, 10).map(i => [i]).reduce((a, b) => a.concat(b))[1]",
        Value.Int(2))
    }
  }

  "reduceRight" - {
    testSig("reduceRight(reducer: (A, A) => A) => A | Null")

    "calls the reducer for every element in the set" in {
      checkIter(
        Seq(Value.Number(1), Value.Number(2), Value.Number(3)),
        "iter.reduceRight((acc, elem) => acc + elem)",
        Value.Number(6))
    }
    "calls the reducer in order from the end of the set to the start of the set" in {
      checkIter(
        Seq(Value.Number(1), Value.Number(2), Value.Number(3)),
        "iter.map(v => [v]).reduceRight((acc, elem) => acc.concat(elem))",
        Value.Array(Value.Number(3), Value.Number(2), Value.Number(1))
      )
    }
    "does not call the reducer for a set of one element" in {
      checkIter(
        Seq(Value.Number(1)),
        "iter.reduceRight((acc, elem) => 500)",
        Value.Number(1))
    }
    "returns null for an empty set" in {
      checkIter(Seq(), "iter.reduceRight((a, b) => 100)", Value.Null(Span.Null))
    }
  }

  "reverse" - {
    testArrSig("reverse() => Array<A>")
    testSetSig("reverse() => Set<A>")

    "returns the iterable reversed" in {
      checkOk(
        auth,
        "[1, 2, 3].reverse()",
        Value.Array(Value.Number(3), Value.Number(2), Value.Number(1)))
      checkOk(
        auth,
        "[1, 2, 3].toSet().reverse().toArray()",
        Value
          .Array(Value.Number(3), Value.Number(2), Value.Number(1)))
    }

    "reverse().reverse() returns the original" in {
      checkOk(
        auth,
        "[1, 2, 3].reverse().reverse()",
        Value.Array(Value.Number(1), Value.Number(2), Value.Number(3)))
      checkOk(
        auth,
        "[1, 2, 3].toSet().reverse().reverse().toArray()",
        Value
          .Array(Value.Number(1), Value.Number(2), Value.Number(3)))
    }
  }

  "order" - {
    testArrSig("order(...ordering: (A => Any) | {}) => Array<A>")
    testSetSig("order(...ordering: (A => Any) | {}) => Set<A>")

    "sorts iterables with no ordering" in {
      evalOk(auth, "[3, 2, 4, 1].toSet().order().toArray()") shouldBe Value.Array(
        Value.Number(1),
        Value.Number(2),
        Value.Number(3),
        Value.Number(4))

      evalOk(auth, "[3, 2, 4, 1].order()") shouldBe Value.Array(
        Value.Number(1),
        Value.Number(2),
        Value.Number(3),
        Value.Number(4))
    }

    "sorts iterables with the given ordering" in {
      checkOk(
        auth,
        "[1, 2, 3, 4].toSet().order(asc(v => v)).paginate()",
        page(
          Seq(Value.Number(1), Value.Number(2), Value.Number(3), Value.Number(4))))
      checkOk(
        auth,
        "[1, 2, 3, 4].toSet().order(desc(v => v)).paginate()",
        page(
          Seq(Value.Number(4), Value.Number(3), Value.Number(2), Value.Number(1))))

      checkOk(
        auth,
        "[1, 2, 3, 4].order(asc(v => v))",
        Value.Array(
          Value.Number(1),
          Value.Number(2),
          Value.Number(3),
          Value.Number(4)))
      checkOk(
        auth,
        "[1, 2, 3, 4].order(desc(v => v))",
        Value.Array(
          Value.Number(4),
          Value.Number(3),
          Value.Number(2),
          Value.Number(1)))

      // make sure it's actually sorting things
      checkOk(
        auth,
        "[3, 1, 2, 4].toSet().order(asc(v => v)).paginate()",
        page(
          Seq(Value.Number(1), Value.Number(2), Value.Number(3), Value.Number(4))))
      checkOk(
        auth,
        "[3, 1, 2, 4].toSet().order(desc(v => v)).paginate()",
        page(
          Seq(Value.Number(4), Value.Number(3), Value.Number(2), Value.Number(1))))

      checkOk(
        auth,
        "[3, 1, 2, 4].order(asc(v => v))",
        Value.Array(
          Value.Number(1),
          Value.Number(2),
          Value.Number(3),
          Value.Number(4)))
      checkOk(
        auth,
        "[3, 1, 2, 4].order(desc(v => v))",
        Value.Array(
          Value.Number(4),
          Value.Number(3),
          Value.Number(2),
          Value.Number(1)))
    }

    "sorts iterables when given multiple orderings" in {
      val objects = """|[
                       |  {
                       |    name: "Bob",
                       |    age: 30,
                       |  },
                       |  {
                       |    name: "Dave",
                       |    age: 20,
                       |  },
                       |  {
                       |    name: "Alice",
                       |    age: 20,
                       |  },
                       |  {
                       |    name: "Carol",
                       |    age: 30,
                       |  },
                       |]""".stripMargin
      val sortedObjects = Seq(
        Value.Struct(
          "name" -> Value.Str("Alice"),
          "age" -> Value.Int(20)
        ),
        Value.Struct(
          "name" -> Value.Str("Dave"),
          "age" -> Value.Int(20)
        ),
        Value.Struct(
          "name" -> Value.Str("Bob"),
          "age" -> Value.Int(30)
        ),
        Value.Struct(
          "name" -> Value.Str("Carol"),
          "age" -> Value.Int(30)
        )
      )

      checkOk(
        auth,
        s"$objects.order(asc(.age), asc(.name))",
        Value.Array(sortedObjects: _*))
      checkOk(
        auth,
        s"$objects.toSet().order(asc(.age), asc(.name)).paginate()",
        page(sortedObjects))
    }
  }

  "take" - {
    testArrSig("take(limit: Number) => Array<A>")
    testSetSig("take(limit: Number) => Set<A>")

    "takes n elements from the iterable" in {
      checkOk(auth, "[1].take(1)", Value.Array(Value.Number(1)))
      checkOk(auth, "[1, 2].take(1)", Value.Array(Value.Number(1)))

      checkOk(auth, "[1].toSet().take(1).toArray()", Value.Array(Value.Number(1)))
      checkOk(auth, "[1, 2].toSet().take(1).toArray()", Value.Array(Value.Number(1)))
    }

    "returns an empty iterable when the limit is 0" in {
      checkOk(auth, "[1].take(0)", Value.Array())
      checkOk(auth, "[1, 2].take(0)", Value.Array())

      checkOk(auth, "[1].toSet().take(0).toArray()", Value.Array())
      checkOk(auth, "[1, 2].toSet().take(0).toArray()", Value.Array())
    }

    "returns the entire set when the limit is larger than the set" in {
      checkOk(auth, "[1].take(10)", Value.Array(Value.Number(1)))
      checkOk(auth, "[1, 2].take(10)", Value.Array(Value.Number(1), Value.Number(2)))

      checkOk(auth, "[1].toSet().take(10).toArray()", Value.Array(Value.Number(1)))
      checkOk(
        auth,
        "[1, 2].toSet().take(10).toArray()",
        Value.Array(Value.Number(1), Value.Number(2)))
    }
  }

  "toSet" - {
    testSig("toSet() => Set<A>")

    "converts iterable to a set" in {
      checkIter(Seq(), "iter.toSet().paginate()", page(Seq()))
      checkIter(
        Seq(Value.Number(1)),
        "iter.toSet().paginate()",
        page(Seq(Value.Number(1))))
      checkIter(
        Seq(Value.Number(1), Value.Number(2)),
        "iter.toSet().paginate()",
        page(Seq(Value.Number(1), Value.Number(2))))
    }
  }

  "where" - {
    testArrSig("where(predicate: A => Boolean | Null) => Array<A>")
    testSetSig("where(predicate: A => Boolean | Null) => Set<A>")

    "filters iterables with the given predicate" in {
      checkOk(
        auth,
        "[1, 2, 3, 4].where(v => v > 2)",
        Value.Array(Value.Number(3), Value.Number(4)))
      checkOk(
        auth,
        "[1, 2, 3, 4].toSet().where(v => v > 2).paginate()",
        page(Seq(Value.Number(3), Value.Number(4))))

      checkOk(auth, "[].where(v => v > 2)", Value.Array())
      checkOk(auth, "[].toSet().where(v => v > 2).paginate()", page(Seq()))
    }

    "null acts like false in the predicate" in {
      checkOk(
        auth,
        "[true, false, null].where(v => v)",
        Value.Array(Value.Boolean(true)))
    }
  }

  "toString" - {
    testSig("toString() => String")

    "returns a debugged version of an array" in {
      checkOk(auth, "[1, 2].toString()", Value.Str("[1, 2]"))
      checkOk(auth, "['a', 'b'].toString()", Value.Str("[\"a\", \"b\"]"))
    }

    "returns a placeholder for sets" in {
      checkOk(auth, "[1, 2].toSet().toString()", Value.Str("[set]"))
      checkOk(auth, "['a', 'b'].toSet().toString()", Value.Str("[set]"))
    }
  }
}
