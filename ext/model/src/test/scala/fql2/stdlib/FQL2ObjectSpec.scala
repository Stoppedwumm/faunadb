package fauna.model.test

import fauna.model.runtime.fql2.{ stdlib, QueryCheckFailure, QueryRuntimeFailure }
import fauna.repo.values.Value
import fql.ast.{ Span, Src }
import fql.error.TypeError
import org.scalactic.source.Position
import scala.collection.immutable.{ ArraySeq, SeqMap }

class FQL2ObjectSpec extends FQL2StdlibHelperSpec("object", stdlib.StructPrototype) {
  val auth = newDB

  def span(start: Int, end: Int) = Span(start, end, Src.Query(""))

  def testStaticSig(sig: String*)(implicit pos: Position) =
    s"has signature $sig" inWithTest { test =>
      test.signatures = sig
      lookupSig(stdlib.ObjectCompanion, test.function) shouldBe sig
    }

  // Be brief.
  def bV(b: Boolean) = Value.Boolean(b)
  def intV(i: Int) = Value.Int(i)
  def strV(s: String) = Value.Str(s)
  def arrV(vs: Value*) = Value.Array(ArraySeq(vs: _*))
  def structV(kvs: (String, Value)*) =
    Value.Struct(SeqMap(kvs: _*))
  def sva(ss: String*): Value.Array =
    Value.Array(ArraySeq.from(ss map Value.Str))

  "[]" - {
    // FIXME: This is wrong, but what should it be? `[key: string] => any`?
    testSig("[](key: String) => Any")

    "works" in {
      checkOk(auth, "{ foo: 3 }['foo']", intV(3))
      checkOk(auth, "{ '3': 5 }['3']", intV(5))

      checkErr(
        auth,
        "{ foo: 3 }['baz']",
        QueryCheckFailure(
          Seq(
            TypeError("Type `{ foo: 3 }` does not have field `baz`", span(11, 16)))))
      checkOk(auth, "{ let x = 'baz'; { foo: 3 }[x] }", Value.Null(Span.Null))
    }

    "object with '[]' key" in {
      checkErr(
        auth,
        "{}['[]']",
        QueryCheckFailure(
          Seq(TypeError("Type `{}` does not have field `[]`", span(3, 7)))))
      checkOk(auth, "{ let x = '[]'; {}[x] }", Value.Null(Span.Null))
      checkOk(auth, "{ '[]': 3 }['[]']", intV(3))
    }
  }

  "keys" - {
    // FIXME: should be `keys(object: { *: Any }) => Array<String>`
    testStaticSig("keys(object: Any) => Array<String>")

    "works" in {
      // Basic.
      checkOk(auth, "Object.keys({ })", sva())
      checkOk(auth, "Object.keys({ 'a' : 0 })", sva("a"))
      checkOk(auth, "Object.keys({ 'a' : 0, 'b' : 0 })", sva("a", "b"))
      checkOk(auth, "Object.keys({ 'a' : 0, 'b' : 0, 'c' : 0 })", sva("a", "b", "c"))

      // Ordering.
      checkOk(auth, "Object.keys({ 'a' : 0, 'b': 0 })", sva("a", "b"))
      checkOk(auth, "Object.keys({ 'b' : 0, 'a': 0 })", sva("b", "a"))
    }

    "allows docs" in {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: 'User' })")

      checkOk(
        auth,
        "Object.keys(User.create({ id: 1234 }))",
        arrV(strV("id"), strV("coll"), strV("ts")))
    }

    "disallows null docs" in {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: 'User' })")

      evalErr(
        auth,
        "Object.keys(User.byId('0'))",
        typecheck = false
      ) shouldBe QueryRuntimeFailure(
        "invalid_argument",
        "invalid argument `object`: collection `User` does not contain document with id 0.",
        Span(11, 27, Src.Query(""))
      )

      // FIXME: Static signature should disallow this.
      pendingUntilFixed {
        checkErr(
          auth,
          "Object.keys(User.byId('0'))",
          QueryCheckFailure(
            Seq(
              TypeError(
                "Type `Null` is not a subtype of `{ *: Any }`",
                Span(15, 29, Src.Query("")))))
        )
      }
    }
  }

  "values" - {
    testStaticSig("values(object: { *: A }) => Array<A>")

    "works" in {
      checkOk(auth, "Object.values({})", arrV())
      checkOk(auth, "Object.values({ a: 0 })", arrV(intV(0)))
      checkOk(auth, "Object.values({ a: 0, b: 'x' })", arrV(intV(0), strV("x")))
    }

    "objects will remain ordered" in {
      checkOk(auth, "Object.values({ a: 0, b: 1 })", arrV(intV(0), intV(1)))
      checkOk(auth, "Object.values({ b: 1, a: 0 })", arrV(intV(1), intV(0)))
    }

    "allows docs" in {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: 'User' })")

      checkOk(
        auth,
        "Object.values(User.create({ id: 1234 })).includes(ID(1234))",
        Value.True)
    }

    "disallows null docs" in {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: 'User' })")

      checkErr(
        auth,
        "Object.values(User.byId('0'))",
        QueryCheckFailure(
          Seq(
            TypeError(
              "Type `Null` is not a subtype of `{ *: Any }`",
              Span(14, 28, Src.Query("")))))
      )

      evalErr(
        auth,
        "Object.values(User.byId('0'))",
        typecheck = false
      ) shouldBe QueryRuntimeFailure(
        "invalid_argument",
        "invalid argument `object`: collection `User` does not contain document with id 0.",
        Span(13, 29, Src.Query(""))
      )
    }
  }

  "entries" - {
    testStaticSig("entries(object: { *: A }) => Array<[String, A]>")

    "works" in {
      checkOk(auth, "Object.entries({})", arrV())
      checkOk(auth, "Object.entries({ a: 0 })", arrV(arrV(strV("a"), intV(0))))
      checkOk(
        auth,
        "Object.entries({ a: 0, b: 'x' })",
        arrV(arrV(strV("a"), intV(0)), arrV(strV("b"), strV("x"))))
    }

    "objects will remain ordered" in {
      checkOk(
        auth,
        "Object.entries({ a: 0, b: 1 })",
        arrV(arrV(strV("a"), intV(0)), arrV(strV("b"), intV(1))))
      checkOk(
        auth,
        "Object.entries({ b: 1, a: 0 })",
        arrV(arrV(strV("b"), intV(1)), arrV(strV("a"), intV(0))))
    }

    "allows docs" in {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: 'User' })")

      checkOk(
        auth,
        "Object.entries(User.create({ id: 1234 })).firstWhere(v => v[0] == 'id')",
        arrV(strV("id"), Value.ID(1234)))
    }

    "disallows null docs" in {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: 'User' })")

      checkErr(
        auth,
        "Object.entries(User.byId('0'))",
        QueryCheckFailure(
          Seq(
            TypeError(
              "Type `Null` is not a subtype of `{ *: Any }`",
              Span(15, 29, Src.Query("")))))
      )

      evalErr(
        auth,
        "Object.entries(User.byId('0'))",
        typecheck = false
      ) shouldBe QueryRuntimeFailure(
        "invalid_argument",
        "invalid argument `object`: collection `User` does not contain document with id 0.",
        Span(14, 30, Src.Query(""))
      )
    }

    "uncapitalize works" in {
      import fauna.model.runtime.fql2.stdlib.ObjectCompanion.uncapitalize

      uncapitalize("") shouldBe ""
      uncapitalize("hello") shouldBe "hello"
      uncapitalize("Hello") shouldBe "hello"
      uncapitalize("HELLO") shouldBe "hELLO"
    }
  }

  "fromEntries" - {
    testStaticSig("fromEntries(entries: Array<[String, A]>) => { *: A }")

    "works" in {
      // Basic.
      checkOk(auth, "Object.fromEntries([])", structV())
      checkOk(
        auth,
        "Object.fromEntries([['a', 0], ['b', 'x']])",
        structV("a" -> intV(0), "b" -> strV("x")))
    }

    "rejects invalid inputs" in {
      // Errors.
      checkErr(
        auth,
        "Object.fromEntries([['a', 0], 'x'])",
        QueryCheckFailure(
          List(
            TypeError(
              "Type `String` is not a subtype of `[String, Any]`",
              Span(30, 33, Src.Query("")))))
      )
      checkErr(
        auth,
        "Object.fromEntries([['a', 0], []])",
        QueryCheckFailure(
          List(
            TypeError(
              "Tuple does not contain enough elements. Expected 2, received 0",
              Span(30, 32, Src.Query(""))
            )))
      )
      checkErr(
        auth,
        "Object.fromEntries([['a', 0], ['a']])",
        QueryCheckFailure(
          List(
            TypeError(
              "Tuple does not contain enough elements. Expected 2, received 1",
              Span(30, 35, Src.Query(""))
            )))
      )
      checkErr(
        auth,
        "Object.fromEntries([['a', 0], ['a', 0, 1]])",
        QueryCheckFailure(
          List(
            TypeError(
              "Tuple contains too many elements. Expected 2, received 3",
              Span(30, 41, Src.Query(""))
            )))
      )
      checkErr(
        auth,
        "Object.fromEntries([['a', 0], [0, 0]])",
        QueryCheckFailure(
          List(
            TypeError(
              """Type `Int` is not a subtype of `String`""",
              Span(31, 32, Src.Query("")))))
      )

      // Ordering.
      checkOk(
        auth,
        "Object.fromEntries([['a', 0], ['b', 1]])",
        structV("a" -> intV(0), "b" -> intV(1)))
      checkOk(
        auth,
        "Object.fromEntries([['b', 1], ['a', 0]])",
        structV("b" -> intV(1), "a" -> intV(0)))

      // Redundant keys.
      checkOk(
        auth,
        "Object.fromEntries([['a', 0], ['b', 'x'], ['a', 2], ['b', 'y'], ['a', 3]])",
        structV("a" -> intV(3), "b" -> strV("y")))
    }
  }

  "assign" - {
    testStaticSig("assign(destination: { *: A }, source: { *: B }) => { *: A | B }")

    "works" in {
      // Basic.
      checkOk(auth, "Object.assign({}, {})", structV())
      checkOk(auth, "Object.assign({ 'a' : 0 }, {})", structV("a" -> intV(0)))
      checkOk(auth, "Object.assign({}, { 'a' : 0 })", structV("a" -> intV(0)))
      checkOk(
        auth,
        "Object.assign({ 'a' : 0 }, { 'b' : 'x' })",
        structV("a" -> intV(0), "b" -> strV("x")))

      // Overrides.
      checkOk(
        auth,
        "Object.assign({ 'a' : 0, 'b' : 1 }, { 'b' : 'x' })",
        structV("a" -> intV(0), "b" -> strV("x")))

      // Ordering.
      checkOk(
        auth,
        "Object.assign({ 'a' : 0, 'b' : 1 }, { 'b' : 1, 'a' : 0 })",
        structV("a" -> intV(0), "b" -> intV(1)))
    }

    "allows docs" in {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: 'User' })")

      checkOk(
        auth,
        "Object.assign(User.create({ id: 1234 }), { ts: 3, coll: 4 })",
        structV("id" -> Value.ID(1234), "coll" -> intV(4), "ts" -> intV(3)))

      checkOk(
        auth,
        "Object.assign({ foo: 3 }, User.create({ id: 1235 })) { foo, id }",
        structV("id" -> Value.ID(1235), "foo" -> intV(3)))
    }

    "disallows null docs" in {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: 'User' })")

      checkErr(
        auth,
        "Object.assign(User.byId('0'), { ts: 3, coll: 4 })",
        QueryCheckFailure(
          Seq(
            TypeError(
              "Type `Null` is not a subtype of `{ *: Any }`",
              Span(14, 28, Src.Query("")))))
      )

      checkErr(
        auth,
        "Object.assign({ foo: 3 }, User.byId('0'))",
        QueryCheckFailure(
          Seq(
            TypeError(
              "Type `Null` is not a subtype of `{ *: Any }`",
              Span(26, 40, Src.Query("")))))
      )

      evalErr(
        auth,
        "Object.assign(User.byId('0'), { ts: 3, coll: 4 })",
        typecheck = false
      ) shouldBe QueryRuntimeFailure(
        "invalid_argument",
        "invalid argument `destination`: collection `User` does not contain document with id 0.",
        Span(13, 49, Src.Query(""))
      )

      evalErr(
        auth,
        "Object.assign({ foo: 3 }, User.byId('0'))",
        typecheck = false
      ) shouldBe QueryRuntimeFailure(
        "invalid_argument",
        "invalid argument `source`: collection `User` does not contain document with id 0.",
        Span(13, 41, Src.Query(""))
      )
    }
  }

  "select" - {
    testStaticSig("select(object: { *: Any }, path: Array<String>) => Any")

    "works" in {
      // Basic.
      checkOk(auth, "Object.select({}, ['a']) ?? 0", (intV(0)))
      checkOk(auth, "Object.select({ a: null }, ['a']) == null", (bV(true)))
      checkOk(auth, "Object.select({ a : 'x' }, ['a'])", (strV("x")))
      checkOk(
        auth,
        "Object.select({ a : { b : 'x' } }, ['a'])",
        (structV("b" -> strV("x"))))
      checkOk(auth, "Object.select({ a : { b : 'x' } }, ['a', 'b'])", (strV("x")))
      checkOk(auth, "Object.select({ a : { b : 'x' } }, ['a', 'c']) ?? 0", (intV(0)))
      checkOk(
        auth,
        "Object.select({ a : { b : 'x' } }, ['a', 'b', 'c']) ?? 0",
        (intV(0)))
    }

    "rejects invalid paths" in {
      checkErr(
        auth,
        "Object.select({ a : { b : 'x' } }, ['a', 1])",
        QueryCheckFailure(
          List(
            TypeError(
              "Type `Int` is not a subtype of `String`",
              Span(41, 42, Src.Query("")))))
      )
    }
  }

  "hasPath" - {
    testStaticSig("hasPath(object: { *: Any }, path: Array<String>) => Boolean")

    "works" in {
      checkOk(auth, "Object.hasPath({}, ['a'])", (bV(false)))
      checkOk(auth, "Object.hasPath({ a: null }, ['a'])", (bV(true)))
      checkOk(auth, "Object.hasPath({ a : 'x' }, ['a'])", (bV(true)))
      checkOk(auth, "Object.hasPath({ a : { b : 'x' } }, ['a'])", (bV(true)))
      checkOk(auth, "Object.hasPath({ a : { b : 'x' } }, ['a', 'b'])", (bV(true)))
      checkOk(auth, "Object.hasPath({ a : { b : 'x' } }, ['a', 'c'])", (bV(false)))
      checkOk(
        auth,
        "Object.hasPath({ a : { b : 'x' } }, ['a', 'b', 'c'])",
        (bV(false)))
    }

    "rejects invalid paths" in {
      checkErr(
        auth,
        "Object.hasPath({ a : { b : 'x' } }, ['a', 1])",
        QueryCheckFailure(
          List(TypeError("Type `Int` is not a subtype of `String`", span(42, 43)))))
    }
  }

  "partials" - {
    "work" in {
      evalOk(
        auth,
        "Collection.create({ name: 'Test', indexes: { byX: { values: [{ field: 'x' }] } } })")
      evalOk(auth, "Test.create({ x: 'a', y: 0 })")

      val partial = "Test.byX().map(.data).first()!"
      evalOk(auth, s"$partial") shouldBe a[Value.Struct.Partial]

      // Basic: functions work directly on partial values.
      checkOk(auth, s"Object.keys($partial)", arrV(strV("x"), strV("y")))
      checkOk(auth, s"Object.values($partial)", arrV(strV("a"), intV(0)))
      checkOk(
        auth,
        s"Object.entries($partial)",
        arrV(arrV(strV("x"), strV("a")), arrV(strV("y"), intV(0))))

      val exp = structV(("x", strV("a")), ("y", intV(0)), ("z", intV(0)))
      checkOk(auth, s"Object.assign($partial, { 'z' : 0 })", exp)
      checkOk(auth, s"Object.assign({ 'z' : 0 }, $partial)", exp)

      checkOk(
        auth,
        s"Object.assign($partial, { 'y' : 1 })",
        structV(("x", strV("a")), ("y", intV(1))))
      checkOk(
        auth,
        s"Object.assign({ 'y' : 1 }, $partial)",
        structV(("x", strV("a")), ("y", intV(0))))

      checkOk(auth, s"$partial['y']", intV(0))
      checkOk(
        auth,
        s"$partial['z']",
        Value.Null.noSuchElement("no element found", Span.Null))
      checkOk(auth, s"Object.select($partial, ['y'])", intV(0))
      checkOk(
        auth,
        s"Object.select($partial, ['z'])",
        Value.Null.noSuchElement("no element found", Span.Null))

      checkOk(auth, s"Object.hasPath($partial, ['y'])", bV(true))
      checkOk(auth, s"Object.hasPath($partial, ['z'])", bV(false))

      checkOk(auth, s"Object.toString($partial)", Value.Str("""{ x: "a", y: 0 }"""))

      // Advanced: functions work on values with nested partials and with partials
      // with nested structs.
      val lit = "{ x: 'b', y: { z: [{ vv: 0, ww: 1 }]} }"
      evalOk(auth, s"Test.create($lit)")

      val partial2 = "Test.byX().map(.data).last()!"
      evalOk(auth, s"$partial2") shouldBe a[Value.Struct.Partial]

      val nested = s"{ a: 0, b: $partial2 }"
      checkOk(auth, s"Object.keys($nested)", arrV(strV("a"), strV("b")))
      checkOk(auth, s"Object.keys($nested.b) == Object.keys($partial2)", bV(true))
      checkOk(
        auth,
        s"Object.keys($nested.b.y.z.at(0))",
        arrV(strV("vv"), strV("ww")))

      checkOk(auth, s"Object.values($nested.b.y.z.at(0))", arrV(intV(0), intV(1)))
      checkOk(
        auth,
        s"Object.values($nested.b) == Object.values($partial2)",
        bV(true))

      checkOk(
        auth,
        s"Object.entries($nested.b.y)",
        arrV(arrV(strV("z"), arrV(structV(("vv", intV(0)), ("ww", intV(1)))))))
      checkOk(
        auth,
        s"Object.entries($nested.b) == Object.entries($partial2)",
        bV(true))

      checkOk(
        auth,
        s"Object.assign($nested, { b: 1, c: 2 }) == { a: 0, b: 1, c: 2}",
        bV(true))
      checkOk(
        auth,
        s"Object.assign({ b: 1, c: 2 }, $nested) == { a:0, b: $partial2, c: 2}",
        bV(true))

      checkOk(
        auth,
        s"Object.select($nested, ['b', 'y', 'z']).at(0).vv == 0",
        bV(true))
      checkOk(
        auth,
        s"Object.select($nested, ['b', 'y', 'zz'])",
        Value.Null.noSuchElement("no element found", Span.Null))
      checkOk(auth, s"Object.select($nested, ['b']) == $partial2", bV(true))

      checkOk(auth, s"Object.hasPath($nested, ['b', 'y', 'z'])", bV(true))
      checkOk(auth, s"Object.hasPath($nested, ['b', 'y', 'zz'])", bV(false))

      checkOk(
        auth,
        s"Object.toString($nested)",
        strV("""{ a: 0, b: { x: "b", y: { z: [{ vv: 0, ww: 1 }] } } }"""))
    }
  }
}
