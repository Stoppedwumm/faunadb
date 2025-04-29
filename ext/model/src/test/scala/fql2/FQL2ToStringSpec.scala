package fauna.model.test

import fauna.model.runtime.fql2.FQLInterpreter
import fauna.model.runtime.fql2.ToString._
import fauna.repo.values.Value
import org.scalactic.source.Position

class FQL2ToStringSpec extends FQL2Spec {
  val auth = newDB

  override def beforeAll() = {
    evalOk(auth, "Collection.create({ name: 'User' })")
  }

  "toString" - {
    "works" in {
      evalOk(auth, "1.toString()") shouldBe Value.Str("1")
      evalOk(auth, "2.0.toString()") shouldBe Value.Str("2")
      evalOk(auth, "2.5.toString()") shouldBe Value.Str("2.5")
      evalOk(auth, "'a'.toString()") shouldBe Value.Str("a")
      evalOk(auth, "Time('1970-01-01T01:02:03Z').toString()") shouldBe Value.Str(
        "1970-01-01T01:02:03Z")
      evalOk(auth, "Date('1973-02-03').toString()") shouldBe Value.Str("1973-02-03")

      evalOk(auth, "true.toString()") shouldBe Value.Str("true")
      evalOk(auth, "false.toString()") shouldBe Value.Str("false")
      evalOk(auth, "null.toString()") shouldBe Value.Str("null")
      // these just use debug formatting
      evalOk(auth, "[1, 2].toString()") shouldBe Value.Str("[1, 2]")
      evalOk(auth, "['a', 'b'].toString()") shouldBe Value.Str("[\"a\", \"b\"]")

      evalOk(auth, "User.toString()") shouldBe Value.Str("User")
      evalOk(auth, "Object.toString(User.create({ id: '3' }))") shouldBe Value.Str(
        """{ id: ID("3"), coll: User, ts: TransactionTime() }""")
      evalOk(auth, "Object.toString(User.all())") shouldBe Value.Str("[set]")
      evalOk(auth, "Object.toString(User.all().toStream())") shouldBe Value.Str(
        "[event source]")
    }
  }

  "toDebugString" - {
    "evals to the original expr" in {
      def roundtrip(original: String)(implicit pos: Position) = {
        val v = evalOk(auth, original)
        val c = new FQLInterpreter(auth)
        val debugged = ctx ! v.toDebugString(c)
        debugged shouldBe original
      }

      roundtrip("1")
      roundtrip("2.0")
      roundtrip("true")
      roundtrip("\"hello\"")
      roundtrip("Time(\"1970-01-01T01:02:03Z\")")
      roundtrip("Date(\"1973-02-03\")")
      roundtrip("null")
      roundtrip("true")
      roundtrip("false")
      roundtrip("[1, 2]")
      roundtrip("[\"a\", \"b\", 3]")
      roundtrip("{ a: 2, b: 3 }")
      roundtrip("{ a: 2, \"3\": 3 }")
      roundtrip("Math")
      roundtrip("TransactionTime()")
    }
  }

  "misc types" - {
    "converts doc ids" in {
      val id = evalOk(auth, "User.create({}).id").asInstanceOf[Value.ID]
      val c = new FQLInterpreter(auth)
      ctx ! id.toDisplayString(c) shouldBe id.value.toString
      ctx ! id.toDebugString(c) shouldBe s"ID(\"${id.value.toString}\")"
    }

    "converts txn time" in {
      val id = evalOk(auth, "User.create({}).ts")
      val c = new FQLInterpreter(auth)
      ctx ! id.toDisplayString(c) shouldBe "[transaction time]"
      ctx ! id.toDebugString(c) shouldBe "TransactionTime()"
    }

    "converts docs" in {
      val out = eval(auth, "User.create({ id: '1234' })")
      val res = out.res.getOrElse(fail())
      val doc = res.value
      val c = new FQLInterpreter(auth)
      val expected = s"""{ id: ID("1234"), coll: User, ts: Time("${res.ts}") }"""
      ctx ! doc.toDisplayString(c) shouldBe expected
      ctx ! doc.toDebugString(c) shouldBe expected
    }

    "doesn't choke on ref cycles" in {
      val out =
        eval(auth, "User.create({ id: '9999', x: 'a', self: User.byId('9999') })")
      val res = out.res.getOrElse(fail())
      val doc = res.value
      val c = new FQLInterpreter(auth)
      val expected =
        s"""{ id: ID("9999"), coll: User, ts: Time("${res.ts}"), x: "a", self: [doc 9999 in User] }"""
      ctx ! doc.toDisplayString(c) shouldBe expected
      ctx ! doc.toDebugString(c) shouldBe expected
    }

    "converts sets" in {
      evalOk(auth, "Collection.create({ name: 'Foo' })")
      evalOk(auth, "Foo.create({ a: 1 })")
      evalOk(auth, "Foo.create({ a: 2 })")
      evalOk(auth, "Foo.create({ a: 3 })")
      val doc = evalOk(auth, "Foo.all()")
      val c = new FQLInterpreter(auth)
      ctx ! doc.toDisplayString(c) shouldBe "[set]"
      ctx ! doc.toDebugString(c) shouldBe "[set]"
    }

    "converts streams" in {
      val stream = evalOk(auth, "User.all().toStream()")
      val c = new FQLInterpreter(auth)
      ctx ! stream.toDisplayString(c) shouldBe "[event source]"
      ctx ! stream.toDebugString(c) shouldBe "[event source]"
    }

    "converts partials" in {
      evalOk(
        auth,
        "Collection.create({ name: 'Author', indexes: { byName: { values: [{ field: 'name' }] } } })")
      evalOk(auth, "Author.create({ name: 'Alice', extra: 3 })")
      evalOk(auth, "Author.create({ name: 'Bob', extra: 4 })")
      evalOk(auth, "Author.create({ name: 'Carol', extra: 5 })")

      val doc = evalOk(auth, "Author.byName().map(.data).first()")
      // just making sure its a partial
      doc shouldBe a[Value.Struct.Partial]
      val c = new FQLInterpreter(auth)
      val expected = s"""{ name: "Alice", extra: 3 }"""
      ctx ! doc.toDisplayString(c) shouldBe expected
      ctx ! doc.toDebugString(c) shouldBe expected
    }
  }
}
