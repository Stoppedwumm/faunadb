package fauna.model.test

import fauna.atoms.ScopeID
import fauna.auth.{ AdminPermissions, Auth }
import fauna.lang.Timestamp
import fauna.model.runtime.fql2.{ FQLInterpreter, ReadBroker, Result }
import fauna.model.runtime.fql2.serialization._
import fauna.model.runtime.fql2.FQLInterpreter.TypeMode
import fauna.repo.query.Query
import fauna.repo.values.Value
import fql.ast.{ Name, Span }
import java.time.{ Instant, LocalDate }
import scala.collection.immutable.ArraySeq
import scala.collection.SeqMap

class FQL2ValueDecoratedSpec extends FQL2WithV4Spec {
  private def evalEncode(auth: Auth, query: String): String = {
    val expr = parseOk(query)
    val intp = new FQLInterpreter(auth)
    val q = intp
      .evalWithTypecheck(expr, Map.empty, TypeMode.InferType)
      .flatMap { case res =>
        intp.runPostEvalHooks().map {
          case Result.Ok(v) =>
            (intp, res.getOrElse(fail(s"unexpected eval error $v"))._1)
          case Result.Err(err) => fail(s"unexpected post-eval hook error: $err")
        }
      }
      .flatMap { case (intp, value) =>
        encodeQ(intp, value)
      }
    ctx ! q
  }

  private def encodeQ(intp: FQLInterpreter, value: Value): Query[String] = {
    FQL2ValueMaterializer.materialize(intp, value).map {
      case Result.Ok(mvalue) =>
        val txnTime = Timestamp(Instant.now())
        val dec = mvalue.Decorator(txnTime)
        dec.encode(mvalue.value)
        dec.sb.result()
      case Result.Err(err) => fail(s"unexpected error materializing: $err")
    }
  }

  private def encode(intp: FQLInterpreter, value: Value): String = {
    ctx ! encodeQ(intp, value)
  }

  // This ensures that encoding the value is equal to the FQL, and that evaluating
  // the FQL results in the same value.
  private def copypaste(value: Value, fql: String): Unit = {
    val auth = Auth.forScope(ScopeID.RootID)
    val intp = new FQLInterpreter(auth)

    val encoded = encode(intp, value)
    val res = eval(auth, fql).res match {
      case Result.Ok(v)    => v
      case Result.Err(err) => fail(s"unexpected error evaluating FQL: $err")
    }

    encoded shouldEqual fql
    res.value shouldEqual value
  }

  private def encode(value: Value, fql: String): Unit = {
    val auth = Auth.forScope(ScopeID.RootID)
    val intp = new FQLInterpreter(auth)

    encode(intp, value) shouldEqual fql
  }

  "FQL2ValueDecoratedSpec" - {
    "numbers" in {
      copypaste(Value.Int(3), "3")
      copypaste(Value.Double(3.0), "3.0")
      copypaste(Value.Double(3e3), "3000.0")
      copypaste(Value.Double(Double.PositiveInfinity), "Math.Infinity")
      copypaste(Value.Double(Double.NegativeInfinity), "-Math.Infinity")

      // NaN is weird and doesn't equal itself, so `copypaste` doesn't work
      val auth = Auth.forScope(ScopeID.RootID)
      val intp = new FQLInterpreter(auth)
      encode(intp, Value.Double(Double.NaN)) shouldEqual "Math.NaN"
    }

    "strings" in {
      copypaste(Value.Str("hello"), "\"hello\"")
      copypaste(Value.Str(""), "\"\"")
      copypaste(Value.Str("escaping: \u0000"), "\"escaping: \\0\"")

      // note that heredoc strings will always add a newline at the end, so the
      // string "new\nline" will not be copy-pastable.
      copypaste(
        Value.Str("new\nline\n"),
        """|<<-END
           |  new
           |  line
           |END""".stripMargin
      )

      copypaste(
        Value.Str("new\nEND\n"),
        """|<<-END0
           |  new
           |  END
           |END0""".stripMargin
      )
      copypaste(
        Value.Str("END\nEND0\n"),
        """|<<-END1
           |  END
           |  END0
           |END1""".stripMargin
      )

      // if a user really puts END, END0, END1, ... and END9 in their query,
      // just give up and render with quotes.
      val s = (0 until 10).map { i => s"END$i" }.mkString("\n")
      val escaped = (0 until 10).map { i => s"END$i" }.mkString("\\n")
      copypaste(
        Value.Str(s"END\n$s"),
        s"\"END\\n$escaped\""
      )
    }
    "multiline string" in {
      encode(
        Value.Str("hi\nhello"),
        """|<<-END
           |  hi
           |  hello
           |END""".stripMargin
      )
    }

    "bools" in {
      copypaste(Value.True, "true")
      copypaste(Value.False, "false")
    }
    "time" in {
      copypaste(Value.Time(Timestamp(63, 0)), "Time(\"1970-01-01T00:01:03Z\")")
    }

    // FIXME: we can't construct Bytes or UUID, so we can't decorate them

    "null" in {
      // Nulls have spans, so Value.Null(Span.Null) != Value.Null(Span(..))
      encode(Value.Null(Span.Null), "null")
    }
    "date" in {
      encode(Value.Date(LocalDate.of(2022, 12, 15)), "Date(\"2022-12-15\")")
    }
    "array" in {
      encode(
        Value.Array(
          ArraySeq(Value.Str("hello"), Value.Str("hello"), Value.Str("moon"))),
        """|[
           |  "hello",
           |  "hello",
           |  "moon"
           |]""".stripMargin
      )

      // Ensure that empty arrays are put on one line
      encode(Value.Array(), "[]")
    }

    "struct" in {
      val struct = Value.Struct(
        SeqMap(
          ("one", Value.Str("two")),
          ("three", Value.Str("four")),
          (
            "five",
            Value.Struct(
              SeqMap(
                ("a", Value.Str("b")),
                ("c", Value.Str("d"))
              )
            ))
        )
      )

      encode(
        struct,
        """|{
           |  one: "two",
           |  three: "four",
           |  five: {
           |    a: "b",
           |    c: "d"
           |  }
           |}""".stripMargin
      )

      // Ensure that empty structs are put on one line
      encode(Value.Struct(), "{}")
    }

    "refs" - {
      "existing ref" in {
        val auth = newDB

        val coll =
          evalEncode(auth, "Collection.create({name: 'User'}) { name, coll }")
        coll shouldEqual """|{
                            |  name: "User",
                            |  coll: Collection
                            |}""".stripMargin

        val doc =
          evalOk(auth, "User.create({ a: User.definition })").to[Value.Doc]
        val id = doc.id.subID.toLong
        val intp = new FQLInterpreter(auth)
        val ts =
          (ctx ! ReadBroker.getField(intp, doc, Name("ts", Span.Null))).unsafeGet
            .asInstanceOf[Value.Time]
            .value

        val user = evalEncode(auth, s"User.byId('$id')!")
        user shouldEqual s"""|{
                             |  id: "$id",
                             |  coll: User,
                             |  ts: Time("$ts"),
                             |  a: Collection.byName("User")
                             |}""".stripMargin

        val doc2 =
          evalOk(
            auth,
            s"User.create({ other: User.byId('$id'), schema: User.definition })")
            .to[Value.Doc]
        val id2 = doc2.id.subID.toLong
        val ts2 =
          (ctx ! ReadBroker.getField(intp, doc2, Name("ts", Span.Null))).unsafeGet
            .asInstanceOf[Value.Time]
            .value

        val user2 = evalEncode(auth, s"User.byId('$id2')!")
        user2 shouldEqual s"""|{
                              |  id: "$id2",
                              |  coll: User,
                              |  ts: Time("$ts2"),
                              |  other: User("$id"),
                              |  schema: Collection.byName("User")
                              |}""".stripMargin
      }
      "ref cycle" in {
        val auth = newDB
        mkColl(auth, "Loop")
        val doc = evalOk(auth, "Loop.create({ id: '0', self: Loop('0') })")
          .to[Value.Doc]
        val intp = new FQLInterpreter(auth)
        val ts =
          (ctx ! ReadBroker.getField(intp, doc, Name("ts", Span.Null))).unsafeGet
            .asInstanceOf[Value.Time]
            .value
        val loop = evalEncode(auth, s"Loop.byId('0')!")
        loop shouldEqual s"""|{
                            |  id: "0",
                            |  coll: Loop,
                            |  ts: Time("$ts"),
                            |  self: Loop("0")
                            |}""".stripMargin
      }
      "non existent ref" in {
        val auth = newDB
        mkColl(auth, "TestCol")
        val nonExistentResponse = evalEncode(auth, "TestCol('1234')")
        nonExistentResponse shouldEqual """TestCol("1234") /* not found */"""
      }

      "deleted ref in one transaction" in {
        val auth = newDB
        mkColl(auth, "TestCol")
        val nonExistentResponse =
          evalEncode(auth, "TestCol.create({ id: '1234' })!.delete()")
        nonExistentResponse shouldEqual """TestCol("1234") /* deleted */"""
      }

      "deleted ref" in {
        val auth = newDB
        mkColl(auth, "TestCol")
        evalOk(auth, "TestCol.create({ id: '1234' })")
        val nonExistentResponse =
          evalEncode(auth, "TestCol.byId('1234')!.delete()")
        nonExistentResponse shouldEqual """TestCol("1234") /* deleted */"""
      }

      // This is by design, `delete()` returns a special doc.
      "delete and byId return different results" in {
        val auth = newDB
        mkColl(auth, "TestCol")
        evalOk(auth, "TestCol.create({ id: '1234' })")
        evalEncode(
          auth,
          """|{
             |  deleted: TestCol.byId('1234')!.delete(),
             |  by_id: TestCol.byId('1234'),
             |}""".stripMargin) shouldBe (
          """|{
             |  deleted: TestCol("1234") /* deleted */,
             |  by_id: TestCol("1234") /* not found */
             |}""".stripMargin
        )
      }

      "ref to deleted collection" in {
        val auth = newDB
        mkColl(auth, "Foo")
        mkColl(auth, "Bar")
        evalOk(auth, "Foo.create({ other: Bar.create({}) })")
        evalOk(auth, "Collection.byName('Bar')!.delete()")
        val id = evalEncode(auth, "Foo.all().first()!.id")
        val otherId = evalEncode(auth, "Foo.all().first()!.other.id")
        val ts = evalEncode(auth, s"Foo.byId($id)!.ts")
        val doc = evalEncode(auth, s"Foo.byId($id)!")
        doc shouldEqual s"""|{
                            |  id: $id,
                            |  coll: Foo,
                            |  ts: $ts,
                            |  other: Collection.Deleted($otherId)
                            |}""".stripMargin
      }

      "permission denied ref" in {
        val auth = newDB.withPermissions(AdminPermissions)
        val roleName = "TestRole"
        mkColl(auth, "Books")
        evalOk(
          auth,
          s"""|Role.create({
              |  name: "$roleName",
              |  privileges: {
              |    resource: "Books",
              |    actions: {
              |      create: true,
              |      read: false
              |    }
              |  }
              |})
              |""".stripMargin
        )

        val roleAuth = mkRoleAuth(auth, roleName)
        val permissionDeniedResponse = evalEncode(roleAuth, "Books.byId('1234')")
        permissionDeniedResponse shouldEqual """Books("1234") /* permission denied */"""
      }
    }

    "index refs" in {
      val auth = newDB
      mkColl(auth, "Author")

      evalV4Ok(
        auth,
        CreateIndex(MkObject("name" -> "myIndex", "source" -> ClassRef("Author"))))

      evalV4Ok(
        auth,
        CreateF(
          ClassRef("Author"),
          MkObject("data" -> MkObject("indexRef" -> IndexRef("myIndex")))))

      val id = evalEncode(auth, "Author.all().toArray()[0].id")
      val ts = evalEncode(auth, s"Author.byId($id)!.ts")
      val res1 = evalEncode(auth, s"Author.byId($id)!")
      res1 shouldEqual s"""|{
                           |  id: $id,
                           |  coll: Author,
                           |  ts: $ts,
                           |  indexRef: [legacy index myIndex]
                           |}""".stripMargin

      evalEncode(
        auth,
        s"Author.byId($id)!.indexRef") shouldBe "[legacy index myIndex]"

      // if you're fetching fields on legacy indexes, you're doing it wrong.
      val res2 = evalEncode(auth, "Author.all().toArray()[0].indexRef.foo")
      res2 shouldEqual "null"
    }
  }
}
