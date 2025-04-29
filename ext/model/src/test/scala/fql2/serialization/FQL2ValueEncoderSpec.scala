package fauna.model.test

import fauna.atoms.ScopeID
import fauna.auth.{ AdminPermissions, Auth }
import fauna.codex.json._
import fauna.codex.json2.{ JSON, JSONWriter }
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.runtime.fql2.{ FQLInterpreter, Result }
import fauna.model.runtime.fql2.serialization._
import fauna.repo.values.Value
import fauna.util.Base64
import fql.ast.Span
import io.netty.buffer.ByteBufAllocator
import java.time.{ Instant, LocalDate }
import scala.collection.immutable.{ ArraySeq, SeqMap }

class FQL2ValueEncoderSpec(val format: ValueFormat) extends FQL2WithV4Spec {
  private val alloc = ByteBufAllocator.DEFAULT

  private def encode0(value: Value, auth: Auth) = {
    val buf = alloc.buffer
    val out = JSONWriter(buf)

    val intp = new FQLInterpreter(auth)

    val mvalue = ctx ! FQL2ValueMaterializer.materialize(intp, value) match {
      case Result.Ok(v)    => v
      case Result.Err(err) => fail(s"unexpected error materializing: $err")
    }

    FQL2ValueEncoder.encode(format, out, mvalue, Timestamp(Instant.now()))
    buf
  }

  def encode(value: Value, auth: Auth = Auth.forScope(ScopeID.RootID)) = {
    encode0(value, auth).toUTF8String
  }

  def evalEncode(auth: Auth, query: String): String = {
    encode(evalOk(auth, query), auth)
  }

  def evalEncodeJSON(auth: Auth, query: String): JSValue = {
    val value = evalOk(auth, query)
    val buf = encode0(value, auth)
    JSON.decode[JSValue](buf)
  }
}

class FQL2ValueEncoderSimpleSpec extends FQL2ValueEncoderSpec(ValueFormat.Simple) {
  "FQL2ValueEncoder" - {
    "render an int" in {
      encode(Value.Int(3)) shouldEqual "3"
    }
    "render a string" in {
      encode(Value.Str("hilo")) shouldEqual "\"hilo\""
    }
    "render a double" in {
      encode(Value.Double(22.2)) shouldEqual "22.2"
    }
    "render special double values" - {
      "NaN renders as \"NaN\"" in {
        encode(Value.Double(Double.NaN)) shouldEqual "\"NaN\""
      }
      "+Infinity renders as \"Infinity\"" in {
        encode(Value.Double(Double.PositiveInfinity)) shouldEqual "\"Infinity\""
      }
      "-Infinity renders as \"-Infinity\"" in {
        encode(Value.Double(Double.NegativeInfinity)) shouldEqual "\"-Infinity\""
      }
    }
    "render a long" in {
      encode(Value.Long(20)) shouldEqual "20"
    }
    "render true" in {
      encode(Value.True) shouldEqual "true"
    }
    "render false" in {
      encode(Value.False) shouldEqual "false"
    }
    "render a timestamp" in {
      val now = Instant.now()

      encode(Value.Time(Timestamp(now))) shouldEqual s"\"${now.toString}\""
    }
    "render a date" in {
      val date = LocalDate.now()

      encode(Value.Date(date)) shouldEqual s"\"${date.toString}\""
    }
    "render a uuid" in {
      val uuid = java.util.UUID.randomUUID()

      encode(Value.UUID(uuid)) shouldEqual s"\"${uuid.toString}\""
    }
    "render null" in {
      encode(Value.Null(Span.Null)) shouldEqual "null"
    }
    "render array" in {
      encode(
        Value.Array(
          ArraySeq(
            Value.Str("hello"),
            Value.Str("hello"),
            Value.Str("moon")))) shouldEqual """["hello","hello","moon"]"""
    }
    "render struct" in {
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
        struct) shouldEqual """{"one":"two","three":"four","five":{"a":"b","c":"d"}}"""
    }

    "refs" in {
      val auth = newDB

      // Selecting just the name and coll fields should give us a string and a
      // string for the module
      val coll =
        evalEncodeJSON(auth, "Collection.create({name: 'User'}) { name, coll }")
      (coll / "name") shouldBe JSString("User")
      (coll / "coll") shouldBe JSString("Collection")

      val coll2 =
        evalEncodeJSON(auth, "Collection.create({name: 'User2'})")
      (coll2 / "name") shouldBe JSString("User2")
      (coll2 / "coll") shouldBe JSString("Collection")
      (coll2 / "ts").isEmpty shouldBe false

      // Serializing a schema doc ref within a doc should render the ref as an
      // object.
      val doc1 = evalEncodeJSON(auth, "User.create({ a: User.definition })")
      (doc1 / "coll") shouldBe JSString("User")
      (doc1 / "a") shouldBe JSObject("name" -> "User", "coll" -> "Collection")
      (doc1 / "ts").isEmpty shouldBe false

      // Serializing a regular ref to another document should render the ref as
      // an object.
      val doc2 = evalEncodeJSON(auth, "User.create({ foo: User.byId('1234') })")
      val id2 = doc2 / "id"
      (doc2 / "coll") shouldBe JSString("User")
      (doc2 / "foo") shouldBe JSObject("id" -> "1234", "coll" -> "User")

      // deleted docs render as null
      val doc3 = evalEncodeJSON(auth, s"User.byId($id2)!.delete()")
      doc3 shouldBe JSNull

      // ref not found should render as null
      val doc4 = evalEncodeJSON(auth, s"User.byId('1234')")
      doc4 shouldBe JSNull

      // read permission denied should render as null
      val admin = auth.withPermissions(AdminPermissions)
      val roleName = "TestRole"
      mkColl(auth, "Books")
      evalOk(
        admin,
        s"""|Role.create({
            |  name: "$roleName",
            |  privileges: {
            |    resource: "Books",
            |    actions: {
            |      create: true,
            |      read: false,
            |    }
            |  }
            |})
            |""".stripMargin
      )
      val roleAuth = mkRoleAuth(admin, roleName)
      val doc5 = evalEncodeJSON(roleAuth, s"User.byId('1234')")
      doc5 shouldBe JSNull
    }

    "refs at multiple timestamps" in {
      val auth = newDB

      updateSchemaOk(auth, "main.fsl" -> "collection User { name: String }")

      val ts0 = evalOk(auth, "Time.now()").asInstanceOf[Value.Time].value
      evalOk(auth, "User.create({ id: 0, name: 'alice' })")
      val ts1 = evalOk(auth, "Time.now()").asInstanceOf[Value.Time].value
      evalOk(auth, "User(0)!.update({ name: 'bob' })")
      val ts2 = evalOk(auth, "Time.now()").asInstanceOf[Value.Time].value

      val json = evalEncodeJSON(
        auth,
        s"""|[
            |  at (Time("$ts0")) User(0),
            |  at (Time("$ts1")) User(0),
            |  at (Time("$ts2")) User(0),
            |]""".stripMargin
      )
      json / 0 shouldBe JSNull
      json / 1 / "name" shouldBe JSString("alice")
      json / 2 / "name" shouldBe JSString("bob")
    }

    "partials" in {
      val auth = newDB

      evalOk(
        auth,
        s"""|Collection.create({
            |  name: "User",
            |  indexes: {
            |    addrByUser: {
            |      terms: [{ field: 'username' }],
            |      values: [{ field: 'address' }]
            |    }
            |  }
            |})
            |""".stripMargin
      )

      evalOk(
        auth,
        s"""|User.create({
            |  username: 'bob',
            |  address: {
            |    line1: '2nd falson street',
            |    line2: 'apt 303'
            |  }
            |})
            |""".stripMargin
      )

      val user =
        JSObject(
          "username" -> "bob",
          "address" -> JSObject(
            "line1" -> "2nd falson street",
            "line2" -> "apt 303"
          ))

      evalEncodeJSON(
        auth,
        "User.addrByUser('bob').map(.data).first()"
      ) shouldBe user

      evalEncodeJSON(
        auth,
        s"""|let addr = User.addrByUser('bob').map(.data).first()
            |{ addr: addr }
            |""".stripMargin
      ) shouldBe JSObject("addr" -> user)

      evalEncodeJSON(
        auth,
        s"""|let addr = User.addrByUser('bob').map(.data).first()
            |[ addr ]
            |""".stripMargin
      ) shouldBe JSArray(user)
    }

    "sets" in {
      val auth = newDB

      evalOk(auth, "Collection.create({ name: 'A' })")
      evalOk(auth, "Collection.create({ name: 'B' })")
      evalOk(auth, "Collection.create({ name: 'C' })")

      val res1 = evalEncodeJSON(auth, "Collection.all()")
      (res1 / "data" / 0 / "name") shouldBe JSString("A")
      (res1 / "data" / 0 / "coll") shouldBe JSString("Collection")
      (res1 / "data" / 1 / "name") shouldBe JSString("B")
      (res1 / "data" / 1 / "coll") shouldBe JSString("Collection")
      (res1 / "data" / 2 / "name") shouldBe JSString("C")
      (res1 / "data" / 2 / "coll") shouldBe JSString("Collection")
      (res1 / "after").isEmpty shouldBe true

      val res2 = evalEncodeJSON(auth, "Collection.all().paginate(2)")
      (res2 / "data" / 0 / "name") shouldBe JSString("A")
      (res2 / "data" / 0 / "coll") shouldBe JSString("Collection")
      (res2 / "data" / 1 / "name") shouldBe JSString("B")
      (res2 / "data" / 1 / "coll") shouldBe JSString("Collection")
      (res2 / "after").as[String]
    }

    "streams" in {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: 'Foo' })")

      val res = evalEncodeJSON(auth, "Foo.all().toStream()")
      Value.EventSource
        .fromBase64(res.as[String])
        .value shouldBe a[Value.EventSource.IR]
    }
  }
}

class FQL2ValueEncoderTaggedSpec extends FQL2ValueEncoderSpec(ValueFormat.Tagged) {
  "FQL2ValueEncoder" - {
    "render an int" in {
      encode(Value.Int(3)) shouldEqual """{"@int":"3"}"""
    }
    "render a string" in {
      encode(Value.Str("hilo")) shouldEqual "\"hilo\""
    }
    "render a double" in {
      encode(Value.Double(22.2)) shouldEqual """{"@double":"22.2"}"""
    }
    "render special double values" - {
      "NaN renders as \"NaN\"" in {
        encode(Value.Double(Double.NaN)) shouldEqual """{"@double":"NaN"}"""
      }
      "+Infinity renders as \"Infinity\"" in {
        encode(
          Value.Double(
            Double.PositiveInfinity)) shouldEqual """{"@double":"Infinity"}"""
      }
      "-Infinity renders as \"-Infinity\"" in {
        encode(
          Value.Double(
            Double.NegativeInfinity)) shouldEqual """{"@double":"-Infinity"}"""
      }
    }
    "render a long" in {
      encode(Value.Long(20)) shouldEqual """{"@long":"20"}"""
    }
    "render true" in {
      encode(Value.True) shouldEqual "true"
    }
    "render false" in {
      encode(Value.False) shouldEqual "false"
    }
    "render a timestamp" in {
      val now = Instant.now()

      encode(
        Value.Time(Timestamp(now))) shouldEqual s"""{"@time":"${now.toString}"}"""
    }
    "render a date" in {
      val date = LocalDate.now()

      encode(Value.Date(date)) shouldEqual s"""{"@date":"${date.toString}"}"""
    }
    "render bytes" in {
      val bytes = Array[Byte](1, 2, 3)

      encode(
        Value.Bytes(
          ArraySeq.unsafeWrapArray(bytes))) shouldEqual s"""{"@bytes":"${Base64
          .encodeStandard(bytes)}"}"""
    }
    "render a uuid" in {
      val uuid = java.util.UUID.randomUUID()

      encode(Value.UUID(uuid)) shouldEqual s"""{"@uuid":"${uuid.toString}\"}"""
    }
    "render null" in {
      encode(Value.Null(Span.Null)) shouldEqual "null"
    }
    "render array" in {
      encode(
        Value.Array(
          ArraySeq(
            Value.Str("hello"),
            Value.Str("hello"),
            Value.Str("moon")))) shouldEqual """["hello","hello","moon"]"""
    }
    "render struct" in {
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
        struct) shouldEqual """{"one":"two","three":"four","five":{"a":"b","c":"d"}}"""
    }

    "render struct with a key starting with @" in {
      val struct = Value.Struct(
        SeqMap(
          ("@foo", Value.Str("one")),
          ("bar", Value.Str("two"))
        )
      )

      encode(struct) shouldEqual """{"@object":{"@foo":"one","bar":"two"}}"""
    }

    "render docs recursively correctly" in {

      val auth = newDB

      // make two docs, pointing at each other. this lets both of them show up in the
      // `docsQ` query.
      evalOk(auth, "Collection.create({ name: 'User' })")
      val id1 = evalOk(auth, "User.create({}).id").asInstanceOf[Value.ID].value
      val id2 = evalOk(auth, "User.create({}).id").asInstanceOf[Value.ID].value
      evalOk(auth, s"User.byId('$id1')!.update({ other: User.byId('$id2')! })")
      evalOk(auth, s"User.byId('$id2')!.update({ other: User.byId('$id1')! })")

      // now both of them should render correctly, and not 500
      val res = evalEncodeJSON(auth, "User.all()")
      (res / "@set" / "data" / 0 / "@doc" / "id").as[String] shouldBe id1.toString
      (res / "@set" / "data" / 1 / "@doc" / "id").as[String] shouldBe id2.toString
      (res / "@set" / "data" / 0 / "@doc" / "other" / "@ref" / "id")
        .as[String] shouldBe id2.toString
      (res / "@set" / "data" / 1 / "@doc" / "other" / "@ref" / "id")
        .as[String] shouldBe id1.toString
    }

    "refs" in {
      val auth = newDB

      // Selecting just the name and coll fields should give us a string and
      // a module object
      val coll =
        evalEncode(auth, "Collection.create({name: 'User'}) { name, coll }")
      coll shouldEqual """{"name":"User","coll":{"@mod":"Collection"}}"""

      val coll2 =
        evalEncodeJSON(auth, "Collection.create({name: 'User2'})")
      (coll2 / "@doc" / "name") shouldBe JSString("User2")
      (coll2 / "@doc" / "coll") shouldBe JSObject("@mod" -> "Collection")
      (coll2 / "@doc" / "ts").isEmpty shouldBe false

      // Serializing a schema doc ref within a ref should give a `@ref` object
      val doc1 = evalEncodeJSON(auth, "User.create({ a: User.definition })")
      (doc1 / "@doc" / "coll") shouldBe JSObject("@mod" -> "User")
      (doc1 / "@doc" / "a") shouldBe JSObject(
        "@ref" -> JSObject(
          "name" -> "User",
          "coll" -> JSObject("@mod" -> "Collection")))
      (doc1 / "@doc" / "ts").isEmpty shouldBe false

      // Serializing a regular ref to another document should give a `@ref`
      val doc2 = evalEncodeJSON(auth, "User.create({ foo: User.byId('1234') })")
      val id2 = doc2 / "@doc" / "id"
      (doc2 / "@doc" / "coll") shouldBe JSObject("@mod" -> "User")
      (doc2 / "@doc" / "foo") shouldBe JSObject(
        "@ref" -> JSObject("id" -> "1234", "coll" -> JSObject("@mod" -> "User")))

      // deleted doc should have exists and cause correctly set
      val doc3 = evalEncodeJSON(auth, s"User.byId($id2)!.delete()")
      doc3 shouldBe JSObject(
        "@ref" -> JSObject(
          "id" -> id2,
          "coll" -> JSObject("@mod" -> "User"),
          "exists" -> false,
          "cause" -> "deleted"
        )
      )

      // ref not found should have exists and cause correctly set
      val doc4 = evalEncodeJSON(auth, s"User.byId('1234')")
      doc4 shouldBe JSObject(
        "@ref" -> JSObject(
          "id" -> "1234",
          "coll" -> JSObject("@mod" -> "User"),
          "exists" -> false,
          "cause" -> "not found"
        )
      )

      // read permission denied should have exists and cause correctly set
      val admin = auth.withPermissions(AdminPermissions)
      val roleName = "TestRole"
      mkColl(auth, "Books")
      evalOk(
        admin,
        s"""|Role.create({
            |  name: "$roleName",
            |  privileges: {
            |    resource: "Books",
            |    actions: {
            |      create: true,
            |      read: false,
            |    }
            |  }
            |})
            |""".stripMargin
      )
      val roleAuth = mkRoleAuth(admin, roleName)
      val doc5 = evalEncodeJSON(roleAuth, s"User.byId('1234')")
      doc5 shouldBe JSObject(
        "@ref" -> JSObject(
          "id" -> "1234",
          "coll" -> JSObject("@mod" -> "User"),
          "exists" -> false,
          "cause" -> "permission denied"
        )
      )
    }

    "partials" in {
      val auth = newDB

      evalOk(
        auth,
        s"""|Collection.create({
            |  name: "User",
            |  indexes: {
            |    addrByUser: {
            |      terms: [{ field: 'username' }],
            |      values: [{ field: 'address' }]
            |    }
            |  }
            |})
            |""".stripMargin
      )

      evalOk(
        auth,
        s"""|User.create({
            |  username: 'bob',
            |  address: {
            |    line1: '2nd falson street',
            |    line2: 'apt 303'
            |  }
            |})
            |""".stripMargin
      )

      val user =
        JSObject(
          "username" -> "bob",
          "address" -> JSObject(
            "line1" -> "2nd falson street",
            "line2" -> "apt 303"
          ))

      evalEncodeJSON(
        auth,
        "User.addrByUser('bob').map(.data).first()"
      ) shouldBe user

      evalEncodeJSON(
        auth,
        s"""|let addr = User.addrByUser('bob').map(.data).first()
            |{ addr: addr }
            |""".stripMargin
      ) shouldBe JSObject("addr" -> user)

      evalEncodeJSON(
        auth,
        s"""|let addr = User.addrByUser('bob').map(.data).first()
            |[ addr ]
            |""".stripMargin
      ) shouldBe JSArray(user)
    }

    "sets" in {
      val auth = newDB

      evalOk(auth, "Collection.create({ name: 'A' })")
      evalOk(auth, "Collection.create({ name: 'B' })")
      evalOk(auth, "Collection.create({ name: 'C' })")

      val res1 = evalEncodeJSON(auth, "Collection.all()")
      (res1 / "@set" / "data" / 0 / "@doc" / "name") shouldBe JSString("A")
      (res1 / "@set" / "data" / 0 / "@doc" / "coll") shouldBe JSObject(
        "@mod" -> "Collection")
      (res1 / "@set" / "data" / 1 / "@doc" / "name") shouldBe JSString("B")
      (res1 / "@set" / "data" / 1 / "@doc" / "coll") shouldBe JSObject(
        "@mod" -> "Collection")
      (res1 / "@set" / "data" / 2 / "@doc" / "name") shouldBe JSString("C")
      (res1 / "@set" / "data" / 2 / "@doc" / "coll") shouldBe JSObject(
        "@mod" -> "Collection")
      (res1 / "@set" / "after").isEmpty shouldBe true

      // FIXME: Shoudl be wrapped in @set
      val res2 = evalEncodeJSON(auth, "Collection.all().paginate(2)")
      (res2 / "data" / 0 / "@doc" / "name") shouldBe JSString("A")
      (res2 / "data" / 0 / "@doc" / "coll") shouldBe JSObject("@mod" -> "Collection")
      (res2 / "data" / 1 / "@doc" / "name") shouldBe JSString("B")
      (res2 / "data" / 1 / "@doc" / "coll") shouldBe JSObject("@mod" -> "Collection")
      (res2 / "after").as[String]
    }

    "streams" in {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: 'Foo' })")

      val res = evalEncodeJSON(auth, "Foo.all().toStream()") / "@stream"
      Value.EventSource
        .fromBase64(res.as[String])
        .value shouldBe a[Value.EventSource.IR]
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

      val res1 = evalEncodeJSON(auth, "Author.all().toArray()[0].indexRef")
      res1 shouldBe JSString("[legacy index myIndex]")

      // if you're fetching fields on legacy indexes, you're doing it wrong.
      val res2 = evalEncodeJSON(auth, "Author.all().toArray()[0].indexRef.foo")
      res2 shouldBe JSNull
    }
  }
}
