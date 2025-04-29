package fauna.api.test

import fauna.codex.json.{ JSArray, JSObject, JSString, JSValue }
import fauna.codex.json2.JSON
import fauna.prop.api.Query27Helpers._
import fql.TextUtil
import io.netty.buffer.Unpooled
import scala.util.Using

class FQL2EnvironmentEndpointSpec extends FQL2APISpec {
  test("returns a 401 with no auth") {
    val res = client.api.get("/environment/1")
    res should respond(401)
    res.json shouldBe JSObject(
      "code" -> JSString("unauthorized"),
      "message" -> JSString("Invalid token, unable to authenticate request")
    )
  }

  test("succeeds with admin key") {
    val db = aDatabase.sample
    val res = client.api.get("/environment/1", token = db.adminKey)
    res should respond(200)
  }

  test("succeeds with server key") {
    val db = aDatabase.sample
    val res = client.api.get("/environment/1", token = db.key)
    res should respond(200)
  }

  test("fails with 403 for non server/admin key") {
    val db = aDatabase.sample
    val res = client.api.get("/environment/1", token = db.clientKey)
    res should respond(403)
    res.json shouldBe JSObject(
      "code" -> JSString("forbidden"),
      "message" -> JSString(
        "A valid admin/server key must be provided to use this endpoint")
    )
  }

  test("returns correct static environment") {
    val db = aDatabase.sample
    val res = client.api.get("/environment/1", token = db.key)

    res should respond(200)
    val actual = res.json.toPrettyString

    val expected =
      Using(getClass.getResourceAsStream("/static_environment_response_v1.json")) {
        stream =>
          val bb = Unpooled.wrappedBuffer(stream.readAllBytes())
          val expected = JSON.parse[JSValue](bb).toPrettyString
          expected.replace(
            "\"$SCHEMA_VERSION$\"",
            (res.json / "schema_version").as[Long].toString)
      }.get

    if (actual != expected) {
      val sb = new StringBuilder
      sb.append("Generated static environment did not equal expected:\n")
      TextUtil.printDiff(actual, expected, sb)
      fail(sb.result())
    }
  }

  test("returns the correct collection types when a user has collections") {
    val db = aDatabase.sample
    queryOk(
      """
        |Collection.create({
        |  name: "Author",
        |  fields: {
        |    firstName: { signature: 'String' },
        |    lastName: { signature: 'String' }
        |  },
        |  indexes: {
        |    byLastName: {
        |      terms: [ { field: "lastName" } ],
        |      values: [ { field: "firstName" }, { field: "lastName" } ]
        |    },
        |    byFullName: {
        |      terms: [ { field: "firstName" }, { field: "lastName" }]
        |    }
        |  }
        |})
        |""".stripMargin,
      db
    )
    val res = client.api.get("/environment/1", token = db.key)
    res should respond(200)

    (res.json / "environment" / "globals").as[JSObject] should containJSON(
      JSObject(
        "Author" -> JSString("AuthorCollection")
      )
    )

    (res.json / "environment" / "types" / "AuthorCollection")
      .as[JSObject] should containJSON(
      JSObject(
        "self" -> JSString("AuthorCollection"),
        "type_hint" -> "user_collection",
        "fields" -> JSObject(
          "all" -> "(() => Set<Author>) & ((range: { from: Any } | { to: Any } | { from: Any, to: Any }) => Set<Author>)",
          "firstWhere" -> "(pred: (Author => Boolean)) => Author | Null",
          "toString" -> "() => String",
          "where" -> "(pred: (Author => Boolean)) => Set<Author>",
          "createData" -> "(data: { firstName: String, lastName: String }) => Author",
          "create" -> "(data: { id: ID | Null, firstName: String, lastName: String }) => Author",
          "definition" -> "CollectionDef",
          "byId" -> "(id: ID) => Ref<Author>",
          "byLastName" -> "((term1: String) => Set<Author>) & ((term1: String, range: { from: Any } | { to: Any } | { from: Any, to: Any }) => Set<Author>)",
          "byFullName" -> "((term1: String, term2: String) => Set<Author>) & ((term1: String, term2: String, range: { from: Any } | { to: Any } | { from: Any, to: Any }) => Set<Author>)"
        )
      )
    )

    (res.json / "environment" / "types" / "Author").as[JSObject] should containJSON(
      JSObject(
        "self" -> "Author",
        "alias" -> "{ id: ID, ts: Time, ttl: Time | Null, firstName: String, lastName: String }",
        "fields" -> JSObject(
          "coll" -> "AuthorCollection",
          "delete" -> "() => NullAuthor",
          "exists" -> "() => true",
          "replace" -> "(data: { firstName: String, lastName: String }) => Author",
          "replaceData" -> "(data: { firstName: String, lastName: String }) => Author",
          "update" -> "(data: { firstName: String | Null, lastName: String | Null }) => Author",
          "updateData" -> "(data: { firstName: String | Null, lastName: String | Null }) => Author"
        )
      )
    )

    (res.json / "environment" / "types" / "NullAuthor")
      .as[JSObject] should containJSON(
      JSObject(
        "self" -> "NullAuthor",
        "fields" -> JSObject(
          "id" -> "ID",
          "toString" -> "() => String",
          "exists" -> "() => false",
          "coll" -> "AuthorCollection"
        ),
        "alias" -> "Null"
      )
    )

    (res.json / "environment" / "truncated_resources").isEmpty shouldBe true
  }
  test("collections with an invalid identifier aren't returned in the environment") {
    val db = aDatabase.sample

    val invalidIdentifier = "Invalid$Identifier"

    legacyQuery(
      CreateCollection(
        MkObject("name" -> invalidIdentifier)
      ),
      db
    )

    val res = client.api.get("/environment/1", token = db.key)
    res should respond(200)

    res.json.toString
      .contains(invalidIdentifier) shouldBe false
  }

  test("returns collection aliases as globals when collections have aliases") {
    val db = aDatabase.sample
    queryOk(
      """
        |Collection.create({
        |  name: "Author",
        |  alias: "BookWriter"
        |})
        |""".stripMargin,
      db
    )
    val res = client.api.get("/environment/1", token = db.key)
    res should respond(200)

    (res.json / "environment" / "globals").as[JSObject] should containJSON(
      JSObject(
        "Author" -> JSString("AuthorCollection"),
        "BookWriter" -> JSString("BookWriterCollection")
      )
    )

    (res.json / "environment" / "types" / "BookWriterCollection")
      .as[JSObject] should containJSON(
      JSObject(
        "self" -> JSString("BookWriterCollection"),
        "type_hint" -> "user_collection",
        "fields" -> JSObject(
          "all" -> "(() => Set<BookWriter>) & ((range: { from: Any } | { to: Any } | { from: Any, to: Any }) => Set<BookWriter>)",
          "firstWhere" -> "(pred: (BookWriter => Boolean)) => BookWriter | Null",
          "toString" -> "() => String",
          "where" -> "(pred: (BookWriter => Boolean)) => Set<BookWriter>",
          "createData" -> "(data: { *: Any }) => BookWriter",
          "create" -> "(data: { id: ID | Null, ttl: Time | Null, data: Null, *: Any }) => BookWriter",
          "definition" -> "CollectionDef",
          "byId" -> "(id: ID) => Ref<BookWriter>"
        )
      )
    )

    (res.json / "environment" / "types" / "BookWriter")
      .as[JSObject] should containJSON(
      JSObject(
        "self" -> "BookWriter",
        "alias" -> "{ id: ID, ts: Time, ttl: Time | Null, *: Any }",
        "fields" -> JSObject(
          "coll" -> "BookWriterCollection",
          "delete" -> "() => NullBookWriter",
          "exists" -> "() => true",
          "replace" -> "(data: { ttl: Time | Null, data: Null, *: Any }) => BookWriter",
          "replaceData" -> "(data: { *: Any }) => BookWriter",
          "update" -> "(data: { ttl: Time | Null, data: Null, *: Any }) => BookWriter",
          "updateData" -> "(data: { *: Any }) => BookWriter"
        )
      )
    )

    (res.json / "environment" / "types" / "NullBookWriter")
      .as[JSObject] should containJSON(
      JSObject(
        "self" -> "NullBookWriter",
        "fields" -> JSObject(
          "id" -> "ID",
          "toString" -> "() => String",
          "exists" -> "() => false",
          "coll" -> "BookWriterCollection"
        ),
        "alias" -> "Null"
      )
    )
  }

  test("returns the correct user function types when a user has user functions") {
    val db = aTypecheckedDatabase.sample
    queryOk(
      """
        |Function.create({
        |  name: "testFunc",
        |  body: "x => x",
        |  signature: "(String) => String"
        |})
        |""".stripMargin,
      db
    )

    val res = client.api.get("/environment/1", token = db.key)
    res should respond(200)

    (res.json / "environment" / "globals").as[JSObject] should containJSON(
      JSObject(
        "testFunc" -> "UserFunction<(x: String) => String>"
      )
    )
  }

  test("returns function aliases when present") {
    val db = aTypecheckedDatabase.sample
    queryOk(
      """
        |Function.create({
        |  name: "testFunc",
        |  alias: "testAlias",
        |  body: "x => x",
        |  signature: "(String) => String"
        |})
        |""".stripMargin,
      db
    )

    val res = client.api.get("/environment/1", token = db.key)
    res should respond(200)

    (res.json / "environment" / "globals").as[JSObject] should containJSON(
      JSObject(
        "testFunc" -> "UserFunction<(x: String) => String>",
        "testAlias" -> "UserFunction<(x: String) => String>"
      )
    )
  }

  test("returns correct schema version") {
    val db = aTypecheckedDatabase.sample
    val resTs = queryRaw(
      """
        |Function.create({
        |  name: "testFunc",
        |  body: "x => x",
        |  signature: "(String) => String"
        |})
        |""".stripMargin,
      db
    )

    resTs should respond(200)
    val expectedSchemaVersion = (resTs.json / "txn_ts").as[Long]

    queryOk(
      """testFunc("hi")""",
      db
    )

    val res = client.api.get("/environment/1", token = db.key)
    res should respond(200)
    val envSchemaVersion = (res.json / "schema_version").as[Long]

    envSchemaVersion shouldEqual expectedSchemaVersion
  }

  test("returns truncated collection response when a user has collections > 1000") {
    val db = aDatabase.sample
    val qStrBuilder = new StringBuilder
    for (i <- 1 to 10) {
      for (i2 <- 1 to 101) {
        qStrBuilder.append(s"Collection.create({ name: 'TestColl$i$i2'})\n")
      }
      queryOk(
        qStrBuilder.result(),
        db
      )
      qStrBuilder.clear()
    }

    val res = client.api.get("/environment/1", token = db.key)
    res should respond(200)
    (res.json / "environment" / "truncated_resources") shouldEqual JSArray(
      "collection"
    )
  }
  test("returns truncated function response when a user > 1000 functions") {
    val db = aDatabase.sample
    val qStrBuilder = new StringBuilder
    for (i <- 1 to 10) {
      for (i2 <- 1 to 101) {
        qStrBuilder.append(
          s"Function.create({ name: 'TestFunc$i$i2', body: 'x => x' })\n")
      }
      queryOk(
        qStrBuilder.result(),
        db
      )
      qStrBuilder.clear()
    }

    val res = client.api.get("/environment/1", token = db.key)
    res should respond(200)
    (res.json / "environment" / "truncated_resources") shouldEqual JSArray(
      "function"
    )
  }
  test(
    "returns truncated [collection, function] response when a user has collectinos > 1000 and functions > 1000") {
    val db = aDatabase.sample
    val qStrBuilder = new StringBuilder
    for (i <- 1 to 20) {
      for (i2 <- 1 to 51) {
        qStrBuilder.append(s"Collection.create({ name: 'TestColli1${i}i2$i2'})\n")
        qStrBuilder.append(
          s"Function.create({ name: 'TestFunci1${i}i2$i2', body: 'x => x' })\n")
      }
      queryOk(
        qStrBuilder.result(),
        db
      )
      qStrBuilder.clear()
    }

    val res = client.api.get("/environment/1", token = db.key)
    res should respond(200)
    (res.json / "environment" / "truncated_resources") shouldEqual JSArray(
      "collection",
      "function"
    )
  }
}
