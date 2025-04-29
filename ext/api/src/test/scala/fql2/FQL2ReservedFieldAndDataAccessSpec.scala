package fauna.api.test

import java.time.temporal.ChronoUnit
import java.time.Instant

class FQL2ReservedFieldAndDataAccessSpec extends FQL2APISpec {
  // `data` is special and causes a type check failure.
  // It's covered by a SchemaCollection test.
  val reservedDocumentFields = Set(
    "ts",
    "delete",
    "update",
    "replace",
    "coll",
    "id"
  )
  test("a document can not be created with reserved fields besides id") {
    val db = aDatabase.sample
    mkCollection(db, "TestColl")
    for (name <- reservedDocumentFields - "id") {
      val errRes = queryErr(
        s"""|TestColl.create({
           |  $name: "Thing",
           |})""".stripMargin,
        db
      )

      val expectedSummary =
        s"""error: Failed to create document in collection `TestColl`.
        |constraint failures:
        |  $name: Is a reserved field and cannot be used
        |at *query*:1:16
        |  |
        |1 |   TestColl.create({
        |  |  ________________^
        |2 | |   $name: "Thing",
        |3 | | })
        |  | |__^
        |  |""".stripMargin

      (errRes / "error" / "code").as[String] shouldEqual "constraint_failure"
      (errRes / "summary").as[String] shouldEqual expectedSummary
    }
  }
  test("a document can not be updated with reserved fields") {
    val db = aDatabase.sample
    mkCollection(db, "TestColl")
    for (name <- reservedDocumentFields) {
      val docId = (queryOk(
        s"""|TestColl.create({
            |  validField: "Thing",
            |})
            |""".stripMargin,
        db
      ) / "id").as[String]

      val errRes = queryErr(
        s"""|
            |let doc = TestColl.byId("$docId")!
            |doc.update({
            |  $name: "will I succeed?"
            |})
            |""".stripMargin,
        db
      )
      val expectedSummary =
        s"""error: Failed to update document with id $docId in collection `TestColl`.
        |constraint failures:
        |  $name: Is a reserved field and cannot be used
        |at *query*:3:11
        |  |
        |3 |   doc.update({
        |  |  ___________^
        |4 | |   $name: "will I succeed?"
        |5 | | })
        |  | |__^
        |  |""".stripMargin

      (errRes / "error" / "code").as[String] shouldEqual "constraint_failure"
      (errRes / "summary").as[String] shouldEqual expectedSummary
    }
  }
  test("a document can not be replaced with reserved fields") {
    val db = aDatabase.sample
    mkCollection(db, "TestColl")
    for (name <- reservedDocumentFields) {
      val docId = (queryOk(
        s"""|TestColl.create({
            |  validField: "Thing",
            |})
            |""".stripMargin,
        db
      ) / "id").as[String]

      val errRes = queryErr(
        s"""|
            |let doc = TestColl.byId("$docId")!
            |doc.replace({
            |  $name: "will I succeed?"
            |})
            |""".stripMargin,
        db
      )

      val expectedSummary =
        s"""error: Failed to update document with id $docId in collection `TestColl`.
                               |constraint failures:
                               |  $name: Is a reserved field and cannot be used
                               |at *query*:3:12
                               |  |
                               |3 |   doc.replace({
                               |  |  ____________^
                               |4 | |   $name: "will I succeed?"
                               |5 | | })
                               |  | |__^
                               |  |""".stripMargin

      (errRes / "error" / "code").as[String] shouldEqual "constraint_failure"
      (errRes / "summary").as[String] shouldEqual expectedSummary
    }
  }
  test(
    "non conflicting fields set at the top level are available at the top level as well as nested underneath the data field") {
    val db = aDatabase.sample
    mkCollection(db, "TestColl")
    val res = queryOk(
      s"""|let doc = TestColl.create({
          |  field1: "v1",
          |  field2: { nestedField: "v3" }
          |})
          |{
          |  d1: { f1: doc.field1, f2: doc.field2 },
          |  d2: { f1: doc.data.field1, f2: doc.data.field2 }
          |}
          |""".stripMargin,
      db
    )
    (res / "d1" / "f1").as[String] shouldEqual "v1"
    (res / "d1" / "f2" / "nestedField").as[String] shouldEqual "v3"

    (res / "d1") shouldEqual (res / "d2")

    val resUpdate = queryOk(
      s"""|let doc = TestColl.create({
          |  field1: "v1",
          |  field2: { nestedField: "v3" }
          |})
          |doc.update({
          |  field1: "v1update",
          |  field2: { nestedField: "v3update" }
          |})
          |{
          |  d1: { f1: doc.field1, f2: doc.field2 },
          |  d2: { f1: doc.data.field1, f2: doc.data.field2 }
          |}
          |""".stripMargin,
      db
    )
    (resUpdate / "d1" / "f1").as[String] shouldEqual "v1update"
    (resUpdate / "d1" / "f2" / "nestedField").as[String] shouldEqual "v3update"

    (resUpdate / "d1") shouldEqual (resUpdate / "d2")
  }
  test("can project off of data field") {
    val db = aDatabase.sample
    mkCollection(db, "TestColl")
    val res = queryOk(
      s"""|let doc = TestColl.create({
          |  field1: "v1",
          |  field2: { nestedField: "v3" }
          |})
          |doc { data { field1 } }
          |""".stripMargin,
      db
    )

    (res / "data" / "field1").as[String] shouldEqual "v1"

    val resUpdate = queryOk(
      s"""|let doc = TestColl.create({
          |  field1: "v1",
          |  field2: { nestedField: "v3" }
          |})
          |doc.update({field1: "v2"})
          |doc { data { field1 } }
          |""".stripMargin,
      db
    )

    (resUpdate / "data" / "field1").as[String] shouldEqual "v2"
  }
  test("can create/update ttl with timestamp type and access it") {
    val db = aDatabase.sample
    mkCollection(db, "TestColl")
    val ttl = Instant.now().plus(1, ChronoUnit.DAYS)

    val res = queryOk(
      s"""|TestColl.create({
          |  field1: "v1",
          |  ttl: Time.fromString("$ttl")
          |})
          |""".stripMargin,
      db
    )
    (res / "ttl").as[String] shouldEqual ttl.toString

    val ttl2 = ttl.plus(1, ChronoUnit.DAYS)
    val resUpdate = queryOk(
      s"""|let doc = TestColl.create({
          |  field1: "v1update",
          |  ttl: Time.fromString("$ttl")
          |})
          |doc.update({ ttl: Time.fromString("$ttl2") })
          |""".stripMargin,
      db
    )

    (resUpdate / "ttl").as[String] shouldEqual ttl2.toString
  }
  test("setting ttl with non timestamp fails") {
    val db = aDatabase.sample
    mkCollection(db, "TestColl")

    val errRes = queryErr(
      s"""|TestColl.create({ field1: "v1", ttl: "is time even real??" })
          |""".stripMargin,
      db,
      FQL2Params(typecheck = Some(false))
    )

    val expectedSummary =
      """error: Failed to create document in collection `TestColl`.
      |constraint failures:
      |  ttl: Expected Time | Null, provided String
      |at *query*:1:16
      |  |
      |1 | TestColl.create({ field1: "v1", ttl: "is time even real??" })
      |  |                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      |  |""".stripMargin

    (errRes / "error" / "code").as[String] shouldEqual "constraint_failure"
    (errRes / "summary").as[String] shouldEqual expectedSummary

    val docCreate = queryOk(
      s"""|TestColl.create({ field1: "v1"})
          |""".stripMargin,
      db
    )
    val errResUpdate = queryErr(
      s"""|let doc = TestColl.byId(${docCreate / "id"})!
          |doc.update({ ttl: "is time even real??" })
          |""".stripMargin,
      db,
      FQL2Params(typecheck = Some(false))
    )

    val expectedSummaryUpdate =
      s"""error: Failed to update document with id ${(docCreate / "id")
          .as[String]} in collection `TestColl`.
                            |constraint failures:
                            |  ttl: Expected Time | Null, provided String
                            |at *query*:2:11
                            |  |
                            |2 | doc.update({ ttl: "is time even real??" })
                            |  |           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                            |  |""".stripMargin

    (errResUpdate / "error" / "code").as[String] shouldEqual "constraint_failure"
    (errResUpdate / "summary").as[String] shouldEqual expectedSummaryUpdate
  }
  test("id, ts, coll are available at the top level") {
    val db = aDatabase.sample
    mkCollection(db, "TestColl")
    val res = queryOk(
      s"""|TestColl.create({
          |  field1: "v1",
          |}) { id, ts, coll }
          |""".stripMargin,
      db
    )
    (res / "id").isEmpty shouldBe false
    (res / "ts").isEmpty shouldBe false
    (res / "coll").isEmpty shouldBe false
  }
  test("fields can be created/updated with underscores") {
    val db = aDatabase.sample
    mkCollection(db, "TestColl")
    val res = queryOk(
      s"""|TestColl.create({
          |  _field1: "v1",
          |})
          |""".stripMargin,
      db
    )

    (res / "_field1").as[String] shouldBe "v1"

    val resUpdate = queryOk(
      s"""|let doc = TestColl.create({
          |  _field1: "v1",
          |})
          |doc.update({ _field1: "v2" })
          |""".stripMargin,
      db
    )
    (resUpdate / "_field1").as[String] shouldBe "v2"
  }
  test("sub documents can be accessed with and without .data") {
    val db = aDatabase.sample
    mkCollection(db, "Books")
    mkCollection(db, "Authors")
    val res = queryOk(
      s"""|let author = Authors.create({
          |  name: "a1",
          |})
          |let book = Books.create({
          |  title: "b1",
          |  author: author
          |})
          |{
          |  a1: book.author.name,
          |  a2: book.author.data.name,
          |  a3: book.data.author.name,
          |  a4: book.data.author.data.name
          |}
          |""".stripMargin,
      db
    )
    (res / "a1").as[String] shouldBe "a1"
    (res / "a2") shouldEqual (res / "a1")
    (res / "a3") shouldEqual (res / "a1")
    (res / "a4") shouldEqual (res / "a1")
  }
  test(
    "when a document is returned, all meta and non-conflicting fields are at the top level") {
    val db = aDatabase.sample
    mkCollection(db, "Books")
    val res = queryOk(
      s"""|
          |Books.create({
          |  title: "b1",
          |  author: "Hobbes"
          |})
          |""".stripMargin,
      db
    )
    (res / "id").isEmpty shouldBe false
    (res / "ts").isEmpty shouldBe false
    (res / "coll").isEmpty shouldBe false
    (res / "title").as[String] shouldEqual "b1"
    (res / "author").as[String] shouldEqual "Hobbes"

    (res / "data").isEmpty shouldBe true
  }
  test("internal document field access works correctly") {
    val db = aDatabase.sample
    mkCollection(db, "Books")
    val res = queryOk(
      s"""|
          |Books.definition.name
          |""".stripMargin,
      db
    )
    res.as[String] shouldEqual "Books"

    val res2 = queryOk(
      s"""|
          |Books.definition
          |""".stripMargin,
      db
    )
    (res2 / "name").as[String] shouldEqual "Books"
    (res2 / "coll").as[String] shouldEqual "Collection"
    (res2 / "ts").isEmpty shouldBe false
  }
  test(
    "when a document is returned non conflicting fields are not shown at the top level and underneath data") {
    val db = aDatabase.sample
    mkCollection(db, "Books")

    val res = queryOk(
      s"""|
          |Books.create({
          |  title: "Hello!"
          |})
          |""".stripMargin,
      db
    )
    (res / "title").as[String] shouldEqual "Hello!"
    (res / "data").isEmpty shouldEqual true
  }
  // todo: test that conflicting fields are not avail at the top level
  // todo: test with nested data access, data.data.foo
  // todo: test indexing with data field
}
