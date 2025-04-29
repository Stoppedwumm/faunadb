package fauna.api.test

import fauna.codex.json._
import fql.parser.Tokens

class FQL2DocSpec extends FQL2APISpec {
  "update" - {
    test("updates a document in the database with the provided fields") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample
      val res = queryOk(
        s"""|
        |let author = $collection.create({
        |  "foo": "bar",
        |  "seattle": "wa",
        |})
        |let updatedAuthor = author.update({"seattle": "va", "windy": "city"})
        |$collection.byId(author.id)
        |""".stripMargin,
        db
      ).as[JSObject]

      (res / "foo").as[String] shouldEqual "bar"
      (res / "seattle").as[String] shouldEqual "va"
      (res / "windy").as[String] shouldEqual "city"
    }
    test("returns the updated document") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample
      val res = queryOk(
        s"""|
        |let author = $collection.create({
        |  "foo": "bar",
        |  "seattle": "wa",
        |})
        |author.update({"seattle": "va", "windy": "city"})
        |""".stripMargin,
        db
      ).as[JSObject]

      (res / "foo").as[String] shouldEqual "bar"
      (res / "seattle").as[String] shouldEqual "va"
      (res / "windy").as[String] shouldEqual "city"
    }
    test("removes document fields when setting them to null") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample
      val res = queryOk(
        s"""|
        |let author = $collection.create({
        |  "foo": "bar",
        |  "seattle": "wa",
        |})
        |author.update({"seattle": null, "windy": "city"})
        |""".stripMargin,
        db
      ).as[JSObject]

      (res / "foo").as[String] shouldEqual "bar"
      (res / "seattle").isEmpty shouldBe true
      (res / "windy").as[String] shouldEqual "city"
    }
    test(
      "attempting to update a document that doesn't exist returns document_not_found") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample
      val res = queryErr(
        s"""|
        |let author = $collection.create({
        |  "foo": "bar",
        |  "seattle": "wa",
        |})
        |author.delete()
        |author.update({ "hi": "bye" })
        |""".stripMargin,
        db
      )

      (res / "error" / "code").as[String] shouldEqual "document_not_found"
    }
    for (f <- Tokens.ReservedFieldNames) {
      test(s"rejects reserved field `$f`") {
        val db = aDatabase.sample
        val collection = aCollection(db).sample
        val errRes = queryErr(
          s"""|
            |let author = $collection.create({
            |  "foo": "bar",
            |  "seattle": "wa",
            |})
            |author.update({$f: "bar"})
            |""".stripMargin,
          db
        )
        val code = (errRes / "error" / "code").as[String]
        // Some fail at runtime, some in type checking.
        (Seq("constraint_failure", "invalid_query") contains code) shouldBe true
      }
    }
  }
  "replace" - {
    test("correctly replaces a document") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample
      val createdAuthor = queryOk(
        s"""|
            |$collection.create({
            |  "foo": "bar",
            |  "seattle": "wa",
            |})
            |""".stripMargin,
        db
      ).as[JSObject]

      val replacedAuthor = queryOk(
        s"""|
            |let author = $collection.byId(${createdAuthor / "id"})!
            |author.replace({"foo": "berry", "yellow": "crayon"})
            |""".stripMargin,
        db
      ).as[JSObject]

      val readReplacedAuthor = queryOk(
        s"""|
        |$collection.byId(${createdAuthor / "id"})
        |""".stripMargin,
        db
      ).as[JSObject]

      (replacedAuthor / "id").as[String] shouldEqual (createdAuthor / "id")
        .as[String]
      (replacedAuthor / "foo").as[String] shouldEqual "berry"
      (replacedAuthor / "yellow").as[String] shouldEqual "crayon"
      (replacedAuthor / "seattle").isEmpty shouldBe true

      (readReplacedAuthor / "id").as[String] shouldEqual (createdAuthor / "id")
        .as[String]
      (readReplacedAuthor / "foo").as[String] shouldEqual (replacedAuthor / "foo")
        .as[String]
      (readReplacedAuthor / "yellow")
        .as[String] shouldEqual (replacedAuthor / "yellow").as[String]
      (readReplacedAuthor / "seattle").isEmpty shouldBe true
    }
    test(
      "returns document_not_found when attempting to replace a deleted document") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample
      val res = queryErr(
        s"""|
        |let author = $collection.create({
        |  "foo": "bar",
        |  "seattle": "wa",
        |})
        |author.delete()
        |author.replace({"foo": "berry", "yellow": "crayon"})
        |""".stripMargin,
        db
      )

      (res / "error" / "code").as[String] shouldEqual "document_not_found"
    }
    for (f <- Tokens.ReservedFieldNames) {
      test(s"rejects reserved field `$f`") {
        val db = aDatabase.sample
        val collection = aCollection(db).sample
        val errRes = queryErr(
          s"""|
            |let author = $collection.create({
            |  "foo": "bar",
            |  "seattle": "wa",
            |})
            |author.replace({$f: "bar"})
            |""".stripMargin,
          db
        )
        val code = (errRes / "error" / "code").as[String]
        // Some fail at runtime, some in type checking.
        (Seq("constraint_failure", "invalid_query") contains code) shouldBe true
      }
    }
  }
  "delete" - {
    test("correctly deletes a document") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample
      val res = queryOk(
        s"""|
        |let author = $collection.create({
        |  "foo": "bar",
        |  "seattle": "wa",
        |})
        |author.delete()
        |$collection.byId(author.id).exists()
        |""".stripMargin,
        db
      )

      res.as[Boolean] shouldBe false
    }
    test("delete returns the document id") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample
      val createRes = queryOk(
        s"""|
            |$collection.create({
            |  "foo": "bar",
            |  "seattle": "wa",
            |})""".stripMargin,
        db,
        FQL2Params(format = Some("tagged"))
      ).as[JSObject]

      val res = queryOk(
        s"""|
            |let author = $collection.byId(${createRes / "@doc" / "id"})!
            |author.delete()""".stripMargin,
        db,
        FQL2Params(format = Some("tagged"))
      ).as[JSObject]

      (res / "@ref" / "coll" / "@mod").as[String] shouldEqual s"$collection"
      (res / "@ref" / "id").as[String] shouldEqual (createRes / "@doc" / "id")
        .as[String]
    }
    test(
      "attempting to delete a document that doesn't exist returns document_not_found") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample
      val res = queryErr(
        s"""|
        |let author = $collection.create({
        |  "foo": "bar",
        |  "seattle": "wa",
        |})
        |author.delete()
        |author.delete()
        |""".stripMargin,
        db
      )

      (res / "error" / "code").as[String] shouldEqual "document_not_found"
    }
  }

  "*Data variants" - {
    test("allow create/replace/update with reserved field names") {
      val db = aDatabase.sample
      val col = aCollection(db).sample

      // No "update" field allowed in standard create.
      val resCrErr = queryErr(s"""$col.create({ a: 0, update : 0 })""", db)
      (resCrErr / "error" / "code").as[String] shouldEqual "constraint_failure"

      // It's fine in createData, though, and the reserved name "update"
      // is rendered into data.
      val resCr = queryOk(s"""$col.createData({ a: 0, "update" : 0 })""", db)
      (resCr / "a").as[Long] shouldEqual 0
      (resCr / "data" / "update").as[Long] shouldEqual 0
      (resCr / "data" / "a").isEmpty shouldEqual true
      (resCr / "update").isEmpty shouldEqual true

      // Can't update the "update" field with standard update.
      val id = (resCr / "id").as[String]
      val resUpErr =
        queryErr(s"""$col.byId("$id")!.update({ "update" : 1 })""", db)
      (resUpErr / "error" / "code").as[String] shouldEqual "constraint_failure"

      // It's fine with updateData, though. Reserved names go under data.
      val resUp = queryOk(s"""$col.byId("$id")!.updateData({ "update" : 1 })""", db)
      (resUp / "a").as[Long] shouldEqual 0
      (resUp / "data" / "update").as[Long] shouldEqual 1
      (resUp / "data" / "a").isEmpty shouldEqual true
      (resUp / "update").isEmpty shouldEqual true

      // Can't replace with a "replace" field with standard replace.
      val resReErr =
        queryErr(s"""$col.byId("$id")!.replace({ "replace" : 0 })""", db)
      (resReErr / "error" / "code").as[String] shouldEqual "constraint_failure"

      // It's fine with replaceData, though. Reserved names go under data.
      val resRe =
        queryOk(s"""$col.byId("$id")!.replaceData({ "replace" : 0 })""", db)
      (resRe / "a").isEmpty shouldEqual true
      (resRe / "data" / "update").isEmpty shouldEqual true
      (resRe / "data" / "replace").as[Long] shouldEqual 0
      (resRe / "data" / "a").isEmpty shouldEqual true
      (resRe / "update").isEmpty shouldEqual true

      // Specifying `id` in createData doesn't set a manual id.
      val resCrId =
        queryOk(s"""$col.createData({ id: "foo", a: 0, "update" : 0 })""", db)
      (resCrId / "a").as[Long] shouldEqual 0
      (resCrId / "data" / "id").as[String] shouldEqual "foo"
      (resCrId / "data" / "update").as[Long] shouldEqual 0
      (resCrId / "data" / "a").isEmpty shouldEqual true
      (resCrId / "update").isEmpty shouldEqual true
    }

    test("handle metadata fields") {
      val db = aDatabase.sample
      val col = aCollection(db).sample

      val ttlStr = "2200-01-01T01:01:01Z"
      val resCr =
        queryOk(s"""$col.create({ a: 0, ttl: Time.fromString("$ttlStr") })""", db)

      val ttlDataStr = "2300-01-01T01:01:01Z"
      val id = (resCr / "id").as[String]
      val resUpD = queryOk(
        s"""$col.byId("$id")!.updateData({ ttl: Time.fromString("$ttlDataStr") })""",
        db)
      (resUpD / "a").as[Long] shouldEqual 0
      (resUpD / "ttl").as[String] shouldEqual ttlStr
      (resUpD / "data" / "ttl").as[String] shouldEqual ttlDataStr
    }

    test("native collections don't have them") {
      val db = aDatabase.sample

      // for credential and token creation
      queryOk("Collection.create({name: 'Dummy'})", db)

      val native = Seq(
        "AccessProvider" -> "{name: 'AP', issuer: 'idp', jwks_uri: 'https://foo.com'}",
        "Collection" -> "{name: 'C'}",
        "Credential" -> "{document: Dummy.create({})}",
        "Function" -> "{name: 'F', body: 'x => x'}",
        "Key" -> "{role: 'admin'}",
        "Role" -> "{name: 'R'}",
        "Token" -> "{document: Dummy.create({})}"
      )

      native.foreach { case (col, create) =>
        info(col)

        (queryErr(s"$col.createData({})", db) / "summary")
          .as[String] should include("does not have field `createData`")
        (queryErr(
          s"$col.createData({})",
          db,
          FQL2Params(typecheck = Some(false))) / "summary")
          .as[String] should include("The function `createData` does not exist")

        val doc = s"$col.create($create)"

        (queryErr(s"$doc.updateData({})", db) / "summary")
          .as[String] should include("does not have field `updateData`")
        (queryErr(
          s"$doc.updateData({})",
          db,
          FQL2Params(typecheck = Some(false))) / "summary")
          .as[String] should include("The function `updateData` does not exist")

        (queryErr(s"$doc.replaceData({})", db) / "summary")
          .as[String] should include("does not have field `replaceData`")
        (queryErr(
          s"$doc.replaceData({})",
          db,
          FQL2Params(typecheck = Some(false))) / "summary")
          .as[String] should include("The function `replaceData` does not exist")
      }
    }
  }

  "Object methods" - {
    test("calling .keys on a document returns its keys") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample
      val res = queryOk(
        s"""|
        |let d = $collection.create({
        |  "a": 0,
        |  "b": 1,
        |})
        |Object.keys(d)
        |""".stripMargin,
        db
      ).as[JSArray]

      res shouldBe JSArray("id", "coll", "ts", "a", "b")
    }

    test("calling .keys on a non-existent document errors") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample
      val res = queryErr(
        s"""|let d = $collection.create({
            |  a: 0,
            |  b: 1,
            |  id: 1234,
            |})
            |d.delete()
            |Object.keys(d)""".stripMargin,
        db
      )

      (res / "error" / "code").as[String] shouldBe "invalid_argument"
      (res / "error" / "message")
        .as[String] shouldBe s"invalid argument `object`: collection `$collection` does not contain document with id 1234."
    }

    test("calling .values on a document returns its field values") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample
      val res = queryOk(
        s"""|
        |let d = $collection.create({
        |  "x": 0,
        |  "y": "a",
        |})
        |Object.values(d)
        |""".stripMargin,
        db
      ).as[JSArray]

      res.length shouldBe 5
      res.value.slice(3, 5) shouldBe Seq[JSValue](0, "a")
    }

    test("calling .entries on a document returns its field value pairs") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample
      val res = queryOk(
        s"""|
        |let d = $collection.create({
        |  "a": 0,
        |  "b": "x",
        |})
        |Object.entries(d)
        |""".stripMargin,
        db
      ).as[JSArray]

      res.length shouldBe 5
      res.value.slice(3, 5) shouldBe Seq(JSArray("a", 0), JSArray("b", "x"))
    }
  }

  test("string argument should work as ID") {
    val db = aDatabase.sample
    val collection = aCollection(db).sample
    queryOk(
      s"$collection.byId(myArg).exists()",
      db,
      FQL2Params(arguments = JSObject("myArg" -> "1234"))
    ) shouldBe JSFalse
  }
}
