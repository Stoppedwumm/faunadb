package fauna.api.test

import fauna.codex.json
import fauna.codex.json._
import fauna.prop.api.Database
import fauna.prop.api.Query27Helpers._
import scala.concurrent.duration._

class FQL2CollectionsSpec extends FQL2APISpec {
  "create" - {
    test("create collection schema") {
      val db = aDatabase.sample

      val res = queryOk(
        """Collection.create({name: "Book"})""",
        db
      )

      (res / "name").as[String] shouldBe "Book"
      (res / "coll").as[String] shouldBe "Collection"
      (res / "id").isEmpty shouldBe true

      val doc = queryOk(
        """Book.create({ title: "The Hitchhiker's Guide to the Galaxy" })""",
        db
      )

      (doc / "title").as[String] shouldBe "The Hitchhiker's Guide to the Galaxy"
      (doc / "coll").as[String] shouldBe "Book"
      (doc / "id").isEmpty shouldBe false

      noException shouldBe thrownBy {
        queryOk("Book.all()", db)
      }
    }

    test("collection reserved name error") {
      val db = aDatabase.sample

      val res = queryErr(
        """Collection.create({name: "Collection"})""",
        db
      )

      (res / "error" / "code").as[String] shouldEqual "constraint_failure"
      (res / "error" / "message")
        .as[String] shouldEqual "Failed to create Collection."
      (res / "summary")
        .as[String] shouldEqual """|error: Failed to create Collection.
        |constraint failures:
        |  name: The identifier `Collection` is reserved.
        |at *query*:1:18
        |  |
        |1 | Collection.create({name: "Collection"})
        |  |                  ^^^^^^^^^^^^^^^^^^^^^^
        |  |""".stripMargin
    }

    test("accepts an optional `data` field for arbitrary metadata object") {
      val db = aDatabase.sample

      val res = queryOk(
        """|Collection.create({
           |  name: "Thing",
           |  data: { foo: "bar" }
           |})""".stripMargin,
        db
      )

      (res / "name").as[String] shouldBe "Thing"
      (res / "coll").as[String] shouldBe "Collection"
      (res / "data" / "foo").as[String] shouldBe "bar"
      (res / "id").isEmpty shouldBe true
    }

    test("only accepts objects for `data` field") {
      val db = aDatabase.sample

      val errRes = queryErr(
        """|Collection.create({
           |  name: "Thing",
           |  data: "foobar"
           |})""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      )

      (errRes / "error" / "code").as[String] shouldEqual "constraint_failure"
      (errRes / "error" / "message")
        .as[String] shouldEqual "Failed to create Collection."
      (errRes / "summary").as[String] shouldEqual
        """|error: Failed to create Collection.
           |constraint failures:
           |  data: Expected { *: Any } | Null, provided String
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Thing",
           |3 | |   data: "foobar"
           |4 | | })
           |  | |__^
           |  |""".stripMargin
    }

    test("only accepts objects for `data` field (typechecked)") {
      val db = aDatabase.sample

      val errRes = queryErr(
        """|Collection.create({
           |  name: "Thing",
           |  data: "foobar"
           |})""".stripMargin,
        db
      )

      (errRes / "error" / "code").as[String] shouldEqual "invalid_query"
      (errRes / "error" / "message")
        .as[String] shouldEqual "The query failed 1 validation check"
      (errRes / "summary").as[String] shouldEqual
        """|error: Type `String` is not a subtype of `Null | { *: Any }`
           |at *query*:3:9
           |  |
           |3 |   data: "foobar"
           |  |         ^^^^^^^^
           |  |""".stripMargin
    }

    test("allows setting ids") {
      val db = aDatabase.sample
      queryOk("""Collection.create({ name: "Book" })""", db)

      val doc = queryOk("""Book.create({ id: "0", name: "The First Book" })""", db)
      (doc / "id").as[String] shouldEqual "0"
      (doc / "name").as[String] shouldEqual "The First Book"

      val doc2 = queryOk("""Book.create({ id: 1, name: "The Second Book" })""", db)
      (doc2 / "id").as[String] shouldEqual "1"
      (doc2 / "name").as[String] shouldEqual "The Second Book"

      val docGet = queryOk(s"""Book.byId(${doc / "id"})""", db)
      (docGet / "id").as[String] shouldEqual "0"
      (docGet / "name").as[String] shouldEqual "The First Book"

      // Creating with an existing ID is an error, as in FQL 4.
      val errExists =
        queryErr("""Book.create({ id: "0", title: "The 1st Book" })""", db)
      (errExists / "error" / "code").as[String] shouldEqual "document_id_exists"

      // Creating with a negative ID is an error, unlike FQL4.
      val errNeg = queryErr("""Book.create({ id: "-1", title: "X" })""", db)
      (errNeg / "error" / "code").as[String] shouldEqual "constraint_failure"
      (errNeg / "error" / "constraint_failures" / 0 / "message")
        .as[String] should include("id must be nonnegative")

      // Arbitrary types are not allowed for IDs.
      val errWord =
        queryErr("""Book.create({ id: "taco", title: "Taco" })""", db)
      (errWord / "error" / "code").as[String] shouldEqual "constraint_failure"
      (errWord / "error" / "constraint_failures" / 0 / "message")
        .as[String] shouldBe "Expected ID | String | Long, provided String"

      val err0 =
        queryErr(
          """Book.create({ id: { x: 1 }, title: "Taco" })""",
          db,
          FQL2Params(typecheck = Some(false)))
      (err0 / "error" / "code").as[String] shouldEqual "constraint_failure"
      (err0 / "error" / "constraint_failures" / 0 / "message")
        .as[String] shouldBe "Expected ID | String | Long, provided { x: Int }"

      // Typechecking disallows invalid types on IDs.
      val err1 =
        queryErr("""Book.create({ id: { x: 1 }, title: "Taco" })""", db)
      (err1 / "error" / "code").as[String] shouldEqual "invalid_query"
      (err1 / "summary")
        .as[String] shouldBe (
        """|error: Type `{ x: 1 }` is not a subtype of `Null | ID`
           |at *query*:1:19
           |  |
           |1 | Book.create({ id: { x: 1 }, title: "Taco" })
           |  |                   ^^^^^^^^
           |  |""".stripMargin
      )
    }
  }

  "list all" - {
    test("list all collections") {
      val db = aDatabase.sample
      val collection0 = aCollection(db).sample
      val collection1 = aCollection(db).sample
      val collection2 = aCollection(db).sample
      val collection3 = aCollection(db).sample

      val res = queryOk(
        """Collection.all().take(5).toArray() { name }""",
        db
      )

      (res / 0 / "name").as[String] shouldBe collection0
      (res / 1 / "name").as[String] shouldBe collection1
      (res / 2 / "name").as[String] shouldBe collection2
      (res / 3 / "name").as[String] shouldBe collection3
    }
  }

  "update" - {
    test("update collection schema") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample

      val res = queryOk(
        s"""$collection.definition.update({name: "Authors", history_days: 10})""",
        db
      )

      (res / "name").as[String] shouldBe "Authors"
      (res / "history_days").as[Long] shouldBe 10L
      (res / "coll").as[String] shouldBe "Collection"
      (res / "id").isEmpty shouldBe true

      val doc = queryOk(
        """Authors.create({name: "William Shakespeare"})""",
        db
      )

      (doc / "name").as[String] shouldBe "William Shakespeare"
      (doc / "coll").as[String] shouldBe "Authors"
      (doc / "id").isEmpty shouldBe false
    }

    test("don't accept invalid fields") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample

      val errRes = queryErr(
        s"""$collection.definition.update({meta: "metadata"})""",
        db
      )

      (errRes / "error" / "code").as[String] shouldEqual "constraint_failure"
      (errRes / "error" / "message")
        .as[String] shouldEqual s"Failed to update Collection `$collection`."
      val expectedSummary =
        s"""error: Failed to update Collection `$collection`.
                  |constraint failures:
                  |  meta: Unexpected field provided
                  |at *query*:1:51
                  |  |
                  |1 | $collection.definition.update({meta: "metadata"})
                  |  |                                                   ^^^^^^^^^^^^^^^^^^^^
                  |  |""".stripMargin
      (errRes / "summary").as[String] shouldEqual expectedSummary
    }

    test("accepts and correctly renders indexes field") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample

      val res = queryOk(
        s"""|$collection.definition.update({indexes: {
            |  termAndValue: {
            |    terms: [{ field: "name" }],
            |    values: [{ field: "_ref" }],
            |  },
            |  termless: {
            |    values: [{ field: "_ref" }],
            |  },
            |  valueless: {
            |    terms: [{ field: "name" }],
            |  },
            |  mvaIdx: {
            |    terms: [{ field: "arr", mva: false }]
            |  }
            |}})
            |""".stripMargin,
        db
      )

      (res / "backingIndexes").isEmpty shouldBe true
      (res / "indexes" / "termAndValue" / "terms" / 0 / "field")
        .as[String] shouldBe "name"
      (res / "indexes" / "termAndValue" / "terms" / 0 / "mva").isEmpty shouldBe true
      (res / "indexes" / "termAndValue" / "values" / 0 / "field")
        .as[String] shouldBe "_ref"
      (res / "indexes" / "termAndValue" / "values" / 0 / "mva").isEmpty shouldBe true

      (res / "indexes" / "termless" / "terms")
        .asOpt[String] shouldBe None
      (res / "indexes" / "termless" / "values" / 0 / "field")
        .as[String] shouldBe "_ref"
      (res / "indexes" / "termless" / "values" / 0 / "mva").isEmpty shouldBe true

      (res / "indexes" / "valueless" / "terms" / 0 / "field")
        .as[String] shouldBe "name"
      (res / "indexes" / "valueless" / "terms" / 0 / "mva").isEmpty shouldBe true
      (res / "indexes" / "valueless" / "values")
        .asOpt[String] shouldBe None

      (res / "indexes" / "mvaIdx" / "terms" / 0 / "field")
        .as[String] shouldBe "arr"
      (res / "indexes" / "mvaIdx" / "terms" / 0 / "mva")
        .as[Boolean] shouldBe false
      (res / "indexes" / "mvaIdx" / "values")
        .asOpt[String] shouldBe None
    }

    test("can't select backingIndexes off of collection schema document") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample

      val res = queryOk(
        s"""|$collection.definition.update({indexes: {
            |  termAndValue: {
            |    terms: [{ field: "name" }],
            |    values: [{ field: "_ref" }],
            |  }
            |}}).backingIndexes
            |""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      )

      res shouldBe a[JSNull]
    }

    test("don't accept invalid shape") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample

      val errRes = queryErr(
        s"""|$collection.definition.update({indexes: {
            |  no_terms_and_values: {
            |    unique: true
            |  },
            |  empty_shape: {
            |  }
            |}})
            |""".stripMargin,
        db
      )

      val expectedCode = "constraint_failure"
      val expectedMessage = s"Failed to update Collection `$collection`."

      val expectedSummary =
        s"""|error: Failed to update Collection `$collection`.
            |constraint failures:
            |  indexes.no_terms_and_values.unique: Unexpected field provided
            |  indexes.no_terms_and_values: Union requires fields [terms], [values], or [terms, values]
            |  indexes.empty_shape: Union requires fields [terms], [values], or [terms, values]
            |at *query*:1:51
            |  |
            |1 |   $collection.definition.update({indexes: {
            |  |  ___________________________________________________^
            |2 | |   no_terms_and_values: {
            |3 | |     unique: true
            |4 | |   },
            |5 | |   empty_shape: {
            |6 | |   }
            |7 | | }})
            |  | |___^
            |  |""".stripMargin

      (errRes / "summary").as[String] shouldEqual expectedSummary
      (errRes / "error" / "code").as[String] shouldEqual expectedCode
      (errRes / "error" / "message").as[String] shouldEqual expectedMessage
    }

    test("fails if non asc/desc value is passed for order") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample

      val errRes = queryErr(
        s"""|$collection.definition.update({indexes: {
            |  invalid_order: {
            |    terms: [{ field: "name" }],
            |    values: [ { field: "age", order: "ascc" } ],
            |    unique: true
            |  },
            |}})
            |""".stripMargin,
        db
      )

      (errRes / "error" / "code").as[String] shouldEqual "constraint_failure"
    }
  }

  "get by name" - {
    test("get collections by name") {
      val db = aDatabase.sample

      // Get user collections.
      val res = queryErr("""Collection("Book")""", db)
      (res / "error" / "code").as[String] shouldBe "invalid_argument"
      (res / "error" / "message")
        .as[String] shouldBe "invalid argument `collection`: No such user collection `Book`."

      queryOk("""Collection.create({ name: "Book" })""", db)

      val doc = queryOk(
        """|let name = "Book"
           |Collection(name).create({ title: "Moby Dick" })""".stripMargin,
        db)

      (doc / "title").as[String] shouldBe "Moby Dick"
      (doc / "coll").as[String] shouldBe "Book"
      (doc / "id").isEmpty shouldBe false

      // Don't get system collections.
      val res2 = queryErr("""Collection("Collection")""", db)
      (res2 / "error" / "code").as[String] shouldBe "invalid_argument"
      (res2 / "error" / "message")
        .as[String] shouldBe "invalid argument `collection`: No such user collection `Collection`."
    }
  }

  "adhoc" - {
    test(".all is the set of all docs") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample

      queryOk(
        s"""|$collection.create({ name: "Bob" })
            |$collection.create({ name: "Alice" })""".stripMargin,
        db)

      val res = queryOk(s"$collection.all()", db)

      (res / "data" / 0 / "name").as[String] shouldEqual "Bob"
      (res / "data" / 1 / "name").as[String] shouldEqual "Alice"
    }

    test(".where is alias for .all.where") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample

      queryOk(
        s"""|$collection.create({ name: "Bob" })
            |$collection.create({ name: "Alice" })""".stripMargin,
        db)

      val res1 = queryOk(s"""$collection.all().where(.name == "Alice")""", db)
      val res2 = queryOk(s"""$collection.where(.name == "Alice")""", db)

      (res1 / "data" / 0 / "name").as[String] shouldEqual "Alice"

      res1 shouldEqual res2
    }

    test(".firstWhere is alias for .all.firstWhere") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample

      queryOk(
        s"""|$collection.create({ name: "Bob" })
            |$collection.create({ name: "Alice" })""".stripMargin,
        db)

      val res1 = queryOk(s"""$collection.all().firstWhere(.name == "Alice")""", db)
      val res2 = queryOk(s"""$collection.firstWhere(.name == "Alice")""", db)

      (res1 / "name").as[String] shouldEqual "Alice"

      res1 shouldEqual res2
    }

    test(".take can take args from json") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample

      queryOk(s"""|$collection.create({ name: "Alice" })""".stripMargin, db)

      queryOk(
        s"""$collection.all().take(myLimit)""",
        db,
        FQL2Params(arguments = JSObject("myLimit" -> 5)))
    }

    test("deleting in protected mode doesn't 500") {
      val db = aProtectedDatabase.sample

      queryOk(
        """Collection.create({ name: "Foo" })""",
        db
      )

      val err = queryErr("Foo.definition.delete()", db)
      (err / "error" / "code").as[String] shouldEqual "constraint_failure"
      (err / "error" / "message")
        .as[String] shouldEqual "Failed to delete Collection `Foo`."
    }

    test("v4 delete doesn't work when protected mode is on") {
      val db = aProtectedDatabase.sample

      queryOk(
        """Collection.create({ name: "Foo" })""",
        db
      )

      val res1 = api.query(DeleteF(ClsRefV("Foo")), db.adminKey)
      res1 should respond(BadRequest)
      (res1.json / "errors").as[JSArray].length shouldBe 1
      (res1.json / "errors" / 0 / "code")
        .as[String] shouldBe "schema validation failed"
      (res1.json / "errors" / 0 / "description")
        .as[String] shouldBe "Cannot delete collection: destructive change forbidden because database is in protected mode."
    }
  }

  "indexes" - {
    case class Person(name: String, age: Long)

    implicit object PersonDecoder extends JsonDecoder[Person] {
      def decode(stream: json.JsonCodec.In): Person =
        Person((stream / "name").as[String], (stream / "age").as[Long])
    }

    val bob20 = Person("bob", 20)
    val bob40 = Person("bob", 40)
    val alice21 = Person("alice", 21)
    val alice41 = Person("alice", 41)
    val mark22 = Person("mark", 22)
    val mark42 = Person("mark", 42)

    def makePersons(db: Database): Unit =
      queryOk(
        s"""|Person.create({name: "${bob40.name}", age: ${bob40.age}})
            |Person.create({name: "${bob20.name}", age: ${bob20.age}})
            |Person.create({name: "${alice21.name}", age: ${alice21.age}})
            |Person.create({name: "${alice41.name}", age: ${alice41.age}})
            |Person.create({name: "${mark22.name}", age: ${mark22.age}})
            |Person.create({name: "${mark42.name}", age: ${mark42.age}})
            |""".stripMargin,
        db
      )

    test("indexes become methods on Collections") {
      val db = aDatabase.sample

      queryOk(
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byName: {
            |      terms: [{ field: "name" }]
            |    }
            |  }
            |})
            |""".stripMargin,
        db
      )

      makePersons(db)

      // byName with index natural order
      val res = queryOk(s"""Person.byName("bob")""", db)
      (res / "data").as[Seq[Person]] shouldBe Seq(bob40, bob20)
    }
    test(
      "misspelling an index call on a collection returns invalid_function_invocation") {
      val db = aDatabase.sample

      queryOk(
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byName: {
            |      terms: [{ field: "name" }]
            |    },
            |  }
            |})
            |""".stripMargin,
        db
      )

      val res1 = queryErr(
        s"""Person.byyName("bob")""",
        db,
        FQL2Params(typecheck = Some(true)))
      (res1 / "error" / "code").as[String] shouldBe "invalid_query"

      val res2 = queryErr(
        s"""Person.byyName("bob")""",
        db,
        FQL2Params(typecheck = Some(false)))
      (res2 / "error" / "code").as[String] shouldBe "invalid_function_invocation"
    }
    test("indexes sorted by single value") {
      val db = aDatabase.sample

      queryOk(
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byNameOrderedByAge: {
            |      terms: [{ field: "name" }],
            |      values: [{ field: "age" }]
            |    }
            |  }
            |})
            |""".stripMargin,
        db
      )

      makePersons(db)

      // byName ordered by asc(.age)
      val res = queryOk(s"""Person.byNameOrderedByAge("bob")""", db)
      (res / "data").as[Seq[Person]] shouldBe Seq(bob20, bob40)
    }
    test("indexes ordered by multiple values") {
      val db = aDatabase.sample

      queryOk(
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    allOrderedByNameAndAge: {
            |      values: [{ field: "name" }, { field: "age" }]
            |    }
            |  }
            |})
            |""".stripMargin,
        db
      )

      makePersons(db)

      // all ordered by asc(.name) and asc(.age)
      val res = queryOk(s"""Person.allOrderedByNameAndAge()""", db)
      (res / "data")
        .as[Seq[Person]] shouldBe Seq(alice21, alice41, bob20, bob40, mark22, mark42)
    }
    test("subsequent Collection.all queries with different scope ids") {
      val dbOne = aDatabase.sample
      val dbTwo = aDatabase.sample

      queryOk(
        s"""|Collection.create({
            |  name: "Person"
            |})
            |""".stripMargin,
        dbTwo
      )

      // Invoke Collection.all on db with no collections first.
      //
      // In the original impl calling with the first db would set that scope for
      // the index config used for AllDocuments on the
      // SchemaCollectionCompanion. Once called once, the index config with the
      // original scope remained for subsequent calls
      queryOk(s"""Collection.all()""", dbOne)
      val dbTwoCollections = queryOk(s"""Collection.all()""", dbTwo)
      (dbTwoCollections / "data" / 0 / "name").as[String] shouldEqual "Person"
    }
    test("index sorts based on passed in order") {
      val db = aDatabase.sample

      queryOk(
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    allOrderedByNameAndAge: {
            |      values: [{ field: "name", order: "asc" }, { field: "age", order: "desc" }]
            |    }
            |  }
            |})
            |""".stripMargin,
        db
      )

      makePersons(db)

      // all ordered by asc(.name) and desc(.age)
      val res = queryOk(s"""Person.allOrderedByNameAndAge()""", db)
      (res / "data")
        .as[Seq[Person]] shouldBe Seq(alice41, alice21, bob40, bob20, mark42, mark22)

    }
    test("index invoked with incorrect number of terms returns error") {
      val db = aDatabase.sample

      queryOk(
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byName: {
            |      terms: [{ field: "name" }]
            |    }
            |  }
            |})
            |""".stripMargin,
        db
      )

      val res1 =
        queryErr(s"""Person.byName()""", db, FQL2Params(typecheck = Some(true)))
      (res1 / "error" / "code").as[String] shouldEqual "invalid_query"

      val res2 =
        queryErr(s"""Person.byName()""", db, FQL2Params(typecheck = Some(false)))
      (res2 / "error" / "code").as[String] shouldEqual "invalid_function_invocation"
    }
    test("index terms support nested fields") {
      val db = aDatabase.sample

      queryOk(
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byCity: {
            |      terms: [{ field: "address.city" }]
            |    }
            |  }
            |})
            |""".stripMargin,
        db
      )
      queryOk(
        s"""|
            |Person.create({name: "bob", address: {city: "Seattle"}})
            |""".stripMargin,
        db
      )
      val res = queryOk(
        s"""|
            |Person.byCity("Seattle")
            |""".stripMargin,
        db
      )
      (res / "data" / 0 / "name").as[String] shouldEqual "bob"
      (res / "data" / 0 / "address" / "city").as[String] shouldEqual "Seattle"
    }
    test("index paths allow fqlx expressions") {
      val db = aDatabase.sample

      queryOk(
        s"""|Collection.create({
            |  name: "A",
            |  indexes: { test: { terms: [{ field: "a.b" }] } }
            |})""".stripMargin,
        db
      )
      queryOk(
        s"""|A.definition.update({
            |  indexes: { test: { terms: [{ field: "a['b']" }] } }
            |})""".stripMargin,
        db
      )
      queryOk(
        s"""|A.definition.update({
            |  indexes: { test: { terms: [{ field: "a[0]" }] } }
            |})""".stripMargin,
        db
      )
      queryOk(
        s"""|A.definition.update({
            |  indexes: { test: { terms: [{ field: "a['f oo']" }] } }
            |})""".stripMargin,
        db
      )
    }
    test("index paths allow fqlx expressions (pending)") {
      val db = aDatabase.sample
      queryOk(
        s"""|Collection.create({
            |  name: "A",
            |  indexes: { test: { terms: [{ field: ".['f oo']" }] } }
            |})""".stripMargin,
        db
      )
      queryOk(
        s"""|A.definition.update({
            |  indexes: { test: { terms: [{ field: "['f oo']" }] } }
            |})""".stripMargin,
        db
      )
    }
    test("index values support ordering on nested fields") {
      val db = aDatabase.sample

      queryOk(
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byCity: {
            |      values: [{ field: "address.city", order: "desc" }]
            |    }
            |  }
            |})
            |""".stripMargin,
        db
      )
      queryOk(
        s"""|
            |Person.create({name: "aname", address: {city: "Ann Arbor"}})
            |Person.create({name: "bname", address: {city: "Seattle"}})
            |Person.create({name: "cname", address: {city: "Chicago"}})
            |""".stripMargin,
        db
      )
      val res = queryOk(
        s"""|
            |Person.byCity()
            |""".stripMargin,
        db
      )

      (res / "data" / 0 / "name").as[String] shouldEqual "bname"
      (res / "data" / 0 / "address" / "city").as[String] shouldEqual "Seattle"
      (res / "data" / 1 / "name").as[String] shouldEqual "cname"
      (res / "data" / 1 / "address" / "city").as[String] shouldEqual "Chicago"
      (res / "data" / 2 / "name").as[String] shouldEqual "aname"
      (res / "data" / 2 / "address" / "city").as[String] shouldEqual "Ann Arbor"
    }
    test("indexes on documents") {
      val db = aDatabase.sample

      queryOk(
        s"""|Collection.create({
            |  name: "Person",
            |})
            |Collection.create({
            |  name: "Orders",
            |  indexes: {
            |    byCustomer: {
            |      terms: [{ field: "customer" }]
            |    }
            |  }
            |})
            |""".stripMargin,
        db
      )
      queryOk(
        s"""|
            |let customer = Person.create({name: "bob"})
            |Orders.create({date: "06/23/22", customer: customer})
            |""".stripMargin,
        db
      )
      val res = queryOk(
        s"""|let customer = Person.firstWhere(.name == "bob")
            |Orders.byCustomer(customer) { date, customer { name } }
            |""".stripMargin,
        db
      )

      (res / "data" / 0 / "date").as[String] shouldEqual "06/23/22"
      (res / "data" / 0 / "customer" / "name").as[String] shouldEqual "bob"
    }
    test("index on multiple terms") {
      val db = aDatabase.sample

      queryOk(
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byNameAndAge: {
            |      terms: [{ field: "name" }, { field: "age" }]
            |    }
            |  }
            |})
            |""".stripMargin,
        db
      )

      makePersons(db)

      val res = queryOk(s"""Person.byNameAndAge("bob", 40)""", db)
      (res / "data").as[Seq[Person]] shouldBe Seq(bob40)
    }

    /** At some point we'll want to add an fqlx indexer of sorts that uses our ReadBroker to pull
      * fields out of the data.  When this happens indexing on id and coll will work as expected.
      * If we add in an implementation before this we will likely want to change how these values
      * are indexed because the storage format doesn't directly map to the fields we want to index.
      * Because id and coll are already indexed and available, opting to not support this to start.
      * Can re-evaluate based on customer feedback if need be.
      */
    test("don't allow index terms on id, coll, or values on coll") {
      val db = aDatabase.sample
      val errResCollTerm = queryErr(
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byColl: {
            |      terms: [{ field: "coll" }],
            |    },
            |  }
            |})""".stripMargin,
        db
      )
      val expectedSummary =
        """|error: Failed to create Collection.
           |constraint failures:
           |  indexes.byColl.terms[0].field: `coll` is not supported as an index term.
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Person",
           |3 | |   indexes: {
           |4 | |     byColl: {
           |5 | |       terms: [{ field: "coll" }],
           |6 | |     },
           |7 | |   }
           |8 | | })
           |  | |__^
           |  |""".stripMargin
      (errResCollTerm / "summary").as[String] shouldBe expectedSummary
      (errResCollTerm / "error" / "code").as[String] shouldBe "constraint_failure"

      val errResCollValue = queryErr(
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byColl: {
            |      values: [{ field: "coll" }],
            |    },
            |  }
            |})""".stripMargin,
        db
      )

      val expectedSummaryCollValue =
        """|error: Failed to create Collection.
           |constraint failures:
           |  indexes.byColl.values[0].field: `coll` is not supported as a specified index value.
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Person",
           |3 | |   indexes: {
           |4 | |     byColl: {
           |5 | |       values: [{ field: "coll" }],
           |6 | |     },
           |7 | |   }
           |8 | | })
           |  | |__^
           |  |""".stripMargin
      (errResCollValue / "summary").as[String] shouldBe expectedSummaryCollValue
      (errResCollValue / "error" / "code").as[String] shouldBe "constraint_failure"

      val errResIdTerm = queryErr(
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byIdIdx: {
            |      terms: [{ field: "id" }],
            |    },
            |  }
            |})""".stripMargin,
        db
      )

      val expectedSummaryIdTerm =
        """|error: Failed to create Collection.
           |constraint failures:
           |  indexes.byIdIdx.terms[0].field: `id` is not supported as an index term, use the native byId method to look up a document by its id.
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Person",
           |3 | |   indexes: {
           |4 | |     byIdIdx: {
           |5 | |       terms: [{ field: "id" }],
           |6 | |     },
           |7 | |   }
           |8 | | })
           |  | |__^
           |  |""".stripMargin
      (errResIdTerm / "error" / "code").as[String] shouldBe "constraint_failure"
      (errResIdTerm / "summary").as[String] shouldBe expectedSummaryIdTerm
    }
    test("don't allow index term or value paths to be empty") {
      val db = aDatabase.sample
      val errResEmptyTerm = queryErr(
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    foo: {
            |      terms: [{ field: "" }],
            |    },
            |  }
            |})""".stripMargin,
        db
      )
      val expectedSummaryTerm =
        """|error: Failed to create Collection.
           |constraint failures:
           |  indexes.foo.terms[0].field: Value cannot be empty.
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Person",
           |3 | |   indexes: {
           |4 | |     foo: {
           |5 | |       terms: [{ field: "" }],
           |6 | |     },
           |7 | |   }
           |8 | | })
           |  | |__^
           |  |""".stripMargin
      (errResEmptyTerm / "summary").as[String] shouldBe expectedSummaryTerm
      (errResEmptyTerm / "error" / "code").as[String] shouldBe "constraint_failure"

      val errResEmptyValue = queryErr(
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    foo: {
            |      values: [{ field: "" }]
            |    },
            |  }
            |})""".stripMargin,
        db
      )
      val expectedSummaryValue =
        """|error: Failed to create Collection.
           |constraint failures:
           |  indexes.foo.values[0].field: Value cannot be empty.
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Person",
           |3 | |   indexes: {
           |4 | |     foo: {
           |5 | |       values: [{ field: "" }]
           |6 | |     },
           |7 | |   }
           |8 | | })
           |  | |__^
           |  |""".stripMargin
      (errResEmptyValue / "summary").as[String] shouldBe expectedSummaryValue
      (errResEmptyValue / "error" / "code").as[String] shouldBe "constraint_failure"
    }
    test("index term and value paths must be valid identifiers") {
      val db = aDatabase.sample

      // OK cases.
      queryOk(
        s"""|Collection.create({
            |  name: "Animal",
            |  indexes: {
            |    foo: {
            |      terms: [{ field: "a.b.c" }],
            |      values: [{ field: ".a.b.c" }],
            |    },
            |  }
            |})""".stripMargin,
        db
      )

      // Not OK cases: bad use of the dot.
      val errDotRes = queryErr(
        s"""|Collection.create({
            |  name: "Vegetable",
            |  indexes: {
            |    foo: {
            |      terms: [{ field: "..a.b" }, { field: "a..b" }, { field: "a.b." }]
            |    },
            |  }
            |})""".stripMargin,
        db
      )
      val expectedDotSummaryValue =
        """|error: Failed to create Collection.
           |constraint failures:
           |  indexes.foo.terms[0].field: Value `..a.b` is not a valid FQL path expression.
           |  indexes.foo.terms[1].field: Value `a..b` is not a valid FQL path expression.
           |  indexes.foo.terms[2].field: Value `a.b.` is not a valid FQL path expression.
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Vegetable",
           |3 | |   indexes: {
           |4 | |     foo: {
           |5 | |       terms: [{ field: "..a.b" }, { field: "a..b" }, { field: "a.b." }]
           |6 | |     },
           |7 | |   }
           |8 | | })
           |  | |__^
           |  |""".stripMargin
      (errDotRes / "summary").as[String] shouldBe expectedDotSummaryValue
      (errDotRes / "error" / "code").as[String] shouldBe "constraint_failure"

      // Not OK cases: bad identifiers.
      val errIdentRes = queryErr(
        s"""|Collection.create({
            |  name: "Mineral",
            |  indexes: {
            |    foo: {
            |      values: [{ field: "a b.c" }, { field: "#a.b" }]
            |    },
            |  }
            |})""".stripMargin,
        db
      )
      val expectedIdentSummaryValue =
        """|error: Failed to create Collection.
           |constraint failures:
           |  indexes.foo.values[0].field: Value `a b.c` is not a valid FQL path expression.
           |  indexes.foo.values[1].field: Value `#a.b` is not a valid FQL path expression.
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Mineral",
           |3 | |   indexes: {
           |4 | |     foo: {
           |5 | |       values: [{ field: "a b.c" }, { field: "#a.b" }]
           |6 | |     },
           |7 | |   }
           |8 | | })
           |  | |__^
           |  |""".stripMargin
      (errIdentRes / "summary").as[String] shouldBe expectedIdentSummaryValue
      (errIdentRes / "error" / "code").as[String] shouldBe "constraint_failure"
    }
    test("don't allow index terms on ts") {
      val db = aDatabase.sample
      val errRes = queryErr(
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byTs: {
            |      terms: [{ field: "ts" }],
            |    },
            |  }
            |})""".stripMargin,
        db
      )

      val expectedSummary =
        """|error: Failed to create Collection.
           |constraint failures:
           |  indexes.byTs.terms[0].field: `ts` may only be used as an index value, not as an index term.
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Person",
           |3 | |   indexes: {
           |4 | |     byTs: {
           |5 | |       terms: [{ field: "ts" }],
           |6 | |     },
           |7 | |   }
           |8 | | })
           |  | |__^
           |  |""".stripMargin

      (errRes / "error" / "code").as[String] shouldBe "constraint_failure"
      (errRes / "summary").as[String] shouldBe expectedSummary
    }
    test(
      "don't accept index names that conflict with provided collection functions") {
      val db = aDatabase.sample

      val collectionReservedFields = Seq(
        "byId",
        "definition",
        "createAll",
        "createData",
        "all",
        "where",
        "firstWhere",
        "create"
      )

      collectionReservedFields foreach { rn =>
        val errRes = withClue(rn) {
          queryErr(
            s"""|Collection.create({
                |  name: "Person",
                |  indexes: {
                |    $rn: {
                |      terms: [{ field: "name" }, { field: "age" }]
                |    }
                |  }
                |})
                |""".stripMargin,
            db
          )
        }

        val expectedSummary =
          s"""error: Failed to create Collection.
          |constraint failures:
          |  indexes: The name '$rn' is reserved.
          |at *query*:1:18
          |  |
          |1 |   Collection.create({
          |  |  __________________^
          |2 | |   name: "Person",
          |3 | |   indexes: {
          |4 | |     $rn: {
          |5 | |       terms: [{ field: "name" }, { field: "age" }]
          |6 | |     }
          |7 | |   }
          |8 | | })
          |  | |__^
          |  |""".stripMargin

        (errRes / "error" / "code").as[String] shouldEqual "constraint_failure"
        (errRes / "summary").as[String] shouldEqual expectedSummary
      }
    }
    test("don't accept empty index names") {
      val db = aDatabase.sample
      val errRes = queryErr(
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    '': {
            |      terms: [{ field: "name" }, { field: "age" }]
            |    }
            |  }
            |})
            |""".stripMargin,
        db
      )

      val expectedSummary =
        s"""error: Failed to create Collection.
        |constraint failures:
        |  indexes: Invalid identifier.
        |at *query*:1:18
        |  |
        |1 |   Collection.create({
        |  |  __________________^
        |2 | |   name: "Person",
        |3 | |   indexes: {
        |4 | |     '': {
        |5 | |       terms: [{ field: "name" }, { field: "age" }]
        |6 | |     }
        |7 | |   }
        |8 | | })
        |  | |__^
        |  |""".stripMargin

      (errRes / "error" / "code").as[String] shouldEqual "constraint_failure"
      (errRes / "summary").as[String] shouldEqual expectedSummary
    }
    test("don't accept index names with .'s") {
      val db = aDatabase.sample
      val errRes = queryErr(
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    'foo.bar': {
            |      terms: [{ field: "name" }, { field: "age" }]
            |    }
            |  }
            |})
            |""".stripMargin,
        db
      )

      val expectedSummary =
        s"""error: Failed to create Collection.
        |constraint failures:
        |  indexes: Invalid identifier.
        |at *query*:1:18
        |  |
        |1 |   Collection.create({
        |  |  __________________^
        |2 | |   name: "Person",
        |3 | |   indexes: {
        |4 | |     'foo.bar': {
        |5 | |       terms: [{ field: "name" }, { field: "age" }]
        |6 | |     }
        |7 | |   }
        |8 | | })
        |  | |__^
        |  |""".stripMargin

      (errRes / "error" / "code").as[String] shouldEqual "constraint_failure"
      (errRes / "summary").as[String] shouldEqual expectedSummary
    }
    test("backingIndexes are filtered out when pulling a collection from fql 4") {
      val db = aDatabase.sample

      queryOk(
        s"""|Collection.create({
            |  name: "TestCol",
            |  indexes: {
            |    byName: {
            |      terms: [{ field: "name" }]
            |    }
            |  }
            |})""".stripMargin,
        db
      )

      val legacyRes = legacyQuery(
        Get(
          Ref("collections/TestCol")
        ),
        db
      )

      (legacyRes / "name").as[String] shouldBe "TestCol"
      (legacyRes / "backingIndexes").isEmpty shouldBe true
    }
  }
  "alias" - {
    test("collections can be created and used with their alias field") {
      val db = aDatabase.sample

      val res = queryOk(
        s"""|Collection.create({
            |  name: "Book",
            |  alias: "Foo"
            |})
            |""".stripMargin,
        db
      )

      (res / "name").as[String] shouldBe "Book"
      (res / "alias").as[String] shouldBe "Foo"
      (res / "coll").as[String] shouldBe "Collection"
      (res / "id").isEmpty shouldBe true

      val bookTitle = "TestBook"

      val doc = queryOk(
        s"""Foo.create({ title: "$bookTitle" })""",
        db
      )

      (doc / "title").as[String] shouldBe bookTitle
      (doc / "coll").as[String] shouldBe "Book"
      (doc / "id").isEmpty shouldBe false

      val all = (queryOk("Foo.all()", db) / "data")
      all.as[JSArray].length shouldBe 1
      (all / 0 / "id").isEmpty shouldBe false
      (all / 0 / "title").as[String] shouldBe bookTitle
      (all / 0 / "coll").as[String] shouldBe "Book"

      all shouldEqual (queryOk("Book.all()", db) / "data")

      val res1 = queryOk(s"""Foo.where(.title == "$bookTitle")""", db)
      (res1 / "data" / 0 / "title").as[String] shouldBe bookTitle
      val res2 = queryOk(s"""Foo.firstWhere(.title == "$bookTitle")""", db)
      (res2 / "title").as[String] shouldBe bookTitle
    }
    test("update collection schema with alias") {
      val db = aDatabase.sample

      queryOk(
        s"""|Collection.create({
            |  name: "Book",
            |  alias: "Foo"
            |})
            |""".stripMargin,
        db
      )

      val res = queryOk(
        s"""Foo.definition.update({name: "Authors", history_days: 10})""",
        db
      )

      (res / "name").as[String] shouldBe "Authors"
      (res / "alias").as[String] shouldBe "Foo"
      (res / "history_days").as[Long] shouldBe 10L
      (res / "coll").as[String] shouldBe "Collection"
      (res / "id").isEmpty shouldBe true

      val doc = queryOk(
        """Authors.create({name: "William Shakespeare"})""",
        db
      )

      (doc / "name").as[String] shouldBe "William Shakespeare"
      (doc / "coll").as[String] shouldBe "Authors"
      (doc / "id").isEmpty shouldBe false

      val docTwo = queryOk(
        """Foo.create({ name: "Test Author" })""",
        db
      )

      (docTwo / "name").as[String] shouldBe "Test Author"
      (docTwo / "coll").as[String] shouldBe "Authors"
      (docTwo / "id").isEmpty shouldBe false

    }
    test("indexes can be used with aliases") {
      val db = aDatabase.sample
      queryOk(
        s"""|Collection.create({
            |  name: "Book",
            |  alias: "Foo",
            |  indexes: {
            |   byName: { terms: [ { field: "name" } ] }
            |  }
            |})
            |""".stripMargin,
        db
      )

      queryOk(
        s"""|Foo.create({ name: "TestBook", author: "TestAuthor" })
            |""".stripMargin,
        db
      )

      val res = queryOk(
        s"""|Foo.byName("TestBook")
            |""".stripMargin,
        db
      )
      (res / "data").as[JSArray].length shouldBe 1
      (res / "data" / 0 / "name").as[String] shouldBe "TestBook"
      (res / "data" / 0 / "author").as[String] shouldBe "TestAuthor"
    }
  }
  "constraints" - {
    "unique" - {
      test("unique constraints can be provided and are returned back") {
        val db = aDatabase.sample
        val res = queryOk(
          s"""|Collection.create({
              |  name: "Book",
              |  constraints: [
              |    { unique: ["name"] }
              |  ]
              |})
              |""".stripMargin,
          db
        )
        // If the query specifies the constraint in the simple format, the returned
        // constraint definition will be in the simple format.
        val expected = JSObject(
          "constraints" ->
            JSArray(
              JSObject(
                "unique" -> JSArray(JSString("name")),
                "status" -> JSString("active")
              )
            ))
        res should containJSON(expected)

        // Add a constraint using the complex format.
        // Simple and complex format constraints can be mixed.
        // The returned format will be the simple format when possible.
        val res2 = queryOk(
          s"""|Book.definition.update({
              |  constraints: [
              |    { unique: ["name"] },
              |    { unique: [{ field: "name" }, { field: "address.city" }] },
              |    { unique: [{ field: "edition", mva: false }] }
              |  ]
              |})
              |""".stripMargin,
          db
        )
        val expectedUpdated = JSObject(
          "constraints" ->
            JSArray(
              JSObject(
                "unique" -> JSArray(JSString("name")),
                "status" -> JSString("active")
              ),
              JSObject(
                "unique" -> JSArray(JSString("name"), JSString("address.city")),
                "status" -> JSString("active")
              ),
              JSObject(
                "unique" -> JSArray(
                  JSObject("field" -> JSString("edition"), "mva" -> JSFalse)),
                "status" -> JSString("active")
              )
            )
        )
        res2 should containJSON(expectedUpdated)

        val errRes = queryErr(
          s"""|Book.create({
              |  name: "TestBook",
              |  address: { city: "Ohio" }
              |})
              |Book.create({
              |  name: "TestBook",
              |  address: { city: "Ohio" }
              |})
              |""".stripMargin,
          db
        )

        val expectedSummary = """error: Failed unique constraint.
                                |constraint failures:
                                |  name: Failed unique constraint
                                |  name,address.city: Failed unique constraint
                                |at *query*:5:12
                                |  |
                                |5 |   Book.create({
                                |  |  ____________^
                                |6 | |   name: "TestBook",
                                |7 | |   address: { city: "Ohio" }
                                |8 | | })
                                |  | |__^
                                |  |""".stripMargin

        (errRes / "summary").as[String] shouldEqual expectedSummary
        (errRes / "error" / "code").as[String] shouldEqual "constraint_failure"
        (errRes / "error" / "constraint_failures") should matchJSON(
          JSArray(
            JSObject(
              "paths" -> JSArray(
                JSArray(
                  "name"
                )
              ),
              "message" -> "Failed unique constraint"
            ),
            JSObject(
              "paths" -> JSArray(
                JSArray(
                  "name"
                ),
                JSArray(
                  "address",
                  "city"
                )
              ),
              "message" -> "Failed unique constraint"
            )
          )
        )
      }
      test("customers are unable to set the status of a unique constraint") {
        val db = aDatabase.sample
        val res = queryErr(
          s"""|Collection.create({
              |  name: "Book",
              |  constraints: [
              |    { unique: ["name"], status: "Active" }
              |  ]
              |})
              |""".stripMargin,
          db,
          FQL2Params(typecheck = Some(false))
        )
        val expectedSummary =
          """error: Failed to create Collection.
          |constraint failures:
          |  constraints[0].status: Failed to update field because it is readonly
          |at *query*:1:18
          |  |
          |1 |   Collection.create({
          |  |  __________________^
          |2 | |   name: "Book",
          |3 | |   constraints: [
          |4 | |     { unique: ["name"], status: "Active" }
          |5 | |   ]
          |6 | | })
          |  | |__^
          |  |""".stripMargin
        (res / "summary").as[String] shouldBe expectedSummary
        (res / "error" / "code").as[String] shouldBe "constraint_failure"
      }

      // TODO: It'd be nice for this also to return an "invalid field provided"
      //            error for "uniqu". It doesn't currently because constraints
      //            are optional.
      test("fails for invalid unique constraint definition") {
        val db = aDatabase.sample
        val res = queryErr(
          s"""|Collection.create({
              |  name: "Book",
              |  constraints: [
              |    { uniqu: ["name"] }
              |  ]
              |})
              |""".stripMargin,
          db
        )
        val expectedSummary =
          """|error: Type `{ name: "Book", constraints: [{ uniqu: ["name"] }] }` is not a subtype of `{ name: String, alias: String | Null, computed_fields: { *: { body: String, signature: String | Null } } | Null, fields: { *: { signature: String, default: String | Null } } | Null, migrations: Array<{ backfill: { field: String, value: String } } | { drop: { field: String } } | { split: { field: String, to: Array<String> } } | { move: { field: String, to: String } } | { add: { field: String } } | { move_conflicts: { into: String } } | { move_wildcard: { into: String } } | { add_wildcard: {} }> | Null, wildcard: String | Null, history_days: Number | Null, ttl_days: Number | Null, document_ttls: Boolean | Null, indexes: { *: { terms: Array<{ field: String, mva: Boolean | Null }> | Null, values: Array<{ field: String, mva: Boolean | Null, order: "asc" | "desc" | Null }> | Null, queryable: Boolean | Null } } | Null, constraints: Array<{ unique: Array<String> | Array<{ field: String, mva: Boolean | Null }> } | { check: { name: String, body: String } }> | Null, data: { *: Any } | Null }`
             |at *query*:1:19
             |  |
             |1 |   Collection.create({
             |  |  ___________________^
             |2 | |   name: "Book",
             |3 | |   constraints: [
             |4 | |     { uniqu: ["name"] }
             |5 | |   ]
             |6 | | })
             |  | |_^
             |  |
             |cause: Type `[{ uniqu: ["name"] }]` is not a subtype of `Null`
             |at *query*:3:16
             |  |
             |3 |     constraints: [
             |  |  ________________^
             |4 | |     { uniqu: ["name"] }
             |5 | |   ]
             |  | |___^
             |  |
             |cause: Type `{ uniqu: ["name"] }` contains extra field `uniqu`
             |at *query*:4:5
             |  |
             |4 |     { uniqu: ["name"] }
             |  |     ^^^^^^^^^^^^^^^^^^^
             |  |
             |cause: Type `{ uniqu: ["name"] }` does not have field `check`
             |at *query*:4:5
             |  |
             |4 |     { uniqu: ["name"] }
             |  |     ^^^^^^^^^^^^^^^^^^^
             |  |
             |cause: Type `{ uniqu: ["name"] }` does not have field `unique`
             |at *query*:4:5
             |  |
             |4 |     { uniqu: ["name"] }
             |  |     ^^^^^^^^^^^^^^^^^^^
             |  |""".stripMargin
        (res / "error" / "code").as[String] shouldEqual "invalid_query"
        (res / "summary").as[String] shouldEqual expectedSummary
      }

      test("unique constraint work with the MVA option") {
        val db = aDatabase.sample
        queryOk(
          s"""|Collection.create({
              |  name: "Foo",
              |  constraints: [
              |    { unique: [{ field: "mva_on", mva: true }] },
              |    { unique: [{ field: "mva_off", mva: false }] }
              |  ]
              |})
              |""".stripMargin,
          db
        )

        def failsUniqueConstraint(fql: String) = {
          val res = queryErr(fql, db)
          (res / "error" / "code").as[String] shouldEqual "constraint_failure"
          (res / "error" / "message")
            .as[String] shouldEqual "Failed unique constraint."
        }

        queryOk("""Foo.create({ mva_on: [0, 1], mva_off: [0, 1] })""", db)

        // Unique+MVA terms take a slot for each array element.
        failsUniqueConstraint("""Foo.create({ mva_on: 0 })""")
        failsUniqueConstraint("""Foo.create({ mva_on: 1 })""")
        failsUniqueConstraint("""Foo.create({ mva_on: [1, 0] })""")
        failsUniqueConstraint("""Foo.create({ mva_on: [0, 1] })""")

        // Unique+non-MVA terms take the slot for the array value only.
        queryOk("""Foo.create({ mva_off: 0 })""", db)
        queryOk("""Foo.create({ mva_off: 1 })""", db)
        queryOk("""Foo.create({ mva_off: [1, 0] })""", db)
        failsUniqueConstraint("""Foo.create({ mva_off: [0, 1] })""")
      }
    }
  }
  "refs" - {
    def setup() = {
      val db = aDatabase.sample

      queryOk(
        """|Collection.create({ name: 'Foo' })
           |Collection.create({ name: 'Bar' })
           |""".stripMargin,
        db)

      val foo = queryOk("Foo.create({ bar: Bar.create({}) })", db)
      val bar = queryOk(s"Foo.byId(${foo / "id"})!.bar", db)
      queryOk("Bar.definition.delete()", db)

      (db, foo, bar)
    }

    "to docs in a deleted collection" - {

      test("materialize correctly") {
        val (db, foo, bar) = setup()

        val foo2 =
          queryOk(
            s"Foo.byId(${foo / "id"})",
            db,
            FQL2Params(format = Some("tagged")))

        val bar2 = foo2 / "@doc" / "bar"

        (bar2 / "@ref" / "id").as[String] shouldEqual (bar / "id").as[String]
        // FIXME: "this should become Collection.Deleted, since it has a
        // parent."
        (bar2 / "@ref" / "coll" / "@mod").as[String] shouldEqual "Deleted"
      }

      test("turn into null docs") {
        val (db, foo, bar) = setup()

        val bar2 = queryOk(
          s"Foo.byId(${foo / "id"})!.bar",
          db,
          FQL2Params(format = Some("tagged")))

        (bar2 / "@ref" / "id").as[String] shouldEqual (bar / "id").as[String]
        // FIXME: "this should become Collection.Deleted, since it has a
        // parent."
        (bar2 / "@ref" / "coll" / "@mod").as[String] shouldEqual "Deleted"
        (bar2 / "@ref" / "exists").as[Boolean] shouldEqual false
        (bar2 / "@ref" / "cause").as[String] shouldEqual "collection deleted"

        queryOk(s"Foo.byId(${foo / "id"})!.bar.exists()", db)
          .as[Boolean] shouldEqual false
        queryOk(s"Foo.byId(${foo / "id"})!.bar == null", db)
          .as[Boolean] shouldEqual true

        // FIXME: "this should become Collection.Deleted, since it has a
        // parent."
        queryOk(s"Foo.byId(${foo / "id"})!.bar.coll", db)
          .as[String] shouldEqual "Deleted"

        queryOk(s"Foo.byId(${foo / "id"})!.bar.id", db)
          .as[String] shouldEqual (bar / "id").as[String]
      }
    }
  }

  "check constraints" - {

    test("run only once") {
      val db = aDatabase.sample

      queryOk(
        """|Collection.create({
            |  name: 'Once',
            |  constraints: [
            |    { check: {
            |      name: 'once',
            |      body: '_ => { log(0); true }'
            |    } }
            |  ]
            |})""".stripMargin,
        db
      )

      // Previously, the log would fire twice, so the summary would be
      // ```
      // info at *check_constraint:once*:1: 0
      //
      // info at *check_constraint:once*:1: 0
      // ```
      (queryRaw("Once.create({})", db).json / "summary")
        .as[String] shouldEqual "info at *check_constraint:once*:1: 0"
    }
  }

  test("v4 indexes can have data field set") {
    val db = aDatabase.sample

    queryOk(
      "Collection.create({ name: 'MyColl' })",
      db
    )

    queryOk(
      """|Set.sequence(0, 256).forEach((i) => {
         |  MyColl.create({ a: 3 })
         |})
         |""".stripMargin,
      db
    )

    api.query(
      CreateIndex(
        MkObject(
          "name" -> "MyIndex",
          "source" -> ClassRef("MyColl"),
          "data" -> MkObject("foo" -> 5),
          "terms" -> JSArray(MkObject("field" -> JSArray("data", "a"))))),
      db.key
    ) should respond(Created)

    eventually(timeout(1.minute)) {
      api.query(Paginate(Match(IndexRef("MyIndex"), 3)), db.key) should respond(OK)
    }
  }
}
