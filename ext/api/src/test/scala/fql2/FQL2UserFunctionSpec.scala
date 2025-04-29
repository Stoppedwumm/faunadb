package fauna.api.test

import fauna.codex.json._
import fauna.prop.api.Database
import fauna.prop.api.Query27Helpers.{ Get, Ref }

class FQL2UserFunctionSpec extends FQL2APISpec {
  def createUDF(db: Database) = queryOk(
    s"""|Function.create({
        |  name: "AddTwo",
        |  body: "x => x + 2"
        |})
        |""".stripMargin,
    db)

  "Basic CRUD" - {
    test("create a function") {
      val db = aDatabase.sample
      val res = createUDF(db)

      (res / "name").as[String] shouldEqual "AddTwo"
      (res / "body").as[String] shouldEqual "x => x + 2"
      (res / "id").isEmpty shouldBe true
    }

    test("retrieve a function by name") {
      val db = aDatabase.sample
      createUDF(db)

      val res = queryOk(s"""Function.byName("AddTwo")""", db)

      (res / "name").as[String] shouldEqual "AddTwo"
      (res / "body").as[String] shouldEqual "x => x + 2"
      (res / "id").isEmpty shouldBe true
    }

    test("definition field") {
      val db = aDatabase.sample
      createUDF(db)

      queryOk(
        s"""AddTwo.definition == Function.byName("AddTwo")""",
        db,
        // FIXME: remove when udf type has definition field
        FQL2Params(typecheck = Some(false))
      ).as[Boolean] shouldEqual true
    }

    test("update a function") {
      val db = aDatabase.sample
      createUDF(db)

      queryOk(
        s"""|let udf = Function.byName("AddTwo")!
            |udf.update({ body: "x => x + 'two'" })""".stripMargin,
        db)

      val res = queryOk(s"""Function.byName("AddTwo")""", db)

      (res / "name").as[String] shouldEqual "AddTwo"
      (res / "body").as[String] shouldEqual "x => x + 'two'"
      (res / "id").isEmpty shouldBe true
    }

    test("delete a function") {
      val db = aDatabase.sample
      createUDF(db)

      val del = queryOk(
        s"""|let udf = Function.byName("AddTwo")!
            |udf.delete()""".stripMargin,
        db)

      // The result of delete() is null
      del shouldEqual JSNull
    }

    test("delete a function (tagged format)") {
      val db = aDatabase.sample
      createUDF(db)

      val del = queryOk(
        """|let udf = Function.byName("AddTwo")!
           |udf.delete()""".stripMargin,
        db,
        FQL2Params(format = Some("tagged")))

      // The result of delete() is special, and shows that the function just got
      // deleted. The result of byName() is just that the function was not found.
      (del / "@ref" / "name") shouldEqual JSString("AddTwo")
      (del / "@ref" / "coll") shouldEqual JSObject("@mod" -> "Function")
      (del / "@ref" / "exists") shouldEqual JSBoolean(false)
      (del / "@ref" / "cause") shouldEqual JSString("deleted")

      val res =
        queryOk("Function.byName('AddTwo')", db, FQL2Params(format = Some("tagged")))

      (res / "@ref" / "name") shouldEqual JSString("AddTwo")
      (res / "@ref" / "coll") shouldEqual JSObject("@mod" -> "Function")
      (res / "@ref" / "exists") shouldEqual JSBoolean(false)
      (res / "@ref" / "cause") shouldEqual JSString("not found")
    }

    test("has a data field for custom object metadata") {
      val db = aDatabase.sample
      createUDF(db)

      queryOk(
        s"""|let udf = Function.byName("AddTwo")!
            |udf.update({ data: { foo: "bar" }})""".stripMargin,
        db)

      val res = queryOk(s"""Function.byName("AddTwo")""", db)

      (res / "name").as[String] shouldEqual "AddTwo"
      (res / "body").as[String] shouldEqual "x => x + 2"
      (res / "data" / "foo").as[String] shouldEqual "bar"
      (res / "id").isEmpty shouldBe true
    }

    test("data field only accepts objects") {
      val db = aDatabase.sample
      createUDF(db)

      val errRes = queryErr(
        s"""|Function.create({
            |  name: "AddTwo",
            |  body: "x => x + 2",
            |  data: "foobar"
            |})
            |""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      )

      (errRes / "error" / "code").as[String] shouldEqual "constraint_failure"
      (errRes / "error" / "message")
        .as[String] shouldEqual "Failed to create Function."
    }

    test("data field only accepts objects (typechecked)") {
      val db = aDatabase.sample
      createUDF(db)

      val errRes = queryErr(
        s"""|Function.create({
            |  name: "AddTwo",
            |  body: "x => x + 2",
            |  data: "foobar"
            |})
            |""".stripMargin,
        db
      )

      (errRes / "error" / "code").as[String] shouldEqual "invalid_query"
      (errRes / "error" / "message")
        .as[String] shouldEqual "The query failed 1 validation check"
      (errRes / "summary").as[String] shouldEqual
        """|error: Type `String` is not a subtype of `Null | { *: Any }`
           |at *query*:4:9
           |  |
           |4 |   data: "foobar"
           |  |         ^^^^^^^^
           |  |""".stripMargin
    }

    test("closures produce a readable error") {
      val db = aDatabase.sample
      createUDF(db)

      val errRes = queryErr(
        s"""|Function.create({
            |  name: "foo",
            |  body: (x) => x + 2,
            |})
            |""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      )

      (errRes / "error" / "code").as[String] shouldEqual "invalid_type"
      (errRes / "error" / "message")
        .as[String] shouldEqual "Anonymous functions cannot be stored. Use a string of FQL instead."
    }

    test("closures produce a readable error (typechecked)") {
      val db = aDatabase.sample
      createUDF(db)

      val errRes = queryErr(
        s"""|Function.create({
            |  name: "foo",
            |  body: (x) => x + 2,
            |})
            |""".stripMargin,
        db
      )

      (errRes / "error" / "code").as[String] shouldEqual "invalid_query"
      (errRes / "error" / "message")
        .as[String] shouldEqual "The query failed 1 validation check"

      (errRes / "summary").as[String] shouldEqual
        """|error: Type `Number => Number` is not a subtype of `String`
           |at *query*:3:9
           |  |
           |3 |   body: (x) => x + 2,
           |  |         ^^^^^^^^^^^^
           |  |""".stripMargin
    }
  }

  "Calling a UDF" - {
    test("call a function") {
      val db = aDatabase.sample
      createUDF(db)

      queryOk(s"""AddTwo(10)""", db).as[Long] shouldEqual 12L

      queryOk(
        s"""|let f = AddTwo
            |f(10)""".stripMargin,
        db
      ).as[Long] shouldEqual 12L
    }

    test("function defined by a short lambda") {
      // FIXME: Lambdas should allow a short lambda or a normal lambda in a
      // one-element tuple.
      pendingUntilFixed {
        val db = aDatabase.sample
        val res = queryOk(
          s"""|Function.create({
              |  name: "Foo",
              |  body: "(.foo)"
              |})
              |""".stripMargin,
          db)

        (res / "name").as[String] shouldEqual "Foo"
        (res / "body").as[String] shouldEqual ".foo"

        queryOk(s"""Foo({ foo: 'bar' })""", db).as[String] shouldEqual "bar"
      }
    }

    test("invalid arity") {
      val db = aDatabase.sample
      createUDF(db)

      val res = queryErr(
        s"""AddTwo(10, 20)""",
        db
      )

      (res / "error" / "code").as[String] shouldBe "invalid_function_invocation"
      (res / "error" / "message")
        .as[String] shouldBe "callee function expects exactly 1 argument, but 2 were provided."
    }

    test("render UDF bodies on errors") {
      val db = aDatabase.sample

      queryOk(
        s"""|Function.create({
            |  name: "Test0",
            |  body: "x => x.someFunc()"
            |})
            |""".stripMargin,
        db)

      queryOk(
        s"""|Function.create({
            |  name: "Test1",
            |  body: "x => Test0(x)"
            |})
            |""".stripMargin,
        db)

      val res = queryErr(
        s"""Test1("foo")""",
        db
      )

      (res / "error" / "code").as[String] shouldBe "invalid_function_invocation"
      (res / "error" / "message")
        .as[String] shouldBe "The function `someFunc` does not exist on `String`"
      (res / "summary")
        .as[String] shouldBe """|error: The function `someFunc` does not exist on `String`
                                |at *udf:Test0*:1:8
                                |  |
                                |1 | x => x.someFunc()
                                |  |        ^^^^^^^^
                                |  |
                                |at *udf:Test1*:1:11
                                |  |
                                |1 | x => Test0(x)
                                |  |           ^^^
                                |  |
                                |at *query*:1:6
                                |  |
                                |1 | Test1("foo")
                                |  |      ^^^^^^^
                                |  |""".stripMargin
    }

    test("omit stack frame if doesn't have permissions") {
      val db = aDatabase.sample

      queryOk(
        s"""|Function.create({
            |  name: "Test0",
            |  body: "x => x.someFunc()"
            |})
            |""".stripMargin,
        db)

      queryOk(
        s"""|Function.create({
            |  name: "Test1",
            |  body: "x => Test0(x)"
            |})
            |""".stripMargin,
        db)

      val key = queryOk(
        """|Role.create({
           |  name: "aRole",
           |  privileges: [{
           |    resource: "Test0",
           |    actions: {
           |      call: true
           |    }
           |  }, {
           |    resource: "Test1",
           |    actions: {
           |      call: true
           |    }
           |  }]
           |})
           |
           |Key.create({
           |  role: "aRole"
           |})""".stripMargin,
        db
      )

      val res = queryErr(
        s"""Test1("foo")""",
        (key / "secret").as[String]
      )

      (res / "error" / "code").as[String] shouldBe "invalid_function_invocation"
      (res / "error" / "message")
        .as[String] shouldBe "The function `someFunc` does not exist on `String`"
      (res / "summary")
        .as[String] shouldBe
        """|error: The function `someFunc` does not exist on `String`
           |at *query*:1:6
           |  |
           |1 | Test1("foo")
           |  |      ^^^^^^^
           |  |""".stripMargin

    }
  }

  "v10 source validation" - {
    test("rejects invalid FQL source code on create") {
      val db = aTypecheckedDatabase.sample

      val res = queryErr(
        s"""|Function.create({
            |  name: "Invalid",
            |  body: "&sfej23 ?rtu9"
            |})
            |""".stripMargin,
        db)
      (res / "error" / "code").as[String] shouldBe "constraint_failure"
      // TODO: (res / "message").as[String] shouldBe "TODO"
    }

    test("rejects function body that references undefined global variable") {
      val db = aTypecheckedDatabase.sample

      val res = queryErr(
        s"""|Function.create({
            |  name: "NoCollection",
            |  body: "x => MyCollection.all().take(x)"
            |})
            |""".stripMargin,
        db
      )

      (res / "error" / "code").as[String] shouldEqual "invalid_schema"
      // TODO: (res / "message").as[String] shouldBe "TODO"
    }

    test("rejects function body that is not a lambda") {
      val db = aTypecheckedDatabase.sample

      val res = queryErr(
        s"""|Function.create({
            |  name: "NotALambda",
            |  body: "'foo'.length"
            |})
            |""".stripMargin,
        db)

      (res / "error" / "code").as[String] shouldBe "constraint_failure"
      // TODO: (res / "message").as[String] shouldBe "TODO"
    }

    test("validates source on update") {
      val db = aTypecheckedDatabase.sample
      createUDF(db)

      val res = queryErr(
        s"""|Function.byName("AddTwo")!.update({
            |  body: "&(#Jdfjsdfouh3tJHOEF:"
            |})
            |""".stripMargin,
        db)

      (res / "error" / "code").as[String] shouldBe "constraint_failure"
      // TODO: (res / "message").as[String] shouldBe "TODO"
    }

    test("deleting a collection that a UDF refers to") {
      val db = aTypecheckedDatabase.sample
      queryOk("""Collection.create({ name: "MyCollection" })""", db)
      queryOk(
        s"""|Function.create({
              |  name: "MyFunction",
              |  body: "x => MyCollection.all().take(x)"
              |})
              |""".stripMargin,
        db
      )
      val res = queryErr("""Collection.byName("MyCollection")!.delete()""", db)

      (res / "error" / "code").as[String] shouldBe "invalid_schema"
      // TODO: (res / "message").as[String] shouldBe "TODO"
    }

    test("validation errors") {
      val db = aDatabase.sample

      // Invalid body, this should spit out an error with two spans. One for the
      // outside query, and one within the body.
      val res0 = queryErr(
        s"""|Function.create({
            |  name: "MyFunction",
            |  body: "let a"
            |})
            |""".stripMargin,
        db
      )

      val message = "Failed to create Function."

      (res0 / "error" / "code").as[String] shouldBe "constraint_failure"
      (res0 / "error" / "message").as[String] shouldBe message
      // The `|` in $message gets screwed up by `stripMargin`, so we need
      // another character on the left here.
      (res0 / "summary").as[String] shouldBe
        s"""|error: $message
            |constraint failures:
            |  body: Unable to parse FQL source code.
            |      error: Expected `:` or `=`
            |      at *body*:1:6
            |        |
            |      1 | let a
            |        |      ^
            |        |
            |at *query*:1:16
            |  |
            |1 |   Function.create({
            |  |  ________________^
            |2 | |   name: "MyFunction",
            |3 | |   body: "let a"
            |4 | | })
            |  | |__^
            |  |""".stripMargin
    }

    test("validation for missing lambda") {
      val db = aDatabase.sample

      // Invalid body, this should spit out an error with two spans. One for the
      // outside query, and one within the body.
      val res0 = queryErr(
        s"""|Function.create({
            |  name: "MyFunction",
            |  body: <<-END
            |    let y = 2 + 3
            |    (x) => {
            |      x + y
            |    }
            |  END
            |})
            |""".stripMargin,
        db
      )

      val message = "Failed to create Function."

      (res0 / "error" / "code").as[String] shouldBe "constraint_failure"
      (res0 / "error" / "message").as[String] shouldBe message
      // The `|` in $message gets screwed up by `stripMargin`, so we need
      // another character on the left here.
      (res0 / "summary").as[String] shouldBe
        s"""|error: $message
            |constraint failures:
            |  body: Unable to parse FQL source code.
            |      error: Body must be a lambda expression.
            |      at *body*:1:1
            |        |
            |      1 |   let y = 2 + 3
            |        |  _^
            |      2 | | (x) => {
            |      3 | |   x + y
            |      4 | | }
            |        | |__^
            |        |
            |at *query*:1:16
            |  |
            |1 |   Function.create({
            |  |  ________________^
            |2 | |   name: "MyFunction",
            |3 | |   body: <<-END
            |4 | |     let y = 2 + 3
            |5 | |     (x) => {
            |6 | |       x + y
            |7 | |     }
            |8 | |   END
            |9 | | })
            |  | |__^
            |  |""".stripMargin
    }

    test("not a lambda suggestion") {
      val db = aDatabase.sample

      val res0 = queryErr(
        s"""|Function.create({
            |  name: "MyFunction",
            |  body: "34"
            |})
            |""".stripMargin,
        db
      )

      val message = "Failed to create Function."

      (res0 / "error" / "code").as[String] shouldBe "constraint_failure"
      (res0 / "error" / "message").as[String] shouldBe message
      (res0 / "summary").as[String] shouldBe
        s"""|error: $message
            |constraint failures:
            |  body: Unable to parse FQL source code.
            |      error: Body must be a lambda expression.
            |      at *body*:1:1
            |        |
            |      1 | 34
            |        | ^^ Expression is not a lambda.
            |        |
            |      hint: Declare a lambda.
            |        |
            |      1 | () => 34
            |        | ++++++
            |        |
            |at *query*:1:16
            |  |
            |1 |   Function.create({
            |  |  ________________^
            |2 | |   name: "MyFunction",
            |3 | |   body: "34"
            |4 | | })
            |  | |__^
            |  |""".stripMargin
    }

    test("cannot create UDF with trailing semicolon") {
      val db = aDatabase.sample

      val res0 = queryErr(
        s"""|Function.create({
            |  name: "MyFunction",
            |  body: "() => {};"
            |})
            |""".stripMargin,
        db
      )

      val message = "Failed to create Function."

      (res0 / "error" / "code").as[String] shouldBe "constraint_failure"
      (res0 / "error" / "message").as[String] shouldBe message
      (res0 / "summary").as[String] shouldBe
        s"""|error: $message
            |constraint failures:
            |  body: Unable to parse FQL source code.
            |      error: Expected end-of-input
            |      at *body*:1:9
            |        |
            |      1 | () => {};
            |        |         ^
            |        |
            |at *query*:1:16
            |  |
            |1 |   Function.create({
            |  |  ________________^
            |2 | |   name: "MyFunction",
            |3 | |   body: "() => {};"
            |4 | | })
            |  | |__^
            |  |""".stripMargin
    }

    test("can use global variables in UDF body") {
      val db = aDatabase.sample
      queryOk(
        s"""|Collection.create({
            |  name: "Dinosaur"
            |})
            |Function.create({
            |  name: "getCollection",
            |  body: "name => Collection.byName(name)"
            |})
            |""".stripMargin,
        db
      )

      val res = queryOk("""getCollection("Dinosaur")""", db)

      (res / "name").as[String] shouldBe "Dinosaur"
      (res / "coll").as[String] shouldBe "Collection"
    }

    test("can create UDF with multiline body") {
      val db = aDatabase.sample
      val res = queryOk(
        s"""|Function.create({
            |  name: "greeting",
            |  body: <<+BODY
            |    name => {
            |      let greet = "Hello, " + name
            |
            |      greet + "."
            |    }
            |  BODY
            |})
            |""".stripMargin,
        db
      )

      (res / "name").as[String] shouldBe "greeting"
      (res / "body").as[String] shouldBe
        """|name => {
           |  let greet = "Hello, " + name
           |
           |  greet + "."
           |}
           |""".stripMargin

      queryOk("""greeting("John Doe")""", db).as[String] shouldBe "Hello, John Doe."
    }

    test("can create UDF with non interpolated string bodies") {
      val db = aDatabase.sample
      val res = queryOk(
        s"""|Function.create({
            |  name: "greeting",
            |  body: <<-BODY
            |    name => {
            |      let greet = "Hello, #{name}."
            |
            |      greet
            |    }
            |  BODY
            |})
            |""".stripMargin,
        db
      )

      (res / "name").as[String] shouldBe "greeting"
      (res / "body").as[String] shouldBe
        """|name => {
           |  let greet = "Hello, #{name}."
           |
           |  greet
           |}
           |""".stripMargin

      queryOk("""greeting("John Doe")""", db).as[String] shouldBe "Hello, John Doe."
    }
  }

  "UDF Permissions" - {
    test("FQL4 calling FQLX") {
      val db = aDatabase.sample

      queryOk(
        s"""|Function.create({
            |  name: "MyCreateCollection",
            |  body: <<-BODY
            |    (collName) => Collection.create({ name: collName })
            |  BODY
            |})
            |""".stripMargin,
        db
      )

      queryOk("MyCreateCollection(\"MyCollection1\")", db)

      val res = queryOk("Collection.all()", db)
      (res / "data").as[Seq[JSObject]].length shouldBe 1

      // Change the role of `MyCreateCollection` to be readonly.
      client.api.query(
        JSObject(
          "update" -> JSObject(
            "function" -> "MyCreateCollection"
          ),
          "params" -> JSObject(
            "object" -> JSObject(
              "role" -> "server-readonly"
            )
          )
        ),
        db.adminKey
      ) should respond(200, 201)

      // Now, this should fail, as it will use the readonly role.
      client.api.query(
        JSObject(
          "call" -> "MyCreateCollection",
          "arguments" -> JSArray("MyCollection2")
        ),
        db.adminKey
      ) should respond(400)

      val res2 = queryOk("Collection.all()", db)
      (res2 / "data").as[Seq[JSObject]].length shouldBe 1
    }

    test("FQLX calling FQL4") {
      val db = aDatabase.sample

      client.api.query(
        JSObject(
          "create_function" -> JSObject(
            "object" -> JSObject(
              "name" -> "MyCreateCollection",
              "body" -> JSObject(
                "@query" -> JSObject(
                  "lambda" -> JSArray("x"),
                  "expr" -> JSObject(
                    "create_collection" -> JSObject(
                      "object" -> JSObject(
                        "name" -> JSObject(
                          "var" -> "x"
                        )
                      )
                    )
                  )
                )
              )
            )
          )
        ),
        db.adminKey
      ) should respond(200, 201)

      val coll = queryOk("MyCreateCollection(\"MyCollection1\")", db)

      (coll / "name").as[String] shouldEqual "MyCollection1"
      (coll / "coll").as[String] shouldEqual "Collection"

      val res = queryOk("Collection.all()", db)
      (res / "data").as[Seq[JSObject]].length shouldBe 1

      // Change the role of `MyCreateCollection` to be readonly.
      client.api.query(
        JSObject(
          "update" -> JSObject(
            "function" -> "MyCreateCollection"
          ),
          "params" -> JSObject(
            "object" -> JSObject(
              "role" -> "server-readonly"
            )
          )
        ),
        db.adminKey
      ) should respond(200, 201)

      // Now, this should fail, as it will use the readonly role.
      queryErr("MyCreateCollection(\"MyCollection2\")", db)

      // TODO: Validate errors from the above call

      val res2 = queryOk("Collection.all()", db)
      (res2 / "data").as[Seq[JSObject]].length shouldBe 1
    }
  }

  "FQL4 UDF" - {
    test("call a function") {
      val db = aDatabase.sample

      client.api.query(
        JSObject(
          "create_function" -> JSObject(
            "object" -> JSObject(
              "name" -> "legacy_udf",
              "body" -> JSObject(
                "@query" -> JSObject("lambda" -> JSArray.empty, "expr" -> 10))
            )
          )
        ),
        db.adminKey
      ) should respond(200, 201)

      queryOk(s"""legacy_udf()""", db).as[Long] shouldBe 10L
    }

    test("correctly adapts documents") {
      val db = aDatabase.sample
      import fauna.prop.api.Query27Helpers._

      queryOk("Collection.create({ name: 'Book' })", db)
      client.api.query(
        CreateFunction(
          MkObject(
            "name" -> "mkBook",
            "body" -> QueryF(
              Lambda("n" -> CreateF(
                ClassRef("Book"),
                MkObject("data" -> MkObject("name" -> Var("n"))))))
          )),
        db.adminKey
      ) should respond(200, 201)

      val book = queryOk("mkBook('foo')", db).as[JSObject]
      (book / "name") shouldEqual JSString("foo")
      (book / "id").asOpt[JSValue].nonEmpty shouldEqual true
      (book / "ts").asOpt[JSValue].nonEmpty shouldEqual true
    }

    test("correctly adapts partials") {
      val db = aDatabase.sample
      import fauna.prop.api.Query27Helpers._

      queryOk(
        """|Collection.create(
           |  { name: 'Author',
           |    indexes: {
           |      byName: { values: [{ field: 'name' }] }
           |    }
           |  }
           |)""".stripMargin,
        db
      )
      queryOk("Collection.create({ name: 'Book' })", db)
      client.api.query(
        CreateFunction(
          MkObject(
            "name" -> "mkBook",
            "body" -> QueryF(
              Lambda(
                JSArray("name", "author_info") -> CreateF(
                  ClassRef("Book"),
                  MkObject("data" -> MkObject(
                    "name" -> Var("name"),
                    "author_info" -> Var("author_info"))))))
          )),
        db.adminKey
      ) should respond(200, 201)

      queryOk("Author.create({ name: 'Jane Austen', dob: '1775-12-16' })", db)
      val book = queryOk(
        "mkBook('Pride and Prejudice', Author.byName().map(.data).first()!)",
        db
      ).as[JSObject]
      (book / "name") shouldEqual JSString("Pride and Prejudice")
      (book / "author_info" / "name") shouldEqual JSString("Jane Austen")
      (book / "author_info" / "dob") shouldEqual JSString("1775-12-16")
      (book / "id").asOpt[JSValue].nonEmpty shouldEqual true
      (book / "ts").asOpt[JSValue].nonEmpty shouldEqual true
    }

    test("adapts arity") {
      val db = aDatabase.sample

      client.api.query(
        JSObject(
          "create_function" -> JSObject(
            "object" -> JSObject(
              "name" -> "udf1",
              "body" -> JSObject("@query" -> JSObject(
                "lambda" -> JSArray("x"),
                "expr" -> JSObject("var" -> "x")))))),
        db.adminKey
      ) should respond(200, 201)

      client.api.query(
        JSObject(
          "create_function" -> JSObject(
            "object" -> JSObject(
              "name" -> "udf2",
              "body" -> JSObject("@query" -> JSObject(
                "lambda" -> "x",
                "expr" -> JSObject("var" -> "x")))))),
        db.adminKey
      ) should respond(200, 201)

      client.api.query(
        JSObject(
          "create_function" -> JSObject("object" -> JSObject(
            "name" -> "udf3",
            "body" -> JSObject("@query" -> JSObject(
              "lambda" -> JSArray("x", "y"),
              "expr" -> JSArray(JSObject("var" -> "x"), JSObject("var" -> "y"))))))),
        db.adminKey
      ) should respond(200, 201)

      queryOk(s"udf1(1)", db) shouldEqual JSLong(1)
      queryOk(s"udf1([1, 2])", db) shouldEqual JSArray(1, 2)

      queryOk(s"udf2(1)", db) shouldEqual JSLong(1)
      queryOk(s"udf2([1, 2])", db) shouldEqual JSArray(1, 2)

      queryOk(s"udf3(1, 2)", db) shouldEqual JSArray(1, 2)
    }

    test("invalid arity") {
      val db = aDatabase.sample

      client.api.query(
        JSObject(
          "create_function" -> JSObject(
            "object" -> JSObject(
              "name" -> "empty_params",
              "body" -> JSObject(
                "@query" -> JSObject("lambda" -> JSArray.empty, "expr" -> 10))
            )
          )
        ),
        db.adminKey
      ) should respond(200, 201)

      client.api.query(
        JSObject(
          "create_function" -> JSObject(
            "object" -> JSObject(
              "name" -> "one_argument_array",
              "body" -> JSObject(
                "@query" -> JSObject("lambda" -> JSArray("x"), "expr" -> 10))
            )
          )
        ),
        db.adminKey
      ) should respond(200, 201)

      client.api.query(
        JSObject(
          "create_function" -> JSObject(
            "object" -> JSObject(
              "name" -> "one_argument_string",
              "body" -> JSObject("@query" -> JSObject("lambda" -> "x", "expr" -> 10))
            )
          )
        ),
        db.adminKey
      ) should respond(200, 201)

      val res0 = queryErr(s"""empty_params(10)""", db)
      (res0 / "error" / "code").as[String] shouldBe "invalid_function_invocation"
      (res0 / "error" / "message").as[
        String] shouldBe "callee function expects no arguments, but 1 was provided."
      (res0 / "summary").as[String] shouldBe
        """|error: callee function expects no arguments, but 1 was provided.
           |at *query*:1:13
           |  |
           |1 | empty_params(10)
           |  |             ^^^^
           |  |""".stripMargin

      val res1 = queryErr(s"""one_argument_array(10, 20)""", db)
      (res1 / "error" / "code").as[String] shouldBe "invalid_function_invocation"
      (res1 / "error" / "message")
        .as[String] shouldBe "callee function expects exactly 1 argument, but 2 were provided."
      (res1 / "summary").as[String] shouldBe
        """|error: callee function expects exactly 1 argument, but 2 were provided.
           |at *query*:1:19
           |  |
           |1 | one_argument_array(10, 20)
           |  |                   ^^^^^^^^
           |  |""".stripMargin

      val res2 = queryErr(s"""one_argument_string(10, 20)""", db)
      (res2 / "error" / "code").as[String] shouldBe "invalid_function_invocation"
      (res2 / "error" / "message")
        .as[String] shouldBe "callee function expects exactly 1 argument, but 2 were provided."
      (res2 / "summary").as[String] shouldBe
        """|error: callee function expects exactly 1 argument, but 2 were provided.
           |at *query*:1:20
           |  |
           |1 | one_argument_string(10, 20)
           |  |                    ^^^^^^^^
           |  |""".stripMargin
    }

    test("errors") {
      val db = aDatabase.sample

      client.api.query(
        JSObject(
          "create_function" -> JSObject(
            "object" -> JSObject(
              "name" -> "legacy_udf",
              "body" -> JSObject(
                "@query" -> JSObject(
                  "lambda" -> JSArray("arg"),
                  "expr" -> JSObject("abort" -> "some message")
                )
              )
            )
          )
        ),
        db.adminKey
      ) should respond(200, 201)

      val res0 = queryErr(s"""legacy_udf(10)""", db)
      (res0 / "error" / "code").as[String] shouldBe "abort"
      (res0 / "error" / "message").as[String] shouldBe "Query aborted."
      (res0 / "error" / "abort").as[String] shouldBe "some message"
      (res0 / "summary").as[String] shouldBe
        """|error: Query aborted.
           |at *query*:1:11
           |  |
           |1 | legacy_udf(10)
           |  |           ^^^^
           |  |""".stripMargin

      val res1 = queryErr(s"""legacy_udf(.name)""", db)
      (res1 / "error" / "code").as[String] shouldBe "invalid_argument"
      (res1 / "error" / "message")
        .as[String] shouldBe "Invalid argument for `arg`. FQL v4 does not support `Lambda` values."
      (res1 / "summary").as[String] shouldBe
        """|error: Invalid argument for `arg`. FQL v4 does not support `Lambda` values.
           |at *query*:1:11
           |  |
           |1 | legacy_udf(.name)
           |  |           ^^^^^^^
           |  |""".stripMargin
    }

    test("can render fql1 bodies") {
      val db = aDatabase.sample

      client.api.query(
        JSObject(
          "create_function" -> JSObject(
            "object" -> JSObject(
              "name" -> "legacy_udf",
              "body" -> JSObject(
                "@query" -> JSObject("lambda" -> JSArray.empty, "expr" -> 10))
            )
          )
        ),
        db.adminKey
      ) should respond(200, 201)

      val res = queryOk(s"""legacy_udf.definition""", db)
      (res / "coll").as[String] shouldBe "Function"
      (res / "body").as[String] shouldBe "[function <lambda>]"
      (res / "name").as[String] shouldBe "legacy_udf"
    }
  }

  "alias" - {
    test("can create UDF with alias and use with alias") {
      val db = aDatabase.sample

      val createRes = queryOk(
        s"""|Function.create({
            |  name: "AddTwo",
            |  alias: "FuncAlias",
            |  body: "x => x + 2"
            |})
            |""".stripMargin,
        db
      )

      (createRes / "name").as[String] shouldEqual "AddTwo"
      (createRes / "alias").as[String] shouldEqual "FuncAlias"
      (createRes / "body").as[String] shouldEqual "x => x + 2"
      (createRes / "id").isEmpty shouldBe true

      queryOk(
        s"""|
            |FuncAlias.definition.update({ body: "x => x + ' two fish'" })""".stripMargin,
        db,
        // FIXME: remove when UDF type has definition field
        FQL2Params(typecheck = Some(false))
      )

      val updateRes =
        queryOk(s"""FuncAlias.definition""", db, FQL2Params(typecheck = Some(false)))

      (updateRes / "name").as[String] shouldEqual "AddTwo"
      (updateRes / "alias").as[String] shouldEqual "FuncAlias"
      (updateRes / "body").as[String] shouldEqual "x => x + ' two fish'"
      (updateRes / "id").isEmpty shouldBe true

      queryOk(s"""FuncAlias('one fish')""", db)
        .as[String] shouldEqual "one fish two fish"
      queryOk(s"""AddTwo('one fish')""", db)
        .as[String] shouldEqual "one fish two fish"
    }
  }

  "FQL4 filtering" - {
    test("internal_sig doesn't show up when a function is queried from FQL4") {
      val db = aDatabase.sample

      queryOk(
        s"""|Function.create({
            |  name: "TestFunc",
            |  body: 'x => x'
            |})""".stripMargin,
        db
      )

      val legacyRes = legacyQuery(
        Get(
          Ref("functions/TestFunc")
        ),
        db
      )

      (legacyRes / "name").as[String] shouldBe "TestFunc"
      (legacyRes / "internal_sig").isEmpty shouldBe true
    }
  }

  test("null spans shouldn't get filtered") {
    val db = aDatabase(typechecked = true, pprotected = false).sample

    queryOk("Function.create({ name: 'Bar', body: 'x => x.foo' })", db)
    queryOk(
      """|Role.create({
         |  name: "call_bar",
         |  privileges: [{
         |    resource: "Bar",
         |    actions: {
         |      call: true
         |    }
         |  }]
         |})""".stripMargin,
      db
    )

    val key = queryOk("Key.create({ role: 'call_bar' }).secret", db).as[String]
    val res1 = queryErr(
      "Bar(0)",
      key
    )
    (res1 / "error" / "code").as[String] shouldBe "invalid_query"
    (res1 / "error" / "message")
      .as[String] shouldBe "The query failed 1 validation check"
    (res1 / "summary").as[String] shouldBe
      """|error: Type `Int` does not have field `foo`
         |at <no source>
         |hint: Type `Int` inferred here
         |at *query*:1:5
         |  |
         |1 | Bar(0)
         |  |     ^
         |  |""".stripMargin
  }
}
