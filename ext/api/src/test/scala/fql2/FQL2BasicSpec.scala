package fauna.api.test

import fauna.codex.json._
import fauna.net.http.NoBody

class FQL2BasicSpec extends FQL2APISpec {

  "endpoint/test harness" - {
    test("can run queries") {
      val db = aDatabase.sample
      queryOk("""'foo'.length""", db).as[Long] shouldEqual 3
    }

    test("can run queries2") {
      val db = aDatabase.sample
      (queryOk("""{ len: 'foo'.length }""", db) / "len").as[Long] shouldEqual 3
    }

    test("fails no return") {
      val db = aDatabase.sample
      val errRes = queryErr("""{ let x = 5 }""", db)
      (errRes / "error" / "code").as[String] shouldEqual "invalid_query"
    }

    test("can provide request parameters") {
      val db = aDatabase.sample
      val headers = Seq(("X-Timeout" -> "600"))
      (queryOk(
        """{ len: 'foo'.length }""",
        db,
        FQL2Params(headers = headers)) / "len")
        .as[Long] shouldEqual 3
    }

    test("can provide arguments") {
      val db = aDatabase.sample
      val arguments = JSObject(
        "aLong" -> JSLong(10),
        "aDouble" -> JSDouble(3.14159),
        "aString" -> JSString("str"),
        "anObject" -> JSObject("x" -> 10, "y" -> 20),
        "anArray" -> JSArray(10, 20)
      )
      val expected = arguments :+
        ("aNestedField" -> JSLong(10)) :+
        ("anIndexedField" -> JSLong(10)) :+
        ("aMappedField" -> JSArray(20, 40))

      queryOk(
        """|let aNestedField = anObject.x
           |let anIndexedField = anArray[0]
           |let aMappedField = anArray.map(i => i * 2)
           |
           |{
           |  aLong: aLong,
           |  aDouble: aDouble,
           |  aString: aString,
           |  anObject: anObject,
           |  anArray: anArray,
           |  aNestedField: aNestedField,
           |  anIndexedField: anIndexedField,
           |  aMappedField: aMappedField
           |}
           |""".stripMargin,
        db,
        FQL2Params(arguments = arguments)
      ) shouldEqual expected
    }

    test("correctly fails invalid auth query") {
      val body = JSObject(("query", JSString("""{ len: 'foo'.length }""")))
      val res = client.api.post("/query/1", body, "notanauth")

      res.statusCode shouldEqual 401

      val error = (res.json / "error").as[JSValue]
      (error / "code").as[String] shouldEqual "unauthorized"
      (error / "message")
        .as[String] shouldEqual "Access token required"
    }

    test("returns invalid request") {
      val db = aDatabase.sample
      val invalidBody = JSObject("foo" -> "bar")
      val res = client.api.post("/query/1", invalidBody, db.adminKey)

      res.statusCode shouldEqual 400

      val error = (res.json / "error").as[JSValue]
      (error / "code").as[String] shouldEqual "invalid_request"
      (error / "message").as[String] shouldEqual "Invalid request body"
    }

    test("returns invalid request for no body") {
      val db = aDatabase.sample
      val res = client.api.post("/query/1", NoBody, db.adminKey)

      res.statusCode shouldEqual 400

      val error = (res.json / "error").as[JSValue]
      (error / "code").as[String] shouldEqual "invalid_request"
      (error / "message").as[String] shouldEqual "No request body provided"
    }

    test("returns unbound_variable for invalid reference") {
      val db = aDatabase.sample
      val errRes = queryErr("""{ x }""", db, FQL2Params(typecheck = Some(false)))

      val expectedMessage = "Unbound variable `x`"
      val expectedSummary = s"""|error: $expectedMessage
                                |at *query*:1:3
                                |  |
                                |1 | { x }
                                |  |   ^
                                |  |""".stripMargin

      (errRes / "summary").as[String] shouldEqual expectedSummary
      (errRes / "error" / "code").as[String] shouldEqual "unbound_variable"
      (errRes / "error" / "message")
        .as[String] shouldEqual "Unbound variable `x`"
    }

    test("returns invalid_syntax when parsing the query fails") {
      val db = aDatabase.sample
      val errRes = queryErr("""{ x """, db)

      (errRes / "error" / "code").as[String] shouldEqual "invalid_query"
      (errRes / "error" / "message")
        .as[String] shouldEqual "The query failed 1 validation check"
    }

    test("returns type_mismatch runtime error") {
      val db = aDatabase.sample
      val expectedCode = "type_mismatch"
      val expectedMessage = "expected type: Boolean, received Int"
      val expectedSumary =
        s"""|error: $expectedMessage
            |at *query*:3:9
            |  |
            |3 |     if (y) {
            |  |         ^
            |  |
            |at *query*:9:4
            |  |
            |9 |   x(3)
            |  |    ^^^
            |  |""".stripMargin

      val errRes = queryErr(
        """|{
           |  let x = (y) => {
           |    if (y) {
           |      "z"
           |    } else {
           |      "?"
           |    }
           |  }
           |  x(3)
           |}""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      )

      (errRes / "error" / "code").as[String] shouldEqual expectedCode
      (errRes / "error" / "message")
        .as[String] shouldEqual expectedMessage
      (errRes / "summary").as[String] shouldEqual expectedSumary
    }

    test("returns invalid_function_invocation runtime error") {
      val db = aDatabase.sample

      val expectedCode = "invalid_function_invocation"
      val expectedMessage =
        "callee function expects exactly 2 arguments, but 1 was provided."
      val expectedSumary =
        s"""|error: $expectedMessage
          |at *query*:5:9
          |  |
          |5 |   concat("only-one-arg")
          |  |         ^^^^^^^^^^^^^^^^
          |  |""".stripMargin
      val errRes = queryErr(
        """|{
           |  let concat = (x, y) => {
           |    x.concat(y)
           |  }
           |  concat("only-one-arg")
           |}""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      )

      (errRes / "error" / "code").as[String] shouldEqual expectedCode
      (errRes / "error" / "message")
        .as[String] shouldEqual expectedMessage
      (errRes / "summary").as[String] shouldEqual expectedSumary

    }

    test("returns invalid_argument for invalid argument type") {
      val db = aDatabase.sample
      val errRes = queryErr(
        """|{
           |  let concat = (x, y) => {
           |    x.concat(y)
           |  }
           |  concat("only-one-arg", 1)
           |}""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      )
      (errRes / "error" / "code").as[String] shouldEqual "invalid_argument"
      (errRes / "error" / "message")
        .as[String] shouldEqual "expected value for `other` of type String, received Int"
    }

    test(
      "returns invalid_null_access when attempting to invoke a function on null") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample
      val errRes = queryErr(
        s"""|
            |let author = $collection.all().firstWhere(.id == "1")
            |author.test()
            |""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      )
      (errRes / "error" / "code")
        .as[String] shouldEqual "invalid_null_access"
      (errRes / "error" / "message")
        .as[String] shouldEqual "Cannot access `test` on null."
    }

    test("multiple traces are shown") {
      val db = aDatabase.sample
      val errRes = queryErr(
        s"[1].firstWhere(v => [1, 2].firstWhere(v => v == null!) == 3)",
        db
      )

      val expectedRes =
        """|error: Null literal value
           |at *query*:1:49
           |  |
           |1 | [1].firstWhere(v => [1, 2].firstWhere(v => v == null!) == 3)
           |  |                                                 ^^^^^
           |  |
           |  |
           |1 | [1].firstWhere(v => [1, 2].firstWhere(v => v == null!) == 3)
           |  |                                      ^^^^^^^^^^^^^^^^^
           |  |
           |  |
           |1 | [1].firstWhere(v => [1, 2].firstWhere(v => v == null!) == 3)
           |  |               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
           |  |""".stripMargin

      (errRes / "error" / "code").as[String] shouldEqual "null_value"
      (errRes / "error" / "message").as[String] shouldEqual "Null literal value"
      (errRes / "summary").as[String] shouldEqual expectedRes
    }

    test("nulls show hints and traces") {
      val db = aDatabase.sample
      val errRes = queryErr(
        s"""|let myVal = null
            |[1, 2].firstWhere(v => myVal!)""".stripMargin,
        db
      )

      val expectedRes =
        """|error: Null literal value
           |at *query*:2:24
           |  |
           |2 | [1, 2].firstWhere(v => myVal!)
           |  |                        ^^^^^^
           |  |
           |hint: Null value created here
           |at *query*:1:13
           |  |
           |1 | let myVal = null
           |  |             ^^^^
           |  |
           |trace:
           |  |
           |2 | [1, 2].firstWhere(v => myVal!)
           |  |                  ^^^^^^^^^^^^^
           |  |""".stripMargin

      (errRes / "error" / "code").as[String] shouldEqual "null_value"
      (errRes / "error" / "message").as[String] shouldEqual "Null literal value"
      (errRes / "summary").as[String] shouldEqual expectedRes
    }

    test("nulls don't show hints and traces when overlapping") {
      val db = aDatabase.sample
      val errRes = queryErr(
        s"[1, 2].firstWhere(v => null!)",
        db
      )

      val expectedRes =
        """|error: Null literal value
           |at *query*:1:24
           |  |
           |1 | [1, 2].firstWhere(v => null!)
           |  |                        ^^^^^
           |  |
           |  |
           |1 | [1, 2].firstWhere(v => null!)
           |  |                  ^^^^^^^^^^^^
           |  |""".stripMargin

      (errRes / "error" / "code").as[String] shouldEqual "null_value"
      (errRes / "error" / "message").as[String] shouldEqual "Null literal value"
      (errRes / "summary").as[String] shouldEqual expectedRes
    }

    test(
      "returns invalid_function_invocation when attempting to invoke a function that does not exist") {
      val db = aDatabase.sample
      val errRes = queryErr(
        """|{
           |  let x = "hi"
           |  x.conct(", how are you?")
           |}""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      )
      (errRes / "error" / "code")
        .as[String] shouldEqual "invalid_function_invocation"
      (errRes / "error" / "message")
        .as[String] shouldEqual "The function `conct` does not exist on `String`"
    }

    test(
      "returns invalid_function_invocation when attempting to invoke a field that is not a function") {
      val db = aDatabase.sample
      val errRes = queryErr(
        """|{
           |  let x = "hi"
           |  x.length()
           |}""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      )
      (errRes / "error" / "code")
        .as[String] shouldEqual "invalid_function_invocation"
      (errRes / "error" / "message")
        .as[String] shouldEqual "`length`: Expected a function, found `Int`"
    }

    test("returns a 400 when reading before MVT") {
      val db = aDatabase.sample
      queryOk(
        """Collection.create({ name: "Author", "history_days": 0 })""",
        db
      )
      val errRes = queryErr(
        """|
            |at (Time("2000-01-01T00:00:00.000Z")) {
            |  Author.byId("1")!.foo
            |}
            |""".stripMargin,
        db
      )
      (errRes / "error" / "code")
        .as[String] shouldEqual "invalid_request"
      (errRes / "error" / "message")
        .as[String] should include(
        "Requested timestamp 2000-01-01T00:00:00Z less than minimum allowed timestamp")
    }

    test("rejects invalid history_days") {
      val db = aDatabase.sample
      val errRes = queryErr(
        """Collection.create({ name: "Author", "history_days": -1 })""",
        db
      )
      (errRes / "error" / "code")
        .as[String] shouldEqual "constraint_failure"
      (errRes / "error" / "message")
        .as[String] shouldEqual "Failed to create Collection."
      (errRes / "summary")
        .as[String] should include("history_days: Non-negative value required.")
    }

    "with typecheck enabled" - {
      test("returns the type of the result value in the static_type field") {
        val db = aDatabase.sample
        val res = queryRaw(
          """|"foo".concat("bar")
             |""".stripMargin,
          db,
          FQL2Params(typecheck = Some(true)))
        res should respond(200)
        (res.json / "static_type").as[String] shouldEqual "String"
      }

      test("renders the error message in the right spot") {
        val db = aDatabase.sample
        val err = queryErr(
          """|2 + "a"
             |""".stripMargin,
          db,
          FQL2Params(typecheck = Some(true)))
        val expectedSummary =
          s"""|error: Type `String` is not a subtype of `Number`
              |at *query*:1:5
              |  |
              |1 | 2 + "a"
              |  |     ^^^
              |  |""".stripMargin
        (err / "error" / "code").as[String] shouldEqual "invalid_query"
        (err / "error" / "message")
          .as[String] shouldEqual "The query failed 1 validation check"
        (err / "summary").as[String] shouldEqual expectedSummary
      }
    }

    test("Avoid exponential materialization in fold + where lambda") {
      val db = aDatabase.sample
      val xs = List.fill(100)(0).mkString("[", ",", "]") // [0, 0, ..., 0].
      // Previously, this query exploded because the lambda closes
      // over `acc`, causing some parts of materialization to
      // go exponential in the length of `xs`.
      val q =
        s"""|$xs.fold([].toSet(), (acc, param) => {
            |  acc.concat([param].toSet().where(x => true))
            |})
            |""".stripMargin
      queryOk(q, db)
    }

    // test("can assert an FQL query returns true") {
    //   val db = aDatabase.sample
    //   fqlAssert("true", db)
    // }

    // test("can assert FQL equality") {
    //   pendingUntilFixed {
    //     val db = aDatabase.sample
    //     fqlAssertEquals("{ foo: 4 }", "4", db)
    //   }
    // }
  }
}
