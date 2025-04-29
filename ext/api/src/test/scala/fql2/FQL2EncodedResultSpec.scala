package fauna.api.test

import fauna.codex.json._

class FQL2EncodedResultSpec extends FQL2APISpec {

  "result values" - {
    test("ids encode as strings") {
      val db = aDatabase.sample
      val collName = aCollection(db).sample
      queryOk(s"""$collName.create({}).id""", db) should matchPattern {
        case JSString(_) =>
      }
    }

    test("number lits encode as numbers") {
      val db = aDatabase.sample
      queryOk("1", db) shouldEqual JSLong(1)
      // Long.MaxValue
      queryOk("9223372036854775807", db) shouldEqual JSLong(9223372036854775807L)
      // Long.MaxValue + 1
      queryOk("9223372036854775808", db) shouldEqual JSDouble(9.223372036854776e18)

      queryOk("1.0", db) shouldEqual JSDouble(1.0)
    }

    test("booleans encode as booleans") {
      val db = aDatabase.sample
      queryOk("true", db) shouldEqual JSTrue
      queryOk("false", db) shouldEqual JSFalse
    }

    test("strings encode as strings") {
      val db = aDatabase.sample
      queryOk(""" "foo" """, db) shouldEqual JSString("foo")
    }

    test("nulls encode as nulls") {
      val db = aDatabase.sample
      queryOk("""null""", db) shouldEqual JSNull

      val collName = aCollection(db).sample
      val docID = queryOk(s"""$collName.create({}).id""", db).as[String]
      queryOk(s"""$collName.byId("$docID")!.foo""", db) shouldEqual JSNull
    }

    test("sets encode as page objects") {
      val db = aDatabase.sample
      val collName = aCollection(db).sample

      val id = queryOk(s"""$collName.create({}).id""", db).as[String]
      val doc = queryOk(s"""$collName.byId("$id")""", db)

      val res = queryOk(s"""$collName.all()""", db)

      res shouldEqual JSObject("data" -> JSArray(doc))
    }

    test("sets don't recursively materialize without end") {
      val db = aDatabase.sample

      queryOk(
        "Function.create({ name: 'bomb', body: '() => [1, 2, 3].toSet().map(_ => bomb())' })",
        db)

      val res =
        queryOk(s"""bomb()""".stripMargin, db, FQL2Params(format = Some("tagged")))

      // nested sets encode as tagged cursors
      for (s <- (res / "@set" / "data").as[Seq[JSObject]]) {
        (s / "@set") should matchPattern { case JSString(_) => }
      }
    }

    test("arrays encode as arrays") {
      val db = aDatabase.sample
      queryOk("[null, null, null]", db) shouldEqual JSArray(JSNull, JSNull, JSNull)
    }

    test("object literals encode as objects") {
      val db = aDatabase.sample
      queryOk("{ a: 2 }", db) shouldEqual JSObject("a" -> 2)
      queryOk("{}", db) shouldEqual JSObject()
    }

    test("docs encode as full objects") {
      val db = aDatabase.sample
      val collName = aCollection(db).sample

      val doc1 = queryOk(s"""$collName.create({}).id""", db).as[String]
      val doc2 =
        queryOk(s"""$collName.create({ a: 2, b: $collName.byId("$doc1") }).id""", db)
          .as[String]

      val ts = queryOk(s"""$collName.byId("$doc2")!.ts""", db)
      val res = queryOk(s"""$collName.byId("$doc2")""", db)

      res shouldEqual JSObject(
        "id" -> doc2,
        "coll" -> s"$collName",
        "ts" -> ts,
        "a" -> 2,
        "b" -> JSObject("id" -> doc1, "coll" -> collName))

      val resTagged =
        queryOk(
          s"""$collName.byId("$doc2")""",
          db,
          FQL2Params(format = Some("tagged")))

      resTagged shouldEqual JSObject(
        "@doc" -> JSObject(
          "id" -> doc2,
          "coll" -> JSObject(
            "@mod" -> collName
          ),
          "ts" -> JSObject(
            "@time" -> ts
          ),
          "a" -> JSObject(
            "@int" -> "2"
          ),
          "b" -> JSObject(
            "@ref" -> JSObject(
              "id" -> doc1,
              "coll" -> JSObject("@mod" -> collName)
            )
          )
        )
      )
    }

    test("docs encode as full objects (2)") {
      val db = aDatabase.sample
      val collName = aCollection(db).sample

      val doc1 = queryOk(s"""$collName.create({})""", db)
      val id = (doc1 / "id").as[String]
      val doc2 = queryOk(s"""$collName.byId("$id")""", db)

      doc1 shouldEqual doc2
    }

    test("singleton objects encode as string stubs") {
      val db = aDatabase.sample
      queryOk(s"""Math""", db) shouldEqual JSString("Math")
    }

    test("native functions encode as string stubs") {
      val db = aDatabase.sample
      queryOk(s"""Math.abs""", db) shouldEqual JSString("[function abs()]")
    }

    test("lambdas encode as lambda stubs") {
      val db = aDatabase.sample
      queryOk(s"""x => x""", db) shouldEqual JSString("[function <lambda>]")
    }

    test("TODO lambdas encode as FQL string snippets in debug mode") { pending }

    test("abort returns an encoded value") {
      val db = aDatabase.sample
      (queryErr("abort(0)", db) / "error" / "abort") shouldEqual (JSLong(0))
      (queryErr("abort('goodbye')", db) / "error" / "abort") shouldEqual (JSString(
        "goodbye"))
      (queryErr(
        "abort([{ x: 0 }, { y: 'taco' }])",
        db) / "error" / "abort") shouldEqual JSArray(
        JSObject("x" -> JSLong(0)),
        JSObject("y" -> JSString("taco")))
    }

    test("fql4 abort returns an encoded value") {
      val db = aDatabase.sample
      client.api.query(
        JSObject(
          "create_function" -> JSObject(
            "object" -> JSObject(
              "name" -> "FQL4Abort0",
              "body" -> JSObject(
                "@query" -> JSObject(
                  "lambda" -> JSArray("x"),
                  "expr" -> JSObject(
                    "abort" -> JSObject(
                      "var" -> "x"
                    )
                  )
                )
              )
            )
          )
        ),
        db.adminKey
      ) should respond(200, 201)
      client.api.query(
        JSObject(
          "create_function" -> JSObject(
            "object" -> JSObject(
              "name" -> "FQL4Abort1",
              "body" -> JSObject(
                "@query" -> JSObject(
                  "lambda" -> JSArray("x"),
                  "expr" -> JSObject(
                    "call" -> JSString("FQL4Abort0"),
                    "arguments" -> JSObject(
                      "var" -> "x"
                    )
                  )
                )
              )
            )
          )
        ),
        db.adminKey
      ) should respond(200, 201)
      client.api.query(
        JSObject(
          "create_function" -> JSObject(
            "object" -> JSObject(
              "name" -> "FQL4Abort2",
              "body" -> JSObject(
                "@query" -> JSObject(
                  "lambda" -> JSArray("x"),
                  "expr" -> JSObject(
                    "call" -> JSString("FQL4Abort1"),
                    "arguments" -> JSObject(
                      "var" -> "x"
                    )
                  )
                )
              )
            )
          )
        ),
        db.adminKey
      ) should respond(200, 201)

      val err0 = queryErr("FQL4Abort0('hi')", db)
      err0 / "error" / "code" shouldEqual JSString("abort")
      err0 / "error" / "abort" shouldEqual JSString("hi")
      err0 / "summary" shouldEqual JSString("""|error: Query aborted.
                                               |at *query*:1:11
                                               |  |
                                               |1 | FQL4Abort0('hi')
                                               |  |           ^^^^^^
                                               |  |""".stripMargin)

      val err1 = queryErr("FQL4Abort1('hi')", db)
      err1 / "error" / "code" shouldEqual JSString("abort")
      err1 / "error" / "abort" shouldEqual JSString("hi")

      val err2 = queryErr("FQL4Abort2('hi')", db)
      err2 / "error" / "code" shouldEqual JSString("abort")
      err2 / "error" / "abort" shouldEqual JSString("hi")
    }

    test("fqlx abort returns an encoded value from fql4") {
      val db = aDatabase.sample
      queryOk(
        """|Function.create({ name: 'FQLXAbort0', body: 'x => abort(x)' })
           |Function.create({ name: 'FQLXAbort1', body: 'x => FQLXAbort0(x)' })
           |Function.create({ name: 'FQLXAbort2', body: 'x => FQLXAbort1(x)' })
           |""".stripMargin,
        db
      )

      // simple abort call
      val res0 = client.api.query(
        JSObject(
          "call" -> JSString("FQLXAbort0"),
          "arguments" -> JSString("hi")
        ),
        db.adminKey
      )
      res0 should respond(400)

      res0.json / "errors" / 0 / "code" shouldBe JSString("transaction aborted")
      res0.json / "errors" / 0 / "description" shouldBe JSString("hi")

      // nested aborts work
      val res1 = client.api.query(
        JSObject(
          "call" -> JSString("FQLXAbort1"),
          "arguments" -> JSString("hi")
        ),
        db.adminKey
      )
      res1 should respond(400)

      res1.json / "errors" / 0 / "code" shouldBe JSString("transaction aborted")
      res1.json / "errors" / 0 / "description" shouldBe JSString("hi")

      // arbitrary values are rendered to strings
      val res2 = client.api.query(
        JSObject(
          "call" -> JSString("FQLXAbort1"),
          "arguments" -> JSLong(5)
        ),
        db.adminKey
      )
      res2 should respond(400)

      res2.json / "errors" / 0 / "code" shouldBe JSString("transaction aborted")
      res2.json / "errors" / 0 / "description" shouldBe JSString("5")
    }
  }
}
