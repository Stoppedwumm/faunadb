package fauna.api.test

import fauna.codex.json._
import fauna.prop.api.Database
import org.scalactic.source.Position

// ProtocolSpec but not 8 years (!!!) old
class FQL2ProtocolSpec extends FQL2APISpec {
  "/query/1" - {

    def queryOkChecked(
      query: String,
      db: Database,
      params: Seq[(String, JSValue)] = Seq.empty,
      headers: Seq[(String, String)] = Seq.empty)(implicit pos: Position) = {
      val q = queryRaw(
        query,
        db,
        FQL2Params(
          arguments = JSObject(params: _*),
          headers = headers,
          typecheck =
            if (headers.exists(_._1 == "x-type-check")) None else Some(true)
        )
      )
      q should respond(200)
      (q.json / "summary") should matchPattern { case JSString(_) => }
      (q.json / "txn_ts") should matchPattern { case JSLong(_) => }
      (q.json / "stats" / "compute_ops") should matchPattern { case JSLong(_) => }
      (q.json / "stats" / "read_ops") should matchPattern { case JSLong(_) => }
      (q.json / "stats" / "write_ops") should matchPattern { case JSLong(_) => }
      (q.json / "stats" / "storage_bytes_read") should matchPattern {
        case JSLong(_) =>
      }
      (q.json / "stats" / "storage_bytes_write") should matchPattern {
        case JSLong(_) =>
      }

      if (headers.exists(_._1 == "x-query-tags")) {
        (q.json / "query_tags") should matchPattern { case JSString(_) => }
      }

      q.json / "data"
    }

    test("handles deeply nested JSON") {
      val db = aDatabase.sample
      val body = {
        var str = """{"value":1}"""
        // build up JSON to exceed stack limit. Might need to increase this in the
        // future.
        for (_ <- 1 to 10000) { str = s"""{"fql":[$str]}""" }
        s"""{"query":$str}"""
      }

      val res = client.api.post("/query/1", body, db.adminKey)
      res should respond(400)
      (res.json / "error" / "code") shouldEqual JSString("invalid_request")
      (res.json / "error" / "message") shouldEqual JSString(
        "Invalid request body: JSON exceeds nesting limit")
    }

    "x-format" - {
      test("defaults to simple format") {
        val db = aDatabase.sample

        val data = queryOkChecked("1", db)
        data shouldBe JSLong(1)
      }

      test("can set the format with headers") {
        val db = aDatabase.sample

        queryOkChecked(
          "1",
          db,
          headers = Seq("x-format" -> "simple")
        ) shouldBe JSLong(1)
        queryOkChecked(
          "1",
          db,
          headers = Seq("x-format" -> "tagged")
        ) shouldBe JSObject("@int" -> "1")
        queryOkChecked(
          "1",
          db,
          headers = Seq("x-format" -> "decorated")
        ) shouldBe JSString("1")
      }

      test("setting an invalid format doesn't 500") {
        val db = aDatabase.sample

        queryErr(
          "1",
          db,
          FQL2Params(headers = Seq("x-format" -> "foo"), format = None))
      }
    }

    "x-typecheck" - {
      test("can set typecheck with headers") {
        val db = aDatabase.sample

        queryOkChecked("1", db, headers = Seq("x-typecheck" -> "false"))
        queryOkChecked("1", db, headers = Seq("x-typecheck" -> "true"))

        queryErr(
          "1",
          db,
          FQL2Params(
            headers = Seq("x-typecheck" -> "foo"),
            typecheck = None
          ))
      }
    }

    "x-linearized" - {
      test("can set linearized with headers") {
        val db = aDatabase.sample

        queryOkChecked("1", db, headers = Seq("x-linearized" -> "false"))
        queryOkChecked("1", db, headers = Seq("x-linearized" -> "true"))

        queryErr("1", db, FQL2Params(headers = Seq("x-linearized" -> "foo")))
      }
    }

    "x-query-timeout-ms" - {
      // This sets the query timeout to 1 millisecond, and then expects it to fail.
      // This will only be flaky if we can execute a query in 1 millisecond. If that
      // ever happens then by all means remove this test.
      test("can set the timeout with headers") {
        val db = aDatabase.sample

        val res =
          queryRaw("1", db, FQL2Params(headers = Seq("x-query-timeout-ms" -> "1")))
        res should respond(440)
        queryOkChecked("1", db, headers = Seq("x-query-timeout-ms" -> "1000"))
      }
    }

    "x-last-txn-ts" - {
      // Testing functionality of this is flaky, so I'm not going to.
      test("can set the last transaction time with headers") {
        val db = aDatabase.sample

        queryErr("1", db, FQL2Params(headers = Seq("x-last-txn-ts" -> "foo")))
        queryOkChecked("1", db, headers = Seq("x-last-txn-ts" -> "123"))
      }
    }

    "x-max-contention-retries" - {
      // Testing functionality of this is flaky, so I'm not going to.
      test("can set the last transaction time with headers") {
        val db = aDatabase.sample

        queryErr(
          "1",
          db,
          FQL2Params(headers = Seq("x-max-contention-retries" -> "-3")))
        queryOkChecked("1", db, headers = Seq("x-max-contention-retries" -> "2"))
      }
    }

    // See also ProtocolSpec, which tests the other version of this (x-fauna-tags)
    "x-query-tags" - {
      test("response is in correct shape") {
        val db = aDatabase.sample

        // presence of tags is checked in queryOkChecked(). see above
        queryOkChecked("1", db, headers = Seq("x-query-tags" -> "foo=bar"))

        val res =
          queryErr("1", db, FQL2Params(headers = Seq("x-query-tags" -> "foo,,")))
        val errorsJson = JSObject(
          "error" -> JSObject(
            "code" -> "invalid_request",
            "message" -> "Invalid header 'x-query-tags': invalid tags"))
        res should equal(errorsJson)
      }

      test("x-fauna-tags should not work") {
        val db = aDatabase.sample

        // x-fauna-tags will be ignored on the fqlx endpoint.
        queryOkChecked("1", db, headers = Seq("x-fauna-tags" -> "foo,,"))
      }
    }
    "schema version" - {
      test("included in successful query response") {
        val db = aDatabase.sample

        val resTs = queryRaw(""" Collection.create({ name: "hi" })  """, db)
        resTs should respond(200)
        val expectedSchemaVersion = (resTs.json / "txn_ts").as[Long]

        val res = queryRaw(""" hi.all().first() """, db)
        res should respond(200)
        val schemaVersion = (res.json / "schema_version").as[Long]

        schemaVersion shouldEqual expectedSchemaVersion
      }

      test("included in error query response") {
        val db = aDatabase.sample

        val resTs = queryRaw(""" Collection.create({ name: "hi" })  """, db)
        resTs should respond(200)
        val expectedSchemaVersion = (resTs.json / "txn_ts").as[Long]

        val res = queryErr(""" hii.all().first() """, db)

        val schemaVersion = (res / "schema_version").as[Long]

        schemaVersion shouldEqual expectedSchemaVersion
      }
    }
  }
}
