package fauna.api.test

import fauna.codex.json._
import fauna.net.http.{ Body, ContentType }
import fauna.net.util.{ URIEncoding, URIQueryString }

class QueryEndpointSpec extends API21Spec {
  "/" - {
    def encode(q: JSValue) = s"q=${URIEncoding.encode(q.toString)}"

    once("requires authentication") {
      for {
        database <- aDatabase
        user <- aUser(database)
      } {
        api.query(JSArray("hi", "world"), null) should respond(Unauthorized)
        api.query(JSArray("hi", "world"), rootKey) should respond(OK)
        api.query(JSArray("hi", "world"), database.key) should respond(OK)
        api.query(JSArray("hi", "world"), database.clientKey) should respond(OK)
        api.query(JSArray("hi", "world"), user.token) should respond(OK)

        api.get("/", query = encode(JSArray("hi", "world"))) should respond(
          Unauthorized)
        api.get("/", query = encode(JSArray("hi", "world")), token = rootKey) should respond(
          OK)
        api.get("/", query = encode(JSArray("hi", "world")), token = database.key) should respond(
          OK)
        api.get("/",
                query = encode(JSArray("hi", "world")),
                token = database.clientKey) should respond(OK)
        api.get("/", query = encode(JSArray("hi", "world")), token = user.token) should respond(
          OK)
      }
    }

    once("GET is read-only") {
      for {
        database <- aDatabase
        cls <- aCollection(database)
      } {
        api.query(JSObject("create" -> cls.refObj), database.key) should respond(
          Created)

        val res =
          api.get("/",
                  query = encode(JSObject("create" -> cls.refObj)),
                  token = database.key)

        res should respond(BadRequest)
        (res.json / "errors" / 0 / "position") should equal(JSArray.empty)
        (res.json / "errors" / 0 / "code").as[String] should equal(
          "invalid expression")
        (res.json / "errors" / 0 / "description").as[String] should equal(
          "Call performs a write, which is not allowed in get requests.")
      }
    }

    once("Handles malformed JSON") {
      for {
        database <- aDatabase
      } {
        val res = api.query(Body("[1", ContentType.JSON), database.key)

        res should respond(BadRequest)
        (res.json / "errors" / 0 / "position") should equal(JSArray.empty)
        (res.json / "errors" / 0 / "code").as[String] should equal(
          "invalid expression")
        (res.json / "errors" / 0 / "description").as[String] should equal(
          "Request body is not valid JSON.")
      }
    }

    once("Handles deeply nested JSON") {
      for { db <- aDatabase } {
        val body = {
          var str = """{"value":1}"""
          // build up JSON to exceed stack limit. Might need to increase this in the
          // future.
          for (_ <- 1 to 10000) { str = s"""{"fql":[$str]}""" }
          s"""{"query":$str}"""
        }

        val res = client.api.post("/", body, db.adminKey)
        res should respond(400)
        (res.json / "errors" / 0 / "code") shouldEqual JSString("invalid expression")
        (res.json / "errors" / 0 / "description") shouldEqual JSString(
          "Request body JSON exceeds nesting limit.")
      }
    }

    test("query string parsing") {
      val arr = JSArray("2+2", "=", "4", "&", "no more")
      val rep = api.get("/", query = encode(arr), token = rootKey)
      rep should respond(OK)
      (rep.json / "resource") should equal(arr)

      // test directly because the HttpClient wont allow the bad query
      URIQueryString.parse("q=%R1&ts=123") shouldBe empty
    }
  }

  "/linearized" - {
    once("requires authentication") {
      for {
        database <- aDatabase
        user <- aUser(database)
      } {
        api.post("/linearized", JSArray("hi", "world")) should respond(Unauthorized)
        api.post("/linearized", JSArray("hi", "world"), rootKey) should respond(OK)
        api.post("/linearized", JSArray("hi", "world"), database.key) should respond(
          OK)
        api.post("/linearized", JSArray("hi", "world"), database.clientKey) should respond(
          OK)
        api.post("/linearized", JSArray("hi", "world"), user.token) should respond(
          OK)
      }
    }

    once("is POST only") {
      for {
        database <- aDatabase
      } {
        val rep = api.get("/linearized", token = database.key)
        rep should respond(MethodNotAllowed)
      }
    }
  }
}
