package fauna.api.test

import fauna.atoms.APIVersion
import fauna.codex.json._
import fauna.net.http.{ Body, ContentType, HTTPHeaders }
import fauna.prop.api.DefaultQueryHelpers
import io.netty.buffer.Unpooled
import java.io.IOException

class ProtocolSpec extends API21Spec with DefaultQueryHelpers {
  "*" / {
    test("returns a proper not found error") {
      val res = api.get("/nonexistent")

      res should respond(NotFound)
      res.json should equal(
        JSObject("errors" -> JSArray(
          JSObject("code" -> "not found", "description" -> "Not Found"))))
    }

    once("disallows dangling surrogates") {
      for {
        db <- aDatabase
      } {
        // This is a dangling surrogate: "\udc00"
        // If we encode that string to UTF8, java will encode it to a single ASCII
        // question mark (?). If we use another language (rust), we get these bytes:
        // [0xed, 0xb0, 0x80].
        //
        // Then, around those bytes, we add quotes ('"'), which makes it valid JSON.
        //
        // Also hex literals always produce integers for whatever reason.
        val buf1 = Seq[Int]('"', 0xed, 0xb0, 0x80, '"').map(_.toByte).toArray
        val res1 =
          api.post("/", Body(Unpooled.wrappedBuffer(buf1), ContentType.JSON), db.key)

        // We can't fix any of the problems below, as that may be relied on in v4.

        // FIXME: This should be a BadRequest, but we allow dangling surrogates.
        // res1 should respond(BadRequest)
        // res1.json should equal(
        //   JSObject(
        //     "errors" -> JSArray(
        //       JSObject(
        //         "position" -> JSArray(),
        //         "code" -> "invalid expression",
        //         "description" -> "Request body is not valid JSON."))))

        // FIXME: In the response, we then convert dangling surrogates to an
        // ASCII `?`, because that's what java does.
        res1 should respond(OK)
        res1.json should equal(JSObject("resource" -> JSString("?")))

        // And I'm just going to sanity check that some valid utf8 is allowed.
        // This is the encoding of è using rust: [0xc3, 0xa8]
        val buf2 = Seq[Int]('"', 0xc3, 0xa8, '"').map(_.toByte).toArray
        val res2 =
          api.post("/", Body(Unpooled.wrappedBuffer(buf2), ContentType.JSON), db.key)

        res2 should respond(OK)
        res2.json should equal(JSObject("resource" -> JSString("è")))
      }
    }

    test("responds to CORS preflight requests") {
      val res = api.options(
        "/databases",
        null,
        null,
        Seq(HTTPHeaders.Origin -> "http://example.com"))
      res.headers.contains(HTTPHeaders.AccessControlAllowOrigin) should be(true)
      res.headers.contains(HTTPHeaders.AccessControlAllowCredentials) should be(true)

      res.headers.get(HTTPHeaders.AccessControlMaxAge) should equal("86400")
      res.headers.get(HTTPHeaders.AccessControlAllowMethods) should equal(
        "GET, POST, PUT, PATCH, DELETE")
      res.headers.get(HTTPHeaders.AccessControlAllowHeaders) should equal(
        "accept-encoding, authorization, content-type, x-driver-env, " +
          "x-faunadb-api-version, x-fauna-driver, x-fauna-source, x-fauna-tags, " +
          "x-format, x-last-seen-txn, x-last-txn-ts, x-linearized, " +
          "x-max-contention-retries, x-max-retries-on-contention, x-query-tags, " +
          "x-query-timeout, x-query-timeout-ms, x-runtime-environment, " +
          "x-runtime-environment-os, x-nodejs-version, traceparent, x-typecheck, x-performance-hints")

      res.headers.get(HTTPHeaders.AccessControlExposeHeaders) should equal("")
    }

    test("exposes 'x-*' headers with appropriate CORS headers") {
      val res = api.get(
        "/databases",
        rootKey,
        null,
        Seq(HTTPHeaders.Origin -> "http://example.com"))
      res.headers
        .get(HTTPHeaders.AccessControlExposeHeaders)
        .split(", ")
        .sorted should equal(
        Seq(
          "x-byte-read-ops",
          "x-byte-write-ops",
          "x-compute-ops",
          "x-query-bytes-in",
          "x-query-bytes-out",
          "x-query-time",
          "x-storage-bytes-read",
          "x-storage-bytes-write",
          "x-txn-retries",
          "x-txn-time"
        ))
    }

    test("CORS headers are returned with an Origin header in the request") {
      val res1 = api.get(
        "/databases",
        rootKey,
        null,
        Seq(HTTPHeaders.Origin -> "http://example.com"))
      res1.headers.contains(HTTPHeaders.AccessControlAllowOrigin) should be(true)
      res1.headers.contains(HTTPHeaders.AccessControlAllowCredentials) should be(
        true)

      val res2 = api.get("/databases", rootKey)
      res2.headers.contains(HTTPHeaders.AccessControlAllowOrigin) should be(false)
      res2.headers.contains(HTTPHeaders.AccessControlAllowCredentials) should be(
        false)
    }

    test("OPTIONS is not allowed by default") {
      api.options("/databases", rootKey) should respond(MethodNotAllowed)
    }

    test("X-Trace header absent should hide FaunaDB-Host") {
      val res1 = api.get("/databases", rootKey)
      res1.headers.contains("X-FaunaDB-Host") should be(false)
      res1.headers.contains("X-FaunaDB-Build") should be(true)

      val res2 =
        api.get("/databases", rootKey, null, Seq(HTTPHeaders.Trace -> "foo"))
      res2.headers.contains("X-FaunaDB-Host") should be(true)
      res2.headers.contains("X-FaunaDB-Build") should be(true)
    }

    test("440 on aggressive timeouts") {
      val cl = client
        .withHeaders(Seq(HTTPHeaders.QueryTimeout -> "1"))
        .withVersion(APIVersion.Unstable.toString)
      val res1 = cl.api.get("/databases", rootKey)
      res1 should respond(ProcessingTimeLimitExceeded)
    }

    // See also FQL2ProtocolSpec, which tests the other version of this
    test("correct response format on invalid tags") {
      val cl = client
        .withHeaders(Seq(HTTPHeaders.FaunaTags -> "foo,,"))
        .withVersion(APIVersion.Unstable.toString)
      val res1 = cl.api.get("/", rootKey)
      res1 should respond(BadRequest)
      val errorsJson = JSObject(
        "errors" -> Seq(
          JSObject(
            "code" -> "bad request",
            "description" -> "Invalid header 'x-fauna-tags': invalid tags")))
      res1.json should equal(errorsJson)
    }

    test("handle out of bound query timeouts") {
      val durationMillis = Long.MaxValue
      val headers = Seq(HTTPHeaders.QueryTimeout -> durationMillis.toString)
      val res1 = api.get("/databases", rootKey, headers = headers)
      res1 should respond(BadRequest)
      val errorsJson = JSObject(
        "errors" -> Seq(JSObject(
          "code" -> "bad request",
          "description" -> "Invalid header 'x-query-timeout': Must be less than or equal to 600000.")))
      res1.json should equal(errorsJson)
    }

    once("API rejects large requests") {
      for {
        database   <- aDatabase
        faunaClass <- aCollection(database)
      } {
        val body = JSObject("data" -> JSObject("foo" -> "f" * 9 * 1024 * 1024))

        @annotation.tailrec
        def testLarge(left: Int): Unit =
          try {
            val res = api.query(Update(faunaClass.refObj, body), database.key)
            res should respond(RequestEntityTooLarge)
          } catch {
            case _: IOException => if (left > 0) testLarge(left - 1)
          }

        testLarge(10)
      }
    }

    once("max retries on contention") {
      for {
        db <- aDatabase
      } {
        val low =
          api.post(
            "/",
            JSNull,
            db.key,
            headers = Seq(HTTPHeaders.MaxRetriesOnContention -> "-1"))

        low should respond(BadRequest)

        val high =
          api.post(
            "/",
            JSNull,
            db.key,
            headers = Seq(HTTPHeaders.MaxRetriesOnContention -> "8"))

        high should respond(BadRequest)

        val min =
          api.post(
            "/",
            JSNull,
            db.key,
            headers = Seq(HTTPHeaders.MaxRetriesOnContention -> "0"))

        min should respond(OK)

        val max =
          api.post(
            "/",
            JSNull,
            db.key,
            headers = Seq(HTTPHeaders.MaxRetriesOnContention -> "7"))

        max should respond(OK)
      }
    }
  }
}
