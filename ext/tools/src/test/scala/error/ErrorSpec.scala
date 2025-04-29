package fauna.tools.error

import fauna.api.test.Spec
import fauna.codex.json.JSObject

class ErrorSpec extends Spec {

  "toError" - {

    "should convert unauthorized code to typed error" in {
      Error.toError(
        json = JSObject("code" -> "unauthorized", "description" -> "desc"),
        statusCode = 0,
      ) shouldBe Error.Unauthorized
    }

    "should convert bad request code to typed error" in {
      Error.toError(
        json = JSObject("code" -> "bad request", "description" -> "bad"),
        statusCode = 0,
      ) shouldBe Error.BadRequest("bad")
    }

    "should convert cluster uninitialized code to typed error" in {
      val input = JSObject("code" -> "cluster uninitialized", "description" -> "kaboom!")
      Error.toError(
        json = input,
        statusCode = 0,
      ) shouldBe Error.ClusterUninitialized("kaboom!", 0, input)
    }

    "should convert unknown code to typed error" in {
      val input = JSObject("code" -> "something went wrong", "description" -> "kaboom!")
      Error.toError(
        json = input,
        statusCode = 0,
      ) shouldBe Error.Unknown("something went wrong", "kaboom!", 0, input)
    }

    "should return unknown for messages that contain no error code" in {
      val input = JSObject("no code" -> "something went wrong", "description" -> "kaboom!")
      Error.toError(
        json = input,
        statusCode = 0,
      ) shouldBe Error.Unknown("Unknown", "Unknown error message", 0, input)
    }

    "should return unknown for messages that contain no error description" in {
      val input = JSObject("code" -> "something went wrong", "no description" -> "kaboom!")
      Error.toError(
        json = input,
        statusCode = 0,
      ) shouldBe Error.Unknown("Unknown", "Unknown error message", 0, input)
    }
  }

  "toErrors" - {
    "should convert a List of errors to typed Errors" in {
      val clusterUninitializedJson = JSObject("code" -> "cluster uninitialized", "description" -> "kaboom!")
      val unknowJson = JSObject("code" -> "oh no", "description" -> "don't know")
      val errorsJson =
        JSObject(
          "errors" -> Seq(
            JSObject("code" -> "unauthorized", "description" -> "kaboom!"),
            JSObject("code" -> "bad request", "description" -> "bad"),
            clusterUninitializedJson,
            unknowJson
          )
        )

      val expectedTypedErrors =
        Seq(
          Error.Unauthorized,
          Error.BadRequest("bad"),
          Error.ClusterUninitialized("kaboom!", 0, clusterUninitializedJson),
          Error.Unknown("oh no", "don't know", 0, unknowJson)
        )

      Error.toErrors(json = errorsJson, statusCode = 0) shouldBe expectedTypedErrors
    }
  }

  "toString" - {
    "should convert a single error message to String" in {

      val errorsJson = JSObject("errors" -> Seq(JSObject("code" -> "unauthorized", "description" -> "kaboom!")))

      Error.toString(errorsJson, 0, ErrorMessage.Common) shouldBe
        "Failed: " + ErrorMessage.Common.toString(Error.Unauthorized)
    }

    "should convert a List of errors to a String message" in {
      val clusterUninitializedJson = JSObject("code" -> "cluster uninitialized", "description" -> "kaboom!")
      val unknownErrorJson = JSObject("code" -> "oh no", "description" -> "don't know")

      val errorsJson =
        JSObject(
          "errors" -> Seq(
            JSObject("code" -> "unauthorized", "description" -> "kaboom!"),
            JSObject("code" -> "bad request", "description" -> "something bad happened!"),
            clusterUninitializedJson,
            unknownErrorJson
          )
        )

      val expectedStringForStatus503 =
        s"""Failed:
           | - ${ErrorMessage.unauthorized}
           | - something bad happened!
           | - ${ErrorMessage.clusterUninitialized("kaboom!")}
           | - ${ErrorMessage.unknown(unknownErrorJson, Some("don't know"))}""".stripMargin

      Error.toString(json = errorsJson, statusCode = 503, ErrorMessage.Common) shouldBe expectedStringForStatus503

      val expectedStringForStatusNot503 =
        s"""Failed:
           | - ${ErrorMessage.unauthorized}
           | - something bad happened!
           | - ${ErrorMessage.unknown(clusterUninitializedJson, Some("kaboom!"))}
           | - ${ErrorMessage.unknown(unknownErrorJson, Some("don't know"))}""".stripMargin

      Error.toString(json = errorsJson, statusCode = 0, ErrorMessage.Common) shouldBe expectedStringForStatusNot503
    }

  }
}
