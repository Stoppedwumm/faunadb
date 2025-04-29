package fauna.model.test

import fauna.ast._
import fauna.atoms._
import fauna.codex.json._
import fauna.storage.doc.ValueRequired

class ErrorSpec extends Spec{
  "error to JSON" - {
    "simple" in {
      val js = Error.toJSValue(APIVersion.Default, QueryNotFound)

      /*
        {
          "code":"invalid expression",
          "description":"No query provided.",
          "position": []
        }
      */
      (js / "code") should equal(JSString("invalid expression"))
      (js / "description") should equal(JSString("No query provided."))
    }

    "with causes" in {
      val js = Error.toJSValue(APIVersion.Default, FunctionCallError(
        UserFunctionID(691),
        KeyPosition(RootPosition, "foo"),
        List(QueryNotFound, BoundsError("x", "x != y", RootPosition)),
      ))

      /*
        {
          "code": "call error",
          "description": "Calling the function resulted in an error.",
          "position": ["foo"],
          "cause": [
            {
              "code": "invalid expression",
              "description": "No query provided.",
              "position": []
            }, {
              "code": "invalid argument",
              "description": "x must be x != y",
              "position": []
            }
          ]
        }
      */
      (js / "code") should equal(JSString("call error"))
      (js / "description") should equal(JSString("Calling the function resulted in an error."))
      (js / "position" / 0) should equal(JSString("foo"))
      val causes = js / "cause"
      (causes / 0 / "code") should equal(JSString("invalid expression"))
      (causes / 0 / "description") should equal(JSString("No query provided."))
      (causes / 1 / "code") should equal(JSString("invalid argument"))
      (causes / 1 / "description") should equal(JSString("x must be x != y"))
    }

    "with validation failures" in {
      val js = Error.toJSValue(APIVersion.Default, ValidationError(
        List(ValueRequired(List("path", "to", "foo"))),
        RootPosition,
      ))

      /*
        {
          "code": "validation failed",
          "description": "instance data is not valid.",
          "position": []
        }
      */
      (js / "code") should equal(JSString("validation failed"))
      (js / "description") should equal(JSString("instance data is not valid."))
    }
  }
}
