package fauna.tools.error

import fauna.api.test.Spec
import fauna.codex.json.JSObject

class ErrorMessageSpec extends Spec {

  "ErrorMessage on Default & Join message" - {
    val defaultAndJoin = Seq(ErrorMessage.Common, ErrorMessage.Join)

    "return unauthorized message for Unauthorized" in {
      defaultAndJoin foreach {
        message =>
          message.toString(Error.Unauthorized) shouldBe ErrorMessage.unauthorized
      }
    }

    "return description for BadRequest" in {
      defaultAndJoin foreach {
        message =>
          message.toString(Error.BadRequest("bad")) shouldBe "bad"
      }
    }

    "return cluster uninitialized message for ClusterUninitialized when status code is 503" in {
      defaultAndJoin foreach {
        message =>
          message.toString(Error.ClusterUninitialized("not init", 503, JSObject())) shouldBe
            ErrorMessage.clusterUninitialized("not init")
      }
    }

    "return unknown message for ClusterUninitialized when status code is not 503" in {
      defaultAndJoin foreach {
        message =>
          message.toString(Error.ClusterUninitialized("not init", 0, JSObject())) shouldBe
            ErrorMessage.unknown(JSObject(), Some("not init"))
      }
    }

    "return unknown message for Unknown" in {
      defaultAndJoin foreach {
        message =>
          message.toString(Error.Unknown("don't know", "something", 0, JSObject())) shouldBe
            ErrorMessage.unknown(JSObject(), Some("something"))
      }
    }
  }

  "ErrorMessage on Join message" - {

    "return return hello timeout message on Unknown with 'hello timeout' code" in {
      ErrorMessage.Join.toString(Error.Unknown("hello timeout", "timed out", 0, JSObject())) shouldBe
        ErrorMessage.helloTimeout
    }
  }
}
