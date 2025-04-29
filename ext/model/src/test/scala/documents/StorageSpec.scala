package fauna.model.test

import fauna.atoms.APIVersion
import fauna.auth.Auth
import fauna.codex.json._
import fauna.codex.json2.JSON
import fauna.lang.Timestamp
import fauna.lang.clocks.Clock
import fauna.model._
import fauna.repo.test.CassandraHelper
import fauna.util.Base64

class StorageSpec extends Spec {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")

  "StorageSpec" - {
    "round-trips custom types" in {
      val scope = ctx ! newScope
      val auth = Auth.forScope(scope)

      ctx ! mkCollection(auth, MkObject("name" -> "types"))

      val bytes = Array[Byte](0x1, 0x2, 0x3, 0x4)
      val base64 = Base64.encodeStandard(bytes)

      val data = MkObject(
        "data" -> MkObject(
          "time" -> TS("1970-01-01T00:00:00+00:00"),
          "date" -> Date("1970-01-01"),
          "bytes" -> Bytes(bytes),
          "uuid" -> UUID("4b928ddc-e22d-46ed-9710-6f336bf3f23b")))
      val inst = ctx ! mkDoc(auth, "types", data)

      val res = ctx ! runQuery(auth, Clock.time, Get(inst.refObj))
      val jsBuf = ctx ! RenderContext.render(auth, APIVersion.Default, Timestamp.Epoch, res, false)
      val js = JSON.parse[JSValue](jsBuf)

      (js / "data" / "time") should equal (JSObject("@ts" -> "1970-01-01T00:00:00Z"))
      (js / "data" / "date") should equal (JSObject("@date" -> "1970-01-01"))
      (js / "data" / "bytes") should equal (JSObject("@bytes" -> base64))
      (js / "data" / "uuid") should equal (JSObject("@uuid" -> "4b928ddc-e22d-46ed-9710-6f336bf3f23b"))
    }
  }
}
