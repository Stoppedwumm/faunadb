package fauna.repo.test

import fauna.repo.values.{ InvalidCast, Value }

class ValueCodecSpec extends Spec {

  "ValueDecoder" - {
    "decode value" in {
      Value.Str("str").as[Value] shouldBe Value.Str("str")
    }

    "decode string" in {
      Value.Str("str").as[String] shouldBe "str"
      Value.Str("str").asOpt[String] shouldBe Some("str")

      an[InvalidCast.type] shouldBe thrownBy {
        Value.Int(10).as[String]
      }
    }

    "decode boolean" in {
      Value.Boolean(true).as[Boolean] shouldBe true
      Value.Boolean(true).asOpt[Boolean] shouldBe Some(true)

      an[InvalidCast.type] shouldBe thrownBy {
        Value.Int(10).as[Boolean]
      }
    }

    "decode struct" in {
      val struct = Value.Struct("foo" -> Value.Str("bar"))

      struct.as[Map[String, Value]] shouldBe Map("foo" -> Value.Str("bar"))
      struct.as[Map[String, String]] shouldBe Map("foo" -> "bar")

      an[InvalidCast.type] shouldBe thrownBy {
        struct.as[Map[String, Boolean]]
      }
    }

    "decode array" in {
      val array = Value.Array(Value.Str("foo"), Value.Str("bar"))

      array.as[Vector[String]] shouldBe Vector("foo", "bar")

      an[InvalidCast.type] shouldBe thrownBy {
        array.as[Vector[Boolean]]
      }
    }
  }
}
