package fauna.model.test

import fauna.atoms._
import fauna.auth.Auth
import fauna.codex.json2.JSON
import fauna.lang.Timestamp
import fauna.model.runtime.fql2.serialization._
import fauna.model.runtime.fql2.FQLInterpreter
import fauna.repo.values.Value
import fauna.util.Base64
import fql.ast.Span
import java.time.LocalDate
import org.scalactic.source.Position
import scala.collection.immutable.{ ArraySeq, SeqMap }

class FQL2ValueDecoderSpec(val format: ValueFormat) extends FQL2Spec {
  private def decodeResult(json: String, auth: Auth) = {
    val bytes = json.getBytes()
    val undecided = JSON.parse[UndecidedValue](bytes)
    ctx ! undecided.toValue(new FQLInterpreter(auth), format)
  }

  def decode(json: String, auth: Auth = Auth.forScope(ScopeID.RootID))(
    implicit pos: Position) = {
    decodeResult(json, auth) match {
      case Right(v)    => v
      case Left(error) => fail(s"unexpected error: $error")
    }
  }
  def decodeErr(json: String, auth: Auth = Auth.forScope(ScopeID.RootID))(
    implicit pos: Position) = {
    decodeResult(json, auth) match {
      case Right(v)    => fail(s"shouldn't succeed with value $v")
      case Left(error) => error.message
    }
  }
}

class FQL2ValueDecoderSimpleSpec extends FQL2ValueDecoderSpec(ValueFormat.Simple) {
  "FQL2ValueDecoder in simple format" - {
    "parses a long" in {
      decode("39") shouldEqual Value.Long(39)
    }
    "parses a string" in {
      decode("\"hilo\"") shouldEqual Value.Str("hilo")
    }
    "parses a double" in {
      decode("3.39E-9") shouldEqual Value.Double(3.39e-9)
    }
    "parses booleans" in {
      decode("true") shouldEqual Value.True
      decode("false") shouldEqual Value.False
    }
    "parses null" in {
      decode("null") shouldEqual Value.Null(Span.Null)
    }
    "parses array" in {
      val json = """["hello","hello","moon"]"""
      decode(json) shouldEqual Value.Array(
        ArraySeq(Value.Str("hello"), Value.Str("hello"), Value.Str("moon")))
    }
    "parses struct" in {
      val json = """{"one":"two","three":"four","five":{"a":"b","c":"d"}}"""
      decode(json) shouldEqual Value.Struct(
        SeqMap(
          ("one", Value.Str("two")),
          ("three", Value.Str("four")),
          (
            "five",
            Value.Struct(
              SeqMap(
                ("a", Value.Str("b")),
                ("c", Value.Str("d"))
              )))
        )
      )
    }

    "parses tagged struct as a normal struct" in {
      val json = """{"@object":{"a":"b"}}"""
      decode(json) shouldEqual Value.Struct(
        SeqMap(
          "@object" ->
            Value.Struct(
              SeqMap(
                "a" -> Value.Str("b")
              )))
      )
    }
  }
}

class FQL2ValueDecoderTaggedSpec extends FQL2ValueDecoderSpec(ValueFormat.Tagged) {
  "FQL2ValueDecoder in tagged format" - {
    // Simple types should be the same in tagged format

    "parses a long" in {
      decodeErr("39") shouldEqual "numbers must be tagged"
    }
    "parses a string" in {
      decode("\"hilo\"") shouldEqual Value.Str("hilo")
    }
    "parses a double" in {
      decodeErr("3.39E-9") shouldEqual "numbers must be tagged"
    }
    "parses booleans" in {
      decode("true") shouldEqual Value.True
      decode("false") shouldEqual Value.False
    }
    "parses null" in {
      decode("null") shouldEqual Value.Null(Span.Null)
    }
    "parses array" in {
      val json = """["hello","hello","moon"]"""
      decode(json) shouldEqual Value.Array(
        ArraySeq(Value.Str("hello"), Value.Str("hello"), Value.Str("moon")))
    }

    "parses struct" in {
      val json = """{"one":"two","three":"four","five":{"a":"b","c":"d"}}"""
      decode(json) shouldEqual Value.Struct(
        SeqMap(
          ("one", Value.Str("two")),
          ("three", Value.Str("four")),
          (
            "five",
            Value.Struct(
              SeqMap(
                ("a", Value.Str("b")),
                ("c", Value.Str("d"))
              )))
        )
      )
    }

    // And now we test tagged types

    "parses int" in {
      val json = """{"@int":"1234"}"""
      decode(json) shouldEqual Value.Int(1234)
    }
    "errors on invalid int" in {
      val json1 = """{"@int":"a"}"""
      decodeErr(json1) shouldBe "invalid integer"

      val json2 = """{"@int":"4294967296"}"""
      decodeErr(json2) shouldBe "invalid integer"
    }

    "parses long" in {
      val json = """{"@long":"1234"}"""
      decode(json) shouldEqual Value.Long(1234)
    }
    "errors on invalid long" in {
      val json1 = """{"@long":"a"}"""
      decodeErr(json1) shouldBe "invalid long"

      val json2 = """{"@long":"18446744073709551616"}"""
      decodeErr(json2) shouldBe "invalid long"
    }

    "parses double" in {
      val json = """{"@double":"12.34"}"""
      decode(json) shouldEqual Value.Double(12.34)
    }
    "errors on invalid double" in {
      val json = """{"@double":"a"}"""
      decodeErr(json) shouldBe "invalid double"
    }

    "parses date" in {
      val json = """{"@date":"2022-12-13"}"""
      decode(json) shouldEqual Value.Date(LocalDate.of(2022, 12, 13))
    }
    "errors on invalid date" in {
      val json = """{"@date":"foo"}"""
      decodeErr(json) shouldBe "invalid date"
    }

    "parses time" in {
      val json = """{"@time":"1970-01-01T00:01:03Z"}"""
      decode(json) shouldEqual Value.Time(Timestamp(63, 0))
    }
    "errors on invalid time" in {
      val json = """{"@time":"foo"}"""
      decodeErr(json) shouldBe "invalid time"
    }
    "handles time without a timezone" in {
      // notice the lack of Z
      val json = """{"@time":"1970-01-01T00:01:03"}"""
      decodeErr(json) shouldBe "invalid time"
    }

    "parses bytes" in {
      val bytes = Array[Byte](1, 2, 3)

      val json = s"""{"@bytes":"${Base64.encodeStandard(bytes)}"}"""
      decode(json) shouldEqual Value.Bytes(ArraySeq.unsafeWrapArray(bytes))
    }
    "errors on invalid bytes" in {
      val json = """{"@bytes":"1"}"""
      decodeErr(json) shouldBe "invalid bytes"
    }

    "parses tagged struct" in {
      val json =
        """{"@object":{"one":"two","three":"four","five":{"a":"b","c":"d"}}}"""
      decode(json) shouldEqual Value.Struct(
        SeqMap(
          ("one", Value.Str("two")),
          ("three", Value.Str("four")),
          (
            "five",
            Value.Struct(
              SeqMap(
                ("a", Value.Str("b")),
                ("c", Value.Str("d"))
              )))
        )
      )

      val json2 = """{"@object":{"@this is a key":"foo"}}"""
      decode(json2) shouldEqual Value.Struct(
        SeqMap(
          "@this is a key" -> Value.Str("foo")
        )
      )
    }

    "errors on invalid tag" in {
      val json1 = """{"@foo":"a"}"""
      decodeErr(json1) shouldBe "invalid tag '@foo'"

      val json2 = """{"@int":"3","@long":4}"""
      decodeErr(json2) shouldBe "too many keys in tagged value"
    }

    for (tag <- Seq("@doc", "@ref")) {
      val auth = newDB

      // We need the collection to exist before we can reference it
      val userColl =
        evalOk(auth, "Collection.create({ name: 'User' })")
          .asInstanceOf[Value.Doc]

      s"refs with $tag" in {

        decode(
          s"""{"$tag":{"coll":{"@mod":"Collection"},"name":"User"}}""",
          auth) shouldEqual userColl
        decodeErr(
          s"""{"$tag":{"coll":{"@mod":"Collection"},"name":"UserTypo"}}""",
          auth) shouldEqual "no such schema object 'UserTypo'"

        decode(
          s"""{"$tag":{"coll":{"@mod":"User"},"id":"3"}}""",
          auth) shouldEqual Value.Doc(
          DocID(SubID(3), CollectionID(userColl.id.subID.toLong)))

        // wrong types
        decodeErr(
          s"""{"$tag":true}""",
          auth) shouldEqual s"$tag requires an object value"
        decodeErr(
          s"""{"$tag":{}}""",
          auth) shouldEqual s"$tag field 'coll' is required"
        decodeErr(
          s"""{"$tag":{"coll":true}}""",
          auth) shouldEqual s"$tag field 'coll' requires a module value"

        // missing id/name
        decodeErr(
          s"""{"$tag":{"coll":{"@mod":"User"}}}""",
          auth) shouldEqual s"$tag field 'id' is required"
        decodeErr(
          s"""{"$tag":{"coll":{"@mod":"Collection"}}}""",
          auth) shouldEqual s"$tag field 'name' is required"

        // more wrong types
        decodeErr(
          s"""{"$tag":{"coll":{"@mod":"User"},"id":true}}""",
          auth) shouldEqual s"$tag field 'id' requires a string value"
        decodeErr(
          s"""{"$tag":{"coll":{"@mod":"User"},"id":"foo"}}""",
          auth) shouldEqual s"$tag field 'id' must be an integer"
        decodeErr(
          s"""{"$tag":{"coll":{"@mod":"Collection"},"name":true}}""",
          auth) shouldEqual s"$tag field 'name' requires a string value"

        val doc = evalOk(auth, "User.create({ a: 2 })").to[Value.Doc]
        val id = doc.id.subID.toLong

        decode(
          s"""{"$tag":{"coll":{"@mod":"User"},"id":"$id"}}""",
          auth) shouldEqual doc

        decode(s"""{"$tag":{"coll":{"@mod":"Key"},"id":"123"}}""") shouldBe Value
          .Doc(DocID(SubID(123), KeyID.collID))
        decode(
          s"""{"$tag":{"coll":{"@mod":"Credentials"},"id":"123"}}""") shouldBe Value
          .Doc(DocID(SubID(123), CredentialsID.collID))
        decode(s"""{"$tag":{"coll":{"@mod":"Token"},"id":"123"}}""") shouldBe Value
          .Doc(DocID(SubID(123), TokenID.collID))
      }

      s"old string refs with $tag" in {
        decode(s"""{"$tag":"User:3"}""", auth) shouldEqual Value.Doc(
          DocID(SubID(3), CollectionID(userColl.id.subID.toLong)))

        decode(s"""{"$tag":"Key:123"}""") shouldBe Value
          .Doc(DocID(SubID(123), KeyID.collID))
        decode(s"""{"$tag":"Credentials:123"}""") shouldBe Value
          .Doc(DocID(SubID(123), CredentialsID.collID))
        decode(s"""{"$tag":"Token:123"}""") shouldBe Value
          .Doc(DocID(SubID(123), TokenID.collID))
      }
    }
  }
}
