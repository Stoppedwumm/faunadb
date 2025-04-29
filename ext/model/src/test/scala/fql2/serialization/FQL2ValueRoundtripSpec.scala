package fauna.model.test

import fauna.atoms.ScopeID
import fauna.auth.Auth
import fauna.codex.json2.{ JSON, JSONWriter }
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.runtime.fql2.{ FQLInterpreter, Result }
import fauna.model.runtime.fql2.serialization._
import fauna.repo.values.Value
import fauna.util.Base64
import io.netty.buffer.ByteBufAllocator
import java.time.Instant
import java.time.LocalDate
import scala.collection.immutable.{ ArraySeq, SeqMap }

// Tests values being encoded to JSON, then decoded again, and ensures that
// they have the same value.

class FQL2ValueRoundtripSpec extends FQL2Spec {
  private val alloc = ByteBufAllocator.DEFAULT

  private def encode(value: Value, auth: Auth = Auth.forScope(ScopeID.RootID)) = {
    val buf = alloc.buffer
    val out = JSONWriter(buf)

    val intp = new FQLInterpreter(auth)

    val mvalue = ctx ! FQL2ValueMaterializer.materialize(intp, value) match {
      case Result.Ok(v)    => v
      case Result.Err(err) => fail(s"unexpected error materializing: $err")
    }

    FQL2ValueEncoder.encode(
      ValueFormat.Tagged,
      out,
      mvalue,
      Timestamp(Instant.now()))
    buf.toUTF8String
  }

  private def decodeResult(json: String, auth: Auth) = {
    val bytes = json.getBytes()
    val undecided = JSON.parse[UndecidedValue](bytes)
    ctx ! undecided.toValue(new FQLInterpreter(auth), ValueFormat.Tagged)
  }

  private def decode(json: String, auth: Auth = Auth.forScope(ScopeID.RootID)) = {
    decodeResult(json, auth) match {
      case Right(v)    => v
      case Left(error) => fail(s"unexpected error: $error")
    }
  }

  private def roundtripJSON(json: String) = {
    encode(decode(json)) shouldEqual json
  }

  private def roundtrip(
    value: Value,
    json: String,
    auth: Auth = Auth.forScope(ScopeID.RootID)) = {
    encode(value, auth) shouldEqual json
    decode(json, auth) shouldEqual value

    // both tests should always hold true, as these undo each other
    encode(decode(json, auth), auth) shouldEqual json
    decode(encode(value, auth), auth) shouldEqual value
  }

  "FQL2RoundtripSpec" - {
    "int" in {
      roundtrip(Value.Int(3), """{"@int":"3"}""")
      roundtrip(Value.Int(-1), """{"@int":"-1"}""")
    }
    "long" in {
      roundtrip(Value.Int(3), """{"@int":"3"}""")
    }
    "double" in {
      roundtrip(Value.Double(22.3), """{"@double":"22.3"}""")
      roundtrip(Value.Double(Double.PositiveInfinity), """{"@double":"Infinity"}""")
      roundtrip(Value.Double(Double.NegativeInfinity), """{"@double":"-Infinity"}""")

      // NaN != NaN, so this only roundtrips for JSON
      roundtripJSON("""{"@double":"NaN"}""")
    }
    "string" in {
      roundtrip(Value.Str("hilo"), "\"hilo\"")
      roundtrip(Value.Str(""), "\"\"")
    }
    "boolean" in {
      roundtrip(Value.True, "true")
      roundtrip(Value.False, "false")
    }
    "array" in {
      roundtrip(Value.Array(ArraySeq()), "[]")
      roundtrip(Value.Array(ArraySeq(Value.Str("hilo"))), """["hilo"]""")
    }
    "object" in {
      roundtrip(Value.Struct(SeqMap("hi" -> Value.Str("foo"))), """{"hi":"foo"}""")
    }
    "object with @" in {
      roundtrip(
        Value.Struct(SeqMap("@foo" -> Value.Str("bar"), "three" -> Value.Int(5))),
        s"""{"@object":{"@foo":"bar","three":{"@int":"5"}}}""")
    }
    "time" in {
      val now = Instant.now()

      roundtrip(Value.Time(Timestamp(now)), s"""{"@time":\"${now.toString}\"}""")
    }
    "date" in {
      val date = LocalDate.of(2022, 12, 14)

      roundtrip(Value.Date(date), s"""{"@date":\"2022-12-14\"}""")
    }
    "bytes" in {
      val bytes = Array[Byte](1, 2, 3)

      roundtrip(
        Value.Bytes(ArraySeq.unsafeWrapArray(bytes)),
        s"""{"@bytes":"${Base64.encodeStandard(bytes)}"}""")
    }
    "docs" in pendingUntilFixed {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: 'User' })")
      val doc = evalOk(auth, "User.create({})").asInstanceOf[Value.Doc]
      val id = doc.id.subID.toLong

      roundtrip(doc, s"""{"@doc":{"id":"$id","coll":{"@mod":"User"}}}}""", auth)
    }
  }
}
