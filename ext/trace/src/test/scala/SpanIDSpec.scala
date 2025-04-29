package fauna.trace.test

import fauna.trace._
import io.netty.util.AsciiString
import scala.util.Random

class SpanIDSpec extends Spec {
  "SpanID" - {
    "fromHexString" in {
      val w3c = new AsciiString("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
      val w3cID = SpanID.fromHexString(w3c, SpanID.W3COffset)
      w3cID should equal (SpanID.fromHexString("00f067aa0ba902b7"))
    }

    "roundtrips" in {
      val id = SpanID.randomID(Random)
      val hex = id.toHexString

      SpanID.fromHexString(hex) should equal (id)
    }
  }
}
