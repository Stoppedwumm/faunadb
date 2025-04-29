package fauna.trace.test

import fauna.trace._
import io.netty.util.AsciiString
import scala.util.Random

class TraceIDSpec extends Spec {
  "TraceID" - {
    "fromHexString" in {
      val w3c = new AsciiString("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
      val w3cID = TraceID.fromHexString(w3c, TraceID.W3COffset)
      w3cID should equal (TraceID.fromHexString("4bf92f3577b34da6a3ce929d0e0e4736"))
    }

    "roundtrips" in {
      val id = TraceID.randomID(Random)
      val hex = id.toHexString

      TraceID.fromHexString(hex) should equal (id)
    }
  }
}
