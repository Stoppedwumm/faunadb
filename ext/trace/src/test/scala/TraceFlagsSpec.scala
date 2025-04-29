package fauna.trace.test

import fauna.trace._
import io.netty.util.AsciiString

class TraceFlagsSpec extends Spec {
  "TraceFlags" - {
    "toHexString" in {
      TraceFlags.Default.toHexString should equal (new AsciiString("00"))
    }

    "fromHexString" in {
      TraceFlags.fromHexString("00") should equal (TraceFlags.Default)

      val w3c = new AsciiString("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
      val flags = TraceFlags.fromHexString(w3c, TraceFlags.W3COffset)
      flags should equal (new TraceFlags(0x01.toByte))
      flags.isSampled should equal (true)
    }

    "isSampled" in {
      TraceFlags.Default.isSampled should equal (false)
      new TraceFlags(TraceFlags.IsSampled.toByte).isSampled should equal (true)
    }

    "withSampled" in {
      TraceFlags.Default.withSampled(true).isSampled should equal (true)
      TraceFlags.Default.withSampled(false).isSampled should equal (false)
    }
  }
}
