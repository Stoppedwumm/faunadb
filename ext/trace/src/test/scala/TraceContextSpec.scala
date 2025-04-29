package fauna.trace.test

import fauna.trace._
import io.netty.util.AsciiString

class TraceContextSpec extends Spec {
  "TraceContext" - {
    "toHTTPHeader" in {
      val w3c = new AsciiString("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")

      val traceID = TraceID.fromHexString(w3c, TraceID.W3COffset)
      val spanID = SpanID.fromHexString(w3c, SpanID.W3COffset)
      val flags = TraceFlags.fromHexString(w3c, TraceFlags.W3COffset)

      TraceContext(traceID, spanID, flags).toHTTPHeader should equal (w3c)
    }

    "fromHTTPHeader" in {
      val w3c = new AsciiString("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")

      val ctx = TraceContext.fromHTTPHeader(w3c).get

      ctx.traceID should equal (TraceID.fromHexString(w3c, TraceID.W3COffset))
      ctx.spanID should equal (SpanID.fromHexString(w3c, SpanID.W3COffset))
      ctx.isSampled should be (true)
    }

    "allows short trace IDs" in {
      val short = new AsciiString("00-0000000000000000a3ce929d0e0e4736-00f067aa0ba902b7-01")

      val ctx = TraceContext.fromHTTPHeader(short).get

      ctx.traceID should equal (TraceID.fromHexString(short, TraceID.W3COffset))
      ctx.spanID should equal (SpanID.fromHexString(short, SpanID.W3COffset))
      ctx.isSampled should be (true)
    }

    "allows unsampled spans" in {
      val unsampled = new AsciiString("00-0000000000000000a3ce929d0e0e4736-00f067aa0ba902b7-00")

      val ctx = TraceContext.fromHTTPHeader(unsampled).get

      ctx.traceID should equal (TraceID.fromHexString(unsampled, TraceID.W3COffset))
      ctx.spanID should equal (SpanID.fromHexString(unsampled, SpanID.W3COffset))
      ctx.isSampled should be (false)
    }

    "rejects invalid IDs" in {
      val trace = new AsciiString("00-00000000000000000000000000000000-00f067aa0ba902b7-01")
      val span = new AsciiString("00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01")
      val shortTrace = new AsciiString("00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01")
      val shortSpan = new AsciiString("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b-01")
      val longTrace = new AsciiString("00-4bf92f3577b34da6a3ce929d0e0e47361-00f067aa0ba902b7-01")
      val longSpan = new AsciiString("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b71-01")

      TraceContext.fromHTTPHeader(trace).isFailure should be (true)
      TraceContext.fromHTTPHeader(span).isFailure should be (true)
      TraceContext.fromHTTPHeader(shortTrace).isFailure should be (true)
      TraceContext.fromHTTPHeader(shortSpan).isFailure should be (true)
      TraceContext.fromHTTPHeader(longTrace).isFailure should be (true)
      TraceContext.fromHTTPHeader(longSpan).isFailure should be (true)
    }

  }
}
