package fauna.trace

import fauna.codex.cbor._
import io.netty.util.AsciiString
import java.util.concurrent.ThreadLocalRandom
import scala.util.{ Random, Try }

object TraceContext {
  implicit val codec = CBOR.RecordCodec[TraceContext]

  /**
    * The size in chars of a compliant W3C traceparent header
    *
    * [ version '-' traceID '-' spanID '-' flags ]
    */
  val W3CSize =
    Hex.CharsPerByte + 1 + TraceID.W3CSize + 1 + SpanID.W3CSize + 1 + TraceFlags.W3CSize

  def fromHTTPHeader(str: CharSequence): Try[TraceContext] =
    Try {
      require(str.length == W3CSize)

      val traceID = TraceID.fromHexString(str, TraceID.W3COffset)
      val spanID = SpanID.fromHexString(str, SpanID.W3COffset)
      val flags = TraceFlags.fromHexString(str, TraceFlags.W3COffset)

      TraceContext(traceID, spanID, flags)
    }

  def random()(implicit rng: Random = ThreadLocalRandom.current()): TraceContext =
    new TraceContext(
      TraceID.randomID,
      SpanID.randomID,
      TraceFlags.Default
    )
}

/**
  * A trace context contains the trace state that must be propagated to
  * child spans and across process boundaries.
  *
  * See https://w3c.github.io/trace-context/
  */
// XXX: add support for tracestate?
// https://github.com/openzipkin/b3-propagation#single-header
final case class TraceContext(
  traceID: TraceID,
  spanID: SpanID,
  flags: TraceFlags) {

  // this field is extremely hot in Sampler.shouldSample
  val isSampled: Boolean =
    flags.isSampled

  def withSampled(enabled: Boolean): TraceContext =
    copy(flags = flags.withSampled(enabled))

  /**
    * Returns a W3C compliant "traceparent" header value.
    */
  def toHTTPHeader: AsciiString = {
    val buf = new Array[Char](TraceContext.W3CSize)
    Hex.toHex(0L, buf, 0)
    buf(2) = '-'
    traceID.toHexString(buf, TraceID.W3COffset)
    buf(SpanID.W3COffset - 1) = '-'
    spanID.toHexString(buf, SpanID.W3COffset)
    buf(TraceFlags.W3COffset - 1) = '-'
    flags.toHexString(buf, TraceFlags.W3COffset)
    new AsciiString(buf)
  }

  override def toString =
    s"TraceContext(traceID = $traceID, spanID = $spanID, flags = $flags)"
}
