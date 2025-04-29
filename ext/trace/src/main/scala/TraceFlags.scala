package fauna.trace

import fauna.codex.cbor._
import fauna.codex.json._
import io.netty.util.AsciiString

object TraceFlags {

  implicit val codec =
    CBOR.AliasCodec[TraceFlags, Byte]({ new TraceFlags(_) }, { _.bits })

  implicit object JSEncoder extends JsonEncoder[TraceFlags] {
    def encodeTo(stream: JsonGenerator, value: TraceFlags): Unit =
      stream.put(value.toHexString)
  }

  val Default = new TraceFlags(0x00)

  /**
    * The size in chars of the TraceFlags component of a W3C traceparent
    * header.
    */
  val W3CSize = Hex.CharsPerByte

  /**
    * The char offset in a compliant W3C traceparent header which
    * begins the Trace Flags segment.
    */
  val W3COffset = 53

  /**
    * Flag indicating whether sampling - and associated overhead -
    * should be enabled in a span.
    */
  val IsSampled = 0x01

  def fromHexString(str: CharSequence, offset: Int = 0): TraceFlags = {
    new TraceFlags(Hex.toByte(str, offset))
  }
}

/**
  * An 8-bit field that controls tracing flags such as sampling, trace
  * level, etc. It is commonly represented as a hex byte, for example
  * 01.
  *
  * Currently, only a single flag (isSampled) is supported, indicated
  * by the least-significant bit. When set, this bit indicates that
  * the caller may have recorded trace data.
  */
final class TraceFlags(val bits: Byte) extends AnyVal {

  override def toString =
    s"TraceFlags($toHexString)"

  def toHexString: AsciiString = {
    val buf = new Array[Char](Hex.CharsPerByte)
    toHexString(buf, 0)
    new AsciiString(buf)
  }

  def toHexString(buf: Array[Char], offset: Int): Unit = {
    Hex.toHex(bits, buf, offset)
  }

  def withSampled(enabled: Boolean): TraceFlags =
    if (enabled) {
      new TraceFlags((bits | TraceFlags.IsSampled).toByte)
    } else {
      new TraceFlags((bits & ~TraceFlags.IsSampled).toByte)
    }

  /**
    * Returns true if sampling - and associated overhead - should be
    * enabled in a span.
    */
  def isSampled: Boolean =
    isSet(TraceFlags.IsSampled)

  private def isSet(flag: Int): Boolean =
    (bits & flag) == flag
}
