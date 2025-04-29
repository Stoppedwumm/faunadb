package fauna.trace

import fauna.codex.cbor._
import fauna.codex.json._
import io.netty.util.AsciiString
import java.lang.{ Long => JLong }
import scala.util.Random

object SpanID {

  implicit val codec = CBOR.AliasCodec[SpanID, Long]({ new SpanID(_) }, _.id)

  implicit object JSEncoder extends JsonEncoder[SpanID] {
    def encodeTo(stream: JsonGenerator, value: SpanID): Unit =
      stream.put(value.toHexString)
  }

  /**
    * The size in chars of the SpanID component of a W3C traceparent
    * header.
    */
  val W3CSize = Hex.CharsPerLong

  /**
    * The char offset in a compliant W3C traceparent header which
    * begins the Span ID segment.
    */
  val W3COffset = 36

  /**
    * Generates a valid SpanID with a random value.
    */
  def randomID(implicit rnd: Random): SpanID = {
    var id = 0L

    while (id == 0) {
      id = rnd.nextLong()
    }

    new SpanID(id)
  }

  /**
    * Decodes a hexidecimal-encoded span ID.
    */
  def fromHexString(str: CharSequence, offset: Int = 0): SpanID = {
    val id = JLong.parseUnsignedLong(str, offset, offset + Hex.CharsPerLong, 16)

    if (id == 0) {
      throw new IllegalArgumentException("SpanID must have at least one non-zero byte")
    } else {
      new SpanID(id)
    }
  }
}

/**
  * A Span ID is an 64 bit identifier for some segment of a trace. it
  * is commonly represented as an 8 byte array, for example
  * 00f067aa0ba902b7.
  *
  * An all-zero value is invalid.
  */
final class SpanID(val id: Long) extends AnyVal {
  override def toString =
    s"SpanID($toHexString)"

  /**
    * Returns the value of this span ID encoded in hexidecimal.
    */
  def toHexString: AsciiString = {
    val buf = new Array[Char](Hex.CharsPerLong)
    toHexString(buf, 0)
    new AsciiString(buf)
  }

  def toHexString(buf: Array[Char], offset: Int): Unit = {
    Hex.toHex(id, buf, offset)
  }
}
