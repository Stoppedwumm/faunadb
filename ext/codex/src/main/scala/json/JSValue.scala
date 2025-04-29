package fauna.codex.json

import com.fasterxml.jackson.core.{ JsonFactory => JacksonFactory, JsonGenerator => JacksonGenerator, JsonEncoding }
import fauna.codex.json2.{ JSON, JSONParser }
import fauna.lang.syntax._
import fauna.util.Base64
import io.netty.buffer.{ ByteBuf, ByteBufOutputStream, ByteBufUtil, Unpooled }
import io.netty.util.AsciiString
import java.io._
import java.nio.ByteBuffer
import language.implicitConversions
import scala.collection.mutable.Builder
import scala.util.{ Failure, Success, Try }

object JS {

  val jsonFactory = new JacksonFactory

  def apply[T](t: T)(implicit enc: JsonEncoder[T]): JSValue = t match {
    case v: JSValue => v
    case t =>
      val b = new JsonScalarGenerator
      enc.encodeTo(b, t)
      b.result
  }

  def parse(buf: ByteBuf): JSValue =
    JSON.tryParse[JSValue](buf)(JSValue.JSONCodec2) match {
      case Success(js) => js
      case Failure(e)  => JSInvalid(JsonParseException(e.getMessage))
    }

  def parse(json: String): JSValue = parse(json.toUTF8Buf)
}

trait JSValue_1 {
  implicit def encodableToJSValue[T: JsonEncoder](t: T): JSValue = JS(t)

  implicit def encodableFieldToJSValue[T: JsonEncoder](t: (String, T)): (String, JSValue) = (t._1, JS(t._2))
}

trait JSValue_0 extends JSValue_1 {

  // Prioritize conversion of strings to speed up compiles

  implicit def stringToJSValue(v: String): JSValue = JSString(v)
  implicit def stringFieldToJSValue(t: (String, String)): (String, JSValue) = (t._1, JSString(t._2))

  implicit def asciiStringToJSValue(v: AsciiString): JSValue = JSRawString(v)

}

object JSValue extends JSValue_0 {
  implicit final object Decoder extends JsonDecoder[JSValue] {
    def decode(stream: JSValue) = stream match {
      case v: JSInvalid  => throw v.unexpectedError
      case v: JSNotFound => throw v.unexpectedError
      case v             => v
    }
  }

  implicit final object Encoder extends JsonEncoder[JSValue] {
    def encodeTo(stream: JsonGenerator, value: JSValue): Unit = stream.put(value)
  }

  implicit final object JSONCodec2 extends JSON.SwitchCodec[JSValue] {

    def encode(stream: JSON.Out, value: JSValue) =
      value match {
        case JSLong(l)      => stream.writeNumber(l)
        case JSDouble(d)    => stream.writeNumber(d)
        case JSTrue         => stream.writeBoolean(true)
        case JSFalse        => stream.writeBoolean(false)
        case JSNull         => stream.writeNull()
        case JSString(s)    => stream.writeString(s)
        case JSRawString(s) => stream.writeString(s)

        case a: JSArray =>
          stream.writeArrayStart()
          a.value foreach { encode(stream, _) }
          stream.writeArrayEnd()

        case o: JSObject =>
          stream.writeObjectStart()
          o.value foreach { case (k, v) => stream.writeString(k); encode(stream, v) }
          stream.writeObjectEnd()

        case v => throw v.unexpectedError
      }

    def readInt(l: Long, stream: JSON.In) = JSLong(l)
    def readBoolean(b: Boolean, stream: JSON.In) = if (b) JSTrue else JSFalse
    def readNull(stream: JSON.In) = JSNull
    def readDouble(d: Double, stream: JSON.In) = JSDouble(d)
    def readString(s: String, stream: JSON.In) = JSString(s)

    def readArrayStart(stream: JSON.In) = {
      val b = JSArray.newBuilder
      while (!stream.skipArrayEnd) b += stream.read(this)
      b.result()
    }

    def readObjectStart(stream: JSON.In) = {
      val b = JSObject.newBuilder
      while (!stream.skipObjectEnd) b += (stream.read(JSONParser.ObjectFieldNameSwitch) -> stream.read(this))
      b.result()
    }
  }
}

sealed abstract class JSValue {

  def isEmpty = false

  // decode

  def as[T](implicit dec: JsonDecoder[T]): T = dec.decode(this)

  def tryAs[T: JsonDecoder]: Try[T] = try Success(as[T]) catch JsonExceptionRescue { Failure(_) }

  def asOpt[T: JsonDecoder]: Option[T] = tryAs[T].toOption

  def is[T: JsonDecoder]: Boolean = asOpt[T].isDefined

  def orElse[T: JsonDecoder](alt: => T): T = asOpt[T] getOrElse alt

  @annotation.nowarn("cat=unused-params")
  def get(field: String): JSValue = JSInvalid(unexpectedError)

  @annotation.nowarn("cat=unused-params")
  def get(idx: Int): JSValue = JSInvalid(unexpectedError)

  def /(field: String): JSValue = get(field)

  def /(idx: Int): JSValue = get(idx)

  def apply[T: JsonDecoder](field: String) = get(field).as[T]

  def apply[T: JsonDecoder](idx: Int) = get(idx).as[T]

  // serialization

  def writeTo(out: JacksonGenerator): Unit

  def writeTo(out: OutputStream, pretty: Boolean): OutputStream = {
    if (pretty) {
      out.write(toPrettyString.toUTF8Bytes)
    } else {
      val gen = JS.jsonFactory.createGenerator(out, JsonEncoding.UTF8)
      writeTo(gen)
      gen.flush()
    }
    out
  }

  def writeTo(buf: ByteBuf, pretty: Boolean): ByteBuf = {
    if (pretty) {
      ByteBufUtil.writeUtf8(buf, toPrettyString)
    } else {
      writeTo(new ByteBufOutputStream(buf), pretty)
    }
    buf
  }

  def toByteBuf(pretty: Boolean): ByteBuf =
    writeTo(Unpooled.buffer, pretty)

  def toByteBuf: ByteBuf = toByteBuf(false)

  def toBase64Buffer: ByteBuffer = Base64.encodeUrlSafeBuffer(toByteBuf(false).nioBuffer)
  def toBase64Array: Array[Byte] = toBase64Buffer.toArray
  def toBase64String: String = toBase64Buffer.toISO8859

  @annotation.nowarn("cat=unused-params")
  private[json] def toPrettyString(width: Int): String = s"$toString\n"

  final def toPrettyString: String = toPrettyString(60)

  override def toString = toByteBuf(false).toUTF8String

  // error/unexpected value

  def unexpectedError: Throwable
}

final case class JSLong(value: Long) extends JSValue {
  def unexpectedError = JsonUnexpectedValue(s"Unexpected integer value $value.")

  def writeTo(out: JacksonGenerator) = out.writeNumber(value)
}

sealed trait JSNull extends JSValue

case object JSNull extends JSNull {

  override def isEmpty = true

  def unexpectedError = JsonUnexpectedValue(s"Unexpected null value.")

  def writeTo(out: JacksonGenerator) = out.writeNull
}

object JSBoolean {
  def apply(b: Boolean) = if (b) JSTrue else JSFalse

  def unapply(v: JSValue) =
    v match {
      case JSTrue  => SomeTrue
      case JSFalse => SomeFalse
      case _       => None
    }
}

sealed trait JSBoolean extends JSValue {
  val value: Boolean
}

case object JSTrue extends JSBoolean {
  val value = true

  def unexpectedError = JsonUnexpectedValue(s"Unexpected true value.")

  def writeTo(out: JacksonGenerator) = out.writeBoolean(true)
}

case object JSFalse extends JSBoolean {
  val value = false

  def unexpectedError = JsonUnexpectedValue(s"Unexpected false value.")

  def writeTo(out: JacksonGenerator) = out.writeBoolean(false)
}

final case class JSDouble(value: Double) extends JSValue {
  def unexpectedError = JsonUnexpectedValue(s"Unexpected float value $value.")

  def writeTo(out: JacksonGenerator) = out.writeNumber(value)
}

final case class JSString(value: String) extends JSValue {
  def unexpectedError = JsonUnexpectedValue(s"Unexpected string value '$value'.")

  def writeTo(out: JacksonGenerator) = out.writeString(value)
}

/**
  * A 'raw', 7-bit clean character array which will be surrounded by
  * double-quotes and written directly to output. No escaping or
  * charset conversion will be performed.
  */
final case class JSRawString(value: AsciiString) extends JSValue {
  def unexpectedError = JsonUnexpectedValue(s"Unexpected ASCII string value '$value'.")

  def writeTo(out: JacksonGenerator) =
    out.writeRawUTF8String(value.array, value.arrayOffset, value.length)
}

object JSArray {
  val empty = new JSArray(Nil)

  def newBuilder: Builder[JSValue, JSArray] = new Builder[JSValue, JSArray] {
    private val b = List.newBuilder[JSValue]

    def addOne(elem: JSValue) = { b += elem; this }
    def clear() = b.clear()
    def result() = new JSArray(b.result())
  }

  def apply(elems: JSValue*): JSArray =
    if (elems.isEmpty) empty else new JSArray(elems)

  def unapplySeq(v: JSValue) = v match {
    case v: JSArray => Some(v.value)
    case _          => None
  }

  implicit final object Decoder extends JsonDecoder[JSArray] {
    def decode(stream: JSValue) = stream match {
      case v: JSArray => v
      case v          => throw v.unexpectedError
    }
  }
}

final class JSArray(val value: Seq[JSValue]) extends JSValue {

  def :+(elem: JSValue) = new JSArray(value :+ elem)

  def +:(elem: JSValue) = new JSArray(elem +: value)

  def ++(o: JSArray) = new JSArray(value ++ o.value)

  def length = value.length

  override def isEmpty = value.isEmpty

  override def get(idx: Int): JSValue = try {
    value(idx)
  } catch {
    case _: IndexOutOfBoundsException => JSNotFound(JsonRangeAccessException(idx))
  }

  def unexpectedError = JsonUnexpectedValue(s"Unexpected array value.")

  def writeTo(out: JacksonGenerator) = {
    out.writeStartArray
    value foreach { _.writeTo(out) }
    out.writeEndArray
  }

  private[json] override def toPrettyString(width: Int) = {
    val valStr = value map { _.toPrettyString(width - 2).trim } mkString ",\n"

    val isScalarValue = value.size == 1 && (value.head match {
      case _: JSObject => false
      case _: JSArray  => false
      case _           => true
    })

    if (valStr.length < width || isScalarValue) {
      val compact = valStr.replaceAll("""\s+""", " ")
      s"[ $compact ]\n"
    } else {
      val lines = List.newBuilder[String]
      lines += "["
      valStr split "\n" foreach { l => lines += s"  $l" }
      lines += "]\n"

      lines.result() mkString "\n"
    }
  }

  override def equals(other: Any) = other match {
    case a: JSArray => value == a.value
    case _          => false
  }

  override def hashCode = value.hashCode
}

object JSObject {
  val empty = new JSObject(Nil)

  def newBuilder: Builder[(String, JSValue), JSObject] = new Builder[(String, JSValue), JSObject] {
    private val b = List.newBuilder[(String, JSValue)]

    def addOne(elem: (String, JSValue)) = { b += elem; this }
    def clear() = b.clear()
    def result() = JSObject(b.result(): _*)
  }

  def apply(pairs: (String, JSValue)*) =
    if (pairs.isEmpty) empty else new JSObject(pairs)

  def unapplySeq(v: JSValue) = v match {
    case v: JSObject => Some(v.value)
    case _           => None
  }

  implicit final object Decoder extends JsonDecoder[JSObject] {
    def decode(stream: JSValue) = stream match {
      case v: JSObject => v
      case v           => throw v.unexpectedError
    }
  }
}

final class JSObject(val value: Seq[(String, JSValue)]) extends JSValue {

  def :+(pair: (String, JSValue)) = new JSObject(value :+ pair)

  def +:(pair: (String, JSValue)) = new JSObject(pair +: value)

  def :++(o: JSObject) = new JSObject(value ++ o.value)

  def ++(o: JSObject) = (this - (o.value.map { _._1 }: _*)) :++ o

  def --(excluded: Set[String]) = new JSObject(value filterNot { t => excluded contains t._1 })

  def -(fields: String*) = this -- fields.toSet

  def keys = value.map { _._1 }

  override def isEmpty = value.isEmpty

  def merge(obj: JSObject) =
    if (isEmpty) obj else if (obj.isEmpty) this else ((this -- obj.keys.toSet) ++ obj)

  def patch(other: JSObject): JSObject =
    patch(other, false)

  def patch(other: JSObject, keepNull: Boolean): JSObject =
    if (other.isEmpty) this else {
      var remaining = other.value.toMap
      val rv = JSObject.newBuilder

      value foreach {
        case (key, v) =>
          (v, remaining.get(key)) match {
            case (prev, None)                   => rv += (key -> prev)
            case (prev, Some(_: JSNotFound))    => rv += (key -> prev)
            case (prev, Some(next: JSObject))   => rv += (key -> (prev orElse JSObject.empty).patch(next, keepNull))
            case (_, Some(JSNull)) if !keepNull => ()
            case (_, Some(next))                => rv += (key -> next)
          }

          remaining = remaining - key
      }

      other.value foreach {
        case (_, JSNull) if !keepNull                 => ()
        case (k, v: JSObject) if remaining contains k => rv += k -> (JSObject.empty.patch(v, keepNull))
        case (k, v) if remaining contains k           => rv += (k -> v)
        case _                                        => ()
      }

      rv.result()
    }

  def patch(path: List[String], other: JSValue): JSObject = {
    def mkPath(path: List[String], other: JSValue): JSValue =
      if (path.isEmpty) other else JSObject(path.head -> mkPath(path.tail, other))

    this patch mkPath(path, other).orElse(JSObject.empty)
  }

  def diffTo(other: JSObject): JSObject = {
    def diffTo0(l: JSObject, r: JSObject): JSValue = {
      val diff = l.diffTo(r)
      val allNulls = diff.value forall {
        case (_, JSNull) => true
        case _           => false
      }
      if (allNulls) JSNull else diff
    }

    if (isEmpty) {
      other
    } else {
      var diff = Seq.empty[(String, JSValue)]
      var remaining = other.value.toMap

      value foreach {
        case (field, value) =>
          val diffOpt =
            (value, remaining.get(field)) match {
              case (l: JSObject, Some(r: JSObject)) => Some(diffTo0(l, r))
              case (l, Some(r)) if l != r           => Some(r)
              case (_, Some(_))                     => None
              case (_, None)                        => Some(JSNull)
            }
          diff :++= diffOpt map { field -> _ }
          remaining -= field
      }

      diff :++= remaining
      JSObject(diff: _*)
    }
  }

  def replace(path: List[String], value: JSValue) = {
    def mkPath(path: List[String], value: JSValue): JSValue =
      if (path.isEmpty) value else JSObject(path.head -> mkPath(path.tail, value))

    def replace0(m: JSObject, segment: String, path: List[String], value: JSValue): JSObject = {
      var replaced = false
      val b = JSObject.newBuilder

      m.value foreach {
        case (k, v) if segment == k =>
          val newValue = v match {
            case m: JSObject if path.nonEmpty => replace0(m, path.head, path.tail, value)
            case _                            => mkPath(path, value)
          }

          replaced = true
          b += (k -> newValue)
        case t => b += t
      }

      if (!replaced) b += (segment -> mkPath(path, value))

      b.result()
    }

    replace0(this, path.head, path.tail, value)
  }

  def clear(path: List[String]) = patch(path, JSNull)

  def slice(fields: String*) = JSObject(value filter { t => fields contains t._1 }: _*)

  override def get(field: String): JSValue =
    value find { _._1 == field } match {
      case Some((_, v)) => v
      case None         => JSNotFound(JsonFieldAccessException(field))
    }

  override def get(idx: Int): JSValue = {
    val key = idx.toString
    value find { _._1 == key } match {
      case Some((_, v)) => v
      case None         => JSNotFound(JsonFieldAccessException(idx))
    }
  }

  def unexpectedError = JsonUnexpectedValue(s"Unexpected object value.")

  def writeTo(out: JacksonGenerator) = {
    out.writeStartObject
    value foreach { t =>
      out.writeFieldName(t._1)
      t._2.writeTo(out)
    }
    out.writeEndObject
  }

  private[json] override def toPrettyString(width: Int) = {
    val valStr = value map { case (k, v) => s""""$k": ${v.toPrettyString(width - 2).trim}""" } mkString ",\n"

    val isScalarValue = value.size == 1 && (value.head match {
      case (_, _: JSObject) => false
      case (_, _: JSArray)  => false
      case _                => true
    })

    if (valStr.length < width || isScalarValue) {
      val compact = valStr.replaceAll("""\s+""", " ")
      s"{ $compact }\n"
    } else {
      val lines = List.newBuilder[String]
      lines += "{"
      valStr split "\n" foreach { l => lines += s"  $l" }
      lines += "}\n"

      lines.result() mkString "\n"
    }
  }

  override def equals(other: Any) = other match {
    case j: JSObject => value == j.value
    case _           => false
  }

  override def hashCode = value.hashCode
}

final case class JSNotFound(error: Throwable) extends JSValue {

  override def toString = s"JSNotFound($error)"

  override def isEmpty = true

  override def get(field: String): JSValue = this

  override def get(idx: Int): JSValue = this

  def unexpectedError = error

  def writeTo(out: JacksonGenerator) = throw error
}

final case class JSInvalid(error: Throwable) extends JSValue {

  override def toString = s"JSInvalid($error)"

  override def get(field: String): JSValue = this

  override def get(idx: Int): JSValue = this

  def unexpectedError = error

  def writeTo(out: JacksonGenerator) = throw error
}

final case class JSRawValue(string: String) extends JSValue {
  def unexpectedError = JsonUnexpectedValue(s"Unexpected raw value.")
  def writeTo(out: JacksonGenerator) = out.writeRawValue(string)
}
