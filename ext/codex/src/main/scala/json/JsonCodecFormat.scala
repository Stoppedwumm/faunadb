package fauna.codex.json

import fauna.codex.builder._
import fauna.lang.syntax._
import fauna.util.Base64
import io.netty.buffer.{ ByteBuf, Unpooled }

sealed trait JsonGenerator {
  def put(value: JSValue): Unit

  def putFieldName(name: String): Unit = ()
}

final class JsonScalarGenerator extends JsonGenerator {
  private[this] var _result: JSValue = null

  def put(value: JSValue) = _result = value

  def result = _result
}

final class JsonArrayGenerator extends JsonGenerator {
  private[this] val b = JSArray.newBuilder

  def put(value: JSValue) = b += value

  def result = b.result()
}

final class JsonObjectGenerator extends JsonGenerator {
  private[this] val b = JSObject.newBuilder
  private[this] var key: String = null

  override def putFieldName(name: String) = key = name

  def put(value: JSValue) = b += (key -> value)

  def result = b.result()
}

object JsonCodecFormat extends NullCodecFormat[JSValue, JsonGenerator]
    with BinaryCodecFormat[JSValue, JsonGenerator]
    with TupleCodecFormat[JSValue, JsonGenerator]
    with SequenceCodecFormat[JSValue, JsonGenerator]
    with RecordCodecFormat[JSValue, JsonGenerator]
    with UnionCodecFormat[JSValue, JsonGenerator] {

  // Primitives

  def readByte(stream: JSValue) = stream match {
    case JSLong(value) if value.toByte == value => value.toByte
    case _                                      => throw stream.unexpectedError
  }

  def readShort(stream: JSValue) = stream match {
    case JSLong(value) if value.toShort == value => value.toShort
    case _                                       => throw stream.unexpectedError
  }

  def readInt(stream: JSValue) = stream match {
    case JSLong(value) if value.toInt == value => value.toInt
    case _                                     => throw stream.unexpectedError
  }

  def readLong(stream: JSValue) = stream match {
    case JSLong(value) => value
    case _             => throw stream.unexpectedError
  }

  def readFloat(stream: JSValue) = stream match {
    case JSDouble(value) => value.toFloat
    case _               => throw stream.unexpectedError
  }

  def readDouble(stream: JSValue) = stream match {
    case JSDouble(value) => value
    case _               => throw stream.unexpectedError
  }

  def readBoolean(stream: JSValue) = stream match {
    case JSTrue  => true
    case JSFalse => false
    case _       => throw stream.unexpectedError
  }

  def readChar(stream: JSValue) = stream match {
    case JSString(value) if value.length == 1 => value(0)
    case _                                    => throw stream.unexpectedError
  }

  def readString(stream: JSValue) = stream match {
    case JSString(value) => value
    case _               => throw stream.unexpectedError
  }

  // null

  def readNull(stream: JSValue) = stream match {
    case JSNull => ()
    case _      => throw stream.unexpectedError
  }

  def readIfNull(stream: JSValue) = stream match {
    case JSNull => true
    case _      => false
  }

  def skipField(stream: JSValue) = ()

  // Binary

  def readBytes(stream: JSValue) = Unpooled.wrappedBuffer(Base64.decode(readString(stream)))

  // Records/Objects

  def readRecord(stream: JSValue, count: Int)(f: (String, JSValue) => Unit) = stream match {
    case j: JSObject => j.value foreach { t => f(t._1, t._2) }
    case _           => throw stream.unexpectedError
  }

  // Tuples

  def readTuple[T](stream: JSValue, count: Int)(f: (Int, JSValue) => Unit) =
    readSequence(stream)(f)

  // Sequences/Arrays

  def readSequence(stream: JSValue)(f: (Int, JSValue) => Unit) = stream match {
    case j: JSArray =>
      var i = 0
      j.value foreach { v => f(i, v); i += 1 }
    case _ => throw stream.unexpectedError
  }

  // Unions

  type TypeTag = String

  def readUnion[T](stream: JSValue)(f: (String, JSValue) => T): T = stream match {
    case JSObject((key, value)) => f(key, value)
    case _                      => throw stream.unexpectedError
  }

  // Primitives

  def writeByte(stream: JsonGenerator, value: Byte) = stream.put(JSLong(value))

  def writeShort(stream: JsonGenerator, value: Short) = stream.put(JSLong(value))

  def writeInt(stream: JsonGenerator, value: Int) = stream.put(JSLong(value))

  def writeLong(stream: JsonGenerator, value: Long) = stream.put(JSLong(value))

  def writeFloat(stream: JsonGenerator, value: Float) = stream.put(JSDouble(value))

  def writeDouble(stream: JsonGenerator, value: Double) = stream.put(JSDouble(value))

  def writeBoolean(stream: JsonGenerator, value: Boolean) = stream.put(if (value) JSTrue else JSFalse)

  def writeChar(stream: JsonGenerator, value: Char) = stream.put(JSString(value.toString))

  def writeString(stream: JsonGenerator, value: String) = stream.put(JSString(value))

  // null

  def writeNull(stream: JsonGenerator) = stream.put(JSNull)

  // Binary

  def writeBytes(stream: JsonGenerator, value: ByteBuf) =
    stream.put(JSString(Base64.encodeUrlSafeBuffer(value.nioBuffer).toISO8859))

  // Records/Objects

  def writeRecord(stream: JsonGenerator, size: Int)(f: JsonGenerator => Unit) = {
    val b = new JsonObjectGenerator
    f(b)
    stream.put(b.result)
  }

  def writeField(stream: JsonGenerator, key: String)(f: JsonGenerator => Unit) = {
    stream.putFieldName(key)
    f(stream)
  }

  // Tuples

  def writeTuple(stream: JsonGenerator, size: Int)(f: JsonGenerator => Unit) =
    writeSequence(stream, size)(f)

  // Sequences/Arrays

  def writeSequence(stream: JsonGenerator, size: Int)(f: JsonGenerator => Unit) = {
    val b = new JsonArrayGenerator
    f(b)
    stream.put(b.result)
  }

  // Union

  def writeUnion(stream: JsonGenerator, tag: String)(f: JsonGenerator => Unit) = {
    val b = new JsonObjectGenerator
    b.putFieldName(tag)
    f(b)
    stream.put(b.result)
  }
}
