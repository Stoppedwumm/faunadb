package fauna.codex.json2

import com.fasterxml.jackson.core.JsonToken._
import com.fasterxml.jackson.core.{ JsonFactory, JsonParser }
import io.netty.buffer.{ ByteBuf, ByteBufInputStream, Unpooled }
import java.io.InputStream

import JSONParser.SkipSwitch
import JSONTypes._

object JSONParser {

  private val jack = new JsonFactory

  def apply(bytes: Array[Byte]): JSONParser =
    JSONParser(Unpooled.wrappedBuffer(bytes))

  def apply(buf: ByteBuf): JSONParser = {
    val stream: InputStream = new ByteBufInputStream(buf)
    JSONParser(jack.createParser(stream))
  }

  final object ArrayStartSwitch extends PartialJSONSwitch[Unit] {
    override def readArrayStart(stream: JSONParser) = ()
  }

  final object ArrayEndSwitch extends PartialJSONSwitch[Unit] {
    override def readArrayEnd(stream: JSONParser) = ()
  }

  final object ObjectStartSwitch extends PartialJSONSwitch[Unit] {
    override def readObjectStart(stream: JSONParser) = ()
  }

  final object ObjectFieldNameSwitch extends PartialJSONSwitch[String] {
    override def readObjectFieldName(name: String, stream: JSONParser) = name
  }

  final object ObjectEndSwitch extends PartialJSONSwitch[Unit] {
    override def readObjectEnd(stream: JSONParser) = ()
  }

  final object SkipSwitch extends JSONSwitch[Unit] {
    def readInt(int: Long, stream: JSONParser) = ()
    def readBoolean(bool: Boolean, stream: JSONParser) = ()
    def readNull(stream: JSONParser) = ()
    def readDouble(double: Double, stream: JSONParser) = ()
    def readString(str: String, stream: JSONParser) = ()

    def readArrayStart(stream: JSONParser) =
      while (!stream.skipArrayEnd) stream.read(this)

    def readArrayEnd(stream: JSONParser) = ()

    def readObjectStart(stream: JSONParser) = {
      while (!stream.skipObjectEnd) {
        stream.read(this)
        stream.read(this)
      }
    }

    def readObjectFieldName(name: String, stream: JSONParser) = ()
    def readObjectEnd(stream: JSONParser) = ()
  }
}

object JSONTypes {
  val IntLabel = "Integer"
  val BooleanLabel = "Boolean"
  val NullLabel = "Null"
  val DoubleLabel = "Double"
  val StringLabel = "String"
  val ArrayStartLabel = "Array Start"
  val ArrayEndLabel = "Array End"
  val ObjectStartLabel = "Object Start"
  val ObjectFieldNameLabel = "Object Field Name"
  val ObjectEndLabel = "Object End"
}

trait JSONSwitch[+R] {
  def readInt(int: Long, stream: JSONParser): R
  def readBoolean(bool: Boolean, stream: JSONParser): R
  def readNull(stream: JSONParser): R
  def readDouble(double: Double, stream: JSONParser): R
  def readString(str: String, stream: JSONParser): R
  def readArrayStart(stream: JSONParser): R
  def readArrayEnd(stream: JSONParser): R
  def readObjectStart(stream: JSONParser): R
  def readObjectFieldName(name: String, stream: JSONParser): R
  def readObjectEnd(stream: JSONParser): R

  def readEndOfStream(stream: JSONParser): R = throw new NoSuchElementException("End of stream.")
}

final case class FailingJSONSwitch(expected: String) extends JSONSwitch[Nothing] {
  protected def error(provided: String) = throw JSONUnexpectedTypeException(provided, expected)

  def readInt(int: Long, stream: JSONParser) = error(IntLabel)
  def readBoolean(bool: Boolean, stream: JSONParser) = error(BooleanLabel)
  def readNull(stream: JSONParser) = error(NullLabel)
  def readDouble(double: Double, stream: JSONParser) = error(DoubleLabel)
  def readString(str: String, stream: JSONParser) = error(StringLabel)
  def readArrayStart(stream: JSONParser) = error(ArrayStartLabel)
  def readArrayEnd(stream: JSONParser) = error(ArrayEndLabel)
  def readObjectStart(stream: JSONParser) = error(ObjectStartLabel)
  def readObjectFieldName(name: String, stream: JSONParser) = error(ObjectFieldNameLabel)
  def readObjectEnd(stream: JSONParser) = error(ObjectEndLabel)
}

trait DelegatingJSONSwitch[I, R] extends JSONSwitch[R] {

  val delegate: JSONSwitch[I]

  def transform(i: I): R

  def readInt(int: Long, stream: JSONParser) = transform(delegate.readInt(int, stream))
  def readBoolean(bool: Boolean, stream: JSONParser) = transform(delegate.readBoolean(bool, stream))
  def readNull(stream: JSONParser) = transform(delegate.readNull(stream))
  def readDouble(double: Double, stream: JSONParser) = transform(delegate.readDouble(double, stream))
  def readString(str: String, stream: JSONParser) = transform(delegate.readString(str, stream))
  def readArrayStart(stream: JSONParser) = transform(delegate.readArrayStart(stream))
  def readArrayEnd(stream: JSONParser) = transform(delegate.readArrayEnd(stream))
  def readObjectStart(stream: JSONParser) = transform(delegate.readObjectStart(stream))
  def readObjectFieldName(name: String, stream: JSONParser) = transform(delegate.readObjectFieldName(name, stream))
  def readObjectEnd(stream: JSONParser) = transform(delegate.readObjectEnd(stream))
}

abstract class PartialJSONSwitch[R](expected: String = null) extends DelegatingJSONSwitch[R, R] {
  val delegate = FailingJSONSwitch(expected)
  def transform(r: R) = r
}

final case class JSONParser(stream: JsonParser) {
  if (!stream.hasCurrentToken) stream.nextToken

  def skipArrayEnd = if (stream.getCurrentToken == END_ARRAY) { stream.nextToken; true } else false

  def skipObjectEnd = if (stream.getCurrentToken == END_OBJECT) { stream.nextToken; true } else false

  def skipNull = if (stream.getCurrentToken == VALUE_NULL) { stream.nextToken; true }
  else false

  def skip(): Unit = read(SkipSwitch)

  def read[R](switch: JSONSwitch[R]): R =
    stream.getCurrentToken match {
      case null => switch.readEndOfStream(this)

      case VALUE_NUMBER_INT =>
        val v = stream.getValueAsLong
        stream.nextToken
        switch.readInt(v, this)

      case VALUE_TRUE =>
        stream.nextToken
        switch.readBoolean(true, this)

      case VALUE_FALSE =>
        stream.nextToken
        switch.readBoolean(false, this)

      case VALUE_NULL =>
        stream.nextToken
        switch.readNull(this)

      case VALUE_NUMBER_FLOAT =>
        val v = stream.getValueAsDouble(0)
        stream.nextToken
        switch.readDouble(v, this)

      case VALUE_STRING =>
        val v = stream.getText
        stream.nextToken
        switch.readString(v, this)

      case START_ARRAY =>
        stream.nextToken
        switch.readArrayStart(this)

      case END_ARRAY =>
        stream.nextToken
        switch.readArrayEnd(this)

      case START_OBJECT =>
        stream.nextToken
        switch.readObjectStart(this)

      case FIELD_NAME =>
        val v = stream.getText
        stream.nextToken
        switch.readObjectFieldName(v, this)

      case END_OBJECT =>
        stream.nextToken
        switch.readObjectEnd(this)

      case tok => throw JSONParseException(s"Invalid token $tok")
    }
}
