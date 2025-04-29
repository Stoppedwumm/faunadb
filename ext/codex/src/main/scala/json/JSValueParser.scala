package fauna.codex.json

import com.fasterxml.jackson.core.{ JsonParser, JsonParseException => JacksonParseException }
import com.fasterxml.jackson.core.JsonToken._

final class JSValueParser(in: JsonParser) {
  def getValue: JSValue = try {
    _getValue
  } catch {
    case ex: JacksonParseException => JSInvalid(JsonParseException(ex.getMessage))
  }

  private[this] def _getValue: JSValue = {
    val rv = in.getCurrentToken match {
      case START_OBJECT       => readObject
      case START_ARRAY        => readArray
      case VALUE_STRING       => JSString(in.getText)
      case VALUE_NUMBER_FLOAT => JSDouble(in.getValueAsDouble(0))
      case VALUE_NUMBER_INT   => JSLong(in.getValueAsLong)
      case VALUE_TRUE         => JSTrue
      case VALUE_FALSE        => JSFalse
      case VALUE_NULL         => JSNull
      case _                  => JSInvalid(JsonUnexpectedValue("Unknown JSON value."))
    }

    // sorta bad, but this relies on decodeObject, and decodeArray
    // to all leave a token around, so this next call is uniform.
    in.nextToken()
    rv
  }

  private[this] def readObject: JSValue = {
    in.nextToken()

    val rv = JSObject.newBuilder

    while (in.getCurrentToken != END_OBJECT) {
      if (in.getCurrentToken != FIELD_NAME) return JSInvalid(JsonParseException("Invalid JSON, expected field name."))
      val name = in.getText; in.nextToken

      rv += (name -> _getValue)
    }

    rv.result()
  }

  private[this] def readArray: JSValue = {
    in.nextToken()

    val rv = JSArray.newBuilder

    while (in.getCurrentToken != END_ARRAY) { rv += _getValue }

    rv.result()
  }
}
