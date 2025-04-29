package fauna.flags.test

import fauna.codex.json._
import fauna.flags._
import java.io.ByteArrayOutputStream
import java.nio.file.{ Files, Path }

/** Represents the following JSON:
  * {
  *   "property_name": "account_id",
  *   "property_value": 123456789,
  *   "flags": {
  *     "run_queries": false
  *   }
  * }
  */
case class FlagProps(name: String, value: Value, flags: Map[String, Value])

object FlagsHelpers {
  def valueToJSON(value: Value): JSValue = {
    value match {
      case BooleanValue(v) => JSBoolean(v)
      case DoubleValue(v)  => JSDouble(v)
      case LongValue(v)    => JSLong(v)
      case StringValue(v)  => JSString(v)
    }
  }

  def writeFlags(path: Path, version: Int, props: Vector[FlagProps]): Unit = {
    val json = JSObject(
      "version" -> version,
      "properties" -> props.map { prop =>
        JSObject(
          "property_name" -> prop.name,
          "property_value" -> valueToJSON(prop.value),
          "flags" -> prop.flags.map { case (k, v) =>
            k -> valueToJSON(v)
          }
        )
      }
    )
    val stream = new ByteArrayOutputStream()
    json.writeTo(stream, pretty = false)

    Files.write(path, stream.toByteArray)
  }
}
