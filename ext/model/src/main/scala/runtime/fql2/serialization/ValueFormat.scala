package fauna.model.runtime.fql2.serialization

import fauna.codex.json2.JSON
import fauna.storage.doc.FieldType
import fauna.storage.ir.StringV
import scala.util.control.NoStackTrace

sealed trait ValueFormat

object ValueFormat {
  final object Simple extends ValueFormat { override val toString = "simple" }
  final object Tagged extends ValueFormat { override val toString = "tagged" }
  final object Decorated extends ValueFormat { override val toString = "decorated" }

  case class InvalidFormat(format: String) extends NoStackTrace {
    override def getMessage(): String = s"Invalid format $format"
  }

  def fromString(string: String) = string match {
    case ValueFormat.Simple.toString    => ValueFormat.Simple
    case ValueFormat.Tagged.toString    => ValueFormat.Tagged
    case ValueFormat.Decorated.toString => ValueFormat.Decorated
    case other                          => throw InvalidFormat(other)
  }

  def fromStringOpt(string: String) =
    try {
      Some(fromString(string))
    } catch {
      case _: InvalidFormat => None
    }

  final val IntTag = "@int"
  final val LongTag = "@long"
  final val DoubleTag = "@double"
  final val ObjectTag = "@object"
  final val TimeTag = "@time"
  final val DateTag = "@date"
  final val BytesTag = "@bytes"
  final val UUIDTag = "@uuid"
  final val ModTag = "@mod"
  final val DocTag = "@doc"
  final val RefTag = "@ref"
  final val SetTag = "@set"
  final val StreamTag = "@stream"

  final val AfterField = JSON.Escaped("after")
  final val DataField = JSON.Escaped("data")

  implicit val ValueFormatT = FieldType[ValueFormat]("ValueFormat") { f =>
    StringV(f.toString)
  } { case StringV(str) =>
    ValueFormat.fromString(str)
  }
}
