package fauna.config

import fauna.codex.json._
import scala.collection.mutable
import scala.reflect.runtime.universe.{ typeOf, TypeTag }
import scala.reflect.ClassTag

trait Configuration {
  protected val _fields = mutable.Map.empty[String, ConfigField[_]]
  def fields = _fields.toMap
}

case class ConfigField[T: ClassTag: TypeTag](
  name: String,
  mods: List[ConfigMod],
  default: T
) {

  def required = mods contains Required
  def deprecated = mods contains Deprecated

  def newName: Option[String] =
    mods collectFirst { case Renamed(name) => name }

  private[this] var value: T = _

  def set(v: Any): Unit =
    (v, typeOf[T]) match {
      // Check for null first.
      case (null, _) => value = null.asInstanceOf[T]

      // Special case longs: `v` can be an Integer or a Long, but we want to store it
      // as a Long.
      case (_, t) if t =:= typeOf[Long] =>
        v match {
          case i: java.lang.Integer => value = i.longValue.asInstanceOf[T]
          case l: java.lang.Long    => value = l.asInstanceOf[T]
          case _ => throw new IllegalArgumentException(s"$v is not a Long.")
        }

      // Special case lists of longs for the same reason.
      case (_, t) if t =:= typeOf[List[Long]] =>
        value = v
          .asInstanceOf[List[_]]
          .map {
            case i: java.lang.Integer => i.longValue
            case l: java.lang.Long    => l
            case _ =>
              throw new IllegalArgumentException(
                s"$v (${v.getClass}) is not a List of Longs.")
          }
          .asInstanceOf[T] // does nothing, we just checked the class tag.

      // Fallback to runtime types/class tags.
      case (v: T, _) => value = v

      case _ =>
        if (default.asInstanceOf[AnyRef] ne null) {
          throw new IllegalArgumentException(
            s"$v (${v.getClass}) is not an instance of ${default.getClass}")
        } else {
          throw new IllegalArgumentException(
            s"$v (${v.getClass}) is not of the expected type.")
        }
    }

  def get: T = Option(value) match {
    case None if required => throw new NullPointerException
    case None             => default
    case Some(v)          => v
  }

  def toJSON: JSObject = {
    def toJS(v: Any): JSValue =
      v match {
        case null                 => JSNull
        case i: java.lang.Integer => JSLong(i.toLong)
        case l: java.lang.Long    => JSLong(l)
        case s: String            => JSString(s)
        case b: java.lang.Boolean => JSBoolean(b)
        case d: java.lang.Double  => JSDouble(d)
        case l: List[_]           => JSArray((l map toJS): _*)
        case a                    => JSString(a.toString)
      }

    JSObject(
      "name" -> name,
      "value" -> toJS(value),
      "default" -> toJS(default),
      "required" -> required,
      "deprecated" -> deprecated
    )
  }
}
