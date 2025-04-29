package fauna.repo.values

import fauna.atoms.DocID
import scala.annotation.implicitNotFound

object InvalidCast extends Exception

@implicitNotFound("Could not find a ValueDecoder for ${T}.")
trait ValueDecoder[T] extends (Value => T)

object ValueDecoder {
  implicit object ValueDecoder extends ValueDecoder[Value] {
    def apply(v: Value): Value = v
  }

  implicit object DocIDDecoder extends ValueDecoder[DocID] {
    def apply(v: Value): DocID = v match {
      case Value.Doc(id, _, _, _, _) => id
      case _                         => throw InvalidCast
    }
  }

  implicit object StringDecoder extends ValueDecoder[String] {
    def apply(v: Value): String = v match {
      case Value.Str(str) => str
      case _              => throw InvalidCast
    }
  }

  implicit object IntDecoder extends ValueDecoder[Int] {
    def apply(v: Value): Int = v match {
      case Value.Int(n) => n
      case _            => throw InvalidCast
    }
  }

  implicit object LongDecoder extends ValueDecoder[Long] {
    def apply(v: Value): Long = v match {
      case Value.Long(l) => l
      case Value.ID(id)  => id
      case _             => throw InvalidCast
    }
  }

  implicit object BooleanDecoder extends ValueDecoder[Boolean] {
    def apply(v: Value): Boolean = v match {
      case Value.Boolean(b) => b
      case _                => throw InvalidCast
    }
  }

  implicit def StringMapDecoder[T](
    implicit ev: ValueDecoder[T]): ValueDecoder[Map[String, T]] = {
    case Value.Struct.Full(fields, _, _, _) =>
      fields map { case (k, v) => k -> ev(v) }
    case _ => throw InvalidCast
  }

  implicit def VectorDecoder[T](
    implicit ev: ValueDecoder[T]): ValueDecoder[Vector[T]] = {
    case Value.Array(elems) => elems map { ev(_) } toVector
    case _                  => throw InvalidCast
  }
}
