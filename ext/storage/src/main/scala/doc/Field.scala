package fauna.storage.doc

import fauna.lang._
import fauna.storage.ir._
import scala.annotation.tailrec
import scala.collection.immutable.Queue

object Field {

  case class ReadException(ex: ValidationException) extends Exception(ex.toString)

  def apply[T: FieldType](segment: String, path: String*): Field[T] = apply(segment :: path.toList)

  def OneOrMore[T](path: List[String])(implicit t: FieldType[T]): Field[Vector[T]] =
    Field(path)(FieldType.OneOrMoreT(t, FieldType.ArrayT(t)))
  def OneOrMore[T: FieldType](segment: String, path: String*): Field[Vector[T]] =
    OneOrMore(segment :: path.toList)

  def ZeroOrMore[T](path: List[String])(implicit t: FieldType[T]): Field[Vector[T]] =
    Field(path)(FieldType.ZeroOrMoreT(t, FieldType.ArrayT(t)))
  def ZeroOrMore[T: FieldType](segment: String, path: String*): Field[Vector[T]] =
    ZeroOrMore(segment :: path.toList)
}

case class Field[T](path: List[String])(implicit val ftype: FieldType[T]) {
  def apply(map: MapV) =
    read(map).fold({ errs => throw Field.ReadException(errs.head) }, identity)

  def read(map: MapV): Either[List[ValidationException], T] = {
    @tailrec
    def read0(map: MapV, path: List[String], prefix: Queue[String]): Either[List[ValidationException], T] =
      path match {
        case _ :: Nil =>
          ftype.decode(map.get(path), prefix ++ path)
        case obj :: rest =>
          map.get(List(obj)) match {
            case None          => ftype.decode(None, prefix ++ path)
            case Some(NullV)   => ftype.decode(None, prefix ++ path)
            case Some(v: MapV) => read0(v, rest, prefix :+ obj)
            case Some(v) =>
              Left(List(InvalidType(prefix.toList ++ path, MapV.Type, v.vtype)))
          }
        case Nil => Left(List(ValueRequired(prefix.toList ++ path)))
      }

    read0(map, path, Queue.empty)
  }

  def readType(map: MapV): Option[IRType] = map.get(path) map { _.vtype }

  def validator[M[+_]](implicit M: Monad[M]): Validator[M] = validator(MaskAll)

  def validator[M[+_]](subMask: MaskTree)(implicit M: Monad[M]): Validator[M] =
    FieldValidator(this, mkMask(subMask))

  private def mkMask(subMask: MaskTree): MaskTree = subMask match {
    case MaskNone | MaskAll        => MaskTree(this.path, subMask)
    case MaskArray(_, elemMask, _) => MaskTree(this.path, elemMask) merge subMask
    case _: MaskFields             => MaskTree(this.path, MaskNone) merge subMask
  }
}
