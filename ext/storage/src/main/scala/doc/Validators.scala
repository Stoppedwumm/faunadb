package fauna.storage.doc

import fauna.lang._
import fauna.storage.ir._

case class DataMapValidator[M[+_]](vtype: Option[IRType], path: List[String])(implicit M: Monad[M]) extends Validator[M] {
  protected val filterMask = MaskTree(path)

  override protected def validateData(data: Data) =
    M.pure((data.fields.get(path), vtype) match {
      case (Some(m @ MapV(_)), Some(vtype)) =>
        m collectValues {
          case (p, v) if v.vtype != vtype => InvalidType(path ++ p, vtype, v.vtype)
        }
      case (Some(MapV(_)), None) => Nil
      case (Some(v), _)          => List(InvalidType(path, MapV.Type, v.vtype))
      case (None, _)             => Nil
    })
}

object DataMapValidator {
  def apply[M[+_]](p: String, ps: String*)(implicit M: Monad[M]): DataMapValidator[M] =
    apply(None, p :: ps.toList)
  def apply[M[+_]](vtype: IRType, p: String, ps: String*)(implicit M: Monad[M]): DataMapValidator[M] =
    apply(Some(vtype), p :: ps.toList)
}

case class MapValidator[M[+_]](vtype: Option[IRType], path: List[String])(implicit M: Monad[M]) extends Validator[M] {
  protected val filterMask = MaskTree(path)

  override protected def validateData(data: Data) =
    M.pure((data.fields.get(path), vtype) match {
      case (Some(MapV(elems)), Some(vtype)) =>
        elems collect { case (k, v) if v.vtype != vtype => InvalidType(path :+ k, vtype, v.vtype) }
      case (Some(MapV(_)), None) => Nil
      case (Some(v), _)          => List(InvalidType(path, MapV.Type, v.vtype))
      case (None, _)             => Nil
    })
}

object MapValidator {
  def apply[M[+_]](p: String, ps: String*)(implicit M: Monad[M]): MapValidator[M] =
    apply(None, p :: ps.toList)
  def apply[M[+_]](vtype: IRType, p: String, ps: String*)(implicit M: Monad[M]): MapValidator[M] =
    apply(Some(vtype), p :: ps.toList)
}

case class ArrayValidator[M[+_]](vtype: Option[IRType], path: List[String])(implicit M: Monad[M]) extends Validator[M] {
  protected val filterMask = MaskTree(path)

  override protected def validateData(data: Data) =
    M.pure((data.fields.get(path), vtype) match {
      case (Some(ArrayV(elems)), Some(vtype)) =>
        elems.toList.zipWithIndex collect {
          case (v, i) if v.vtype != vtype => InvalidType(path :+ i.toString, vtype, v.vtype)
        }
      case (Some(ArrayV(_)), None) => Nil
      case (Some(v), _)            => List(InvalidType(path, ArrayV.Type, v.vtype))
      case (None, _)               => Nil
    })
}

object ArrayValidator {
  def apply[M[+_]](p: String, ps: String*)(implicit M: Monad[M]): ArrayValidator[M] =
    apply(None, p :: ps.toList)
  def apply[M[+_]](vtype: IRType, p: String, ps: String*)(implicit M: Monad[M]): ArrayValidator[M] =
    apply(Some(vtype), p :: ps.toList)
}

case class FieldValidator[T, M[+_]](field: Field[T], protected val filterMask: MaskTree)(implicit M: Monad[M]) extends Validator[M] {

  val isRequired = field.ftype.isRequired

  override protected def validateData(data: Data) =
    M.pure(field.read(data.fields) match {
      case Right(_)                                    => Nil
      case Left(List(ValueRequired(_))) if !isRequired => Nil
      case Left(errs)                                  => errs
    })
}
