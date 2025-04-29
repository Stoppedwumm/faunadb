package fauna.storage.doc

import fauna.storage.ir._
import scala.collection.immutable.HashMap

sealed trait MaskTree {
  def select(map: MapV): MapV
  def reject(map: MapV): MapV
  def merge(other: MaskTree): MaskTree
}

object MaskTree {
  val empty: MaskTree = MaskNone
  val all: MaskTree = MaskAll

  def apply(path: List[String], last: MaskTree = MaskAll): MaskTree = path match {
    case Nil       => last
    case k :: tail => MaskFields(HashMap(k -> MaskTree(tail, last)))
  }

  def apply(arrayPath: List[String], elemPath: List[String]): MaskTree =
    arrayPath match {
      case Nil  => MaskAll
      case path => MaskTree(path.init, MaskArray(path.last, MaskTree(elemPath)))
    }

  def apply(fields: (String, MaskTree)*): MaskTree =
    MaskFields(HashMap(fields: _*))

  def Wildcard(wildcard: MaskTree): MaskTree =
    MaskFields(HashMap.empty, Some(wildcard))
}

case class MaskArray(k: String, mask: MaskTree, tail: MaskTree = MaskNone)
    extends MaskTree {

  def select(map: MapV): MapV =
    MapV(map.elems flatMap {
      case (key, ArrayV(elems)) if key == k =>
        Some(key -> ArrayV(elems map {
          case em @ MapV(_) => mask.select(em)
          case e            => e
        }))
      case (key, v) => tail.select(MapV(key -> v)).elems.headOption
    })

  def reject(map: MapV): MapV =
    MapV(map.elems flatMap {
      case (key, ArrayV(elems)) if key == k =>
        Some(key -> ArrayV(elems map {
          case em @ MapV(_) => mask.reject(em)
          case e            => e
        }))
      case (key, v) => tail.reject(MapV(key -> v)).elems.headOption
    })

  def merge(other: MaskTree): MaskTree = other match {
    case MaskNone => this
    case MaskAll  => other
    case MaskArray(ok, om, ot) if ok == k =>
      MaskArray(k, mask merge om, tail merge ot)
    case m => MaskArray(k, mask, tail merge m)
  }
}

case class MaskFields(
  fields: HashMap[String, MaskTree],
  wildcard: Option[MaskTree] = None)
    extends MaskTree {
  def select(map: MapV): MapV =
    MapV(map.elems flatMap { case (k, v) =>
      fields.get(k).orElse(wildcard) flatMap { submask =>
        v match {
          case m @ MapV(_) => MapV.lift(submask.select(m)) map { k -> _ }
          case v           => Some(k -> v)
        }
      }
    })

  def reject(map: MapV): MapV =
    MapV(map.elems flatMap { case (k, v) =>
      fields.get(k).orElse(wildcard) match {
        case Some(submask) =>
          v match {
            case m @ MapV(_) => MapV.lift(submask.reject(m)) map { k -> _ }
            case _           => None
          }
        case None => Some(k -> v)
      }
    })

  def merge(other: MaskTree): MaskTree = other match {
    case MaskNone => this
    case MaskAll  => other
    case MaskFields(others, otherWildcard) =>
      MaskFields(
        (fields merged others) { (l, r) => l._1 -> (l._2 merge r._2) },
        (wildcard, otherWildcard) match {
          case (Some(l), Some(r)) => Some(l merge r)
          case (Some(l), None)    => Some(l)
          case (None, Some(r))    => Some(r)
          case (None, None)       => None
        }
      )
    case m: MaskArray => m merge this
  }
}

case object MaskAll extends MaskTree {
  def select(map: MapV): MapV = map
  def reject(map: MapV): MapV = MapV.empty
  def merge(other: MaskTree): MaskTree = this
}

case object MaskNone extends MaskTree {
  def select(map: MapV): MapV = MapV.empty
  def reject(map: MapV): MapV = map
  def merge(other: MaskTree): MaskTree = other
}
