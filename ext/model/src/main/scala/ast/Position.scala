package fauna.ast

import scala.language.implicitConversions

// A Position points up the tree to its parent, unlike an Expression,
// which points down the tree to its children. So, in parse(),
// Positions are created top down, while expressions are created
// bottom up. Expressions refer to Positions, but not vice versa: to
// have both would require a mutable, non-case-class implementation.
object Position {

  case class PosElem(elem: Either[Int, String]) extends AnyVal

  object PosElem {
    implicit def fromIndex(idx: Int) = PosElem(Left(idx))
    implicit def fromKey(key: String) = PosElem(Right(key))
  }

  def apply(elems: PosElem*): Position =
    (elems foldLeft (RootPosition: Position)) {
      case (pos, PosElem(Left(idx)))  => pos at idx
      case (pos, PosElem(Right(key))) => pos at key
    }
}

sealed trait Position {
  def at(idx: Int): Position = IndexPosition(this, idx)
  def at(key: String): Position = KeyPosition(this, key)

  def toElems: List[Either[Int, String]] = toElems0(this, Nil)

  def toPath: List[String] = toElems map { _.fold(_.toString, identity) }

  @annotation.tailrec
  private def toElems0(pos: Position, tail: List[Either[Int, String]]): List[Either[Int, String]] =
    pos match {
      case RootPosition => tail
      case IndexPosition(parent, idx) => toElems0(parent, Left(idx) :: tail)
      case KeyPosition(parent, key) => toElems0(parent, Right(key) :: tail)
    }

  override def toString: String = {
    val parts = toElems map {
      _.fold(_.toString, e => "\"%s\"" format e)
    }
    parts.mkString("[", ",", "]")
  }
}

case object RootPosition extends Position
case class IndexPosition(parent: Position, idx: Int) extends Position
case class KeyPosition(parent: Position, key: String) extends Position
