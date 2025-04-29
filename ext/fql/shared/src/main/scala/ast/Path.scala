package fql.ast

// Represent a field path, e.g. .foo.bar.
// Right now we parse field paths, but only accept top-level named paths,
// e.g. .foo, not .foo.bar or .foo[5].
final case class Path(elems: List[PathElem], span: Span) extends WithNullSpanPath {
  def toList: List[Either[Long, String]] = elems.map {
    case PathElem.Field(name, _) => Right(name)
    case PathElem.Index(idx, _)  => Left(idx)
  }

  def head: PathElem = elems.head

  override def hashCode: Int = toList.hashCode
  override def equals(that: Any): Boolean = that match {
    case that: Path => toList == that.toList
    case _          => false
  }

  def parent: Option[Path] = Option.when(elems.sizeIs > 1)(Path(elems.init, span))
}

object Path {
  def empty: Path = Path(Nil, Span.Null)

  def apply(str: String): Path =
    Path(List(PathElem.Field(str, Span.Null)), Span.Null)

  def fromList(elems: List[Either[Long, String]], span: Span = Span.Null): Path = {
    Path(
      elems.map {
        case Right(name) => PathElem.Field(name, span)
        case Left(idx)   => PathElem.Index(idx, span)
      },
      span)
  }
}

sealed trait PathElem {
  def span: Span

  def asField: Option[String] = this match {
    case PathElem.Field(name, _) => Some(name)
    case _                       => None
  }

  def asIndex: Option[Long] = this match {
    case PathElem.Index(idx, _) => Some(idx)
    case _                      => None
  }
}

object PathElem {
  final case class Field(name: String, span: Span) extends PathElem
  final case class Index(idx: Long, span: Span) extends PathElem
}
