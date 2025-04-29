package fauna.repo.schema

object Path {

  type Elem = Either[Long, String]

  /** A prefix is _reversed_ in order to allow easy construction during type
    * validation.
    */
  final class Prefix(val elems: List[Elem]) extends AnyVal {
    def :+(elem: Long) = new Prefix(Left(elem) :: elems)
    def :+(elem: String) = new Prefix(Right(elem) :: elems)
    def toPath: Path = new Path(elems.reverse)
  }

  val Root = new Path(Nil)
  val RootPrefix = new Prefix(Nil)

  def empty = Root

  def apply(elements: Elem*) =
    new Path(elements.toList)

  def unapplySeq(path: Path) =
    Some(path.elements)

}

// FIXME: Remove in favor of `fql.ast.Path`.
final class Path(val elements: List[Path.Elem]) extends AnyVal {

  override def toString: String = {
    val sb = new StringBuilder()
    elements.zipWithIndex foreach {
      case (Right(str), 0) =>
        sb.append(str)
      case (Right(str), _) =>
        sb.append(".")
        sb.append(str)
      case (Left(int), _) =>
        sb.append("[")
        sb.append(int)
        sb.append("]")
    }
    sb.result()
  }
}
