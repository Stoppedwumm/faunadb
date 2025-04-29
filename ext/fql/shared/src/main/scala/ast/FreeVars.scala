package fql.ast

import scala.collection.immutable.HashMap

object FreeVars {
  val empty = FreeVars(HashMap.empty)

  def apply(name: String, span: Span): FreeVars =
    FreeVars(HashMap((name, Set(span))))
}

final case class FreeVars(sites: HashMap[String, Set[Span]]) extends AnyVal {
  def isEmpty = sites.isEmpty
  def nonEmpty = sites.nonEmpty

  def contains(name: String) = sites.contains(name)

  def names = sites.keys

  def ids = sites.iterator
    .flatMap { case (n, spans) => spans.map { Expr.Id(n, _) } }
    .to(Iterable)

  def |(o: FreeVars) =
    FreeVars((sites merged o.sites) { case ((k, v1), (_, v2)) => (k, v1 | v2) })

  def -(n: String) = FreeVars(sites - n)

  def --(ns: Iterable[String]) = FreeVars(sites -- ns)
}
