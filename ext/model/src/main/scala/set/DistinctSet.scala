package fauna.model

import fauna.ast.EvalContext
import fauna.auth.Auth
import fauna.lang.syntax._
import fauna.repo._
import fauna.repo.query.Query
import fauna.storage._
import fauna.storage.index.IndexTerm
import fauna.storage.ir.DocIDV

import EventSet._


case class Distinct(set: EventSet) extends EventSet {

  def count = set.count

  def isFiltered: Boolean = set.isFiltered

  def isComposite: Boolean = true

  def filteredForRead(auth: Auth): Query[Option[EventSet]] =
    set.filteredForRead(auth) mapT { Distinct(_) }

  def history(ec: EvalContext, from: Event, to: Event, size: Int, ascending: Boolean): PagedQuery[Iterable[Elem[Event]]] =
    set.history(ec, from, to, size, ascending)

  def sortedValues(ec: EvalContext, from: Event, to: Event, size: Int, ascending: Boolean): PagedQuery[Iterable[Elem[Event]]] =
    set.sortedValues(ec, from, to, size, ascending)

  private def filter(page: PagedQuery[Iterable[Elem[Event]]]) = {
    def values(evt: Event): Vector[IndexTerm] = evt match {
      case ie: DocEvent                      => Vector(IndexTerm(DocIDV(ie.docID)))
      case se: SetEvent if se.values.isEmpty => Vector(IndexTerm(DocIDV(se.docID)))
      case se: SetEvent                      => se.values
    }

    def equiv(a: Elem[Event], b: Elem[Event]): Boolean = values(a.value) == values(b.value)

    def distinct0(coll: Iterable[Elem[Event]]): Iterable[Elem[Event]] = if (coll.isEmpty) coll else {
      val h = coll.head
      val t = coll.tail
      val res = List.newBuilder += h
      t.foldLeft(h) { case (prev, elem) =>
        if (equiv(prev, elem)) {
          prev
        } else {
          res += elem
          elem
        }
      }
      res.result()
    }
    page alignByT { equiv(_, _) } mapT distinct0
  }

  def snapshot(ec: EvalContext, from: Event, to: Event, size: Int, ascending: Boolean) =
    filter(set.snapshot(ec, from, to,size, ascending))

  def sparseSnapshot(ec: EvalContext, keys: Vector[Event], ascending: Boolean) =
    filter(set.sparseSnapshot(ec, keys, ascending))

  def shape: Shape = set.shape
}
