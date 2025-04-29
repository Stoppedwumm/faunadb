package fauna.lang

import fauna.lang.syntax._
import scala.collection.mutable.{ ListBuffer, PriorityQueue }

trait PageMergeStrategy[A, B, S] {
  def initialState: S
  def next(index: Int, elem: A, state: S): (Iterable[B], S)
  def remainder(state: S): Iterable[B]
}

object PageMergeStrategy {
  object Simple extends PageMergeStrategy[Any, Any, Unit] {
    def apply[T] = this.asInstanceOf[PageMergeStrategy[T, T, Unit]]

    val initialState = ()

    def next(index: Int, elem: Any, state: Unit) = (List(elem), ())

    def remainder(state: Unit) = Nil
  }

  object Dedup extends DedupBy[Any, Any](l => List(l.head._1)) {
    def apply[T] = this.asInstanceOf[DedupBy[T, T]]
  }

  class DedupBy[A, B](f: List[(A, Int)] => List[B]) extends PageMergeStrategy[A, B, List[(A, Int)]] {
    def initialState = Nil

    def next(index: Int, elem: A, state: List[(A, Int)]) = state match {
      case (prev, _) :: _ if prev != elem => (f(state), List((elem, index)))
      case ps                             => (Nil,      (elem, index) :: ps)
    }

    def remainder(state: List[(A, Int)]) = if (state.isEmpty) Nil else f(state)
  }

  class Function[A, B, S](val initialState: S, f: (Option[(A, Int)], S) => (List[B], S)) extends PageMergeStrategy[A, B, S] {
    def next(index: Int, elem: A, state: S) = f(Some((elem, index)), state)
    def remainder(state: S) = f(None, state)._1
  }
}

/**
  * Merges a sequence of paginated values into a single sequence using
  * a +PageMergeStrategy+.
  *
  * The merger will yield equal elements to the strategy in
  * breadth-first order across the sequence of input streams, but does
  * not guarantee order within each horizontal "slice".
  */
class PageMerger[M[+_], A, B, S](
  ms: Seq[M[Page[M, Iterable[A]]]],
  strat: PageMergeStrategy[A, B, S],
  faultSize: Int,
  ord: Ordering[A],
  M: Monad[M]) {

  private type MergeElem = (Int, LazyList[A], Option[M[Page[M, Iterable[A]]]])
  private type MergeStep = M[(Int, Page[M, Iterable[A]])]

  private implicit def monadInst = M

  private def pageOrdering(implicit ord: Ordering[A]): Ordering[MergeElem] =
    new Ordering[MergeElem] {
      def compare(a: MergeElem, b: MergeElem) =
        if (a._2.nonEmpty && b._2.nonEmpty) {
          ord.compare(a._2.head, b._2.head)
        } else {
          if (a._2.isEmpty) {
            if (b._2.isEmpty) 0 else -1
          } else {
            1
          }
        }
    }

  def result: M[Page[M, List[B]]] =
    M.map(ms.sequence) {
      case Seq() => Page[M](Nil)
      case pages =>
        // PQ orders *greatest* first.
        val heap = PriorityQueue.empty[MergeElem](pageOrdering(ord).reverse)
        pages.iterator.zipWithIndex
          .foreach {
            case (Page(v, next), i) => heap.enqueue((i, v.to(LazyList), next map { _.apply() }))
          }
        merge0(strat.initialState, ListBuffer.empty, heap, faultSize)
    }


  /**
    * Gather elements from streams whose head element is equivalent to
    * +init+, terminating when either the heap of streams is drained,
    * or the head element of the top stream is distinct from init.
    */
  @annotation.tailrec
  private def step(init: A, heap: PriorityQueue[MergeElem], cont: List[MergeElem] = Nil): List[MergeElem] = {
    if (heap.isEmpty) {
      return cont
    }

    val candidate = heap.dequeue()
    if (candidate._2.nonEmpty && ord.equiv(candidate._2.head, init)) {
      step(init, heap, candidate +: cont)
    } else {
      heap.enqueue(candidate)
      cont
    }
  }

  // defeat tail-call optimization
  private def merge1(
    state: S,
    vectB: ListBuffer[B],
    heap: PriorityQueue[MergeElem],
    faultSize: Int): Page[M, List[B]] =
    merge0(state, vectB, heap, faultSize)

  @annotation.tailrec
  private def merge0(
    state: S,
    vectB: ListBuffer[B],
    heap: PriorityQueue[MergeElem],
    faultSize: Int): Page[M, List[B]] = {

    val (i, buf, cont) = heap.dequeue()

    if (buf.nonEmpty) {
      // compute the remainder of the slice of streams
      val slice = step(buf.head, heap)

      // compute the slice's seed, and fold through the streams to
      // aggregate elements
      val (elems, newState) = strat.next(i, buf.head, state)
      val (es, ns) = slice.foldLeft((elems, newState)) {
        case ((elems, state), (i, buf, _)) =>
          val (pg, st) = strat.next(i, buf.head, state)
          (elems ++ pg, st)
      }

      // push streams back on to the heap for the next iteration
      heap.enqueue((i, buf.tail, cont))
      slice foreach { case (i, buf, cont) =>
        heap.enqueue((i, buf.tail, cont))
      }

      // FIXME: optimize degenerate sets by emitting a page here
      merge0(ns, vectB ++= es, heap, faultSize)

    } else {
      cont match {
        case None =>
          if (heap.isEmpty) {
            vectB ++= strat.remainder(state)
            Page[M](vectB.toList)
          } else {
            merge0(state, vectB, heap, faultSize)
          }

        case Some(cont) =>
          val nextVect = vectB.toList
          val qs = ListBuffer.empty[MergeStep]

          qs += M.map(cont) { i -> _ }

          var heapRef = buildNextHeap(qs, heap)

          // nextCont can potentially be ran multiple times, so the
          // ref/synchronization shenanigans prevents invocations from
          // sharing the same mutable structure.
          val nextCont = M.map(qs.result().sequence) { ps =>
            val nextHeap = heap.synchronized {
              val h = if (heapRef ne null) heapRef else buildNextHeap(null, heap)
              heapRef = null
              h
            }

            ps foreach { case (i, Page(v, next)) => nextHeap.enqueue((i, v.to(LazyList), next map { _.apply() })) }
            merge1(state, ListBuffer.empty, nextHeap, faultSize)
          }

          Page[M](nextVect, nextCont)
      }
    }
  }

  private def buildNextHeap(qs: ListBuffer[MergeStep], heap: PriorityQueue[MergeElem]) = {
    val next = PriorityQueue[MergeElem]()(heap.ord)

    heap foreach {
      case (i, buf, Some(q)) if buf.lengthIs <= faultSize =>
        next += ((i, buf, None))
        if (qs ne null) qs += M.map(q) { i -> _ }
      case (i, buf, q) =>
        next += ((i, buf, q))
    }

    next
  }
}
