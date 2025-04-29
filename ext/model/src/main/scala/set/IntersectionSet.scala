package fauna.model

import fauna.ast.EvalContext
import fauna.auth.Auth
import fauna.lang.Page
import fauna.lang.syntax._
import fauna.repo._
import fauna.repo.query.Query
import fauna.storage._
import scala.collection.mutable.{
  ArrayBuffer,
  ListBuffer,
  PriorityQueue,
  Map => MMap
}
import EventSet._

/**
 * Intersecting sets requires an element to exist in all sets.
 *
 * Beware: this is a highly optimized piece of code with some subtle behavioral differences
 * from standard AbstractAlgebraicSets. Sparse intersections can be very expensive. These
 * optimizations are an attempt at reducing those costs.
 */
case class Intersection(sets: List[EventSet]) extends AbstractAlgebraicSet {
  override def snapshot(ec: EvalContext, from: Event, to: Event, size: Int, ascending: Boolean) = {
    val start = from match {
      case ie: DocEvent => ie
      case se: SetEvent if se.values.isEmpty => se
      case se: SetEvent =>
        val tuple = if (ascending) Event.MinValue else Event.MaxValue
        tuple.copy(values = se.values)
    }

    val ord = SnapshotOrdering(ascending).reverse
    val streams = sets.iterator.zipWithIndex map {
      case (set, i) =>
        StreamBuffer(i, ec, set, start, to, size, ascending, ord)
    } toList

    Query.MonadInstance.sequence(streams) map {
      case Seq() => Page[Query, Iterable[Elem[Event]]](Nil)
      case streams =>
        val heap = PriorityQueue.empty[StreamBuffer] ++ streams
        merge0(ListBuffer.empty, heap)
    }
  }

  /**
   * Collect StreamBuffers from the given heap which are non-empty
   * and whose head element is equivalent to init. The heap is sorted
   * by head element, so we can exit as soon as we hit a heap whose
   * head is not a match.
   */
  @annotation.tailrec
  private def step(
    init: Elem[Event],
    heap: PriorityQueue[StreamBuffer],
    cont: List[StreamBuffer] = Nil): List[StreamBuffer] =
    if (heap.isEmpty) {
      cont
    } else {
      // careful, we're mutating the heap
      val candidate: StreamBuffer = heap.dequeue()
      if (candidate.nonEmpty && candidate.equivHead(init)) {
        // matched. add the StreamBuffer to the list and move on to the
        // next item in the heap
        step(init, heap, candidate +: cont)
      } else {
        // no match. put the StreamBuffer back on the heap and return
        // what we found
        heap += candidate
        cont
      }
    }

  /**
   * Grab every StreamBuffer with a head that matches the head of `first`
   * this is our slice of matching elements
   */
  @inline
  private def mkSlice(
    first: StreamBuffer,
    heap: PriorityQueue[StreamBuffer]): List[StreamBuffer] =
    step(first.head, heap) :+ first

  /**
   * Gather matching elements from the streams into a single stream
   */
  @annotation.tailrec
  private def merge0(
    // elements that match the intersection
    vectB: ListBuffer[Elem[Event]],
    // our heap of sets to intersect
    heap: PriorityQueue[StreamBuffer]): Page[Query, List[Elem[Event]]] = {

    val first = heap.dequeue()

    // the `first` stream has something, we have work to do
    if (first.nonEmpty) {
      val slice = mkSlice(first, heap)

      // Intersect requires the element to exist in all sets
      // Consume and drop the matches we did find
      val es = if (slice.lengthIs != setSize) {
        slice foreach { _.dequeue() }
        Nil

      } else {
        // merge down to the fewest matches
        // that is, if one stream contains 4 matching elements and
        // another only contains 3, we'll only return 3
        val ps = slice map { stream => (stream.dequeue(), stream.i) }
        val sources = ps flatMap { _._1.sources } distinct
        val byStreamBuffer = ps groupBy { _._2 }

        val minStreamBuffer = byStreamBuffer.foldLeft(List.empty[Elem[Event]]) {
          case (Nil, (_, es))                     => es map { _._1 }
          case (as, (_, bs)) if as.size > bs.size => bs map { _._1 }
          case (as, _)                            => as
        }
        minStreamBuffer map { _.copy(sources = sources) }
      }

      // we've consumed the head from the StreamBuffers
      // push them back on to the heap for the next iteration
      heap ++= slice

      // FIXME: optimize degenerate sets by emitting a page here
      merge0(vectB ++= es, heap)

    // The `first` stream is empty, create the Page
    } else {
      if (first.isDone) {
        // the `first` has been completely consumed
        // Since intersection requires participation in all streams, we're done
        Page[Query](vectB.toList)
      } else {
        Page(vectB.toList, nextCont(first :: Nil, heap))
      }
    }
  }

  /**
   * Build the next continuation.
   * We can step through remaining stream buffers and throw out elems that we know won't match.
   * Any elem that matches non-empty stream buffers will used in a multi-get from the empty buffers.
   */
  @annotation.tailrec
  private def nextCont(
    // list of buffers that require us to fetch the next page of elements
    emptyStreamBuffers: List[StreamBuffer],
    // our heap of sets to intersect
    heap: PriorityQueue[StreamBuffer],
    // we found these elems in the associated stream
    existing: ArrayBuffer[(StreamBuffer, Elem[Event])] = ArrayBuffer.empty,
    // we need search these streams for the associated elem
    needed: ArrayBuffer[(StreamBuffer, Elem[Event])] = ArrayBuffer.empty)
    : PagedQuery[List[Elem[Event]]] = {
    require(emptyStreamBuffers.size + heap.size == setSize)

    // we have non-empty streams that we need to consume from before
    // going back to storage for each stream's next page
    if (heap.nonEmpty) {
      val heapCount = heap.length
      val first = heap.dequeue()

      // stream is empty, add it to the empty list and go again
      if (first.isEmpty) {
        nextCont(emptyStreamBuffers :+ first, heap, existing, needed)

      } else {
        val slice = mkSlice(first, heap)

        // The element matches all non-empty streams. We need to find it
        // in the empty streams before we can accept it.
        val (n, e) = if (slice.lengthCompare(heapCount) == 0) {
          // associate this element with our current empty stream list and put them
          // in the `needed` pile
          val n = emptyStreamBuffers map { _ -> first.head }

          // for all other streams pull the element and associate the two in the
          // `existing` pile.
          val e = slice map { stream => (stream, stream.dequeue()) }

          (n, e)

        // The element didn't match all non-empty streams
        } else {
          // consume and drop the element from all non-empty streams
          slice foreach { _.dequeue() }

          // nothing new to add to `needed` and `existing`
          (Nil, Nil)
        }

        // put the streams back on the heap for the next iteration
        heap ++= slice

        nextCont(emptyStreamBuffers, heap, existing ++= e, needed ++= n)
      }

    // We've consumed all we have. Now we need to build a query to fill the next
    // page of the intersection
    } else {
      // group our list of needed elements by their associated stream
      val neededG: Map[StreamBuffer, ArrayBuffer[(StreamBuffer, Elem[Event])]] =
        needed.groupBy { _._1 }

      // have each stream buffer pull its next page based on the elements
      // that it needs
      val neededQ: Iterable[Query[StreamBuffer]] = neededG map {
        case (s, ns) =>
          // pass the list of needed events into the stream's `next` function
          s.next(ns.iterator map { _._2.value } toList)
      }

      // group our list of existing elements by their associated stream
      val havesG: Map[StreamBuffer, ArrayBuffer[(StreamBuffer, Elem[Event])]] =
        existing.groupBy { _._1 }

      // create specialized streams where the current page is the known
      // elements
      val haves: Iterable[StreamBuffer] = havesG map {
        case (s, es) =>
          // if we don't need anything from this stream and it's has more
          // pages, we should provide a continuation query for the next page.
          // otherwise the continuation will come from the stream in the needed
          // list
          val continue = !neededG.contains(s) && !s.isDone
          s.from(continue, es.iterator map { _._2 } toList)
      }

      // if we need to pull anything, that's our continuation query
      val qs = if (neededQ.nonEmpty) {
        neededQ
      // otherwise we have all the empty buffers pull their next page
      } else {
        emptyStreamBuffers map { _.continue }
      }

      qs.sequence map { needed =>
        // build a new heap with the in-memory streams we created
        // and the streams we just pulled from storage
        // NB: the newHeap should be an accurate representation of
        // the streams - their `i`s should be unique and no
        // different from the initial set
        val newHeap = PriorityQueue.empty[StreamBuffer]

        // WARNING UGLY CODE INBOUND
        // We have an invariant to uphold: namely we can't duplicate
        // the streams. HOWEVER it is totally possible for a stream to have
        // both elements that were matched with other streams and have missed
        // elements present in other streams. Due to ordering all needed
        // elements must, by ordering invariant, be sorted after elements we
        // previously matched. Therefore to maintain our unduplicated stream
        // invariant it is sufficient to append needed elements to existing
        // elements in the same stream.
        // Consider the following:
        // 0: Empty(Some(cont))
        // 1: (C, D)
        // 2: (C, F)
        // Walking this would mean that C existed in 1/2 and was needed in 0
        // and F was needed in 1. So we would query 1. for F + snapshot (needed)
        // but have C in haves. So we need to prepend C to whatever continuation
        // we queried for.
        // When we have both we preserve the continuation for needed.
        val havesByIndex = MMap.from(haves map { s => s.i -> s })
        needed foreach { n =>
          havesByIndex.remove(n.i) foreach { h =>
            n.queue = h.queue ++ n.queue
          }
          newHeap += n
        }

        // grab each queue and add it to the heap
        newHeap ++= havesByIndex.values

        merge0(ListBuffer.empty, newHeap)
      }
    }
  }

  val setSize = sets.size

  def isPresent(memberships: Set[Int]): Boolean = memberships.size == setSize

  def isFiltered: Boolean = sets.exists { _.isFiltered }

  def filteredForRead(auth: Auth): Query[Option[Intersection]] =
    (sets map { _.filteredForRead(auth) } sequence) map {
      _.flatten.toList match {
        case Nil  => None
        case sets => Some(Intersection(sets))
      }
    }
}

// helper object for building a stream buffer from the first page of a set
private object StreamBuffer {
  def apply(
    i: Int,
    ec: EvalContext,
    set: EventSet,
    from: Event,
    to: Event,
    size: Int,
    ascending: Boolean,
    ord: Ordering[Elem[Event]]): Query[StreamBuffer] =

    set.snapshot(ec, from, to, size, ascending) map {
      case Page(vs, n) =>
        StreamBuffer(i, ec, set, to, size, ascending, n map { _.apply() }, ord, vs.toList)
    }
}

// A structure that provides the ability to mutate the in-memory page in
// an effort to reduce allocation rates when merging sets.
private case class StreamBuffer(
    i: Int,
    ec: EvalContext,
    set: EventSet,
    to: Event,
    size: Int,
    ascending: Boolean,
    cont: Option[PagedQuery[Iterable[Elem[Event]]]],
    ord: Ordering[Elem[Event]],
    var queue: List[Elem[Event]]) extends Ordered[StreamBuffer] {

  override def hashCode = i.hashCode
  override def equals(o: Any): Boolean = o match {
    case s: StreamBuffer => s.i == i
    case _               => false
  }

  override def toString = s"StreamBuffer($i, ${queue.size}, ${queue.headOption})"

  private def steppedSize = (size * 2) max 1 min MaxInternalPageSize

  // There isn't another page available to pull from storage
  def isDone = cont.isEmpty

  // Our current, in-memory, page is (non-)empty
  def isEmpty = queue.isEmpty
  def nonEmpty = queue.nonEmpty

  // head of the in-memory page
  def head = queue.head

  // check for equivalence to our current head
  def equivHead(other: Elem[Event]) = ord.equiv(head, other)

  // pop an element off the page
  def dequeue(): Elem[Event] = {
    val e = queue.head
    queue = queue.tail
    e
  }

  /**
   * if both buffers have an element, compare just the head
   * empty buffers sort lower than non-empty buffers
   */
  def compare(that: StreamBuffer): Int =
    if (that.nonEmpty && nonEmpty) {
      ord.compare(head, that.head)
    } else {
      if (that.isEmpty) {
        if (isEmpty) 0 else -1
      } else {
        1
      }
    }

  // create a new buffer whose in-memory page is the `existing`
  // elements. Remove the continuation query if we shouldn't
  // continue.
  def from(continue: Boolean, existing: List[Elem[Event]]): StreamBuffer =
    if (continue) {
      copy(size = steppedSize, queue = existing)
    } else {
      copy(cont = None, queue = existing)
    }

  // Query to grab the next page of elements or this, presumably, empty
  // stream if we don't have a continuation.
  def continue: Query[StreamBuffer] = cont match {
    case Some(cont) =>
      cont map { case Page(vs, n) => copy(size = steppedSize, cont = n map { _.apply() }, queue = vs.toList) }
    case None =>
      Query.value(this)
  }

  /**
   * Query to grab the next page of elements based on the events
   * in `needed`. This will use sparseSnapshot to attempt to grab
   * only the elements we need and anything that sorts after the
   * last element in the `needed` list.
   */
  def next(needed: List[Event]): Query[StreamBuffer] = {

    // ensure we get the sorting right so we don't miss the event
    val tuple = if (ascending) Event.MinValue else Event.MaxValue
    val es: List[Event] = needed map {
      case ie: DocEvent => ie
      case se: SetEvent if se.values.isEmpty => se
      case se: SetEvent => tuple.copy(values = se.values)
    }

    val q = es match {
      // nothing to grab
      case Nil =>
        (Query.none, Query.none)

      // for a single element we only need the snapshot read of all elements
      // after and including the given element.
      case elem :: Nil =>
        (Query.none, set.snapshot(ec, elem, to, size, ascending) map { Some(_) })

      // do a sparse read of all but the last element of the list
      // and a snapshot read of the last element plus anything after it
      case elems =>
        val init = set.sparseSnapshot(ec, elems.init.toVector, ascending) map { Some(_) }
        val last = set.snapshot(ec, elems.last, to, size, ascending) map { Some(_) }

        (init, last)
    }

    q par { (init, last) =>
      var cont = Option.empty[PagedQuery[Iterable[Elem[Event]]]]
      val nextQueue = List.newBuilder[Elem[Event]]

      // add our sparse read elements to the new page
      // NB: this assumes the query will return all of our requested
      // elements (if they exist) in a single page
      init foreach { case Page(vs, _) => nextQueue ++= vs }

      // add our snapshot read elements to the new page
      // and set the continuation query based on the snapshot page
      last foreach {
        case Page(vs, n) =>
          cont = n map { _.apply() }
          nextQueue ++= vs
      }

      Query.value(copy(size = steppedSize, cont = cont, queue = nextQueue.result()))
    }
  }
}
