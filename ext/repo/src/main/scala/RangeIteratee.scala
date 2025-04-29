package fauna.repo

import fauna.lang.syntax._
import fauna.repo.query.Query
import scala.util.control.NoStackTrace

/** A ColIteratee is the processing kernel for a RangeIteratee. It is
  * provided with each chunk of the input stream until the stream is
  * exhausted, after which it is called once more with `None`.
  *
  * A ColIteratee may indicate that processing should stop before the
  * input stream is exhausted by returning `None`, otherwise it should
  * return another ColIteratee to receive the next chunk of the input
  * stream.
  *
  * If its "mother" RangeIteratee supports skipping, a ColIteratee may signal
  * that processing for the current row should be skipped by returning a query
  * failed with RangeIteratee.SkipRowException. This may be done in the middle
  * of processing the row but not during the finish (when the iteratee is called
  * with None). A SkipRowException that occurs after the columns have all been
  * processed will cause an IllegalStateException. Skipping when skipping is not
  * supported causes an IllegalStateException.
  * TODO: Support skipping during the finish?
  */
trait ColIteratee[C] extends (Option[Iterable[C]] => Query[Option[ColIteratee[C]]])

object ColIteratee {
  def apply[C](
    f: Option[Iterable[C]] => Query[Option[ColIteratee[C]]]): ColIteratee[C] =
    new ColIteratee[C] { def apply(page: Option[Iterable[C]]) = f(page) }

  def skipColIteratee[C](): ColIteratee[C] =
    ColIteratee[C] { cols => Query.value(cols map { _ => skipColIteratee[C]() }) }
}

/** A RangeIteratee is an abstraction for incrementally processing a
  * sequential stream of input.
  *
  * In this case, the input is provided by a `PagedQuery`, and the
  * processing is accomplished by `ColIteratee`.
  *
  * Processing will continue until the `PagedQuery` ceases to yield
  * additional data. Implementations may augment the stream after it
  * would otherwise be exhausted using `finish()`.
  *
  * NOTE: The input stream may have been artificially truncated using
  *       `Page.takeT` and similar. The end of the stream is _NOT_
  *       necessarily the end of the data!
  */
trait RangeIteratee[ID, C] extends (ID => Query[Option[ColIteratee[C]]]) {
  type PendingPages = Query[Option[PagedQuery[Iterable[(ID, C)]]]]

  // When true, a ColIteratee may fail with a SkipRowException, causing the
  // currently processing row to be skipped.
  // When false, the iteratee does not support skipping, and attempting to skip
  // a row with a SkipRowException will cause an IllegalStateException to be thrown.
  // NB: A non-skipping iteratee does not have to accumulate all columns for a row
  //     before finishing processing some of them, which can save a lot of heap.
  val isSkippingIteratee = false

  /** For each uniquely-identifiable row in the input,
    * `RangeIteratee.run()` will call this method. Implementations are
    * expected to yield a `ColIteratee` to process the columns of that
    * row. If the row is not of interest, implementations should
    * return `None`, and all columns associated with `rowID` will not
    * be processed.
    */
  def apply(rowID: ID): Query[Option[ColIteratee[C]]]

  /** Implementations may override this method to return additional
    * input which will be yielded to the ColIteratee after the input
    * would otherwise be exhausted.
    */
  def finish(): PendingPages = Query.none
}

object RangeIteratee {

  // SkipRowException may be thrown when processing an input stream.
  // It will cause remaining columns for that ID to be skipped and
  // roll back the query to before the row was processed.
  object SkipRowException
      extends UnretryableException("Skipped row")
      with NoStackTrace

  final case class State(step: Query[Option[State]])

  // This null token guarantees that the (id == rowid) check below
  // returns false, creating the initial ColIteratee.
  private[this] val nullToken = new Object

  // Convenience methods to permit `iter.init()` and `iter.run()`
  implicit class IterateeOps[ID, C](iter: RangeIteratee[ID, C]) {
    def init(pq: PagedQuery[Iterable[(ID, C)]]): State = RangeIteratee.init(iter, pq)
    def run(pq: PagedQuery[Iterable[(ID, C)]]): Query[Unit] =
      RangeIteratee.run(init(pq))
  }

  /** Initializes the iteratee given by `iter` with the input stream
    * given in `pg`, returning an initial state. The iteratee may be
    * incrementally executed with `run()`.
    */
  def init[ID, C](
    iter: RangeIteratee[ID, C],
    pq: PagedQuery[Iterable[(ID, C)]]): State =
    State(runStep(iter, nullToken.asInstanceOf[ID], None, pq, None))

  /** Processes an iteratee until its input is exhausted. Each
    * incremental iteration is executed in a single transaction.
    */
  def run(state: State): Query[Unit] = Query.transaction(state.step) flatMap {
    case RepoContext.Result(_, Some(s)) => run(s)
    case RepoContext.Result(_, None)    => Query.unit
  }

  private def runStep[ID, C](
    rowIter: RangeIteratee[ID, C],
    rowID: ID,
    colIter: Option[ColIteratee[C]],
    pq: PagedQuery[Iterable[(ID, C)]],
    curr: Option[(ID, Vector[C])]): Query[Option[State]] = {

    def runStep0(
      rowID: ID,
      colIter: Option[ColIteratee[C]],
      spans: List[(ID, Vector[C])]): Query[(ID, Option[ColIteratee[C]])] =
      spans match {
        case (id, cols) :: next =>
          val colIterQ = if (id == rowID) {
            Query(colIter)
          } else {
            colIter match {
              case Some(ci) =>
                try {
                  ci(None) flatMap { _ => rowIter(id) }
                } catch {
                  case SkipRowException =>
                    throw new IllegalStateException("End of row", SkipRowException)
                }
              case None => rowIter(id)
            }
          }

          colIterQ flatMap {
            case Some(ci) =>
              ci(Some(cols)) recover {
                // Recover errors in processing on the first batch for a row,
                // so that all the row's batches' effects will be rolled back.
                case RangeIteratee.SkipRowException if id != rowID =>
                  if (rowIter.isSkippingIteratee) {
                    Some(ColIteratee.skipColIteratee[C]())
                  } else {
                    throw new IllegalStateException(
                      "Skipping not supported by this RangeIteratee")
                  }

                // If processing another row would exceed the
                // transaction size limits, skip the remainder and
                // attempt to commit the progress so far.
                case e: TxnTooLargeException if id != rowID =>
                  if (rowIter.isSkippingIteratee) {
                    Some(ColIteratee.skipColIteratee[C]())
                  } else {
                    throw e
                  }
              } flatMap { ci0 =>
                Query.defer(runStep0(id, ci0, next))
              }

            case None =>
              Query.defer(runStep0(id, None, next))
          }

        case Nil =>
          Query.value((rowID, colIter))
      }

    pq flatMap { page =>
      // This is a little tricky! A non-skipping iteratee does not need to
      // accumulate a whole row in `curr`. Pretending that every page is the
      // the last page in `byID` stops the accumulation.
      val (nextRows, nextCurr) =
        byID(page.value, curr, !rowIter.isSkippingIteratee || page.next.isEmpty)
      runStep0(rowID, colIter, nextRows) flatMap { case (id, colIter) =>
        page.next match {
          case Some(pq) =>
            Query.some(State(runStep(rowIter, id, colIter, pq(), nextCurr)))
          case None =>
            // Signal end of input and augment with any additional input from
            // finish().
            val nextPg = colIter match {
              case Some(ci) => ci(None) flatMap { _ => rowIter.finish() }
              case None     => rowIter.finish()
            }
            nextPg mapT { next =>
              State(runStep(rowIter, id, colIter, next, nextCurr))
            }
        }
      }
    }
  }

  private def byID[ID, C](
    page: Iterable[(ID, C)],
    curr: Option[(ID, Vector[C])],
    lastPage: Boolean): (List[(ID, Vector[C])], Option[(ID, Vector[C])]) =
    if (page.isEmpty) (curr.toList, None)
    else {
      val spans = List.newBuilder[(ID, Vector[C])]
      val currSpan = Vector.newBuilder[C]
      curr foreach { case (_, c) => currSpan ++= c }
      var currID = curr.fold(page.head._1) { _._1 }

      val iter = page.iterator
      while (iter.hasNext) {
        val (id, c) = iter.next()

        if (id != currID) {
          spans += ((currID, currSpan.result()))
          currSpan.clear()
          currID = id
        }

        currSpan += c
      }

      val last = currSpan.result()
      if (lastPage) {
        spans += ((currID, currSpan.result()))
        (spans.result(), None)
      } else {
        val nextCurr = if (last.nonEmpty) Some((currID, currSpan.result())) else None
        (spans.result(), nextCurr)
      }
    }
}
