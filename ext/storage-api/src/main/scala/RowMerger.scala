package fauna.storage.api

import fauna.exec.ImmediateExecutionContext
import fauna.lang.syntax._
import fauna.lang.Page
import fauna.storage.{ Cell, Conflict, Mutation }
import fauna.storage.ops.Write
import io.netty.buffer.ByteBuf
import java.util.concurrent.atomic.LongAdder
import scala.collection.mutable.SortedSet
import scala.concurrent.Future
import scala.language.implicitConversions

/** Merge cells coming from disk with cells derived from write intents (sorted by
  * name). The row merger initially returns cells but, cells can be mapped into other
  * types. The merger keeps track of the number of bytes read from disk throughout
  * data transformations. The number of bytes reported in its final result only
  * accounts for values generated from cells coming from disk.
  */
private[api] object RowMerger {

  final case class Result[A](
    values: Vector[A],
    bytesEmitted: Int
  )

  final case class Entry[A](
    value: A,
    private[RowMerger] bytesRead: Int,
    private[RowMerger] isRemoval: Boolean = false
  )

  object Entry {

    /** Allows to dereference `A` from `Entry[A]`. Usefull for predicate functions. */
    implicit def viewAs[A](entry: Entry[A]): A = entry.value

    /** Allow entries to behave as mutations for conflict resolution. */
    private[RowMerger] final class Mut[A <: Mutation](val unwrap: Entry[A])
        extends AnyVal
        with Mutation {
      def ts = unwrap.value.ts
      def action = unwrap.value.action
      def isChange = unwrap.value.isChange
    }

    /** Sort entries wrapping cells by name. */
    private[RowMerger] implicit def cellEntryOrdering(
      implicit ord: Ordering[ByteBuf]) =
      Ordering.by[Entry[Cell], ByteBuf] { _.value.name }
  }

  final class BytesCounter(private val _bytes: LongAdder = new LongAdder)
      extends AnyVal {
    def bytes = _bytes.sum().toInt
    private[RowMerger] def incr(entry: Entry[_]) =
      _bytes.add(entry.bytesRead)
  }

  def empty[A] = new RowMerger(MultiPage.empty[Entry[A]])

  def apply(
    columnFamily: String,
    diskCells: MultiPage[Cell],
    writeIntents: Iterable[Write]
  )(implicit nameOrdering: Ordering[ByteBuf]): RowMerger[Cell] = {

    implicit val ec = ImmediateExecutionContext

    @inline def diskEntries =
      diskCells mapValuesT { c =>
        Entry(c, c.byteSize)
      }

    if (writeIntents.isEmpty) {
      new RowMerger(diskEntries)
    } else {
      // Sort write intents and keep track of their removal flag. Cells derived from
      // removal write ops are logical and, unlike traditional tombstones, don't
      // carry any information about them being removals. This algorithm can't merge
      // logical deletes unless it knows they are removals beforehand, hence, the
      // write's `isRemove` flag must be tracked by the `Entry` class.
      val sortedIntents = SortedSet.empty[Entry[Cell]]
      val iter = writeIntents.iterator

      while (iter.hasNext) {
        val write = iter.next()
        if (write.isClearRow) {
          // Clear rows are atemporal. A clear row write means the entire set of
          // on-disk cells will be removed.
          return empty
        } else {
          write.toPendingCell(columnFamily) foreach { cell =>
            // No bytes read since this cell comes from in-memory pending writes.
            sortedIntents.add(Entry(cell, bytesRead = 0, isRemoval = write.isRemove))
          }
        }
      }

      // Merge write intents with on-disk cells in cell name order. If a logical
      // removal is found, carry its cell forward so it finds the cell it's intended
      // to remove. Otherwise, include live cells in the result and reset the removed
      // cell tracking.
      //
      // NB. Ordering is important! The leftmost slice must contain cells derived
      // from write intents so that they override equivalent cells upon merging.
      val mergedCells =
        Page.mergeReduce[Future, Entry[Cell], Entry[Cell], Option[Entry[Cell]]](
          pages = Seq(MultiPage(sortedIntents), diskEntries),
          state = None
        ) {
          case (None, _) => (Nil, None) // End of merge.
          case (Some((e0, _)), _) if e0.isRemoval =>
            (Nil, Some(e0)) // Removal found.
          case (Some((e0, _)), None) => (List(e0), None) // Live cell.
          case (Some((e0, _)), Some(e1)) => // Live and removal cells found.
            if (nameOrdering.equiv(e0.value.name, e1.value.name)) {
              (Nil, None) // Live cell is intended for removal. Skip it.
            } else {
              (List(e0), None) // Live cell is NOT intended for removal. Add it.
            }
        }

      new RowMerger(mergedCells)
    }
  }

  /** Returns the result of calling `fn` on the given `merger` along side a counter for
    * the number of bytes that passed through the merger but may or may not be
    * discarded by the mapping function `fn`.
    *
    * Note that the `BytesCounter` returned is incremented as data passes by.
    * Therefore, this counter is only stable after the `RowMerger`'s materialization
    * is done via `take(..)` or `flatten(..)`.
    */
  @inline def counted[A, B](merger: RowMerger[A])(
    fn: RowMerger[A] => B): (B, BytesCounter) = {
    val (merger0, counter) = merger.counted
    (fn(merger0), counter)
  }
}

final class RowMerger[A] private (val entries: MultiPage[RowMerger.Entry[A]])
    extends AnyVal {

  import RowMerger._

  @inline private implicit def ec = ImmediateExecutionContext

  /** Maps merged entries from `A` to `B`. */
  def mapT[B](fn: A => B): RowMerger[B] = {
    val next = entries mapValuesT { e => e.copy(value = fn(e.value)) }
    new RowMerger(next)
  }

  /** Reduces merged entries from `A` to `B`. Note that the standard reduceStream
    * function for pages allows the reduce function to produce multiple `B` values
    * for each `A` value. However, this is a problem for entries, because it's not
    * clear how to apportion the bytes read between multiple `B`'s. Therefore,
    * RowMerger's reduceStream requires the reduce function to produce at most one
    * value.
    */
  def reduceStream[B, S](seed: S)(
    fn: (Option[Entry[A]], S) => (Option[Entry[B]], S)): RowMerger[B] = {
    val reduced =
      entries.reduceStreamT(seed) { case (a, s) =>
        val (b, s0) = fn(a, s)
        (b.toList, s0)
      }
    new RowMerger(reduced)
  }

  /** Selects entries from the merger whose value satisfies the predicate. */
  def select(pred: Entry[A] => Boolean): RowMerger[A] =
    new RowMerger(entries selectT pred)

  /** Rejects entries from the merger whose value satisfies the predicate. */
  def reject(pred: Entry[A] => Boolean): RowMerger[A] =
    new RowMerger(entries rejectT pred)

  /** Takes the prefix of entries whose values all satisfy the predicate. */
  def takeWhile(pred: Entry[A] => Boolean): RowMerger[A] =
    new RowMerger(entries takeWhileT pred)

  /** Drops the prefix of entries whose values all satisfy the predicate. */
  def dropWhile(pred: Entry[A] => Boolean): RowMerger[A] =
    new RowMerger(entries dropWhileT pred)

  /** Resolve conflicts on merged mutations, preserving canonical versions only. */
  def resolveConflicts[B <: Mutation](equiv: Equiv[B])(
    implicit ev0: A =:= B,
    ev1: Entry[A] =:= Entry[B]): RowMerger[B] = {

    val next =
      entries alignByT { (a, b) =>
        equiv.equiv(a.value, b.value)
      } mapT { entries =>
        val mutations = entries.view map { new Entry.Mut[B](_) }
        val conflicts = Conflict.resolve(mutations) { (a, b) =>
          equiv.equiv(a.unwrap.value, b.unwrap.value)
        }
        conflicts map { _.canonical.unwrap }
      }

    new RowMerger(next)
  }

  /** Finalize the merge operation by fetching at most `count` merged items from the
    * available disk cells or write intents. Reports the number of bytes read from
    * on-disk cells to generate the merged result.
    */
  def take(count: Int): Future[Result[A]] =
    entries.takeT(count).flattenT map { prepareResult(_) }

  /** Finalize the merge operation by fetching all results. Reports the
    * number of bytes read from on-disk cells to generate the merged result.
    */
  def flatten(): Future[Result[A]] =
    entries.flattenT map { prepareResult(_) }

  private def prepareResult(entries: Seq[Entry[A]]) = {
    val values = Vector.newBuilder[A]
    var bytesEmitted = 0

    entries foreach { entry =>
      values += entry.value
      bytesEmitted += entry.bytesRead
    }

    Result(values.result(), bytesEmitted)
  }

  /** Returns a new row merger and a bytes counter from which all bytes read are
    * aggregated into as they pass by the merger. Be aware that the total amount of
    * bytes read from disk is only available after the merge is complete via `take`
    * or `flatten`. Moreover, entries discarded via `select` or `reject` prior to
    * calling this method won't be counted.
    */
  private def counted: (RowMerger[A], BytesCounter) = {
    val counter = new BytesCounter()
    val next = entries mapValuesT { entry =>
      counter.incr(entry)
      entry
    }
    (new RowMerger(next), counter)
  }

}
