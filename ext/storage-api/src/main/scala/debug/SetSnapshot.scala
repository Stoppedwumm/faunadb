package fauna.storage.api.debug

import fauna.atoms._
import fauna.codex.cbor._
import fauna.exec.ImmediateExecutionContext
import fauna.lang._
import fauna.scheduler.PriorityGroup
import fauna.storage.{ Order, Predicate, Tables, Value }
import fauna.storage.api._
import fauna.storage.api.set.{ IndexValueCursor, Term }
import fauna.storage.index._
import fauna.storage.ops.Write
import io.netty.buffer._
import scala.concurrent.{ ExecutionContext, Future }

object SetSnapshot {
  val DefaultMaxResults = 64

  final case class Cursor(
    max: IndexValue = IndexValue.MaxValue,
    min: IndexValue = IndexValue.MinValue)
      extends IndexValueCursor[Cursor](IndexValue.ByValueOrdering) {

    def order = Order.Descending

    def withCopy(max: IndexValue, min: IndexValue) =
      copy(max = max, min = min)
  }

  final case class Result(values: Vector[IndexValue], next: Option[SetSnapshot])
      extends Read.Result {

    // Nothing to see here.
    def bytesRead = 0
    def lastModifiedTS = Timestamp.Epoch
    def lastAppliedTS = Timestamp.Epoch
  }

  @annotation.nowarn("cat=unused")
  def apply(
    scopeID: ScopeID,
    indexID: IndexID,
    terms: Vector[Term],
    cursor: SetSnapshot.Cursor,
    snapshotTS: Timestamp,
    maxResults: Int = SetSnapshot.DefaultMaxResults)(
    implicit ctl: ConsoleControl): SetSnapshot =
    new SetSnapshot(scopeID, indexID, terms, cursor, snapshotTS, maxResults)
}

final class SetSnapshot private (
  val scopeID: ScopeID,
  indexID: IndexID,
  terms: Vector[Term],
  cursor: SetSnapshot.Cursor,
  val snapshotTS: Timestamp,
  maxResults: Int
) extends Read[SetSnapshot.Result] {

  implicit def codec: CBOR.Codec[SetSnapshot.Result] =
    throw new IllegalStateException("Console-only reads cannot cross the network!")

  def name = "Set.Snapshot.Debug"

  @inline def columnFamily = Tables.SortedIndex.CFName
  @inline def schema = Tables.SortedIndex.Schema

  lazy val rowKey = Tables.Indexes.rowKey(scopeID, indexID, terms map { _.unwrap })

  def isRelevant(write: Write): Boolean = false

  private[api] def run(
    source: HostID,
    ctx: Storage.Context,
    priority: PriorityGroup,
    writes: Iterable[Write],
    deadline: TimeBound)(implicit ec: ExecutionContext): Future[SetSnapshot.Result] =
    run(ctx, deadline)

  def run(ctx: Storage.Context, deadline: TimeBound)(
    implicit ec: ExecutionContext): Future[SetSnapshot.Result] = {

    val (fromVal, toVal) = cursor.inDiskBounds(Timestamp.Epoch)
    val key = IndexKey(scopeID, indexID, terms map { t => IndexTerm(t.unwrap) })

    val (rowKey, from, to) =
      setupPredicates(
        schema,
        Tables.SortedIndex.toKey(key, fromVal),
        Tables.SortedIndex.toKey(key, toVal),
        Order.Descending)

    val rowPageF =
      RowPaginator(
        ctx.engine,
        columnFamily,
        Unpooled.wrappedBuffer(Tables.Indexes.rowKey(key)),
        from,
        to,
        maxResults + 1, // Overscan by one to get the next page's bound.
        Order.Descending,
        PriorityGroup.Root,
        snapshotTS,
        deadline
      )

    implicit val iec = ImmediateExecutionContext
    rowPageF flatMap { rowPage =>
      // Overscan by one to get the next page's bound.
      rowPage.cells
        .takeT(maxResults + 1)
        .mapValuesT { cell =>
          val values = schema.nameComparator.bytesToCValues(cell.name)
          val pred = Predicate(rowKey, values)
          val value = new Value[Tables.SortedIndex.Key](pred, cell.value, cell.ts)
          IndexValue.fromSortedValue(scopeID, value)
        } map { page =>
        val result = page.value.take(maxResults).toVector
        val next = page.value.drop(maxResults).headOption map { v =>
          cursor.copy(max = v)
        } map { cur =>
          new SetSnapshot(scopeID, indexID, terms, cur, snapshotTS, maxResults)
        }

        SetSnapshot.Result(result, next)
      }
    }
  }
}
