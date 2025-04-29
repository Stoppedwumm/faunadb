package fauna.storage.api

import fauna.atoms._
import fauna.codex.cbor._
import fauna.exec._
import fauna.lang._
import fauna.scheduler.PriorityGroup
import fauna.storage.{ Storage => _, _ }
import fauna.storage.lookup.LookupEntry
import fauna.storage.ops.Write
import scala.concurrent.{ ExecutionContext, Future }

object Lookups {

  implicit val Codec = CBOR.TupleCodec[Lookups]

  final case class Result(
    values: Vector[LookupEntry],
    next: Option[Lookups],
    lastModifiedTS: Timestamp,
    lastAppliedTS: Timestamp,
    bytesRead: Int)
      extends Read.Result

  object Result {
    implicit val Codec = CBOR.TupleCodec[Result]
  }

  // :shrug:
  val DefaultMaxResults = 4096
}

final case class Lookups(
  start: LookupEntry,
  snapshotTS: Timestamp,
  maxResults: Int = Lookups.DefaultMaxResults)
    extends Read[Lookups.Result] {
  import Lookups._
  import RowPaginator._

  require(maxResults > 0, "must request a positive number of results")

  def codec = Result.Codec

  def name = "Lookups"

  override def toString: String =
    s"Lookups(start=$start, snapshotTS=$snapshotTS, maxResults=$maxResults)"

  // Not all global IDs are scope IDs...
  def scopeID: ScopeID = ScopeID.RootID

  def table = Tables.Lookups

  def columnFamily: String = table.CFName

  lazy val rowKey = CBOR.encode(start.globalID)

  implicit private def nameOrd = table.Schema.nameOrdering

  def isRelevant(write: Write): Boolean =
    write.isRelevant(columnFamily, rowKey, from, to)

  private lazy val (cRowKey, from, to) = {
    val fromPred = table.toKey(start)
    val toPred = table.toKey(LookupEntry.MinValue.copy(globalID = start.globalID))
    setupPredicates(table.Schema, fromPred, toPred, Order.Descending)
  }

  private def read(
    engine: StorageEngine,
    priority: PriorityGroup,
    writes: Iterable[Write],
    deadline: TimeBound
  )(implicit ec: ExecutionContext): Future[(RowPage, RowMerger[LookupEntry])] = {
    val rowPageF =
      RowPaginator(
        engine,
        table.CFName,
        rowKey,
        from,
        to,
        maxResults + 1, // Overscan by one to get the next page's bound.
        Order.Descending,
        priority,
        snapshotTS,
        deadline
      )

    implicit val iec = ImmediateExecutionContext
    rowPageF map { rowPage =>
      val merged =
        RowMerger(columnFamily, rowPage.cells, writes)
          .mapT { cell =>
            val values = table.Schema.nameComparator.bytesToCValues(cell.name)
            val pred = Predicate(cRowKey, values)
            val value = new Value[Tables.Lookups.Key](pred, cell.value, cell.ts)
            LookupEntry(value)
          }
          .resolveConflicts { LookupEntry.Order }

      (rowPage, merged)
    }
  }

  private[api] def run(
    source: HostID,
    ctx: Storage.Context,
    priority: PriorityGroup,
    writes: Iterable[Write],
    deadline: TimeBound
  )(implicit ec: ExecutionContext): Future[Lookups.Result] = {
    require(writes forall { isRelevant(_) }, "irrelevant writes found")

    val mergerF = read(ctx.engine, priority, writes, deadline)

    implicit val iec = ImmediateExecutionContext
    mergerF flatMap { case (page, merger) =>
      // Overscan by one to get the next page's bound.
      val resultsF =
        merger.take(maxResults + 1)

      resultsF map { results =>
        val next =
          Option.when(results.values.sizeIs > maxResults) {
            Lookups(
              results.values.last,
              snapshotTS,
              maxResults
            )
          }

        Result(
          results.values,
          next,
          page.lastModifiedTS,
          ctx.engine.appliedTimestamp,
          rowKey.readableBytes + results.bytesEmitted
        )
      }
    }
  }
}
