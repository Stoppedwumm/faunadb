package fauna.storage.api.set

import fauna.atoms._
import fauna.codex.cbor._
import fauna.exec.ImmediateExecutionContext
import fauna.lang._
import fauna.scheduler.PriorityGroup
import fauna.storage.{ Storage => _, _ }
import fauna.storage.api._
import fauna.storage.index.{ IndexKey, IndexTerm, IndexValue }
import fauna.storage.ops.Write
import scala.concurrent.{ ExecutionContext, Future }

object SetHistory {

  /* Set history's range cursor. */
  final case class Cursor(
    max: IndexValue = IndexValue.MaxValue,
    min: IndexValue = IndexValue.MinValue,
    order: Order = Order.Descending)
      extends IndexValueCursor[Cursor](IndexValue.ByEventOrdering) {

    protected def withCopy(max: IndexValue, min: IndexValue) =
      copy(max = max, min = min)
  }

  object Cursor {
    implicit val Coded = CBOR.TupleCodec[Cursor]
  }

  implicit val Codec = CBOR.TupleCodec[SetHistory]

  /** A set history read result, which consists of a page of the set's history and
    * the set history operation that fetches the next page of results.
    */
  final case class Result(
    values: Vector[Element],
    next: Option[SetHistory],
    lastModifiedTS: Timestamp,
    lastAppliedTS: Timestamp,
    bytesRead: Int
  ) extends Read.Result

  object Result {
    implicit val Codec = CBOR.TupleCodec[Result]
  }

  // This is an arbitrary number. Perhaps it could be tuned?
  val DefaultMaxResults = 64

  /** Builds an unpadded set history between two valid times. */
  def apply(
    scopeID: ScopeID,
    indexID: IndexID,
    terms: Vector[Term],
    snapshotTS: Timestamp,
    minValidTS: MVTMap = MVTMap.Default,
    maxTS: Timestamp = Timestamp.MaxMicros,
    minTS: Timestamp = Timestamp.Epoch,
    order: Order = Order.Descending,
    maxResults: Int = DefaultMaxResults
  ): SetHistory =
    new SetHistory(
      scopeID,
      indexID,
      terms,
      Cursor(
        IndexValue.MaxValue.copy(ts = AtValid(maxTS)),
        IndexValue.MinValue.copy(ts = AtValid(minTS)),
        order
      ),
      snapshotTS,
      minValidTS,
      maxResults
    )
}

/** A read of a history of a set.
  *
  * A history read returns the events, both add and remove, in the history of the
  * set, between the bounds `from` and `to`, in the order specified by `order`, as of
  * the snapshot time. The order is relative to the natural valid-time ordering of
  * events, so descending order is from most-recent to least-recent.
  *
  * Requirements:
  * * `maxResults` > 0
  * * `minValidTS` <= `snapshotTS`
  *
  * Set history is TRUNCATED based on the following rules:
  * - An event at MVT is preserved if it's an ADD;
  * - Other events at or below MVT are discarded.
  *
  * This behavior preserves some parity with document history where CREATE events at
  * MVT are preserved but DELETE events are discarded. See `DocHistory` for details.
  */
final case class SetHistory(
  scopeID: ScopeID,
  indexID: IndexID,
  terms: Vector[Term],
  cursor: SetHistory.Cursor,
  snapshotTS: Timestamp,
  minValidTS: MVTMap,
  maxResults: Int
) extends Read[SetHistory.Result] {
  import SetHistory._

  require(maxResults > 0, "must request a positive number of results")
  require(
    minValidTS.all forall { _ <= snapshotTS },
    "min. valid time greater than snapshot time"
  )

  def codec = Result.Codec

  def name: String = "Set.History"

  override def toString: String =
    s"SetHistory($scopeID, $indexID, $terms, $cursor, snapshotTS=$snapshotTS, " +
      s"minValidTS=$minValidTS maxResults=$maxResults)"

  lazy val rowKey = Tables.Indexes.rowKey(scopeID, indexID, terms map { _.unwrap })
  def columnFamily = Tables.HistoricalIndex.CFName
  @inline private def schema = Tables.HistoricalIndex.Schema

  private implicit def nameOrd =
    cursor.order match {
      case Order.Ascending  => schema.nameOrdering.reverse
      case Order.Descending => schema.nameOrdering
    }

  // Storage level cursors, truncated at min. valid time (inclusively).
  private lazy val (cRowKey, fromP, toP) = {
    val (from, to) = cursor.inDiskBounds(minValidTS.all.max, truncated = true)
    val indexKey = IndexKey(scopeID, indexID, terms map { t => IndexTerm(t.unwrap) })
    val fromPred = Tables.HistoricalIndex.toKey(indexKey, from)
    val toPred = Tables.HistoricalIndex.toKey(indexKey, to)
    setupPredicates(schema, fromPred, toPred, cursor.order)
  }

  def isRelevant(write: Write): Boolean =
    write.isRelevant(columnFamily, rowKey, fromP, toP)

  private[api] def run(
    source: HostID,
    ctx: Storage.Context,
    priority: PriorityGroup,
    writes: Iterable[Write],
    deadline: TimeBound
  )(implicit ec: ExecutionContext): Future[Result] = {
    require(writes forall { isRelevant(_) }, "irrelevant writes found")

    val mergerF = read(ctx.engine, priority, writes, deadline)

    implicit val iec = ImmediateExecutionContext
    mergerF flatMap { case (page, bytesRead, merger) =>
      merger.take(maxResults + 1) map { results =>
        val elements =
          results.values.view
            .take(maxResults)
            .map { Element(_) }
            .toVector

        val next =
          Option.when(results.values.sizeIs > maxResults) {
            SetHistory(
              scopeID,
              indexID,
              terms,
              cursor.next(results.values.last),
              snapshotTS,
              minValidTS,
              maxResults
            )
          }

        val rowKeyBytes = rowKey.readableBytes
        val bytesInOutput = rowKeyBytes + results.bytesEmitted
        val totalBytesRead = rowKeyBytes + bytesRead.bytes

        ctx.stats.count("SetHistory.BytesInOutput", bytesInOutput)
        ctx.stats.count("SetHistory.TotalBytesRead", totalBytesRead)

        Result(
          elements,
          next,
          page.lastModifiedTS,
          ctx.engine.appliedTimestamp,
          bytesInOutput
        )
      }
    }
  }

  private def read(
    engine: StorageEngine,
    priority: PriorityGroup,
    writes: Iterable[Write],
    deadline: TimeBound
  )(implicit ec: ExecutionContext) = {

    val rowPageF =
      RowPaginator(
        engine,
        columnFamily,
        rowKey,
        fromP,
        toP,
        maxResults + 1, // Overscan by one to get the next page's bound.
        cursor.order,
        priority,
        snapshotTS,
        deadline
      )

    implicit val iec = ImmediateExecutionContext
    rowPageF map { rowPage =>
      val (merger, bytesRead) =
        RowMerger.counted(RowMerger(columnFamily, rowPage.cells, writes)) { merger =>
          merger
            .mapT { cell =>
              type Key = Tables.HistoricalIndex.Key
              val values = schema.nameComparator.bytesToCValues(cell.name)
              val pred = Predicate(cRowKey, values)
              val value = new Value[Key](pred, cell.value, cell.ts)
              IndexValue.fromHistoricalValue(scopeID, value)
            }
            .resolveConflicts(IndexValue.ByValueEquiv)
            .select { value =>
              val validTS = value.ts.validTS
              val mvt = minValidTS.getOrFail(value.docID.collID)
              validTS > mvt || (validTS == mvt && value.isCreate)
            }
        }
      (rowPage, bytesRead, merger)
    }
  }
}
