package fauna.storage.api.set

import fauna.atoms._
import fauna.codex.cbor._
import fauna.exec._
import fauna.lang._
import fauna.scheduler.PriorityGroup
import fauna.storage.{ Storage => _, _ }
import fauna.storage.api._
import fauna.storage.index._
import fauna.storage.ops.Write
import scala.concurrent.{ ExecutionContext, Future }

object SetSortedValues {

  /** Specifies a range for a values-sorted set.
    *
    * The bounds' tuples MUST be padded. See IndexConfig.pad. Failure to pad will
    * result in incorrect results!
    *
    * TODO: It's a major headache that the caller needs to pad the element, but
    * IndexConfig lives in repo, which depends on this project.
    */
  final case class Cursor(
    max: IndexValue = IndexValue.MaxValue,
    min: IndexValue = IndexValue.MinValue,
    order: Order = Order.Descending)
      extends IndexValueCursor[Cursor](IndexValue.ByValueOrdering) {

    protected def withCopy(max: IndexValue, min: IndexValue) =
      copy(max = max, min = min)
  }

  object Cursor {
    implicit val Codec = CBOR.TupleCodec[Cursor]
  }

  implicit val Codec = CBOR.TupleCodec[SetSortedValues]

  /** A set sorted values result, which is a slice of a events in a set in
    * sorted-index-order or reversed-sorted-index-order. Consumers can
    * obtain the next slice of results by running `next`, which is
    * present if there may be more elements in the set.
    */
  final case class Result(
    values: Vector[Element],
    next: Option[SetSortedValues],
    lastModifiedTS: Timestamp,
    lastAppliedTS: Timestamp,
    bytesRead: Int)
      extends Read.Result

  object Result {
    implicit val Codec = CBOR.TupleCodec[Result]
  }

  // This is an arbitrary number. Perhaps it could be tuned?
  val DefaultMaxResults = 64
}

/** A collection of events of a set in values-order.
  *
  * The default order orders result from highest index values to lowest index
  * values. The snapshot begins at `cursor`. The tuple in the cursor must be
  * padded (see IndexConfig.pad).
  *
  * Note that (maintaining parity with the classic API), sorted values does not
  * enforce document TTLs.
  *
  * Requirements:
  * * `maxResults` > 0
  * * `minValidTS` <= `snapshotTS`
  */
final case class SetSortedValues(
  scopeID: ScopeID,
  indexID: IndexID,
  terms: Vector[Term],
  cursor: SetSortedValues.Cursor,
  snapshotTS: Timestamp,
  minValidTS: MVTMap = MVTMap.Default,
  maxResults: Int = SetSortedValues.DefaultMaxResults)
    extends Read[SetSortedValues.Result] {
  import SetSortedValues._

  require(maxResults > 0, "must request a positive number of results")
  require(
    minValidTS.all forall { _ <= snapshotTS },
    "all min. valid times must be greater than snapshot time"
  )

  def codec = Result.Codec

  def name = "Set.SortedValues"

  override def toString: String =
    s"SetSortedValues($scopeID, $indexID, $terms, $cursor, snapshotTS=$snapshotTS, " +
      s"minValidTS=$minValidTS, maxResults=$maxResults)"

  @inline def columnFamily = Tables.SortedIndex.CFName
  @inline def schema = Tables.SortedIndex.Schema
  lazy val rowKey = Tables.Indexes.rowKey(scopeID, indexID, terms map { _.unwrap })

  private implicit def nameOrd =
    cursor.order match {
      case Order.Ascending  => schema.nameOrdering.reverse
      case Order.Descending => schema.nameOrdering
    }

  def isRelevant(write: Write): Boolean =
    write.isRelevant(columnFamily, rowKey, from, to)

  /** Force the encoding of internal C* row keys and predicates. This is a workaround
    * to fail queries gracefully at the compute notes due to C*'s 32k limit on
    * partition keys. By forcing the read op's encoding at the compute layer, the
    * system can properly catch and handle the `ComponentTooLargeException` emitted
    * by the C*'s codec.
    */
  @throws[ComponentTooLargeException]("If C* partition keys are too large.")
  def forceEncodable(): SetSortedValues = {
    cRowKey // NB: proxy to force the encoding of C* predicates.
    this
  }

  // Storage-level row key and predicate. Selects _all_ events in range.
  private lazy val (cRowKey, from, to) = {
    val (from, to) = cursor.inDiskBounds(minValidTS.all.max)
    val key = IndexKey(scopeID, indexID, terms map { t => IndexTerm(t.unwrap) })
    val fromPred = Tables.SortedIndex.toKey(key, from)
    val toPred = Tables.SortedIndex.toKey(key, to)
    setupPredicates(schema, fromPred, toPred, cursor.order)
  }

  private[api] def run(
    source: HostID,
    ctx: Storage.Context,
    priority: PriorityGroup,
    writes: Iterable[Write],
    deadline: TimeBound
  )(implicit ec: ExecutionContext): Future[SetSortedValues.Result] = {
    require(writes forall { isRelevant(_) }, "irrelevant writes found")

    val mergerF = read(ctx.engine, priority, writes, deadline)

    implicit val iec = ImmediateExecutionContext
    mergerF flatMap { case (page, bytesRead, merger) =>
      // Overscan by one to get the next page's bound.
      merger.take(maxResults + 1) map { results =>
        val elements =
          results.values.view
            .take(maxResults)
            .map { Element(_) }
            .toVector

        val next =
          Option.when(results.values.sizeIs > maxResults) {
            // NB: Elements returned from Cassandra scans will automatically be
            // properly padded.
            SetSortedValues(
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

        ctx.stats.count("SetSortedValues.BytesInOutput", bytesInOutput)
        ctx.stats.count("SetSortedValues.TotalBytesRead", totalBytesRead)

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

  private[set] def read(
    engine: StorageEngine,
    priority: PriorityGroup,
    writes: Iterable[Write],
    deadline: TimeBound
  )(implicit ec: ExecutionContext) = {
    require(writes forall { isRelevant(_) }, "irrelevant writes found")

    val rowPageF =
      RowPaginator(
        engine,
        columnFamily,
        rowKey,
        from,
        to,
        maxResults + 1, // Overscan by one to get the next page's bound.
        cursor.order,
        priority,
        snapshotTS,
        deadline
      )

    implicit val iec = ImmediateExecutionContext
    rowPageF map { rowPage =>
      val (merged, bytesRead) =
        RowMerger.counted(RowMerger(columnFamily, rowPage.cells, writes)) { merger =>
          merger
            .mapT { cell =>
              type Key = Tables.SortedIndex.Key
              val values = schema.nameComparator.bytesToCValues(cell.name)
              val pred = Predicate(cRowKey, values)
              val value = new Value[Key](pred, cell.value, cell.ts)
              IndexValue.fromSortedValue(scopeID, value)
            }
            .resolveConflicts { IndexValue.ByValueEquiv }
        }

      (rowPage, bytesRead, applyGCRules(merged))
    }
  }

  /** Similarly to `DocHistory`, enforces inline GC by: returning ADDs and REMOVEs
    * above MVT; as well as returning the first ADD at or below MVT for each indexed
    * tuple.
    *
    * NOTE: The sorted index cf allows for two tuples to exist in the same valid time
    * when an update to its TTL field happens. This is NOT treated as a conflict,
    * therefore these index values won't be resolved by conflict resolution prior to
    * applying inline GC. When encountering two index values at the same valid time,
    * the ADD should win.
    */
  private def applyGCRules(merger: RowMerger[IndexValue]) = {
    val sentinel =
      RowMerger.Entry(
        CollectionAtTSSentinel.atValidTS(Timestamp.Min),
        bytesRead = 0
      )

    def mvt(entry: RowMerger.Entry[IndexValue]) =
      if (entry == sentinel) {
        Timestamp.Epoch
      } else {
        minValidTS.getOrFail(entry.docID.collID)
      }

    def maybeEmit(entry: RowMerger.Entry[IndexValue]) =
      Option.when(
        cursor.contains(entry) && (entry.ts.validTS > mvt(entry) || (
          entry.isCreate && entry.tuple.ttl.forall { _ >= mvt(entry) }
        ))
      )(entry)

    def shouldChooseBetween(
      a: RowMerger.Entry[IndexValue],
      b: RowMerger.Entry[IndexValue]) =
      a.ts.validTS == b.ts.validTS || (
        a.ts.validTS <= mvt(a) &&
          b.ts.validTS <= mvt(b)
      )

    def choose(a: RowMerger.Entry[IndexValue], b: RowMerger.Entry[IndexValue]) =
      if (a.ts.validTS == b.ts.validTS && a.isCreate) a
      else if (a.ts.validTS > b.ts.validTS) a
      else b

    merger.reduceStream(sentinel) {
      case (Some(a), b) if !a.tuple.equiv(b.tuple)   => (maybeEmit(b), a)
      case (Some(a), b) if shouldChooseBetween(a, b) => (None, choose(a, b))
      case (Some(a), b)                              => (maybeEmit(b), a)
      case (None, b)                                 => (maybeEmit(b), b)
    }
  }
}
