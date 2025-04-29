package fauna.storage.api.set

import fauna.atoms._
import fauna.codex.cbor._
import fauna.exec.ImmediateExecutionContext
import fauna.lang._
import fauna.lang.syntax._
import fauna.scheduler.PriorityGroup
import fauna.storage.{ Storage => _, _ }
import fauna.storage.api._
import fauna.storage.index._
import fauna.storage.ops.Write
import scala.concurrent.{ ExecutionContext, Future }

object SetSnapshot {

  /** Specifies a range for a set snapshot.
    *
    * The tuple MUST be padded. See IndexConfig.pad. Failure to pad will result in
    * incorrect snapshot results!
    *
    * TODO: It's a major headache that the caller needs to pad the element, but
    * IndexConfig lives in repo, which depends on this project.
    */
  final case class Cursor(
    max: IndexTuple = IndexTuple.MaxValue,
    min: IndexTuple = IndexTuple.MinValue,
    order: Order = Order.Descending)
      extends Read.Cursor[IndexTuple, Cursor] {

    protected def withCopy(max: IndexTuple, min: IndexTuple) =
      copy(max = max, min = min)
  }

  object Cursor {
    implicit val Codec = CBOR.TupleCodec[Cursor]
  }

  implicit val Codec = CBOR.TupleCodec[SetSnapshot]

  /** A set snapshot result, which is a slice of a snapshot of a set in
    * sorted-index-order or reversed-sorted-index-order. Consumers can
    * obtain the next slice of results by running `next`, which is
    * present if there may be more elements in the snapshot.
    */
  final case class Result(
    values: Vector[Element.Live],
    next: Option[SetSnapshot],
    lastModifiedTS: Timestamp,
    lastAppliedTS: Timestamp,
    bytesRead: Int)
      extends Read.Result

  object Result {
    implicit val Codec = CBOR.TupleCodec[Result]
  }

  // This is an arbitrary number. Perhaps it could be tuned?
  val DefaultMaxResults = 64

  private val LargeReadThresholdConfig =
    "fauna.storage-api.large-read-threshold-bytes"

  // Reads above this threshold will be logged for debugging.
  private val LargeReadThresholdBytes =
    Option(System.getProperty(LargeReadThresholdConfig)) flatMap { _.toIntOption }

  private lazy val log = getLogger()
}

/** A snapshot of a set at a particular valid time.
  *
  * A set snapshot returns the membership of a set as of a given valid time. The
  * default order orders result from highest index values to lowest index values.
  * The snapshot begins at `cursor`. The tuple in the cursor must be padded
  * (see `SetPadding.pad(..)`).
  *
  * Requirements:
  * * `maxResults` > 0
  * * `minValidTS` <= `snapshotTS`
  * * `minValidTS` >= `validTS`
  */
final case class SetSnapshot(
  scopeID: ScopeID,
  indexID: IndexID,
  terms: Vector[Term],
  cursor: SetSnapshot.Cursor,
  snapshotTS: Timestamp,
  minValidTS: MVTMap = MVTMap.Default,
  validTS: Option[Timestamp] = None,
  maxResults: Int = SetSnapshot.DefaultMaxResults
) extends Read[SetSnapshot.Result] {
  import SetSnapshot._

  require(maxResults > 0, "must request a positive number of results")
  require(minValidTS.all forall { snapshotTS >= _ }, "mvt below snapshot time")
  validTS foreach { ReadValidTimeBelowMVT.maybeThrow(_, minValidTS) }

  def codec = Result.Codec

  def name = "Set.Snapshot"

  override def toString: String =
    s"SetSnapshot($scopeID, $indexID, $terms, $cursor, snapshotTS=$snapshotTS, " +
      s"minValidTS=$minValidTS, validTS=$validTS, maxResults=$maxResults)"

  @inline private def readTS =
    validTS.getOrElse(BiTimestamp.UnresolvedSentinel)

  /** Force the encoding of internal C* row keys and predicates. This is a workaround
    * to fail queries gracefully at the compute notes due to C*'s 32k limit on
    * partition keys. By forcing the read op's encoding at the compute layer, the
    * system can properly catch and handle the `ComponentTooLargeException` emitted
    * by the C*'s codec.
    */
  @throws[ComponentTooLargeException]("If C* partition keys are too large.")
  def forceEncodable(): SetSnapshot = {
    sortedRead.forceEncodable()
    this
  }

  private lazy val sortedRead =
    SetSortedValues(
      scopeID,
      indexID,
      terms,
      SetSortedValues.Cursor(
        IndexValue.MaxValue.withTuple(cursor.max).atValidTS(readTS),
        IndexValue.MinValue.withTuple(cursor.min),
        cursor.order
      ),
      snapshotTS,
      minValidTS,
      maxResults
    )

  def columnFamily = sortedRead.columnFamily
  def schema = sortedRead.schema
  def rowKey = sortedRead.rowKey

  def isRelevant(write: Write): Boolean =
    sortedRead.isRelevant(write)

  private[api] def run(
    source: HostID,
    ctx: Storage.Context,
    priority: PriorityGroup,
    writes: Iterable[Write],
    deadline: TimeBound
  )(implicit ec: ExecutionContext): Future[SetSnapshot.Result] = {
    require(writes forall { isRelevant(_) }, "irrelevant writes found")

    val mergerF = sortedRead.read(ctx.engine, priority, writes, deadline)

    implicit val iec = ImmediateExecutionContext
    mergerF flatMap { case (page, bytesRead, merger) =>
      // We must charge for bytes read but discarded by the snapshot computation.
      val (resultsF, bytesPreReduce) =
        RowMerger.counted(merger) { merger =>
          // Overscan by one to get the next page's bound.
          reduceSnapshot(merger)
            .reject { _.tuple.ttl exists { _ <= snapshotTS } }
            .take(maxResults + 1)
        }

      resultsF map { results =>
        val elements =
          results.values.view
            .take(maxResults)
            .map { Element.Live(_) }
            .toVector

        val next =
          Option.when(results.values.sizeIs > maxResults) {
            // NB: Elements returned from Cassandra scans will automatically be
            // properly padded.
            SetSnapshot(
              scopeID,
              indexID,
              terms,
              cursor.next(results.values.last.tuple),
              snapshotTS,
              minValidTS,
              validTS,
              maxResults
            )
          }

        val rowKeyBytes = rowKey.readableBytes
        val bytesInOutput = rowKeyBytes + results.bytesEmitted
        val bytesBeforeSnapshot = rowKeyBytes + bytesPreReduce.bytes
        val totalBytesRead = rowKeyBytes + bytesRead.bytes

        ctx.stats.count("SetSnapshot.BytesInOutput", bytesInOutput)
        ctx.stats.count("SetSnapshot.TotalBytesRead", totalBytesRead)
        ctx.stats.count("SetSnapshot.BytesBeforeSnapshot", bytesBeforeSnapshot)

        LargeReadThresholdBytes foreach { threshold =>
          if (totalBytesRead >= threshold && log.isInfoEnabled) {
            val total = totalBytesRead.toDouble
            val inlineGCDrop = (total - bytesBeforeSnapshot) / total
            val totalDrop = (total - bytesInOutput) / total

            log.info(
              s"Large read found: bytes=[$totalBytesRead/$bytesBeforeSnapshot/$bytesInOutput] " +
                f"drop=[$inlineGCDrop%.2f/$totalDrop%.2f] source=$source read=$this"
            )
          }
        }

        Result(
          elements,
          next,
          page.lastModifiedTS,
          ctx.engine.appliedTimestamp,
          bytesBeforeSnapshot
        )
      }
    }
  }

  /** Produces a set snapshot below or at `readTS` by coalescing ADDs and REMOVEs for
    * the same tuple together. Only surving ADD events are returned.
    */
  private def reduceSnapshot(merger: RowMerger[IndexValue]) = {
    def maybeEmit(entry: RowMerger.Entry[IndexValue]) =
      Option.when(entry.isCreate && entry.ts.validTS <= readTS)(entry)

    def bothBelowReadTS(
      a: RowMerger.Entry[IndexValue],
      b: RowMerger.Entry[IndexValue]) =
      a.ts.validTS <= readTS && b.ts.validTS <= readTS

    def choose(a: RowMerger.Entry[IndexValue], b: RowMerger.Entry[IndexValue])(
      cmp: (Timestamp, Timestamp) => Boolean) =
      if (cmp(a.ts.validTS, b.ts.validTS)) a else b

    val sentinel =
      RowMerger.Entry(
        CollectionAtTSSentinel,
        bytesRead = 0
      )

    merger.reduceStream(sentinel) {
      case (Some(a), b) if !a.tuple.equiv(b.tuple) => (maybeEmit(b), a)
      case (Some(a), b) if bothBelowReadTS(a, b)   => (None, choose(a, b)(_ >= _))
      case (Some(a), b)                            => (None, choose(a, b)(_ < _))
      case (None, b)                               => (maybeEmit(b), b)
    }
  }
}
