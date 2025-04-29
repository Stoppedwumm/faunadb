package fauna.storage.api.version

import fauna.atoms.{ DocID, HostID, ScopeID }
import fauna.codex.cbor.CBOR
import fauna.exec.ImmediateExecutionContext
import fauna.lang.{ TimeBound, Timestamp }
import fauna.scheduler.PriorityGroup
import fauna.storage.{ Storage => _, _ }
import fauna.storage.api._
import fauna.storage.api.version.StorageVersion
import fauna.storage.ops.{ VersionAdd, Write }
import scala.concurrent.{ ExecutionContext, Future }

object DocHistory {

  /** The history read's logical cursor. */
  final case class Cursor(
    max: VersionID = VersionID.MaxValue,
    min: VersionID = VersionID.MinValue,
    order: Order = Order.Descending)
      extends Read.Cursor.OnDisk[VersionID, Cursor] {

    protected def validTS(v: VersionID) = v.validTS

    protected def adjust(v: VersionID, ts: Timestamp, floor: Boolean) =
      v.copy(validTS = ts, action = if (floor) Create else Delete)

    protected def withCopy(max: VersionID, min: VersionID) =
      copy(max = max, min = min)
  }

  object Cursor {
    implicit val Codec = CBOR.TupleCodec[Cursor]
  }

  implicit val Codec = CBOR.TupleCodec[DocHistory]

  /** A history read result, which is a contiguous slice of the document's
    * history and the next operation the caller can run to get more results.
    */
  final case class Result(
    versions: Vector[StorageVersion],
    next: Option[DocHistory],
    lastModifiedTS: Timestamp,
    lastAppliedTS: Timestamp,
    bytesRead: Int)
      extends Read.Result

  object Result {
    implicit val Codec = CBOR.TupleCodec[Result]
  }

  // This is an arbitrary number. Perhaps it could be tuned?
  val DefaultMaxResults = 64

  // Copied from fauna.storage.doc.Version.conflictEquiv.
  private val RawVersionConflictEquiv: Equiv[StorageVersion] = { (a, b) =>
    a.scopeID.toLong == b.scopeID.toLong &&
    a.docID == b.docID && a.ts.validTS == b.ts.validTS
  }

  // Convenience builder for doc history between two valid times.
  def apply(
    scopeID: ScopeID,
    docID: DocID,
    maxTS: Timestamp,
    minTS: Timestamp,
    snapshotTS: Timestamp,
    maxResults: Int = DocHistory.DefaultMaxResults,
    order: Order = Order.Descending,
    minValidTS: Timestamp = Timestamp.Epoch
  ): DocHistory = {

    val cursor =
      Cursor(
        VersionID.MaxValue.copy(validTS = maxTS),
        VersionID.MinValue.copy(validTS = minTS),
        order
      )

    DocHistory(
      scopeID,
      docID,
      cursor,
      snapshotTS,
      minValidTS,
      maxResults
    )
  }
}

/** A history read of a document.
  *
  * A history read returns the sequence of versions comprising the history of the
  * document between two version IDs. Typically, it's used to find versions between
  * two timestamps. Moreover, historical reads apply inline GC rules based on the
  * given minimum valid time. See `applyGCRules` and `rewriteGCEdge` for details.
  *
  * The default order Descending orders results from most-recent to least-recent.
  *
  * Requirements:
  * * `maxResults` > 0
  * * `minValidTS` <= `snapshotTS`
  */
final case class DocHistory(
  scopeID: ScopeID,
  docID: DocID,
  cursor: DocHistory.Cursor,
  snapshotTS: Timestamp,
  minValidTS: Timestamp,
  maxResults: Int
) extends Read[DocHistory.Result] {
  import DocHistory._

  require(maxResults > 0, "must request a positive number of results")
  require(minValidTS <= snapshotTS, "min. valid time greater than snapshot time")

  def name = "Doc.History"

  override def toString: String =
    s"DocHistory($scopeID, $docID, $cursor, snapshotTS=$snapshotTS, " +
      s"minValidTS=$minValidTS, maxResults=$maxResults)"

  def codec = Result.Codec

  lazy val rowKey = Tables.Versions.rowKeyByteBuf(scopeID, docID)
  def columnFamily = Tables.Versions.CFName
  @inline private def schema = Tables.Versions.Schema

  implicit private def nameOrd =
    cursor.order match {
      case Order.Ascending  => schema.nameOrdering.reverse
      case Order.Descending => schema.nameOrdering
    }

  private lazy val (cRowKey, fromP, toP) = {
    val (from, to) = cursor.inDiskBounds(minValidTS)
    val fromPred = Predicate((rowKey, from.validTS, from.action))
    val toPred = Predicate((rowKey, to.validTS, to.action))
    setupPredicates(schema, fromPred, toPred, cursor.order)
  }

  private def cellToRawVersion(cell: Cell) = {
    val prefix = schema.nameComparator.bytesToCValues(cell.name)
    val pred = Predicate(cRowKey, prefix)
    val value = new Value[Tables.Versions.Key](pred, cell.value, cell.ts)
    StorageVersion.fromValue(value)
  }

  def isRelevant(write: Write): Boolean =
    write.isRelevant(columnFamily, rowKey, fromP, toP)

  /** DocHistory can skip it's IO when reading the latest version of a document as
    * long as the set of pending writes contains an unresolved VersionAdd to the same
    * document.
    */
  override def skipIO(pendingWrites: Iterable[Write]): Option[Result] = {
    var result: Result = null
    if (
      cursor.max == VersionID.MaxValue &&
      cursor.order == Order.Descending &&
      maxResults == 1
    ) {
      val iter = pendingWrites.iterator
      while (iter.hasNext) {
        val write = iter.next()
        require(isRelevant(write), s"irrelevant write found: $write")

        write match {
          case v: VersionAdd if v.writeTS == Unresolved =>
            require(result eq null, s"Unmerged write found: $write")
            result = Result(
              versions = Vector(
                StorageVersion.fromDecoded(
                  v.scope,
                  v.id,
                  Unresolved,
                  v.action,
                  v.schemaVersion,
                  // NB. TTL is not known at this time. Null forces decoding it from
                  // the document's data. See StorageVersion.
                  ttl = null,
                  v.data,
                  v.diff
                )
              ),
              // NB. Returns a cursor starting from previous version id so that if
              // continuing reading from the cursor we don't skip IO anymore. This is
              // safe given that VersionID.MaxValue is the edge of max time and its
              // its previous version is still surely above max writable timestamp.
              next = Some(
                this.copy(
                  cursor = cursor.copy(max = cursor.max.saturatingDecr)
                )),
              // NB. The following fields are unused in case of a skipped read.
              lastModifiedTS = Timestamp.Epoch,
              lastAppliedTS = Timestamp.Epoch,
              bytesRead = 0
            )
          case _ =>
            return None
        }
      }
    }
    Option(result)
  }

  private[api] def run(
    source: HostID,
    ctx: Storage.Context,
    priority: PriorityGroup,
    writes: Iterable[Write],
    deadline: TimeBound
  )(implicit ec: ExecutionContext): Future[Result] = {

    val readF =
      withMVTHint(ctx, scopeID, docID.collID, minValidTS) {
        read(ctx.engine, priority, writes, deadline)
      }

    implicit val iec = ImmediateExecutionContext
    readF flatMap { case (page, bytesRead, merger) =>
      // Overscan by one to get the next page's bound.
      merger.take(maxResults + 1) map { results =>
        val versions = results.values.take(maxResults)
        val next =
          Option.when(results.values.sizeIs > maxResults) {
            DocHistory(
              scopeID,
              docID,
              cursor.next(results.values.last.versionID),
              snapshotTS,
              minValidTS,
              maxResults
            )
          }

        val rowKeyBytes = rowKey.readableBytes
        val bytesInOutput = rowKeyBytes + results.bytesEmitted
        val totalBytesRead = rowKeyBytes + bytesRead.bytes

        ctx.stats.count("DocHistory.BytesInOutput", bytesInOutput)
        ctx.stats.count("DocHistory.TotalBytesRead", totalBytesRead)

        Result(
          versions,
          next,
          page.lastModifiedTS,
          ctx.engine.appliedTimestamp,
          bytesInOutput
        )
      }
    }
  }

  private[version] def read(
    engine: StorageEngine,
    priority: PriorityGroup,
    writes: Iterable[Write],
    deadline: TimeBound
  )(implicit ec: ExecutionContext) = {
    require(writes forall { isRelevant(_) }, "irrelevant writes found")

    val pageF =
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
    pageF map { page =>
      val (merger, bytesRead) =
        RowMerger.counted(RowMerger(columnFamily, page.cells, writes)) { merger =>
          merger
            .mapT { cellToRawVersion(_) }
            .resolveConflicts(DocHistory.RawVersionConflictEquiv)
        }

      (page, bytesRead, applyGCRules(merger))
    }
  }

  /** Detects and rewrite the GC edge while discarding versions below it.
    *
    * Note that in order to preserve the document's existence, live versions at the
    * edge of GC are maintained. At the same time, deleted versions at the edge of GC
    * are always discarded as it makes no sense for a document to start its history
    * with a delete. If a delete is followed by a create, then we presume the create
    * to NOT have a diff, thus, history consistency is maintained (the document
    * started on the create, NOT on the delete).
    *
    * We consider the surviving GC edge the document's GC root. Upon rewriting the GC
    * root, diffs are discarded at the GC root so that it no longer points to
    * versions below it.
    */
  private def applyGCRules(merger: RowMerger[StorageVersion]) = {
    @volatile var edge = null: RowMerger.Entry[StorageVersion]

    def isRetained(entry: RowMerger.Entry[StorageVersion]) = {
      val retained = entry.ts.validTS > minValidTS
      if (!retained) edge = entry
      retained
    }

    def rewriteGCEdge(entry: RowMerger.Entry[StorageVersion]) =
      Option.when(
        entry.isLive &&
          entry.ttl.forall { _ >= minValidTS }
      ) {
        entry.copy(
          value = entry.value.copy(
            action = Create,
            _diff = StorageVersion.EmptyDiff.duplicate()
          ))
      } filter { entry =>
        cursor.contains(entry.versionID)
      }

    cursor.order match {
      case Order.Descending =>
        merger
          .takeWhile { isRetained(_) }
          .reduceStream(()) {
            case (None, _) if edge ne null => (rewriteGCEdge(edge), ())
            case other                     => other
          }

      case Order.Ascending =>
        merger
          .dropWhile { !isRetained(_) }
          .reduceStream(Option.empty[RowMerger.Entry[StorageVersion]]) {
            case (curr, prev) =>
              if (edge eq null) {
                (prev, curr)
              } else {
                val res = (rewriteGCEdge(edge), curr)
                edge = null
                res
              }
          }
    }
  }
}
