package fauna.storage.api.version

import fauna.atoms._
import fauna.codex.cbor.CBOR
import fauna.exec.ImmediateExecutionContext
import fauna.lang.{ TimeBound, Timestamp }
import fauna.scheduler.PriorityGroup
import fauna.storage.{ Storage => _, _ }
import fauna.storage.api._
import fauna.storage.ops.{ VersionAdd, Write }
import scala.concurrent.{ ExecutionContext, Future }

object DocSnapshot {

  /** A snapshot read result, which consists principally of the snapshot version
    * of the document, if it is not deleted.
    */
  final case class Result(
    version: Option[StorageVersion],
    lastModifiedTS: Timestamp = Timestamp.Epoch,
    lastAppliedTS: Timestamp = Timestamp.Epoch,
    bytesRead: Int = 0
  ) extends Read.Result

  object Result {
    implicit val Codec = CBOR.TupleCodec[Result]
  }

  implicit val Codec = CBOR.TupleCodec[DocSnapshot]

  // Convenience builder for doc snapshots, which typically specify no valid TS or
  // just a validTS, and not an entire VersionID.
  def apply(
    scopeID: ScopeID,
    docID: DocID,
    snapshotTS: Timestamp,
    minValidTS: Timestamp = Timestamp.Epoch,
    validTS: Option[Timestamp] = None): DocSnapshot =
    DocSnapshot(
      scopeID,
      docID,
      snapshotTS,
      minValidTS,
      VersionID(
        validTS.getOrElse(BiTimestamp.UnresolvedSentinel),
        DocAction.MaxValue))
}

/** A snapshot read of a document.
  *
  * A snapshot read returns the state of the document at a particular moment
  * `validTS` in valid time, as of the transaction time `snapshotTS`. The valid
  * timestamp is optional and, if not supplied, the valid time of the snapshot is the
  * snapshot time. When a valid timestamp is supplied, the read cannot be invalidated
  * by live writes. Moreover, snapshot reads apply inline GC rules based on the given
  * min valid time. See `DocHistory` for details.
  *
  * Consider the following document history drawn on a valid-time timeline,
  * where the notation (v=1,t=2) signifies a bi-timestamp with a valid time
  * of 1 and a transaction time of 2.
  *
  * 0 ----- CREATE(x=0) --- UPDATE(x=1) --- DELETE --- CREATE(x=2) --- UPDATE(x=3) --->
  *         (v=1,t=1)       (v=2,t=7)       (v=3,t=3)  (v=4,t=5)       (v=6,t=6)
  *
  * Then the results of various snapshot reads would be:
  * Read@(v=latest,t=latest) => x=3
  * Read@(v=2,t=5) => x=0
  * Read@(v=2,t=latest) => x=1
  * Read@(v=3,t=latest) => DELETED
  * Read@(v=5,t=latest) => x=2
  * Read@(v=0,t=latest) => DELETED
  */
final case class DocSnapshot(
  scopeID: ScopeID,
  docID: DocID,
  snapshotTS: Timestamp,
  minValidTS: Timestamp,
  versionID: VersionID)
    extends Read[DocSnapshot.Result] {
  import DocSnapshot._

  require(minValidTS <= snapshotTS, "mvt higher than snapshot time")
  ReadValidTimeBelowMVT.maybeThrow(versionID.validTS, minValidTS, docID.collID)

  def name = "Doc.Snapshot"

  override def toString: String =
    s"DocSnapshot($scopeID, $docID, snapshotTS=$snapshotTS, versionID=$versionID, " +
      s"minValidTS=$minValidTS)"

  def codec = Result.Codec

  lazy val rowKey = historyOp.rowKey

  def columnFamily = Tables.Versions.CFName

  // The DocHistory op that is equivalent to this snapshot read. Defined here so we
  // can use it to implement `isRelevant` and `run`.
  //
  // NB: The `maxResults` value of 1 triggers a special-case efficient path
  //     in DocHistory.
  private[this] lazy val historyOp =
    DocHistory(
      scopeID,
      docID,
      DocHistory.Cursor(versionID),
      snapshotTS,
      minValidTS,
      maxResults = 1
    )

  def isRelevant(write: Write): Boolean = historyOp.isRelevant(write)

  /** This method is used to possibly skip a read from storage.
    * If during the transaction there was a write to a given document
    * then we don't need to issue a read to storage since we already
    * have the result as part of the write.
    * This method takes the relevant writes for a transaction and
    * returns the ReadResult if there is a matching Write. If there
    * is not a matching write it will return None.
    */
  override def skipIO(pendingWrites: Iterable[Write]): Option[Result] = {
    // we currently only skip io for non temporal snapshot reads
    if (versionID.validTS != Timestamp.MaxMicros) {
      None
    } else {
      var ret: Option[Result] = None
      val iter = pendingWrites.iterator
      while (iter.hasNext) {
        val write = iter.next()
        require(this.isRelevant(write))
        write match {
          case va: VersionAdd if va.writeTS == Unresolved =>
            if (ret.isDefined) {
              throw new IllegalStateException(
                s"Found multiple pending writes for the same document and write time, these should have been merged into a single write." +
                  s" ScopeID: ${va.scope} DocID: ${va.id}"
              )
            }

            val v = StorageVersion.fromDecoded(
              va.scope,
              va.id,
              Unresolved,
              va.action,
              va.schemaVersion,
              null,
              va.data,
              va.diff
            )

            val snap = Option.when(v.isLive && v.ttl.forall(_ > snapshotTS))(v)
            ret = Some(DocSnapshot.Result(snap))

          // If we find a non VersionAdd, just falling back to None here and allowing
          // the normal read path to work through it.
          case _ =>
            return None
        }
      }
      ret
    }
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
        historyOp.read(ctx.engine, priority, writes, deadline)
      }

    implicit val iec = ImmediateExecutionContext
    readF flatMap { case (page, bytesRead, merger) =>
      // We must charge for bytes read above MVT that didn't make into the output.
      val (resultsF, bytesPreSnapshot) = RowMerger.counted(merger) { _.take(1) }

      resultsF map { results =>
        val version =
          results.values.headOption flatMap { v =>
            Option.when(v.isLive && v.ttl.forall(_ > snapshotTS))(v)
          }

        val rowKeyBytes = rowKey.readableBytes
        val bytesInOutput = rowKeyBytes + results.bytesEmitted
        val bytesBeforeSnapshot = rowKeyBytes + bytesPreSnapshot.bytes
        val totalBytesRead = rowKeyBytes + bytesRead.bytes

        ctx.stats.count("DocSnapshot.BytesInOutput", bytesInOutput)
        ctx.stats.count("DocSnapshot.TotalBytesRead", totalBytesRead)
        ctx.stats.count("DocSnapshot.BytesBeforeSnapshot", bytesBeforeSnapshot)

        DocSnapshot.Result(
          version,
          page.lastModifiedTS,
          ctx.engine.appliedTimestamp,
          bytesBeforeSnapshot
        )
      }
    }
  }
}
