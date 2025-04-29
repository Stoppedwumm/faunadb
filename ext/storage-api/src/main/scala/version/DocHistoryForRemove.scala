package fauna.storage.api.version

import fauna.atoms._
import fauna.codex.cbor.CBOR
import fauna.exec.ImmediateExecutionContext
import fauna.lang._
import fauna.lang.syntax._
import fauna.scheduler.PriorityGroup
import fauna.storage.{ Order, Tables, Unresolved, VersionID }
import fauna.storage.api._
import fauna.storage.api.version._
import fauna.storage.ops.{ VersionAdd, Write }
import java.util.concurrent.atomic.{ AtomicReference, LongAdder }
import scala.concurrent.{ ExecutionContext, Future }

object DocHistoryForRemove {

  sealed trait Result extends Read.Result

  object Result {
    final case class Rewrite(
      before: Option[StorageVersion],
      current: Option[StorageVersion],
      after: Option[StorageVersion],
      lastModifiedTS: Timestamp,
      lastAppliedTS: Timestamp,
      bytesRead: Int)
        extends Result

    final case class SkipIO(current: StorageVersion) extends Result {
      def lastModifiedTS = Timestamp.Epoch
      def lastAppliedTS = Timestamp.Epoch
      def bytesRead = 0
    }

    val Codec: CBOR.Codec[Result] =
      CBOR.SumCodec(
        CBOR.TupleCodec[Rewrite]
      )
  }
}

/** Returns the relevant portion of the given doc's history so that coordinators can
  * compute the necessary changes upon removing the provided version ID the doc.
  */
final case class DocHistoryForRemove(
  scopeID: ScopeID,
  docID: DocID,
  versionID: VersionID,
  snapshotTS: Timestamp,
  minValidTS: Timestamp)
    extends Read[DocHistoryForRemove.Result] {
  import DocHistoryForRemove._

  def codec = Result.Codec
  def name = "Doc.HistoryForRemove"
  def columnFamily = Tables.Versions.CFName

  lazy val rowKey = Tables.Versions.rowKeyByteBuf(scopeID, docID)

  private lazy val (beforeOp, afterOp) = {
    def op(cursor: DocHistory.Cursor) =
      DocHistory(scopeID, docID, cursor, snapshotTS, minValidTS, maxResults = 1)

    (
      op(DocHistory.Cursor(max = versionID)),
      op(DocHistory.Cursor(min = versionID.saturatingIncr, order = Order.Ascending))
    )
  }

  def isRelevant(write: Write) =
    beforeOp.isRelevant(write) || afterOp.isRelevant(write)

  override def skipIO(pendingWrites: Iterable[Write]): Option[Result.SkipIO] =
    if (versionID.validTS != Timestamp.MaxMicros) {
      None
    } else {
      val iter = pendingWrites.iterator
      var result: Result.SkipIO = null

      while (iter.hasNext) {
        val write = iter.next()
        require(isRelevant(write), s"irrelevant write found: $write")

        write match {
          case v: VersionAdd if v.writeTS == Unresolved =>
            require(result eq null, s"Unmerged write found: $write, result=$result")
            result = Result.SkipIO(
              StorageVersion.fromDecoded(
                v.scope,
                v.id,
                v.writeTS,
                v.action,
                v.schemaVersion,
                // NB. TTL is not known at this time. Null forces decoding it from
                // the document's data. See StorageVersion.
                ttl = null,
                v.data,
                v.diff
              ))
          case _ =>
            return None
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
  )(implicit ec: ExecutionContext): Future[Result.Rewrite] = {
    require(
      writes forall { isRelevant(_) },
      s"irrelevant writes found: $writes, read op: $this")

    val totalBytesRead = new LongAdder
    val totalBytesInOutput = new LongAdder
    val lastModifiedTS = new AtomicReference(Timestamp.Epoch)

    def read(op: DocHistory, take: Int) = {
      val relevant = writes filter { op.isRelevant(_) }
      val readF = op.read(ctx.engine, priority, relevant, deadline)

      implicit val iec = ImmediateExecutionContext
      readF flatMap { case (page, bytesRead, merger) =>
        merger.take(take) map { results =>
          totalBytesRead.add(rowKey.readableBytes + bytesRead.bytes)
          totalBytesInOutput.add(rowKey.readableBytes + results.bytesEmitted)
          lastModifiedTS.accumulateAndGet(page.lastModifiedTS, _ max _)
          results.values
        }
      }
    }

    val beforeF = read(beforeOp, take = 2)
    val afterF = read(afterOp, take = 1)

    implicit val iec = ImmediateExecutionContext
    (beforeF, afterF) par { case (versionsBefore, versionsAfter) =>
      val rowKeyBytes = 2 * rowKey.readableBytes
      val bytesRead = rowKeyBytes + totalBytesRead.sum()
      val bytesInOutput = rowKeyBytes + totalBytesInOutput.sum()

      ctx.stats.count("DocHistoryForRemove.BytesInOutput", bytesInOutput)
      ctx.stats.count("DocHistoryForRemove.TotalBytesRead", bytesRead)

      val (current, before) =
        versionsBefore.view partition { v =>
          v.ts.validTS == versionID.validTS &&
          v.action.isCreate == versionID.action.isCreate
        }

      Future.successful(
        Result.Rewrite(
          current = current.headOption,
          before = before.headOption,
          after = versionsAfter.headOption,
          lastModifiedTS = lastModifiedTS.get,
          lastAppliedTS = ctx.engine.appliedTimestamp,
          bytesRead = bytesInOutput.toInt
        ))
    }
  }

  override def toString: String =
    s"DocHistoryForRemove($scopeID, $docID, $versionID, " +
      s"snapshotTS=$snapshotTS, minValidTS=$minValidTS)"
}
