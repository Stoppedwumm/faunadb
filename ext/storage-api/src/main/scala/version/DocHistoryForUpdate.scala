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
import scala.util.Success

object DocHistoryForUpdate {

  sealed trait Result extends Read.Result

  object Result {
    final case class Append(
      latest: Option[StorageVersion],
      lastModifiedTS: Timestamp,
      lastAppliedTS: Timestamp,
      bytesRead: Int)
        extends Result

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
        CBOR.TupleCodec[Append],
        CBOR.TupleCodec[Rewrite]
      )
  }
}

/** Returns the relevant portion of the given doc's history so that coordinators can
  * compute the necessary changes upon updating the doc.
  */
final case class DocHistoryForUpdate(
  scopeID: ScopeID,
  docID: DocID,
  versionID: VersionID,
  snapshotTS: Timestamp,
  minValidTS: Timestamp)
    extends Read[DocHistoryForUpdate.Result] {
  import DocHistoryForUpdate._

  def codec = Result.Codec
  def name = "Doc.HistoryForUpdate"
  def columnFamily = Tables.Versions.CFName

  lazy val rowKey = Tables.Versions.rowKeyByteBuf(scopeID, docID)

  private lazy val (latestOp, beforeOp, afterOp) = {
    def op(cursor: DocHistory.Cursor) =
      DocHistory(scopeID, docID, cursor, snapshotTS, minValidTS, maxResults = 1)

    (
      op(DocHistory.Cursor()),
      op(DocHistory.Cursor(max = versionID)),
      op(DocHistory.Cursor(min = versionID.saturatingIncr, order = Order.Ascending))
    )
  }

  def isRelevant(write: Write) =
    latestOp.isRelevant(write) ||
      beforeOp.isRelevant(write) ||
      afterOp.isRelevant(write)

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
  )(implicit ec: ExecutionContext): Future[Result] = {
    require(
      writes forall { isRelevant(_) },
      s"irrelevant writes found: $writes, read op: $this")

    val totalBytesRead = new LongAdder
    val bytesInOutput = new LongAdder
    val lastModifiedTS = new AtomicReference(Timestamp.Epoch)

    def read(op: DocHistory, take: Int) = {
      val relevant = writes filter { op.isRelevant(_) }
      val readF = op.read(ctx.engine, priority, relevant, deadline)

      implicit val iec = ImmediateExecutionContext
      readF flatMap { case (page, bytesRead, merger) =>
        merger.take(take) map { results =>
          lastModifiedTS.accumulateAndGet(page.lastModifiedTS, _ max _)
          bytesInOutput.add(results.bytesEmitted)
          totalBytesRead.add(bytesRead.bytes)
          results.values
        }
      }
    }

    val latestF = read(latestOp, take = 1)

    implicit val iec = ImmediateExecutionContext
    latestF flatMap { latestVersions =>
      latestVersions.headOption match {
        case Some(latest) if latest.ts.validTS >= versionID.validTS =>
          (read(beforeOp, take = 2), read(afterOp, take = 1)) par {
            case (versionsBefore, versionsAfter) =>
              val (current, before) =
                versionsBefore.view partition { v =>
                  v.ts.validTS == versionID.validTS &&
                  v.action.isCreate == versionID.action.isCreate
                }

              val rowKeyBytes = 3 * rowKey.readableBytes
              bytesInOutput.add(rowKeyBytes)
              totalBytesRead.add(rowKeyBytes)

              Future.successful(
                Result.Rewrite(
                  current = current.headOption,
                  before = before.headOption,
                  after = versionsAfter.headOption,
                  lastModifiedTS = lastModifiedTS.get,
                  lastAppliedTS = ctx.engine.appliedTimestamp,
                  bytesRead = bytesInOutput.sum().toInt
                ))
          }

        case latest =>
          bytesInOutput.add(rowKey.readableBytes)
          totalBytesRead.add(rowKey.readableBytes)

          Future.successful(
            Result.Append(
              latest = latest,
              lastModifiedTS = lastModifiedTS.get,
              lastAppliedTS = ctx.engine.appliedTimestamp,
              bytesRead = bytesInOutput.sum().toInt
            ))
      }
    } andThen { case Success(_) =>
      ctx.stats.count("DocHistoryForUpdate.BytesInOutput", bytesInOutput.sum())
      ctx.stats.count("DocHistoryForUpdate.TotalBytesRead", totalBytesRead.sum())
    }
  }

  override def toString: String =
    s"DocHistoryForUpdate($scopeID, $docID, $versionID, " +
      s"snapshotTS=$snapshotTS, minValidTS=$minValidTS)"
}
