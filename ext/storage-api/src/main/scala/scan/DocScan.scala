package fauna.storage.api.scan

import fauna.exec.ImmediateExecutionContext
import fauna.lang.{ TimeBound, Timestamp }
import fauna.scheduler.PriorityGroup
import fauna.storage.{ Storage => _, _ }
import fauna.storage.api.{ Scan, Storage }
import fauna.storage.api.version.StorageVersion
import fauna.storage.cassandra.CValue
import io.netty.buffer.Unpooled
import scala.collection.mutable.ReusableBuilder
import scala.concurrent.{ ExecutionContext, Future }

object DocScan {

  final case class Result(values: Vector[StorageVersion], next: Option[ScanSlice])
      extends Scan.Result
}

/** A document scan. A document scan returns the latest version for each document
  * within the bounds in `slice`. A single DocScan operation will return at least
  * `countHint` versions if available, but may return extra versions. Callers may
  * use the `next` Slice to retrieve further results. The result set is exhausted
  * when there is no `next` slice.
  */
final case class DocScan(
  snapshotTS: Timestamp,
  slice: ScanSlice,
  selector: Selector,
  countHint: Int
) extends Scan[DocScan.Result] {
  import DocScan._

  require(countHint > 0, "must request a positive number of results")
  require(slice.cf == Tables.Versions.CFName, "doc scan applies to versions CF only")

  def name = "Doc.Scan"

  // Ok.
  private type Builder = ReusableBuilder[StorageVersion, Vector[StorageVersion]]

  private def nextSlice(
    pred: Option[Predicate],
    slice: ScanSlice): Option[ScanSlice] =
    pred map { p =>
      val bs = ScanBounds(Left(p.rowKey.bytes), slice.bounds.right)
      val from = Tables.Versions.Schema
        .encodePrefix(p, Predicate.GTE)
        .getOrElse(Unpooled.EMPTY_BUFFER)
      ScanSlice(Tables.Versions.CFName, bs, from)
    }

  def run(ctx: Storage.Context, priority: PriorityGroup, deadline: TimeBound)(
    implicit ec: ExecutionContext): Future[Result] = {

    def run0(
      builder: Builder,
      currSlice: ScanSlice,
      currCount: Int): Future[(Builder, Option[ScanSlice])] = {
      ctx.engine.sparseScan(
        snapshotTS,
        currSlice,
        selector,
        countHint,
        priority,
        deadline) flatMap { case (rows, cursor) =>
        val nonEmptyRows = rows filter { _.cells.nonEmpty }
        nonEmptyRows foreach { case ScanRow(_, key, cells) =>
          val cell = cells.head
          val pred = Predicate(
            CValue(Tables.Versions.Schema.keyComparator, key),
            Tables.Versions.Schema.nameComparator.bytesToCValues(cell.name))
          val value = new Value[Tables.Versions.Key](pred, cell.value, cell.ts)
          builder += StorageVersion.fromValue(value)
        }

        val lastKey = nonEmptyRows.lastOption map { _.key }
        val from = lastKey.fold(cursor map { case (key, cell) =>
          // If no live cells came out of the scan, but the engine
          // gave back a cursor, we must continue from that cursor -
          // there is more data to read.
          Predicate(
            CValue(Tables.Versions.Schema.keyComparator, key),
            Tables.Versions.Schema.nameComparator.bytesToCValues(cell))
        }) {
          // Start the next C*-scan at the almost-largest (in CF order) version for
          // the current key. The first result should be the smallest (in CF order)
          // version for the next document (AKA the latest version in valid time).
          // The largest valid TS (Timestamp.Min) doesn't work, but it should be
          // just as good to use 1 microsecond.
          key => Option(Predicate((key, Timestamp.ofMicros(1), DocAction.MinValue)))
        }

        val nextCount = currCount + nonEmptyRows.size
        nextSlice(from, currSlice) match {
          case Some(slice) if nextCount < countHint =>
            run0(builder, slice, nextCount)
          case next => Future.successful((builder, next))
        }
      }
    }

    val b = Vector.newBuilder[StorageVersion]
    b.sizeHint(countHint)
    implicit val iec = ImmediateExecutionContext
    run0(b, slice, 0) map { case (bld, next) =>
      Result(bld.result(), next)
    }
  }
}
