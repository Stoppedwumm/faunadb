package fauna.storage.api.scan

import fauna.exec.ImmediateExecutionContext
import fauna.lang.{ TimeBound, Timestamp }
import fauna.scheduler.PriorityGroup
import fauna.storage.{ ScanSlice, Tables }
import fauna.storage.{ Storage => _, _ }
import fauna.storage.api.{ Scan, Storage }
import fauna.storage.cassandra.CValue
import fauna.storage.lookup.LookupEntry
import io.netty.buffer.ByteBuf
import scala.collection.mutable.ReusableBuilder
import scala.concurrent.{ ExecutionContext, Future }

object LookupScan {

  // ¯\_(ツ)_/¯
  val DefaultPageSize = 4096

  // The number of cells requested per call to the underlying storage's scan.
  val ScanCellCount = 4096

  final case class Cursor(slice: ScanSlice, skipStart: Boolean)

  final case class Result(values: Vector[LookupEntry], next: Option[Cursor])
      extends Scan.Result
}

/** A low-level scan of all lookups.
  *
  * TODO: A lot of this code is shared with ElementScan.
  */
final case class LookupScan(
  snapshotTS: Timestamp,
  cursor: LookupScan.Cursor,
  countHint: Int = LookupScan.DefaultPageSize
) extends Scan[LookupScan.Result] {
  import LookupScan._

  require(countHint > 0, "must request a positive number of results")
  require(
    cursor.slice.cf == Tables.Lookups.CFName,
    "lookup scan applies to lookups CF only")

  def name = "Lookup.Scan"

  val schema = Tables.Lookups.Schema

  private type Builder = ReusableBuilder[LookupEntry, Vector[LookupEntry]]

  private def nextSlice(
    next: Option[(ByteBuf, ByteBuf)],
    slice: ScanSlice): Option[ScanSlice] =
    next map { case (key, col) =>
      val bs = ScanBounds(key, slice.bounds.right)
      ScanSlice(slice.cf, bs, col)
    }

  def run(ctx: Storage.Context, priority: PriorityGroup, deadline: TimeBound)(
    implicit ec: ExecutionContext): Future[Result] = {

    def run0(
      builder: Builder,
      currCursor: Cursor,
      currCount: Int): Future[(Builder, Option[Cursor])] = {
      val adjCount = {
        val c = ScanCellCount min countHint
        if (currCursor.skipStart) c + 1 else c
      }
      ctx.engine.fullScan(
        snapshotTS,
        currCursor.slice,
        Selector.All,
        adjCount,
        priority,
        deadline) flatMap { case (rows, next) =>
        var skipStart0 = currCursor.skipStart
        val entries = rows flatMap { case ScanRow(_, key, cells) =>
          val rowKey = CValue(schema.keyComparator, key.duplicate)
          val cs =
            if (!skipStart0 || cells.isEmpty) {
              cells
            } else {
              skipStart0 = false
              currCursor.slice.bounds.left match {
                // only skip the first cell if the left scan bound and the first cell
                // match. When the page boundary ends on an unborn or dead cell it is
                // possible that the cursor cell and the last cell we received aren't
                // the same. In this scenario we do not want to skip over the first
                // cell in the result set.
                case Left(scanBoundRowKey)
                    if key == scanBoundRowKey && cells.headOption.exists(
                      _.name == currCursor.slice.startCol) =>
                  cells.tail
                case _ => cells
              }
            }

          cs map { cell =>
            val k = Predicate(
              rowKey,
              schema.nameComparator.bytesToCValues(cell.name.duplicate))
              .as[Tables.Lookups.Key]
            LookupEntry(new Value(k, cell.name.duplicate, cell.ts))
          }
        }

        builder ++= entries
        val nextCount = currCount + entries.size
        val cursor = nextSlice(next, currCursor.slice) map {
          Cursor(_, skipStart = true)
        }
        if (nextCount >= countHint || cursor.isEmpty) {
          Future.successful((builder, cursor))
        } else {
          run0(builder, cursor.get, nextCount)
        }
      }
    }

    val b = Vector.newBuilder[LookupEntry]
    b.sizeHint(countHint)
    implicit val iec = ImmediateExecutionContext
    run0(b, cursor, 0) map { case (bld, next) =>
      Result(bld.result(), next)
    }
  }
}
