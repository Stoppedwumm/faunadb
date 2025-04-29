package fauna.storage.api.scan

import fauna.atoms.{ DocID, IndexID, ScopeID }
import fauna.codex.cbor.CBOR
import fauna.exec.ImmediateExecutionContext
import fauna.lang.{ TimeBound, Timestamp }
import fauna.scheduler.PriorityGroup
import fauna.storage.{ Storage => _, _ }
import fauna.storage.api.{ Scan, Storage }
import fauna.storage.cassandra.CValue
import fauna.storage.index.IndexTerm
import fauna.storage.ir.DocIDV
import io.netty.buffer.{ ByteBuf, Unpooled }
import scala.collection.mutable.ReusableBuilder
import scala.concurrent.{ ExecutionContext, Future }

// Holds some common functionality for index scans.
private object IndexScan {
  val AllowedCFs = Set(
    Tables.SortedIndex.CFName,
    Tables.SortedIndex.CFName2,
    Tables.HistoricalIndex.CFName,
    Tables.HistoricalIndex.CFName2)

  def cfToSchema(cf: String) = cf match {
    case Tables.SortedIndex.CFName      => Tables.SortedIndex.Schema
    case Tables.SortedIndex.CFName2     => Tables.SortedIndex.Schema2
    case Tables.HistoricalIndex.CFName  => Tables.HistoricalIndex.Schema
    case Tables.HistoricalIndex.CFName2 => Tables.HistoricalIndex.Schema2
    case _ => throw new IllegalArgumentException(s"invalid term scan cf: $cf")
  }
}

object KeyScan {

  case class Entry(scope: ScopeID, index: IndexID, key: ByteBuf)

  // A prefix of largest column name we can write down for SortedIndex, which
  // orders columns by values descending.
  protected val SortedMaxCol = CBOR.encode(Long.MinValue)

  // A prefix of largest column name we can write down for HistoricalIndex,
  // which orders columns by valid timestamp descending.
  protected val HistoricalMaxCol = Timestamp.Epoch

  final case class Result(values: Vector[Entry], next: Option[ScanSlice])
      extends Scan.Result
}

/** A low-level scan of all terms (rows) in an index. A term scan returns the scope,
  * index, and undecoded key within the bounds in `slice`. A single KeyScan operation
  * will return no more than `count` keys. Callers may use the `next` slice to retrieve
  * further results. The result set is exhausted when there is no `next` slice.
  */
final case class KeyScan(
  snapshotTS: Timestamp,
  slice: ScanSlice,
  count: Int
) extends Scan[KeyScan.Result] {
  import IndexScan._
  import KeyScan._

  require(count > 0, "must request a positive number of results")
  require(
    AllowedCFs contains slice.cf,
    s"key scan applies to index CFs only: ${slice.cf}")

  def name = "Index.Key.Scan"

  val schema = cfToSchema(slice.cf)

  private type Builder = ReusableBuilder[Entry, Vector[Entry]]

  private def nextSlice(
    pred: Option[Predicate],
    slice: ScanSlice): Option[ScanSlice] =
    pred map { p =>
      val bs = ScanBounds(Left(p.rowKey.bytes), slice.bounds.right)
      val from =
        schema.encodePrefix(p, Predicate.GTE).getOrElse(Unpooled.EMPTY_BUFFER)
      ScanSlice(slice.cf, bs, from)
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
        Selector.All,
        1,
        priority,
        deadline) flatMap { case (rows, cursor) =>
        // NB: Although the scan is limited to returning a single cell, it may
        //     return multiple rows, of which only one will have a cell, and that
        //     cell is the one whose key we need.
        val nextPreds = rows flatMap { case ScanRow(_, key, cells) =>
          cells.headOption map { _ =>
            val (scope, index, _) = Tables.Indexes.decode(key.duplicate)
            builder += Entry(scope, index, key.duplicate)
            slice.cf match {
              case Tables.SortedIndex.CFName | Tables.SortedIndex.CFName2 =>
                Predicate((key, SortedMaxCol))
              case Tables.HistoricalIndex.CFName | Tables.HistoricalIndex.CFName2 =>
                Predicate((key, HistoricalMaxCol))
              case bad =>
                throw new IllegalStateException(s"invalid KeyScan cf: $bad")
            }
          }
        }
        require(
          nextPreds.size <= 1,
          s"unexpected multiple cells in term scan: $currSlice")
        val nextCount = currCount + nextPreds.size

        // If no live cells came out of the scan, but the engine
        // gave back a cursor, we must continue from that cursor -
        // there is more data to read.
        val from = nextPreds.lastOption.orElse {
          cursor map { case (key, cell) =>
            // Both CFs share the same key.
            val rowKey = CValue(Tables.SortedIndex.Schema.keyComparator, key)

            slice.cf match {
              case Tables.SortedIndex.CFName | Tables.SortedIndex.CFName2 =>
                Predicate(
                  rowKey,
                  Tables.SortedIndex.Schema.nameComparator.bytesToCValues(cell))
              case Tables.HistoricalIndex.CFName | Tables.HistoricalIndex.CFName2 =>
                Predicate(
                  rowKey,
                  Tables.HistoricalIndex.Schema.nameComparator.bytesToCValues(cell))
              case bad =>
                throw new IllegalStateException(s"invalid KeyScan cf: $bad")
            }
          }
        }

        nextSlice(from, currSlice) match {
          case Some(slice) if nextCount < count => run0(builder, slice, nextCount)
          case next                             => Future.successful((builder, next))
        }
      }
    }

    val b = Vector.newBuilder[Entry]
    b.sizeHint(count)
    implicit val iec = ImmediateExecutionContext
    run0(b, slice, 0) map { case (bld, next) =>
      Result(bld.result(), next)
    }
  }
}

object ElementScan {

  case class Entry(scope: ScopeID, key: ByteBuf, cellName: ByteBuf, doc: DocID)

  val DefaultPageSize = 1024

  // The number of cells requested per call to the underlying storage's scan.
  val ScanCellCount = 64

  final case class Cursor(slice: ScanSlice, skipStart: Boolean)

  final case class Result(values: Vector[Entry], next: Option[Cursor])
      extends Scan.Result
}

/** A low-level scan of all elements (cells) in an index. An element scan returns each
  * entry in the index as an undecoded (key, value) pair, along with the ID of the
  * document the entry points to.
  *
  * NB: A precise count could be implemented, but it complicates the code and I don't
  *     think the users of ElementScan need it
  */
final case class ElementScan(
  snapshotTS: Timestamp,
  cursor: ElementScan.Cursor,
  countHint: Int
) extends Scan[ElementScan.Result] {
  import ElementScan._
  import IndexScan._

  require(countHint > 0, "must request a positive number of results")
  require(
    AllowedCFs contains cursor.slice.cf,
    s"element scan applies to index CFs only: ${cursor.slice.cf}")

  def name = "Index.Element.Scan"

  val schema = cfToSchema(cursor.slice.cf)

  private type Builder = ReusableBuilder[Entry, Vector[Entry]]

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
          val (scope, _, _) = Tables.Indexes.decode(key.duplicate)
          val cs =
            if (!skipStart0 || cells.isEmpty)
              cells
            else {
              skipStart0 = false
              cells.tail
            }

          cs map { cell =>
            // Short-circuit the typical cell => Value => IndexValue path.
            currCursor.slice.cf match {
              case Tables.HistoricalIndex.CFName | Tables.HistoricalIndex.CFName2 =>
                val k = Predicate(
                  CValue(schema.keyComparator, key.duplicate),
                  schema.nameComparator.bytesToCValues(cell.name.duplicate))
                  .as[Tables.HistoricalIndex.Key]
                val (_, _, _, bytes, _) = k
                val values = CBOR.parse[Vector[IndexTerm]](bytes)
                Entry(scope, key, cell.name, values.head.value.asInstanceOf[DocIDV].value)
              case Tables.SortedIndex.CFName | Tables.SortedIndex.CFName2 =>
                val k = Predicate(
                  CValue(schema.keyComparator, key.duplicate),
                  schema.nameComparator.bytesToCValues(cell.name.duplicate))
                  .as[Tables.SortedIndex.Key]
                val (_, bytes, _, _, _) = k
                val values = CBOR.parse[Vector[IndexTerm]](bytes)
                Entry(scope, key, cell.name, values.last.value.asInstanceOf[DocIDV].value)
              case bad =>
                throw new IllegalStateException(s"invalid ElementScan cf: $bad")
            }
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

    val b = Vector.newBuilder[Entry]
    b.sizeHint(countHint)
    implicit val iec = ImmediateExecutionContext
    run0(b, cursor, 0) map { case (bld, next) =>
      Result(bld.result(), next)
    }
  }
}
