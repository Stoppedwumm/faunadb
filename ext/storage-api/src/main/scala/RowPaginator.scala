package fauna.storage.api

import fauna.lang.{ Page, TimeBound, Timestamp }
import fauna.scheduler.PriorityGroup
import fauna.storage.{ Cell, Order, Row, StorageEngine }
import io.netty.buffer.ByteBuf
import scala.concurrent.{ ExecutionContext, Future }

/** Low-level utility to paginate through a row's cells.
  *
  * Enforces a maximum page size to promote fairness among concurrent reads,
  * and a minimum page size for correctness. Smaller page sizes may be more
  * efficient.
  */
private[api] object RowPaginator {

  // 1 is no good as a minimum -- see `dropHead` below.
  private val MinPageSize = 2
  private val MaxPageSize = 4096 + 1 // + 1 for overscan at 4096.

  object RowPage {
    def empty = RowPage(Timestamp.Epoch, MultiPage.empty)
  }

  final case class RowPage(
    lastModifiedTS: Timestamp,
    cells: MultiPage[Cell]
  )

  def apply(
    engine: StorageEngine,
    columnFamily: String,
    rowKey: ByteBuf,
    from: ByteBuf,
    to: ByteBuf,
    pageSize: Int,
    order: Order,
    priority: PriorityGroup,
    snapshotTS: Timestamp,
    deadline: TimeBound
  )(implicit ec: ExecutionContext): Future[RowPage] = {

    def read(from: ByteBuf, count: Int, knownRowTS: Option[Timestamp] = None) =
      engine.readRow(
        snapshotTS,
        rowKey,
        Vector(
          StorageEngine.Slice(
            cf = columnFamily,
            ranges = Vector((from, to)),
            order,
            count = count + 1 // Overscan by one to get the next page's bound.
          )),
        priority,
        deadline,
        knownRowTS)

    def rowCells(row: Row, dropHead: Boolean) =
      row.cfs.iterator
        .collect { case (`columnFamily`, cells) => cells.iterator }
        .flatten
        .drop(if (dropHead) 1 else 0)
        .toVector

    def readNext(rowTS: Timestamp, from: ByteBuf, prevSize: Int) =
      Page.unfold((from, prevSize)) { case ((from, prevSize)) =>
        val nextSize = (prevSize * 2) min MaxPageSize
        read(from, nextSize, Some(rowTS)) map { row =>
          val cells = rowCells(row, dropHead = true)
          (cells, cells.lastOption map { c => (c.name, nextSize) })
        }
      }

    def hasMoreResults(cells: Iterable[Cell], count: Int): Boolean =
      cells.sizeIs >= count - 1 && cells.last.name != to

    val initialSize = MinPageSize max (pageSize min MaxPageSize)

    read(from, initialSize) map { row =>
      val cells = rowCells(row, dropHead = false)
      val page =
        Page(
          cells,
          Option.when(hasMoreResults(cells, initialSize))(() =>
            readNext(row.ts, cells.last.name, initialSize)))
      RowPage(row.ts, MultiPage(page))
    }
  }
}
