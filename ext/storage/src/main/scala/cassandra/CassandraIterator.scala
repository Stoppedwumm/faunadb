package fauna.storage.cassandra

import com.google.common.collect.Iterables
import fauna.atoms.Location
import fauna.lang.Timestamp
import fauna.storage.{ Cell, ScanBounds, Selector }
import io.netty.buffer.{ ByteBuf, Unpooled }
import java.util.{ ArrayList, Iterator => JIterator }
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.LongAdder
import org.apache.cassandra.db.{
  Cell => CCell,
  ColumnFamilyStore,
  DataRange,
  DecoratedKey,
  RangeTombstone,
  RowPosition
}
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator
import org.apache.cassandra.db.filter.{ ColumnSlice, SliceQueryFilter }
import org.apache.cassandra.dht.{ IncludingExcludingBounds, LongToken }
import org.apache.cassandra.service.StorageService
import org.apache.cassandra.utils.{ CloseableIterator, MergeIterator }
import scala.annotation.tailrec

object IteratorStats {
  val zero = IteratorStats(0, 0, 0, 0, 0, 0, 0)
}

case class IteratorStats(
  rows: Long,
  cells: Long,
  liveCells: Long,
  unbornCells: Long,
  deletedCells: Long,
  bytesRead: Long,
  tombstones: Long) {

  val kiloBytesRead: Long = bytesRead / 1024

  def -(other: IteratorStats): IteratorStats =
    IteratorStats(
      rows = rows - other.rows,
      cells = cells - other.cells,
      liveCells = liveCells - other.liveCells,
      unbornCells = unbornCells - other.unbornCells,
      deletedCells = deletedCells - other.deletedCells,
      bytesRead = bytesRead - other.bytesRead,
      tombstones = tombstones - other.tombstones
    )
}

class IteratorStatsCounter {
  val rows = new LongAdder
  val cells = new LongAdder
  val liveCells = new LongAdder
  val unbornCells = new LongAdder
  val deletedCells = new LongAdder
  val bytesRead = new LongAdder
  val tombstones = new LongAdder

  def snapshot(): IteratorStats = {
    IteratorStats(
      rows = rows.sum(),
      cells = cells.sum(),
      liveCells = liveCells.sum(),
      unbornCells = unbornCells.sum(),
      deletedCells = deletedCells.sum(),
      bytesRead = bytesRead.sum(),
      tombstones = tombstones.sum()
    )
  }
}

/** This iterator mimics ColumnFamilyStore.getRangeSlice() except it consumes the rows lazily.
  * This class will always iterate the rows using partition key order.
  *
  * See ColumnFamilyStore.getRangeSlice()
  *
  * **** IMPORTANT ****
  * DO NOT USE WHEN COMPACTIONS ARE ENABLED!
  *
  * This class does some Bad Things, and compactions running at the same time will cause the
  * process to segfault. So, this should never be used on a live node, it is only for offline
  * tools.
  * **** IMPORTANT ****
  */
final class CassandraIterator(
  counter: IteratorStatsCounter,
  val cfs: ColumnFamilyStore,
  bounds: ScanBounds,
  selector: Selector,
  snapTime: Timestamp)
    extends Iterator[(ByteBuf, Cell)] {

  private val dataRange = createDataRange(bounds)
  private val keyCellIter = mergeIterator(dataRange, snapTime)

  var cellIter: CloseableIterator[CCell] = _
  var rowKey: ByteBuf = _

  var actual: (ByteBuf, Cell) = _
  // The current location of the iterator, for progress bar reasons. This is only
  // updated once every 1000 cells read, as locating a key is expensive.
  var currentLocation: Location = Location.MinValue
  val count = new AtomicLong(0)

  override def hasNext: Boolean = {
    if (actual ne null) {
      return true
    }

    actual = computeNext()

    actual ne null
  }

  override def next(): (ByteBuf, Cell) = {
    val toReturn = if (actual eq null) {
      computeNext()
    } else {
      actual
    }

    assert(toReturn ne null)

    actual = null
    toReturn
  }

  @tailrec
  private def computeNext(): (ByteBuf, Cell) = {
    if (cellIter eq null) {
      if (!keyCellIter.hasNext) {
        keyCellIter.close()
        return null
      }

      val (key0, cellIter0) = keyCellIter.next()
      if (count.getAndIncrement() % 1000 == 0) {
        currentLocation =
          CassandraKeyLocator.locate(Unpooled.wrappedBuffer(key0.getKey))
      }

      if (
        !dataRange
          .stopKey()
          .isMinimum() && dataRange.stopKey().compareTo(key0) < 0
      ) {
        keyCellIter.close()
        return null
      }

      if (dataRange.contains(key0)) {
        rowKey = Unpooled.wrappedBuffer(key0.getKey)
        cellIter = cellIter0

        counter.rows.increment()
      }

      computeNext()
    } else {
      if (cellIter.hasNext) {
        val c = cellIter.next()

        if (!c.isLive(snapTime.millis)) {
          computeNext()
        } else if (c.timestamp > snapTime.micros) {
          computeNext()
        } else {
          val name = Unpooled.wrappedBuffer(c.name().toByteBuffer)
          val value = Unpooled.wrappedBuffer(c.value())
          val ts = Timestamp.ofMicros(c.timestamp())

          (rowKey, Cell(name, value, ts))
        }
      } else {
        cellIter.close()
        cellIter = null
        rowKey = null

        computeNext()
      }
    }
  }

  private def createDataRange(bounds: ScanBounds) = {
    val rowBounds = {
      val left = bounds.left match {
        case Left(key) =>
          RowPosition.ForKey.get(key.nioBuffer, StorageService.getPartitioner)
        case Right(loc) => new LongToken(loc.token).minKeyBound()
      }

      val right = new LongToken(bounds.right.token).minKeyBound()

      new IncludingExcludingBounds[RowPosition](left, right)
    }

    val sliceQueryFilter = new SliceQueryFilter(
      ColumnSlice.ALL_COLUMNS_ARRAY,
      false,
      Int.MaxValue,
      SliceQueryFilter.IGNORE_TOMBSTONED_PARTITIONS)

    new DataRange(rowBounds, sliceQueryFilter) {
      override def contains(key: DecoratedKey): Boolean =
        selector.keep(cfs.name, Unpooled.wrappedBuffer(key.getKey))
    }
  }

  /** Returns a iterator that merges the same keys from different SSTables.
    *
    * See RowIteratorFactory.getIterator()
    */
  private def mergeIterator(dataRange: DataRange, snapTime: Timestamp)
    : CloseableIterator[(DecoratedKey, CloseableIterator[CCell])] = {

    val view = cfs.select(cfs.viewFilter(dataRange.keyRange()))

    val iterators = new ArrayList[CloseableIterator[OnDiskAtomIterator]](
      Iterables.size(view.memtables) + view.sstables.size)

    view.sstables.forEach { sstable =>
      iterators.add(sstable.getScanner(dataRange))
    }

    MergeIterator.get(
      iterators,
      onDiskAtomIteratorComparator,
      new OnDiskAtomIteratorReducer(dataRange, snapTime))
  }

  /** Iterator that filters out tombstones.
    *
    * See QueryFilter.gatherTombstones()
    */
  private class FilterTombstones(iter: OnDiskAtomIterator, snapTime: Timestamp)
      extends CloseableIterator[CCell] {
    var current: CCell = _

    private[this] var cells = 0L
    private[this] var liveCells = 0L
    private[this] var unbornCells = 0L
    private[this] var deletedCells = 0L
    private[this] var bytesRead = 0L
    private[this] var tombstones = 0L

    override def hasNext: Boolean = {
      if (current ne null) {
        return true
      }

      current = computeNext()

      current ne null
    }

    override def next(): CCell = {
      val toReturn = if (current eq null) {
        computeNext()
      } else {
        current
      }

      assert(toReturn ne null)

      current = null
      toReturn
    }

    @tailrec
    private def computeNext(): CCell = {
      if (iter.hasNext) {
        (iter.next(): @unchecked) match {
          case cell: CCell =>
            if (!cell.isLive(snapTime.millis)) {
              deletedCells += 1
            } else if (cell.timestamp() > snapTime.micros) {
              unbornCells += 1
            } else {
              liveCells += 1
            }

            cells += 1
            bytesRead += cell.cellDataSize()

            cell

          case t: RangeTombstone =>
            val size = iter.getColumnFamily.getComparator
              .rangeTombstoneSerializer()
              .serializedSizeForSSTable(t)

            tombstones += 1
            bytesRead += size

            computeNext()
        }
      } else {
        null
      }
    }

    override def close(): Unit = {
      counter.cells.add(cells)
      counter.liveCells.add(liveCells)
      counter.unbornCells.add(unbornCells)
      counter.deletedCells.add(deletedCells)
      counter.bytesRead.add(bytesRead)
      counter.tombstones.add(tombstones)

      iter.close()
    }
  }

  /** Reducer that merges multiple disk iterators (Cells) for the same key.
    */
  private class OnDiskAtomIteratorReducer(dataRange: DataRange, snapTime: Timestamp)
      extends MergeIterator.Reducer[
        OnDiskAtomIterator,
        (DecoratedKey, CloseableIterator[CCell])]() {
    val colIters = new ArrayList[OnDiskAtomIterator]
    var key: DecoratedKey = _

    /** This method is called always for the same key.
      * If a different key is found, onKeyChange() is called before and getReduced() later.
      */
    override def reduce(current: OnDiskAtomIterator): Unit = {
      colIters.add(current)
      key = current.getKey

      // Every time a key is discovered in a new SSTable, the key is read and this
      // method is called.
      // It means a key can be read multiple times.
      counter.bytesRead.add(key.getKey.remaining())
    }

    override protected def getReduced: (DecoratedKey, CloseableIterator[CCell]) = {
      if (colIters.stream().anyMatch(_.getColumnFamily.isMarkedForDelete)) {
        counter.tombstones.increment()
        colIters.clear()
        return (key, EmptyIterator)
      }

      val cellIter = if (colIters.size() == 1) {
        new FilterTombstones(colIters.get(0), snapTime)
      } else {
        val cellComparator = dataRange
          .columnFilter(key.getKey)
          .getColumnComparator(cfs.metadata.comparator)

        /** Reducer that merges multiple cells of the same key, it means only the most recent live cell will be returned,
          * excluding tombstones, because it will be filtered previously.
          */
        val cellReducer = new MergeIterator.Reducer[CCell, CCell] {
          var current: CCell = _

          override def reduce(next: CCell): Unit = {
            assert((current eq null) || cellComparator.compare(current, next) == 0)

            current = if (current eq null) {
              next
            } else {
              current.reconcile(next)
            }
          }

          override def getReduced: CCell = {
            assert(current ne null)

            val toReturn = current
            current = null
            toReturn
          }

          override def trivialReduceIsTrivial(): Boolean = true
        }

        val cellIters = new ArrayList[JIterator[CCell]](colIters.size())

        colIters.forEach { iter =>
          cellIters.add(new FilterTombstones(iter, snapTime))
        }

        MergeIterator.get(cellIters, cellComparator, cellReducer)
      }

      colIters.clear()

      (key, cellIter)
    }
  }

  private val EmptyIterator: CloseableIterator[CCell] =
    new CloseableIterator[CCell] {
      override def hasNext: Boolean = false
      override def next(): CCell = null
      override def close(): Unit = ()
    }

  private def onDiskAtomIteratorComparator(
    o1: OnDiskAtomIterator,
    o2: OnDiskAtomIterator): Int =
    DecoratedKey.comparator.compare(o1.getKey, o2.getKey)
}
