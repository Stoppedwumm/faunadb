package fauna.storage.cassandra

import fauna.logging.ExceptionLogging
import org.apache.cassandra.db.{ Cell, OnDiskAtom }
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator
import scala.annotation.tailrec
import scala.util.control.NonFatal

/** A CellFilter composes an OnDiskAtomIterator with a selective
  * filter function, yielding only those Cells for which the filter
  * function returns true.
  */
abstract class CellFilter(private[this] val iterator: OnDiskAtomIterator)
    extends OnDiskAtomIterator
    with ExceptionLogging {

  private[this] var actual: OnDiskAtom = _

  // Cell counts are extremely high velocity. Buffer them locally and
  // flush to stats when the iterator is closed.
  private[this] var cells: Int = 0
  private[this] var totalCells: Int = 0

  /** Sub-classes should define this method to return true if the cell
    * should be included in the iterator's output, and false
    * otherwise.
    */
  def filter(cell: Cell): Boolean

  /** The number of cells included in the iterator's output.
    *
    * NOTE: This value is computed as a side-effect of iteration. It's
    * value will not be stable until hasNext() returns false.
    */
  final def cellCount: Int = cells

  /** The total number of cells observed by the iterator before filter().
    *
    * NOTE: This value is computed as a side-effect of iteration. It's
    * value will not be stable until hasNext() returns false.
    */
  final def totalCellCount: Int = totalCells

  final def next(): OnDiskAtom = {
    val toReturn = if (actual eq null) {
      computeNext()
    } else {
      actual
    }

    assert(toReturn ne null)

    actual = null
    toReturn
  }

  final def hasNext: Boolean = {
    if (actual ne null) {
      return true
    }

    actual =
      try {
        computeNext()
      } catch {
        case NonFatal(ex) =>
          logException(ex)

          // Re-throw to cancel this compaction task.
          throw ex
      }

    actual ne null
  }

  final def close() = iterator.close()
  final def getKey() = iterator.getKey()
  final def getColumnFamily() = iterator.getColumnFamily()

  protected final def incrCells() = cells += 1
  protected final def incrTotalCells() = totalCells += 1

  protected def computeNext(): OnDiskAtom = {
    // Define a local tailrec method so scalac may TCO this method,
    // but computeNext() may still be overridden.
    @tailrec
    def compute0(): OnDiskAtom = {
      if (iterator.hasNext()) {
        val atom = iterator.next()
        incrTotalCells()

        require(atom.isInstanceOf[Cell], "Range tombstones should not exist.")

        if (filter(atom.asInstanceOf[Cell])) {
          incrCells()
          atom
        } else {
          compute0()
        }
      } else {
        null
      }
    }

    compute0()
  }

}
