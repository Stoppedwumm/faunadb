package fauna.storage.cassandra

import org.apache.cassandra.db._
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator
import org.apache.cassandra.utils.CloseableIterator

/** This class wraps an atom iterator (typically an IMergeIterator)
  * and filters out null atoms for composition with a CellFilter,
  * which uses null to indicate end-of-iteration.
  */
final class NullFilter(
  private[this] val cf: ColumnFamily,
  private[this] val key: DecoratedKey,
  private[this] val inner: CloseableIterator[OnDiskAtom])
    extends OnDiskAtomIterator {

  private[this] var actual: OnDiskAtom = _

  // Implements OnDiskAtomIterator
  def getColumnFamily(): ColumnFamily = cf
  def getKey(): DecoratedKey = key
  def close(): Unit = inner.close()

  def next(): OnDiskAtom = {
    val toReturn = if (actual eq null) {
      computeNext()
    } else {
      actual
    }

    require(toReturn ne null)
    actual = null
    toReturn
  }

  def hasNext: Boolean = {
    if (actual ne null) {
      return true
    }

    actual = computeNext()
    actual ne null
  }

  @annotation.tailrec
  private def computeNext(): OnDiskAtom = {
    if (inner.hasNext()) {
      val atom = inner.next()

      // Keep non-nulls, loop otherwise.
      if (atom ne null) {
        atom
      } else {
        computeNext()
      }
    } else {
      null
    }
  }
}
