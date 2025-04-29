package fauna.storage.cassandra

import fauna.lang.Timestamp
import org.apache.cassandra.db.{ Cell, DecoratedKey }
import org.apache.cassandra.io.sstable._
import org.apache.cassandra.io.util.RandomAccessReader

/** A SnapshotFilter yields cells live as of a point-in-time,
  * resulting in a consistent view of row data - a snapshot.
  */
final class SnapshotFilter(
  private[this] val sstable: SSTableReader,
  private[this] val dataFile: RandomAccessReader,
  private[this] val snapshotTS: Timestamp,
  private[this] val key: DecoratedKey,
  private[this] val dataSize: Long)
    extends CellFilter(new SSTableIdentityIterator(sstable, dataFile, key, dataSize)) {

  def filter(cell: Cell): Boolean =
    Timestamp.ofMicros(cell.timestamp()) <= snapshotTS
}
