package fauna.storage.cassandra

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.{ Collections, List => JList }
import org.apache.cassandra.db._
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator
import org.apache.cassandra.db.compaction._
import org.apache.cassandra.db.index.SecondaryIndexManager
import org.apache.cassandra.io.sstable.{
  ColumnNameHelper,
  ColumnStats,
  SSTable,
  SSTableWriter
}
import org.apache.cassandra.io.util.{ DataOutputBuffer, DataOutputPlus }
import org.apache.cassandra.utils.{ MergeIterator, StreamingHistogram }

object LazilyFilteredRow {

  def apply(
    controller: CompactionController,
    collect: CollectionFilter.CollectionMVT,
    mkFilter: CollectionFilter.Builder,
    rows: JList[OnDiskAtomIterator]): LazilyFilteredRow = {
    require(!rows.isEmpty)

    new LazilyFilteredRow(rows.get(0).getKey(), controller, collect, mkFilter, rows)
  }
}

/** This is largely a re-implementation of C*'s LazilyCompactedRow in
  * Scala. However, support for RangeTombstones and Counters has been
  * removed.
  */
final class LazilyFilteredRow private (
  // Do not name this field 'key'! scalac will generate an incorrect field
  // access and an IAE will be thrown at runtime.
  rowKey: DecoratedKey,
  controller: CompactionController,
  collect: CollectionFilter.CollectionMVT,
  mkFilter: CollectionFilter.Builder,
  rows: JList[OnDiskAtomIterator])
    extends AbstractCompactedRow(rowKey) {

  private[this] var isClosed: Boolean = false

  // Initialized as a side-effect of write().
  // XXX: but not update()?
  // Public for access after iteration - see CollectionTask.compact().
  // XXX: there must be a better way...
  var columnStats: ColumnStats = _
  var collectionStats: CollectionFilter.Stats = _

  // Initialized as a side-effect of either (both?) write() or update().
  private[this] var indexBuilder: ColumnIndex.Builder = _

  private[this] val maxRowTombstone: DeletionTime = {
    var mrt = DeletionTime.LIVE

    rows forEach { row =>
      val tombstone = row.getColumnFamily().deletionInfo().getTopLevelDeletion()

      if (mrt.compareTo(tombstone) < 0) {
        mrt = tombstone
      }
    }

    mrt
  }

  private[this] val emptyColumnFamily: ColumnFamily = {
    val absc = ArrayBackedSortedColumns.factory.create(controller.cfs.metadata)
    absc.delete(maxRowTombstone)
    if (
      !maxRowTombstone
        .isLive() && maxRowTombstone.markedForDeleteAt < maxPurgeableTimestamp
    ) {
      absc.purgeTombstones(controller.gcBefore)
    }
    absc
  }

  // FaunaDB does not use SIs, but keep updating them just in case.
  private[this] val indexer: SecondaryIndexManager.Updater =
    controller.cfs.indexManager.gcUpdaterFor(key)

  private[this] val reducer: Reducer = new Reducer()

  private[this] val merger: CollectionFilter = {
    val cmp = emptyColumnFamily.getComparator().onDiskAtomComparator()

    // CellFilter uses null to indicate end-of-iteration. Filter
    // nulls - which MergeIterator may return - prior to building one.
    val nonNull =
      new NullFilter(emptyColumnFamily, key, MergeIterator.get(rows, cmp, reducer))

    mkFilter(nonNull, controller, collect)
  }

  private[this] lazy val maxPurgeableTimestamp =
    controller.maxPurgeableTimestamp(key)

  def write(currentPosition: Long, out: DataOutputPlus): RowIndexEntry = {
    require(!isClosed)

    indexBuilder = new ColumnIndex.Builder(emptyColumnFamily, key.getKey(), out)
    val index = indexBuilder.buildForCompaction(merger)

    // No columns or tombstones were found.
    if (index.columnsIndex.isEmpty() && !emptyColumnFamily.isMarkedForDelete()) {
      return null
    }

    collectionStats = merger.stats.build()
    columnStats = new ColumnStats(
      reducer.columns,
      reducer.minTimestampTracker.get(),
      Math.max(
        emptyColumnFamily.deletionInfo.maxTimestamp,
        reducer.maxTimestampTracker.get),
      reducer.maxDeletionTimeTracker.get(),
      reducer.tombstones,
      reducer.minColumnNameSeen,
      reducer.maxColumnNameSeen,
      false /* hasLegacyCounterShards - counters don't exist in FaunaDB */
    )

    // When no columns are written, an empty header may still be
    // needed for a top-level tombstone.
    indexBuilder.maybeWriteEmptyRowHeader()

    out.writeShort(SSTableWriter.END_OF_ROW)

    close()

    val tombstone = emptyColumnFamily.deletionInfo.getTopLevelDeletion()
    RowIndexEntry.create(currentPosition, tombstone, index)
  }

  def update(digest: MessageDigest): Unit = {
    val serializer = new OnDiskAtom.SerializerForWriting() {

      override def serializeForSSTable(atom: OnDiskAtom, out: DataOutputPlus): Unit =
        atom.updateDigest(digest)

      override def serializedSizeForSSTable(atom: OnDiskAtom): Long = 0
    }

    update(digest, serializer)
  }

  def update(
    digest: MessageDigest,
    serializer: OnDiskAtom.SerializerForWriting): Unit = {
    require(!isClosed)

    val out = new DataOutputBuffer()

    indexBuilder =
      new ColumnIndex.Builder(emptyColumnFamily, key.getKey(), out, serializer)

    DeletionTime.serializer.serialize(
      emptyColumnFamily.deletionInfo.getTopLevelDeletion(),
      out)

    val deleted =
      emptyColumnFamily.deletionInfo.getTopLevelDeletion() != DeletionTime.LIVE

    // Copied verbatim from C*:
    //
    // do not update digest in case of missing or purged row level tombstones, see
    // CASSANDRA-8979
    // - digest for non-empty rows needs to be updated with deletion
    //   in any case to match digest with versions before patch
    // - empty rows must not update digest in case of LIVE delete
    //   status to avoid mismatches with non-existing rows
    //
    // this will however introduce in return a digest mismatch for
    // versions before patch (which would update digest in any case)
    if (merger.hasNext() || deleted) {
      digest.update(out.getData(), 0, out.getLength());
    }

    indexBuilder.buildForCompaction(merger)

    close()
  }

  def close(): Unit = {
    rows forEach { row =>
      row.close()
    }

    isClosed = true
  }

  private final class Reducer extends MergeIterator.Reducer[OnDiskAtom, OnDiskAtom] {

    // All columns reduced together will share the same name; this
    // container will only contain a single column. Building an ABSC
    // here re-uses the conflict resolution code from ColumnFamily.
    // Re-initialized in getReduced().
    var container =
      ArrayBackedSortedColumns.factory.create(emptyColumnFamily.metadata)

    var minColumnNameSeen: JList[ByteBuffer] = Collections.emptyList()
    var maxColumnNameSeen: JList[ByteBuffer] = Collections.emptyList()

    var columns: Int = 0

    val minTimestampTracker = {
      val tracker = new ColumnStats.MinLongTracker(Long.MinValue)

      val ts = if (maxRowTombstone.isLive()) {
        Long.MaxValue
      } else {
        maxRowTombstone.markedForDeleteAt
      }

      tracker.update(ts)
      tracker
    }

    val maxTimestampTracker = {
      val tracker = new ColumnStats.MaxLongTracker(Long.MaxValue)

      val ts = if (maxRowTombstone.isLive()) {
        Int.MinValue
      } else {
        maxRowTombstone.localDeletionTime
      }

      tracker.update(ts)
      tracker
    }

    val maxDeletionTimeTracker = new ColumnStats.MaxIntTracker(Int.MaxValue)

    val tombstones = {
      val histo = new StreamingHistogram(SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE)

      if (!maxRowTombstone.isLive()) {
        histo.update(maxRowTombstone.localDeletionTime)
      }

      histo
    }

    /** Called once for each cell sharing the same column name.
      */
    def reduce(current: OnDiskAtom): Unit = {
      require(!current.isInstanceOf[RangeTombstone])

      val cell = current.asInstanceOf[Cell]
      container.addColumn(cell)

      // Very likely - FaunaDB doesn't use SIs.
      if (indexer == SecondaryIndexManager.nullUpdater) {
        return
      }

      // Just in case SIs suddenly spring into existence...
      if (cell.isLive() && !container.getColumn(cell.name).equals(cell)) {
        indexer.remove(cell)
      }
    }

    /** Called after reduce() has been called for each cell sharing the same name. */
    protected def getReduced(): OnDiskAtom = {
      container.delete(maxRowTombstone)

      var iter = container.iterator()
      val cell = iter.next()

      val shouldPurge = cell.getLocalDeletionTime() < Int.MaxValue &&
        cell.timestamp < maxPurgeableTimestamp

      val overriddenGCBefore =
        if (shouldPurge) {
          controller.gcBefore
        } else {
          Int.MinValue
        }

      ColumnFamilyStore.removeDeletedColumnsOnly(
        container,
        overriddenGCBefore,
        indexer)

      iter = container.iterator()
      if (!iter.hasNext()) {
        // don't call clear() because that resets the deletion time. See
        // CASSANDRA-7808.
        container =
          ArrayBackedSortedColumns.factory.create(emptyColumnFamily.metadata)
        return null
      }

      val localDeletionTime =
        container.deletionInfo.getTopLevelDeletion().localDeletionTime
      if (localDeletionTime < Int.MaxValue) {
        tombstones.update(localDeletionTime)
      }

      val reduced = iter.next()
      container = ArrayBackedSortedColumns.factory.create(emptyColumnFamily.metadata)

      // removeDeletedColumnsOnly() has checked top-level tombstones,
      // but not range tombstones. They shouldn't exist, but use the
      // tombstone tracker to check for them just in case.
      if (indexBuilder.tombstoneTracker.isDeleted(reduced)) {
        indexBuilder.tombstoneTracker.update(reduced, false)
        indexer.remove(reduced)
        return null
      }

      columns += 1
      minTimestampTracker.update(reduced.timestamp())
      maxTimestampTracker.update(reduced.timestamp())

      val cmp = controller.cfs.metadata.comparator
      minColumnNameSeen =
        ColumnNameHelper.minComponents(minColumnNameSeen, reduced.name, cmp)
      maxColumnNameSeen =
        ColumnNameHelper.maxComponents(maxColumnNameSeen, reduced.name, cmp)

      val deletionTime = reduced.getLocalDeletionTime()
      if (deletionTime < Int.MaxValue) {
        tombstones.update(deletionTime)
      }

      reduced
    }
  }

}
