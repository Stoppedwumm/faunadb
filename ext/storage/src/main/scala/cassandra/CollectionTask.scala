package fauna.storage.cassandra

import fauna.lang.Timing
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import java.io.{ File, IOException }
import java.util.{
  ArrayList,
  Collection,
  Comparator,
  HashMap,
  HashSet,
  List => JList,
  UUID
}
import org.apache.cassandra.db.{ ColumnFamilyStore, SystemKeyspace }
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator
import org.apache.cassandra.db.compaction.{ CompactionIterable => _, _ }
import org.apache.cassandra.db.compaction.CompactionManager.CompactionExecutorStatsCollector
import org.apache.cassandra.io.sstable._
import org.apache.cassandra.io.sstable.metadata.MetadataCollector
import org.apache.cassandra.utils.{ CloseableIterator, MergeIterator }
import org.apache.cassandra.utils.concurrent.Refs
import scala.collection.mutable.{ Map => MMap }
import scala.util.control.NonFatal

object CollectionTask {
  val OverlapWarnThreshold = "fauna.storage.compaction.overlap-warn-threshold"

  private val comparator = new Comparator[OnDiskAtomIterator] {
    def compare(a: OnDiskAtomIterator, b: OnDiskAtomIterator): Int =
      a.getKey.compareTo(b.getKey)
  }

  private final class CompactionIterable(
    collect: CollectionFilter.CollectionMVT,
    typ: OperationType,
    scanners: JList[ISSTableScanner],
    controller: CompactionController,
    mkFilter: CollectionFilter.Builder)
      extends AbstractCompactionIterable[LazilyFilteredRow](
        controller,
        typ,
        scanners) {

    // Implements AbstractCompactionIterable
    def iterator(): CloseableIterator[LazilyFilteredRow] =
      MergeIterator.get(scanners, comparator, new Reducer)

    private class Reducer
        extends MergeIterator.Reducer[OnDiskAtomIterator, LazilyFilteredRow] {

      private[this] val rows = new ArrayList[OnDiskAtomIterator]()

      def reduce(current: OnDiskAtomIterator): Unit =
        rows.add(current)

      protected def getReduced(): LazilyFilteredRow = {
        require(!rows.isEmpty())

        updateCounterFor(rows.size())

        try {
          // Create a new container for rows, because ours will be
          // cleared for the next reduction.
          val list = new ArrayList[OnDiskAtomIterator]()
          rows forEach { list.add(_) }

          LazilyFilteredRow(controller, collect, mkFilter, list)
        } finally {
          rows.clear()

          var n = 0L
          scanners forEach { scanner =>
            n += scanner.getCurrentPosition()
          }

          bytesRead = n
        }
      }
    }
  }
}

/** A CollectionTask is a type of compaction task which applies a
  * garbage collection policy to a set of SSTables.
  *
  * Similar to other types of compaction, this task will output a set
  * of SSTables which are equal-or-smaller in size to its input by
  * rewriting its input without propagating dead cells to its output.
  *
  * The decision as to which cells should survive (or not) is given by
  * `collect` - a mapping of row keys to the earliest valid timestamp
  * at which any cells may be considered alive. Any cells prior to
  * this time are not rewritten, and are thereby removed from storage.
  *
  * Collections may also be executed offline, in which case the input
  * SSTables are released and not registered with any data tracker. It
  * is the caller's responsibility to discard them as necessary.
  *
  * @param gcBefore Timestamp in epoch micros.
  */
final class CollectionTask(
  cfs: ColumnFamilyStore,
  candidate: LeveledManifest.CompactionCandidate,
  gcBefore: Int,
  collect: CollectionFilter.CollectionMVT,
  mkFilter: CollectionFilter.Builder,
  offline: Boolean = false)
    extends AbstractCompactionTask(cfs, new HashSet(candidate.sstables))
    with ExceptionLogging {
  import CollectionTask._

  private[this] val logger = getLogger()
  private[this] var collector: CompactionExecutorStatsCollector = _

  private[this] var rowsOutput = 0L
  private[this] var cellsOutput = 0L
  private[this] var cellsFiltered = 0L
  private[this] var cellsIgnored = 0L

  // Key is the number of overlapping SSTables *outside* the
  // compaction. Value is the number of rows with that number of
  // overlaps.
  private[this] val rowOverlaps = MMap.empty[Long, Long]

  private[this] val overlapWarnThreshold =
    Option(System.getProperty(OverlapWarnThreshold)) flatMap { _.toIntOption }

  // Implements DiskAwareRunnable
  def runMayThrow(): Unit = {
    if (sstables.size() == 0) {
      return
    }

    // This process may still run out of disk, but a quick check just
    // in case doesn't hurt.
    checkDiskSpace()

    // Sanity check.
    require(sstables.stream allMatch { _.descriptor.cfname == cfs.name })

    val uuid = SystemKeyspace.startCompaction(cfs, sstables)
    require(uuid ne null, "Collection Tasks are never valid on system tables.")

    logger.info(s"Task $uuid: Collecting $sstables")

    val start = Timing.start

    val controller =
      new CompactionController(cfs, sstables, gcBefore)
    try {
      val expired = controller.getFullyExpiredSSTables
      val live = new ArrayList[SSTableReader]()

      sstables.stream
        .filter { !expired.contains(_) }
        .forEach { live.add(_) }

      val refs = Refs.ref(live)
      val scanners = cfs.getCompactionStrategy.getScanners(live)

      val compaction = new CompactionIterable(
        collect,
        compactionType,
        scanners.scanners,
        controller,
        mkFilter)

      if (collector ne null) {
        collector.beginCompaction(compaction)
      }

      val iter = compaction.iterator()

      val rewritten =
        try {
          if (!offline && !controller.cfs.getCompactionStrategy.isEnabled) {
            throw new CompactionInterruptedException(compaction.getCompactionInfo())
          }

          // If this compaction would result in no output at all, mark
          // the input obsolete and move on.
          if (!iter.hasNext()) {
            cfs.markObsolete(sstables, compactionType)
            return
          }

          compact(uuid, iter, live) { () =>
            if (compaction.isStopRequested) {
              throw new CompactionInterruptedException(
                compaction.getCompactionInfo())
            }
          }

        } finally {
          iter.close()
          scanners.close()
          refs.close()

          SystemKeyspace.finishCompaction(uuid)

          if (collector ne null) {
            collector.finishCompaction(compaction)
          }
        }

      if (offline) {
        Refs.release(Refs.selfRefs(rewritten))
      } else {
        cfs.getDataTracker.markCompactedSSTablesReplaced(
          sstables,
          rewritten,
          compactionType)
      }

      val elapsed = start.elapsedMillis
      val startBytes = SSTableReader.getTotalBytes(sstables)
      val endBytes = SSTableReader.getTotalBytes(rewritten)

      val info = new StringBuilder
      rewritten forEach { reader =>
        info.append(reader.infoString()).append("; ")
      }

      var totalRows = 0L
      val counts = compaction.getMergedRowCounts()
      val mergeSummary = new StringBuilder(counts.length * 10)
      val mergedRows = new HashMap[Integer, Long]()

      for (i <- 0 until counts.length) {
        val count = counts(i)

        if (count != 0) {
          val rows = i + 1
          totalRows += rows * count
          mergeSummary.append(String.format("%d:%d, ", rows, count))
          mergedRows.put(rows, count)
        }
      }

      val overlaps = if (rowOverlaps.isEmpty) {
        "none"
      } else {
        // This output formats as like: "{ 1 -> 2, 2 -> 10 }",
        // where { 1, 2 } are the number of overlaps and { 2, 10 } are
        // the number of rows with that number of overlaps.
        rowOverlaps.mkString("{ ", ", ", " }")
      }

      logger.info(
        s"Task $uuid: Collected ${sstables.size} SSTables to [$info]. " +
          s"$startBytes bytes to $endBytes in ${elapsed}ms. " +
          s"$totalRows rows merged to $rowsOutput " +
          s"(cells: $cellsOutput output, $cellsFiltered filtered, $cellsIgnored ignored). " +
          s"Row merge counts were {$mergeSummary} " +
          s"Row overlap counts were $overlaps")

    } finally {
      controller.close()
    }
  }

  // Implements AbstractCompactionTask
  protected def executeInternal(collector: CompactionExecutorStatsCollector): Int = {
    this.collector = collector
    run()
    sstables.size()
  }

  private def compact(
    uuid: UUID,
    iter: CloseableIterator[LazilyFilteredRow],
    live: Collection[SSTableReader])(
    maybeStop: () => Unit): Collection[SSTableReader] = {
    val maxAge = maxDataAge(live)
    val writer = new SSTableRewriter(cfs, sstables, maxAge, offline)

    try {

      val estimatedTotalKeys =
        cfs.metadata.getMinIndexInterval.toLong max SSTableReader
          .getApproximateKeyCount(live)
      val keysPerSSTable =
        Math.ceil(estimatedTotalKeys.toDouble / (fileCount(live) max 1)).toLong
      val maxSSTableBytes = candidate.maxSSTableBytes
      val fileSize =
        expectedWriteSize(live) min maxSSTableBytes

      val dir = getWriteDirectory(fileSize)
      def newLocation() = cfs.directories.getLocationForDisk(dir)
      writer.switchWriter(makeWriter(newLocation(), keysPerSSTable))

      while (iter.hasNext()) {
        maybeStop()

        val row = iter.next()

        if (writer.append(row) ne null) {
          val stats = row.columnStats

          rowsOutput += 1
          cellsOutput += stats.columnCount
          cellsFiltered += row.collectionStats.filtered
          cellsIgnored += row.collectionStats.ignored

          if (row.collectionStats.overlaps > 0) {
            rowOverlaps.updateWith(row.collectionStats.overlaps) {
              case None    => Some(1)
              case Some(n) => Some(n + 1)
            }

            overlapWarnThreshold match {
              case Some(threshold) if row.collectionStats.overlaps > threshold =>
                logger.warn(
                  s"Found ${row.collectionStats.overlaps} overlapping SSTables while compacting " +
                    s"key ${row.key.getKey().toHexString}!")
              case _ => ()
            }
          }

          // Switch writers in conformance with LCS.
          if (writer.currentWriter.getOnDiskFilePointer() > maxSSTableBytes) {
            writer.switchWriter(makeWriter(newLocation(), keysPerSSTable))
          }
        }
      }

      writer.finish()
    } catch {
      // NB. This would normally be NonFatals only, but any exception
      // MUST call abort() to prevent leaving a half-baked SSTable on
      // the filesystem.
      case ex: Throwable =>
        logger.info(s"Task $uuid: Error occurred while compacting $sstables: $ex")

        try {
          writer.abort(s"CollectionTask (${ex.getMessage})")
        } catch {
          case NonFatal(sup) => ex.addSuppressed(sup)
        }

        // C* logs exceptions to core.log. Put it in exception.log,
        // too.
        logException(ex)
        throw ex
    }
  }

  private def expectedWriteSize(sstables: Collection[SSTableReader]) =
    cfs.getExpectedCompactedFileSize(sstables, compactionType)

  private def checkDiskSpace() = {
    val totalBytes = expectedWriteSize(sstables) max 1
    val files = fileCount(sstables) max 1

    if (!getDirectories.hasAvailableDiskSpace(files, totalBytes)) {
      throw new IOException(
        "Not enough space for compaction! " +
          s"sstables = $files, size = $totalBytes bytes")
    }
  }

  private def fileCount(sstables: Collection[SSTableReader]): Long = {
    val totalBytes = expectedWriteSize(sstables) max 1
    totalBytes / cfs.getCompactionStrategy.getMaxSSTableBytes
  }

  private def maxDataAge(sstables: Collection[SSTableReader]): Long = {
    var max = 0L

    sstables forEach { sst =>
      if (sst.maxDataAge > max) {
        max = sst.maxDataAge
      }
    }

    max
  }

  private def makeWriter(dir: File, keysPerSSTable: Long): SSTableWriter =
    new SSTableWriter(
      cfs.getTempSSTablePath(dir),
      keysPerSSTable,
      0, /* repairedAt, unused */
      cfs.metadata,
      cfs.partitioner,
      new MetadataCollector(sstables, cfs.metadata.comparator, candidate.level))
}
