package fauna.storage.cassandra

import com.google.common.collect.ArrayListMultimap
import fauna.lang.syntax._
import fauna.storage._
import java.lang.{ Integer => JInt }
import java.util.{ ArrayList, Collection, Collections, Map => JMap }
import org.apache.cassandra.db.compaction._
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy.ScannerList
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy.LeveledScanner
import org.apache.cassandra.db.ColumnFamilyStore
import org.apache.cassandra.dht.{ Range => CRange, Token }
import org.apache.cassandra.io.sstable.{ ISSTableScanner, SSTableReader }
import scala.util.control.NonFatal

object CollectionStrategy {
  val SSTableSizeOption = "sstable_size_in_mb"

  lazy val hints = new CollectionHints

  private val logger = getLogger()

  private def configureSSTableSize(options: JMap[String, String]): Int = {
    var sizeMB = 160 // Default from C*.

    if (options ne null) {
      if (options.containsKey(SSTableSizeOption)) {
        sizeMB = JInt.parseInt(options.get(SSTableSizeOption))
      }
    }

    sizeMB
  }

  def filterBuilder(cfName: String): CollectionFilter.Builder = (it, ctrl, hints) =>
    // NB. Disable docs GC during transfers to prevent data resurrection since not
    // all SSTables containing the row are loaded at once. See `CompactionController`
    // for details.
    if (ctrl.keepTombstones) {
      NopFilter(cfName, it, ctrl)
    } else {
      cfName match {
        case Tables.Versions.CFName        => VersionsFilter(it, ctrl, hints)
        case Tables.SortedIndex.CFName     => SortedIndexFilter(it, ctrl, hints)
        case Tables.HistoricalIndex.CFName => HistoricalIndexFilter(it, ctrl, hints)
        case _                             => NopFilter(cfName, it, ctrl)
      }
    }
}

/** The CollectionStrategy is substantially the same as
  * LeveledCompactionStrategy, however it creates CollectionTasks,
  * which are able to apply Document GC policies as they execute.
  *
  * NOTE: This ctor must not change signature - C* uses reflection to
  * allocate instances of this class.
  */
final class CollectionStrategy(cfs: ColumnFamilyStore, options: JMap[String, String])
    extends AbstractCompactionStrategy(cfs, options) {

  import CollectionStrategy._

  private[this] val maxSSTableSizeMB = configureSSTableSize(options)

  val manifest: LeveledManifest = {
    val stcsOptions = new SizeTieredCompactionStrategyOptions(options)
    new LeveledManifest(cfs, maxSSTableSizeMB, stcsOptions)
  }

  /** @param gcBefore Timestamp in epoch micros.
    */
  def getNextBackgroundTask(gcBefore: Int): AbstractCompactionTask =
    synchronized {
      val tasks = getMaximalTask(gcBefore)

      if ((tasks eq null) || tasks.size == 0) {
        null
      } else {
        tasks.iterator.next()
      }
    }

  /** @param gcBefore Timestamp in epoch micros.
    */
  def getMaximalTask(gcBefore: Int): Collection[AbstractCompactionTask] = {
    var op = OperationType.COMPACTION
    var candidate = manifest.getCompactionCandidates(
      gcBefore,
      tombstoneThreshold,
      tombstoneCompactionInterval)

    if (candidate eq null) {
      findDroppableSSTable(gcBefore) match {
        case None =>
          logger.debug(s"No compaction necessary for $this")
          return null

        case Some(sstable) =>
          candidate = new LeveledManifest.CompactionCandidate(
            Collections.singleton(sstable),
            sstable.getSSTableLevel,
            getMaxSSTableBytes)

          op = OperationType.TOMBSTONE_COMPACTION
      }
    }

    if (cfs.getDataTracker.markCompacting(candidate.sstables)) {
      val task =
        new CollectionTask(
          cfs,
          candidate,
          gcBefore,
          CollectionStrategy.hints,
          filterBuilder(cfs.name)
        )
      task.setCompactionType(op)
      Collections.singletonList(task)
    } else {
      null
    }
  }

  override def getUnleveledSSTables() = getLevelSize(0)

  override def getScanners(
    sstables: Collection[SSTableReader],
    range: CRange[Token]): ScannerList = {

    val sstablesPerLevel =
      manifest.getSStablesPerLevelSnapshot() // FIXME: capitalize 't'

    // TODO: replace guava data structure
    val byLevel = ArrayListMultimap.create[Integer, SSTableReader]()

    sstables forEach { sstable =>
      var level = sstable.getSSTableLevel()

      // If an sstable is not on the manifest, it was recently added
      // or removed so we add it to level -1 and create exclusive
      // scanners for it. - see below (CASSANDRA-9935)
      if (
        level >= sstablesPerLevel.length || !sstablesPerLevel(level).contains(
          sstable)
      ) {
        logger.warn(
          s"Live sstable ${sstable.getFilename()} from level $level" +
            " is not on corresponding level in the leveled manifest." +
            " This is not a problem per se, but may indicate an orphaned sstable due to a failed" +
            " compaction not cleaned up properly.")
        level = -1
      }

      byLevel.get(level).add(sstable)
    }

    val scanners = new ArrayList[ISSTableScanner](sstables.size)

    try {
      byLevel.keySet forEach { level =>
        // Level can be -1 when sstables are added to DataTracker but
        // not to LeveledManifest since we don't know to which level
        // those sstables belong yet, we simply do the same as L0
        // sstables.
        if (level <= 0) {
          // L0 makes no guarantees about overlapping-ness. Just create a direct
          // scanner for each.
          byLevel.get(level) forEach { sstable =>
            scanners.add(
              sstable.getScanner(range, CompactionManager.instance.getRateLimiter()))
          }
        } else {
          // Create a LeveledScanner that only opens one sstable at a time, in sorted
          // order.
          val intersecting = LeveledScanner.intersecting(byLevel.get(level), range)

          if (!intersecting.isEmpty()) {
            scanners.add(new LeveledScanner(intersecting, range))
          }
        }
      }

      new ScannerList(scanners)
    } catch {
      case NonFatal(ex) =>
        try {
          new ScannerList(scanners).close()
        } catch {
          case NonFatal(supp) => ex.addSuppressed(supp)
        }

        throw ex
    }
  }

  def getUserDefinedTask(sstables: Collection[SSTableReader], gcBefore: Int) =
    throw new UnsupportedOperationException(
      "User-defined compactions are not supported.")

  def getEstimatedRemainingTasks() = manifest.getEstimatedTasks()
  def getMaxSSTableBytes() = maxSSTableSizeMB * 1024 * 1024

  def addSSTable(sstable: SSTableReader, forceL0: Boolean): Unit =
    manifest.add(sstable, forceL0)

  def removeSSTable(sstable: SSTableReader): Unit =
    manifest.remove(sstable)

  def getLevelSize(level: Int): Int =
    manifest.getLevelSize(level)

  override def toString = s"CollectionStrategy(${cfs.name})"

  /** Find the first SSTable which contains a large number of droppable
    * tombstones and is worth rewriting, starting at the highest level
    * of the hierarchy.
    *
    * @param gcBefore Timestamp in epoch micros.
    */
  private def findDroppableSSTable(gcBefore: Int): Option[SSTableReader] = {
    def find1(
      level: Int,
      sstables: Collection[SSTableReader]): Option[SSTableReader] = {
      val compacting = cfs.getDataTracker.unsafeGetCompacting

      val iter = sstables.iterator

      while (iter.hasNext()) {
        val sstable = iter.next()

        // Once an SSTable with a ratio below the threshold has been
        // seen, no other SSTables may be droppable due to the sort
        // order. Move to the next level down the hierarchy.
        val ratio = sstable.getEstimatedDroppableTombstoneRatio(gcBefore)
        if (ratio <= tombstoneThreshold) {
          return find0(level - 1)
        }

        if (
          !compacting.contains(sstable) &&
          !sstable.isMarkedSuspect &&
          worthDroppingTombstones(sstable, gcBefore)
        ) {
          return Some(sstable)
        }
      }

      None
    }

    @annotation.tailrec
    def find0(level: Int): Option[SSTableReader] = {
      if (level == 0) {
        return None
      }

      // Sort SSTables in this level by droppable ratio, descending.
      val sstables = manifest.getLevelSorted(
        level,
        { (a, b) =>
          val r1 = a.getEstimatedDroppableTombstoneRatio(gcBefore)
          val r2 = b.getEstimatedDroppableTombstoneRatio(gcBefore)
          -1 * r1.compareTo(r2)
        })

      if (!sstables.isEmpty) {
        val candidate = find1(level, sstables)
        if (candidate.nonEmpty) {
          return candidate
        }
      }

      find0(level - 1)
    }

    find0(manifest.getLevelCount())
  }
}
