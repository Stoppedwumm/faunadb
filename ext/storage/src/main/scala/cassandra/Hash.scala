package fauna.storage.cassandra

import fauna.atoms.{ Location, Segment }
import fauna.lang.{ TimeBound, Timestamp, Timing }
import fauna.lang.syntax._
import fauna.stats.StatsRecorder
import fauna.storage._
import io.netty.buffer.{ ByteBuf, Unpooled }
import java.lang.{ Long => JLong }
import java.security.MessageDigest
import java.util.{ Arrays, List => JList }
import java.util.concurrent.ThreadLocalRandom
import org.apache.cassandra.db.{
  Cell => CCell,
  ColumnFamilyStore,
  DataRange,
  DecoratedKey,
  OnDiskAtom
}
import org.apache.cassandra.db.compaction.{
  CompactionController,
  CompactionInterruptedException,
  CompactionIterable,
  OperationType
}
import org.apache.cassandra.dht.LongToken
import org.apache.cassandra.io.sstable.{
  ISSTableScanner,
  SSTableReader
}
import org.apache.cassandra.io.util.DataOutputPlus
import scala.jdk.CollectionConverters._

object Hash {

  final class Controller(cfs: ColumnFamilyStore, gcBefore: Timestamp)
      extends CompactionController(cfs, cfs.gcBefore(gcBefore.millis)) {

    override def maxPurgeableTimestamp(key: DecoratedKey) = Long.MaxValue
  }

  final class Scanner(
    cfs: ColumnFamilyStore,
    scanners: JList[ISSTableScanner],
    gcBefore: Timestamp)
      extends CompactionIterable(OperationType.UNKNOWN, // Don't use OT.CLEANUP.
                                 scanners,
                                 new Controller(cfs, gcBefore))

  // This dingus allows us to know whether or not any bytes at all
  // have passed through the digest. If none have, we don't have a
  // hash at all.
  final class CountingDigest(inner: MessageDigest)
      extends MessageDigest(inner.getAlgorithm) {

    private[this] var count = 0L

    def bytes: Long = count

    override def engineUpdate(input: Byte): Unit = {
      inner.update(input)
      count += 1
    }

    override def engineUpdate(input: Array[Byte], off: Int, len: Int): Unit = {
      inner.update(input, off, len)
      count += len
    }

    override def engineDigest(): Array[Byte] = inner.digest()

    override def engineReset(): Unit = inner.reset()
  }

}

case class HashFilter(
  filterKey: ByteBuf => Boolean,
  filterCell: (ByteBuf, CCell) => Boolean)

object HashFilter {
  private val defaultFilterKey = (_: ByteBuf) => true
  private val defaultFilterCell = (_: ByteBuf, _: CCell) => true

  val All = HashFilter(defaultFilterKey, defaultFilterCell)

  def apply(filterKey: ByteBuf => Boolean): HashFilter =
    HashFilter(filterKey, defaultFilterCell)

  def apply(filterCell: (ByteBuf, CCell) => Boolean): HashFilter =
    HashFilter(defaultFilterKey, filterCell)
}

/**
  * Hashes a segment within a set of SSTables, returning a HashTree of
  * the data therein.
  *
  * The data within the segment can optionally be filtered with the
  * `filterKey` predicate to select only data which is of interest.
  *
  * See also: HashTree
  */
final class Hash(
  cfs: ColumnFamilyStore,
  sstables: Iterable[SSTableReader],
  segment: Segment,
  snapshotTS: Timestamp,
  maxDepth: Int,
  deadline: TimeBound,
  stats: StatsRecorder,
  filter: HashFilter) {

  import Hash._

  private[this] val logger = getLogger

  def get(): HashTree = {
    val start = Timing.start
    val range = CassandraHelpers.toRange(segment)

    // assume data is evenly distributed across the keyspace
    var partitions = 0L
    sstables foreach { sst =>
      partitions += sst.estimatedKeysForRanges(Arrays.asList(range))
    }

    val depth = if (partitions > 0) {
      (63 - JLong.numberOfLeadingZeros(partitions)) min maxDepth
    } else {
      1
    }

    val tree = HashTree.newBuilder(segment, depth)
    val scanners = sstables map { _.getScanner(DataRange.forKeyRange(range)) } toList
    val scanner = new Scanner(cfs, scanners.asJava, snapshotTS)
    val iter = scanner.iterator

    var totalRows = 0
    var totalCells = 0
    while (iter.hasNext) {
      if (ThreadLocalRandom.current.nextDouble() < StorageEngine.DeadlineCheckPercentage) {
        deadline.checkOrThrow()
      }

      if (scanner.isStopRequested()) {
        throw new CompactionInterruptedException(scanner.getCompactionInfo())
      }

      val row = iter.next
      val rowKey = Unpooled.wrappedBuffer(row.key.getKey)
      if (filter.filterKey(rowKey)) {
        var cells = 0
        val digest = new CountingDigest(MessageDigest.getInstance("SHA-256"))
        val serializer = new OnDiskAtom.SerializerForWriting {
          def serializeForSSTable(
            atom: OnDiskAtom,
            out: DataOutputPlus): Unit = {
            // Discard unborn, tombstones, and expired cells
            atom match {
              case cell: CCell =>
                if (cell.isLive(snapshotTS.millis) &&
                  cell.timestamp <= snapshotTS.micros &&
                  filter.filterCell(rowKey, cell)) {
                  atom.updateDigest(digest)
                  cells += 1
                }
              case _ =>
            }
          }

          // Mimic C* implementation
          def serializedSizeForSSTable(atom: OnDiskAtom): Long = 0L
        }

        digest.update(row.key.getKey.duplicate())
        row.update(digest, serializer)

        // NB: Rows with no live cells (because they are unborn or tombstoned)
        // should be omitted from the tree.
        val tok = row.key.getToken.asInstanceOf[LongToken]
        if (cells > 0 && digest.bytes > 0) {
          val hash = digest.digest
          logger.debug(s"Hashing row at $tok with hash ${hash.toHexString}")
          tree.insert(Location(tok.getTokenValue), hash)
          totalCells += cells
          totalRows += 1
        } else {
          logger.debug(s"Not hashing row at $tok because it has no live cells")
        }
      }
    }

    stats.count("Storage.Rows.Hashed", totalRows)
    stats.count("Storage.Cells.Hashed", totalCells)
    stats.timing("Storage.Hash.Time", start.elapsedMillis)

    logger.info(
      s"Hashing segment $segment of ${cfs.name} as of $snapshotTS complete. " +
        s"rows=$totalRows cells=$totalCells")

    tree.result
  }
}
