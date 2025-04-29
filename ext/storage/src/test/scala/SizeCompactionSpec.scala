package fauna.storage.test

import fauna.atoms._
import fauna.lang.clocks.Clock
import fauna.stats._
import fauna.storage.{ Mutation => _, _ }
import fauna.storage.cassandra._
import fauna.storage.cassandra.comparators._
import org.apache.cassandra.db._
import org.apache.cassandra.utils.ByteBufferUtil
import scala.concurrent.duration._

class SizeCompactionSpec extends Spec {

  val columnFamily = ColumnFamilySchema[(Array[Byte], Array[Byte])](
    "SizeCompaction",
    Seq(BytesType),
    compression = Compression.None)

  override def beforeEach() = {
    super.beforeEach()
    val storage = Storage(keyspaceName, Seq(columnFamily))

    storage.init()

    val ks = Keyspace.open(keyspaceName)
    val cfs = ks.getColumnFamilyStore(columnFamily.name)
    cfs.disableAutoCompaction()
  }

  test("tombstones") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val cfs = keyspace.getColumnFamilyStore(columnFamily.name)

      val cells = 10

      val counter = new IteratorStatsCounter()
      val snapshotTS = now + cells.millis + 1.milli

      val key = decoratedKey("key")
      val adds = new Mutation(keyspaceName, key.getKey())

      // Insert some data.
      (0 until cells) foreach { i =>
        adds.add(
          columnFamily.name,
          cellname("cell", i.toString),
          ByteBufferUtil.EMPTY_BYTE_BUFFER,
          now.millis + i)
      }

      adds.apply()
      cfs.forceBlockingFlush()

      val live =
        new CassandraIterator(
          counter,
          cfs,
          ScanBounds(Segment.All),
          Selector.All,
          snapshotTS).toSeq

      live.size should equal(cells)

      val removes = new Mutation(keyspaceName, key.getKey())

      // Remove all the data.
      (0 until cells) foreach { i =>
        removes.delete(
          columnFamily.name,
          cellname("cell", i.toString),
          now.millis + i + 1)
      }

      removes.apply()
      cfs.forceBlockingFlush()

      val dead =
        new CassandraIterator(
          counter,
          cfs,
          ScanBounds(Segment.All),
          Selector.All,
          snapshotTS).toSeq

      dead.size should equal(0)

      val builder = SizeCompaction
        .newBuilder(cfs)
        .disableOverlap()
        .setOffline(false)

      val sstables = cfs.markAllCompacting()
      sstables forEach { builder.add(_) }
      builder.size() should equal(2)

      val tasks = builder.build()

      tasks foreach { _.run() }

      // SSTables were compacted together, but no tombstones were
      // applied, leaving a single SSTable.
      cfs.getSSTables().size should equal(1)

      val compact =
        new CassandraIterator(
          counter,
          cfs,
          ScanBounds(Segment.All),
          Selector.All,
          snapshotTS).toSeq

      // Data remains empty.
      compact.size should equal(0)
    }
  }
}
