package fauna.storage.test

import fauna.stats._
import fauna.storage.{ Mutation => _, _ }
import fauna.storage.cassandra._
import fauna.storage.cassandra.comparators._
import java.nio.ByteBuffer
import org.apache.cassandra.db._
import org.apache.cassandra.db.compaction._
import org.apache.cassandra.io.sstable.SSTableReader
import org.apache.cassandra.utils.{ ByteBufferUtil, FBUtilities }
import org.scalatest.concurrent.Eventually
import scala.concurrent.duration._
import scala.util.Random

class CollectionStrategySpec extends Spec with Eventually {

  val columnFamily = ColumnFamilySchema[(Array[Byte], Array[Byte])](
    "Bytes",
    Seq(BytesType),
    compaction = Compaction.Collection,
    compression = Compression.None)

  override def beforeEach() = {
    super.beforeEach()
    val storage = Storage(keyspaceName, Seq(columnFamily))

    storage.init()

    val ks = Keyspace.open(keyspaceName)
    val cfs = ks.getColumnFamilyStore(columnFamily.name)
    cfs.enableAutoCompaction()
  }

  override def afterEach() = {
    super.afterEach()
    val ks = Keyspace.open(keyspaceName)
    val cfs = ks.getColumnFamilyStore(columnFamily.name)
    cfs.truncateBlocking()
  }

  def wait(cfs: ColumnFamilyStore): Unit = {
    val strat = cfs.getCompactionStrategy.asInstanceOf[WrappingCompactionStrategy]

    var nonEmptyL1 = false

    eventually(timeout(1.minute)) {
      strat.getWrappedStrategies forEach { strat =>
        strat match {
          case cs: CollectionStrategy =>
            (1 until 5) foreach { i =>
              if (cs.getLevelSize(i) > 0) {
                nonEmptyL1 = true
              }
            }
          case s =>
            fail(s"$s found, but CollectionStrategy expected")
        }
      }

      nonEmptyL1 should be(true)
    }
  }

  test("levels") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val keyspace = storageEngine.keyspace
      val cfs = keyspace.getColumnFamilyStore(columnFamily.name)

      val b = new Array[Byte](100 * 1024)
      Random.nextBytes(b)
      val buf = ByteBuffer.wrap(b)

      val rows = 200
      val cells = 10

      (0 until rows) foreach { i =>
        val key = decoratedKey(String.valueOf(i))
        val mut = new Mutation(keyspaceName, key.getKey())

        (0 until cells) foreach { j =>
          mut.add(columnFamily.name, cellname("cell", j.toString), buf, 0)
        }

        mut.apply()
        cfs.forceBlockingFlush()
      }

      wait(cfs)

      val wrap = cfs.getCompactionStrategy.asInstanceOf[WrappingCompactionStrategy]
      val strategy =
        wrap.getWrappedStrategies.get(1).asInstanceOf[CollectionStrategy]

      strategy.getLevelSize(1) should be > 0

      val sstables = strategy.manifest.getLevel(1)
      val scanners = strategy.getScanners(sstables).scanners

      scanners.size should be(1)

      val scanner = scanners.get(0)
      while (scanner.hasNext()) {
        scanner.next()
      }

      scanner.getCurrentPosition should be(
        SSTableReader.getTotalUncompressedBytes(sstables))
    }
  }

  test("tombstones") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val keyspace = storageEngine.keyspace
      val cfs = keyspace.getColumnFamilyStore(columnFamily.name)

      val cells = 10

      val key = decoratedKey("key")
      val adds = new Mutation(keyspaceName, key.getKey())

      // Insert some data.
      (0 until cells) foreach { i =>
        adds.add(
          columnFamily.name,
          cellname("cell", i.toString),
          ByteBufferUtil.EMPTY_BYTE_BUFFER,
          0)
      }

      adds.apply()
      cfs.forceBlockingFlush()

      val removes = new Mutation(keyspaceName, key.getKey())

      // Remove all the data.
      (0 until cells) foreach { i =>
        removes.delete(columnFamily.name, cellname("cell", i.toString), 1)
      }

      removes.apply()
      cfs.forceBlockingFlush()

      FBUtilities.waitOnFutures(
        CompactionManager.instance.submitMaximal(cfs, Integer.MAX_VALUE))

      val wrap = cfs.getCompactionStrategy.asInstanceOf[WrappingCompactionStrategy]
      val strategy =
        wrap.getWrappedStrategies.get(1).asInstanceOf[CollectionStrategy]

      val levels = strategy.manifest.getAllLevelSize()
      levels.size should be > 0

      // All levels are empty - the data + tombstones were compacted away.
      levels foreach { size => size should be(0) }
    }
  }

  test("pause tombstone purges") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val keyspace = storageEngine.keyspace
      val cfs = keyspace.getColumnFamilyStore(columnFamily.name)

      val key = decoratedKey("key")
      val adds = new Mutation(keyspaceName, key.getKey())

      // Insert some data.
      adds.add(
        columnFamily.name,
        cellname("cell", "a"),
        ByteBufferUtil.EMPTY_BYTE_BUFFER,
        0
      )

      adds.apply()
      cfs.forceBlockingFlush()

      val removes = new Mutation(keyspaceName, key.getKey())

      // Remove all the data.
      removes.delete(columnFamily.name, cellname("cell", "a"), 1)

      removes.apply()
      cfs.forceBlockingFlush()

      CompactionController.pauseTombstonePurge()
      try {
        FBUtilities.waitOnFutures(
          CompactionManager.instance.submitMaximal(cfs, Integer.MAX_VALUE))
      } finally {
        CompactionController.unpauseTombstonePurge()
      }

      val wcs = cfs.getCompactionStrategy.asInstanceOf[WrappingCompactionStrategy]
      val strategy = wcs.getWrappedStrategies.get(1).asInstanceOf[CollectionStrategy]
      val levels = strategy.manifest.getAllLevelSize()
      levels.size should be > 0

      // Not all levels are empty -- the data + tombstones were not compacted away.
      atLeast(1, levels) should be > 0
    }
  }
}
