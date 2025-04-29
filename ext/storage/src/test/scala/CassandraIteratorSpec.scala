package fauna.storage.test

import fauna.atoms._
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.stats.StatsRecorder
import fauna.storage.{ Cell, _ }
import fauna.storage.cassandra._
import fauna.storage.cassandra.comparators._
import io.netty.buffer.Unpooled
import org.apache.cassandra.db.{ Mutation => CMutation }
import org.apache.cassandra.db.composites.CellName
import org.apache.cassandra.dht.LongToken
import org.apache.cassandra.service.StorageService
import org.apache.cassandra.utils.ByteBufferUtil
import scala.concurrent.ExecutionContext
import scala.util.Random

class CassandraIteratorSpec extends Spec {
  implicit val ec = ExecutionContext.parasitic

  val iteratorCF = ColumnFamilySchema[(Array[Byte], Array[Byte])](
    "CassandraIteratorSpec",
    Seq(BytesType))

  private def toCell(cellName: CellName, data: Int, ts: Timestamp) =
    Cell(
      Unpooled.wrappedBuffer(cellName.toByteBuffer),
      Unpooled.wrappedBuffer(ByteBufferUtil.bytes(data)),
      ts)

  override def beforeEach() = {
    super.beforeEach()
    val storage = Storage(
      keyspaceName,
      Seq(iteratorCF, Tables.Versions.Schema, Tables.RowTimestamps.Schema))
    storage.init(overrideIDs = false)
  }

  test("Scans whole token ring") {
    val counter = new IteratorStatsCounter()

    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(iteratorCF.name)

      store.clearUnsafe()
      store.disableAutoCompaction()

      val rowKey0 =
        StorageService.getPartitioner.decorateKey(ByteBufferUtil.bytes("row0"))
      val rowKey1 =
        StorageService.getPartitioner.decorateKey(ByteBufferUtil.bytes("row1"))
      val rowKey2 =
        StorageService.getPartitioner.decorateKey(ByteBufferUtil.bytes("row2"))

      val cell0 = cellname(Seq(ByteBufferUtil.bytes("col0")))
      val cell1 = cellname(Seq(ByteBufferUtil.bytes("col1")))
      val cell2 = cellname(Seq(ByteBufferUtil.bytes("col2")))

      val now = Clock.time

      Seq(rowKey0, rowKey1, rowKey2).zipWithIndex foreach {
        case (rowKey, rowIndex) =>
          val mut = new CMutation(keyspaceName, rowKey.getKey)

          Seq(cell0, cell1, cell2).zipWithIndex foreach { case (cell, cellIndex) =>
            val data = rowIndex * 3 + cellIndex

            mut.add(iteratorCF.name, cell, ByteBufferUtil.bytes(data), now.micros)
          }

          mut.apply()
      }

      store.forceBlockingFlush()

      val iter = new CassandraIterator(
        counter,
        store,
        ScanBounds(Segment.All),
        Selector.All,
        now)
      val cells = iter.toSeq map { case (key, cell) =>
        (key.toHexString, cell)
      }

      cells shouldBe Seq(
        // row 0
        (rowKey0.getKey.toHexString, toCell(cell0, 0, now)),
        (rowKey0.getKey.toHexString, toCell(cell1, 1, now)),
        (rowKey0.getKey.toHexString, toCell(cell2, 2, now)),
        // row 1
        (rowKey1.getKey.toHexString, toCell(cell0, 3, now)),
        (rowKey1.getKey.toHexString, toCell(cell1, 4, now)),
        (rowKey1.getKey.toHexString, toCell(cell2, 5, now)),
        // row 2
        (rowKey2.getKey.toHexString, toCell(cell0, 6, now)),
        (rowKey2.getKey.toHexString, toCell(cell1, 7, now)),
        (rowKey2.getKey.toHexString, toCell(cell2, 8, now))
      )

      counter.rows.sum() shouldBe 3L
      counter.cells.sum() shouldBe 9L
    }
  }

  test("Scans sub segment") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(iteratorCF.name)

      store.clearUnsafe()
      store.disableAutoCompaction()

      val rowKey0 =
        StorageService.getPartitioner.decorateKey(ByteBufferUtil.bytes("row0"))
      val rowKey1 =
        StorageService.getPartitioner.decorateKey(ByteBufferUtil.bytes("row1"))
      val rowKey2 =
        StorageService.getPartitioner.decorateKey(ByteBufferUtil.bytes("row2"))

      val cell0 = cellname(Seq(ByteBufferUtil.bytes("col0")))
      val cell1 = cellname(Seq(ByteBufferUtil.bytes("col1")))
      val cell2 = cellname(Seq(ByteBufferUtil.bytes("col2")))

      val now = Clock.time

      Seq(rowKey0, rowKey1, rowKey2).zipWithIndex foreach {
        case (rowKey, rowIndex) =>
          val mut = new CMutation(keyspaceName, rowKey.getKey)

          Seq(cell0, cell1, cell2).zipWithIndex foreach { case (cell, cellIndex) =>
            val data = rowIndex * 3 + cellIndex

            mut.add(iteratorCF.name, cell, ByteBufferUtil.bytes(data), now.micros)
          }

          mut.apply()
      }

      store.forceBlockingFlush()

      // scans only segment that contains rowKey0
      val token = rowKey0.getToken.asInstanceOf[LongToken]
      val segment =
        Segment(Location(token.getTokenValue), Location(token.getTokenValue + 1))

      val counter = new IteratorStatsCounter()
      val iter =
        new CassandraIterator(counter, store, ScanBounds(segment), Selector.All, now)
      val cells = iter.toSeq map { case (key, cell) =>
        (key.toHexString, cell)
      }

      cells shouldBe Seq(
        (rowKey0.getKey.toHexString, toCell(cell0, 0, now)),
        (rowKey0.getKey.toHexString, toCell(cell1, 1, now)),
        (rowKey0.getKey.toHexString, toCell(cell2, 2, now))
      )

      counter.rows.sum() shouldBe 1L
      counter.cells.sum() shouldBe 3L
    }
  }

  test("Only most recent column survive") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(iteratorCF.name)

      store.clearUnsafe()
      store.disableAutoCompaction()

      val rowKey =
        StorageService.getPartitioner.decorateKey(ByteBufferUtil.bytes("row"))

      val cell = cellname(Seq(ByteBufferUtil.bytes("coll")))

      val now = Clock.time

      val mut = new CMutation(keyspaceName, rowKey.getKey)

      mut.add(iteratorCF.name, cell, ByteBufferUtil.bytes(0), now.micros - 2)
      mut.add(iteratorCF.name, cell, ByteBufferUtil.bytes(1), now.micros - 1)
      mut.add(iteratorCF.name, cell, ByteBufferUtil.bytes(2), now.micros - 0)

      mut.apply()

      store.forceBlockingFlush()

      val counter = new IteratorStatsCounter()
      val iter =
        new CassandraIterator(
          counter,
          store,
          ScanBounds(Segment.All),
          Selector.All,
          now)
      val cells = iter.toSeq map { case (key, cell) =>
        (key.toHexString, cell)
      }

      cells shouldBe Seq((rowKey.getKey.toHexString, toCell(cell, 2, now)))

      counter.rows.sum() shouldBe 1L
      counter.cells.sum() shouldBe 1L
    }
  }

  test("row tombstone") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(iteratorCF.name)

      store.clearUnsafe()
      store.disableAutoCompaction()

      def randomString() = Random.nextString(8)

      val rowKey = StorageService.getPartitioner.decorateKey(
        ByteBufferUtil.bytes(randomString()))

      val cell0 = cellname(Seq(ByteBufferUtil.bytes(randomString())))
      val cell1 = cellname(Seq(ByteBufferUtil.bytes(randomString())))
      val cell2 = cellname(Seq(ByteBufferUtil.bytes(randomString())))

      val now = Clock.time

      val dataInt = 0x1234
      val data = ByteBufferUtil.bytes(dataInt)

      // 3 cells
      val mut0 = new CMutation(keyspaceName, rowKey.getKey)
      mut0.add(iteratorCF.name, cell0, data, now.micros)
      mut0.add(iteratorCF.name, cell1, data, now.micros)
      mut0.add(iteratorCF.name, cell2, data, now.micros)
      mut0.apply()

      store.forceBlockingFlush()

      // row tombstone
      val mut1 = new CMutation(keyspaceName, rowKey.getKey)
      mut1.delete(iteratorCF.name, now.micros)
      mut1.apply()

      store.forceBlockingFlush()

      val counter = new IteratorStatsCounter()
      val iterator =
        new CassandraIterator(
          counter,
          store,
          ScanBounds(Segment.All),
          Selector.All,
          now)

      iterator.size shouldBe 0

      val keySize = rowKey.getKey.remaining() * 2 // key lives in two sstables

      counter.rows.sum() shouldBe 1L
      counter.cells.sum() shouldBe 0L
      counter.liveCells.sum() shouldBe 0L
      counter.deletedCells.sum() shouldBe 0L
      counter.unbornCells.sum() shouldBe 0L
      counter.tombstones.sum() shouldBe 1L
      counter.bytesRead.sum() shouldBe keySize
    }
  }

  test("cell tombstone") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(iteratorCF.name)

      store.clearUnsafe()
      store.disableAutoCompaction()

      def randomString() = Random.nextString(8)

      val rowKey = StorageService.getPartitioner.decorateKey(
        ByteBufferUtil.bytes(randomString()))

      val cell0 = cellname(Seq(ByteBufferUtil.bytes(randomString())))
      val cell1 = cellname(Seq(ByteBufferUtil.bytes(randomString())))
      val cell2 = cellname(Seq(ByteBufferUtil.bytes(randomString())))

      val now = Clock.time

      val dataInt = 0x1234
      val data = ByteBufferUtil.bytes(dataInt)

      val mut0 = new CMutation(keyspaceName, rowKey.getKey)
      mut0.add(iteratorCF.name, cell0, data, now.micros) // liveCell
      mut0.add(iteratorCF.name, cell1, data, now.micros + 1) // unbornCell
      mut0.add(iteratorCF.name, cell2, data, now.micros) // liveCell
      mut0.apply()

      store.forceBlockingFlush()

      // cell tombstone
      val mut1 = new CMutation(keyspaceName, rowKey.getKey)
      mut1.delete(iteratorCF.name, cell2, now.micros) // deleted
      mut1.apply()

      store.forceBlockingFlush()

      val counter = new IteratorStatsCounter()
      val iterator =
        new CassandraIterator(
          counter,
          store,
          ScanBounds(Segment.All),
          Selector.All,
          now)

      val cells = iterator.toSeq map { case (key, cell) =>
        (key.toHexString, cell)
      }

      cells shouldBe Seq(
        (rowKey.getKey.toHexString, toCell(cell0, dataInt, now))
      )

      val keySize = rowKey.getKey.remaining() * 2 // key lives in two sstables
      val cell0Size = cell0.toByteBuffer.remaining() + data.remaining() + 8
      val cell1Size = cell1.toByteBuffer.remaining() + data.remaining() + 8
      val cell2Size = cell2.toByteBuffer.remaining() + data.remaining() + 8
      val cell2DeletedSize = cell2.toByteBuffer.remaining() + 8 + 4

      counter.rows.sum() shouldBe 1L
      counter.cells.sum() shouldBe 4L
      counter.liveCells.sum() shouldBe 2L
      counter.deletedCells.sum() shouldBe 1L
      counter.unbornCells.sum() shouldBe 1L
      counter.tombstones.sum() shouldBe 0L
      counter.bytesRead
        .sum() shouldBe (keySize + cell0Size + cell1Size + cell2Size + cell2DeletedSize)
    }
  }
}
