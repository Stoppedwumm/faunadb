package fauna.storage.test

import fauna.atoms._
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.lang.{ TimeBound, Timestamp, Timing }
import fauna.prop.Prop
import fauna.stats.{ StatsRecorder, StatsRequestBuffer }
import fauna.storage._
import fauna.storage.cassandra._
import fauna.storage.cassandra.comparators._
import fauna.storage.ops._
import io.netty.buffer.{ ByteBuf, ByteBufAllocator, Unpooled }
import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.Charset
import java.nio.file._
import java.util.{ Arrays, Collections, UUID }
import org.apache.cassandra.db.{ Mutation => CMutation, _ }
import org.apache.cassandra.db.filter.QueryFilter
import org.apache.cassandra.dht.{ LongToken, Range, Token }
import org.apache.cassandra.io.sstable.SSTableReader
import org.apache.cassandra.service.StorageService
import org.apache.cassandra.utils.ByteBufferUtil
import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext }
import scala.util.Random

class StorageEngineSpec extends Spec {
  implicit val ec = ExecutionContext.parasitic

  val storageCF = ColumnFamilySchema[(Array[Byte], Array[Byte])](
    "StorageEngineSpec",
    Seq(BytesType))

  val transferCF =
    ColumnFamilySchema[(Array[Byte], Array[Byte])]("Transfers", Seq(BytesType))
  val hashCF =
    ColumnFamilySchema[(Array[Byte], Array[Byte])]("Hashes", Seq(BytesType))

  val cleanupCF =
    ColumnFamilySchema[(Array[Byte], Array[Byte])]("Cleanups", Seq(BytesType))

  var rowKey: ByteBuffer = _

  override def beforeEach() = {
    super.beforeEach()
    val storage = Storage(
      keyspaceName,
      Seq(
        storageCF,
        transferCF,
        hashCF,
        cleanupCF,
        Tables.Versions.Schema,
        Tables.RowTimestamps.Schema))
    storage.init(overrideIDs = false)
    val bytes = ByteBufferUtil.bytes("row")
    rowKey = StorageService.getPartitioner.decorateKey(bytes).getKey()
  }

  private def transferSSTables(
    sstables: java.util.Collection[SSTableReader]): Path = {
    val tmpSstableDir = Files.createTempDirectory("transfer-test")
    tmpSstableDir.toFile().deleteOnExit()

    val sstablesIter = sstables.iterator
    while (sstablesIter.hasNext()) {
      val target = sstablesIter.next()
      val sstable = SSTableReader.open(target.descriptor)
      val ranges = Collections.singletonList(
        new Range(
          StorageService.getPartitioner.getMinimumToken,
          StorageService.getPartitioner.getMinimumToken))

      transferSSTable(sstable, ranges, destinationDir = Some(tmpSstableDir))
    }

    tmpSstableDir
  }

  private def transferSSTable(
    sstable: SSTableReader,
    ranges: java.util.Collection[Range[Token]],
    destinationDir: Option[Path] = None,
    chunkSize: Int = 512): Path = {
    val dest = destinationDir map { dd =>
      File.createTempFile("transfer-test", "-Data.db", dd.toFile())
    } getOrElse {
      File.createTempFile("transfer-test", "-Data.db")
    }

    dest.deleteOnExit()

    val file = FileChannel.open(
      dest.toPath,
      StandardOpenOption.CREATE,
      StandardOpenOption.READ,
      StandardOpenOption.WRITE)

    val transfer = new CompressedTransfer(
      sstable.selfRef,
      sstable.estimatedKeysForRanges(ranges),
      sstable.getPositionsForRanges(ranges),
      StatsRecorder.Null)

    try {
      var read = 0
      while (read < transfer.totalSize) {
        val chunk = transfer.nextChunk(ByteBufAllocator.DEFAULT, chunkSize)
        read += chunk.readBytes(file, read, chunk.readableBytes)
        chunk.release()
      }
    } finally {
      transfer.close()
      file.close()
    }

    dest.toPath()
  }

  prop("unborn cells don't affect pages") {
    for {
      keys <- Prop.int(1 to 1000)
      unborn <- Prop.int(1 to keys)
      count <- Prop.int(0 to (keys - unborn))
      cellNames = (0 until keys) map nameBuf
      unbornNames <- Prop.shuffle(cellNames) map { _.take(unborn) }
    } withStorageEngine(StatsRecorder.Null) { storageEngine =>

      val now = Clock.time.micros
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(storageCF.name)

      store.clearUnsafe()
      store.disableAutoCompaction()

      cellNames foreach { name =>
        val rowMutation = new CMutation(keyspaceName, rowKey)
        rowMutation.add(storageCF.name, cellname(Seq(name)),
          ByteBufferUtil.EMPTY_BYTE_BUFFER, now)
        rowMutation.apply
      }

      unbornNames foreach { name =>
        val rowMutation = new CMutation(keyspaceName, rowKey)
        rowMutation.add(storageCF.name, cellname(Seq(name)),
          ByteBufferUtil.EMPTY_BYTE_BUFFER, now + 2)
        rowMutation.apply
      }

      val expected = cellNames collect {
        case n if !unbornNames.contains(n) => Unpooled.wrappedBuffer(n).toHexString
      }

      store.forceBlockingFlush()

      val range = Seq((
        Unpooled.EMPTY_BUFFER,
        Unpooled.EMPTY_BUFFER))

      val cells = storageEngine.rowCFSlice(
        Timestamp.ofMicros(now + 1),
        Unpooled.wrappedBuffer(rowKey),
        storageCF.name,
        range,
        Order.Descending,
        count,
        TimeBound.Max)

      cells.size should equal(count)
      cells.map { _.name.toHexString } should equal(expected.take(count))
    }
  }

  test("hashes rows") {
    val stats = new StatsRequestBuffer(Set(
      "Storage.Rows.Hashed", "Storage.Cells.Hashed", "Storage.PersistedTimestamp"))

    withStorageEngine(stats) { storageEngine =>
      val keys = 1_000_000
      val arr = new Array[Byte](1024)

      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(hashCF.name)

      store.clearUnsafe()
      store.disableAutoCompaction()
      stats.getD("Storage.PersistedTimestamp") should equal (0.0)

      for (i <- 0 until keys) {
        Arrays.fill(arr, i.toByte)

        val bytes = ByteBufferUtil.bytes(String.valueOf(i))
        val key = StorageService.getPartitioner.decorateKey(bytes)
        val mut = new CMutation(keyspaceName, key.getKey)
        val cell = cellname(Seq(ByteBufferUtil.bytes("col")))

        mut.add(hashCF.name, cell, ByteBuffer.wrap(arr), now.micros)
        keyspace.apply(mut, false /* writeCommitLog */ )
      }

      store.forceBlockingFlush()

      val timing = Timing.start
      val hashF = storageEngine.hash(
        cf = hashCF.name,
        segment = Segment.All,
        snapTime = now,
        deadline = 2.minutes.bound,
        maxDepth = 8)

      storageEngine.updateMaxAppliedTimestamp(now)

      Await.result(hashF, 1.minutes)
      stats.countOrZero("Storage.Rows.Hashed") should equal (keys)
      stats.countOrZero("Storage.Cells.Hashed") should equal (keys)
      stats.getD("Storage.PersistedTimestamp") should be > 0.0

      val elapsed = timing.elapsedMillis / 1000.0
      val keysPerSecond = keys / elapsed
      info(s"$keys keys: $elapsed secs, $keysPerSecond keys/sec")
    }
  }

  test("hash discards tombstones and unborn cells") {
    val stats = new StatsRequestBuffer(Set(
      "Storage.Rows.Hashed", "Storage.Cells.Hashed"))

    withStorageEngine(stats) { storageEngine =>
      val keys = 1_000
      val v1 = new Array[Byte](1024)
      val v2 = new Array[Byte](1024)
      val v3 = new Array[Byte](1024)

      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(hashCF.name)

      store.clearUnsafe()
      store.disableAutoCompaction()

      def addVersion(
        key: String,
        version: String,
        data: Array[Byte],
        ts: Long): Unit = {

        val bytes = ByteBufferUtil.bytes(key)
        val rowKey = StorageService.getPartitioner.decorateKey(bytes)
        val mut = new CMutation(keyspaceName, rowKey.getKey)
        val cell = cellname(Seq(ByteBufferUtil.bytes(version)))
        mut.add(hashCF.name, cell, ByteBuffer.wrap(data), ts)
        mut.apply()
      }

      for (i <- 0 until keys) {
        Arrays.fill(v1, (i - 1).toByte)
        Arrays.fill(v2, i.toByte)
        Arrays.fill(v3, (i + 1).toByte)

        addVersion(String.valueOf(i), "v1", v1, now.micros - 10)
        addVersion(String.valueOf(i), "v2", v2, now.micros)
        addVersion(String.valueOf(i), "v3", v3, now.micros + 10)
      }

      store.forceBlockingFlush()

      val timing = Timing.start
      val hashF = storageEngine.hash(
        cf = hashCF.name,
        segment = Segment.All,
        snapTime = now + 5.micros,
        deadline = 2.minutes.bound,
        maxDepth = 8)

      storageEngine.updateMaxAppliedTimestamp(now + 10.micros)

      Await.result(hashF, 1.minute)
      // hashes v1 and v2, thus discarding unborn cell v3
      stats.countOrZero("Storage.Rows.Hashed") should equal (keys)
      stats.countOrZero("Storage.Cells.Hashed") should equal (keys * 2)

      val elapsed = timing.elapsedMillis / 1000.0
      val keysPerSecond = keys / elapsed
      info(s"$keys keys: $elapsed secs, $keysPerSecond keys/sec")

      val emptyHashF = storageEngine.hash(
          cf = hashCF.name,
          segment = Segment.All,
          snapTime = now - 20.micros,
          deadline = TimeBound.Max,
          maxDepth = 8)

      // If only has unborn cells, regard the hash as empty.
      Await.result(emptyHashF, 1.minute).hash shouldBe empty
    }
  }

  test("hash can filter keys") {
    val stats = new StatsRequestBuffer(Set(
      "Storage.Rows.Hashed", "Storage.Cells.Hashed"))

    withStorageEngine(stats) { storageEngine =>
      val keys = 1_000
      val arr = new Array[Byte](1024)

      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(hashCF.name)

      store.clearUnsafe()
      store.disableAutoCompaction()

      for (i <- 0 until keys) {
        Arrays.fill(arr, i.toByte)

        val bytes = ByteBufferUtil.bytes(i.toLong)
        val key = StorageService.getPartitioner.decorateKey(bytes)
        val mut = new CMutation(keyspaceName, key.getKey)
        val cell = cellname(Seq(ByteBufferUtil.bytes("col")))

        mut.add(hashCF.name, cell, ByteBuffer.wrap(arr), now.micros)
        mut.apply()
      }

      store.forceBlockingFlush()

      def filterKey(buf: ByteBuf): Boolean = {
        val i = ByteBufferUtil.toLong(buf.nioBuffer())
        i < keys/2
      }

      val timing = Timing.start
      val hashF = storageEngine.hash(
        cf = hashCF.name,
        segment = Segment.All,
        snapTime = now,
        deadline = 2.minutes.bound,
        maxDepth = 8,
        filterKey = filterKey)

      storageEngine.updateMaxAppliedTimestamp(now)

      Await.result(hashF, 1.minute)
      stats.countOrZero("Storage.Rows.Hashed") should equal (keys/2)
      stats.countOrZero("Storage.Cells.Hashed") should equal (keys/2)

      val elapsed = timing.elapsedMillis / 1000.0
      val keysPerSecond = keys / elapsed
      info(s"$keys keys: $elapsed secs, $keysPerSecond keys/sec")
    }
  }

  test("receive transfer") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time.millis
      val keyspace = Keyspace.open(keyspaceName)
      val store = keyspace.getColumnFamilyStore(transferCF.name)

      store.disableAutoCompaction()

      val key = StorageService.getPartitioner.decorateKey(
        ByteBufferUtil.bytes(String.valueOf(0))
      )

      val key2 = StorageService
        .getPartitioner()
        .decorateKey(
          ByteBufferUtil.bytes(String.valueOf(1))
        )

      val keyVals = Seq((key, "hello"), (key2, "goodbye"))

      keyVals foreach { case (k, v) =>
        val mut = new CMutation(keyspaceName, k.getKey())
        mut.add(
          transferCF.name,
          cellname(Seq(ByteBufferUtil.bytes("col"))),
          ByteBufferUtil.bytes(v),
          now)
        keyspace.apply(mut, false /* writeCommitLog */ )
        store.forceBlockingFlush()
      }

      val transferLocation = transferSSTables(store.getSSTables())

      // clear data
      store.truncateBlocking()

      // validate nothing is present for the keys
      keyVals foreach { case (key, _) =>
        val row = keyspace.getRow(
          QueryFilter.getIdentityFilter(key, transferCF.name, now)
        )
        row.cf shouldBe null
      }

      // load data via receiveTransfer
      Await.result(
        storageEngine.receiveTransfer(
          UUID.randomUUID(),
          transferCF.name,
          transferLocation,
          2.minutes.bound),
        1 minutes)

      val values = keyVals map { case (key, _) =>
        val row = keyspace.getRow(
          QueryFilter.getIdentityFilter(key, transferCF.name, now)
        )
        val cells = row.cf.getSortedColumns()
        cells.size() shouldEqual 1
        new String(cells.stream().findFirst().get().value().array())
      }

      values should contain theSameElementsAs keyVals.map(_._2)
    }
  }

  test("cleans up rows") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      def keyExists(key: ByteBuffer): Boolean = {
        val range = Seq((
          Unpooled.EMPTY_BUFFER,
          Unpooled.EMPTY_BUFFER))

        storageEngine.rowCFSlice(
          Clock.time,
          Unpooled.wrappedBuffer(key),
          cleanupCF.name,
          range,
          Order.Descending,
          1,
          TimeBound.Max).nonEmpty
      }

      val keys = 1_000
      val arr = new Array[Byte](1024)

      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(cleanupCF.name)

      store.clearUnsafe()
      store.disableAutoCompaction()

      var tokens = Map.empty[LongToken, Seq[ByteBuffer]]

      for (i <- 0 until keys) {
        Arrays.fill(arr, i.toByte)

        val bytes = ByteBufferUtil.bytes(i.toLong)
        val key = StorageService.getPartitioner.decorateKey(bytes)

        val token = key.getToken.asInstanceOf[LongToken] // I pinky-promise.
        tokens.get(token) match {
          case None =>
            tokens += token -> Seq(bytes)
          case Some(ks) =>
            tokens += token -> (ks :+ bytes)
        }

        val mut = new CMutation(keyspaceName, key.getKey)
        val cell = cellname(Seq(ByteBufferUtil.bytes("col")))

        mut.add(cleanupCF.name, cell, ByteBuffer.wrap(arr), now.micros)
        mut.apply()
      }

      store.forceBlockingFlush()

      // everything exists
      tokens.values foreach { keys =>
        keys forall { keyExists(_) } should be (true)
      }

      // pick some tokens to keep and some to cleanup
      val (keep, kill) = tokens partition { case (_, _) => Random.nextBoolean() }

      // translate from C* to FaunaDB lingo
      val segs = keep map {
        case (token, _) =>
          Segment(Location(token.getTokenValue), Location(token.getTokenValue + 1))
      }

      storageEngine.needsCleanup(cleanupCF.name, segs.toSeq) should be(true)
      storageEngine.cleanup(cleanupCF.name, segs.toSeq) should be(true)
      storageEngine.needsCleanup(cleanupCF.name, segs.toSeq) should be(false)

      // in with the good
      keep.values foreach { keys =>
        keys forall { keyExists(_) } should be (true)
      }

      // out with the bad
      kill.values foreach { keys =>
        keys forall { !keyExists(_) } should be (true)
      }
    }
  }

  prop("roundtrips") {
    for {
      keys <- Prop.int(1 to 1_000_000)
      chunkSize <- Prop.int(1 to 512 * 1024)
    } {
      val now = Clock.time.millis
      val keyspace = Keyspace.open(keyspaceName)
      val store = keyspace.getColumnFamilyStore(transferCF.name)

      store.clearUnsafe()
      store.disableAutoCompaction()

      for (i <- 0 until keys) {
        val bytes = ByteBufferUtil.bytes(String.valueOf(i))
        val key = StorageService.getPartitioner.decorateKey(bytes)
        val mut = new CMutation(keyspaceName, key.getKey())

        mut.add(transferCF.name, cellname(Seq(ByteBufferUtil.bytes("col"))),
          ByteBufferUtil.EMPTY_BYTE_BUFFER, now)

        keyspace.apply(mut, false /* writeCommitLog */)
      }

      store.forceBlockingFlush()

      val target = store.getSSTables.iterator.next
      val sstable = SSTableReader.open(target.descriptor)
      val ranges = Collections.singletonList(
        new Range(StorageService.getPartitioner.getMinimumToken,
          StorageService.getPartitioner.getMinimumToken))

      val loadedSSTableLocation =
        transferSSTable(sstable, ranges, chunkSize = chunkSize)
      val file = FileChannel.open(loadedSSTableLocation)
      try {
        val fileInfo = CompressedTransfer.loadFileInfo(loadedSSTableLocation, file, StatsRecorder.Null)

        val writer = CompressedTransfer.load(
          keyspaceName,
          fileInfo,
          file,
          StatsRecorder.Null)
        val reader = writer.closeAndOpenReader

        (reader.first, reader.last) should equal ((sstable.first, sstable.last))
        reader.onDiskLength should equal (sstable.onDiskLength)
        reader.estimatedKeys should equal (sstable.estimatedKeys)

        reader.selfRef.release()
      } finally {
        file.close()
      }
    }
  }

  once("records and reads a failure") {
    for {
      row <- Prop.alphaNumString()
    } withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val ts = Clock.time
      val key = Unpooled.copiedBuffer(row, Charset.forName("UTF-8"))

      val failure = RemoveAllWrite(storageCF.name, key)

      storageEngine.recordFailure(key, ts, failure, Seq(failure))
      // make sure we can read the failure we just wrote. this will throw an exception if we can't.
      storageEngine.readFailure(storageEngine.config.rootPath / s"transaction-failure-$ts")
    }
  }

  test("lock gauge works") {
    val stats = new StatsRequestBuffer(Set("Storage.PersistedTimestamp", "Storage.Locked"))

    withStorageEngine(stats) { storageEngine =>
      stats.getD("Storage.Locked") should equal (0.0)
      storageEngine.lock()
      stats.getD("Storage.Locked") should equal (1.0)
      storageEngine.unlock()
      stats.getD("Storage.Locked") should equal (0.0)
    }
  }
}
