package fauna.storage.test

import fauna.atoms._
import fauna.lang.clocks.Clock
import fauna.lang.Timestamp
import fauna.stats.StatsRecorder
import fauna.storage._
import fauna.storage.cassandra._
import fauna.storage.index._
import fauna.storage.ops._
import io.netty.buffer.{ ByteBuf, Unpooled }
import java.util.{ HashSet, Set => JSet }
import org.apache.cassandra.db.{
  Cell => CCell,
  ColumnFamilyStore,
  Keyspace,
  OnDiskAtom
}
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator
import org.apache.cassandra.db.compaction._
import org.apache.cassandra.io.sstable.SSTableReader
import org.apache.cassandra.utils.CloseableIterator
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class HistoricalIndexFilterSpec extends Spec(keyspaceName = "historicalfilter") {
  override def beforeAll() = {
    super.beforeAll()
    val storage = Storage(
      keyspaceName,
      Seq(
        Tables.HistoricalIndex.Schema.copy(compaction = Compaction.Collection),
        Tables.SortedIndex.Schema
      ) // Sorted is required for write ops
    )

    storage.init(overrideIDs = false)
  }

  override def afterEach() = {
    super.afterEach()

    val ks = Keyspace.open(keyspaceName)
    val cfs = ks.getColumnFamilyStore(Tables.HistoricalIndex.CFName)
    cfs.truncateBlocking()
  }

  def iterator(cfs: ColumnFamilyStore)
    : (CloseableIterator[OnDiskAtomIterator], JSet[SSTableReader]) = {
    val wrap = cfs.getCompactionStrategy.asInstanceOf[WrappingCompactionStrategy]
    val strategy =
      wrap.getWrappedStrategies.get(1).asInstanceOf[CollectionStrategy]

    val sstables = strategy.manifest.getLevel(0)
    val scanners = strategy.getScanners(sstables).scanners

    scanners.size should be(1)
    (scanners.get(0), new HashSet(sstables))
  }

  def decodeValue(key: ByteBuf, cell: Cell) = {
    def decodeKey(bytes: ByteBuf): CValue =
      CValue(Tables.HistoricalIndex.Schema.keyComparator, bytes)

    def decodePredicate(key: CValue, name: ByteBuf) = {
      val prefixComponents =
        Tables.HistoricalIndex.Schema.nameComparator.bytesToCValues(name)
      Predicate(key, prefixComponents)
    }

    val pred = decodePredicate(decodeKey(key), cell.name)
    new Value[Tables.HistoricalIndex.Key](pred, cell.value, transactionTS = cell.ts)
  }

  // Gathers all events as of the provided snapshot time using
  // CassandraIterator as a check against the compaction strategy's
  // scanners.
  def allEvents(cfs: ColumnFamilyStore, snapshotTS: Timestamp) = {
    val counter = new IteratorStatsCounter()
    val rows =
      new CassandraIterator(
        counter,
        cfs,
        ScanBounds(Segment.All),
        Selector.All,
        snapshotTS).toSeq

    rows map { case (k, c) =>
      IndexValue.fromHistoricalValue(ScopeID(0), decodeValue(k, c))
    }
  }

  def event(key: ByteBuf, atom: OnDiskAtom): IndexValue = {
    def cell(atom: OnDiskAtom): Cell = {
      val cc = atom.asInstanceOf[CCell] // NB. range tombstones should not exist.
      val name = Unpooled.wrappedBuffer(cc.name.toByteBuffer)
      val value = Unpooled.wrappedBuffer(cc.value)
      val cts = Timestamp.ofMicros(cc.timestamp)

      Cell(name, value, cts)
    }

    IndexValue.fromHistoricalValue(ScopeID(0), decodeValue(key, cell(atom)))
  }

  test("missing gcBefore remains") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.HistoricalIndex.CFName)

      store.clearUnsafe()

      // Insert an "add" with a validTS at +10 msecs, and a "remove"
      // for the same values at +20 msecs.
      val add = SetAdd(
        ScopeID(0),
        IndexID(32768),
        Vector.empty, // terms
        Vector.empty, // values
        DocID(SubID(1), CollectionID(1024)),
        Resolved(Timestamp.ofMillis(10), now + 10.millis),
        Add
      )

      val remove = SetAdd(
        ScopeID(0),
        IndexID(32768),
        Vector.empty, // terms
        Vector.empty, // values
        DocID(SubID(1), CollectionID(1024)),
        Resolved(Timestamp.ofMillis(20), now + 20.millis),
        Remove
      )

      Seq(add, remove) foreach { write =>
        val key = write.rowKey.nioBuffer()

        val mut =
          CassandraMutation(keyspaceName, key, index1CF = true, index2CF = false)

        write.mutateAt(mut, write.writeTS.transactionTS)
        keyspace.apply(mut.cmut, false /* writeCommitLog */ )
      }

      store.forceBlockingFlush()

      // Assert that both events exist at +20 msecs.
      val before = allEvents(store, now + 20.millis)
      before.size should equal(2)

      before.head.tuple.scopeID should equal(ScopeID(0))
      before.head.tuple.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.head.ts.validTS should equal(Timestamp.ofMillis(20))
      before.head.action should equal(Remove)

      before.last.tuple.scopeID should equal(ScopeID(0))
      before.last.tuple.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.last.ts.validTS should equal(Timestamp.ofMillis(10))
      before.last.action should equal(Add)

      val (rows, sstables) = iterator(store)

      while (rows.hasNext) {
        val row = rows.next()

        // Filter without gcBefore for this collection.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val stats = CollectionFilter.Stats.newBuilder
        val filter = new HistoricalIndexFilter(row, controller, Map.empty, stats)
        val key = Unpooled.wrappedBuffer(filter.getKey.getKey)

        // The events are included.
        val events = filter.asScala.toSeq map { event(key, _) }
        events.size should be(2)
        events.head.ts.validTS should equal(Timestamp.ofMillis(20))
        events.last.ts.validTS should equal(Timestamp.ofMillis(10))

        filter.cellCount should be(2)
        filter.totalCellCount should be(2)
        stats.build().filtered should be(0)
      }
    }
  }

  test("single add") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.HistoricalIndex.CFName)

      store.clearUnsafe()

      // Insert an "add" with a validTS at +10 msecs.
      val write = SetAdd(
        ScopeID(0),
        IndexID(32768),
        Vector.empty, // terms
        Vector.empty, // values
        DocID(SubID(1), CollectionID(1024)),
        Resolved(Timestamp.ofMillis(10), now + 10.millis),
        Add
      )

      val key = write.rowKey.nioBuffer()

      val mut =
        CassandraMutation(keyspaceName, key, index1CF = true, index2CF = false)

      write.mutateAt(mut, now + 10.millis)
      keyspace.apply(mut.cmut, false /* writeCommitLog */ )

      store.forceBlockingFlush()

      // Assert that we find the event at +20 msecs.
      val before = allEvents(store, now + 20.millis)
      before.size should equal(1)
      before.head.tuple.scopeID should equal(ScopeID(0))
      before.head.tuple.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.head.ts.validTS should equal(Timestamp.ofMillis(10))
      before.head.action should equal(Add)

      val (rows, sstables) = iterator(store)

      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +20 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val stats = CollectionFilter.Stats.newBuilder
        val filter = new HistoricalIndexFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(20)),
          stats)

        // The event is gone.
        filter.asScala.size should be(0)
        filter.cellCount should be(0)
        filter.totalCellCount should be(1)
        stats.build().filtered should be(1)
      }
    }
  }

  test("single remove") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.HistoricalIndex.CFName)

      store.clearUnsafe()

      // Insert a "remove" with a validTS at +10 msecs.
      val write = SetAdd(
        ScopeID(0),
        IndexID(32768),
        Vector.empty, // terms
        Vector.empty, // values
        DocID(SubID(1), CollectionID(1024)),
        Resolved(Timestamp.ofMillis(10), now + 10.millis),
        Remove
      )

      val key = write.rowKey.nioBuffer()

      val mut =
        CassandraMutation(keyspaceName, key, index1CF = true, index2CF = false)

      write.mutateAt(mut, now + 10.millis)
      keyspace.apply(mut.cmut, false /* writeCommitLog */ )

      store.forceBlockingFlush()

      // Assert that we find the event at +20 msecs.
      val before = allEvents(store, now + 20.millis)
      before.size should equal(1)
      before.head.tuple.scopeID should equal(ScopeID(0))
      before.head.tuple.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.head.ts.validTS should equal(Timestamp.ofMillis(10))
      before.head.action should equal(Remove)

      val (rows, sstables) = iterator(store)

      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +20 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val stats = CollectionFilter.Stats.newBuilder
        val filter = new HistoricalIndexFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(20)),
          stats)

        // The event is gone.
        filter.asScala.size should be(0)
        filter.cellCount should be(0)
        filter.totalCellCount should be(1)
        stats.build().filtered should be(1)
      }
    }
  }

  test("preserves cells when tombstones purge is paused") {
    withStorageEngine(StatsRecorder.Null) { engine =>
      val now = Clock.time
      val keyspace = engine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.HistoricalIndex.CFName)
      store.clearUnsafe()

      // Insert an "add" with a validTS at +10 msecs.
      val write = SetAdd(
        ScopeID(0),
        IndexID(32768),
        Vector.empty, // terms
        Vector.empty, // values
        DocID(SubID(1), CollectionID(1024)),
        Resolved(Timestamp.ofMillis(10), now + 10.millis),
        Add
      )

      val key = write.rowKey.nioBuffer()
      val mut =
        CassandraMutation(
          keyspaceName,
          key,
          index1CF = true,
          index2CF = false
        )

      write.mutateAt(mut, now + 10.millis)
      keyspace.apply(mut.cmut, false /* writeCommitLog */ )
      store.forceBlockingFlush()

      // Assert that we find the event at +20 msecs.
      val before = allEvents(store, now + 20.millis)
      before should have size 1
      before.head.tuple.scopeID should equal(ScopeID(0))
      before.head.tuple.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.head.ts.validTS should equal(Timestamp.ofMillis(10))
      before.head.action should equal(Add)

      val (_, sstables) = iterator(store)
      store.getDataTracker.markCompacting(sstables) should be(true)

      val candidate =
        new LeveledManifest.CompactionCandidate(
          sstables,
          /* level */ 1,
          store.getCompactionStrategy.getMaxSSTableBytes
        )

      val task =
        new CollectionTask(
          store,
          candidate,
          Int.MaxValue, // gcBefore
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(30)),
          CollectionStrategy.filterBuilder(Tables.HistoricalIndex.CFName)
        )

      CompactionController.pauseTombstonePurge()
      try {
        noException shouldBe thrownBy { task.run() }
      } finally {
        store.getDataTracker.unmarkCompacting(sstables)
        CompactionController.unpauseTombstonePurge()
      }

      // Assert the row remains the same
      val after = allEvents(store, now + 40.millis)
      after should have size 1
    }
  }
}
