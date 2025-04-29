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

class SortedIndexFilterSpec extends Spec(keyspaceName = "sortedfilter") {
  override def beforeAll() = {
    super.beforeAll()
    val storage = Storage(
      keyspaceName,
      Seq(
        Tables.HistoricalIndex.Schema, // Historical is required for write ops
        Tables.SortedIndex.Schema.copy(compaction = Compaction.Collection))
    )

    storage.init(overrideIDs = false)
  }

  override def afterEach() = {
    super.afterEach()

    val ks = Keyspace.open(keyspaceName)
    val cfs = ks.getColumnFamilyStore(Tables.SortedIndex.CFName)
    cfs.truncateBlocking()
  }

  def iterator(
    cfs: ColumnFamilyStore,
    level: Int = 0): (CloseableIterator[OnDiskAtomIterator], JSet[SSTableReader]) = {
    val wrap = cfs.getCompactionStrategy.asInstanceOf[WrappingCompactionStrategy]
    val strategy =
      wrap.getWrappedStrategies.get(1).asInstanceOf[CollectionStrategy]

    val sstables = strategy.manifest.getLevel(level)
    val scanners = strategy.getScanners(sstables).scanners

    scanners.size should be(1)
    (scanners.get(0), new HashSet(sstables))
  }

  def decodeValue(key: ByteBuf, cell: Cell) = {
    def decodeKey(bytes: ByteBuf): CValue =
      CValue(Tables.SortedIndex.Schema.keyComparator, bytes)

    def decodePredicate(key: CValue, name: ByteBuf) = {
      val prefixComponents =
        Tables.SortedIndex.Schema.nameComparator.bytesToCValues(name)
      Predicate(key, prefixComponents)
    }

    val pred = decodePredicate(decodeKey(key), cell.name)
    new Value[Tables.SortedIndex.Key](pred, cell.value, transactionTS = cell.ts)
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
      IndexValue.fromSortedValue(ScopeID(0), decodeValue(k, c))
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

    IndexValue.fromSortedValue(ScopeID(0), decodeValue(key, cell(atom)))
  }

  test("missing gcBefore remains") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.SortedIndex.CFName)

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
        val filter = SortedIndexFilter(row, controller, Map.empty)
        val key = Unpooled.wrappedBuffer(filter.getKey.getKey)

        // The events are included.
        val events = filter.asScala.toSeq map { event(key, _) }
        events.size should be(2)
        events.head.ts.validTS should equal(Timestamp.ofMillis(20))
        events.last.ts.validTS should equal(Timestamp.ofMillis(10))

        filter.cellCount should be(2)
        filter.totalCellCount should be(2)
      }
    }
  }

  test("single tombstone remains") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.SortedIndex.CFName)

      store.clearUnsafe()

      val write = SetAdd(
        ScopeID(0),
        IndexID(32768),
        Vector.empty, // terms
        Vector.empty, // values
        DocID(SubID(1), CollectionID(1024)),
        Resolved(Timestamp.ofMillis(10), now + 10.millis),
        Add
      )

      // Insert an "add" tombstone with a timestamp < MaxMicros.
      storageEngine.applyMutationForKey(write.rowKey.duplicate) { mut =>
        val cell = write.toPendingCell(store.name).get
        mut.delete(store.name, cell.name, now + 30.days)
      }

      store.forceBlockingFlush()

      val (rows, sstables) = iterator(store)

      while (rows.hasNext) {
        val row = rows.next()

        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val stats = CollectionFilter.Stats.newBuilder
        val filter = new SortedIndexFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(20)),
          stats)
        val key = Unpooled.wrappedBuffer(filter.getKey.getKey)

        // The event is included.
        val events = filter.asScala.toSeq map { event(key, _) }
        events.size should be(1)
        events.head.ts.validTS should equal(Timestamp.ofMillis(10))

        filter.cellCount should be(1)
        filter.totalCellCount should be(1)

        val s = stats.build()
        s.overlaps should be(0)
        s.filtered should be(0)
      }
    }
  }

  test("single add remains") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.SortedIndex.CFName)

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
        val filter = SortedIndexFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(20)))
        val key = Unpooled.wrappedBuffer(filter.getKey.getKey)

        // The event is included.
        val events = filter.asScala.toSeq map { event(key, _) }
        events.size should be(1)
        events.head.ts.validTS should equal(Timestamp.ofMillis(10))

        filter.cellCount should be(1)
        filter.totalCellCount should be(1)
      }
    }
  }

  test("TTL'd add") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.SortedIndex.CFName)

      store.clearUnsafe()

      // Insert an "add" with a validTS at +10 msecs. and TTL at +20 msecs.
      val write = SetAdd(
        ScopeID(0),
        IndexID(32768),
        Vector.empty, // terms
        Vector.empty, // values
        DocID(SubID(1), CollectionID(1024)),
        Resolved(Timestamp.ofMillis(10), now + 10.millis),
        Add,
        Some(Timestamp.ofMillis(20))
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

      var (rows, sstables) = iterator(store)

      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +20 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val filter = SortedIndexFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(20)))
        val key = Unpooled.wrappedBuffer(filter.getKey.getKey)

        // The event is included.
        val events = filter.asScala.toSeq map { event(key, _) }
        events.size should be(1)
        events.head.ts.validTS should equal(Timestamp.ofMillis(10))

        filter.cellCount should be(1)
        filter.totalCellCount should be(1)
      }

      val iter = iterator(store)
      rows = iter._1
      sstables = iter._2

      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +21 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val filter = SortedIndexFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(21)))
        val key = Unpooled.wrappedBuffer(filter.getKey.getKey)

        // The event is excluded.
        val events = filter.asScala.toSeq map { event(key, _) }
        events.size should be(0)

        filter.cellCount should be(0)
        filter.totalCellCount should be(1)
      }
    }
  }

  // This can happen before compaction merges the corresponding "add"
  // contiguous with this "remove". Despite being earlier than MVT,
  // this event must remain.
  test("single remove remains") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.SortedIndex.CFName)

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
        val filter = SortedIndexFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(20)))
        val key = Unpooled.wrappedBuffer(filter.getKey.getKey)

        // The event is excluded.
        val events = filter.asScala.toSeq map { event(key, _) }
        events.size should be(0)

        filter.cellCount should be(0)
        filter.totalCellCount should be(1)
      }
    }
  }

  test("TTL'd remove") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.SortedIndex.CFName)

      store.clearUnsafe()

      // Insert a "remove" with a validTS at +10 msecs. and TTL at +20 msecs.
      val write = SetAdd(
        ScopeID(0),
        IndexID(32768),
        Vector.empty, // terms
        Vector.empty, // values
        DocID(SubID(1), CollectionID(1024)),
        Resolved(Timestamp.ofMillis(10), now + 10.millis),
        Remove,
        Some(Timestamp.ofMillis(20))
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

      var (rows, sstables) = iterator(store)

      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +20 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val filter = SortedIndexFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(20)))

        // The event is excluded.
        filter.asScala.toSeq.size should be(0)
        filter.cellCount should be(0)
        filter.totalCellCount should be(1)
      }
    }
  }

  test("add/remove at the same valid time") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.SortedIndex.CFName)

      store.clearUnsafe()

      // Insert an "add" with a validTS at +10 msecs, and a "remove"
      // for the same values at +10 msecs. "add" expired at +30msecs.
      val add = SetAdd(
        ScopeID(0),
        IndexID(32768),
        Vector.empty, // terms
        Vector.empty, // values
        DocID(SubID(1), CollectionID(1024)),
        Resolved(Timestamp.ofMillis(10), now + 10.millis),
        Add,
        ttl = Some(Timestamp.ofMillis(30))
      )

      val remove = SetAdd(
        ScopeID(0),
        IndexID(32768),
        Vector.empty, // terms
        Vector.empty, // values
        DocID(SubID(1), CollectionID(1024)),
        // NB. for the purposes of conflict resolution, the
        // transaction time doesn't matter.
        // Out of convenience:
        // +12 msec <= gcBefore (+15 msec) <= snapshot time (+20 msec).
        Resolved(Timestamp.ofMillis(10), now + 12.millis),
        Remove,
        ttl = None
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
      before.head.ts.validTS should equal(Timestamp.ofMillis(10))
      before.head.action should equal(Add)

      before.last.tuple.scopeID should equal(ScopeID(0))
      before.last.tuple.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.last.ts.validTS should equal(Timestamp.ofMillis(10))
      before.last.action should equal(Remove)

      // Before TTL
      var (rows, sstables) = iterator(store)
      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +15 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val filter = SortedIndexFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(15)))
        val key = Unpooled.wrappedBuffer(filter.getKey.getKey)

        // Both the "add" and "remove" remains.
        val events = filter.asScala.toSeq map { event(key, _) }
        events.size should be(2)
        events(0).ts.validTS should equal(Timestamp.ofMillis(10))
        events(0).action.isCreate should be(true)
        events(1).ts.validTS should equal(Timestamp.ofMillis(10))
        events(1).action.isCreate should be(false)

        filter.cellCount should be(2)
        filter.totalCellCount should be(2)
      }

      // After TTL
      var (rows0, sstables0) = iterator(store)
      while (rows0.hasNext) {
        val row = rows0.next()

        // Filter with gcBefore set to +30 msecs.
        val controller = new CompactionController(store, sstables0, Int.MaxValue)
        val filter = SortedIndexFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(31)))
        val key = Unpooled.wrappedBuffer(filter.getKey.getKey)

        // None remains
        val events = filter.asScala.toSeq map { event(key, _) }
        events.size should be(0)
        filter.cellCount should be(0)
        filter.totalCellCount should be(2)
      }
    }
  }

  test("add/remove") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.SortedIndex.CFName)

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

      var (rows, sstables) = iterator(store)

      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +15 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val early = SortedIndexFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(15)))

        // The events are included.
        early.asScala.toSeq.size should be(2)
        early.cellCount should be(2)
        early.totalCellCount should be(2)
      }

      val iter = iterator(store)
      rows = iter._1
      sstables = iter._2

      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +30 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val late = SortedIndexFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(30)))

        // The events are gone.
        late.asScala.toSeq.size should be(0)
        late.cellCount should be(0)
        late.totalCellCount should be(2)
      }
    }
  }

  test("preserves cells when tombstone purge is paused") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.SortedIndex.CFName)
      store.clearUnsafe()

      // Insert an "add" with a validTS at +10 msecs, and a "remove" for the same
      // values at +20 msecs.
      val add =
        SetAdd(
          ScopeID(0),
          IndexID(32768),
          Vector.empty, // terms
          Vector.empty, // values
          DocID(SubID(1), CollectionID(1024)),
          Resolved(Timestamp.ofMillis(10), now + 10.millis),
          Add
        )

      val remove =
        SetAdd(
          ScopeID(0),
          IndexID(32768),
          Vector.empty, // terms
          Vector.empty, // values
          DocID(SubID(1), CollectionID(1024)),
          Resolved(Timestamp.ofMillis(20), now + 20.millis),
          Remove
        )

      Seq(add, remove) foreach { write =>
        val mut =
          CassandraMutation(
            keyspaceName,
            write.rowKey.nioBuffer(),
            index1CF = true,
            index2CF = false
          )
        write.mutateAt(mut, write.writeTS.transactionTS)
        keyspace.apply(mut.cmut, false /* writeCommitLog */ )
      }

      store.forceBlockingFlush()

      // Assert that both events exist at +20 msecs.
      val before = allEvents(store, now + 20.millis)
      before should have size 2

      before.head.tuple.scopeID should equal(ScopeID(0))
      before.head.tuple.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.head.ts.validTS should equal(Timestamp.ofMillis(20))
      before.head.action should equal(Remove)

      before.last.tuple.scopeID should equal(ScopeID(0))
      before.last.tuple.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.last.ts.validTS should equal(Timestamp.ofMillis(10))
      before.last.action should equal(Add)

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
          CollectionStrategy.filterBuilder(Tables.SortedIndex.CFName)
        )

      CompactionController.pauseTombstonePurge()
      try {
        noException shouldBe thrownBy { task.run() }
      } finally {
        store.getDataTracker.unmarkCompacting(sstables)
        CompactionController.unpauseTombstonePurge()
      }

      // Row remains the same after compaction
      val after = allEvents(store, now + 40.millis)
      after should have size 2
    }
  }

  test("remove/remove") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.SortedIndex.CFName)

      store.clearUnsafe()

      // Insert a "remove" with a validTS at +10 msecs, and another
      // "remove" for the same values at +20 msecs.
      val remove1 = SetAdd(
        ScopeID(0),
        IndexID(32768),
        Vector.empty, // terms
        Vector.empty, // values
        DocID(SubID(1), CollectionID(1024)),
        Resolved(Timestamp.ofMillis(10), now + 10.millis),
        Remove
      )

      val remove2 = SetAdd(
        ScopeID(0),
        IndexID(32768),
        Vector.empty, // terms
        Vector.empty, // values
        DocID(SubID(1), CollectionID(1024)),
        Resolved(Timestamp.ofMillis(20), now + 20.millis),
        Remove
      )

      Seq(remove1, remove2) foreach { write =>
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
      before.last.action should equal(Remove)

      var (rows, sstables) = iterator(store)

      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +15 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val early = SortedIndexFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(15)))
        val key = Unpooled.wrappedBuffer(early.getKey.getKey)

        // The "remove" below MVT is excluded.
        val events = early.asScala.toSeq map { event(key, _) }
        events.size should be(1)
        events.head.ts.validTS should equal(Timestamp.ofMillis(20))

        early.cellCount should be(1)
        early.totalCellCount should be(2)
      }

      val iter = iterator(store)
      rows = iter._1
      sstables = iter._2

      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +30 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val late = SortedIndexFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(30)))

        // The last "remove" is excluded.
        late.asScala.toSeq.size should be(0)
        late.cellCount should be(0)
        late.totalCellCount should be(2)
      }
    }
  }

  // This can happen if a document is re-added to this set, but
  // compaction has not yet merged the "remove" and its corresponding
  // "add" and these events end up contiguous first.
  test("remove/add") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.SortedIndex.CFName)

      store.clearUnsafe()

      // Insert a "remove" with a validTS at +10 msecs, and an
      // "add" for the same values at +20 msecs.
      val remove = SetAdd(
        ScopeID(0),
        IndexID(32768),
        Vector.empty, // terms
        Vector.empty, // values
        DocID(SubID(1), CollectionID(1024)),
        Resolved(Timestamp.ofMillis(10), now + 10.millis),
        Remove
      )

      val add = SetAdd(
        ScopeID(0),
        IndexID(32768),
        Vector.empty, // terms
        Vector.empty, // values
        DocID(SubID(1), CollectionID(1024)),
        Resolved(Timestamp.ofMillis(20), now + 20.millis),
        Add
      )

      Seq(remove, add) foreach { write =>
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
      before.head.action should equal(Add)

      before.last.tuple.scopeID should equal(ScopeID(0))
      before.last.tuple.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.last.ts.validTS should equal(Timestamp.ofMillis(10))
      before.last.action should equal(Remove)

      var (rows, sstables) = iterator(store)

      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +15 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val early = SortedIndexFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(15)))
        val key = Unpooled.wrappedBuffer(early.getKey.getKey)

        // The "add" remains.
        val events = early.asScala.toSeq map { event(key, _) }
        events.size should be(1)
        events.head.ts.validTS should equal(Timestamp.ofMillis(20))

        early.cellCount should be(1)
        early.totalCellCount should be(2)
      }

      val iter = iterator(store)
      rows = iter._1
      sstables = iter._2

      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +30 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val late = SortedIndexFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(30)))
        val key = Unpooled.wrappedBuffer(late.getKey.getKey)

        // The "add" remains.
        val events = late.asScala.toSeq map { event(key, _) }
        events.size should be(1)
        events.head.ts.validTS should equal(Timestamp.ofMillis(20))

        late.cellCount should be(1)
        late.totalCellCount should be(2)
      }
    }
  }

  test("remove/add tombstone") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.SortedIndex.CFName)

      store.clearUnsafe()

      // Insert a "remove" with a validTS at +20 msecs, and an
      // "add" tombstone for the same values at +10 msecs.
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

      val key = remove.rowKey.nioBuffer()
      val mut =
        CassandraMutation(keyspaceName, key, index1CF = true, index2CF = false)

      remove.mutateAt(mut, remove.writeTS.transactionTS)
      keyspace.apply(mut.cmut, false /* writeCommitLog */ )

      // Tombstone the "add" with a timestamp < MaxMicros.
      storageEngine.applyMutationForKey(add.rowKey.duplicate) { mut =>
        val cell = add.toPendingCell(store.name).get
        mut.delete(store.name, cell.name, now + 30.days)
      }

      store.forceBlockingFlush()

      val (rows, sstables) = iterator(store)

      while (rows.hasNext) {
        val row = rows.next()

        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val stats = CollectionFilter.Stats.newBuilder
        val filter = new SortedIndexFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(30)),
          stats)
        val key = Unpooled.wrappedBuffer(filter.getKey.getKey)

        // Only the tombstone remains.
        val cells = filter.asScala.toSeq
        cells.size should be(1)
        cells.head.asInstanceOf[CCell].isLive should be(false)

        val events = cells map { event(key, _) }
        events.head.ts.validTS should equal(Timestamp.ofMillis(10))

        filter.cellCount should be(1)
        filter.totalCellCount should be(2)

        val s = stats.build()
        s.overlaps should be(0)
        s.filtered should be(1)
      }
    }
  }

  test("add/remove with overlaps") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.SortedIndex.CFName)

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

      {
        val sstables = store.getSSTables()

        // Presuming the flush landed in L0, let's move up to L1 to
        // leave room for a new SSTable in L0.
        val level = 1
        val maxSSTableBytes = store.getCompactionStrategy.getMaxSSTableBytes

        store.getDataTracker.markCompacting(sstables) should be(true)

        val candidate =
          new LeveledManifest.CompactionCandidate(sstables, level, maxSSTableBytes)

        val task = new CollectionTask(
          store,
          candidate,
          Int.MaxValue, // gcBefore
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(15)),
          SortedIndexFilter.apply)

        noException shouldBe thrownBy {
          task.run()
        }
      }

      // Insert a duplicate of the `add` event, and flush to a new
      // SSTable.
      Seq(add) foreach { write =>
        val key = write.rowKey.nioBuffer()

        val mut =
          CassandraMutation(keyspaceName, key, index1CF = true, index2CF = false)

        write.mutateAt(mut, write.writeTS.transactionTS)
        keyspace.apply(mut.cmut, false /* writeCommitLog */ )
      }

      store.forceBlockingFlush()

      var (rows, sstables) = iterator(store, level = 1)

      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +15 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val stats = CollectionFilter.Stats.newBuilder
        val early = new SortedIndexFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(15)),
          stats)

        // The events are included.
        early.asScala.toSeq.size should be(2)
        early.cellCount should be(2)
        early.totalCellCount should be(2)

        val s = stats.build()
        s.overlaps should be(1)
        s.filtered should be(0)
      }

      var iter = iterator(store, level = 1)
      rows = iter._1
      sstables = iter._2

      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +30 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val stats = CollectionFilter.Stats.newBuilder
        val late = new SortedIndexFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(30)),
          stats)

        // The events are included.
        late.asScala.toSeq.size should be(2)
        late.cellCount should be(2)
        late.totalCellCount should be(2)

        val s = stats.build()
        s.overlaps should be(1)
        s.filtered should be(0)
      }

      {
        val sstables = store.getSSTables()

        // Promote the L0 SSTable up to L1, removing the duplicate cell.
        val level = 1
        val maxSSTableBytes = store.getCompactionStrategy.getMaxSSTableBytes

        store.getDataTracker.markCompacting(sstables) should be(true)

        val candidate =
          new LeveledManifest.CompactionCandidate(sstables, level, maxSSTableBytes)

        val task = new CollectionTask(
          store,
          candidate,
          Int.MaxValue, // gcBefore
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(15)),
          SortedIndexFilter.apply)

        noException shouldBe thrownBy {
          task.run()
        }
      }

      iter = iterator(store, level = 1)
      rows = iter._1
      sstables = iter._2

      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +30 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val stats = CollectionFilter.Stats.newBuilder
        val late = new SortedIndexFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(30)),
          stats)

        // The events are excluded.
        late.asScala.toSeq.size should be(0)
        late.cellCount should be(0)
        late.totalCellCount should be(2)

        val s = stats.build()
        s.overlaps should be(0)
        s.filtered should be(2)
      }
    }
  }
}
