package fauna.storage.api.test

import fauna.atoms._
import fauna.lang.clocks.Clock
import fauna.lang.Timestamp
import fauna.stats.StatsRecorder
import fauna.storage._
import fauna.storage.api.version.StorageVersion
import fauna.storage.cassandra._
import fauna.storage.cassandra.comparators._
import fauna.storage.doc._
import fauna.storage.ops._
import io.netty.buffer.{ ByteBuf, Unpooled }
import java.util.{ ArrayList, HashSet, Set => JSet }
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

class VersionsFilterSpec extends Spec(keyspaceName = "versionsfilter") {
  override def beforeAll() = {
    super.beforeAll()
    val storage = Storage(
      keyspaceName,
      Seq(Tables.Versions.Schema.copy(compaction = Compaction.Collection)))

    storage.init(overrideIDs = false)
  }

  override def afterEach() = {
    super.afterEach()

    val ks = Keyspace.open(keyspaceName)
    val cfs = ks.getColumnFamilyStore(Tables.Versions.CFName)
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
      CValue(Tables.Versions.Schema.keyComparator, bytes)

    def decodePredicate(key: CValue, name: ByteBuf) = {
      val prefixComponents =
        Tables.Versions.Schema.nameComparator.bytesToCValues(name)
      Predicate(key, prefixComponents)
    }

    val pred = decodePredicate(decodeKey(key), cell.name)
    new Value[Tables.Versions.Key](pred, cell.value, transactionTS = cell.ts)
  }

  def rowsAndCounter(cfs: ColumnFamilyStore, snapshotTS: Timestamp) = {
    val counter = new IteratorStatsCounter()
    val rows =
      new CassandraIterator(
        counter,
        cfs,
        ScanBounds(Segment.All),
        Selector.All,
        snapshotTS).toSeq
    (rows, counter)
  }

  // Gathers all versions as of the provided snapshot time using
  // CassandraIterator as a check against the compaction strategy's
  // scanners.
  def allVersions(cfs: ColumnFamilyStore, snapshotTS: Timestamp) = {
    rowsAndCounter(cfs, snapshotTS)._1 map { case (k, c) =>
      // TODO: Patch up?
      TestVersion.fromVersion(StorageVersion.fromValue(Tables.decodeValue[Tables.Versions.Key](Tables.Versions.Schema, k, c)))
    }
  }

  def rowCountFromCounter(cfs: ColumnFamilyStore, snapshotTS: Timestamp) = {
    rowsAndCounter(cfs, snapshotTS)._2.rows.sum()
  }

  def version(key: ByteBuf, atom: OnDiskAtom): TestVersion = {
    def cell(atom: OnDiskAtom): Cell = {
      val cc = atom.asInstanceOf[CCell] // NB. range tombstones should not exist.
      val name = Unpooled.wrappedBuffer(cc.name.toByteBuffer)
      val value = Unpooled.wrappedBuffer(cc.value)
      val cts = Timestamp.ofMicros(cc.timestamp)

      Cell(name, value, cts)
    }

    TestVersion.fromVersion(StorageVersion.fromValue(Tables.decodeValue[Tables.Versions.Key](Tables.Versions.Schema, key, cell(atom))))
  }

  def predicate[T: CassandraDecoder](
    rowKey: ByteBuf,
    cell: Cell,
    schema: ColumnFamilySchema) = {
    val prefix = schema.nameComparator.bytesToCValues(cell.name)
    val key = CValue(schema.keyComparator, rowKey)
    Predicate(key, prefix).as[T]
  }

  test("missing gcBefore remains") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.Versions.CFName)

      store.clearUnsafe()

      // Insert a version with a validTS at +10 msecs.
      val write = VersionAdd(
        ScopeID(0),
        DocID(SubID(1), CollectionID(1024)),
        Resolved(Timestamp.ofMillis(10), now + 10.millis),
        Create,
        SchemaVersion.Min,
        Data.empty,
        None)

      val key = write.rowKey.nioBuffer()

      val mut =
        CassandraMutation(keyspaceName, key, index1CF = false, index2CF = false)

      write.mutateAt(mut, now + 10.millis)
      keyspace.apply(mut.cmut, false /* writeCommitLog */ )

      store.forceBlockingFlush()

      // Assert that we find the version at +20 msecs.
      val before = allVersions(store, now + 20.millis)
      before.size should equal(1)
      before.head.scopeID should equal(ScopeID(0))
      before.head.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.head.validTS.get should equal(Timestamp.ofMillis(10))
      before.head.action should equal(Create)

      val (rows, sstables) = iterator(store)

      while (rows.hasNext) {
        val row = rows.next()

        // Filter without gcBefore for this collection.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val stats = CollectionFilter.Stats.newBuilder
        val filter = new VersionsFilter(row, controller, Map.empty, stats)
        val key = Unpooled.wrappedBuffer(filter.getKey.getKey)

        // The version is included.
        val versions = filter.asScala.toSeq map { version(key, _) }
        versions.size should be(1)
        versions.head.validTS.get should equal(Timestamp.ofMillis(10))

        filter.cellCount should be(1)
        filter.totalCellCount should be(1)
        stats.build().filtered should be(0)
      }
    }
  }

  test("single create remains") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.Versions.CFName)

      store.clearUnsafe()

      // Insert a version with a validTS at +10 msecs.
      val write = VersionAdd(
        ScopeID(0),
        DocID(SubID(1), CollectionID(1024)),
        Resolved(Timestamp.ofMillis(10), now + 10.millis),
        Create,
        SchemaVersion.Min,
        Data.empty,
        None)

      val key = write.rowKey.nioBuffer()

      val mut =
        CassandraMutation(keyspaceName, key, index1CF = false, index2CF = false)

      write.mutateAt(mut, now + 10.millis)
      keyspace.apply(mut.cmut, false /* writeCommitLog */ )

      store.forceBlockingFlush()

      // Assert that we find the version at +20 msecs.
      val before = allVersions(store, now + 20.millis)
      before.size should equal(1)
      before.head.scopeID should equal(ScopeID(0))
      before.head.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.head.validTS.get should equal(Timestamp.ofMillis(10))
      before.head.action should equal(Create)

      val (rows, sstables) = iterator(store)

      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +20 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val stats = CollectionFilter.Stats.newBuilder
        val filter = new VersionsFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(20)),
          stats)
        val key = Unpooled.wrappedBuffer(filter.getKey.getKey)

        // The version is included.
        val versions = filter.asScala.toSeq map { version(key, _) }
        versions.size should be(1)
        versions.head.validTS.get should equal(Timestamp.ofMillis(10))

        filter.cellCount should be(1)
        filter.totalCellCount should be(1)
        stats.build().filtered should be(0)
      }
    }
  }

  test("TTL'd create") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.Versions.CFName)

      store.clearUnsafe()

      // Insert a version with a validTS at +10 msecs. and TTL at +20
      // msecs.
      val write = VersionAdd(
        ScopeID(0),
        DocID(SubID(1), CollectionID(1024)),
        Resolved(Timestamp.ofMillis(10), now + 10.millis),
        Create,
        SchemaVersion.Min,
        Data(Field[Option[Timestamp]]("ttl") -> Some(Timestamp.ofMillis(20))),
        None
      )

      val key = write.rowKey.nioBuffer()

      val mut =
        CassandraMutation(keyspaceName, key, index1CF = false, index2CF = false)

      write.mutateAt(mut, now + 10.millis)
      keyspace.apply(mut.cmut, false /* writeCommitLog */ )

      store.forceBlockingFlush()

      // Assert that we find the version at +20 msecs.
      val before = allVersions(store, now + 20.millis)
      before.size should equal(1)
      before.head.scopeID should equal(ScopeID(0))
      before.head.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.head.validTS.get should equal(Timestamp.ofMillis(10))
      before.head.action should equal(Create)

      var (rows, sstables) = iterator(store)

      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +20 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val stats = CollectionFilter.Stats.newBuilder
        val filter = new VersionsFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(20)),
          stats)
        val key = Unpooled.wrappedBuffer(filter.getKey.getKey)

        // The version is included.
        val versions = filter.asScala.toSeq map { version(key, _) }
        versions.size should be(1)
        versions.head.validTS.get should equal(Timestamp.ofMillis(10))

        filter.cellCount should be(1)
        filter.totalCellCount should be(1)
        stats.build().filtered should be(0)
      }

      val iter = iterator(store)
      rows = iter._1
      sstables = iter._2

      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +21 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val stats = CollectionFilter.Stats.newBuilder
        val filter = new VersionsFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(21)),
          stats)
        val key = Unpooled.wrappedBuffer(filter.getKey.getKey)

        // The version is excluded.
        val versions = filter.asScala.toSeq map { version(key, _) }
        versions.size should be(0)

        filter.cellCount should be(0)
        filter.totalCellCount should be(1)
        stats.build().filtered should be(1)
      }
    }
  }

  test("single delete remains") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.Versions.CFName)

      store.clearUnsafe()

      // Insert a version with a validTS at +10 msecs.
      val write = VersionAdd(
        ScopeID(0),
        DocID(SubID(1), CollectionID(1024)),
        Resolved(Timestamp.ofMillis(10), now + 10.millis),
        Delete,
        SchemaVersion.Min,
        Data.empty,
        None)

      val key = write.rowKey.nioBuffer()

      val mut =
        CassandraMutation(keyspaceName, key, index1CF = false, index2CF = false)

      write.mutateAt(mut, now + 10.millis)
      keyspace.apply(mut.cmut, false /* writeCommitLog */ )

      store.forceBlockingFlush()

      // Assert that we find the version at +20 msecs.
      val before = allVersions(store, now + 20.millis)
      before.size should equal(1)
      before.head.scopeID should equal(ScopeID(0))
      before.head.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.head.validTS.get should equal(Timestamp.ofMillis(10))
      before.head.action should equal(Delete)

      val (rows, sstables) = iterator(store)

      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +20 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val stats = CollectionFilter.Stats.newBuilder
        val filter = new VersionsFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(20)),
          stats)
        val key = Unpooled.wrappedBuffer(filter.getKey.getKey)

        // The version is excluded.
        val versions = filter.asScala.toSeq map { version(key, _) }
        versions.size should be(0)

        filter.cellCount should be(0)
        filter.totalCellCount should be(1)
        stats.build().filtered should be(1)
      }
    }
  }

  test("create/delete") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.Versions.CFName)

      store.clearUnsafe()

      // Insert a create with a validTS at +10 msecs, and a delete at
      // +20 msecs.
      Seq(Create -> 10, Delete -> 20) foreach { case (action, i) =>
        val write = VersionAdd(
          ScopeID(0),
          DocID(SubID(1), CollectionID(1024)),
          Resolved(Timestamp.ofMillis(i), now + i.millis),
          action,
          SchemaVersion.Min,
          Data.empty,
          None)

        val key = write.rowKey.nioBuffer()

        val mut =
          CassandraMutation(keyspaceName, key, index1CF = false, index2CF = false)

        write.mutateAt(mut, now + i.millis)
        keyspace.apply(mut.cmut, false /* writeCommitLog */ )
      }

      store.forceBlockingFlush()

      // Assert that we find both versions
      val before = allVersions(store, now + 20.millis)
      before.size should equal(2)

      before.head.scopeID should equal(ScopeID(0))
      before.head.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.head.validTS.get should equal(Timestamp.ofMillis(20))
      before.head.action should equal(Delete)

      before.last.scopeID should equal(ScopeID(0))
      before.last.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.last.validTS.get should equal(Timestamp.ofMillis(10))
      before.last.action should equal(Create)

      var (rows, sstables) = iterator(store)

      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +15 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val stats = CollectionFilter.Stats.newBuilder
        val early = new VersionsFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(15)),
          stats)

        // Both versions are included.
        early.asScala.toSeq.size should be(2)
        early.cellCount should be(2)
        early.totalCellCount should be(2)
        stats.build().filtered should be(0)
      }

      val iter = iterator(store)
      rows = iter._1
      sstables = iter._2

      while (rows.hasNext) {
        val row = rows.next()
        // Filter with gcBefore set to +30 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val stats = CollectionFilter.Stats.newBuilder
        val late = new VersionsFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(30)),
          stats)
        val key = Unpooled.wrappedBuffer(late.getKey.getKey)

        // The root is dropped.
        val versions = late.asScala.toSeq map { version(key, _) }
        versions.size should be(0)

        late.cellCount should be(0)
        late.totalCellCount should be(2)
        stats.build().filtered should be(2)
      }
    }
  }

  test("delete/delete") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.Versions.CFName)

      store.clearUnsafe()

      // Insert a delete with a validTS at +10 msecs, and a delete at
      // +20 msecs.
      Seq(Delete -> 10, Delete -> 20) foreach { case (action, i) =>
        val write = VersionAdd(
          ScopeID(0),
          DocID(SubID(1), CollectionID(1024)),
          Resolved(Timestamp.ofMillis(i), now + i.millis),
          action,
          SchemaVersion.Min,
          Data.empty,
          None)

        val key = write.rowKey.nioBuffer()

        val mut =
          CassandraMutation(keyspaceName, key, index1CF = false, index2CF = false)

        write.mutateAt(mut, now + i.millis)
        keyspace.apply(mut.cmut, false /* writeCommitLog */ )
      }

      store.forceBlockingFlush()

      // Assert that we find both versions
      val before = allVersions(store, now + 20.millis)
      before.size should equal(2)

      before.head.scopeID should equal(ScopeID(0))
      before.head.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.head.validTS.get should equal(Timestamp.ofMillis(20))
      before.head.action should equal(Delete)

      before.last.scopeID should equal(ScopeID(0))
      before.last.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.last.validTS.get should equal(Timestamp.ofMillis(10))
      before.last.action should equal(Delete)

      var (rows, sstables) = iterator(store)

      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +15 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val stats = CollectionFilter.Stats.newBuilder
        val early = new VersionsFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(15)),
          stats)

        // Drop the delete at +10 msecs.
        early.asScala.toSeq.size should be(1)
        early.cellCount should be(1)
        early.totalCellCount should be(2)
        stats.build().filtered should be(1)
      }

      val iter = iterator(store)
      rows = iter._1
      sstables = iter._2

      while (rows.hasNext) {
        val row = rows.next()
        // Filter with gcBefore set to +30 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val stats = CollectionFilter.Stats.newBuilder
        val late = new VersionsFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(30)),
          stats)
        val key = Unpooled.wrappedBuffer(late.getKey.getKey)

        // The root is dropped.
        val versions = late.asScala.toSeq map { version(key, _) }
        versions.size should be(0)

        late.cellCount should be(0)
        late.totalCellCount should be(2)
        stats.build().filtered should be(2)
      }
    }
  }

  test("delete/create") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.Versions.CFName)

      store.clearUnsafe()

      // Insert a delete with a validTS at +10 msecs, and a create at
      // +20 msecs.
      Seq(Delete -> 10, Create -> 20) foreach { case (action, i) =>
        val write = VersionAdd(
          ScopeID(0),
          DocID(SubID(1), CollectionID(1024)),
          Resolved(Timestamp.ofMillis(i), now + i.millis),
          action,
          SchemaVersion.Min,
          Data.empty,
          None)

        val key = write.rowKey.nioBuffer()

        val mut =
          CassandraMutation(keyspaceName, key, index1CF = false, index2CF = false)

        write.mutateAt(mut, now + i.millis)
        keyspace.apply(mut.cmut, false /* writeCommitLog */ )
      }

      store.forceBlockingFlush()

      // Assert that we find both versions
      val before = allVersions(store, now + 20.millis)
      before.size should equal(2)

      before.head.scopeID should equal(ScopeID(0))
      before.head.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.head.validTS.get should equal(Timestamp.ofMillis(20))
      before.head.action should equal(Create)

      before.last.scopeID should equal(ScopeID(0))
      before.last.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.last.validTS.get should equal(Timestamp.ofMillis(10))
      before.last.action should equal(Delete)

      var (rows, sstables) = iterator(store)

      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +15 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val stats = CollectionFilter.Stats.newBuilder
        val early = new VersionsFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(15)),
          stats)

        // Drop the delete at +10 msecs.
        early.asScala.toSeq.size should be(1)
        early.cellCount should be(1)
        early.totalCellCount should be(2)
        stats.build().filtered should be(1)
      }

      val iter = iterator(store)
      rows = iter._1
      sstables = iter._2

      while (rows.hasNext) {
        val row = rows.next()
        // Filter with gcBefore set to +30 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val stats = CollectionFilter.Stats.newBuilder
        val late = new VersionsFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(30)),
          stats)
        val key = Unpooled.wrappedBuffer(late.getKey.getKey)

        // The root remains.
        val versions = late.asScala.toSeq map { version(key, _) }
        versions.size should be(1)

        late.cellCount should be(1)
        late.totalCellCount should be(2)
        stats.build().filtered should be(1)
      }
    }
  }

  test("create/row tombstone") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.Versions.CFName)

      store.clearUnsafe()

      // Insert a create then flush it in its own sstable.
      val write = VersionAdd(
        ScopeID(0),
        DocID(SubID(1), CollectionID(1024)),
        Resolved(Timestamp.ofMillis(10), now + 10.millis),
        Create,
        SchemaVersion.Min,
        Data.empty,
        None)

      val key = write.rowKey.nioBuffer()

      val mut =
        CassandraMutation(keyspaceName, key, index1CF = false, index2CF = false)

      write.mutateAt(mut, now + 10.millis)
      keyspace.apply(mut.cmut, false)

      store.forceBlockingFlush()

      // Move it to L1.
      {
        val sstables = store.getSSTables()

        val level = 1
        val maxSSTableBytes = store.getCompactionStrategy.getMaxSSTableBytes

        store.getDataTracker.markCompacting(sstables) should be(true)

        val candidate =
          new LeveledManifest.CompactionCandidate(sstables, level, maxSSTableBytes)

        val task = new CollectionTask(
          store,
          candidate,
          Int.MaxValue, // gcBefore
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(20)),
          VersionsFilter.apply)

        noException shouldBe thrownBy {
          task.run()
        }
        store.getDataTracker.unmarkCompacting(sstables)
      }

      // Insert a row tombstone for that document then flush it in its own sstable.
      val delete = DocRemove(ScopeID(0), DocID(SubID(1), CollectionID(1024)))

      val deleteKey = delete.rowKey.nioBuffer()
      val deleteMut =
        CassandraMutation(
          keyspaceName,
          deleteKey,
          index1CF = false,
          index2CF = false)

      delete.mutateAt(deleteMut, now + 15.millis)
      keyspace.apply(deleteMut.cmut, false)

      store.forceBlockingFlush()

      // Test that we can still find the create.
      rowCountFromCounter(store, now + 15.millis) should equal(1)

      // Compact both sstables together, which should eliminate the row.
      {
        val sstables = store.getSSTables()

        val level = 1
        val maxSSTableBytes = store.getCompactionStrategy.getMaxSSTableBytes

        store.getDataTracker.markCompacting(sstables) should be(true)

        val candidate =
          new LeveledManifest.CompactionCandidate(sstables, level, maxSSTableBytes)

        val task = new CollectionTask(
          store,
          candidate,
          Int.MaxValue, // gcBefore
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(20)),
          VersionsFilter.apply)

        noException shouldBe thrownBy {
          task.run()
        }
        store.getDataTracker.unmarkCompacting(sstables)
      }

      rowCountFromCounter(store, now + 15.millis) should equal(0)
    }

  }

  test("create/create") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.Versions.CFName)

      store.clearUnsafe()

      // Insert a create with a validTS at +10 msecs, and a create at
      // +20 msecs.
      Seq(Create -> 10, Create -> 20) foreach { case (action, i) =>
        val write = VersionAdd(
          ScopeID(0),
          DocID(SubID(1), CollectionID(1024)),
          Resolved(Timestamp.ofMillis(i), now + i.millis),
          action,
          SchemaVersion.Min,
          Data.empty,
          None)

        val key = write.rowKey.nioBuffer()

        val mut =
          CassandraMutation(keyspaceName, key, index1CF = false, index2CF = false)

        write.mutateAt(mut, now + i.millis)
        keyspace.apply(mut.cmut, false /* writeCommitLog */ )
      }

      store.forceBlockingFlush()

      // Assert that we find both versions
      val before = allVersions(store, now + 20.millis)
      before.size should equal(2)

      before.head.scopeID should equal(ScopeID(0))
      before.head.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.head.validTS.get should equal(Timestamp.ofMillis(20))
      before.head.action should equal(Create)

      before.last.scopeID should equal(ScopeID(0))
      before.last.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.last.validTS.get should equal(Timestamp.ofMillis(10))
      before.last.action should equal(Create)

      var (rows, sstables) = iterator(store)

      while (rows.hasNext) {
        val row = rows.next()

        // Filter with gcBefore set to +15 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val stats = CollectionFilter.Stats.newBuilder
        val early = new VersionsFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(15)),
          stats)

        // Both versions are included.
        early.asScala.toSeq.size should be(2)
        early.cellCount should be(2)
        early.totalCellCount should be(2)
        stats.build().filtered should be(0)
      }

      val iter = iterator(store)
      rows = iter._1
      sstables = iter._2

      while (rows.hasNext) {
        val row = rows.next()
        // Filter with gcBefore set to +30 msecs.
        val controller = new CompactionController(store, sstables, Int.MaxValue)
        val stats = CollectionFilter.Stats.newBuilder
        val late = new VersionsFilter(
          row,
          controller,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(30)),
          stats)
        val key = Unpooled.wrappedBuffer(late.getKey.getKey)

        // The root remains.
        val versions = late.asScala.toSeq map { version(key, _) }
        versions.size should be(1)
        versions.head.validTS.get should equal(Timestamp.ofMillis(20))

        late.cellCount should be(1)
        late.totalCellCount should be(2)
        stats.build().filtered should be(1)
      }
    }
  }

  test("filters Versions with overlapping add/remove") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.Versions.CFName)

      store.clearUnsafe()

      {
        // Insert a version with a validTS at +10 msecs.
        val create = VersionAdd(
          ScopeID(0),
          DocID(SubID(1), CollectionID(1024)),
          Resolved(Timestamp.ofMillis(10), now + 10.millis),
          Create,
          SchemaVersion.Min,
          Data.empty,
          None
        )

        val key = create.rowKey.nioBuffer()

        val mut =
          CassandraMutation(keyspaceName, key, index1CF = false, index2CF = false)

        // NB. the "create" cell timestamp must be greater than the
        // "delete" timestamp.
        create.mutateAt(mut, now + 10.millis)
        keyspace.apply(mut.cmut, false /* writeCommitLog */ )

        store.forceBlockingFlush()
      }

      {
        val sstables = store.getSSTables()

        // Presuming the flush landed in L0, let's move up to L3 to
        // leave room for a L1 promotion with a gap in the levels, and
        // an L2 promotion without one.
        val level = 3
        val maxSSTableBytes = store.getCompactionStrategy.getMaxSSTableBytes

        store.getDataTracker.markCompacting(sstables) should be(true)

        val candidate =
          new LeveledManifest.CompactionCandidate(sstables, level, maxSSTableBytes)

        val task = new CollectionTask(
          store,
          candidate,
          Int.MaxValue, // gcBefore
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(20)),
          VersionsFilter.apply)

        noException shouldBe thrownBy {
          task.run()
        }
        store.getDataTracker.unmarkCompacting(sstables)
      }

      val levelThree = rowsAndCounter(store, now + 20.millis)._1
      levelThree.size should equal(1)

      val (k, c) = levelThree.head
      val (_, validTS, _, _) =
        predicate[Tables.Versions.Key](k, c, Tables.Versions.Schema)

      validTS should be(Timestamp.ofMillis(10))

      val fresh = store.getSSTables().toArray(Array[SSTableReader]())

      fresh.size should be(1)
      fresh(0).getSSTableMetadata.sstableLevel should be(3)

      {
        // Insert a version with a validTS at +50 msecs.
        val delete = VersionAdd(
          ScopeID(0),
          DocID(SubID(1), CollectionID(1024)),
          Resolved(Timestamp.ofMillis(50), now + 50.millis),
          Delete,
          SchemaVersion.Min,
          Data.empty,
          None
        )

        val key = delete.rowKey.nioBuffer()

        val mut =
          CassandraMutation(keyspaceName, key, index1CF = false, index2CF = false)

        delete.mutateAt(mut, now + 50.millis)
        keyspace.apply(mut.cmut, false /* writeCommitLog */ )

        store.forceBlockingFlush()
      }

      // Promote the "delete" from L0 -> L1, then L1 -> L2.
      Seq(1, 2) foreach { level =>
        val wrap = store.getCompactionStrategy.asInstanceOf[WrappingCompactionStrategy]
        val strategy =
          wrap.getWrappedStrategies.get(1).asInstanceOf[CollectionStrategy]

        val sstables =
          new ArrayList[SSTableReader](strategy.manifest.getLevel(level - 1))

        val maxSSTableBytes = store.getCompactionStrategy.getMaxSSTableBytes

        store.getDataTracker.markCompacting(sstables) should be(true)

        val candidate =
          new LeveledManifest.CompactionCandidate(sstables, level, maxSSTableBytes)

        val task = new CollectionTask(
          store,
          candidate,
          Int.MaxValue, // gcBefore
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(50)),
          VersionsFilter.apply)

        noException shouldBe thrownBy {
          task.run()
        }

        store.getDataTracker.unmarkCompacting(sstables)

        val promoted = store.getSSTables().toArray(Array[SSTableReader]())

        // The "delete" is retained due to the overlap with L3.
        promoted.size should be(2)

      }

      {
        val sstables = store.getSSTables()

        // Promote L2 -> L3, including the overlap already in L3.
        val level = 3
        val maxSSTableBytes = store.getCompactionStrategy.getMaxSSTableBytes

        store.getDataTracker.markCompacting(sstables) should be(true)

        val candidate =
          new LeveledManifest.CompactionCandidate(sstables, level, maxSSTableBytes)

        val task = new CollectionTask(
          store,
          candidate,
          Int.MaxValue, // gcBefore
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(100)),
          VersionsFilter.apply)

        noException shouldBe thrownBy {
          task.run()
        }

        store.getDataTracker.unmarkCompacting(sstables)
      }

      val merged = store.getSSTables().toArray(Array[SSTableReader]())

      // All gone.
      merged.size should be(0)
    }
  }

  test("filters Versions with overlapping remove/add") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.Versions.CFName)

      store.clearUnsafe()

      {
        // Insert a version with a validTS at +50 msecs.
        val delete = VersionAdd(
          ScopeID(0),
          DocID(SubID(1), CollectionID(1024)),
          Resolved(Timestamp.ofMillis(50), now + 50.millis),
          Delete,
          SchemaVersion.Min,
          Data.empty,
          None
        )

        val key = delete.rowKey.nioBuffer()

        val mut =
          CassandraMutation(keyspaceName, key, index1CF = false, index2CF = false)

        delete.mutateAt(mut, now + 50.millis)
        keyspace.apply(mut.cmut, false /* writeCommitLog */ )

        store.forceBlockingFlush()
      }

      {
        val sstables = store.getSSTables()

        // Presuming the flush landed in L0, let's move up to L3 to
        // leave room for a L1 promotion with a gap in the levels, and
        // an L2 promotion without one.
        val level = 3
        val maxSSTableBytes = store.getCompactionStrategy.getMaxSSTableBytes

        store.getDataTracker.markCompacting(sstables) should be(true)

        val candidate =
          new LeveledManifest.CompactionCandidate(sstables, level, maxSSTableBytes)

        val task = new CollectionTask(
          store,
          candidate,
          Int.MaxValue, // gcBefore
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(20)),
          VersionsFilter.apply)

        noException shouldBe thrownBy {
          task.run()
        }

        store.getDataTracker.unmarkCompacting(sstables)
      }

      val levelThree = rowsAndCounter(store, now + 50.millis)._1

      val (k, c) = levelThree.head
      val (_, validTS, _, _) =
        predicate[Tables.Versions.Key](k, c, Tables.Versions.Schema)

      validTS should be(Timestamp.ofMillis(50))

      val fresh = store.getSSTables().toArray(Array[SSTableReader]())

      fresh.size should be(1)
      fresh(0).getSSTableMetadata.sstableLevel should be(3)

      {
        // Insert a version with a validTS at +10 msecs.
        val create = VersionAdd(
          ScopeID(0),
          DocID(SubID(1), CollectionID(1024)),
          Resolved(Timestamp.ofMillis(10), now + 10.millis),
          Create,
          SchemaVersion.Min,
          Data.empty,
          None
        )

        val key = create.rowKey.nioBuffer()

        val mut =
          CassandraMutation(keyspaceName, key, index1CF = false, index2CF = false)

        // NB. the "create" cell timestamp must be greater than the
        // "delete" timestamp.
        create.mutateAt(mut, now + 10.millis)
        keyspace.apply(mut.cmut, false /* writeCommitLog */ )

        store.forceBlockingFlush()
      }

      // Promote the "create" from L0 -> L1, then L1 -> L2.
      Seq(1, 2) foreach { level =>
        val wrap = store.getCompactionStrategy.asInstanceOf[WrappingCompactionStrategy]
        val strategy =
          wrap.getWrappedStrategies.get(1).asInstanceOf[CollectionStrategy]

        val sstables =
          new ArrayList[SSTableReader](strategy.manifest.getLevel(level - 1))

        val maxSSTableBytes = store.getCompactionStrategy.getMaxSSTableBytes

        store.getDataTracker.markCompacting(sstables) should be(true)

        val candidate =
          new LeveledManifest.CompactionCandidate(sstables, level, maxSSTableBytes)

        val task = new CollectionTask(
          store,
          candidate,
          Int.MaxValue, // gcBefore
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(50)),
          VersionsFilter.apply)

        noException shouldBe thrownBy {
          task.run()
        }

        store.getDataTracker.unmarkCompacting(sstables)

        val promoted = store.getSSTables().toArray(Array[SSTableReader]())

        // The "create" is retained in L1 due to the overlap with L2.
        promoted.size should be(2)
      }

      {
        val sstables = store.getSSTables()

        // Promote L2 -> L3, including the overlap already in L3.
        val level = 3
        val maxSSTableBytes = store.getCompactionStrategy.getMaxSSTableBytes

        store.getDataTracker.markCompacting(sstables) should be(true)

        val candidate =
          new LeveledManifest.CompactionCandidate(sstables, level, maxSSTableBytes)

        val task = new CollectionTask(
          store,
          candidate,
          Int.MaxValue, // gcBefore
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(100)),
          VersionsFilter.apply)

        noException shouldBe thrownBy {
          task.run()
        }

        store.getDataTracker.unmarkCompacting(sstables)
      }

      val merged = store.getSSTables().toArray(Array[SSTableReader]())

      // All gone.
      merged.size should be(0)
    }
  }

  test("preserves cells when tombstone purge is paused") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.Versions.CFName)
      store.clearUnsafe()

      // Insert a create with a validTS at +10 msecs, and a delete at +20 msecs.
      Seq(Create -> 10, Delete -> 20) foreach { case (action, i) =>
        val write =
          VersionAdd(
            ScopeID(0),
            DocID(SubID(1), CollectionID(1024)),
            Resolved(Timestamp.ofMillis(i), now + i.millis),
            action,
            SchemaVersion.Min,
            Data.empty,
            None
          )

        val mut =
          CassandraMutation(
            keyspaceName,
            write.rowKey.nioBuffer(),
            index1CF = false,
            index2CF = false
          )

        write.mutateAt(mut, now + i.millis)
        keyspace.apply(mut.cmut, false /* writeCommitLog */ )
      }

      store.forceBlockingFlush()

      // Assert that we find both versions
      val before = allVersions(store, now + 20.millis)
      before should have size 2

      before.head.scopeID should equal(ScopeID(0))
      before.head.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.head.validTS.get should equal(Timestamp.ofMillis(20))
      before.head.action should equal(Delete)

      before.last.scopeID should equal(ScopeID(0))
      before.last.docID should equal(DocID(SubID(1), CollectionID(1024)))
      before.last.validTS.get should equal(Timestamp.ofMillis(10))
      before.last.action should equal(Create)

      var (_, sstables) = iterator(store)
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
          (now + 50.millis).micros.toInt,
          Map((ScopeID(0), CollectionID(1024)) -> Timestamp.ofMillis(30)),
          CollectionStrategy.filterBuilder(Tables.Versions.CFName)
        )

      CompactionController.pauseTombstonePurge()
      try {
        noException shouldBe thrownBy { task.run() }
      } finally {
        store.getDataTracker.unmarkCompacting(sstables)
        CompactionController.unpauseTombstonePurge()
      }

      // Assert row still exists after compaction
      val after = allVersions(store, now + 40.millis)
      after should have size 2
    }
  }
}
