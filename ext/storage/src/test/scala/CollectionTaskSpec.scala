package fauna.storage.test

import fauna.atoms._
import fauna.codex.cbor.CBOR
import fauna.lang.clocks.Clock
import fauna.lang.Timestamp
import fauna.stats.StatsRecorder
import fauna.storage._
import fauna.storage.cassandra._
import fauna.storage.cassandra.comparators._
import fauna.storage.doc._
import fauna.storage.index._
import fauna.storage.ir._
import fauna.storage.ops._
import io.netty.buffer.{ ByteBuf, Unpooled }
import java.nio.ByteBuffer
import org.apache.cassandra.db.compaction._
import org.scalatest.concurrent.Eventually
import org.scalatest.Ignore
import scala.collection.immutable.HashMap
import scala.concurrent.duration._

// FIXME: figure out why the segfalts are happening and re-enable this suite.
@Ignore
class CollectionTaskSpec extends Spec with Eventually {

  override def beforeEach() = {
    super.beforeEach()
    val storage = Storage(
      keyspaceName,
      Seq(
        Tables.Versions.Schema,
        Tables.SortedIndex.Schema,
        Tables.HistoricalIndex.Schema))

    storage.init(overrideIDs = false)
  }

  def predicate[T: CassandraDecoder](
    rowKey: ByteBuf,
    cell: Cell,
    schema: ColumnFamilySchema) = {
    val prefix = schema.nameComparator.bytesToCValues(cell.name)
    val key = CValue(schema.keyComparator, rowKey)
    Predicate(key, prefix).as[T]
  }

  test("filters Versions") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val docs = 10
      val versions = 30

      val now = Clock.time
      val keyspace = storageEngine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.Versions.CFName)

      store.clearUnsafe()

      val keys = Set.newBuilder[ByteBuffer]

      for (i <- 0 until docs) {
        for (j <- 0 until versions) {
          val write = VersionAdd(
            ScopeID(0),
            DocID(SubID(i + 1), CollectionID(1024)),
            Resolved(Timestamp.ofMillis(j), now + j.millis),
            Create,
            SchemaVersion.Min,
            Data.empty,
            None)

          val key = write.rowKey.nioBuffer()

          val mut =
            CassandraMutation(keyspaceName, key, index1CF = false, index2CF = false)

          write.mutateAt(mut, now + j.millis)
          keyspace.apply(mut.cmut, false /* writeCommitLog */ )

          keys += key

          if (j % 3 == 0) {
            store.forceBlockingFlush()
          }
        }
      }

      store.forceBlockingFlush()

      val counter = new IteratorStatsCounter()
      val snapshotTS = now + versions.millis

      // Check with CI as a cross-check for missing cells.
      val before =
        new CassandraIterator(
          counter,
          store,
          ScanBounds(Segment.All),
          Selector.All,
          snapshotTS).toSeq
      before.size should equal(docs * versions)

      eventually(timeout(1.minute)) {
        val sstables = store.getSSTables()
        val level = 1 // Presuming the flush landed in L0, let's move up to L1
        val maxSSTableBytes = store.getCompactionStrategy.getMaxSSTableBytes

        store.getDataTracker.markCompacting(sstables) should be(true)

        val candidate =
          new LeveledManifest.CompactionCandidate(sstables, level, maxSSTableBytes)

        // GC half of every doc's cells.
        val minValidTime = Timestamp.ofMillis(versions / 2)
        val task = new CollectionTask(
          store,
          candidate,
          Int.MaxValue, // gcBefore
          HashMap((ScopeID(0), CollectionID(1024)) -> minValidTime),
          VersionsFilter.apply)

        noException shouldBe thrownBy {
          task.run()
        }

        val after =
          new CassandraIterator(
            counter,
            store,
            ScanBounds(Segment.All),
            Selector.All,
            snapshotTS).toSeq
        after.size should equal(docs * (versions / 2) + docs)

        after foreach { case (key, cell) =>
          val (_, validTS, _, _) =
            predicate[Tables.Versions.Key](key, cell, Tables.Versions.Schema)

          // -1 msec. to account for the extra version prior to MVT.
          validTS should be >= (minValidTime - 1.milli)
        }
      }

      val fresh = store.getSSTables().toArray

      fresh.size should be(1)
    }
  }

  test("filters SortedIndex") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val sets = 10
      val entries = 30

      val now = Clock.time
      val keyspace = storageEngine.keyspace

      val store = keyspace.getColumnFamilyStore(Tables.SortedIndex.CFName)

      store.clearUnsafe()

      val keys = Set.newBuilder[ByteBuffer]

      for (i <- 0 until sets) {
        for (j <- 0 until entries) {
          // This event has no corresponding "remove", despite
          // validTS < MVT. It stays alive.
          val alive = SetAdd(
            ScopeID(0),
            IndexID(32768 + i),
            Vector.empty, // terms
            Vector.empty, // values
            DocID(SubID(j + entries), CollectionID(1024)),
            Resolved(Timestamp.ofMillis(j - 2), now + (j - 2).millis),
            Add
          )

          val add = SetAdd(
            ScopeID(0),
            IndexID(32768 + i),
            Vector.empty, // terms
            Vector.empty, // values
            DocID(SubID(j + 1), CollectionID(1024)),
            Resolved(Timestamp.ofMillis(j - 1), now + (j - 1).millis),
            Add
          )

          val remove = SetAdd(
            ScopeID(0),
            IndexID(32768 + i),
            Vector.empty, // terms
            Vector.empty, // values
            DocID(SubID(j + 1), CollectionID(1024)),
            Resolved(Timestamp.ofMillis(j), now + j.millis),
            Remove
          )

          Seq(alive, add, remove) foreach { write =>
            val key = write.rowKey.nioBuffer()

            val mut =
              CassandraMutation(keyspaceName, key, index1CF = true, index2CF = false)

            write.mutateAt(mut, now + j.millis)
            keyspace.apply(mut.cmut, false /* writeCommitLog */ )

            keys += key

            if (j % 3 == 0) {
              store.forceBlockingFlush()
            }
          }
        }
      }

      store.forceBlockingFlush()

      val counter = new IteratorStatsCounter()
      val snapshotTS = now + entries.millis

      // Check with CI as a cross-check for missing cells.
      val before =
        new CassandraIterator(
          counter,
          store,
          ScanBounds(Segment.All),
          Selector.All,
          snapshotTS).toSeq
      before.size should equal(sets * entries * 3)

      eventually(timeout(1.minute)) {
        val sstables = store.getSSTables()
        val level = 1 // Presuming the flush landed in L0, let's move up to L1
        val maxSSTableBytes = store.getCompactionStrategy.getMaxSSTableBytes

        store.getDataTracker.markCompacting(sstables) should be(true)

        val candidate =
          new LeveledManifest.CompactionCandidate(sstables, level, maxSSTableBytes)

        // GC every set, removing the pair of dead cells.
        val minValidTime = Timestamp.ofMillis(entries)
        val task = new CollectionTask(
          store,
          candidate,
          Int.MaxValue, // gcBefore
          HashMap((ScopeID(0), CollectionID(1024)) -> minValidTime),
          SortedIndexFilter.apply)

        noException shouldBe thrownBy {
          task.run()
        }

        val after =
          new CassandraIterator(
            counter,
            store,
            ScanBounds(Segment.All),
            Selector.All,
            snapshotTS).toSeq
        after.size should equal(sets * entries - sets)

        after foreach { case (key, cell) =>
          val (_, bytes, validTS, action, _) =
            predicate[Tables.SortedIndex.Key](key, cell, Tables.SortedIndex.Schema)
          val values = CBOR.parse[Vector[IndexTerm]](bytes)
          val docID = values.last.value.asInstanceOf[DocIDV].value

          action should be(Add)
          validTS should be(Timestamp.ofMillis(docID.subID.toLong - entries - 2))
        }
      }

      val fresh = store.getSSTables().toArray

      fresh.size should be(1)
    }
  }

  test("filters HistoricalIndex") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val sets = 10
      val entries = 30

      val now = Clock.time
      val keyspace = storageEngine.keyspace

      val store = keyspace.getColumnFamilyStore(Tables.HistoricalIndex.CFName)

      store.clearUnsafe()

      val keys = Set.newBuilder[ByteBuffer]

      for (i <- 0 until sets) {
        for (j <- 0 until entries) {
          // Despite being an "add", this event is before the MVT, and
          // is deleted.
          val dead = SetAdd(
            ScopeID(0),
            IndexID(32768 + i),
            Vector.empty, // terms
            Vector.empty, // values
            DocID(SubID(j + entries), CollectionID(1024)),
            Resolved(Timestamp.ofMillis(j - 2), now + (j - 2).millis),
            Add
          )

          val add = SetAdd(
            ScopeID(0),
            IndexID(32768 + i),
            Vector.empty, // terms
            Vector.empty, // values
            DocID(SubID(j + 1), CollectionID(1024)),
            Resolved(Timestamp.ofMillis(j - 1), now + (j - 1).millis),
            Add
          )

          val remove = SetAdd(
            ScopeID(0),
            IndexID(32768 + i),
            Vector.empty, // terms
            Vector.empty, // values
            DocID(SubID(j + 1), CollectionID(1024)),
            Resolved(Timestamp.ofMillis(j), now + j.millis),
            Remove
          )

          Seq(dead, add, remove) foreach { write =>
            val key = write.rowKey.nioBuffer()

            val mut =
              CassandraMutation(keyspaceName, key, index1CF = true, index2CF = false)

            write.mutateAt(mut, now + j.millis)
            keyspace.apply(mut.cmut, false /* writeCommitLog */ )

            keys += key
          }
        }
      }

      store.forceBlockingFlush()

      val counter = new IteratorStatsCounter()
      val snapshotTS = now + entries.millis

      // Check with CI as a cross-check for missing cells.
      val before =
        new CassandraIterator(
          counter,
          store,
          ScanBounds(Segment.All),
          Selector.All,
          snapshotTS).toSeq
      before.size should equal(sets * entries * 3)

      eventually(timeout(1.minute)) {
        val sstables = store.getSSTables()
        val level = 1 // Presuming the flush landed in L0, let's move up to L1
        val maxSSTableBytes = store.getCompactionStrategy.getMaxSSTableBytes

        store.getDataTracker.markCompacting(sstables) should be(true)

        val candidate =
          new LeveledManifest.CompactionCandidate(sstables, level, maxSSTableBytes)

        // GC every set, removing the pair of dead cells.
        val minValidTime = Timestamp.ofMillis(entries)
        val task = new CollectionTask(
          store,
          candidate,
          Int.MaxValue, // gcBefore
          HashMap((ScopeID(0), CollectionID(1024)) -> minValidTime),
          HistoricalIndexFilter.apply)

        noException shouldBe thrownBy {
          task.run()
        }
      }

      val after =
        new CassandraIterator(
          counter,
          store,
          ScanBounds(Segment.All),
          Selector.All,
          snapshotTS).toSeq
      after.size should equal(0)

      store.getSSTables().size should be(0)
    }
  }

  test("upgrades index cells lacking transaction times") {
    withStorageEngine(StatsRecorder.Null) { storageEngine =>
      val keyspace = storageEngine.keyspace

      val siStore = keyspace.getColumnFamilyStore(Tables.SortedIndex.CFName)
      val hiStore = keyspace.getColumnFamilyStore(Tables.HistoricalIndex.CFName)

      siStore.clearUnsafe()
      hiStore.clearUnsafe()

      val keyBytes = CBOR.encode((ScopeID(1), IndexID(32768), Vector(LongV(42))))
      val key = Predicate(keyBytes).uncheckedRowKeyBytes

      val mut =
        CassandraMutation(
          keyspaceName,
          key.nioBuffer,
          index1CF = true,
          index2CF = false)

      val docID = DocID(SubID(42), CollectionID(1024))
      val validTS = Timestamp.ofMicros(1)
      val txnTS = Timestamp.ofMicros(2)

      def sortedWrite() = {
        // Yes, two of them.
        val values = Vector(IndexTerm(DocIDV(docID)), IndexTerm(DocIDV(docID)))
        val pred = (key, CBOR.encode(values), validTS, Add) // No txn TS!
        val value = new Value(pred, Unpooled.EMPTY_BUFFER)
        mut.add(
          Tables.SortedIndex.CFName,
          value.keyPredicate.uncheckedColumnNameBytes,
          Unpooled.EMPTY_BUFFER,
          txnTS,
          0)
      }

      def historicalWrite() = {
        // Yes, two of them.
        val values = Vector(IndexTerm(DocIDV(docID)), IndexTerm(DocIDV(docID)))
        val pred = (key, validTS, Add, CBOR.encode(values)) // No txn TS!
        val value = new Value(pred, Unpooled.EMPTY_BUFFER)

        mut.add(
          Tables.HistoricalIndex.CFName,
          value.keyPredicate.uncheckedColumnNameBytes,
          Unpooled.EMPTY_BUFFER,
          txnTS,
          0)
      }

      sortedWrite()
      historicalWrite()

      keyspace.apply(mut.cmut, false /* writeCommitLog */ )

      siStore.forceBlockingFlush()
      hiStore.forceBlockingFlush()

      Seq(siStore, hiStore) foreach { store =>
        eventually(timeout(1.minute)) {
          val sstables = store.getSSTables()
          val level = 1 // Presuming the flush landed in L0, let's move up to L1
          val maxSSTableBytes = store.getCompactionStrategy.getMaxSSTableBytes

          store.getDataTracker.markCompacting(sstables) should be(true)

          val candidate =
            new LeveledManifest.CompactionCandidate(sstables, level, maxSSTableBytes)

          val filter: CollectionFilter.Builder = store.name match {
            case Tables.SortedIndex.CFName     => SortedIndexFilter.apply
            case Tables.HistoricalIndex.CFName => HistoricalIndexFilter.apply
            case name                          => fail(s"Unexpected CF name $name")
          }

          val minValidTime = Clock.time
          val task = new CollectionTask(
            store,
            candidate,
            Int.MaxValue, // gcBefore
            HashMap((ScopeID(1), CollectionID(1024)) -> minValidTime),
            filter)

          noException shouldBe thrownBy {
            task.run()
          }
        }
      }
    }
  }
}
