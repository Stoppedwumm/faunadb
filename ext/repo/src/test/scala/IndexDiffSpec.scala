package fauna.repo.test

import fauna.atoms._
import fauna.lang._
import fauna.lang.clocks.Clock
import fauna.prop.Prop
import fauna.repo._
import fauna.repo.doc.Indexer
import fauna.storage._
import fauna.storage.api.MVTMap
import fauna.storage.index._
import fauna.storage.ir._
import fauna.storage.ops._
import org.scalatest.BeforeAndAfter

class IndexDiffSpec extends PropSpec with BeforeAndAfter {

  val ctx = CassandraHelper.context("repo")

  before {
    val engine = ctx.service.storage
    val keyspace = engine.keyspace

    keyspace.getColumnFamilyStores forEach {
      _.truncateBlocking()
    }
  }

  def newScope = Prop.const(ScopeID(ctx.nextID()))

  def insert(write: SetWrite, primary: Boolean, secondary: Boolean) = {
    val mut = CassandraMutation(
      Cassandra.KeyspaceName,
      write.rowKey.nioBuffer(),
      index1CF = primary,
      index2CF = secondary
    )

    write.mutateAt(mut, write.writeTS.transactionTS)

    val engine = ctx.service.storage
    val keyspace = engine.keyspace

    keyspace.apply(mut.cmut, false /* writeCommitLog */ )
  }

  def flush() = {
    val engine = ctx.service.storage
    val keyspace = engine.keyspace

    keyspace.getColumnFamilyStores forEach {
      _.forceBlockingFlush()
    }
  }

  def diff(mvts: Map[ScopeID, MVTMap], snapshotTS: Timestamp): IndexDiff = {
    val engine = ctx.service.storage
    val keyspace = engine.keyspace

    val primary = keyspace.getColumnFamilyStore(Tables.SortedIndex.CFName)
    val secondary = keyspace.getColumnFamilyStore(Tables.SortedIndex.CFName2)

    new IndexDiff(primary, secondary, mvts, snapshotTS)
  }

  once("equal") {
    for {
      scope <- newScope
      ts    <- Prop.timestamp()
    } {
      val key = IndexKey(
        scope,
        IndexID(32768),
        Vector(IndexTerm(DocIDV(CollectionID(1024).toDocID))))
      val value = IndexValue(
        IndexTuple(scope, DocID(SubID(1), CollectionID(1024))),
        Resolved(ts, ts),
        Add)

      val write = SetAdd(
        key.scope,
        key.id,
        key.terms map { _.value },
        Vector.empty,
        value.docID,
        value.ts,
        value.action,
        ttl = None)

      insert(write, primary = true, secondary = true)
      flush()

      val events = diff(Map.empty, Clock.time).run()
      events should be(empty)
    }
  }

  once("missing primary") {
    for {
      scope <- newScope
      ts    <- Prop.timestamp()
    } {
      val key = IndexKey(
        scope,
        IndexID(32768),
        Vector(IndexTerm(DocIDV(CollectionID(1024).toDocID))))
      val value = IndexValue(
        IndexTuple(scope, DocID(SubID(1), CollectionID(1024))),
        Resolved(ts, ts),
        Add)

      val write = SetAdd(
        key.scope,
        key.id,
        key.terms map { _.value },
        Vector.empty,
        value.docID,
        value.ts,
        value.action,
        ttl = None)

      insert(write, primary = false, secondary = true)
      flush()

      val events = diff(Map.empty, Clock.time).run()
      events.size should be(1)

      events.head should be(IndexDiff.Add(IndexRow(key, value, Indexer.Empty)))
    }
  }

  once("missing secondary") {
    for {
      scope <- newScope
      ts    <- Prop.timestamp()
    } {
      val key = IndexKey(
        scope,
        IndexID(32768),
        Vector(IndexTerm(DocIDV(CollectionID(1024).toDocID))))
      val value = IndexValue(
        IndexTuple(scope, DocID(SubID(1), CollectionID(1024))),
        Resolved(ts, ts),
        Add)

      val write = SetAdd(
        key.scope,
        key.id,
        key.terms map { _.value },
        Vector.empty,
        value.docID,
        value.ts,
        value.action,
        ttl = None)

      insert(write, primary = true, secondary = false)
      flush()

      val events = diff(Map.empty, Clock.time).run()
      events.size should be(1)

      events.head should be(IndexDiff.Remove(IndexRow(key, value, Indexer.Empty)))
    }
  }

  once("missing unborn") {
    for {
      scope      <- newScope
      snapshotTS <- Prop.timestamp()
      ts         <- Prop.timestampAfter(snapshotTS)
    } {
      val key = IndexKey(
        scope,
        IndexID(32768),
        Vector(IndexTerm(DocIDV(CollectionID(1024).toDocID))))
      val value = IndexValue(
        IndexTuple(scope, DocID(SubID(1), CollectionID(1024))),
        Resolved(ts, ts),
        Add)

      val write = SetAdd(
        key.scope,
        key.id,
        key.terms map { _.value },
        Vector.empty,
        value.docID,
        value.ts,
        value.action,
        ttl = None)

      insert(write, primary = true, secondary = false)
      flush()

      val events = diff(Map.empty, snapshotTS).run()
      events should be(empty)
    }
  }

  once("missing added below MVT") {
    for {
      scope      <- newScope
      addTS      <- Prop.timestamp()
      mvt        <- Prop.timestampAfter(addTS)
      removeTS   <- Prop.timestampAfter(mvt)
      snapshotTS <- Prop.timestampAfter(removeTS)
    } {
      val key = IndexKey(
        scope,
        IndexID(32768),
        Vector(IndexTerm(DocIDV(CollectionID(1024).toDocID))))
      val add = IndexValue(
        IndexTuple(scope, DocID(SubID(1), CollectionID(1024))),
        Resolved(addTS, addTS),
        Add)
      val remove = IndexValue(
        IndexTuple(scope, DocID(SubID(1), CollectionID(1024))),
        Resolved(removeTS, removeTS),
        Remove)

      Seq(add, remove) foreach { value =>
        val write = SetAdd(
          key.scope,
          key.id,
          key.terms map { _.value },
          Vector.empty,
          value.docID,
          value.ts,
          value.action,
          ttl = None)

        insert(write, primary = true, secondary = false)
      }

      flush()

      val mvts = Map(scope -> MVTMap(Map(CollectionID(1024) -> mvt)))
      val events = diff(mvts, snapshotTS).run()
      events.size should be(2)

      events.head should be(IndexDiff.Remove(IndexRow(key, remove, Indexer.Empty)))
      events.last should be(IndexDiff.Remove(IndexRow(key, add, Indexer.Empty)))
    }
  }

  once("missing deleted below MVT") {
    for {
      scope      <- newScope
      addTS      <- Prop.timestamp()
      removeTS   <- Prop.timestampAfter(addTS)
      mvt        <- Prop.timestampAfter(removeTS)
      snapshotTS <- Prop.timestampAfter(mvt)
    } {
      val key = IndexKey(
        scope,
        IndexID(32768),
        Vector(IndexTerm(DocIDV(CollectionID(1024).toDocID))))
      val add = IndexValue(
        IndexTuple(scope, DocID(SubID(1), CollectionID(1024))),
        Resolved(addTS, addTS),
        Add)
      val remove = IndexValue(
        IndexTuple(scope, DocID(SubID(1), CollectionID(1024))),
        Resolved(removeTS, removeTS),
        Remove)

      Seq(add, remove) foreach { value =>
        val write = SetAdd(
          key.scope,
          key.id,
          key.terms map { _.value },
          Vector.empty,
          value.docID,
          value.ts,
          value.action,
          ttl = None)

        insert(write, primary = true, secondary = false)
      }

      flush()

      val mvts = Map(scope -> MVTMap(Map(CollectionID(1024) -> mvt)))
      val events = diff(mvts, snapshotTS).run()
      events should be(empty)
    }
  }

  once("primary missing in the middle") {
    for {
      scope      <- newScope
      add1TS     <- Prop.timestamp()
      add2TS     <- Prop.timestampAfter(add1TS)
      removeTS   <- Prop.timestampAfter(add2TS)
      snapshotTS <- Prop.timestampAfter(removeTS)
    } {
      val key = IndexKey(
        scope,
        IndexID(32768),
        Vector(IndexTerm(DocIDV(CollectionID(1024).toDocID))))

      val add1 = IndexValue(
        IndexTuple(scope, DocID(SubID(1), CollectionID(1024))),
        Resolved(add1TS, add1TS),
        Add)
      val add2 = IndexValue(
        IndexTuple(scope, DocID(SubID(2), CollectionID(1024))),
        Resolved(add2TS, add2TS),
        Add)
      val remove = IndexValue(
        IndexTuple(scope, DocID(SubID(1), CollectionID(1024))),
        Resolved(removeTS, removeTS),
        Remove)

      // Both share the first add.
      {
        val write = SetAdd(
          key.scope,
          key.id,
          key.terms map { _.value },
          Vector.empty,
          add1.docID,
          add1.ts,
          add1.action,
          ttl = None)

        insert(write, primary = true, secondary = true)
      }

      // Primary is missing the second add.
      {
        val write = SetAdd(
          key.scope,
          key.id,
          key.terms map { _.value },
          Vector.empty,
          add2.docID,
          add2.ts,
          add2.action,
          ttl = None)

        insert(write, primary = false, secondary = true)
      }

      // Both share the remove.
      {
        val write = SetAdd(
          key.scope,
          key.id,
          key.terms map { _.value },
          Vector.empty,
          remove.docID,
          remove.ts,
          remove.action,
          ttl = None)

        insert(write, primary = true, secondary = true)
      }

      flush()

      val events = diff(Map.empty, snapshotTS).run()
      events.size should be(1)

      events.head should be(IndexDiff.Add(IndexRow(key, add2, Indexer.Empty)))
    }
  }

  once("secondary missing in the middle") {
    for {
      scope      <- newScope
      add1TS     <- Prop.timestamp()
      add2TS     <- Prop.timestampAfter(add1TS)
      removeTS   <- Prop.timestampAfter(add2TS)
      snapshotTS <- Prop.timestampAfter(removeTS)
    } {
      val key = IndexKey(
        scope,
        IndexID(32768),
        Vector(IndexTerm(DocIDV(CollectionID(1024).toDocID))))

      val add1 = IndexValue(
        IndexTuple(scope, DocID(SubID(1), CollectionID(1024))),
        Resolved(add1TS, add1TS),
        Add)
      val add2 = IndexValue(
        IndexTuple(scope, DocID(SubID(2), CollectionID(1024))),
        Resolved(add2TS, add2TS),
        Add)
      val remove = IndexValue(
        IndexTuple(scope, DocID(SubID(1), CollectionID(1024))),
        Resolved(removeTS, removeTS),
        Remove)

      // Both share the first add.
      {
        val write = SetAdd(
          key.scope,
          key.id,
          key.terms map { _.value },
          Vector.empty,
          add1.docID,
          add1.ts,
          add1.action,
          ttl = None)

        insert(write, primary = true, secondary = true)
      }

      // Secondary is missing the second add.
      {
        val write = SetAdd(
          key.scope,
          key.id,
          key.terms map { _.value },
          Vector.empty,
          add2.docID,
          add2.ts,
          add2.action,
          ttl = None)

        insert(write, primary = true, secondary = false)
      }

      // Both share the remove.
      {
        val write = SetAdd(
          key.scope,
          key.id,
          key.terms map { _.value },
          Vector.empty,
          remove.docID,
          remove.ts,
          remove.action,
          ttl = None)

        insert(write, primary = true, secondary = true)
      }

      flush()

      val events = diff(Map.empty, snapshotTS).run()
      events.size should be(1)

      events.head should be(IndexDiff.Remove(IndexRow(key, add2, Indexer.Empty)))
    }
  }

}
