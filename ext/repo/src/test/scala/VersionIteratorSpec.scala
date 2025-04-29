package fauna.repo.test

import fauna.atoms._
import fauna.lang.clocks.Clock
import fauna.prop.Prop
import fauna.repo.VersionIterator
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.storage._
import fauna.storage.api.MVTMap
import fauna.storage.cassandra._
import fauna.storage.doc._
import fauna.storage.ops.VersionAdd

class VersionIteratorSpec extends PropSpec {

  // Used to simulate diffs for rewriting at the GC edge.
  val field = Field[Int]("foo")

  val ctx = CassandraHelper.context("repo")

  def newScope = Prop.const(ScopeID(ctx.nextID()))

  once("yields unknown collections") {
    for {
      scope <- newScope
      ts    <- Prop.timestamp()
    } {
      val live = VersionAdd(
        scope,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(ts, ts),
        Create,
        SchemaVersion.Min,
        Data.empty,
        None)

      val dead = VersionAdd(
        scope,
        DocID(SubID(2), CollectionID(1024)),
        Resolved(ts, ts),
        Delete,
        SchemaVersion.Min,
        Data.empty,
        None)

      val expired = VersionAdd(
        scope,
        DocID(SubID(3), CollectionID(1024)),
        Resolved(ts, ts),
        Create,
        SchemaVersion.Min,
        Data(Version.TTLField -> Some(ts)),
        None)

      Seq(live, dead, expired) foreach { write =>
        ctx ! Query.write(write)
      }

      val engine = ctx.service.storage
      val keyspace = engine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.Versions.CFName)

      store.forceBlockingFlush()

      val counter = new IteratorStatsCounter()

      val citer = new CassandraIterator(
        counter,
        store,
        ScanBounds.All,
        Selector.from(scope),
        Clock.time)

      val versions = new VersionIterator(citer, Map.empty).toSeq
      versions.size should be(3)

      val ids = versions map { v => (v.parentScopeID, v.id.collID) } toSet

      ids should contain only ((scope, CollectionID(1024)))

    }
  }

  once("yields documents above MVT") {
    for {
      scope    <- newScope
      mvt      <- Prop.timestamp()
      createTS <- Prop.timestampAfter(mvt)
      updateTS <- Prop.timestampAfter(createTS)
      deleteTS <- Prop.timestampAfter(updateTS)
    } {
      val create = VersionAdd(
        scope,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(createTS, createTS),
        Create,
        SchemaVersion.Min,
        Data(field -> 1),
        None)

      val update = VersionAdd(
        scope,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(updateTS, updateTS),
        Create,
        SchemaVersion.Min,
        Data(field -> 2),
        Some(Diff(field -> 1)))

      val delete = VersionAdd(
        scope,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(deleteTS, deleteTS),
        Delete,
        SchemaVersion.Min,
        Data.empty,
        None)

      Seq(create, update, delete) foreach { write =>
        ctx ! Query.write(write)
      }

      val engine = ctx.service.storage
      val keyspace = engine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.Versions.CFName)

      store.forceBlockingFlush()

      val counter = new IteratorStatsCounter()

      val citer = new CassandraIterator(
        counter,
        store,
        ScanBounds.All,
        Selector.from(scope),
        Clock.time)

      val mvts = Map(scope -> MVTMap(Map(CollectionID(1024) -> mvt)))

      val versions = new VersionIterator(citer, mvts).toSeq
      versions.size should be(3)

      versions map { _.ts.validTS } should be(Seq(deleteTS, updateTS, createTS))
    }
  }

  once("filters documents below MVT") {
    for {
      scope    <- newScope
      createTS <- Prop.timestamp()
      updateTS <- Prop.timestampAfter(createTS)
      mvt      <- Prop.timestampAfter(updateTS)
    } {
      val create = VersionAdd(
        scope,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(createTS, createTS),
        Create,
        SchemaVersion.Min,
        Data(field -> 1),
        None)

      val update = VersionAdd(
        scope,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(updateTS, updateTS),
        Create,
        SchemaVersion.Min,
        Data(field -> 2),
        Some(Diff(field -> 1)))

      Seq(create, update) foreach { write =>
        ctx ! Query.write(write)
      }

      val engine = ctx.service.storage
      val keyspace = engine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.Versions.CFName)

      store.forceBlockingFlush()

      val counter = new IteratorStatsCounter()

      val citer = new CassandraIterator(
        counter,
        store,
        ScanBounds.All,
        Selector.from(scope),
        Clock.time)

      val mvts = Map(scope -> MVTMap(Map(CollectionID(1024) -> mvt)))

      val versions = new VersionIterator(citer, mvts).toSeq
      versions.size should be(1)

      // Rewritten GC edge
      versions.head.ts.validTS should be(updateTS)
      versions.head.action should be(Create)
      versions.head.diff should be(empty)
    }
  }

  once("filters deleted documents past MVT") {
    for {
      scope    <- newScope
      createTS <- Prop.timestamp()
      deleteTS <- Prop.timestampAfter(createTS)
      mvt      <- Prop.timestampAfter(deleteTS)
    } {
      val live = VersionAdd(
        scope,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(createTS, createTS),
        Create,
        SchemaVersion.Min,
        Data.empty,
        None)

      val dead = VersionAdd(
        scope,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(deleteTS, deleteTS),
        Delete,
        SchemaVersion.Min,
        Data.empty,
        None)

      Seq(live, dead) foreach { write =>
        ctx ! Query.write(write)
      }

      val engine = ctx.service.storage
      val keyspace = engine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.Versions.CFName)

      store.forceBlockingFlush()

      val counter = new IteratorStatsCounter()

      val citer = new CassandraIterator(
        counter,
        store,
        ScanBounds.All,
        Selector.from(scope),
        Clock.time)

      val mvts = Map(scope -> MVTMap(Map(CollectionID(1024) -> mvt)))

      val versions = new VersionIterator(citer, mvts).toSeq
      versions.size should be(0)
    }
  }

  once("yields deleted documents above MVT") {
    for {
      scope    <- newScope
      createTS <- Prop.timestamp()
      updateTS <- Prop.timestampAfter(createTS)
      mvt      <- Prop.timestampAfter(updateTS)
      deleteTS <- Prop.timestampAfter(mvt)
    } {
      val create = VersionAdd(
        scope,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(createTS, createTS),
        Create,
        SchemaVersion.Min,
        Data(field -> 1),
        None)

      val update = VersionAdd(
        scope,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(updateTS, updateTS),
        Create,
        SchemaVersion.Min,
        Data(field -> 2),
        Some(Diff(field -> 1)))

      val delete = VersionAdd(
        scope,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(deleteTS, deleteTS),
        Delete,
        SchemaVersion.Min,
        Data.empty,
        None)

      Seq(create, update, delete) foreach { write =>
        ctx ! Query.write(write)
      }

      val engine = ctx.service.storage
      val keyspace = engine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.Versions.CFName)

      store.forceBlockingFlush()

      val counter = new IteratorStatsCounter()

      val citer = new CassandraIterator(
        counter,
        store,
        ScanBounds.All,
        Selector.from(scope),
        Clock.time)

      val mvts = Map(scope -> MVTMap(Map(CollectionID(1024) -> mvt)))

      val versions = new VersionIterator(citer, mvts).toSeq
      // The delete and the first create below MVT.
      versions.size should be(2)

      versions.head.ts.validTS should be(deleteTS)

      // Rewritten GC edge
      versions.last.ts.validTS should be(updateTS)
      versions.last.action should be(Create)
      versions.last.diff should be(empty)
    }
  }

  once("filters expired documents past MVT") {
    for {
      scope    <- newScope
      createTS <- Prop.timestamp()
      updateTS <- Prop.timestampAfter(createTS)
      ttl      <- Prop.timestampAfter(updateTS)
      mvt      <- Prop.timestampAfter(ttl)
    } {
      val create = VersionAdd(
        scope,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(createTS, createTS),
        Create,
        SchemaVersion.Min,
        Data(field -> 1, Version.TTLField -> Some(ttl)),
        None)

      val update = VersionAdd(
        scope,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(updateTS, updateTS),
        Create,
        SchemaVersion.Min,
        Data(field -> 2, Version.TTLField -> Some(ttl)),
        Some(Diff(field -> 1)))

      Seq(create, update) foreach { write =>
        ctx ! Query.write(write)
      }

      val engine = ctx.service.storage
      val keyspace = engine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.Versions.CFName)

      store.forceBlockingFlush()

      val counter = new IteratorStatsCounter()

      val citer = new CassandraIterator(
        counter,
        store,
        ScanBounds.All,
        Selector.from(scope),
        Clock.time)

      val mvts = Map(scope -> MVTMap(Map(CollectionID(1024) -> mvt)))

      val versions = new VersionIterator(citer, mvts).toSeq
      versions.size should be(0)
    }
  }

  once("yields expired documents above MVT") {
    for {
      scope    <- newScope
      createTS <- Prop.timestamp()
      updateTS <- Prop.timestampAfter(createTS)
      mvt      <- Prop.timestampAfter(updateTS)
      ttl      <- Prop.timestampAfter(mvt)
    } {
      val create = VersionAdd(
        scope,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(createTS, createTS),
        Create,
        SchemaVersion.Min,
        Data(field -> 1, Version.TTLField -> Some(ttl)),
        None)

      val update = VersionAdd(
        scope,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(updateTS, updateTS),
        Create,
        SchemaVersion.Min,
        Data(field -> 2, Version.TTLField -> Some(ttl)),
        Some(Diff(field -> 1)))

      Seq(create, update) foreach { write =>
        ctx ! Query.write(write)
      }

      val engine = ctx.service.storage
      val keyspace = engine.keyspace
      val store = keyspace.getColumnFamilyStore(Tables.Versions.CFName)

      store.forceBlockingFlush()

      val counter = new IteratorStatsCounter()

      val citer = new CassandraIterator(
        counter,
        store,
        ScanBounds.All,
        Selector.from(scope),
        Clock.time)

      val mvts = Map(scope -> MVTMap(Map(CollectionID(1024) -> mvt)))

      val versions = new VersionIterator(citer, mvts).toSeq
      versions.size should be(1)

      // Rewritten GC edge
      versions.head.ts.validTS should be(updateTS)
      versions.head.action should be(Create)
      versions.head.diff should be(empty)
    }
  }
}
