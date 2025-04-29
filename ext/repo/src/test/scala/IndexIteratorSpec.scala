package fauna.repo.test

import fauna.atoms._
import fauna.lang.clocks.Clock
import fauna.prop.Prop
import fauna.repo.query.Query
import fauna.repo.IndexIterator
import fauna.storage._
import fauna.storage.api.MVTMap
import fauna.storage.cassandra._
import fauna.storage.ir._
import fauna.storage.ops.SetAdd

class IndexIteratorSpec extends PropSpec {

  val ctx = CassandraHelper.context("repo")

  def newScope = Prop.const(ScopeID(ctx.nextID()))

  def iterate(scope: ScopeID, mvts: Map[ScopeID, MVTMap]) = {
    val engine = ctx.service.storage
    val keyspace = engine.keyspace
    val store = keyspace.getColumnFamilyStore(Tables.SortedIndex.CFName)

    store.forceBlockingFlush()

    val counter = new IteratorStatsCounter()

    val citer = new CassandraIterator(
      counter,
      store,
      ScanBounds.All,
      Selector.from(scope),
      Clock.time)

    IndexIterator(citer, mvts).toSeq
  }

  once("yields unknown collections") {
    for {
      scope <- newScope
      ts    <- Prop.timestamp()
    } {
      val live = SetAdd(
        scope,
        IndexID(32768),
        Vector(DocIDV(CollectionID(1024).toDocID)),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(ts, ts),
        Add,
        ttl = None)

      val dead = SetAdd(
        scope,
        IndexID(32768),
        Vector(DocIDV(CollectionID(1024).toDocID)),
        Vector.empty,
        DocID(SubID(2), CollectionID(1024)),
        Resolved(ts, ts),
        Remove,
        ttl = None)

      val expired = SetAdd(
        scope,
        IndexID(32768),
        Vector(DocIDV(CollectionID(1024).toDocID)),
        Vector.empty,
        DocID(SubID(3), CollectionID(1024)),
        Resolved(ts, ts),
        Add,
        ttl = Some(ts))

      Seq(live, dead, expired) foreach { write =>
        ctx ! Query.write(write)
      }

      val entries = iterate(scope, Map.empty)
      entries.size should be(3)

      val ids = entries map { e => (e.key.scope, e.value.docID.collID) } toSet

      ids should contain only ((scope, CollectionID(1024)))
    }
  }

  once("MVT below all events") {
    for {
      scope      <- newScope
      mvt        <- Prop.timestamp()
      createTS   <- Prop.timestampAfter(mvt)
      updateTS   <- Prop.timestampAfter(createTS)
      deleteTS   <- Prop.timestampAfter(updateTS)
      recreateTS <- Prop.timestampAfter(deleteTS)
    } {
      val create = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("alice")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(createTS, createTS),
        Add,
        ttl = None)

      val update1 = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("alice")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(updateTS, updateTS),
        Remove,
        ttl = None)

      val update2 = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("bob")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(updateTS, updateTS),
        Add,
        ttl = None)

      val delete = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("bob")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(deleteTS, deleteTS),
        Remove,
        ttl = None)

      val recreate = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("alice")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(recreateTS, recreateTS),
        Add,
        ttl = None)

      Seq(create, update1, update2, delete, recreate) foreach { write =>
        ctx ! Query.write(write)
      }

      val mvts = Map(scope -> MVTMap(Map(CollectionID(1024) -> mvt)))
      val entries = iterate(scope, mvts)
      entries.size should be(5)
    }
  }

  once("MVT after create") {
    for {
      scope      <- newScope
      createTS   <- Prop.timestamp()
      mvt        <- Prop.timestampAfter(createTS)
      updateTS   <- Prop.timestampAfter(mvt)
      deleteTS   <- Prop.timestampAfter(updateTS)
      recreateTS <- Prop.timestampAfter(deleteTS)
    } {
      val create = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("alice")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(createTS, createTS),
        Add,
        ttl = None)

      val update1 = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("alice")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(updateTS, updateTS),
        Remove,
        ttl = None)

      val update2 = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("bob")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(updateTS, updateTS),
        Add,
        ttl = None)

      val delete = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("bob")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(deleteTS, deleteTS),
        Remove,
        ttl = None)

      val recreate = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("alice")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(recreateTS, recreateTS),
        Add,
        ttl = None)

      Seq(create, update1, update2, delete, recreate) foreach { write =>
        ctx ! Query.write(write)
      }

      val mvts = Map(scope -> MVTMap(Map(CollectionID(1024) -> mvt)))
      val entries = iterate(scope, mvts)
      entries.size should be(5)
    }
  }

  once("MVT after update") {
    for {
      scope      <- newScope
      createTS   <- Prop.timestamp()
      updateTS   <- Prop.timestampAfter(createTS)
      mvt        <- Prop.timestampAfter(updateTS)
      deleteTS   <- Prop.timestampAfter(mvt)
      recreateTS <- Prop.timestampAfter(deleteTS)
    } {
      val create = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("alice")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(createTS, createTS),
        Add,
        ttl = None)

      val update1 = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("alice")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(updateTS, updateTS),
        Remove,
        ttl = None)

      val update2 = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("bob")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(updateTS, updateTS),
        Add,
        ttl = None)

      val delete = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("bob")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(deleteTS, deleteTS),
        Remove,
        ttl = None)

      val recreate = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("alice")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(recreateTS, recreateTS),
        Add,
        ttl = None)

      Seq(create, update1, update2, delete, recreate) foreach { write =>
        ctx ! Query.write(write)
      }

      val mvts = Map(scope -> MVTMap(Map(CollectionID(1024) -> mvt)))
      val entries = iterate(scope, mvts)
      entries.size should be(3)

      val timeline = entries map { _.value.ts.validTS }

      timeline should contain theSameElementsAs (Seq(recreateTS, deleteTS, updateTS))
    }
  }

  once("MVT after delete") {
    for {
      scope      <- newScope
      createTS   <- Prop.timestamp()
      updateTS   <- Prop.timestampAfter(createTS)
      deleteTS   <- Prop.timestampAfter(updateTS)
      mvt        <- Prop.timestampAfter(deleteTS)
      recreateTS <- Prop.timestampAfter(mvt)
    } {
      val create = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("alice")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(createTS, createTS),
        Add,
        ttl = None)

      val update1 = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("alice")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(updateTS, updateTS),
        Remove,
        ttl = None)

      val update2 = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("bob")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(updateTS, updateTS),
        Add,
        ttl = None)

      val delete = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("bob")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(deleteTS, deleteTS),
        Remove,
        ttl = None)

      val recreate = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("alice")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(recreateTS, recreateTS),
        Add,
        ttl = None)

      Seq(create, update1, update2, delete, recreate) foreach { write =>
        ctx ! Query.write(write)
      }

      val mvts = Map(scope -> MVTMap(Map(CollectionID(1024) -> mvt)))
      val entries = iterate(scope, mvts)
      entries.size should be(1)
      entries.head.value.ts.validTS should be(recreateTS)
    }
  }

  once("MVT after recreate") {
    for {
      scope      <- newScope
      createTS   <- Prop.timestamp()
      updateTS   <- Prop.timestampAfter(createTS)
      deleteTS   <- Prop.timestampAfter(updateTS)
      recreateTS <- Prop.timestampAfter(deleteTS)
      mvt        <- Prop.timestampAfter(recreateTS)
    } {
      val create = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("alice")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(createTS, createTS),
        Add,
        ttl = None)

      val update1 = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("alice")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(updateTS, updateTS),
        Remove,
        ttl = None)

      val update2 = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("bob")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(updateTS, updateTS),
        Add,
        ttl = None)

      val delete = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("bob")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(deleteTS, deleteTS),
        Remove,
        ttl = None)

      val recreate = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("alice")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(recreateTS, recreateTS),
        Add,
        ttl = None)

      Seq(create, update1, update2, delete, recreate) foreach { write =>
        ctx ! Query.write(write)
      }

      val mvts = Map(scope -> MVTMap(Map(CollectionID(1024) -> mvt)))
      val entries = iterate(scope, mvts)
      entries.size should be(1)
      entries.head.value.ts.validTS should be(recreateTS)
    }
  }

  once("MVT before TTL") {
    for {
      scope    <- newScope
      createTS <- Prop.timestamp()
      mvt      <- Prop.timestampAfter(createTS)
      ttl      <- Prop.timestampAfter(mvt)
    } {
      val add = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("alice")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(createTS, createTS),
        Add,
        ttl = Some(ttl))

      ctx ! Query.write(add)

      val mvts = Map(scope -> MVTMap(Map(CollectionID(1024) -> mvt)))
      val entries = iterate(scope, mvts)
      entries.size should be(1)
      entries.head.value.ts.validTS should be(createTS)
    }
  }

  once("MVT after TTL") {
    for {
      scope    <- newScope
      createTS <- Prop.timestamp()
      ttl      <- Prop.timestampAfter(createTS)
      mvt      <- Prop.timestampAfter(ttl)
    } {
      val add = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("alice")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(createTS, createTS),
        Add,
        ttl = Some(ttl))

      ctx ! Query.write(add)

      val mvts = Map(scope -> MVTMap(Map(CollectionID(1024) -> mvt)))
      iterate(scope, mvts).size should be(0)
    }
  }

  once("live doc earlier than dead") {
    for {
      scope    <- newScope
      liveTS   <- Prop.timestamp()
      addTS    <- Prop.timestampAfter(liveTS)
      removeTS <- Prop.timestampAfter(addTS)
      mvt      <- Prop.timestampAfter(removeTS)
    } {
      val live = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("alice")),
        Vector.empty,
        DocID(SubID(1), CollectionID(1024)),
        Resolved(liveTS, liveTS),
        Add,
        ttl = None)

      val add = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("alice")),
        Vector.empty,
        DocID(SubID(2), CollectionID(1024)), // Different doc.
        Resolved(addTS, addTS),
        Add,
        ttl = None)

      val remove = SetAdd(
        scope,
        IndexID(32768),
        Vector(StringV("alice")),
        Vector.empty,
        DocID(SubID(2), CollectionID(1024)),
        Resolved(removeTS, removeTS),
        Remove,
        ttl = None)

      Seq(live, add, remove) foreach { write =>
        ctx ! Query.write(write)
      }

      val mvts = Map(scope -> MVTMap(Map(CollectionID(1024) -> mvt)))
      val entries = iterate(scope, mvts)
      entries.size should be(1)

      entries.head.value.ts.validTS should be(liveTS)
    }
  }
}
