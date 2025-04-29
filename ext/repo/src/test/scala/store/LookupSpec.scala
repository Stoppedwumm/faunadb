package fauna.repo.test

import fauna.atoms._
import fauna.codex.cbor._
import fauna.lang.{ Page, Timestamp }
import fauna.lang.syntax._
import fauna.repo._
import fauna.repo.query.Query
import fauna.repo.store._
import fauna.storage._
import fauna.storage.cassandra._
import fauna.storage.cassandra.comparators._
import fauna.storage.lookup._
import fauna.storage.ops._
import io.netty.buffer.Unpooled
import scala.concurrent.duration._

class LookupSpec extends Spec {
  def TS(ts: Long) = Resolved(Timestamp.ofMicros(ts))
  def TS(ts: String) = Resolved(Timestamp.parse(ts))

  val ctx = CassandraHelper.context("repo")
  def newScope = ScopeID(ctx.nextID())
  def newKey = GlobalKeyID(ctx.nextID())

  // Just here to support OldLookups, below.
  class Table[K: CassandraCodec](val schema: ColumnFamilySchema) {

    type Key = K

    val keyCodec: CassandraCodec[K] = implicitly[CassandraCodec[K]]
  }

  object OldLookups {
    type Key = Tables.Lookups.OldKey
    val table: Table[Key] = new Table[Key](Tables.Lookups.OldSchema)

    def add(entry: LookupEntry): Query[Unit] = {
      val value = toValue(entry)
      Query.write(InsertDataValueWrite(
        table.schema.name,
        value.keyPredicate.uncheckedRowKeyBytes,
        value.keyPredicate.uncheckedColumnNameBytes,
        value.data,
        None))
    }

    def remove(entry: LookupEntry): Query[Unit] =
      Query.write(LookupRemove(entry))

    private def toValue(entry: LookupEntry) = {
      val rowKey = Predicate(Tables.Lookups.rowKey(entry.globalID)).uncheckedRowKeyBytes
      val predicate = Predicate(
        (rowKey,
         entry.scope.toLong,
         CBOR.encode(entry.id).toByteArray,
         entry.ts.validTS,
         entry.action))
      new Value[Key](predicate, Unpooled.EMPTY_BUFFER)
    }
  }

  def allDatabaseIDs: Query[Iterable[(ScopeID, DatabaseID)]] = {
    val entries = LookupStore.localScan(ScanBounds(Segment.All)) collectT {
      case (scope, entry) if entry.id.collID == DatabaseID.collID =>
        Some((scope, entry))
      case _ => None
    } flattenT

    entries map { es =>
      val byGlobal = es.groupBy { _._1 }.toSeq

      byGlobal flatMap {
        case (_, entries) =>
          val byDoc = entries groupBy { _._2.id } toSeq

          byDoc map {
            case (_, entries) =>
              val latest = entries.sortBy { _._2.ts.validTS } head

              (latest._2.scope, latest._2.id.as[DatabaseID])
          }
      }
    }
  }

  "DatabaseLookups" - {
    "work" in {
      val sID = newScope
      val dbScopeID = newScope
      val dbID = DatabaseID(3)

      ctx ! LookupStore.add(LiveLookup(dbScopeID, sID, dbID.toDocID, Unresolved, Add))

      val dbRep = ctx ! Store.getDatabaseID(dbScopeID)
      dbRep.isDefined should be(true)
      dbRep.get should equal ((sID, dbID))

      val db2Rep = ctx ! Store.getDatabaseID(sID)
      db2Rep.isEmpty should be(true)

      val db2ID = DatabaseID(4)
      ctx ! LookupStore.add(LiveLookup(dbScopeID, sID, db2ID.toDocID, Unresolved, Add))
      ctx ! LookupStore.add(LiveLookup(dbScopeID, sID, db2ID.toDocID, Unresolved, Remove))

      val allDBs = ctx ! allDatabaseIDs
      allDBs should have size (2)
      allDBs should contain theSameElementsAs List(sID -> dbID, sID -> db2ID)
    }

    "handles deleted records" in {
      val sID = newScope
      val dbScopeID = newScope
      val dbID = DatabaseID(5678)
      ctx ! LookupStore.add(LiveLookup(dbScopeID, sID, dbID.toDocID, Unresolved, Add))
      ctx ! LookupStore.add(LiveLookup(dbScopeID, sID, dbID.toDocID, Unresolved, Remove))

      (ctx ! Store.getDatabaseID(dbScopeID)) should be(None)

      ctx ! LookupStore.add(
        LiveLookup(dbScopeID, sID, dbID.toDocID, Unresolved, Add))
      (ctx ! Store.getDatabaseID(dbScopeID)) should be(Some(sID -> dbID))
    }

    "handles moved records" in {
      val gID, newScopeID, oldScopeID = newScope
      val oldID = DatabaseID(1).toDocID
      val newID = DatabaseID(2).toDocID

      ctx ! LookupStore.add(LiveLookup(gID, oldScopeID, oldID, TS(1), Add))
      ctx ! (for {
        _ <- LookupStore.add(LiveLookup(gID, oldScopeID, oldID, TS(2), Remove))
        _ <- LookupStore.add(LiveLookup(gID, newScopeID, newID, TS(2), Add))
      } yield ())

      (ctx ! Store.getLatestDatabaseID(gID)) shouldBe
        Some(newScopeID -> newID.as[DatabaseID])
    }

    "removes legacy entries" in {
      val sID = newScope
      val dbScopeID = newScope
      val dbID = DatabaseID(5678)
      val entry = LiveLookup(dbScopeID, sID, dbID.toDocID, TS(1), Add)

      ctx ! OldLookups.add(entry)
      (ctx ! Store.getDatabaseID(dbScopeID)) should be(Some(sID -> dbID))

      ctx ! LookupStore.remove(LiveLookup(dbScopeID, sID, dbID.toDocID, TS(1), Add))
      (ctx ! Store.getDatabaseID(dbScopeID)) should be(None)
    }
  }

  "KeyLookups" - {
    "work" in {
      val sID = newScope
      val gID = newKey
      val keyID = KeyID(4)

      ctx ! LookupStore.add(LiveLookup(gID, sID, keyID.toDocID, Unresolved, Add))

      val keyRep = ctx ! Store.keys(gID).flattenT
      keyRep should have size (1)
      keyRep.head should equal ((sID, keyID))

      val key2ID = KeyID(5)
      ctx ! LookupStore.add(LiveLookup(gID, sID, key2ID.toDocID, Unresolved, Add))
      val key2Rep = (ctx ! Store.keys(gID).flattenT)
      key2Rep should have size (2)
      key2Rep should contain theSameElementsAs (Seq(sID -> keyID, sID -> key2ID))

      val key3Rep = (ctx ! Store.keys(GlobalKeyID(4)).flattenT)
      key3Rep.isEmpty should be(true)
    }

    "removes legacy entries" in {
      val sID = newScope
      val gID = newKey
      val keyID = KeyID(5678)
      val entry = LiveLookup(gID, sID, keyID.toDocID, TS(1), Add)

      ctx ! OldLookups.add(entry)
      val keyRep = ctx ! Store.keys(gID).flattenT
      keyRep should have size (1)
      keyRep.head should equal ((sID, keyID))

      ctx ! LookupStore.remove(LiveLookup(gID, sID, keyID.toDocID, TS(1), Add))
      val key3Rep = (ctx ! Store.keys(GlobalKeyID(4)).flattenT)
      key3Rep.isEmpty should be(true)
    }

    "handles deleted records" in {
      val sID = newScope
      val s2ID = newScope
      val gID = newKey
      val keyID = KeyID(5678)

      ctx ! LookupStore.add(LiveLookup(gID, sID, keyID.toDocID, Unresolved, Add))
      ctx ! LookupStore.add(LiveLookup(gID, sID, keyID.toDocID, Unresolved, Remove))

      (ctx ! Store.keys(gID).flattenT).isEmpty should be(true)

      ctx ! LookupStore.add(LiveLookup(gID, sID, keyID.toDocID, Unresolved, Add))
      (ctx ! Store.keys(gID).flattenT) should be(List(sID -> keyID))

      ctx ! LookupStore.add(LiveLookup(gID, s2ID, keyID.toDocID, Unresolved, Add))
      (ctx ! Store.keys(gID).flattenT) should contain theSameElementsAs (List(
        sID -> keyID,
        s2ID -> keyID))

      ctx ! LookupStore.add(LiveLookup(gID, sID, keyID.toDocID, Unresolved, Remove))
      (ctx ! Store.keys(gID).flattenT) should be(List(s2ID -> keyID))

      ctx ! LookupStore.add(LiveLookup(gID, sID, keyID.toDocID, Unresolved, Add))
      ctx ! LookupStore.add(LiveLookup(gID, s2ID, keyID.toDocID, Unresolved, Remove))
      (ctx ! Store.keys(gID).flattenT) should be(List(sID -> keyID))

      ctx ! LookupStore.add(LiveLookup(gID, sID, keyID.toDocID, Unresolved, Remove))
      (ctx ! Store.keys(gID).flattenT).isEmpty should be(true)
    }
  }

  "CollectionAtTS" - {
    "returns the most recent live entry" in {
      val globalID1 = GlobalDatabaseID(ctx.nextID())
      val globalID2 = GlobalDatabaseID(ctx.nextID())

      val entries = Seq(
        (
          globalID1,
          LiveLookup(
            globalID1,
            ScopeID(323245150180474979L),
            DatabaseID(323066921250455650L).toDocID,
            TS("2022-02-08T22:58:15.238Z"),
            Add)),
        (
          globalID1,
          LiveLookup(
            globalID1,
            ScopeID(323245150180474979L),
            DatabaseID(323066921250455650L).toDocID,
            TS("2022-02-08T22:31:19.910Z"),
            Add)),
        (
          globalID1,
          LiveLookup(
            globalID1,
            ScopeID(323245150180474979L),
            DatabaseID(323066921250455650L).toDocID,
            TS("2022-02-08T22:31:15.310Z"),
            Add)),
        (
          globalID2,
          LiveLookup(
            globalID2,
            ScopeID(322959549407429730L),
            DatabaseID(323244758719791203L).toDocID,
            TS("2022-02-10T21:45:33.940Z"),
            Remove)),
        (
          globalID2,
          LiveLookup(
            globalID2,
            ScopeID(322959549407429730L),
            DatabaseID(323244758719791203L).toDocID,
            TS("2022-02-10T21:44:06.910Z"),
            Add)),
        (
          globalID2,
          LiveLookup(
            globalID2,
            ScopeID(322959549407429730L),
            DatabaseID(323244758719791203L).toDocID,
            TS("2022-02-10T21:37:54.430Z"),
            Add))
      )

      val output = ctx ! CollectionAtTS(
        Query.value(Page(entries)),
        Timestamp.Max,
        1.day
      ).flattenT

      output shouldBe Seq(
        LiveLookup(
          globalID1,
          ScopeID(323245150180474979L),
          DatabaseID(323066921250455650L).toDocID,
          TS("2022-02-08T22:58:15.238Z"),
          Add))

      entries foreach { case (_, entry) =>
        ctx ! LookupStore.add(entry)
      }

      val store = ctx ! Store.getDatabaseID(globalID1)
      store.isEmpty should be(false)
      store.get._1 should be(ScopeID(323245150180474979L))
      store.get._2 should be(DatabaseID(323066921250455650L))
    }

    "same database with different scopeID and globalID" in {
      val globalID = GlobalDatabaseID(ctx.nextID())
      val scopeID = ScopeID(2L)
      val dbID = DatabaseID(1234L).toDocID

      val entries = Seq(
        (globalID, LiveLookup(globalID, scopeID, dbID, TS(1), Add)),
        (globalID, LiveLookup(globalID, scopeID, dbID, TS(2), Add)),
        (scopeID, LiveLookup(scopeID, scopeID, dbID, TS(1), Add)),
        (scopeID, LiveLookup(scopeID, scopeID, dbID, TS(2), Add))
      )

      val output = ctx ! CollectionAtTS(
        Query.value(Page(entries)),
        Timestamp.Max,
        1.day
      ).flattenT

      output shouldBe Seq(
        LiveLookup(globalID, scopeID, dbID, TS(2), Add),
        LiveLookup(scopeID, scopeID, dbID, TS(2), Add))

      entries foreach { case (_, entry) =>
        ctx ! LookupStore.add(entry)
      }

      val store = ctx ! Store.getDatabaseID(globalID)
      store.isEmpty should be(false)
      store.get._1 should be(scopeID)
      store.get._2 should be(dbID.as[DatabaseID])
    }

    "works with rewritten scopes having the same globalID" in {
      val globalID = GlobalDatabaseID(323066921250457698L)

      val entries = Seq(
        (
          globalID,
          LiveLookup(
            globalID,
            ScopeID(323245150180474979L),
            DatabaseID(323066921250455650L).toDocID,
            TS("2022-02-10T21:45:33.940Z"),
            Remove)),
        (
          globalID,
          LiveLookup(
            globalID,
            ScopeID(323245150180474979L),
            DatabaseID(323066921250455650L).toDocID,
            TS("2022-02-08T22:58:15.238Z"),
            Add)),
        (
          globalID,
          LiveLookup(
            globalID,
            ScopeID(323245150180474979L),
            DatabaseID(323066921250455650L).toDocID,
            TS("2022-02-08T22:31:19.910Z"),
            Add)),
        (
          globalID,
          LiveLookup(
            globalID,
            ScopeID(323245150180474979L),
            DatabaseID(323066921250455650L).toDocID,
            TS("2022-02-08T22:31:15.310Z"),
            Add)),
        (
          globalID,
          LiveLookup(
            globalID,
            ScopeID(323244668119679075L),
            DatabaseID(323066921250455650L).toDocID,
            TS("2022-02-10T21:37:54.430Z"),
            Remove)),
        (
          globalID,
          LiveLookup(
            globalID,
            ScopeID(323244668119679075L),
            DatabaseID(323066921250455650L).toDocID,
            TS("2022-02-08T22:58:15.238Z"),
            Add)),
        (
          globalID,
          LiveLookup(
            globalID,
            ScopeID(323244668119679075L),
            DatabaseID(323066921250455650L).toDocID,
            TS("2022-02-08T22:31:19.910Z"),
            Add)),
        (
          globalID,
          LiveLookup(
            globalID,
            ScopeID(323244668119679075L),
            DatabaseID(323066921250455650L).toDocID,
            TS("2022-02-08T22:31:15.310Z"),
            Add)),
        (
          globalID,
          LiveLookup(
            globalID,
            ScopeID(322959549407429730L),
            DatabaseID(323245240560386147L).toDocID,
            TS("2022-02-10T21:45:33.940Z"),
            Add)),
        (
          globalID,
          LiveLookup(
            globalID,
            ScopeID(322959549407429730L),
            DatabaseID(323244758719791203L).toDocID,
            TS("2022-02-10T21:45:33.940Z"),
            Remove)),
        (
          globalID,
          LiveLookup(
            globalID,
            ScopeID(322959549407429730L),
            DatabaseID(323244758719791203L).toDocID,
            TS("2022-02-10T21:44:06.910Z"),
            Add)),
        (
          globalID,
          LiveLookup(
            globalID,
            ScopeID(322959549407429730L),
            DatabaseID(323244758719791203L).toDocID,
            TS("2022-02-10T21:37:54.430Z"),
            Add)),
        (
          globalID,
          LiveLookup(
            globalID,
            ScopeID(322959549407429730L),
            DatabaseID(323066921250455650L).toDocID,
            TS("2022-02-10T21:37:54.430Z"),
            Remove)),
        (
          globalID,
          LiveLookup(
            globalID,
            ScopeID(322959549407429730L),
            DatabaseID(323066921250455650L).toDocID,
            TS("2022-02-10T21:36:27.040Z"),
            Add)),
        (
          globalID,
          LiveLookup(
            globalID,
            ScopeID(322959549407429730L),
            DatabaseID(323066921250455650L).toDocID,
            TS("2022-02-08T22:58:15.238Z"),
            Add)),
        (
          globalID,
          LiveLookup(
            globalID,
            ScopeID(322959549407429730L),
            DatabaseID(323066921250455650L).toDocID,
            TS("2022-02-08T22:31:19.910Z"),
            Add)),
        (
          globalID,
          LiveLookup(
            globalID,
            ScopeID(322959549407429730L),
            DatabaseID(323066921250455650L).toDocID,
            TS("2022-02-08T22:31:15.310Z"),
            Add))
      )

      val output = ctx ! CollectionAtTS(
        Query.value(Page(entries)),
        Timestamp.Max,
        1.day
      ).flattenT

      output shouldBe Seq(
        LiveLookup(
          globalID,
          ScopeID(322959549407429730L),
          DatabaseID(323245240560386147L).toDocID,
          TS("2022-02-10T21:45:33.940Z"),
          Add))

      entries foreach { case (_, entry) =>
        ctx ! LookupStore.add(entry)
      }

      val live =
        ctx.copy(schemaRetentionDays = 1.day) ! Store.getDatabaseID(globalID)
      live.isEmpty should be(false)
      live.get._1 should be(ScopeID(322959549407429730L))
      live.get._2 should be(DatabaseID(323245240560386147L))
    }

    "honors retention period" in {
      val globalID = GlobalDatabaseID(ctx.nextID())
      val scopeID = ScopeID(ctx.nextID())

      val entries = Seq(
        (
          globalID,
          LiveLookup(globalID, scopeID, DatabaseID(1234L).toDocID, TS(3L), Remove)),
        (
          globalID,
          LiveLookup(globalID, scopeID, DatabaseID(1234L).toDocID, TS(2L), Add)),
        (
          globalID,
          LiveLookup(globalID, scopeID, DatabaseID(1234L).toDocID, TS(1L), Add)),
        (
          globalID,
          LiveLookup(globalID, scopeID, DatabaseID(1234L).toDocID, TS(0L), Add))
      )

      val retentionPeriod = 1.day

      ctx ! CollectionAtTS(
        Query.value(Page(entries)),
        Timestamp.Epoch + 1.day,
        retentionPeriod
      ).flattenT shouldBe Seq(
        LiveLookup(globalID, scopeID, DatabaseID(1234L).toDocID, TS(3L), Remove))

      ctx ! CollectionAtTS(
        Query.value(Page(entries)),
        Timestamp.Epoch + 2.day,
        retentionPeriod
      ).flattenT shouldBe Seq.empty
    }

    "returns latest live before a delete" in {
      val globalID = GlobalDatabaseID(ctx.nextID())
      val liveScope = ScopeID(ctx.nextID())
      val deadScope = ScopeID(ctx.nextID())
      val db = DatabaseID(ctx.nextID())

      ctx ! LookupStore.add(LiveLookup(globalID, liveScope, db.toDocID, TS(1L), Add))
      ctx ! LookupStore.add(LiveLookup(globalID, deadScope, db.toDocID, TS(1L), Add))
      ctx ! LookupStore.add(
        LiveLookup(globalID, deadScope, db.toDocID, TS(2L), Remove))

      val live = ctx ! Store.getDatabaseID(globalID)
      live.isEmpty should be(false)
      live.get._1 should be(liveScope)
      live.get._2 should be(db)
    }
  }
}
