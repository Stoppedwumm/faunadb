package fauna.model.test

import fauna.atoms._
import fauna.lang.syntax._
import fauna.model._
import fauna.model.tasks._
import fauna.repo._
import fauna.repo.store._
import fauna.repo.test.CassandraHelper
import fauna.stats._
import fauna.storage._
import fauna.storage.doc.Data
import fauna.storage.lookup._
import fauna.storage.ops.Write
import org.scalatest.PrivateMethodTester._
import scala.concurrent.duration._

class LookupRepairSpec extends Spec {
  import SocialHelpers._
  Write invokePrivate PrivateMethod[Unit](Symbol("mungeOffset"))(0.millis)

  "LookupRepair" - {

    val ctx = CassandraHelper.context("model")
    def statsBuf = new StatsRequestBuffer(
      Set("Lookup.Repair.Events.Added", "Lookup.Repair.Events.Removed"))

    val exec = TaskExecutor(ctx)

    def runTasks() = {
      val parent =
        ctx ! Task.createLocal(ScopeID(0), "dummy parent", Data.empty, None)
      ctx ! Task.maybeBlock(parent, Vector.empty, Data.empty, 0)

      ctx ! Repair.RepairLookups.create(parent.id, Repair.Commit, None)
      ctx ! Repair.RepairReverseLookups.create(parent.id, Repair.Commit, None)

      while ((ctx ! getTasks()).nonEmpty) {
        exec.step()
      }
    }

    def newDB = {
      val scope = ctx ! newScope
      (ctx ! Database.forScope(scope)).get
    }

    "repair global ID holes" in {
      val db = newDB

      val root =
        ctx ! ModelStore.versions(db.parentScopeID, db.id.toDocID).headValueT

      val cleared = root.get.data.remove(Database.GlobalIDField)
      ctx ! Store.insertUnmigrated(db.parentScopeID, db.id.toDocID, cleared)

      val id = GlobalDatabaseID(db.scopeID.toLong)
      val backfilled = cleared.update(Database.GlobalIDField -> id)
      ctx ! (Store.insertUnmigrated(
        db.parentScopeID,
        db.id.toDocID,
        backfilled) flatMap { v =>
        LookupStore.add(
          LiveLookup(
            id,
            db.parentScopeID,
            db.id.toDocID,
            v.ts,
            v.action.toSetAction))
      })

      // Whoops. Original global ID still works...
      val orig = (ctx ! Store.getDatabaseID(db.globalID))
      orig should equal(Some((db.parentScopeID, db.id)))

      // ... but so does the backfill! Two global IDs reach one database.
      {
        val byScope =
          (ctx ! Store.getDatabaseID(GlobalDatabaseID(db.scopeID.toLong)))
        byScope should equal(Some((db.parentScopeID, db.id)))
      }

      val stats = statsBuf

      CassandraHelper.withStats(stats) {
        runTasks()
      }

      // Original global ID is now gone.
      (ctx ! Store.getDatabaseID(db.globalID)) should be(None)

      // Backfilled lookup ID works.
      {
        val byScope =
          (ctx ! Store.getDatabaseID(GlobalDatabaseID(db.scopeID.toLong)))
        byScope should equal(Some((db.parentScopeID, db.id)))
      }
    }

    "adds missing database lookup entries" in {
      val db = newDB
      val globals = Seq(db.scopeID, db.globalID)

      globals foreach { g =>
        (ctx ! Store.getDatabaseID(g)) shouldEqual Some(db.parentScopeID -> db.id)
      }

      val lookups =
        (ctx ! globals.map(Store.lookups(_).flattenT).sequence).flatten
      ctx ! lookups.map(LookupStore.remove(_)).join

      globals foreach { g =>
        (ctx ! Store.getDatabaseID(g)) shouldEqual None
      }

      val stats = statsBuf

      CassandraHelper.withStats(stats) {
        runTasks()
      }

      stats.countOrZero("Lookup.Repair.Events.Added") shouldEqual 2

      globals foreach { g =>
        (ctx ! Store.getDatabaseID(g)) shouldEqual Some(db.parentScopeID -> db.id)
      }

      ctx ! Store.removeUnmigrated(db.parentScopeID, db.id.toDocID)

      val stats2 = statsBuf

      CassandraHelper.withStats(stats2) {
        runTasks()
      }

      stats2.countOrZero("Lookup.Repair.Events.Added") should equal(2)
      globals foreach { g =>
        (ctx ! Store.getDatabaseID(g)) shouldEqual None
      }
    }

    "adds missing key lookup entries" in {
      val db = newDB
      val dbName = db.name

      val (_, key) = ctx ! mkKey(dbName)

      (ctx ! Store.keys(key.globalID).flattenT) should equal(
        List(key.parentScopeID -> key.id))

      ctx ! (Store.lookups(key.globalID) foreachValueT { entry =>
        LookupStore.remove(entry)
      })
      (ctx ! Store.keys(key.globalID).flattenT).isEmpty should be(true)

      val stats = statsBuf

      CassandraHelper.withStats(stats) {
        runTasks()
      }

      stats.countOrZero("Lookup.Repair.Events.Added") should equal(1)
      (ctx ! Store.keys(key.globalID).flattenT) should equal(
        List(key.parentScopeID -> key.id))

      ctx ! Store.removeUnmigrated(key.parentScopeID, key.id.toDocID)

      val stats2 = statsBuf

      CassandraHelper.withStats(stats2) {
        runTasks()
      }

      stats2.countOrZero("Lookup.Repair.Events.Added") should equal(1)
      (ctx ! Store.keys(key.globalID).flattenT).isEmpty should be(true)
    }

    "remove stale database lookup entries" in {
      val gID = ScopeID(1)
      val sID = ScopeID(1)
      val dbID = DatabaseID(1)

      ctx ! LookupStore.add(LiveLookup(gID, sID, dbID.toDocID, Unresolved, Add))
      (ctx ! Store.getDatabaseID(gID)) should equal(Some(sID -> dbID))

      val stats = statsBuf

      CassandraHelper.withStats(stats) {
        runTasks()
      }

      stats.countOrZero("Lookup.Repair.Events.Removed") should equal(1)
      (ctx ! Store.getDatabaseID(gID)) should be(None)
    }

    "remove stale key lookup entries" in {
      val gID = GlobalKeyID(1)
      val sID = ScopeID(1)
      val keyID = KeyID(1)

      ctx ! LookupStore.add(LiveLookup(gID, sID, keyID.toDocID, Unresolved, Add))
      (ctx ! Store.keys(gID).flattenT) should equal(List(sID -> keyID))

      val stats = statsBuf

      CassandraHelper.withStats(stats) {
        runTasks()
      }

      stats.countOrZero("Lookup.Repair.Events.Removed") should equal(1)
      (ctx ! Store.keys(gID).flattenT).isEmpty should be(true)
    }
  }
}
