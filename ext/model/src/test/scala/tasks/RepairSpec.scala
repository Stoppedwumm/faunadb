package fauna.model.test

import fauna.ast._
import fauna.atoms._
import fauna.auth.Auth
import fauna.codex.json.JSArray
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.model._
import fauna.model.schema.CollectionConfig
import fauna.model.tasks._
import fauna.repo._
import fauna.repo.store._
import fauna.repo.test.CassandraHelper
import fauna.storage._
import fauna.storage.doc._
import fauna.storage.lookup._
import fauna.storage.ops.Write
import org.scalatest.PrivateMethodTester._
import scala.concurrent.duration._

class RepairSpec extends Spec {
  import SocialHelpers._
  Write invokePrivate PrivateMethod[Unit](Symbol("mungeOffset"))(0.millis)

  val ctx = CassandraHelper.context("model")

  "RepairTask" - {

    val exec = TaskExecutor(ctx)

    val scope = ctx ! newScope
    val auth = Auth.forScope(scope)

    def tasks =
      Task
        .getAllRunnable()
        .flattenT

    def processAllTasks() =
      while ((ctx ! tasks).nonEmpty) exec.step()

    "adds missing database lookup entries" in {
      val dbID = (ctx ! Database.forScope(scope)).get.id

      (ctx ! Store.getDatabaseID(scope)).get should equal(
        Database.RootScopeID -> dbID)
      ctx ! (Store.lookups(scope) foreachValueT { entry =>
        LookupStore.remove(entry)
      })

      (ctx ! Store.getDatabaseID(scope)) should be(None)

      ctx ! Repair.RootTask.createOpt(Clock.time, Repair.Commit, Repair.Model)
      processAllTasks()

      (ctx ! Store.getDatabaseID(scope)).get should equal(
        Database.RootScopeID -> dbID)
    }

    "adds missing database lookup entries filtering by scope" in {
      val scope1 = ctx ! newScope
      val adminAuth = Auth.adminForScope(scope1)
      val scope2 = ctx ! newScope(adminAuth, APIVersion.Default)

      val dbID1 = (ctx ! Database.forScope(scope1)).get
      val dbID2 = (ctx ! Database.forScope(scope2)).get

      (ctx ! Store.getDatabaseID(scope1)).get should equal(
        Database.RootScopeID -> dbID1.id)
      (ctx ! Store.getDatabaseID(scope2)).get should equal(scope1 -> dbID2.id)

      //remove the database entries
      ctx ! (Store.lookups(scope1) foreachValueT { entry =>
        LookupStore.remove(entry)
      })

      ctx ! (Store.lookups(scope2) foreachValueT { entry =>
        LookupStore.remove(entry)
      })

      //make sure lookup doesn't return the entries
      (ctx ! Store.getDatabaseID(scope1)) should be(None)
      (ctx ! Store.getDatabaseID(scope2)) should be(None)

      //repair non related scope
      ctx ! Repair.RootTask.createOpt(Clock.time, Repair.Commit, Repair.Model, Some(ScopeID(10)))
      processAllTasks()

      (ctx ! Store.getDatabaseID(scope1)) should be(None)
      (ctx ! Store.getDatabaseID(scope2)) should be(None)

      //repair the scope
      ctx ! Repair.RootTask.createOpt(Clock.time, Repair.Commit, Repair.Model, Some(scope1))
      processAllTasks()

      //make sure lookup only return the database from the repaired scope
      (ctx ! Store.getDatabaseID(scope1)) should be(None)
      (ctx ! Store.getDatabaseID(scope2)).get should equal(scope1 -> dbID2.id)
    }

    "adds missing key lookup entries" in {
      val dbName = (ctx ! Database.forScope(scope)).get.name
      val (_, key) = ctx ! mkKey(dbName)

      (ctx ! Store.keys(key.globalID).flattenT) should equal(
        List(Database.RootScopeID -> key.id))

      ctx ! (Store.lookups(key.globalID) foreachValueT { entry =>
        LookupStore.remove(entry)
      })
      (ctx ! Store.keys(key.globalID).flattenT).isEmpty should be(true)

      ctx ! Repair.RootTask.createOpt(Clock.time, Repair.Commit, Repair.Model)
      processAllTasks()

      (ctx ! Store.keys(key.globalID).flattenT) should equal(
        List(Database.RootScopeID -> key.id))
    }

    "adds missing key lookup entries filtering by scope" in {
      val scope1 = ctx ! newScope
      val adminAuth = Auth.adminForScope(scope1)
      val scope2 = ctx ! newScope(adminAuth, APIVersion.Default)

      val dbName1 = (ctx ! Database.forScope(scope1)).get.name
      val dbName2 = (ctx ! Database.forScope(scope2)).get.name

      val (_, key1) = ctx ! mkKey(dbName1)
      val (_, key2) = ctx ! mkKey(dbName2, auth = adminAuth)

      (ctx ! Store.keys(key1.globalID).flattenT) should equal(
        List(Database.RootScopeID -> key1.id))

      (ctx ! Store.keys(key2.globalID).flattenT) should equal(
        List(scope1 -> key2.id))

      //remove the keys
      ctx ! (Store.lookups(key1.globalID) foreachValueT { entry =>
        LookupStore.remove(entry)
      })

      ctx ! (Store.lookups(key2.globalID) foreachValueT { entry =>
        LookupStore.remove(entry)
      })

      //make sure lookup doesn't return the keys
      (ctx ! Store.keys(key1.globalID).flattenT) should equal(List.empty)
      (ctx ! Store.keys(key2.globalID).flattenT) should equal(List.empty)

      //repair non related scope
      ctx ! Repair.RootTask.createOpt(Clock.time, Repair.Commit, Repair.Model, Some(ScopeID(10)))
      processAllTasks()

      (ctx ! Store.keys(key1.globalID).flattenT) should equal(List.empty)
      (ctx ! Store.keys(key2.globalID).flattenT) should equal(List.empty)

      //repair the scope
      ctx ! Repair.RootTask.createOpt(Clock.time, Repair.Commit, Repair.Model, Some(scope1))
      processAllTasks()

      //make sure lookup only return the key from the repaired scope
      (ctx ! Store.keys(key1.globalID).flattenT) should equal(List.empty)
      (ctx ! Store.keys(key2.globalID).flattenT) should equal(
        List(scope1 -> key2.id))
    }

    "remove stale database lookup entries" in {
      val gID = ScopeID(1)
      val sID = ScopeID(2)
      val dbID = DatabaseID(1)

      ctx ! LookupStore.add(LiveLookup(gID, sID, dbID.toDocID, Unresolved, Add))
      (ctx ! Store.getDatabaseID(gID)) should equal(Some(sID -> dbID))

      ctx ! Repair.RootTask.createOpt(Clock.time, Repair.Commit, Repair.Model)
      processAllTasks()

      (ctx ! Store.getDatabaseID(gID)) should be(None)
    }

    "remove stale database lookup entries filtering by scope" in {
      val gID = ScopeID(1)
      val sID = ScopeID(2)
      val dbID = DatabaseID(1)

      ctx ! LookupStore.add(LiveLookup(gID, sID, dbID.toDocID, Unresolved, Add))
      (ctx ! Store.getDatabaseID(gID)) should equal(Some(sID -> dbID))

      //repair non related scope should not affect the entry
      ctx ! Repair.RootTask.createOpt(Clock.time, Repair.Commit, Repair.Model, Some(ScopeID(10)))
      processAllTasks()

      (ctx ! Store.getDatabaseID(gID)) should equal(Some(sID -> dbID))

      //repair gID should affect only its entries
      ctx ! Repair.RootTask.createOpt(Clock.time, Repair.Commit, Repair.Model, Some(gID))
      processAllTasks()

      (ctx ! Store.getDatabaseID(gID)) should be(None)
    }

    "remove stale key lookup entries" in {
      val gID = GlobalKeyID(1)
      val sID = ScopeID(1)
      val keyID = KeyID(1)

      ctx ! LookupStore.add(LiveLookup(gID, sID, keyID.toDocID, Unresolved, Add))
      (ctx ! Store.keys(gID).flattenT) should equal(List(sID -> keyID))

      ctx ! Repair.RootTask.createOpt(Clock.time, Repair.Commit, Repair.Model)
      processAllTasks()

      (ctx ! Store.keys(gID).flattenT).isEmpty should be(true)
    }

    "remove stale key lookup entries filtering by scope" in {
      val gID = GlobalKeyID(1)
      val sID = ScopeID(2)
      val keyID = KeyID(1)

      ctx ! LookupStore.add(LiveLookup(gID, sID, keyID.toDocID, Unresolved, Add))
      (ctx ! Store.keys(gID).flattenT) should equal(List(sID -> keyID))

      //repair non related scope should not affect the entry
      ctx ! Repair.RootTask.createOpt(Clock.time, Repair.Commit, Repair.Model, Some(ScopeID(10)))
      processAllTasks()

      (ctx ! Store.keys(gID).flattenT) should equal(List(sID -> keyID))

      //repair gID should affect only its entries
      ctx ! Repair.RootTask.createOpt(Clock.time, Repair.Commit, Repair.Model, Some(ScopeID(gID.toLong)))
      processAllTasks()

      (ctx ! Store.keys(gID).flattenT) should equal(List.empty)
    }

    "removes its journal entry" in {
      val repairQ = ctx ! Repair.RootTask.createOpt(Clock.time, Repair.Commit, Repair.Model)
      repairQ.isEmpty should be(false)

      val before = ctx ! JournalEntry.readByTag(Repair.EntryTag, Clock.time).flattenT
      before.isEmpty should be(false)

      processAllTasks()

      val after = ctx ! JournalEntry.readByTag(Repair.EntryTag, Clock.time).flattenT
      after.isEmpty should be(true)
    }

    "should not abort repair if bindings returns invalid value" in {
      val collField = Field[String]("data", "collection")

      val cls = ctx ! mkCollection(auth, MkObject("name" -> "unresolved-coll"))

      ctx ! runQuery(auth, CreateIndex(MkObject(
        "name" -> "unresolved-idx",
        "active" -> true,
        "source" -> MkObject(
          "collection" -> ClassRef("unresolved-coll"),
          "fields" -> MkObject(
            "build-ref" -> QueryF(Lambda("doc" ->
              MkRef(ClassRef(Select(JSArray("data", "collection"), Var("doc"))), "123")
            ))
          )
        ),
        "values" -> JSArray(
          MkObject("field" -> "ref"),
          MkObject("binding" -> "build-ref")
        )
      )))

      val schema = ctx ! CollectionConfig(scope, cls).map { _.get.Schema }

      ctx ! Store.insertCreate(schema, ID(1, cls), TS(1), Data(collField -> "unresolved-coll"))
      ctx ! Store.insertCreate(schema, ID(2, cls), TS(1), Data(collField -> "non-existent-collection"))

      val idxQ = collection(auth, Match(IndexRef("unresolved-idx")))

      val expected1 = ArrayL(List(RefL(scope, ID(1, cls)), RefL(scope, DocID(SubID(123), cls))))
      val expected2 = ArrayL(List(RefL(scope, ID(2, cls)), NullL))

      (ctx ! idxQ).elems shouldBe List(expected1, expected2)

      ctx ! Repair.RootTask.createOpt(Clock.time, Repair.Commit, Repair.Model)

      processAllTasks()

      (ctx ! idxQ).elems shouldBe List(expected1, expected2)
    }
  }
}
