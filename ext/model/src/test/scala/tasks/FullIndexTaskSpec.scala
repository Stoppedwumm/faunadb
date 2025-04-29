package fauna.model.test

import fauna.atoms._
import fauna.auth.{ Auth, RootAuth }
import fauna.lang.clocks._
import fauna.lang.syntax._
import fauna.model._
import fauna.model.tasks._
import fauna.repo._
import fauna.repo.schema.CollectionSchema
import fauna.repo.store._
import fauna.repo.test.CassandraHelper
import fauna.storage.doc._
import fauna.storage.index.{ IndexTerm, NativeIndexID }
import fauna.storage.ir._
import scala.concurrent.duration._

class FullIndexTaskSpec extends Spec {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")
  val nameField = Field[String]("data", "name")

  def tasks(retainTime: Duration) = Seq(
    Mapper.Task(
      HistoricalIndex.elementScan,
      FullIndexTask(retainTime, HistoricalIndex.tombstone)(_)),
    Mapper.Task(
      SortedIndex.elementScan,
      FullIndexTask(retainTime, SortedIndex.tombstone)(_))
  )

  // This test fixture sets up a Database with a Collection, "cls", and an Index,
  // "idx", on Collection("cls") which covers the "data.name" field.
  class Fixture(
    parentAuth: Auth = RootAuth,
    clsName: String = "cls",
    idxName: String = "idx",
    parentClock: Option[TestClock] = None) {
    val scope = ctx ! newScope(parentAuth)
    val db = (ctx ! Database.forScope(scope)).get
    val auth = Auth.adminForScope(scope)

    val cls = setupCollection()
    val idx = setupIndex()

    val setupTS = (ctx ! getInstance(auth, ClassRef(clsName), Clock.time)).ts.validTS
    val clock = parentClock getOrElse new TestClock(setupTS)

    def now = clock.time

    def setupCollection(name: String = clsName) =
      ctx ! mkCollection(auth, MkObject("name" -> name))

    def setupIndex(idxName: String = idxName, clsName: String = clsName) =
      ctx ! mkIndex(auth, idxName, clsName, List(nameField.path))

    def deleteCollection(clsName: String = clsName) = {
      ctx ! runQuery(auth, now, DeleteF(ClassRef(clsName)))
      ctx.cacheContext.schema.invalidate()
    }

    def deleteIndex(idxName: String = idxName) = {
      ctx ! runQuery(auth, now, DeleteF(IndexRef(idxName)))
      ctx.cacheContext.schema.invalidate()
    }

    def deleteDB() = {
      ctx ! runQuery(parentAuth, now, DeleteF(DatabaseRef(db.name)))
      ctx.cacheContext.schema.invalidate()
    }

    def insertDoc(name: String, clsName: String = clsName) =
      ctx ! mkDoc(auth, clsName, MkObject("data" -> MkObject("name" -> name)))

    def getIndexEntries(term: IRValue, idx: IndexConfig = idx) =
      ctx ! Store.historicalIndex(idx, Vector(IndexTerm(term))).flattenT

    def getVersions(docID: DocID) =
      ctx ! Store
        .versions(CollectionSchema.empty(scope, docID.collID), docID)
        .flattenT

    def newChild(clsName: String = clsName, idxName: String = idxName) =
      new Fixture(auth, clsName, idxName, Some(clock))
  }

  "FullIndexTask" - {
    // These tasks commit their side-effects non-transactionally, and
    // must see all data on all hosts.
    "should scan all local segments" in {
      val sorted = new FullSortedIndexScanner(
        ctx,
        retainTime = 1.minute,
        totalTime = 7.days,
        timeout = 5.minutes)

      val historical = new FullSortedIndexScanner(
        ctx,
        retainTime = 1.minute,
        totalTime = 7.days,
        timeout = 5.minutes)

      sorted.primarySegmentsOnly should be(false)
      historical.primarySegmentsOnly should be(false)
    }

    "after no schema objects are deleted" - {
      "does not purge any index entries" in {
        val f = new Fixture
        import f._

        val doc = insertDoc("Steve")

        getVersions(doc.id).size should equal(1)
        getIndexEntries("Steve").size should equal(1)

        clock.advance(1.minute)
        runTasks(ctx, clock, tasks(0.seconds))

        getVersions(doc.id).size should equal(1)
        getIndexEntries("Steve").size should equal(1)
      }
    }

    "after a Collection is deleted" - {
      "purges index entries for documents in the deleted Collection" in {
        val f = new Fixture
        import f._

        val doc = insertDoc("Samwise")

        clock.advance(1.minute)
        deleteCollection()

        getVersions(doc.id).size should equal(1)
        getIndexEntries("Samwise").size should equal(1)

        clock.advance(1.minute)
        runTasks(ctx, clock, tasks(0.seconds))

        getIndexEntries("Samwise").size should equal(0)
      }

      "does not purge index entries before the retention period is over" in {
        val f = new Fixture
        import f._

        val doc = insertDoc("Julia")

        clock.advance(1.minute)
        deleteCollection()

        getVersions(doc.id).size should equal(1)
        getIndexEntries("Julia").size should equal(1)

        clock.advance(13.days)
        runTasks(ctx, clock, tasks(14.days))

        getVersions(doc.id).size should equal(1)
        getIndexEntries("Julia").size should equal(1)
      }

      "does not purge index entries for documents in other Collections" in {
        val f = new Fixture
        import f._

        setupCollection("other")
        val doc = insertDoc("Frodo") // in Collection("cls")

        clock.advance(1.minute)
        deleteCollection("other")

        getVersions(doc.id).size should equal(1)
        getIndexEntries("Frodo").size should equal(1)

        clock.advance(1.minute)
        runTasks(ctx, clock, tasks(0.seconds))

        getVersions(doc.id).size should equal(1)
        getIndexEntries("Frodo").size should equal(1)
      }

      "index entries from the DocumentsByCollection native index" - {
        "are purged for the deleted Collection" in {
          val f = new Fixture
          import f._

          val doc = insertDoc("Peregrin")
          val documentsByCollectionIdx =
            (ctx ! Index.getConfig(
              scope,
              NativeIndexID.DocumentsByCollection.id)).get

          getIndexEntries(
            doc.id.collID.toDocID,
            documentsByCollectionIdx).size should equal(1)

          clock.advance(1.minute)
          deleteCollection()

          getIndexEntries(
            doc.id.collID.toDocID,
            documentsByCollectionIdx).size should equal(1)

          clock.advance(1.minute)
          runTasks(ctx, clock, tasks(0.seconds))

          getIndexEntries(
            doc.id.collID.toDocID,
            documentsByCollectionIdx).size should equal(0)
        }

        "are not purged before the retention period is over" in {
          val f = new Fixture
          import f._

          val doc = insertDoc("Meriadoc")
          val documentsByCollectionIdx =
            (ctx ! Index.getConfig(
              scope,
              NativeIndexID.DocumentsByCollection.id)).get

          getIndexEntries(
            doc.id.collID.toDocID,
            documentsByCollectionIdx).size should equal(1)

          clock.advance(1.minute)
          deleteCollection()

          getIndexEntries(
            doc.id.collID.toDocID,
            documentsByCollectionIdx).size should equal(1)

          clock.advance(13.days)
          runTasks(ctx, clock, tasks(14.days))

          getIndexEntries(
            doc.id.collID.toDocID,
            documentsByCollectionIdx).size should equal(1)
        }

        "are not purged for Collections that are not deleted" in {
          val f = new Fixture
          import f._

          setupCollection("other")
          val doc = insertDoc("Rosie") // in Collection("cls")
          val documentsByCollectionIdx =
            (ctx ! Index.getConfig(
              scope,
              NativeIndexID.DocumentsByCollection.id)).get

          getIndexEntries(
            doc.id.collID.toDocID,
            documentsByCollectionIdx).size should equal(1)

          clock.advance(1.minute)
          deleteCollection("other")

          getIndexEntries(
            doc.id.collID.toDocID,
            documentsByCollectionIdx).size should equal(1)

          clock.advance(1.minute)
          runTasks(ctx, clock, tasks(0.seconds))

          getIndexEntries(
            doc.id.collID.toDocID,
            documentsByCollectionIdx).size should equal(1)
        }
      }
    }
  }
}
