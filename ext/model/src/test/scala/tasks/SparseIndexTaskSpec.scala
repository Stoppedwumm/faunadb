package fauna.model.test

import fauna.ast.{ DatabaseWriteConfig, EvalContext }
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

class SparseIndexTaskSpec extends Spec {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")
  val nameField = Field[String]("data", "name")

  def tasks(retainTime: Duration) = Seq(
    Mapper.Task(HistoricalIndex.sparseScan, SparseIndexTask(retainTime)(_)),
    Mapper.Task(SortedIndex.sparseScan, SparseIndexTask(retainTime)(_)))

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
    }

    def deleteIndex(idxName: String = idxName) = {
      ctx ! runQuery(auth, now, DeleteF(IndexRef(idxName)))
    }

    def deleteDB() = {
      ctx ! runQuery(parentAuth, now, DeleteF(DatabaseRef(db.name)))
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

  def withNativeDocumentsIndex(test: NativeIndexID => Unit): Unit = {
    val indexes =
      Seq(
        NativeIndexID.DocumentsByCollection,
        NativeIndexID.ChangesByCollection
      )

    indexes foreach { nativeIndex =>
      withClue(s"Using $nativeIndex") {
        test(nativeIndex)
      }
    }
  }

  "SparseIndexTask" - {
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

    "after a Database is deleted" - {
      "purges index entries in the deleted DB" in {
        val f = new Fixture
        import f._

        val doc = insertDoc("Ravi")

        clock.advance(1.minute)
        deleteDB()

        getVersions(doc.id).size should equal(1)
        getIndexEntries("Ravi").size should equal(1)

        clock.advance(1.minute)
        runTasks(ctx, clock, tasks(0.seconds))

        getIndexEntries("Ravi").size should equal(0)
      }

      "does not purge index entries before the retention period is over" in {
        val f = new Fixture
        import f._

        val doc = insertDoc("Alex")

        clock.advance(1.minute)
        deleteDB()

        getVersions(doc.id).size should equal(1)
        getIndexEntries("Alex").size should equal(1)

        clock.advance(13.days)
        runTasks(ctx, clock, tasks(14.days))

        getVersions(doc.id).size should equal(1)
        getIndexEntries("Alex").size should equal(1)
      }

      "purges index entries in child DBs of the deleted DB" in {
        val f = new Fixture
        import f._

        val child = newChild()
        val doc = child.insertDoc("Susan")

        clock.advance(1.minute)
        deleteDB()

        child.getVersions(doc.id).size should equal(1)
        child.getIndexEntries("Susan").size should equal(1)

        clock.advance(1.minute)
        runTasks(ctx, clock, tasks(0.seconds))

        child.getIndexEntries("Susan").size should equal(0)
      }

      "does not purge index entries in other DBs" in {
        val f = new Fixture
        import f._

        val other = new Fixture(parentClock = Some(clock))
        val doc = other.insertDoc("Vic")

        clock.advance(1.minute)
        deleteDB()

        other.getVersions(doc.id).size should equal(1)
        other.getIndexEntries("Vic").size should equal(1)

        clock.advance(1.minute)
        runTasks(ctx, clock, tasks(0.seconds))

        other.getVersions(doc.id).size should equal(1)
        other.getIndexEntries("Vic").size should equal(1)
      }

      "purges native index entries within the deleted DB" - {
        "CollectionByName" in {
          val f = new Fixture
          import f._

          val collectionByNameIdx =
            (ctx ! Index.getConfig(scope, NativeIndexID.CollectionByName.id)).get

          getIndexEntries("cls", collectionByNameIdx).size should equal(1)

          clock.advance(1.minute)
          deleteDB()

          getIndexEntries("cls", collectionByNameIdx).size should equal(1)

          clock.advance(1.minute)
          runTasks(ctx, clock, tasks(0.seconds))

          getIndexEntries("cls", collectionByNameIdx).size should equal(0)
        }

        "IndexByName" in {
          val f = new Fixture
          import f._

          val indexByNameIdx =
            (ctx ! Index.getConfig(scope, NativeIndexID.IndexByName.id)).get

          getIndexEntries("idx", indexByNameIdx).size should equal(1)

          clock.advance(1.minute)
          deleteDB()

          getIndexEntries("idx", indexByNameIdx).size should equal(1)

          clock.advance(1.minute)
          runTasks(ctx, clock, tasks(0.seconds))

          getIndexEntries("idx", indexByNameIdx).size should equal(0)
        }

        "Native documents indexes" in withNativeDocumentsIndex { index =>
          val f = new Fixture
          import f._

          val doc = insertDoc("Hodor")
          val documentsByCollectionIdx = (ctx ! Index.getConfig(scope, index.id)).get

          getIndexEntries(
            doc.id.collID.toDocID,
            documentsByCollectionIdx).size should equal(1)

          clock.advance(1.minute)
          deleteDB()

          getIndexEntries(
            doc.id.collID.toDocID,
            documentsByCollectionIdx).size should equal(1)

          clock.advance(1.minute)
          runTasks(ctx, clock, tasks(0.seconds)) should be(true)

          getIndexEntries(
            doc.id.collID.toDocID,
            documentsByCollectionIdx).size should equal(0)
        }
      }

      "does not purge native index entries that point to the deleted DB document" - {
        "DatabaseByName" in {
          val f = new Fixture
          import f._

          val databaseByNameIdx =
            (ctx ! Index.getConfig(scope, NativeIndexID.DatabaseByName.id)).get
          val child = newChild()

          getIndexEntries(child.db.name, databaseByNameIdx).size should equal(1)

          clock.advance(1.minute)
          child.deleteDB()

          // The index will have an additional 'remove' event.
          getIndexEntries(child.db.name, databaseByNameIdx).size should equal(2)

          clock.advance(1.minute)
          runTasks(ctx, clock, tasks(0.seconds))

          getIndexEntries(child.db.name, databaseByNameIdx).size should equal(2)
        }

        "DatabaseByDisabled" in {
          val f = new Fixture
          import f._

          val databaseByDisabledIdx =
            (ctx ! Index.getConfig(scope, NativeIndexID.DatabaseByDisabled.id)).get
          val child = newChild()

          getIndexEntries(true, databaseByDisabledIdx).size should equal(0)

          clock.advance(1.minute)
          val ec = EvalContext.write(auth, now, APIVersion.Default)
          ctx ! DatabaseWriteConfig.Default.disable(ec, child.db.id.toDocID)

          getIndexEntries(true, databaseByDisabledIdx).size should equal(1)

          clock.advance(1.minute)
          child.deleteDB()

          // The index will have an additional 'remove' event.
          getIndexEntries(true, databaseByDisabledIdx).size should equal(2)

          clock.advance(1.minute)
          runTasks(ctx, clock, tasks(0.seconds))

          getIndexEntries(true, databaseByDisabledIdx).size should equal(2)
        }

        "DocumentsByCollection (for the Databases collection)" in {
          val f = new Fixture
          import f._

          val documentsByCollectionIdx = (ctx ! Index.getConfig(
            scope,
            NativeIndexID.DocumentsByCollection.id)).get
          val child = newChild()

          getIndexEntries(
            DatabaseID.collID.toDocID,
            documentsByCollectionIdx).size should equal(1)

          clock.advance(1.minute)
          child.deleteDB()

          // The index will have an additional 'remove' event.
          getIndexEntries(
            DatabaseID.collID.toDocID,
            documentsByCollectionIdx).size should equal(2)

          clock.advance(1.minute)
          runTasks(ctx, clock, tasks(0.seconds))

          getIndexEntries(
            DatabaseID.collID.toDocID,
            documentsByCollectionIdx).size should equal(2)
        }
      }

      "does not purge native index entries for child DB documents inside the deleted DB" - {
        "DatabaseByName" in {
          val f = new Fixture
          import f._

          val databaseByNameIdx =
            (ctx ! Index.getConfig(scope, NativeIndexID.DatabaseByName.id)).get
          val child = newChild()

          getIndexEntries(child.db.name, databaseByNameIdx).size should equal(1)

          clock.advance(1.minute)
          deleteDB()

          // The index will have an additional 'remove' event.
          getIndexEntries(child.db.name, databaseByNameIdx).size should equal(2)

          clock.advance(1.minute)
          runTasks(ctx, clock, tasks(0.seconds))

          getIndexEntries(child.db.name, databaseByNameIdx).size should equal(2)
        }

        "DatabaseByDisabled" in {
          val f = new Fixture
          import f._

          val databaseByDisabledIdx =
            (ctx ! Index.getConfig(scope, NativeIndexID.DatabaseByDisabled.id)).get
          val child = newChild()

          getIndexEntries(true, databaseByDisabledIdx).size should equal(0)

          clock.advance(1.minute)
          val ec = EvalContext.write(auth, now, APIVersion.Default)
          ctx ! DatabaseWriteConfig.Default.disable(ec, child.db.id.toDocID)

          getIndexEntries(true, databaseByDisabledIdx).size should equal(1)

          clock.advance(1.minute)
          deleteDB()

          // The index will have an additional 'remove' event.
          getIndexEntries(true, databaseByDisabledIdx).size should equal(2)

          clock.advance(1.minute)
          runTasks(ctx, clock, tasks(0.seconds))

          getIndexEntries(true, databaseByDisabledIdx).size should equal(2)
        }

        "DocumentsByCollection (for the Databases collection)" in {
          val f = new Fixture
          import f._

          val documentsByCollectionIdx = (ctx ! Index.getConfig(
            scope,
            NativeIndexID.DocumentsByCollection.id)).get
          newChild()

          getIndexEntries(
            DatabaseID.collID.toDocID,
            documentsByCollectionIdx).size should equal(1)

          clock.advance(1.minute)
          deleteDB()

          // The index will have an additional 'remove' event.
          getIndexEntries(
            DatabaseID.collID.toDocID,
            documentsByCollectionIdx).size should equal(2)

          clock.advance(1.minute)
          runTasks(ctx, clock, tasks(0.seconds))

          getIndexEntries(
            DatabaseID.collID.toDocID,
            documentsByCollectionIdx).size should equal(2)
        }
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

      "index entries from the native documents indexes" - {
        "are purged for the deleted Collection" in withNativeDocumentsIndex {
          index =>
            val f = new Fixture
            import f._

            val doc = insertDoc("Peregrin")
            val documentsByCollectionIdx =
              (ctx ! Index.getConfig(scope, index.id)).get

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

        "are not purged before the retention period is over" in withNativeDocumentsIndex {
          index =>
            val f = new Fixture
            import f._

            val doc = insertDoc("Meriadoc")
            val documentsByCollectionIdx =
              (ctx ! Index.getConfig(scope, index.id)).get

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

        "are not purged for Collections that are not deleted" in withNativeDocumentsIndex {
          index =>
            val f = new Fixture
            import f._

            setupCollection("other")
            val doc = insertDoc("Rosie") // in Collection("cls")
            val documentsByCollectionIdx =
              (ctx ! Index.getConfig(scope, index.id)).get

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

    "after an Index is deleted" - {
      "purges index entries for the deleted Index" in {
        val f = new Fixture
        import f._

        val doc = insertDoc("Morty")

        clock.advance(1.minute)
        deleteIndex()

        getVersions(doc.id).size should equal(1)
        getIndexEntries("Morty").size should equal(1)

        clock.advance(1.minute)
        runTasks(ctx, clock, tasks(0.seconds))

        getVersions(doc.id).size should equal(1)
        getIndexEntries("Morty").size should equal(0)
      }

      "does not purge index entries before the retention period is over" in {
        val f = new Fixture
        import f._

        val doc = insertDoc("Summer")

        clock.advance(1.minute)
        deleteIndex()

        getVersions(doc.id).size should equal(1)
        getIndexEntries("Summer").size should equal(1)

        clock.advance(13.days)
        runTasks(ctx, clock, tasks(14.days))

        getVersions(doc.id).size should equal(1)
        getIndexEntries("Summer").size should equal(1)
      }

      "does not purge index entries for other Indexes" in {
        val f = new Fixture
        import f._

        setupIndex("other")
        val doc = insertDoc("Beth")

        clock.advance(1.minute)
        deleteIndex("other")

        getVersions(doc.id).size should equal(1)
        getIndexEntries("Beth").size should equal(1)

        clock.advance(1.minute)
        runTasks(ctx, clock, tasks(0.seconds))

        getVersions(doc.id).size should equal(1)
        getIndexEntries("Beth").size should equal(1)
      }
    }
  }
}
