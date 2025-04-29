package fauna.model.test

import fauna.ast._
import fauna.atoms._
import fauna.auth.{ AdminPermissions, Auth }
import fauna.codex.json.JSArray
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.model._
import fauna.model.schema.NativeIndex
import fauna.model.schema.SchemaCollection
import fauna.model.tasks._
import fauna.repo._
import fauna.repo.cassandra.CassandraService
import fauna.repo.query.Query
import fauna.repo.service.rateLimits.FixedWriteOpsLimiter
import fauna.repo.test.CassandraHelper
import fauna.storage.doc.Data
import fauna.storage.index.{ IndexTerm, IndexValue }
import fauna.storage.ir._
import org.scalatest.tags.Slow
import org.scalatest.Inside
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.util.Random

object TaskHelpers {
  import fauna.prop.api.DefaultQueryHelpers._

  def processAllTasks(ctx: RepoContext, exec: Executor): Unit =
    while ((ctx ! tasks).nonEmpty) {
      exec.step()
    }

  def rootTask(ctx: RepoContext): Task =
    ((ctx ! tasks) find { _.name == IndexBuild.RootTask.name }).get

  def task(ctx: RepoContext, id: TaskID) = (ctx ! Task.get(id)).get

  def toggleIndexActive(
    ctx: RepoContext,
    auth: Auth,
    idx: Index,
    active: Boolean) = {
    ctx ! SocialHelpers.runQuery(
      auth,
      Update(IndexRef(idx.name), MkObject("active" -> active)))
    ctx.cacheContext.schema.invalidate()
  }

  def tasks: Query[Iterable[Task]] =
    Task.getRunnableByHost(CassandraService.instance.localID.get).flattenT
}

@Slow
class TableScanSpec extends Spec with Inside {
  import SocialHelpers._
  import TaskHelpers._

  "IndexBuildSpec (table scan)" - {
    val ctx = CassandraHelper.context("model")

    val exec = TaskExecutor(ctx)
    val scope = ctx ! newScope
    val auth = Auth.forScope(scope)

    "transitions" in {
      ctx ! mkCollection(auth, MkObject("name" -> "people"))

      // Avoid the synchronous build path.
      for (_ <- 0 until Index.BuildSyncSize + 1) {
        ctx ! mkDoc(auth, "people")
      }

      processAllTasks(ctx, exec) // process class invalidation task

      ctx ! mkIndex(
        auth,
        "ages",
        "people",
        List(List("data", "age")),
        active = false)

      // root task
      (ctx ! tasks).size should equal(1)
      val root = rootTask(ctx)
      root.data(IndexBuild.StateField) should equal("build")

      (ctx ! tasks).size should equal(1)
      inside(task(ctx, root.id).state) { case Task.Runnable(data, _) =>
        data(IndexBuild.StateField) should equal("build")
      }

      // fork build
      exec.step()
      (ctx ! tasks).size should equal(1)
      inside(task(ctx, root.id).state) { case Task.Blocked(ids, data, _) =>
        ids.size should equal(1)
        data(IndexBuild.StateField) should equal("activate")
      }

      // exec build
      processAllTasks(ctx, exec)

      val idxQ = runQuery(auth, Clock.time, Get(Ref("indexes/ages"))) flatMap toIndex

      (ctx ! idxQ).isActive should be(true)
      (ctx ! tasks).isEmpty should be(true)
    }

    "test slow index builds" in {
      ctx ! mkCollection(auth, MkObject("name" -> "plays"))
      val inst1 = ctx ! mkDoc(
        auth,
        "plays",
        params = MkObject(
          "data" -> MkObject("name" -> "Romeo and Juliet", "genre" -> "tragedy")))
      val inst2 = ctx ! mkDoc(
        auth,
        "plays",
        params =
          MkObject("data" -> MkObject("name" -> "King Lear", "genre" -> "tragedy")))

      ctx ! mkIndex(
        auth,
        "plays_by_genres",
        "plays",
        List(List("data", "genre")),
        active = false)

      processAllTasks(ctx, exec)

      val coll =
        ctx ! collection(auth, Match(Ref("indexes/plays_by_genres"), "tragedy"))
      coll.elems should equal(Seq(RefL(scope, inst1.id), RefL(scope, inst2.id)))
    }

    "ensures presence of both existing and new instances in scan build" in {
      ctx ! mkCollection(auth, MkObject("name" -> "foo"))

      processAllTasks(ctx, exec) // process other tasks

      val existingInst = ctx ! mkDoc(
        auth,
        "foo",
        params = MkObject(
          "data" ->
            MkObject("name" -> "Alex")))

      // Avoid the synchronous build path.
      for (_ <- 0 until Index.BuildSyncSize) {
        ctx ! mkDoc(auth, "foo")
      }

      val idx = ctx ! mkIndex(
        auth,
        "foo_by_name",
        "foo",
        List(List("data", "name")),
        active = false)

      // invalidate by source schema
      ctx.cacheContext.schema.invalidate()

      val newInst = ctx ! mkDoc(
        auth,
        "foo",
        params = MkObject(
          "data" ->
            MkObject("name" -> "Alex")))

      val newInstOnly = Seq(newInst) map { i => RefL(scope, i.id) }
      val bothInstances = Seq(existingInst, newInst) map { i => RefL(scope, i.id) }

      val q = Match(Ref("indexes/foo_by_name"), "Alex")

      val root = rootTask(ctx)

      // toggle active on and back off again to test the index in
      // partially-constructed state
      toggleIndexActive(ctx, auth, idx, true)
      (ctx ! collection(auth, q)).elems should equal(newInstOnly)
      toggleIndexActive(ctx, auth, idx, false)

      task(ctx, root.id).data(IndexBuild.StateField) should equal("build")
      processAllTasks(ctx, exec)

      (ctx ! collection(auth, q)).elems should equal(bothInstances)
    }

    "all sources index must scan all instances" in {
      ctx ! mkCollection(auth, MkObject("name" -> "foo1"))
      ctx ! mkCollection(auth, MkObject("name" -> "foo2"))
      val inst1 = ctx ! mkDoc(
        auth,
        "foo1",
        params = MkObject("data" -> MkObject("nom" -> "Alex")))
      val inst2 = ctx ! mkDoc(
        auth,
        "foo2",
        params = MkObject("data" -> MkObject("nom" -> "Alex")))

      ctx ! allSourcesIndex(
        auth,
        "all-sources",
        terms = List(List("data", "nom")),
        active = false)
      val allInstances = Seq(inst1, inst2) map { i => RefL(scope, i.id) }
      val q = Match(Ref("indexes/all-sources"), "Alex")

      processAllTasks(ctx, exec)

      (ctx ! collection(auth, q)).elems should equal(allInstances)
    }

    "all source termless index should not index native collections" in {
      val scope = ctx ! newScope
      val auth = Auth.forScope(scope)

      ctx ! mkCollection(auth, MkObject("name" -> "foo"))
      ctx ! allSourcesIndex(auth, "all_sources_table_scan", active = false)

      processAllTasks(ctx, exec)

      val inst = ctx ! mkDoc(auth, "foo", params = MkObject("data" -> MkObject()))

      processAllTasks(ctx, exec)

      val elems =
        (ctx ! collection(auth, Match(Ref("indexes/all_sources_table_scan")))).elems

      elems shouldBe List(RefL(scope, inst.id))
    }
  }
}

@Slow
class IndexScanSpec extends Spec with Inside {
  import SocialHelpers._
  import TaskHelpers._

  "IndexBuildSpec (index scan)" - {
    val ctx = CassandraHelper.context("model")
    val exec = TaskExecutor(ctx)

    val scope = ctx ! newScope
    val auth = Auth.forScope(scope).withPermissions(AdminPermissions)

    "create index with deleted instances should returns all other elements" in {
      ctx ! mkCollection(auth, MkObject("name" -> "data", "history_days" -> 30))

      val numInstances = 2 * BulkPageSize + 50
      val deleteInstances = 20

      val instances = Random.shuffle((1 to numInstances) map { _ =>
        ctx ! mkDoc(auth, "data")
      })

      val toDelete = instances.take(deleteInstances)
      val notDeleted = instances.drop(deleteInstances)

      toDelete foreach { inst =>
        ctx ! runQuery(auth, DeleteF(inst.refObj))
      }

      ctx ! mkIndex(auth, "all_data", "data", List.empty, active = false)

      processAllTasks(ctx, exec)

      val refs = notDeleted map { inst => RefL(scope, inst.id) }

      val page = ctx ! collection(auth, Match(Ref("indexes/all_data")))

      page.elems.size shouldBe (numInstances - deleteInstances)
      page.elems.toSet shouldBe refs.toSet
    }

    "transitions" in {
      val colName = "animals"
      ctx ! mkCollection(auth, MkObject("name" -> colName))

      // enough docs to make the index build async
      for { i <- 0 to Index.BuildSyncSize } {
        ctx ! mkDoc(
          auth,
          colName,
          params =
            MkObject("data" -> MkObject("name" -> s"name$i", "const" -> "const")))
      }

      processAllTasks(ctx, exec) // process class invalidation task

      ctx ! mkIndex(
        auth,
        "names",
        colName,
        List(List("data", "name")),
        active = false)

      // root task
      (ctx ! tasks).size should equal(1)
      val root = rootTask(ctx)
      root.data(IndexBuild.StateField) should equal("build")

      // fork build
      exec.step()
      (ctx ! tasks).size should equal(1)
      inside(task(ctx, root.id).state) { case Task.Blocked(ids, data, _) =>
        ids.size should equal(1)
        data(IndexBuild.StateField) should equal("activate")
      }

      // exec build
      processAllTasks(ctx, exec)

      val idxQ =
        runQuery(auth, Clock.time, Get(Ref("indexes/names"))) flatMap toIndex

      (ctx ! idxQ).isActive should be(true)
      (ctx ! tasks).isEmpty should be(true)
    }

    "fallback to table scan" in {
      val colName = "fauna"
      ctx ! mkCollection(auth, MkObject("name" -> colName))

      // enough docs to make the index build async
      for { i <- 0 to Index.BuildSyncSize } {
        ctx ! mkDoc(
          auth,
          colName,
          params =
            MkObject("data" -> MkObject("name" -> s"name$i", "const" -> "const")))
      }

      processAllTasks(ctx, exec) // cache shootdown

      ctx ! mkIndex(
        auth,
        "species",
        colName,
        List(List("data", "species")),
        active = false)

      val root = rootTask(ctx)

      // proceed until the build
      exec.step()

      while (task(ctx, root.id).data(IndexBuild.StateField) == "invalidate") {
        exec.step()
      }

      exec.step()
      exec.step()
      (ctx ! tasks).size should equal(1)

      inside(task(ctx, root.id).state) { case Task.Blocked(ids, data, _) =>
        ids.size should equal(1)
        data(IndexBuild.StateField) should equal("activate")
      }

      // cancel the reindex, fallback to scan
      val reindex = ((ctx ! tasks) find { _.name == Reindex.ReindexDocs.name }).get
      ctx ! Task.cancel(reindex, None, 0)
      exec.step()

      (ctx ! tasks).size should equal(1)

      processAllTasks(ctx, exec) // finish the build

      val idxQ =
        runQuery(auth, Clock.time, Get(Ref("indexes/species"))) flatMap toIndex

      (ctx ! idxQ).isActive should be(true)
      (ctx ! tasks).isEmpty should be(true)
    }

    "index builds with less than doc + history limit are executed synchronously" in {
      val collName = "plays"
      val idxName = "plays_by_genres"
      val collId = ctx ! mkCollection(auth, MkObject("name" -> collName))
      val inst1 = ctx ! mkDoc(
        auth,
        collName,
        params = MkObject(
          "data" -> MkObject("name" -> "Romeo and Juliet", "genre" -> "tragedy")))
      val inst2 = ctx ! mkDoc(
        auth,
        collName,
        params =
          MkObject("data" -> MkObject("name" -> "King Lear", "genre" -> "tragedy")))

      ctx ! (ctx ! Store.historicalIndex(
        NativeIndex.DocumentsByCollection(scope),
        Vector(IndexTerm(collId, false)),
        IndexValue.MaxValue,
        IndexValue.MinValue,
        pageSize = 2
      )).count shouldBe 2

      processAllTasks(ctx, exec)

      ctx ! mkIndex(
        auth,
        idxName,
        collName,
        List(List("data", "genre")),
        active = false)
      (ctx ! tasks).isEmpty shouldBe true
      // Don't run tasks
      val idxQ = runQuery(auth, Clock.time, Get(IndexRef(idxName))) flatMap toIndex
      (ctx ! idxQ).isActive should be(true)

      val idx = ctx ! collection(auth, Match(IndexRef(idxName), "tragedy"))
      idx.elems should equal(Seq(RefL(scope, inst1.id), RefL(scope, inst2.id)))
    }

    "index builds with more than the limit in docs are not executed synchronously" in {
      val docCount = Index.BuildSyncSize + 1
      val className = s"gen$docCount"
      val collId = ctx ! mkCollection(auth, MkObject("name" -> className))
      val idxName = "gen_idx"

      val docs = List.tabulate(docCount) { i =>
        ctx ! mkDoc(
          auth,
          className,
          params =
            MkObject("data" -> MkObject("name" -> s"name$i", "const" -> "const")))
      }

      val expected = docs map { doc => RefL(scope, doc.id) }

      ctx ! (ctx ! Store.historicalIndex(
        NativeIndex.DocumentsByCollection(scope),
        Vector(IndexTerm(collId, false)),
        IndexValue.MaxValue,
        IndexValue.MinValue,
        pageSize = 2
      )).count shouldBe docCount

      processAllTasks(ctx, exec)
      ctx ! mkIndex(
        auth,
        idxName,
        className,
        List(List("data", "const")),
        active = false)
      (ctx ! tasks).isEmpty shouldBe false
      // Don't run tasks

      val queryIdx = collection(auth, Match(IndexRef(idxName), "const"))
      val idxQ = runQuery(auth, Clock.time, Get(IndexRef(idxName))) flatMap toIndex
      (ctx ! idxQ).isActive should be(false)

      processAllTasks(ctx, exec)
      (ctx ! idxQ).isActive should be(true)
      val idxAfter = ctx ! queryIdx
      idxAfter.elems should equal(expected)
    }

    "sync index builds don't pop rate limits" in {
      val collName = "users"
      val idxName = "by_name"

      ctx ! mkCollection(auth, MkObject("name" -> collName))
      (1 to Index.BuildSyncSize).foreach { i =>
        ctx ! mkDoc(
          auth,
          collName,
          params = MkObject("data" -> MkObject("name" -> s"$i")))
      }

      // Definitely too few writes to pass, if the index build writes count.
      val limiter = FixedWriteOpsLimiter(Index.BuildSyncSize / 2)
      val buildQ =
        mkIndex(auth, idxName, collName, List(List("data", "name")), active = false)
      noException shouldBe thrownBy(
        Await.result(
          ctx.run(buildQ, Clock.time, AccountID.Root, auth.scopeID, limiter),
          1.minute))

      // Tasks not run: build must be sync.
      val idxQ = runQuery(auth, Clock.time, Get(IndexRef(idxName))) flatMap toIndex
      (ctx ! idxQ).isActive should be(true)

      // Index did build.
      val idx = ctx ! collection(auth, Match(IndexRef(idxName), "1"))
      idx.elems.size shouldBe 1
    }

    "index builds task works with native collections" in {
      val docCount = Index.BuildSyncSize + 1
      val idxName = "key_idx"

      val keys = List.tabulate(docCount) { _ =>
        (ctx ! mkCurrentDBKey("server", auth))._2.id
      }

      val expected = keys.map(id => RefL(scope, id.toDocID))

      ctx ! (ctx ! Store.historicalIndex(
        NativeIndex.DocumentsByCollection(scope),
        Vector(IndexTerm(KeyID.collID, false)),
        IndexValue.MaxValue,
        IndexValue.MinValue,
        pageSize = 2
      )).count shouldBe docCount

      processAllTasks(ctx, exec)

      ctx ! runQuery(
        auth,
        Clock.time,
        CreateIndex(
          MkObject(
            "name" -> "key_idx",
            "source" -> KeysNativeClassRef,
            "terms" -> JSArray(MkObject("field" -> "role"))
          )))

      (ctx ! tasks).isEmpty shouldBe false
      // Don't run tasks

      val queryIdx = collection(auth, Match(IndexRef(idxName), "server"))
      val idxQ = runQuery(auth, Clock.time, Get(IndexRef(idxName))) flatMap toIndex
      (ctx ! idxQ).isActive should be(false)

      processAllTasks(ctx, exec)
      (ctx ! idxQ).isActive should be(true)
      val idxAfter = ctx ! queryIdx
      idxAfter.elems should equal(expected)
    }

    "index builds with more than the limit in history are not executed synchronously" in {
      val histCount =
        Index.BuildSyncSize + 1 // accounts for original doc insert- total Changes will be this number.
      val className = s"genHistOf$histCount"
      val collId = ctx ! mkCollection(auth, MkObject("name" -> className))
      val idxName = "gen_counts"

      val doc = ctx ! mkDoc(
        auth,
        className,
        params = MkObject("data" -> MkObject("name" -> "baseDoc", "count" -> 0)))

      for (i <- 1 to histCount) {
        ctx ! runQuery(
          auth,
          Update(doc.refObj, MkObject("data" -> MkObject("count" -> i))))
      }

      ctx ! (ctx ! Store.historicalIndex(
        NativeIndex.DocumentsByCollection(scope),
        Vector(IndexTerm(collId, false)),
        IndexValue.MaxValue,
        IndexValue.MinValue,
        pageSize = 2
      )).count shouldBe 1

      processAllTasks(ctx, exec)

      ctx ! mkIndex(
        auth,
        idxName,
        className,
        List(List("data", "name")),
        active = false)
      val idxQ = runQuery(auth, Clock.time, Get(IndexRef(idxName))) flatMap toIndex
      (ctx ! idxQ).isActive should be(false)
      (ctx ! tasks).isEmpty shouldBe false

      processAllTasks(ctx, exec)
      (ctx ! idxQ).isActive should be(true)

      val queryIdx = collection(auth, Match(IndexRef(idxName), "baseDoc"))
      val idxAfter = ctx ! queryIdx
      idxAfter.elems should equal(List(RefL(scope, doc.id)))
    }

    "ensures presence of both existing and new instances in scan build" in {
      val colName = "foo"
      ctx ! mkCollection(auth, MkObject("name" -> colName))

      // enough docs to make the index build async
      for (i <- 0 to Index.BuildSyncSize) {
        ctx ! mkDoc(
          auth,
          colName,
          params = MkObject("data" -> MkObject("name" -> s"name$i")))
      }

      processAllTasks(ctx, exec) // process other tasks

      val existingInst = ctx ! mkDoc(
        auth,
        colName,
        params = MkObject(
          "data" ->
            MkObject("name" -> "Alex")))

      val idx = ctx ! mkIndex(
        auth,
        "foo_by_name",
        colName,
        List(List("data", "name")),
        active = false)

      // invalidate by source schema
      ctx.cacheContext.schema.invalidate()

      val newInst = ctx ! mkDoc(
        auth,
        colName,
        params = MkObject(
          "data" ->
            MkObject("name" -> "Alex")))

      val newInstOnly = Seq(newInst) map { i => RefL(scope, i.id) }
      val bothInstances = Seq(existingInst, newInst) map { i => RefL(scope, i.id) }

      val q = Match(Ref("indexes/foo_by_name"), "Alex")

      val root = rootTask(ctx)

      // toggle active on and back off again to test the index in
      // partially-constructed state
      toggleIndexActive(ctx, auth, idx, true)
      (ctx ! collection(auth, q)).elems should equal(newInstOnly)
      toggleIndexActive(ctx, auth, idx, false)

      task(ctx, root.id).data(IndexBuild.StateField) should equal("build")
      processAllTasks(ctx, exec)

      (ctx ! collection(auth, q)).elems should equal(bothInstances)
    }

    "all sources index must scan all instances" in {
      ctx ! mkCollection(auth, MkObject("name" -> "foo1"))
      ctx ! mkCollection(auth, MkObject("name" -> "foo2"))
      val inst1 = ctx ! mkDoc(
        auth,
        "foo1",
        params = MkObject("data" -> MkObject("nom" -> "Alex")))
      val inst2 = ctx ! mkDoc(
        auth,
        "foo2",
        params = MkObject("data" -> MkObject("nom" -> "Alex")))

      ctx ! allSourcesIndex(
        auth,
        "all-sources",
        terms = List(List("data", "nom")),
        active = false)
      val allInstances = Seq(inst1, inst2) map { i => RefL(scope, i.id) }
      val q = Match(Ref("indexes/all-sources"), "Alex")

      processAllTasks(ctx, exec)

      (ctx ! tasks).isEmpty should be(true)
      (ctx ! collection(auth, q)).elems should equal(allInstances)
    }

    "all source termless index should not index native collections" in {
      val scope = ctx ! newScope
      val auth = Auth.forScope(scope)

      ctx ! mkCollection(auth, MkObject("name" -> "foo"))
      ctx ! allSourcesIndex(auth, "all_sources_index_scan", active = false)

      processAllTasks(ctx, exec)

      val inst = ctx ! mkDoc(auth, "foo", params = MkObject("data" -> MkObject()))

      processAllTasks(ctx, exec)

      val elems =
        (ctx ! collection(auth, Match(Ref("indexes/all_sources_index_scan")))).elems

      elems shouldBe List(RefL(scope, inst.id))
    }

    "tolerates indexes with no sources" in {
      val scope = ctx ! newScope
      val auth = Auth.forScope(scope)

      ctx ! {
        for {
          nextID <- Query.nextID
          indexID = IndexID(nextID)
          indexer <- Index.getIndexer(scope, IndexID.collID)
          // invalid source ids have the same practical
          // effect as deleted collections
          indexData = Data(
            MapV(
              "name" -> "no_source",
              "source" -> CollectionID(-1),
              "active" -> false))
          _ <- SchemaCollection
            .Index(auth.scopeID)
            .insert(indexID, indexData, isCreate = true)
          _ <- Index.build(scope, indexID)
        } yield ()
      }

      processAllTasks(ctx, exec)

      val isActiveQ = runQuery(auth, Select("active", Get(IndexRef("no_source"))))
      (ctx ! isActiveQ) shouldBe TrueL
    }

    "should ignore UnresolvedRefL on bindings" in {
      val collID = ctx ! mkCollection(auth, MkObject("name" -> "unresolved-coll"))

      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> "unresolved-idx",
            "active" -> true,
            "source" -> MkObject(
              "collection" -> ClassRef("unresolved-coll"),
              "fields" -> MkObject(
                "build-ref" -> QueryF(
                  Lambda("doc" ->
                    MkRef(
                      ClassRef(Select(JSArray("data", "collection"), Var("doc"))),
                      "123")))
              )
            ),
            "values" -> JSArray(
              MkObject("field" -> "ref"),
              MkObject("binding" -> "build-ref")
            )
          ))
      )

      val doc1 = ctx ! mkDoc(
        auth,
        "unresolved-coll",
        MkObject("data" -> MkObject("collection" -> "unresolved-coll")))
      val doc2 = ctx ! mkDoc(
        auth,
        "unresolved-coll",
        MkObject("data" -> MkObject("collection" -> "non-existent-collection")))

      val page = ctx ! collection(auth, Match(IndexRef("unresolved-idx")))

      val expected1 =
        ArrayL(List(RefL(scope, doc1.id), RefL(scope, DocID(SubID(123L), collID))))
      val expected2 = ArrayL(List(RefL(scope, doc2.id), NullL))

      page.elems shouldBe List(expected1, expected2)
    }
  }
}
