package fauna.model.gc.test

import fauna.atoms._
import fauna.auth.Auth
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.{ Collection, Index }
import fauna.model.gc._
import fauna.model.tasks.TaskExecutor
import fauna.model.test._
import fauna.repo.{ MVTProvider => RepoMVTProvider, _ }
import fauna.repo.cassandra.CassandraService
import fauna.repo.query.Query
import fauna.repo.test.CassandraHelper
import fauna.storage.{ AtValid, Tables }
import fauna.storage.api._
import fauna.storage.api.set._
import fauna.storage.cassandra._
import fauna.storage.index._
import fauna.storage.SetAction
import org.apache.cassandra.db.compaction.{ CompactionManager, LeveledManifest }
import org.apache.cassandra.utils.FBUtilities
import scala.concurrent.duration._

class IndexBuildAsyncSpec extends Spec {
  import SocialHelpers._

  // Provide a version of the MVT provider that sets the MVT for a specific
  // collection (and also records MVT hints).
  private case class FakeMVTProvider(c: CollectionID, ts: Timestamp)
      extends RepoMVTProvider {

    def get(scopeID: ScopeID, collID: CollectionID) = {
      val q = if (collID == c) {
        Query.value(ts)
      } else {
        MVTProvider.get(scopeID, collID)
      }
      q map { mvt =>
        CollectionStrategy.hints.put(scopeID, collID, mvt)
        mvt
      }
    }

    def get(cfg: IndexConfig, terms: Vector[Term]) =
      MVTProvider.get(cfg, terms) map { mvts =>
        mvts match {
          case MVTMap.Default => sys.error("unexpected mvt map")
          case MVTMap.Mapping(map) =>
            map foreach { case (collID, mvt) =>
              CollectionStrategy.hints.put(cfg.scopeID, collID, mvt)
            }
        }
        mvts
      }
  }

  "Async index builds respect MVT" - {
    val baseCtx = CassandraHelper.context("model")
    val scope = baseCtx ! newScope
    val auth = Auth.forScope(scope)

    // The one thing that runQuery doesn't do is run the query.
    baseCtx ! runQuery(auth, CreateCollection(MkObject("name" -> "users")))
    val colID = (baseCtx ! Collection.idByNameActive(scope, "users")).get

    // Set up the test case doc history:
    // CREATE "Robert" => UPDATE "Bob" => UPDATE "Bob" => UPDATE "Bob"
    //                                             MVT-^
    // Compaction at the indicated MVT will alter the history causing table scan
    // index builds to compute an improper diff using the old storage API.
    val userRef = RefV(0, ClsRefV("users"))
    baseCtx ! runQuery(
      auth,
      CreateF(userRef, MkObject("data" -> MkObject("name" -> "Robert"))))
    baseCtx ! runQuery(
      auth,
      Update(userRef, MkObject("data" -> MkObject("name" -> "Bob"))))
    baseCtx ! runQuery(
      auth,
      Update(userRef, MkObject("data" -> MkObject("name" -> "Bob"))))
    baseCtx ! runQuery(
      auth,
      Update(userRef, MkObject("data" -> MkObject("name" -> "Bob"))))

    // Insert enough chaff to trigger a table scan index build.
    val createQ = (1 until 2 * Index.BuildSyncSize) map { id =>
      runQuery(auth, CreateF(RefV(id, ClsRefV("users"))))
    }
    baseCtx ! createQ.sequence

    // Compact.
    val id = DocID(SubID(0), colID)
    val vs = baseCtx ! ModelStore.versions(scope, id).flattenT
    val gcRootTS = vs(1).ts.validTS
    runCompactions(scope, colID, gcRootTS.nextMicro)

    // Queries must run with the compaction MVT for the users collection.
    // The second context forces an index build table scan.
    val mvtCtx =
      baseCtx.copy(mvtProvider = FakeMVTProvider(colID, gcRootTS.nextMicro))
    val ctxScan = mvtCtx.withReindexDocsCancelLimit(Index.BuildSyncSize)

    // Check for the proper history post-compaction.
    val compactedHistory = mvtCtx ! ModelStore.versions(scope, id).flattenT
    compactedHistory.size should equal(2)
    compactedHistory(0) should equal(vs(0))
    compactedHistory(1) should equal(vs(1))
    compactedHistory(1).diff should equal(None) // GC root diff rewritten.

    // Check an index build post-compaction creates entries that respect MVT.
    for ((scen, ctx) <- Seq("reindex" -> mvtCtx, "scan" -> ctxScan)) {
      s"when run as a $scen" in {
        // Build the index _after_ compaction.
        ctx ! runQuery(
          auth,
          CreateIndex(
            MkObject(
              "name" -> s"users_by_name_$scen",
              "source" -> ClassRef("users"),
              "active" -> true,
              "terms" -> Seq(MkObject("field" -> Seq("data", "name")))))
        )
        TaskHelpers.processAllTasks(ctx, TaskExecutor(ctx))

        // If the build used the old API, there would be no index history from the
        // improper diff.
        // With the new API, there is an entry for "Bob" entering the set.
        val idxID = (ctx ! Index.idByName(scope, s"users_by_name_$scen")).get
        val history = ctx ! (Index.get(scope, idxID) flatMap { idx =>
          Store.sortedIndex(idx.get, Vector(IndexTerm("Bob")))
        }).flattenT
        history.size should equal(1)
        history.head should equal(
          IndexValue(
            IndexTuple(scope, id, Vector(), None),
            AtValid(gcRootTS),
            SetAction.Add))
      }
    }
  }

  // Runs compactions on versions and index CFs with the MVT for `(scope, col)` at
  // `mvt`.
  // Adapted from InlineGCSpec.
  private def runCompactions(scope: ScopeID, col: CollectionID, mvt: Timestamp) = {
    def compact(cfName: String) = {
      val store =
        CassandraService.instance.storage.keyspace.getColumnFamilyStore(cfName)
      store.forceBlockingFlush()

      val sstables = store.getSSTables()
      val maxSSTableBytes = store.getCompactionStrategy.getMaxSSTableBytes

      // NB. Recompact all sstables into a single file at L0. We're not concerned
      // with the leveling structure as much as we're concerned with compacting all
      // available cells for a given column family so that expired cells can be
      // removed in a single compaction.
      val candidate =
        new LeveledManifest.CompactionCandidate(
          sstables,
          0 /* level */,
          maxSSTableBytes
        )

      val hints = CollectionStrategy.hints
      hints.put(scope, col, mvt)

      val task =
        new CollectionTask(
          store,
          candidate,
          Int.MaxValue, // gcBefore
          CollectionStrategy.hints,
          CollectionStrategy.filterBuilder(cfName)
        )

      assert(store.getDataTracker.markCompacting(sstables))
      FBUtilities.waitOnFuture(CompactionManager.instance.submitTask(task))
    }

    Set(
      Tables.Versions.CFName,
      Tables.SortedIndex.CFName,
      Tables.HistoricalIndex.CFName
    ) foreach { cfName =>
      eventually(timeout(2.minute)) {
        compact(cfName)
      }
    }
  }
}
