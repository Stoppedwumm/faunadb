package fauna.model.test

import fauna.ast.RefL
import fauna.atoms._
import fauna.auth._
import fauna.lang.clocks.Clock
import fauna.model._
import fauna.model.schema.NativeIndex
import fauna.model.tasks._
import fauna.prop.{ Prop, PropConfig }
import fauna.repo._
import fauna.repo.cassandra.CassandraService
import fauna.repo.test.CassandraHelper
import fauna.storage.index._
import fauna.storage.ir._
import org.scalatest.tags.Slow
import scala.util.Random

@Slow
class ReindexSpec extends Spec {
  import SocialHelpers._

  implicit val propConfig = PropConfig()

  val ctx = CassandraHelper.context("model")
  val exec = TaskExecutor(ctx)

  val scope = ctx ! newScope
  val auth = Auth.forScope(scope)

  def tasks =
    Task
      .getRunnableByHost(CassandraService.instance.localID.get)
      .flattenT

  def processAllTasks() =
    while ((ctx ! tasks).nonEmpty) exec.step()

  def resetTasks() = {
    val idx = NativeIndex.DocumentsByCollection(ScopeID.RootID)
    val terms = Vector(IndexTerm(DocIDV(TaskID.collID.toDocID)))
    val ts = Store.collectDocuments(idx, terms, Clock.time) { (v, ts) =>
      Task.get(v.docID.as[TaskID], ts)
    }

    ts foreachValueT {
      Task.complete(_, None, 0) map { _ => () }
    }
  }

  "ReindexSpec" - {
    "builds an index" in {
      val size = Prop.int(DefaultPageSize * 2 to DefaultPageSize * 20).sample
      val cls = ctx ! mkCollection(auth, MkObject("name" -> "people"))

      processAllTasks()

      val docs = (0 until size) map { _ =>
        ctx ! mkDoc(auth, "people")
      }

      val idx = ctx ! mkIndex(auth, "people_by_class", "people", List(List("class")))

      ctx ! resetTasks()

      val q = Match(IndexRef("people_by_class"), ClassRef("people"))
      (ctx ! collection(auth, q)).elems.isEmpty should be(true)

      val terms = Vector(DocIDV(cls.toDocID))
      ctx ! Reindex.Root.create(
        scope,
        NativeIndexID.DocumentsByCollection,
        terms,
        idx.id,
        None)

      processAllTasks()

      val result = (ctx ! collection(auth, q)).elems map {
        case RefL(_, id) => Inst(id, "people")
        case v           => fail(s"unexpected $v")
      }

      result should equal(docs)
    }

    "all source termless index should not index native collections" in {
      val cls = ctx ! mkCollection(auth, MkObject("name" -> "foo"))
      val inst = ctx ! mkDoc(auth, "foo")

      processAllTasks()

      val index = ctx ! allSourcesIndex(auth, "all_sources_reindex", active = true)

      ctx ! resetTasks()

      val terms = Vector(DocIDV(cls.toDocID))
      ctx ! Reindex.Root.create(
        scope,
        NativeIndexID.DocumentsByCollection,
        terms,
        index.id,
        None)

      processAllTasks()

      val elems =
        (ctx ! collection(auth, Match(IndexRef("all_sources_reindex")))).elems

      elems shouldBe List(RefL(scope, inst.id))
    }

    // NB. Canceled reindex tasks fallback to table scans. See IndexBuildSpec.
    "cancel based on bytes read limit" in {
      val coll = ctx ! mkCollection(auth, MkObject("name" -> "large_docs"))

      for (_ <- 1 to 25) { // 25mb
        ctx ! mkDoc(
          auth,
          "large_docs",
          MkObject(
            "data" -> MkObject(
              "bytes" -> Random.nextBytes(1024 * 1024) // 1mb
            ))
        )
      }

      val index = ctx ! mkIndex(auth, "large_index", "large_docs", terms = Nil)

      val task =
        ctx ! Reindex.Root.create(
          scope,
          NativeIndexID.DocumentsByCollection,
          Vector(DocIDV(coll.toDocID)),
          index.id,
          None
        )

      val limitedCtx = ctx.copy(reindexCancelBytesLimit = 10 * 1024 * 1024) // 10mb

      val exec = TaskExecutor(limitedCtx)
      while ((ctx ! tasks).nonEmpty) exec.step()

      val updatedTask = ctx ! Task.get(task.id)
      val canceled = updatedTask.value.state.isCancelled
      assert(canceled, "reindex task was not canceled")
    }
  }
}
