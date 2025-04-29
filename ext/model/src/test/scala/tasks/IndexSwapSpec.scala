package fauna.model.test

import fauna.ast._
import fauna.auth._
import fauna.model._
import fauna.model.tasks.{ IndexSwap, TaskExecutor }
import fauna.repo.cassandra.CassandraService
import fauna.repo.query.Query
import fauna.repo.test.CassandraHelper

class IndexSwapSpec extends Spec {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")
  val exec = TaskExecutor(ctx)

  def processAllTasks(): Unit = {
    def tasks: Query[Iterable[Task]] =
      Task.getRunnableByHost(CassandraService.instance.localID.get).flattenT

    while ((ctx ! tasks).nonEmpty) {
      exec.step()
    }
  }

  "IndexSwap" - {
    "rebuilds an index" in {
      val scope = ctx ! newScope
      val auth = Auth.forScope(scope)

      ctx ! mkCollection(auth, MkObject("name" -> "animals"))
      val idx = ctx ! mkIndex(
        auth,
        "animals_by_class",
        "animals",
        List(List("data", "class")))

      val lion = ctx ! mkDoc(
        auth,
        "animals",
        MkObject("data" -> MkObject("class" -> "Mammalia")))
      val tiger = ctx ! mkDoc(
        auth,
        "animals",
        MkObject("data" -> MkObject("class" -> "Mammalia")))
      val bear = ctx ! mkDoc(
        auth,
        "animals",
        MkObject("data" -> MkObject("class" -> "Mammalia")))

      val expected = Seq(lion, tiger, bear) map { inst =>
        RefL(scope, inst.id)
      }

      val initial = ctx ! collection(auth, Match("animals_by_class", "Mammalia"))
      initial.elems should contain theSameElementsAs (expected)

      ctx ! IndexSwap.Root.create(scope, idx.id)

      processAllTasks()

      // The old index is preserved, and hidden.
      val old = ctx ! Index.idByName(scope, "animals_by_class_swap").map { _.get }
      old should equal(idx.id)

      val hidden = ctx ! Index.get(scope, old)
      hidden.get.isHidden should be(true)

      // The new index is active, and not hidden.
      val neuu = ctx ! Index.idByName(scope, "animals_by_class").map { _.get }
      neuu shouldNot equal(idx.id)

      val live = ctx ! Index.get(scope, neuu)
      live.get.isHidden shouldNot be(true)

      val rebuilt = ctx ! collection(auth, Match("animals_by_class", "Mammalia"))
      rebuilt.elems should contain theSameElementsAs (expected)
    }

    "updates roles" in {
      val scope = ctx ! newScope
      val auth = Auth.adminForScope(scope)

      ctx ! mkCollection(auth, MkObject("name" -> "animals"))
      val idx = ctx ! mkIndex(
        auth,
        "animals_by_class",
        "animals",
        List(List("data", "class")))

      val role = ctx ! mkRole(
        auth,
        "read_a_book",
        Seq(
          MkObject(
            "resource" -> IndexRef("animals_by_class"),
            "actions" -> MkObject("read" -> true))))

      val before = ctx ! Role.get(scope, role).map { _.get }
      before.privileges.size should be(1)
      before.privileges.head.resource should be(Right(idx.id.toDocID))

      ctx ! IndexSwap.Root.create(scope, idx.id)

      processAllTasks()

      val neuu = ctx ! Index.idByName(scope, "animals_by_class").map { _.get }
      neuu shouldNot equal(idx.id)

      val after = ctx ! Role.get(scope, role).map { _.get }
      after.privileges.size should be(1)
      after.privileges.head.resource should be(Right(neuu.toDocID))
    }
  }
}
