package fauna.model.test

import fauna.auth.{ Auth, RootAuth }
import fauna.codex.json._
import fauna.flags.RunTasks
import fauna.lang.clocks.Clock
import fauna.model.{ Database, Index, Task }
import fauna.model.account.Account
import fauna.model.tasks.TaskRouter
import fauna.repo.test.CassandraHelper
import org.scalatest.concurrent.Eventually
import scala.util.Random

class TaskRouterSpec extends Spec with Eventually {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")

  "TaskRouter" - {
    "pauses tasks when 'run_tasks' is false" in {
      val accountID = Random.nextInt(1 << 20) // Generate nonnegative account ids.

      val dbScope =
        ctx ! newScope(RootAuth, "bad_builder", accountID = Some(accountID))

      val auth = Auth.adminForScope(dbScope)

      ctx ! mkCollection(auth, MkObject("name" -> "baddies"))

      // Avoid the synchronous build path.
      for (_ <- 0 until Index.BuildSyncSize + 1) {
        ctx ! mkDoc(auth, "baddies")
      }

      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> "bad_build",
            "source" -> "_",
            "terms" ->
              JSArray(MkObject("field" -> List("data", "somefield"))))))

      val task = ctx ! (Task.getAllRunnable() findValueT { task =>
        task.scopeID == dbScope
      })

      task.isEmpty should be(false)

      CassandraHelper.ffService.withAccount(RunTasks, false) {
        CassandraHelper.invalidateCaches(ctx) // force refreshing flags on next load

        val db = ctx ! Database.forScope(dbScope)
        db.isEmpty should be(false)
        (ctx ! Account.flagForScope(dbScope, RunTasks)) should be(false)

        val router = new TaskRouter
        (ctx ! router(task.get, Clock.time)).isPaused should be(true)
      }
    }
  }
}
