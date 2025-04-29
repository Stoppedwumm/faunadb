package fauna.model.test

import fauna.atoms._
import fauna.lang._
import fauna.model._
import fauna.model.tasks._
import fauna.repo._
import fauna.repo.query._
import fauna.repo.test.CassandraHelper
import fauna.storage.doc._

class TaskBouncerSpec extends Spec {
  import Database._
  import TaskBouncer._

  val ctx = CassandraHelper.context("model")
  val otherID = HostID.randomID

  val service =
    new Service {
      def isReady = true
      def selfID = ctx.service.localID.get
      def runningHosts = Set(selfID, otherID)
    }

  object FakeTask extends Type("fake-task") with Executor.Router[Task, Task.State] {
    def step(task: Task, ts: Timestamp): Query[Task.State] =
      Query.value(Task.Completed())
  }

  "TaskBouncer" - {
    "should bounce tasks off local host" in {
      var task = ctx ! Task.createLocal(RootScopeID, FakeTask.name, Data.empty, None)
      task.host shouldBe ctx.service.localID.get

      val bouncer = new TaskBouncer(ctx, service)
      bouncer.bounceAllTasks()

      task = (ctx ! Task.get(task.id)).get
      task.host shouldBe otherID
    }
  }
}
