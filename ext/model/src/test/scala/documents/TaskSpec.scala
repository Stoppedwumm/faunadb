package fauna.model.test

import fauna.atoms._
import fauna.lang.{ AdminControl, Timestamp }
import fauna.lang.clocks._
import fauna.lang.syntax._
import fauna.model._
import fauna.model.schema.InternalCollection
import fauna.model.schema.NativeIndex
import fauna.model.tasks._
import fauna.repo._
import fauna.repo.cassandra.CassandraService
import fauna.repo.query.Query
import fauna.repo.test.CassandraHelper
import fauna.storage.doc.{ Data, Field }
import fauna.storage.index.IndexTerm
import fauna.storage.ir._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Promise

class TaskSpec extends Spec {
  val ctx = CassandraHelper.context("model")

  def localID = CassandraService.instance.localID.get

  def TS(ts: Long) = Timestamp.ofMicros(ts)

  def tasksForHost(id: HostID, ts: Timestamp): Query[Seq[Task]] = {
    Store.collectDocuments(
      NativeIndex.DocumentsByCollection(ScopeID.RootID),
      Vector(IndexTerm(DocIDV(TaskID.collID.toDocID))),
      ts) { case (tuple, ts) =>
      Task.get(tuple.docID.as[TaskID], ts)
    } selectT { _.host == id } flattenT
  }

  def runnable() =
    Task.getRunnableByHost(localID).flattenT

  def cleanupJournal() =
    JournalEntry.readByTag(Repair.EntryTag, Clock.time) foreachValueT { entry =>
      JournalEntry.remove(entry.id)
    }

  def cancelTask(id: TaskID) =
    withTask(id) { task =>
      task.cancel(None)
    }

  def withTask[A](id: TaskID)(taskFn: Task => Query[A]): Query[A] =
    Task.get(id) flatMap {
      case Some(task) => taskFn(task)
      case None       => Query.fail(new RuntimeException(s"Task $id doesn't exist."))
    }

  case object TestTask extends Type("test") with Executor.Router[Task, Task.State] {
    def step(task: Task, ts: Timestamp): Query[Task.State] =
      Query.value(Task.Completed())
  }

  case class LongTask()
      extends Type("long-task")
      with Executor.Router[Task, Task.State] {
    val CollField = Field[Vector[Long]]("collection")

    val lock = Promise[Unit]()

    def step(task: Task, ts: Timestamp): Query[Task.State] =
      Query.future { lock.future map { _ => Task.Runnable(Data.empty, None) } }

    def unlock() = lock.success(())

    def create() =
      Task.createLocal(
        Database.RootScopeID,
        this.name,
        Data(CollField -> Vector.empty),
        None)
  }
  class TestRouter(val extraTypes: Map[String, Type] = Map()) extends TaskRouter {
    override def dispatch(task: Task): Option[Type] =
      super.dispatch(task).orElse(extraTypes.get(task.name))
  }

  "TaskSpec" - {
    "manipulates tasks" in {
      val task =
        ctx ! Task.create(Database.RootScopeID, "foo", localID, Data.empty, None)
      task.isPending shouldBe (true)
      task.createdAt shouldBe >(Timestamp.Epoch)
      task.elapsedMillis shouldBe (0)

      val tasks1 = ctx ! tasksForHost(localID, Clock.time)
      tasks1 map { _.id } should equal(Seq(task.id))

      val run = ctx ! Task.runnable(task, Data.empty, None, elapsedMillis = 10)

      val completed = ctx ! Task.complete(run, None, 0)
      completed.isCompleted shouldBe (true)
      completed.elapsedMillis shouldBe (10)

      tasks1 foreach { task =>
        task.host should equal(localID)
        ctx ! Task.remove(task.id)
      }

      val tasks2 = ctx ! tasksForHost(localID, Clock.time)
      tasks2 should equal(Seq.empty)
    }

    "prioritizes tasks" in {
      val root =
        ctx ! Task.create(Database.RootScopeID, "foo", localID, Data.empty, None)
      val child = ctx ! Task.create(
        Database.RootScopeID,
        "foo",
        localID,
        Data.empty,
        Some(root.id))

      root.priority should be(Task.DefaultUserPriority)
      child.priority should be(root.priority + 1)

      val oproot = ctx ! Task.create(
        Database.RootScopeID,
        "foo",
        localID,
        Data.empty,
        None,
        isOperational = true)
      val opchild = ctx ! Task.create(
        Database.RootScopeID,
        "foo",
        localID,
        Data.empty,
        Some(oproot.id))

      oproot.priority should be(Task.DefaultOperationalPriority)
      oproot.isOperational should be(true)
      opchild.priority should be(oproot.priority + 1)
      opchild.isOperational should be(true)

      root.priority should be > (oproot.priority)
      root.priority should be > (opchild.priority)

      // cleanup
      ctx ! child.abort()
      ctx ! opchild.abort()
    }

    "re-prioritize task" in {
      val exec = TaskExecutor(ctx)

      (ctx ! runnable()).isEmpty should be(true)

      val insertQ = Repair.RootTask.createOpt(Clock.time, Repair.Commit, Repair.Full)
      val root = (ctx ! insertQ).get

      (ctx ! runnable()).size should equal(1)

      exec.step() // fork off subtasks

      val tasks = ctx ! runnable()
      tasks.size shouldBe 1

      val child = ctx ! tasks.head.reprioritize(10)
      child.priority shouldBe 10

      val newRoot = ctx ! withTask(root.id) { task =>
        task.reprioritize(1)
      }
      newRoot.priority shouldBe 1

      ctx ! newRoot.cancel(None)

      ctx ! cleanupJournal()

      (ctx ! runnable()).isEmpty should be(true)
    }

    "doesn't re-prioritize if priority is higher than its children priorities" in {
      val exec = TaskExecutor(ctx)

      (ctx ! runnable()).isEmpty should be(true)

      val insertQ = Repair.RootTask.createOpt(Clock.time, Repair.Commit, Repair.Full)
      val root = (ctx ! insertQ).get

      (ctx ! runnable()).size should equal(1)

      exec.step() // fork off subtasks

      val tasks = ctx ! runnable()
      tasks.size shouldBe 1

      val thrown = the[RuntimeException] thrownBy {
        ctx ! (Task.get(root.id, Clock.time) flatMap { task =>
          task.get.reprioritize(root.priority + 10)
        })
      }
      thrown.getMessage shouldBe "Parent priority cannot be higher than its children priorities."

      ctx ! cancelTask(root.id)

      ctx ! cleanupJournal()

      (ctx ! runnable()).isEmpty should be(true)
    }

    "steals tasks" in {
      // workaround to 'steal' in a one-node cluster
      val id = HostID.randomID
      val data = Data(
        Task.HostField -> id,
        Task.NameField -> "foo",
        Task.CompletedField -> false,
        Task.DataField -> Data.empty)

      val insertQ = Query.nextID map { TaskID(_) } flatMap { id =>
        InternalCollection.Task(Database.RootScopeID).insert(id, data)
      }

      val vers = ctx ! insertQ
      val task = (ctx ! Task.get(vers.id.as[TaskID])).get
      val me = HostID.randomID

      ctx ! Task.steal(task, Some(me))

      val stolen = ctx ! tasksForHost(me, Clock.time)

      stolen map { _.id } should equal(Seq(task.id))

      val empty = ctx ! Task.getRunnableByHost(id).flattenT

      empty.isEmpty should be(true)

      stolen foreach { t => ctx ! Task.remove(t.id) }
    }

    "decodes completed tasks with no data" in {
      val StateField = Field[Data]("state")

      val id = ctx ! Query.nextID.map(TaskID(_))
      val data = Data(
        Task.HostField -> localID,
        Task.NameField -> "foo",
        StateField -> Data(MapV("name" -> StringV("completed")))
      )

      ctx ! InternalCollection.Task(Database.RootScopeID).insert(id, data)
      val t = ctx ! Task.get(id)
      t.get.state.data shouldEqual Data.empty
    }

    "decodes completed tasks with data" in {
      val StateField = Field[Data]("state")

      val id = ctx ! Query.nextID.map(TaskID(_))
      val data = Data(
        Task.HostField -> localID,
        Task.NameField -> "foo",
        StateField -> Data(
          MapV(
            "name" -> StringV("completed"),
            "data" -> MapV("bar" -> StringV("baz"))))
      )

      ctx ! InternalCollection.Task(Database.RootScopeID).insert(id, data)
      val t = ctx ! Task.get(id)
      t.get.state.data shouldEqual Data(MapV("bar" -> StringV("baz")))
    }

    "decodes cancelled tasks with no data" in {
      val StateField = Field[Data]("state")

      val id = ctx ! Query.nextID.map(TaskID(_))
      val data = Data(
        Task.HostField -> localID,
        Task.NameField -> "foo",
        StateField -> Data(MapV("name" -> StringV("cancelled")))
      )

      ctx ! InternalCollection.Task(Database.RootScopeID).insert(id, data)
      val t = ctx ! Task.get(id)
      t.get.state.data shouldEqual Data.empty
    }

    "decodes cancelled tasks with data" in {
      val StateField = Field[Data]("state")

      val id = ctx ! Query.nextID.map(TaskID(_))
      val data = Data(
        Task.HostField -> localID,
        Task.NameField -> "foo",
        StateField -> Data(
          MapV(
            "name" -> StringV("cancelled"),
            "data" -> MapV("bar" -> StringV("baz"))))
      )

      ctx ! InternalCollection.Task(Database.RootScopeID).insert(id, data)
      val t = ctx ! Task.get(id)
      t.get.state.data shouldEqual Data(MapV("bar" -> StringV("baz")))
    }

    "cancels forked tasks" in {
      val exec = TaskExecutor(ctx)

      (ctx ! runnable()).isEmpty should be(true)

      val insertQ = Repair.RootTask.createOpt(Clock.time, Repair.Commit, Repair.Full)
      val root = (ctx ! insertQ).get

      (ctx ! runnable()).size should equal(1)

      exec.step() // fork off subtasks

      val ts = ctx ! runnable()
      ts.size should equal(1)

      // parent will be Blocking, and not on the runq
      ts.head.parent.isEmpty should be(false)
      ts.head.parent foreach { p =>
        val parent = ctx ! Task.get(p)
        parent.isEmpty should be(false)
        parent.get.isPending should be(true)
      }

      ctx ! cancelTask(root.id)

      ctx ! cleanupJournal()

      (ctx ! runnable()).isEmpty should be(true)
    }

    "cancels unforked tasks" in {
      (ctx ! runnable()).isEmpty should be(true)

      val insertQ =
        Task.createLocal(Database.RootScopeID, TestTask.name, Data.empty, None)
      val root = ctx ! insertQ

      (ctx ! runnable()).size should equal(1)

      ctx ! cancelTask(root.id)

      (ctx ! runnable()).isEmpty should be(true)
    }

    "cancels in-flight tasks" in {
      val longTask = LongTask()

      val exec = TaskExecutor(ctx, new TestRouter(Map(longTask.name -> longTask)))

      (ctx ! runnable()).isEmpty should be(true)

      val root = ctx ! longTask.create()

      (ctx ! runnable()).size should equal(1)

      val t = new Thread({ () =>
        exec.step() // start the long running task
      })

      t.start()

      ctx ! cancelTask(root.id)

      longTask.unlock()

      (ctx ! runnable()).isEmpty should be(true)

      ctx ! withTask(root.id) { task =>
        task.isCancelled should be(true)
        Query.unit
      }
    }

    "pause/unpause tasks" in {
      val task =
        ctx ! Task.create(Database.RootScopeID, "foo", localID, Data.empty, None)
      task.isPending shouldBe true
      task.isRunnable shouldBe true

      val paused = ctx ! task.pause("test")
      paused.isPaused shouldBe true
      paused.isRunnable shouldBe false

      val unpaused = ctx ! task.unpause()
      unpaused.isPaused shouldBe false
      unpaused.isRunnable shouldBe true

      ctx ! unpaused.abort()
    }

    "pause blocked task should pause its children" in {
      val exec = TaskExecutor(ctx, new TestRouter)

      (ctx ! runnable()).isEmpty should be(true)

      val insertQ = Repair.RootTask.createOpt(Clock.time, Repair.Commit, Repair.Full)
      val root = (ctx ! insertQ).get

      (ctx ! runnable()).size should equal(1)

      exec.step() // fork off subtasks

      val children = ctx ! runnable()
      children.size should equal(1)

      ctx ! withTask(root.id) { task =>
        task.isForked shouldBe true
        task.pause("test")
      }

      (ctx ! runnable()).size shouldBe 0

      ctx ! withTask(children.head.id) { task =>
        task.isPaused shouldBe true

        Query.none
      }

      ctx ! cancelTask(root.id)

      ctx ! cleanupJournal()

      (ctx ! runnable()).isEmpty shouldBe true
    }

    "un-pause blocked task should un-pause its children" in {
      val exec = TaskExecutor(ctx, new TestRouter)

      (ctx ! runnable()).isEmpty shouldBe true

      val insertQ = Repair.RootTask.createOpt(Clock.time, Repair.Commit, Repair.Full)
      val root = (ctx ! insertQ).get

      (ctx ! runnable()).size shouldBe 1

      exec.step() // fork off subtasks

      val children = ctx ! runnable()
      children.size should equal(1)

      ctx ! withTask(root.id) { task =>
        task.isForked shouldBe true
        task.pause("test")
      }

      (ctx ! runnable()).size shouldBe 0

      ctx ! withTask(children.head.id) { task =>
        task.isPaused shouldBe true

        Query.none
      }

      ctx ! withTask(root.id) { task =>
        task.unpause()
      }

      (ctx ! runnable()).size shouldBe 1

      ctx ! withTask(children.head.id) { task =>
        task.isPaused shouldBe false

        Query.unit
      }

      ctx ! cancelTask(root.id)

      ctx ! cleanupJournal()

      (ctx ! runnable()).isEmpty shouldBe true
    }

    "paused tasks should not step" in {
      pending // XXX: this is an infinite loop in the executor and needs re-thinking.

      val StepsField = Field[Int]("steps")

      case object StepsTask
          extends Type("steps-task")
          with Executor.Router[Task, Task.State] {
        def create(): Query[TaskID] = {
          val data = Data(StepsField -> 0)

          Task.createLocal(Database.RootScopeID, name, data, None) map { _.id }
        }

        def step(task: Task, ts: Timestamp): Query[Task.State] = {
          val steps = task.data(StepsField) + 1

          Query.value(
            Task.Runnable(task.data.update(StepsField -> steps), task.parent))
        }
      }

      val exec = TaskExecutor(ctx, new TestRouter(Map(StepsTask.name -> StepsTask)))

      val taskID = ctx ! StepsTask.create()

      ctx ! withTask(taskID) { task =>
        task.data(StepsField) shouldBe 0

        Query.unit
      }

      exec.step()

      ctx ! withTask(taskID) { task =>
        task.data(StepsField) shouldBe 1

        task.pause("test")
      }

      (1 to 10) foreach { _ => exec.step() }

      ctx ! withTask(taskID) { task =>
        task.data(StepsField) shouldBe 1

        task.unpause()
      }

      (1 to 10) foreach { _ => exec.step() }

      ctx ! withTask(taskID) { task =>
        task.data(StepsField) shouldBe 11

        task.cancel(None)
      }
    }

    "pause in-flight tasks" in {
      val longTask = LongTask()

      val exec = TaskExecutor(ctx, new TestRouter(Map(longTask.name -> longTask)))

      (ctx ! runnable()).isEmpty should be(true)

      val root = ctx ! longTask.create()

      (ctx ! runnable()).size shouldBe 1

      // I'm not sure if this test makes sense, because that transaction will begin
      // before the task is paused, so the TestExecutor will only have access to the
      // task version in the past, it won't know if the task was updated.
      val t = new Thread({ () =>
        exec.step() // start the long running task
      })

      t.start()

      ctx ! withTask(root.id) { task =>
        task.pause("test")
      }

      longTask.unlock()

      exec.step()

      ctx ! withTask(root.id) { task =>
        task.isPaused shouldBe true

        Query.unit
      }

      (ctx ! runnable()).isEmpty shouldBe true

      ctx ! cancelTask(root.id)
    }

    "get runnable hierarchy" in {
      val StateField = Field[String]("state")
      val LevelField = Field[Int]("level")

      case object HierarchyTask
          extends Type("hierarchy-task")
          with Executor.Router[Task, Task.State] {
        def create(level: Int): Query[Task] = {
          val data = Data(
            StateField -> "start",
            LevelField -> level
          )

          Task.createLocal(Database.RootScopeID, name, data, None)
        }

        def step(task: Task, ts: Timestamp): Query[Task.State] = {
          task.data(StateField) match {
            case "start" =>
              val level = task.data(LevelField)

              if (level > 0) {
                task.fork {
                  val tasks = (1 to 2).map { _ =>
                    HierarchyTask.create(level - 1)
                  }.sequence

                  tasks map {
                    (_, task.data.update(StateField -> "running"))
                  }
                }
              } else {
                Query.value(Task
                  .Runnable(task.data.update(StateField -> "running"), task.parent))
              }

            case "running" =>
              Query.value(task.state)
          }
        }
      }

      val exec =
        TaskExecutor(ctx, new TestRouter(Map(HierarchyTask.name -> HierarchyTask)))

      ctx ! HierarchyTask.create(3)

      (ctx ! runnable()).size should equal(1)
      (ctx ! Task.getRunnableHierarchy()).size should equal(1)

      exec.step()

      (ctx ! runnable()).size should equal(2)
      (ctx ! Task.getRunnableHierarchy()).size should equal(3)

      exec.step()

      (ctx ! runnable()).size should equal(4)
      (ctx ! Task.getRunnableHierarchy()).size should equal(7)

      exec.step()

      (ctx ! runnable()).size should equal(8)
      (ctx ! Task.getRunnableHierarchy()).size should equal(15)
    }

    "dry-run" in {
      implicit val adminCtl = new AdminControl

      val StateField = Field[String]("state")

      case object DryRunTask
          extends Type("dry-run-task")
          with Executor.Router[Task, Task.State] {
        def create(): Query[TaskID] = {
          val data = Data(
            StateField -> "start"
          )

          Task.createLocal(Database.RootScopeID, name, data, None) map { _.id }
        }

        def step(task: Task, ts: Timestamp): Query[Task.State] = {
          task.data(StateField) match {
            case "start" =>
              Query.value(
                Task
                  .Runnable(task.data.update(StateField -> "running"), task.parent))

            case "running" =>
              Query.value(Task.Completed())
          }
        }
      }

      val exec =
        TaskExecutor(ctx, new TestRouter(Map(DryRunTask.name -> DryRunTask)))

      val taskID = ctx ! DryRunTask.create()

      def getState() = (ctx ! Task.get(taskID)).get.state

      val newState0 = ctx ! Task.dryRun(taskID, exec)
      newState0 shouldBe Task.Runnable(Data(StateField -> "running"), None)

      getState() shouldBe Task.Runnable(Data(StateField -> "start"), None)

      exec.step()

      getState() shouldBe Task.Completed()
    }

    "pauses tasks that fail the onComplete callback" in {
      case object FailsOnCompleteTask
          extends Type("test")
          with Executor.Router[Task, Task.State] {
        @volatile var failOnComplete = true

        def create(): Query[TaskID] =
          Task.createLocal(Database.RootScopeID, name, Data.empty, None) map { _.id }

        def step(task: Task, ts: Timestamp): Query[Task.State] =
          Query.value(Task.Completed())

        override def onComplete(task: Task) = if (failOnComplete) {
          Query.fail(new Exception("spanner in the works"))
        } else {
          Query.unit
        }
      }

      val exec = TaskExecutor(
        ctx,
        new TestRouter(Map(FailsOnCompleteTask.name -> FailsOnCompleteTask)))

      val id = ctx ! FailsOnCompleteTask.create()

      noException should be thrownBy exec.runTask(id)

      // onComplete failed: task is paused.
      ctx ! withTask(id) { task =>
        task.state should matchPattern {
          case Task.Paused(
                "Task failed to complete: spanner in the works",
                _,
                _
              ) => // OK.
        }
        Query.unit
      }

      // onComplete would now succeed, but the task should remain paused.
      FailsOnCompleteTask.failOnComplete = false
      noException should be thrownBy exec.runTask(id)
      ctx ! withTask(id) { task =>
        task.state should matchPattern {
          case Task.Paused(
                "Task failed to complete: spanner in the works",
                _,
                _
              ) => // OK.
        }
        Query.unit
      }

      // Unpause the task and check it completes.
      ctx ! withTask(id) { _.unpause() }
      noException should be thrownBy exec.runTask(id)
      ctx ! withTask(id) { task =>
        task.state should matchPattern { case _: Task.Completed => // OK.
        }
        Query.unit
      }
    }
  }
}
