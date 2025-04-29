package fauna.model.test

import fauna.atoms._
import fauna.lang.{ TimeBound, Timestamp }
import fauna.model._
import fauna.model.tasks._
import fauna.repo.{ Executor, SchemaContentionException }
import fauna.repo.cassandra.CassandraService
import fauna.repo.query._
import fauna.repo.test.CassandraHelper
import fauna.storage.doc.{ Data, Field }
import fauna.trace.Sampler
import java.util.concurrent.Executors
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._
import scala.util.{ Random, Try }

private object RunAndShutdown {

  private def ignoreExceptions[T](code: => T): Unit =
    try {
      code
    } catch {
      case _: Throwable =>
    }

  /** Runs some code with a dedicated scheduler and shuts the scheduler after immediate execution.
    * All the async threads initialized within this execution should fail due to the scheduler failing
    */
  def apply[T](timeout: FiniteDuration)(code: => T): Try[Unit] = {
    val executor = Executors.newSingleThreadExecutor
    implicit val ec = ExecutionContext.fromExecutor(executor)
    try {
      val future = Future(ignoreExceptions(code))
      Try(Await.result(future, timeout))
    } finally {
      executor.shutdownNow
    }
  }
}

case object TestTask extends Type("test") with Executor.Router[Task, Task.State] {
  def step(task: Task, ts: Timestamp): Query[Task.State] =
    Query.value(Task.Completed())
}

case object TwiceTask extends Type("twice") with Executor.Router[Task, Task.State] {
  val ExecField = Field[Boolean]("executed")

  def step(task: Task, ts: Timestamp): Query[Task.State] =
    if (task.data.getOrElse(ExecField, false)) {
      Query.value(Task.Completed())
    } else {
      Query.value(Task.Runnable(task.data.update(ExecField -> true), task.parent))
    }
}

case object ForkingTask extends Type("forking") {
  val CountField = Field[Option[Int]]("count")

  private def getCount(data: Data) =
    data(CountField).getOrElse(0)

  private def incrCount(data: Data) =
    data.update(CountField -> Some(getCount(data) + 1))

  def fork(task: Task) = {
    task.fork {
      Type.DummyTask.create(task) map { dummy =>
        (Seq(dummy), task.data)
      }
    }
  }

  // this results in the Forked state updating, which used to then get moved to
  // Blocked state on checkpoint.
  override def preStep(task: Task): Query[Task.State] =
    Query.value(task.state.withData(incrCount(task.data)))

  override def step(task: Task, snapshotTS: Timestamp): Query[Task.State] = {
    def work() = {
      val count = getCount(task.data)

      if (count % 2 == 0) {
        throw SchemaContentionException(ScopeID(0), SchemaVersion.Min)
      }

      if (count > 10) {
        Query.value(Task.Completed())
      } else {
        fork(task)
      }
    }

    if (task.isForked) {
      task.joinThen()(_ => work())
    } else {
      work()
    }
  }
}

class TestRouter extends TaskRouter {
  val tasks = Map(
    Type.DummyTask.name -> Type.DummyTask,
    TestTask.name -> TestTask,
    TwiceTask.name -> TwiceTask,
    ForkingTask.name -> ForkingTask)

  override def dispatch(task: Task): Option[Type] =
    super.dispatch(task).orElse(tasks.get(task.name))
}

class TaskExecutorSpec extends Spec {

  val ctx = CassandraHelper.context("model")

  val local = CassandraService.instance.localID.get

  def newExecutor(
    batchSize: Int,
    router: TaskRouter = new TestRouter): TaskExecutor =
    new TaskExecutor(
      repo = ctx,
      router = router,
      service = CassandraService.instance,
      reprioritizer = new TaskReprioritizer(ctx, CassandraService.instance),
      sampler = Sampler.Default,
      batchSize = batchSize,
      consumeTimeSlice = false
    )

  def createTaskInFakeHost(
    scopeID: ScopeID,
    name: String,
    host: HostID,
    msg: Data,
    parent: Option[TaskID],
    isOperational: Boolean = false): Query[Task] = {
    // Create asserts that the host is in the cluster, so this is a quick work
    // around. Only suitable for tests.
    Task.createLocal(scopeID, name, msg, parent, isOperational) flatMap { t =>
      Task.write(t.copy(host = host))
    }
  }

  /** Creates a scenario where the exception mentioned in issue ENG-XXX could occur.
    *
    * To re-create the issue uncomment InterruptedException case match
    * in [[TaskExecutor.foldErrors]] increase the number of iterations from 1 to 20.
    * and you should see this test print the error messages mentioned in the issue ticket.
    */
  "TaskExecutor" - {
    "should not log InterruptedException on shutdown" in {

      val executor = TaskExecutor(ctx)

      // randomly execute step with timeout.
      (1 to 1) map { i =>
        RunAndShutdown(i.millisecond) {
          executor.step()
        }
      }
    }

    "executes tasks FIFO" in {
      val exec = newExecutor(batchSize = 1)

      val total = 10

      val b = Seq.newBuilder[Task]

      (0 until total) foreach { _ =>
        b += ctx ! Task.createLocal(
          Database.RootScopeID,
          TwiceTask.name,
          Data.empty,
          None)
      }

      ctx ! exec.runQueue(local).countT should equal(total)

      val tasks = b.result()

      // Each step() processes a single batch from the head of the
      // runQ, i.e. 1 task.
      (0 until total) foreach { i =>
        var head = ctx ! exec.runQueue(local).headValueT
        head.get should equal(tasks(i).id)

        exec.step()

        // TwiceTask stays on the queue for two executions; it should
        // stay on the head in FIFO mode.
        head = ctx ! exec.runQueue(local).headValueT
        head.get should equal(tasks(i).id)
        exec.step()
      }

      ctx ! exec.runQueue(local).countT should equal(0)
    }

    "executes tasks one batch at a time" in {
      val total = 10
      val batchSize = 2

      val exec = newExecutor(batchSize)

      (0 until total) foreach { _ =>
        ctx ! Task.createLocal(Database.RootScopeID, TestTask.name, Data.empty, None)
      }

      ctx ! exec.runQueue(local).countT should equal(total)

      // Each step() processes a single batch from the head of the
      // runQ.
      (0 until total / batchSize) foreach { i =>
        ctx ! exec.runQueue(local).countT should equal(total - i * batchSize)
        exec.step()
      }

      ctx ! exec.runQueue(local).countT should equal(0)
    }

    "executes tasks in descending priority order" in {
      val total = 10

      val exec = newExecutor(batchSize = 1)

      val b = Seq.newBuilder[Task]

      (0 until total) foreach { _ =>
        val t = ctx ! Task.createLocal(
          Database.RootScopeID,
          TestTask.name,
          Data.empty,
          None)

        // Update each task with a random priority.
        b += ctx ! Task.write(t.copy(priority = Random.nextInt()))
      }

      // The queue should be sorted in descending priority order.
      val tasks = b.result().sortBy { t => -t.priority } map { _.id }

      ctx ! exec.runQueue(local).countT should equal(total)
      ctx ! exec.runQueue(local).flattenT should equal(tasks)

      // Drain the queue.
      (0 until total) foreach { i =>
        ctx ! exec.runQueue(local).countT should equal(total - i)
        exec.step()
      }

      ctx ! exec.runQueue(local).countT should equal(0)
    }

    "does not double-block forked tasks" in {
      val exec = newExecutor(batchSize = 1)

      val t = ctx ! Task.createLocal(
        Database.RootScopeID,
        ForkingTask.name,
        Data.empty,
        None)

      while ((exec.repo ! getTasks()).nonEmpty) {
        exec.step()
      }

      val t0 = ctx ! Task.get(t.id).map(_.get)

      t0.isCompleted shouldBe true
    }
  }

  "TaskStealer" - {
    "steal() function steals a task" in {
      val exec = newExecutor(batchSize = 1)

      val otherHost = HostID.randomID

      val t = ctx ! createTaskInFakeHost(
        Database.RootScopeID,
        TestTask.name,
        otherHost,
        Data.empty,
        None)

      ctx ! exec.runQueue(local).countT should equal(0)
      ctx ! exec.stealer.steal(t.id)
      ctx ! exec.runQueue(local).countT should equal(1)

      // Drain the queue.
      exec.step()
      ctx ! exec.runQueue(local).countT should equal(0)
    }

    "recvSteal() steals tasks" in {
      val exec = newExecutor(batchSize = 1)

      val otherHost = HostID.randomID

      // Put 3 tasks on local, and 2 on otherHost
      val t1 =
        ctx ! Task.createLocal(Database.RootScopeID, TestTask.name, Data.empty, None)
      val t2 =
        ctx ! Task.createLocal(Database.RootScopeID, TestTask.name, Data.empty, None)
      ctx ! Task.createLocal(Database.RootScopeID, TestTask.name, Data.empty, None)
      ctx ! createTaskInFakeHost(
        Database.RootScopeID,
        TestTask.name,
        otherHost,
        Data.empty,
        None)
      ctx ! createTaskInFakeHost(
        Database.RootScopeID,
        TestTask.name,
        otherHost,
        Data.empty,
        None)

      ctx ! exec.runQueue(local).countT should equal(3)
      ctx ! exec.runQueue(otherHost).countT should equal(2)

      // The returned list should match the tasks stolen.
      exec.stealer.recvSteal(
        otherHost,
        Vector(t1.id, t2.id),
        TimeBound(10.seconds)) should equal(Vector(t1.id, t2.id))

      ctx ! exec.runQueue(local).countT should equal(1)
      ctx ! exec.runQueue(otherHost).countT should equal(4)

      // Drain
      exec.step()
      ctx ! exec.runQueue(local).countT should equal(0)
    }

    "recvSteal() won't steal tasks it doesn't own" in {
      val exec = newExecutor(batchSize = 1)

      val otherHost = HostID.randomID

      // Put 1 tasks on local, and 1 on otherHost
      val t1 =
        ctx ! Task.createLocal(Database.RootScopeID, TestTask.name, Data.empty, None)
      val t2 = ctx ! createTaskInFakeHost(
        Database.RootScopeID,
        TestTask.name,
        otherHost,
        Data.empty,
        None)

      ctx ! exec.runQueue(local).countT should equal(1)
      ctx ! exec.runQueue(otherHost).countT should equal(1)

      // The returned list should match the tasks stolen.
      exec.stealer.recvSteal(
        otherHost,
        Vector(t1.id, t2.id),
        TimeBound(10.seconds)) should equal(Vector(t1.id))

      ctx ! exec.runQueue(local).countT should equal(0)
      ctx ! exec.runQueue(otherHost).countT should equal(2)
    }

    "recvSteal() won't steal non-runnable tasks" in {
      val exec = newExecutor(batchSize = 1)

      val otherHost = HostID.randomID
      val localHost = CassandraService.instance.localID.get

      // Put 2 tasks on this host
      var t1 =
        ctx ! Task.createLocal(Database.RootScopeID, TestTask.name, Data.empty, None)
      val t2 =
        ctx ! Task.createLocal(Database.RootScopeID, TestTask.name, Data.empty, None)

      ctx ! exec.runQueue(local).countT should equal(2)
      ctx ! exec.runQueue(otherHost).countT should equal(0)

      // Both tasks start on this host
      exec.getTask(t1.id).get.host should equal(localHost)
      exec.getTask(t2.id).get.host should equal(localHost)

      // Pause t1, so it won't be moved
      t1 = ctx ! t1.pause("test")

      ctx ! exec.runQueue(local).countT should equal(1)
      ctx ! exec.runQueue(otherHost).countT should equal(0)

      // This simulates the otherHost sending a request for tasks t1 and t2,
      // and the local host handling that request, via the recvSteal() function.
      // The returned list should match the tasks stolen.
      exec.stealer.recvSteal(
        otherHost,
        Vector(t1.id, t2.id),
        TimeBound(10.seconds)) should equal(Vector(t2.id))

      ctx ! exec.runQueue(local).countT should equal(0)
      ctx ! exec.runQueue(otherHost).countT should equal(1)

      // t2 got moved to the other host, but t1 should stay in the same place
      exec.getTask(t1.id).get.host should equal(localHost)
      exec.getTask(t2.id).get.host should equal(otherHost)

      // Cleanup
      t1 = ctx ! t1.unpause()
      exec.runTask(t1.id)

      (ctx ! Task.get(t1.id)).get.isCompleted should be(true)
    }

    "recvSteal() won't steal tasks in the processing list" in {
      val exec = newExecutor(batchSize = 1)

      val otherHost = HostID.randomID

      // Put 3 tasks on local
      val t1 =
        ctx ! Task.createLocal(Database.RootScopeID, TestTask.name, Data.empty, None)
      val t2 =
        ctx ! Task.createLocal(Database.RootScopeID, TestTask.name, Data.empty, None)
      val t3 =
        ctx ! Task.createLocal(Database.RootScopeID, TestTask.name, Data.empty, None)

      ctx ! exec.runQueue(local).countT should equal(3)
      ctx ! exec.runQueue(otherHost).countT should equal(0)

      // We are processing t2, so the below recvSteal shouldn't steal it.
      exec.processing.tryMarkProcessing(t2.id :: Nil)

      // We are stealing t3, so the below recvSteal shouldn't steal it nor drain
      // should run it.
      exec.processing.tryMarkStealing(t3.id :: Nil)

      // The returned list should match the tasks stolen.
      exec.stealer.recvSteal(
        otherHost,
        Vector(t1.id, t2.id, t3.id),
        TimeBound(10.seconds)
      ) should equal(Vector(t1.id))

      ctx ! exec.runQueue(local).countT should equal(2)
      ctx ! exec.runQueue(otherHost).countT should equal(1)

      // NB. `tryMarkProcessing` was supposed to be called by the task executor.
      // Here we'll unmark t2 so it can do the marking and unmarking itself and run
      // the task.
      exec.processing.unmark(t2.id :: Nil)

      // Executes only t2 since t1 was stolen and t3 is marked as stealing.
      exec.step()
      ctx ! exec.runQueue(local).countT should equal(1)
    }
  }
}
