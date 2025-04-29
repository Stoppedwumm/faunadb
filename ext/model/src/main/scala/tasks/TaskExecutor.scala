package fauna.model.tasks

import fauna.ast.RefL
import fauna.atoms._
import fauna.auth.RootAuth
import fauna.codex.json._
import fauna.exec._
import fauna.lang.{ AdminControl, ConsoleControl, Timestamp, Timing }
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.model._
import fauna.model.util.TaskLogging
import fauna.repo._
import fauna.repo.cassandra._
import fauna.repo.query.Query
import fauna.stats._
import fauna.trace._
import io.netty.buffer.ByteBufAllocator
import java.util.concurrent.{
  ForkJoinPool,
  ForkJoinTask,
  RecursiveTask,
  TimeoutException
}
import java.util.concurrent.atomic.AtomicReference
import java.util.ArrayList
import scala.annotation.unused
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.Try

object TaskExecutor {
  // We want to be able to turn on verbose logging just for the executor but
  // log4J makes it more work than it's worth. Hence this kludge.
  @volatile private var enableVerboseLogging = false

  def setVerboseLogging(enable: Boolean)(implicit @unused ctl: ConsoleControl) =
    enableVerboseLogging = enable

  private sealed trait StepResult
  private object StepResult {
    case object EmptyQueue extends StepResult
    final case class TasksRan(n: Int) extends StepResult
  }

  def apply(
    repo: RepoContext,
    router: TaskRouter = TaskRouter,
    service: CassandraService = CassandraService.instance
  ): TaskExecutor = {
    val reprioritizer = new TaskReprioritizer(repo, service)
    new TaskExecutor(repo, router, service, reprioritizer)
  }

  def repoWithTaskPriority(
    repo: RepoContext,
    task: Task,
    timeout: FiniteDuration): RepoContext =
    if (task.scopeID == repo.priorityGroup.id) {
      repo
    } else {
      repo.runSynchronously(Database.forScope(task.scopeID), timeout).value match {
        case Some(db) => repo.withPriority(db.priorityGroup)
        case None     => repo
      }
    }
}

/** The lowest component in the task execution stack, this finds
  * pending tasks for this host, and executes their kernel.
  *
  * If a task completes successfully, the executor will transition the
  * task state to completed. If the task yields a continuation, it
  * will update the task payload with the continuation. The task may
  * be re-executed immediately, or at some undefined time in the
  * future.
  */
final class TaskExecutor(
  val repo: RepoContext,
  val router: TaskRouter,
  service: CassandraService,
  val reprioritizer: TaskReprioritizer,
  sampler: Sampler = Sampler.Default,
  parallelism: Int = 1,
  priority: Int = Thread.MIN_PRIORITY,
  yieldTime: FiniteDuration = 5.minutes,
  initSleepTime: FiniteDuration = 10.seconds,
  maxSleepTime: FiniteDuration = 5.minutes,
  batchSize: Int = 32,
  val stealSize: Int = 32,
  val unhealthyStealSize: Int = 1,
  consumeTimeSlice: Boolean = true,
  enableStealing: Boolean = true)
    extends LoopThreadService("TaskExecutor", priority = priority)
    with Executor
    with HealthCheck
    with TaskLogging {
  import TaskExecutor.StepResult

  private[this] val tracer = GlobalTracer.instance.withSampler(sampler)

  private[this] lazy val pool = new ForkJoinPool(parallelism)

  private[this] val lastSeenTS = new AtomicReference(Timestamp.Min)

  StatsRecorder.polling(30.seconds) {
    if (service.isRunning) {
      service.localID foreach { id =>
        val runQ = runQueue(id).countT

        try {
          val size = repo.runSynchronously(runQ, 20.seconds).value
          repo.stats.count("TaskExecutor.Queue.Size", size)
        } catch {
          case _: TimeoutException =>
            log.warn("TaskExecutor: Metrics lookup timed out")
        }
      }
    }
  }

  override def start() = {
    super.start()
    reprioritizer.start()
  }
  override def stop() = {
    reprioritizer.stop()
    super.stop()
  }

  val log = getLogger

  // Logs at debug level, or at info level if verbose logging is on.
  // Additionally, all messages logged will be prefixed.
  private val verbosePrefix = "TaskExecutor: Verbose"
  private def logVerbose(msg: => String) =
    if (TaskExecutor.enableVerboseLogging) {
      log.info(s"$verbosePrefix: $msg")
    } else if (log.isDebugEnabled) {
      log.debug(s"$verbosePrefix: $msg")
    }

  private def logVerboseWithTask(id: TaskID)(msg: => String): Unit =
    logVerbose(s"task $id: $msg")

  val backOff = new BackOff(initSleepTime, maxSleepTime)

  val stealer = new TaskStealer(this, maxSleepTime, service, sampler, enableStealing)
  val processing = new ProcessingList()

  protected def loop(): Unit = {

    val span = tracer
      .buildSpan("task.loop")
      .ignoreParent()
      .withKind(Consumer)
      .start()

    val scope = tracer.activate(span)

    try {
      localHealthCheck()
      backOff { wakeup =>
        // If storage is up and this host is a cluster member, check the runQ.
        if (service.isRunning && service.localID.isDefined) {
          step(wakeup)
        } else {
          BackOff.Operation.Wait
        }
      }
    } finally {
      scope foreach { _.close() }
    }
  }

  def wakeup(snapshotTS: Timestamp): Unit = {
    logVerbose(s"woken up for new task at $snapshotTS")
    lastSeenTS.getAndUpdate { cur => cur max snapshotTS }
    backOff.wakeup()
  }

  def step(wakeup: Boolean): BackOff.Operation = {
    require(service.isRunning, "CassandraService isn't started. Cannot run tasks.")

    logVerbose(s"check runQ (wakeup=$wakeup)")
    consume(runQueue(service.localID.get)) match {
      case StepResult.TasksRan(proc) =>
        logVerbose(s"check runQ: processed $proc tasks")
        BackOff.Operation.Break

      case StepResult.EmptyQueue =>
        // If there are no tasks currently in this host's queue, attempt to steal
        // some tasks.
        implicit val ec = ImmediateExecutionContext
        // If any tasks were stolen, continue processing - this queue now has tasks.
        // If not, back off.
        tracer.withSpan("task.attempt-steal") {
          // attemptSteal() returns true if tasks were stolen. This means we want to
          // break, and since this is in a loop, we will immediately retry.
          run(
            repo,
            stealer.attemptSteal() map BackOff.Operation.breakIf,
            backOff.currentDuration,
            "AttemptSteal"
          )(logVerbose)(BackOff.Operation.Wait)
        }
    }
  }

  def runQueue(host: HostID): PagedQuery[Iterable[TaskID]] =
    runQueue(host, batchSize)

  def runQueue(host: HostID, size: Int): PagedQuery[Iterable[TaskID]] =
    Task.getRunnableIDsByHost(host) splitT size

  /** Attempt to execute the task given by `id`, returning the task's
    * next state or an error.
    */
  def tryTask(id: TaskID)(implicit ctl: AdminControl): Try[Task.State] =
    Try {
      val q = Query.snapshotTime flatMap { ts =>
        Task.get(id, ts) flatMap {
          case Some(task) => router(task, ts)
          case None => throw new RuntimeException(s"Task $id does not exist at $ts")
        }
      }

      tracer.withSpan("task.try") {
        // NB: this will not emit stats, nor will it save the task's new
        // state.
        repo.runSynchronously(q, maxSleepTime).value
      }
    }

  /** Executes one page of the given by `pageQ`. Returns the number of tasks which
    * processed successfully.
    *
    * Note that races with task stealer may cause more pages to be polled until
    * `consume` finds a page with tasks it can process.
    */
  private def consume(pageQ: PagedQuery[Iterable[TaskID]]): StepResult = {
    import StepResult._

    val pageOpt =
      tracer.withSpan("task.page") {
        // NB. Tasks might wake up due to streaming notifications with a fresh txn
        // time that isn't observed by all nodes yet. Make sure we poll the run queue
        // with the last seen timestamp so we observe tasks that came from a
        // streaming notification.
        run(
          repo,
          pageQ.map(Option(_)),
          maxSleepTime,
          "Page",
          lastSeenTS.get
        )(logVerbose)(None)
      }

    // Return NoTasks only if we know for sure the run queue is empty (no failures).
    val page = pageOpt match {
      case Some(page) => if (page.value.nonEmpty) page else return EmptyQueue
      case None       => return TasksRan(0) // failed to load page.
    }

    // NB. Racing with task stealer can cause all tasks in the page to be stolen. Try
    // to process tasks from next page, or else return 0 since there are no more
    // tasks in the run queue.
    val taskIDs = processing.tryMarkProcessing(page.value)
    if (taskIDs.isEmpty) {
      return page.next match {
        case Some(next) => consume(next())
        case None       => EmptyQueue
      }
    }

    try {
      // Capture the tracing ctx from this thread to enable tracing within each task.
      val shouldTrace = isTracingEnabled
      log.debug(
        s"TaskExecutor: processing ${taskIDs.size} tasks: ${taskIDs mkString ", "}")

      val root = new RecursiveTask[Int] {
        private def mkSubtask(id: TaskID): RecursiveTask[Int] = { () =>
          val span = tracer
            .buildSpan("task.execute")
            .withKind(Consumer)
            .start()

          val scope = tracer.activate(span)

          try {
            if (shouldTrace) {
              foldErrors(printTrace(runTask(id)))(logVerboseWithTask(id))(0)
            } else {
              foldErrors(runTask(id))(logVerboseWithTask(id))(0)
            }
          } finally {
            scope foreach {
              _.close()
            }
          }
        }

        def compute(): Int = {
          val todo = new ArrayList[RecursiveTask[Int]](taskIDs.size)

          taskIDs foreach { id =>
            todo.add(mkSubtask(id))
          }

          val results = ForkJoinTask.invokeAll(todo)

          var complete = 0
          results forEach { t =>
            if (t.isCompletedNormally) {
              complete += t.get()
            }
          }

          complete
        }
      }

      TasksRan(pool.invoke(root))
    } finally {
      processing.unmark(taskIDs)
    }
  }

  // Public for unit tests
  private[model] def getTask(id: TaskID): Option[Task] =
    run(repo, Task.get(id), maxSleepTime, "GetTask")(logVerbose)(None)

  private def checkpointTask(
    task: Task,
    next: Task.State,
    elapsedMillis: Long): Integer =
    tracer.withSpan("task.update") {
      if (task.state != next) {
        getTask(task.id) match {
          case Some(task) if task.isPending && !task.isPaused =>
            logVerbose(s"saving progress for task ${task.id}")
            val q = {
              val updateQ = next match {
                case Task.Runnable(data, p) =>
                  Task.runnable(task, data, p, elapsedMillis)
                case Task.Paused(reason, previous, p) =>
                  Task.paused(task, reason, previous, p, elapsedMillis)

                // both Forked and Blocked -> block(), so users of the
                // task framework can always return and receive Forked,
                // without needing to concern themselves with how the
                // Blocked -> Forked transition happens. In practice, task
                // users are unlikely to ever use Blocked.
                case Task.Forked(tasks, data, _) =>
                  Task.maybeBlock(task, tasks, data, elapsedMillis)
                case Task.Blocked(tasks, data, _) =>
                  Task.maybeBlock(task, tasks, data, elapsedMillis)
                case Task.Completed(data) =>
                  router.complete(task, data, elapsedMillis)
                case Task.Cancelled(data) =>
                  router.cancel(task, data)
              }

              updateQ recoverWith { case NonFatal(e) =>
                logVerbose(s"failed to update ${task.id}: ${e.getMessage}")
                Query.fail(e)
              }
            }

            run(repo, q map { _ => 1 }, maxSleepTime, "UpdateTask")(logVerbose)(0)
          case _ => 0
        }
      } else {
        0
      }
    }

  // Public for unit tests
  private[model] def runTask(id: TaskID): Integer =
    getTask(id) match {
      // NB. Task stealer may race and grab a task before we get a chance to run it.
      // Double check if the given task still belongs to the local host prior to
      // executing anything.
      case Some(init) if !service.localID.contains(init.host) =>
        logVerbose(s"tried to run task $id but I no longer own it")
        0

      case Some(init) if !init.isRunnable =>
        logVerbose(s"tried to run task $id but it is not runnable")
        0

      case Some(init) =>
        logVerbose(s"beginning run of task $id: $init")
        val start = Timing.start
        val statsBuffer = new StatsRequestBuffer
        var task = init
        var execution = 0

        // returns true if task made progress
        def runPhase(phase: String)(thnk: => Task.State): Boolean = {
          logVerbose(
            s"running $phase for task $id (${start.elapsed.toMillis}ms spent in step loop)")

          val state = thnk

          if (state == task.state) {
            // the task stalled
            return false
          }

          logVerbose(
            s"updating progress after $phase for task $id: new state $state")

          // register progress in memory
          task = task.copy(state = state)

          // Inform the reprioritizer when a task completes.
          if (task.isCompleted && router.hasCustomPriority(task.name)) {
            reprioritizer.finishTask(task)
          }

          true
        }

        try {
          tracer.withSpan(s"task.${init.name}.execute") {
            if (task.elapsedMillis == 0) {
              val delay = Clock.time.difference(task.createdAt).toMillis
              repo.stats.timing("Task.Queue.Time", delay)
              tracer.activeSpan foreach { _.addAttribute("queue_time", delay) }
            }

            runPhase("pre-step") {
              repo.runSynchronously(router.preStep(task), maxSleepTime).value
            }

            // proceed to step task if it is runnable
            var continue = task.isRunnable

            while (continue && start.elapsed < yieldTime) {
              repo.stats.time(
                s"TaskExecutor.Time",
                s"TaskExecutor.Time.${task.statName}") {
                val q = router(task, Clock.time)
                val stepRepo = TaskExecutor
                  .repoWithTaskPriority(repo, task, maxSleepTime)
                  .withStats(
                    StatsRecorder.Multi(
                      Seq(
                        statsBuffer,
                        repo.stats,
                        repo.stats
                          .scoped(s"Tasks.${task.statName}")
                          .filtered(QueryMetrics.BytesReadWrite))))

                val stateChanged = runPhase("step") {
                  stepRepo.runSynchronously(q, maxSleepTime).value
                }

                if (!stateChanged) {
                  repo.stats.incr(s"TaskExecutor.StalledProgress.${task.statName}")
                  log.warn(
                    s"TaskExecutor: progress stalled processing ${task.statName} ${task.id}")
                }

                // don't spin on forked tasks - no progress would be made.
                continue &&= consumeTimeSlice
                continue &&= stateChanged
                continue &&= task.isRunnable
                continue &&= !task.isForked
              }

              repo.stats.incr(s"TaskExecutor.Processed.${task.statName}")
              repo.stats.incr(s"TaskExecutor.Processed")
            }
          }
        } finally {
          runPhase("pre-checkpoint") {
            repo.runSynchronously(router.preCheckpoint(task), maxSleepTime).value
          }

          val elapsed = start.elapsedMillis

          if (router.isLoggable(task) && !task.isOperational) {
            logVerbose(s"logging progress for task $id")

            val alloc = ByteBufAllocator.DEFAULT
            val buf = alloc.buffer

            try {

              /** Because the auth query is looking up the database we leverage that to grab the global id path
                * off the database because we need to add that to the task.log
                */
              val authQ = Database.forScope(task.scopeID) flatMap {
                case Some(db) =>
                  RenderContext
                    .renderTo(
                      RootAuth,
                      APIVersion.V212,
                      Timestamp.Epoch,
                      Map.empty,
                      buf,
                      RefL(db.parentScopeID, db.id),
                      pretty = false)
                    .map(_ => db.globalIDPath)
                case None => Query.value(Nil)
              }

              val globalIDPath = repo.runSynchronously(authQ, maxSleepTime).value

              logTask(
                task,
                JSRawValue(buf.toUTF8String),
                QueryMetrics(elapsed, statsBuffer),
                globalIDPath
              )
            } finally {
              buf.release()
            }
          }

          repo.stats.timing("Task.Time", elapsed)

          execution = checkpointTask(init, task.state, elapsed)
          logVerbose(
            s"task $id updated: processing of this task is finished for this loop")
        }

        execution
      case _ =>
        logVerbose(s"tried to run task $id but didn't find it")
        0
    }
}
