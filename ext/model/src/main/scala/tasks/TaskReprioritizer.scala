package fauna.model.tasks

import fauna.atoms.AccountID
import fauna.exec.{ BackOff, LoopThreadService }
import fauna.flags.EnableIndexBuildPriorities
import fauna.lang.syntax.AccumulateOp
import fauna.lang.LoggingSyntax
import fauna.model.Task
import fauna.repo.{ ContentionException, RepoContext }
import fauna.repo.cassandra.CassandraService
import fauna.repo.query.Query
import fauna.trace._
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration._
import scala.concurrent.TimeoutException
import scala.util.control.NonFatal

object TaskReprioritizer {

  /** This is the amount we decrement the priority by for tasks that have custom
    * priority. This should be large enough that child tasks don't interfere
    * with the task priority.
    */
  val TaskPriorityDecrement = 10
}

final class TaskReprioritizer(
  val repo: RepoContext,
  service: CassandraService,
  sampler: Sampler = Sampler.Default,
  initSleepTime: FiniteDuration = 10.seconds,
  maxSleepTime: FiniteDuration = 5.minutes
) extends LoopThreadService("TaskReprioritizer")
    with LoggingSyntax {
  val log = getLogger

  private[this] val tracer = GlobalTracer.instance.withSampler(sampler)
  private[this] val backOff = new BackOff(initSleepTime, maxSleepTime)

  override protected def loop(): Unit = {
    val span = tracer
      .buildSpan("task.reprioritizer.loop")
      .ignoreParent()
      .withKind(Consumer)
      .start()

    val scope = tracer.activate(span)

    try {
      // Ignore the wakeup flag, as we do the same thing regardless of if we
      // were woken or not.
      backOff { _ =>
        // If storage is up and this host is a cluster member, check the queue.
        if (service.isRunning && service.localID.isDefined) {
          step()
        } else {
          BackOff.Operation.Wait
        }
      }
    } finally {
      scope foreach {
        _.close()
      }
    }
  }

  // Stores all the completed tasks. This is cleared by this thread loop
  // each time it is added to, or possibly async if needed.
  private[this] val finishedTasks = new ConcurrentHashMap[AccountID, Set[String]]

  /** Updates the priorities for all other tasks created by the author of
    * the given task.
    *
    * This stores the account id and name of task in a concurrent map. The
    * task itself is not stored, so you can mutate this task after calling
    * this without any races happening.
    */
  def finishTask(task: Task): Unit = {
    finishedTasks.compute(
      task.accountID,
      (_, names) =>
        if (names eq null) {
          Set(task.name)
        } else {
          names + task.name
        })
  }

  // Returns true if we should retry immediately, and false if we should backoff.
  def step(): BackOff.Operation = {
    try {
      if (finishedTasks.isEmpty) {
        BackOff.Operation.Wait
      } else {
        reprioritize()
        BackOff.Operation.Break
      }
    } catch {
      case _: TimeoutException =>
        repo.stats.incr(s"$name.TaskReprioritizerTimeouts")
        tracer.activeSpan foreach { _.setStatus(DeadlineExceeded("timeout")) }
        log.warn("TaskReprioritizer: Timeout, retrying")
        BackOff.Operation.Break
      case _: ContentionException =>
        repo.stats.incr(s"$name.TaskReprioritizerContentions")
        tracer.activeSpan foreach { _.setStatus(Aborted("contention")) }
        log.warn("TaskReprioritizer: Contention, backing off")
        BackOff.Operation.Wait
      case _: InterruptedException =>
        repo.stats.incr(s"$name.TaskReprioritizerInterruptions")
        tracer.activeSpan foreach { _.setStatus(Aborted("interrupted")) }
        log.warn("TaskReprioritizer: Interrupted, backing off")
        BackOff.Operation.Wait
      case NonFatal(e) =>
        logException(e)
        repo.stats.incr(s"$name.TaskReprioritizerErrors")
        tracer.activeSpan foreach { _.setStatus(InternalError(e.getMessage)) }
        log.warn("TaskReprioritizer: Exception, backing off")
        BackOff.Operation.Wait
    }
  }

  /** Reprioritizes all tasks that this ThreadReprioritizer is aware of.
    */
  def reprioritize(): Unit = {
    val enabled = runQ(repo.hostFlag(EnableIndexBuildPriorities))

    if (!enabled) {
      return
    }

    val entries = finishedTasks.entrySet()
    entries forEach { entry =>
      val accountID = entry.getKey
      val names = entry.getValue

      names foreach { name =>
        if (TaskRouter.hasCustomPriority(name)) {
          val query = Task
            .getAllByAccount(accountID, name)
            .splitT(repo.taskReprioritizerLimit)
            .initValuesT
            .flatMap { page =>
              var prio = 0
              page.map { task =>
                task.state match {
                  // If no one has touched this task, we write it's priority.
                  // We only decrease `prio` here, as we want all the tasks
                  // that have not been touched to have linearly decreasing
                  // priority.
                  case Task.Runnable(_, _) =>
                    val q = task.reprioritize(prio)
                    prio -= TaskReprioritizer.TaskPriorityDecrement
                    q
                  // If this task has progressed, then someone is already
                  // processing it, so we don't touch it.
                  case _ => Query.unit
                }
              }.sequence
            }

          tracer.withSpan("task.reprioritizer.query") {
            log.info(
              f"TaskReprioritizer: Reprioritizing tasks for account $accountID")
            runQ(query)
            log.info(
              f"TaskReprioritizer: Finished reprioritizing tasks for account $accountID")
          }
        }
      }

      finishedTasks.remove(accountID)
    }
  }

  def runQ[T](query: Query[T]): T = {
    repo.stats.time(s"$name.TaskReprioritizerTime") {
      repo.runSynchronously(query, maxSleepTime).value
    }
  }
}
