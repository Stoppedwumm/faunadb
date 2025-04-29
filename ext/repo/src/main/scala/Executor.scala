package fauna.repo

import fauna.atoms.{ HostID, TaskID }
import fauna.exec.BackOff
import fauna.lang.{ AdminControl, Timestamp }
import fauna.logging.ExceptionLogging
import fauna.repo.query.Query
import fauna.trace._
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import scala.util.Try

trait Executor extends ExceptionLogging {

  val name: String
  val stealer: WorkStealing

  def repo: RepoContext
  def start(): Unit
  def stop(graceful: Boolean): Unit

  /** Makes one trip through the runQueue, processing each runnable task.
    *
    * If `wakeup` is true, indicates that this step was triggered by
    * `wakeup()`. In such cases, the executor should spin on the queue
    * until a runnable task is found - one will show up soon.
    */
  def step(wakeup: Boolean = false): BackOff.Operation

  /** Notify the executor of pending work; if it is asleep, it should
    * wake and re-check the runQ as of snapshotTS.
    */
  def wakeup(snapshotTS: Timestamp): Unit

  def run[T](
    r: RepoContext,
    q: Query[T],
    timeout: FiniteDuration,
    prefix: String,
    minSnapTime: Timestamp = Timestamp.Epoch)(log: (=> String) => Unit)(
    recover: => T): T =
    foldErrors(prefix, r.runSynchronously(q, timeout, minSnapTime).value)(log)(
      recover)

  /** The list of runnable tasks waiting in `host`'s queue.
    */
  def runQueue(host: HostID, size: Int): PagedQuery[Iterable[TaskID]]

  /** The number of tasks that will be stolen on healthy steal attempts.
    */
  def stealSize: Int

  /** The number of tasks that will be stolen on unhealthy steal attempts.
    */
  def unhealthyStealSize: Int

  /** Attempt to execute the task given by `id`, returning the task's
    * next state or an error.
    */
  def tryTask(id: TaskID)(implicit ctl: AdminControl): Try[Any]

  /** Recover from commonly-encountered, non-fatal errors during task
    * execution. `log` is a handle for logging stuff ¯\_(ツ)_/¯.
    */
  protected def foldErrors[T](prefix: String, thunk: => T)(log: (=> String) => Unit)(
    recover: => T): T = {
    val tracer = GlobalTracer.instance

    try {
      repo.stats.time(s"$name.${prefix}Time") {
        thunk
      }
    } catch {
      case _: TimeoutException =>
        log("failure due to timeout")
        repo.stats.incr(s"$name.${prefix}Timeouts")
        tracer.activeSpan foreach { _.setStatus(DeadlineExceeded("timeout")) }
        recover
      case _: ContentionException =>
        log("failure due to contention")
        repo.stats.incr(s"$name.${prefix}Contentions")
        tracer.activeSpan foreach { _.setStatus(Aborted("contention")) }
        recover
      case _: InterruptedException =>
        log("failure due to interruption")
        repo.stats.incr(s"$name.${prefix}Interruptions")
        tracer.activeSpan foreach { _.setStatus(Aborted("interrupted")) }
        recover
      case _: Executor.TaskProgressException =>
        log("failure due to lack of progress")

        // Increment a global state for a global monitor.
        repo.stats.incr(s"Task.ProgressFailures")
        repo.stats.incr(s"$name.${prefix}ProgressFailures")
        tracer.activeSpan foreach { _.setStatus(Aborted("progress")) }
        recover
      case e: Throwable =>
        // Just log it once, I guess.
        log("failure: see exception log")
        logException(e)
        repo.stats.incr(s"$name.${prefix}Errors")
        tracer.activeSpan foreach { _.setStatus(InternalError(e.getMessage)) }
        if (NonFatal(e)) {
          recover
        } else {
          throw e
        }
    }
  }

  protected def foldErrors[T](thunk: => T)(log: (=> String) => Unit)(
    recover: => T): T =
    foldErrors("", thunk)(log)(recover)

}

object Executor {
  // Lives here because this is where task exceptions get handled.
  // More properly belongs to the Type flavor of task.
  case class TaskProgressException(name: String, id: TaskID)
      extends Exception(
        s"Unable to make progress on paginated task $name with id:$id using minimum page size of 1")

  type Router[T, S] = (T, Timestamp) => Query[S]
}
