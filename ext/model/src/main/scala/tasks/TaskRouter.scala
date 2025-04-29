package fauna.model.tasks

import fauna.atoms.AccountID
import fauna.flags.RunTasks
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.account.Account
import fauna.model.Task
import fauna.repo.query.Query
import fauna.repo.Executor
import fauna.storage.doc.Data
import scala.util.control.NonFatal

/** TaskRouter used by default in prod. In testing, this should be overwritten
  * if custom tasks are used.
  */
object TaskRouter extends TaskRouter

/** Used by the TaskExecutor to dispatch tasks by "type". Any tasks of
  * unknown type are skipped.
  */
class TaskRouter extends Executor.Router[Task, Task.State] {
  private[this] val log = getLogger

  private[this] val types = Seq(
    Type.DummyTask,
    IndexBuild.RootTask,
    IndexBuild.ScanTask,
    IndexBuild.IndexTask,
    Repair.RootTask,
    Repair.RepairLookups,
    Repair.RepairReverseLookups,
    Repair.RestoreReplicationIncremental,
    IndexRebuild.RootTask,
    IndexRebuild.BuildLocalMetadataIndexes,
    IndexRebuild.BuildLocalDocumentIndexes,
    IndexRebuild.DistributeLocalMetadataIndexes,
    IndexRebuild.DistributeLocalDocumentIndexes,
    Reindex.Root,
    Reindex.ReindexDocs,
    DocumentRepair.Root,
    DocumentRepair.Scan,
    IndexSwap.Root,
    MigrationTask.Root,
    MigrationTask.Scan,
    MigrationTask.ByIndex.Root,
    MigrationTask.ByIndex.Leaf,
    ExportDataTask.Root,
    ExportDataTask.TableScan,
    ExportDataTask.ByIndex.Root,
    ExportDataTask.ByIndex.Leaf
  ) map { t => t.name -> t } toMap

  protected def dispatch(task: Task): Option[Type] = types.get(task.name)

  /** Dispatch and apply a task's thunk, returning an optional
    * continuation.
    */
  def apply(task: Task, snapshotTS: Timestamp) =
    Account.flagForScope(task.scopeID, RunTasks) flatMap { runTask =>
      if (!runTask) {
        Query.stats map { stats =>
          log.warn(
            s"Pausing ${task.name} ${task.id} - task execution is disabled for ${task.scopeID}.")
          stats.incr("TaskExecutor.Ignored")
          Task.Paused(
            s"task execution is disabled for ${task.scopeID}",
            task.state,
            task.parent)
        }
      } else {
        dispatch(task) match {
          case Some(fn) =>
            fn(task, snapshotTS) map { state =>
              log.debug(
                s"Processing ${task.name} ${task.id} ${task.state} => $state")
              state
            }
          case None =>
            Query.stats map { stats =>
              log.debug(s"Dropping ${task.name} ${task.id})")
              stats.incr("TaskExecutor.Dropped")
              // Task type no longer exists, so we're done.
              Task.Completed()
            }
        }
      }
    }

  /** Dispatch and cancel a task and any children (if any). If the
    * task was unable to be cancelled, return its continuation.
    */
  def cancel(task: Task, data: Option[Data]): Query[Unit] =
    if (task.isCompleted) {
      log.warn(s"Attempted to cancel already completed/cancelled task ${task.id}")
      Query.unit
    } else {
      Query.stats flatMap { stats =>
        dispatch(task) match {
          case Some(fn) =>
            fn.onCancel(task) flatMap { _ =>
              task.cancel(data) map { task =>
                log.debug(s"Canceling ${task.name} ${task.id}")
                stats.incr("TaskExecutor.Canceled")
              }
            }
          case None =>
            stats.incr("TaskExecutor.Dropped")
            Query.unit
        }
      }
    }

  /** Dispatch and complete a task.
    */
  def complete(task: Task, data: Option[Data], elapsedMillis: Long): Query[Task] = {
    if (task.isCompleted) {
      log.warn(s"Attempted to complete already completed/cancelled task ${task.id}")
      Query.value(task)
    } else {
      val callback = dispatch(task) match {
        case Some(fn) => fn.onComplete(task)
        case None     => Query.unit
      }

      callback flatMap { _ =>
        Task.complete(task, data, elapsedMillis)
      } recoverWith { case NonFatal(ex) =>
        task.pause(s"Task failed to complete: ${ex.getMessage}")
      }
    }
  }

  def preStep(task: Task): Query[Task.State] =
    dispatch(task) match {
      case Some(fn) => fn.preStep(task)
      case None     =>
        // Task type no longer exists, so we're done.
        Query.value(Task.Completed())
    }

  def preCheckpoint(task: Task): Query[Task.State] =
    dispatch(task) match {
      case Some(fn) => fn.preCheckpoint(task)
      case None     =>
        // Task type no longer exists, so we're done.
        Query.value(Task.Completed())
    }

  /** Given a task, determines whether it may be run on any host in
    * the cluster.
    */
  def isStealable(task: Task): Boolean =
    dispatch(task) exists {
      _.isStealable(task)
    }

  /** Given a task name and account, this will return the priority this task *should* have.
    * For most tasks this is `DefaultUserPriority`, but for index builds it will be negative
    * if that account has other tasks already in queue, and for migration tasks it
    * will be smaller than the priority of any index build.
    */
  def customPriorityForTask(taskName: String, accountID: AccountID): Query[Int] =
    types.get(taskName) map {
      _.customPriority(accountID)
    } getOrElse Query.value(Task.DefaultUserPriority)

  /** Returns true if the task has a custom priority function.
    */
  def hasCustomPriority(taskName: String): Boolean =
    types.get(taskName) exists {
      _.hasCustomPriority
    }

  /** Given a task, determines whether its execution should be
    * logged. See TaskLogging.
    */
  def isLoggable(task: Task): Boolean =
    dispatch(task) exists {
      _.isLoggable
    }
}
