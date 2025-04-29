package fauna.model.tasks

import fauna.atoms._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.Task
import fauna.repo._
import fauna.repo.query.Query
import fauna.storage.doc._
import fauna.storage.Selector
import scala.annotation.unused

object Type {
  private[tasks] val log = getLogger

  object DummyTask extends Type("dummy") {
    def step(task: Task, snapshotTS: Timestamp) = Query.value(Task.Completed())

    def create(parent: Task): Query[Task] =
      create(parent.scopeID, parent.host, Some(parent.id))

    def create(scope: ScopeID, host: HostID, parent: Option[TaskID]): Query[Task] =
      Task.create(scope, name, host, Data.empty, parent)
  }
}

/** The superclass of all non-continuous[1] tasks, this represents the
  * "type" of the task. The TaskRouter will dispatch tasks by their
  * name by calling a corresponding Type's apply() method. Tasks (and
  * any children thereof) may also be cancelled by the router.
  *
  * [1] For continuous tasks, such as garbage collection, see RangeTask.
  */
abstract class Type(val label: String, val version: Int = 0) {

  val name = if (version == 0) label else s"$label.$version"

  /** Execute the continuation associated with the task's current
    * state, and return the next task state.
    *
    * TODO: Constrain this to the Task's data, once better conceptions
    * of fork()/join() exist.
    */
  def step(task: Task, snapshotTS: Timestamp): Query[Task.State]

  def onComplete(@unused task: Task): Query[Unit] = Query.unit
  def onCancel(@unused task: Task): Query[Unit] = Query.unit

  /** Callback executed before task step iteration.
    */
  def preStep(task: Task): Query[Task.State] = Query.value(task.state)

  /** Callback executed after the task step iteration, but before its state
    * is checkpointed.
    */
  def preCheckpoint(task: Task): Query[Task.State] = Query.value(task.state)

  val isHealthy: Query[Boolean] = Query.repo flatMap { _.isClusterHealthyQ }

  /** Returns true if this task type is runnable on any host in the
    * cluster.
    */
  def isStealable(task: Task): Boolean = true

  /** Returns a new priority if this task needs a custom priority. This is overriden
    * for index builds, as we de-prioritize those when a single account makes a
    * bunch of indexes, and for migration tasks, as they should not be executed over
    * index builds.
    */
  def customPriority(@unused accountID: AccountID): Query[Int] =
    Query.value(Task.DefaultUserPriority)

  /** Returns `true` if this function is going to return a custom priority.
    * This returns true for index builds and migration tasks.
    */
  def hasCustomPriority: Boolean = false

  /** Returns true if this task type's execution should be logged. See
    * TaskLogging.
    */
  def isLoggable: Boolean = false

  def apply(task: Task, snapshotTS: Timestamp): Query[Task.State] =
    isHealthy flatMap {
      case false => Query.value(task.state)
      case true  => step(task, snapshotTS)
    }

  /** Cancels all tasks and any forked subtasks of this Type.
    */
  def cancelAll(router: TaskRouter): Query[Unit] =
    Task.getAllRunnableAndPaused() selectT { _.name == name } foreachValueT {
      router.cancel(_, None)
    }

  protected def foreach[E](task: Task, field: Field[Vector[E]], gauge: String)(
    f: E => Query[Unit]): Query[Task.State] =
    fold(task, field, gauge) { (e, d) => f(e) map { _ => d } }

  protected def fold[E](task: Task, field: Field[Vector[E]], gauge: String)(
    f: (E, Data) => Query[Data]): Query[Task.State] = {
    val vs = task.data(field)

    if (vs.isEmpty) {
      Query.value(Task.Completed())
    } else {
      val rest = vs.tail
      Query.repo flatMap { repo =>
        f(vs.head, task.data) map { data =>
          repo.stats.set(gauge, rest.size.toDouble)
          Task.Runnable(data.update(field -> rest), task.parent)
        }
      }
    }
  }
}

abstract class AccountCustomPriorityType(
  override val label: String,
  override val version: Int = 0)
    extends Type(label, version) {

  protected def priorityDecrement: Int = TaskReprioritizer.TaskPriorityDecrement

  override def customPriority(accountID: AccountID): Query[Int] =
    Query.context flatMap { ctx =>
      // The more tasks for this account, the lower the priority should be
      Task.executingCountMax(accountID, name, ctx.repo.taskReprioritizerLimit) map {
        count =>
          count * -priorityDecrement
      }
    }

  override def hasCustomPriority: Boolean = true
}

// Checks the topology version of the task matches the current version.
// Extensions of this class are responsible for ensuring the topology
// version is propagated in the task's data.
abstract class SegmentPaginateType(name: String, version: Int)
    extends PaginateType[Segment](name, version) {
  self: Paginator[Segment] =>

  val RangesField = Field[Vector[Segment]]("ranges")
  val TopologyVersionField = Field[Long]("topology_version")

  override final def paginate(
    task: Task,
    field: Field[Vector[Segment]],
    gauge: String,
    iter: RangeIteratee[RowID, Col],
    selector: Selector): Query[Task.State] = Query.repo flatMap { repo =>
    val current = repo.service.partitioner.partitioner.version
    // TODO: Require the field be present once it's rolled out.
    val expected = task.data.getOrElse(TopologyVersionField, -1L)
    if (expected >= 0 && expected != current) {
      repo.stats.incr("Task.TopologyVersionChange")
      Query.value(Task.Paused(
        "topology version change detected: parent must be repartitioned or restarted",
        task.state,
        task.parent))
    } else {
      super.paginate(task, field, gauge, iter, selector)
    }
  }

  // Repartition `sourceTask`'s segments, splitting out subsegments that belong to
  // `targetHost`
  // and relocating the scan for those segments to a task on that host. If
  // `targetTask` is
  // provided, that task is assigned the new segments; otherwise, a new task is
  // created.
  // `createTaskData` specifies how to create the data for the new task, and should
  // generally
  // be a partially-applied version of the task's standard create function.
  // Both the source task and the target task (if it is provided) must be paused.
  def repartitionTaskSegments(
    sourceTask: Task,
    targetHost: HostID,
    targetTask: Option[TaskID])(createTaskData: (Long, Vector[Segment]) => Data) =
    if (!sourceTask.isPaused) {
      Query.fail(
        new IllegalArgumentException(
          s"Task ${sourceTask.id} found, but is not paused."))
    } else {
      Query.repo flatMap { repo =>
        val currentTopologyVersion = repo.service.partitioner.partitioner.version
        val taskSegments = sourceTask.data(RangesField).sorted
        val targetSegments = repo.keyspace.segmentsForHost(targetHost)

        val remaining = Segment.diff(taskSegments, targetSegments)
        val intersections = Segment.diff(taskSegments, remaining)

        val updateOriginalTaskQ = {
          val d =
            sourceTask.state.data
              .update(RangesField -> remaining.toVector)
              .update(CursorField -> None)
              .update(TopologyVersionField -> currentTopologyVersion)

          Task.write(sourceTask.copy(state = sourceTask.state.withData(d)))
        }

        val createOrUpdateTargetTaskQ =
          Query.snapshotTime flatMap { snapshotTS =>
            targetTask match {
              case Some(targetTaskID) =>
                Task.get(targetTaskID, snapshotTS) flatMap {
                  case None =>
                    Query.fail(
                      new IllegalArgumentException(
                        s"Target task $targetTaskID not found."))

                  case Some(targetTask) if !targetTask.isPaused =>
                    Query.fail(
                      new IllegalArgumentException(
                        s"Target task $targetTaskID found, but it is not paused."))

                  case Some(targetTask) =>
                    val targetRanges = Segment.normalize(
                      targetTask.state.data(RangesField) ++ intersections)

                    Task.write(
                      targetTask.copy(state = targetTask.state.withData(
                        targetTask.state.data
                          .update(RangesField -> targetRanges)
                          .update(CursorField -> None)
                          .update(TopologyVersionField -> currentTopologyVersion)
                      )))
                }

              case None =>
                Task.create(
                  ScopeID.RootID,
                  name,
                  targetHost,
                  createTaskData(currentTopologyVersion, intersections.toVector),
                  parent = sourceTask.parent
                ) flatMap { task =>
                  sourceTask.parent.fold(Query.unit) { parentID =>
                    Task.get(parentID, snapshotTS) flatMap {
                      case None =>
                        Query.fail(new IllegalArgumentException(
                          s"Parent task $parentID not found."))
                      case Some(parentTask) =>
                        Task.addChild(parentTask, task.id)
                    }
                  }
                }
            }
          }

        Seq(updateOriginalTaskQ, createOrUpdateTargetTaskQ).join
      }
    }
}

abstract class PaginateType[Key](name: String, version: Int)
    extends Type(name, version) {
  self: Paginator[Key] =>

  // lazy to allow for FieldType initialization
  lazy val CursorField = Field[Option[Cursor]]("cursor")

  // snapshot time at which to read data, if applicable. read latest if unset.
  lazy val SnapshotOverrideField = Field[Option[Timestamp]]("snapshot_ts")
  val PageSizeField = Field[Option[Int]]("page_size")
  val TransactionSizeField = Field[Option[Int]]("sub_transaction_size")

  /** Paginates once through the rows given by `key`, passing each row
    * to the provided Iteratee `iter`.
    *
    * Precondition: a task which has never called `paginateOnce`
    * should have an empty CursorField.
    */
  protected def paginateOnce(
    task: Task,
    key: Key,
    gauge: String,
    iter: RangeIteratee[RowID, Col],
    selector: Selector): Query[Task.State] = {

    val pageSize = task.data(PageSizeField)
    val transactionSize = task.data(TransactionSizeField)
    val cursor = task.data(CursorField)
    val snapshotOverride = task.data(SnapshotOverrideField)

    paginate(
      gauge,
      key,
      snapshotOverride,
      cursor,
      iter,
      pageSize,
      transactionSize,
      selector) map {
      case Finished => Task.Completed()
      case Continue(cursor, pageSize, txnSize) =>
        Task.Runnable(
          task.data.update(
            CursorField -> Some(cursor),
            PageSizeField -> Some(pageSize),
            TransactionSizeField -> Some(txnSize)),
          task.parent)
      case Retry(pageSize, txnSize) =>
        Task.Runnable(
          task.data.update(
            PageSizeField -> Some(pageSize),
            TransactionSizeField -> Some(txnSize)),
          task.parent)
    }
  }

  /** Paginates incrementally through a set of keys (found within a
    * `task`'s data in `field`), passing each row to the provided
    * Iteratee, `iter`.
    *
    * Pagination stats will be prefixed with the `gauge` string.
    */
  protected def paginate(
    task: Task,
    field: Field[Vector[Key]],
    gauge: String,
    iter: RangeIteratee[RowID, Col],
    selector: Selector): Query[Task.State] = {

    val vs = task.data(field)
    val pageSize = task.data(PageSizeField)
    val transactionSize = task.data(TransactionSizeField)
    val snapshotOverride = task.data(SnapshotOverrideField)

    if (vs.isEmpty) {
      Query.value(Task.Completed())
    } else {
      Query.stats flatMap { stats =>
        paginate(
          gauge,
          vs.head,
          snapshotOverride,
          task.data(CursorField),
          iter,
          pageSize,
          transactionSize,
          selector) map { result =>
          val data = result match {
            case Continue(cursor, pageSize, txnSize) =>
              task.data.update(
                CursorField -> Some(cursor),
                PageSizeField -> Some(pageSize),
                TransactionSizeField -> Some(txnSize))
            case Retry(pageSize, txnSize) =>
              task.data.update(
                PageSizeField -> Some(pageSize),
                TransactionSizeField -> Some(txnSize))
            case Finished =>
              stats.set(s"$gauge.Remaining", (vs.size - 1).toDouble)
              task.data.update(field -> vs.tail, CursorField -> None)
          }

          Task.Runnable(data, task.parent)
        } recoverWith { case _: PaginatorProgressException =>
          throw Executor.TaskProgressException(name, task.id)
        }
      }
    }
  }
}
