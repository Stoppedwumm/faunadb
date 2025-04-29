package fauna.model

import fauna.atoms._
import fauna.flags.EnableIndexBuildPriorities
import fauna.lang.{ AdminControl, Timestamp }
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.model.schema.{ InternalCollection, NativeIndex }
import fauna.model.tasks.TaskRouter
import fauna.repo._
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.storage.api.set._
import fauna.storage.doc._
import fauna.storage.index.IndexTerm
import fauna.storage.ir._
import fauna.trace.{ GlobalTracer, Producer, TraceContext }
import scala.concurrent.duration._
import scala.util.{ Failure, Random, Success }

/** Tasks are used to schedule asynchronous computation on a node in
  * the cluster. All tasks are associated with a HostID. As cluster
  * topology changes, the following will occur:
  *
  * - If a node is added, a new HostID is generated and it will have no
  *   tasks.
  * - If a node is removed, its HostID will cease to exist and its
  *   tasks will be orphaned. This implies that decommissioning a
  *   node must consume associated tasks.
  *
  * Pending tasks are consumed by a TaskExecutor corresponding to
  * their assigned endpoint. The kernel of each task is dictated by
  * its "name" field, which is dispatched by TaskRouter.
  *
  * Tasks are prioritized - a higher value is a higher priority. A
  * forked task is always lower priority than its children to prevent
  * priority inversion.
  *
  * Tasks of the same priority are executed in FIFO order using the
  * transaction time of their creation.
  */
final case class Task(
  id: TaskID,
  host: HostID,
  name: String,
  scopeID: ScopeID,
  accountID: AccountID,
  priority: Int,
  createdAt: Timestamp,
  elapsedMillis: Long,
  trace: Option[TraceContext],
  state: Task.State,
  validTS: Timestamp,
  isOperational: Boolean) {

  lazy val statName = name.split('-') map { _.capitalize } mkString

  def isCancelled: Boolean = state.isCancelled

  def isCompleted: Boolean = state.isCompleted

  def isRunnable: Boolean = state.isRunnable

  def isForked: Boolean = state.isForked

  def isPending: Boolean = !isCompleted

  def isPaused: Boolean = state.isPaused

  def data: Data = state.data

  def parent: Option[TaskID] =
    state match {
      case Task.Runnable(_, p)                   => p
      case Task.Paused(_, _, p)                  => p
      case Task.Forked(_, _, p)                  => p
      case Task.Blocked(_, _, p)                 => p
      case _: Task.Completed | _: Task.Cancelled => None
    }

  /** Returns this Task's previous version, if any exists.
    */
  def previous: Query[Option[Task]] =
    Task.get(id, validTS - 1.nano)

  /** Blocks the current task until a set of subtasks complete. When
    * the last subtask completes, it will unblock this task and put it
    * back on the runq, requiring every forked task to have at least
    * one subtask to maintain progress.
    */
  def fork(subtasks: => Query[(Iterable[Task], Data)]): Query[Task.State] =
    subtasks flatMap { case (tasks, data) =>
      val ids = tasks map { _.id } toVector

      require(!ids.contains(id))
      require(ids.nonEmpty)

      val children = tasks.toSeq map { child =>
        Task.runnable(child, child.data, Some(id), 0)
      } join

      children map { _ =>
        Task.Forked(ids, data, parent)
      }
    }

  /** joinThen folds over each of a forked task's pending sub-tasks,
    * reducing them as they are marked completed. Once all sub-tasks
    * are complete, f() executes with the forked task's data.
    */
  def joinThen()(
    f: Either[Iterable[Task], Data] => Query[Task.State]): Query[Task.State] =
    state match {
      case s @ Task.Forked(tasks, data, parent) =>
        val tasksQ = Query.snapshotTime flatMap { snapshotTS =>
          tasks map { Task.get(_, snapshotTS) } sequence
        }

        val pendingQ = tasksQ collectT {
          case Some(t) if t.isPending => Query(Seq(t))
        }

        val cancelledQ = tasksQ collectT {
          case Some(t) if t.isCancelled => Query(Seq(t))
        }

        (pendingQ, cancelledQ) par { (pending, cancelled) =>
          if (cancelled.nonEmpty) { // cancellation always takes precedence

            // NB. we do not auto-cancel the pending list; callers
            // should take into account whether any tasks left pending
            // continue to be valid after a cancellation occurs.
            f(Left(cancelled))
          } else if (pending.isEmpty) {
            f(Right(data)) map {
              case Task.Runnable(d, _) => Task.Runnable(d, parent)
              case state               => state
            }
          } else {
            Query.value(s.copy(tasks = pending map { _.id } toVector))
          }
        }
      case s => Query(s)
    }

  /** Simple join - success completes, and failure cancels this task.
    */
  def join(): Query[Task.State] =
    joinThen() {
      case Right(_) => Query.value(Task.Completed())
      case Left(_)  => Query.value(Task.Cancelled())
    }

  /** Cancels this and any forked subtasks, recursively.
    */
  def cancel(data: Option[Data]): Query[Task] = {
    def cancel0(tasks: Vector[TaskID]) =
      Query.snapshotTime flatMap { snapshotTS =>
        val tasksQ = Query(tasks) flatMapT { id =>
          Task.get(id, snapshotTS) map { _.toSeq } // hax
        } selectT { _.isPending }

        tasksQ flatMap { subs =>
          subs.map { _.cancel(None) } sequence
        } join
      }

    val cancelQ = this match {
      case Task.Children(tasks) => cancel0(tasks)
      case _                    => Query.unit
    }

    cancelQ flatMap { _ =>
      Task.cancel(this, data, 0)
    }
  }

  /** Pause this task if it or any of its children are runnable, recursively.
    */
  def pause(reason: String): Query[Task] = {
    def pause0(tasks: Vector[TaskID]): Query[Unit] =
      Query.snapshotTime flatMap { snapshotTS =>
        val tasksQ = Query.value(tasks) flatMapT { id =>
          Task.get(id, snapshotTS) map { _.toSeq }
        }

        tasksQ flatMap { tasks =>
          tasks map { _.pause(reason) } sequence
        } join
      }

    this.state match {
      case Task.Runnable(_, parent) =>
        Task.paused(this, reason, this.state, parent, 0)

      case Task.Forked(tasks, _, _) =>
        pause0(tasks) flatMap { _ =>
          Task.paused(this, reason, this.state, parent, 0)
        }

      case Task.Blocked(tasks, _, _) =>
        pause0(tasks) map { _ => this }

      case _ =>
        Query.value(this)
    }
  }

  /** Un-pause this task if it or any of its children are paused, recursively.
    */
  def unpause(): Query[Task] = {
    def unpause0(tasks: Vector[TaskID]): Query[Unit] =
      Query.snapshotTime flatMap { snapshotTS =>
        val tasksQ = Query.value(tasks) flatMapT { id =>
          Task.get(id, snapshotTS) map { _.toSeq }
        }

        tasksQ flatMap { tasks =>
          tasks map { _.unpause() } sequence
        } join
      }

    this.state match {
      case Task.Paused(_, previous, _) =>
        Task.write(this.copy(state = previous))

      case Task.Forked(tasks, _, _) =>
        unpause0(tasks) map { _ => this }

      case Task.Blocked(tasks, _, _) =>
        unpause0(tasks) map { _ => this }

      case _ =>
        Query.value(this)
    }
  }

  /** Cancels this and any parent tasks, recursively.
    */
  def abort(): Query[Unit] = {
    def abort0(parent: TaskID) =
      Query.snapshotTime flatMap { snapshotTS =>
        Task.get(parent, snapshotTS) flatMap {
          case Some(parent) => parent.abort()
          case None         => Query.unit
        } flatMap { _ =>
          Task.cancel(this, None, 0) map { _ => () }
        }
      }

    parent match {
      case Some(t) => abort0(t)
      case None =>
        Task.cancel(this, None, 0) map { _ => () }
    }
  }

  /** Change task priority. The task priority cannot be changed
    * to a value higher than its children priorities.
    */
  def reprioritize(priority: Int): Query[Task] = {
    def checkChildrenPriority(task: Task): Query[Unit] = task match {
      case Task.Children(tasks) =>
        tasks map { taskID =>
          Task.get(taskID) flatMap {
            case Some(child) if child.priority > priority =>
              checkChildrenPriority(child)

            case Some(_) =>
              Query.fail(
                new RuntimeException(
                  "Parent priority cannot be higher than its children priorities."))

            case _ =>
              Query.unit
          }
        } join

      case _ =>
        Query.unit
    }

    checkChildrenPriority(this) flatMap { _ =>
      Task.write(this.copy(priority = priority))
    }
  }
}

object Task {

  val TaskColl = InternalCollection.Task(Database.RootScopeID)

  // tasks initiated due to user activity begin at priority 0;
  // operational tasks are 'nice'-ed down to Int.MinValue such that
  // user activity is very likely to take precedence over operations.
  //
  // This will not affect the priority of index builds, as those
  // have a custom priority based on the number of tasks active for
  // that account.
  val DefaultUserPriority = 0
  val DefaultOperationalPriority = Int.MinValue

  val TTL = 7.days

  sealed abstract class State {
    def data: Data

    def withData(data: Data): State

    def isCompleted: Boolean = this match {
      case _: Completed | _: Cancelled                      => true
      case _: Runnable | _: Paused | _: Forked | _: Blocked => false
    }

    def isCancelled: Boolean = this match {
      case _: Cancelled                                                    => true
      case _: Completed | _: Runnable | _: Paused | _: Forked | _: Blocked => false
    }

    def isRunnable: Boolean = this match {
      case _: Runnable | _: Forked                              => true
      case _: Paused | _: Blocked | _: Completed | _: Cancelled => false
    }

    def isForked: Boolean = this match {
      case _: Forked | _: Blocked                                => true
      case _: Runnable | _: Paused | _: Completed | _: Cancelled => false
    }

    def isPaused: Boolean = this match {
      case _: Paused => true
      case _         => false
    }
  }

  object State {
    def name(s: State) = s match {
      case Runnable(_, _)   => "runnable"
      case Paused(_, _, _)  => "paused"
      case Forked(_, _, _)  => "forked"
      case Blocked(_, _, _) => "blocked"
      case Completed(_)     => "completed"
      case Cancelled(_)     => "cancelled"
    }
  }

  case class Runnable(data: Data, parent: Option[TaskID]) extends State {
    def withData(d: Data) = this.copy(data = d)
  }

  case class Paused(reason: String, previous: State, parent: Option[TaskID])
      extends State {
    val data = previous.data

    def withData(d: Data) = Paused(reason, previous.withData(d), parent)
  }

  case class Forked(tasks: Vector[TaskID], data: Data, parent: Option[TaskID])
      extends State {
    def withData(d: Data) = this.copy(data = d)
  }

  case class Blocked(tasks: Vector[TaskID], data: Data, parent: Option[TaskID])
      extends State {
    def withData(d: Data) = this.copy(data = d)
  }

  case class Completed(_data: Option[Data] = None) extends State {
    def data = _data.getOrElse(Data.empty)
    def withData(d: Data) = this.copy(_data = Some(d))
  }

  case class Cancelled(_data: Option[Data] = None) extends State {
    def data = _data.getOrElse(Data.empty)
    def withData(d: Data) = this.copy(_data = Some(d))
  }

  // helper to extract child tasks from a forked task, if any
  object Children {
    def unapply(task: Task): Option[Vector[TaskID]] =
      task.state match {
        case Forked(ts, _, _)                                      => Some(ts)
        case Blocked(ts, _, _)                                     => Some(ts)
        case _: Runnable | _: Paused | _: Completed | _: Cancelled => None
      }
  }

  val HostField = Field[HostID]("host")
  val NameField = Field[String]("name")
  val ScopeField = Field[Option[ScopeID]]("database")
  val AccountField = Field[AccountID]("account")
  val PriorityField = Field[Int]("priority")
  val CreatedAtField = Field[Timestamp]("created_at")
  val ElapsedField = Field[Long]("elapsed_ms")

  // An "operational" task is one which is executed on behalf of a
  // Fauna employee (the operator). These tasks should not result in
  // billable activity and should be deprioritized in favor of
  // user-initiated activity.
  //
  // This attribute is also inherited by child tasks forked from an
  // operational task, affected an entire subtree of tasks.
  val OperationalField = Field[Boolean]("operational")

  implicit val TraceT = FieldType.CBORBytes[TraceContext]
  val TraceField = Field[Option[TraceContext]]("trace")

  implicit val StateT = FieldType.SumCodec[String, State](
    NameField,
    "runnable" -> FieldType.RecordCodec[Runnable],
    "paused" -> FieldType.Lazy(PausedT),
    "forked" -> FieldType.RecordCodec[Forked],
    "completed" -> FieldType.RecordOverrideCodec[Completed]("_data" -> "data"),
    "cancelled" -> FieldType.RecordOverrideCodec[Cancelled]("_data" -> "data"),
    "blocked" -> FieldType.RecordCodec[Blocked]
  )

  val PausedT: FieldType[Task.Paused] = FieldType.RecordCodec[Paused]

  val StateField = Field[State]("state")

  // legacy field definitions
  val CancelledField = Field[Option[Boolean]]("cancelled")
  val CompletedField = Field[Boolean]("completed")
  val ForkedField = Field[Option[Boolean]]("forked")
  val TasksField = Field.ZeroOrMore[TaskID]("tasks")
  val ParentField = Field[Option[TaskID]]("parent")
  val DataField = Field[Data]("data")

  // Extracts both
  //   { "completed" -> true }
  // and
  //   { "state" -> { "name" -> "completed" } }
  // structures into a boolean for indexing.
  val CompletedExtractor: TermExtractor = { (version, _) =>
    val completed = version.data.getOrElse(
      Task.CompletedField, {
        // FIXME: re-define the index from "not not runnable" to "is runnable"
        val state = version.data(Task.StateField)
        !state.isRunnable && !state.isPaused
      }
    )

    Query.value(Vector(IndexTerm(completed, false)))
  }

  val RunnableExtractor: TermExtractor = { (version, _) =>
    val state = version.data.getOrElse(Task.StateField, legacyState(version))

    Query.value(Vector(IndexTerm(state.isRunnable, false)))
  }

  /** Determines whether a task is currently eligible for execution,
    * for the purpose of limiting the number of tasks a single account
    * may execute.
    */
  val ExecutingExtractor: TermExtractor = { (version, _) =>
    val state = version.data.getOrElse(Task.StateField, legacyState(version))

    val executing = state match {
      case _: Runnable | _: Forked | _: Blocked | _: Paused => true
      case _: Completed | _: Cancelled                      => false
    }

    Query.value(Vector(IndexTerm(executing, false)))
  }

  def apply(v: Version): Task =
    Task(
      v.docID.as[TaskID],
      v.data(HostField),
      v.data(NameField),
      v.data(ScopeField) getOrElse Database.RootScopeID,
      v.data.getOrElse(AccountField, Database.RootDatabase.accountID),
      v.data.getOrElse(PriorityField, DefaultUserPriority),
      v.data.getOrElse(CreatedAtField, Timestamp.Epoch),
      v.data.getOrElse(ElapsedField, 0L),
      v.data(TraceField),
      v.data.getOrElse(StateField, legacyState(v)),
      v.ts.validTS,
      v.data.getOrElse(OperationalField, false)
    )

  def get(
    id: TaskID,
    validTS: Timestamp = Timestamp.MaxMicros): Query[Option[Task]] =
    TaskColl.get(id, validTS) map { _.map(apply) }

  /** Returns the most recent version of the Task with the given id
    * wherein it was runnable, if any exists.
    */
  def getLatestRunnable(id: TaskID): Query[Option[Task]] =
    TaskColl.versions(id) findValueT { ver =>
      !ver.isDeleted && ver.data.getOrElse(StateField, legacyState(ver)).isRunnable
    } mapT apply

  def getAllRunnableAndPaused(): PagedQuery[Iterable[Task]] = {
    val idx = NativeIndex.TaskByCompletion(Database.RootScopeID)
    val terms = Vector(IndexTerm(FalseV))
    Query.snapshotTime flatMap { snapshotTS =>
      Store.collectDocuments(idx, terms, snapshotTS) { (v, ts) =>
        get(v.docID.as[TaskID], ts)
      }
    }
  }

  def getAllRunnable(): PagedQuery[Iterable[Task]] =
    getAllRunnableAndPaused() selectT { task => task.isRunnable }

  def getRunnableHierarchy(): Query[Iterable[Task]] = {
    def getParentTasks(
      task: Task,
      taskMap: Map[TaskID, Task]): Query[Map[TaskID, Task]] = {
      task.parent match {
        case Some(parent) if !taskMap.contains(parent) =>
          Query.snapshotTime flatMap { snapshotTS =>
            Task.get(parent, snapshotTS) flatMap {
              case Some(parentTask) =>
                getParentTasks(parentTask, taskMap + (parent -> parentTask))
              case None =>
                Query.value(taskMap)
            }
          }
        case _ =>
          Query.value(taskMap)
      }
    }

    getAllRunnableAndPaused().foldLeftMT(Map.empty[TaskID, Task]) {
      case (acc, tasks) =>
        tasks.foldLeft(Query.value(acc)) { case (accQ, task) =>
          accQ flatMap { taskMap =>
            getParentTasks(task, taskMap + (task.id -> task))
          }
        }
    } map { taskMap =>
      taskMap.values
    }
  }

  def getRunnableIDsByHost(host: HostID): PagedQuery[Iterable[TaskID]] = {
    val idx = NativeIndex.PrioritizedTasksByCreatedAt(Database.RootScopeID)
    val terms = Vector(Scalar(StringV(host.uuid.toString)), Scalar(TrueV))
    Query.snapshotTime flatMap { snapshotTS =>
      Store.collection(idx, terms, snapshotTS) mapValuesT { _.docID.as[TaskID] }
    }
  }

  def getRunnableByHost(host: HostID) = {
    val idx = NativeIndex.PrioritizedTasksByCreatedAt(Database.RootScopeID)
    val terms = Vector(IndexTerm(StringV(host.uuid.toString)), IndexTerm(TrueV))
    Query.snapshotTime flatMap { snapshotTS =>
      Store.collectDocuments(idx, terms, snapshotTS) { (v, ts) =>
        get(v.docID.as[TaskID], ts)
      }
    }
  }

  /** Returns the number of tasks eligible for execution - i.e. they
    * are neither completed nor cancelled - by a TaskExecutor, given
    * the accountID and the name of the task.
    */
  def executingCount(accountID: AccountID, name: String): Query[Int] = {
    val idx = NativeIndex.ExecutingTasksByAccountAndName(Database.RootScopeID)
    val terms =
      Vector(Scalar(LongV(accountID.toLong)), Scalar(StringV(name)), Scalar(TrueV))
    Query.snapshotTime flatMap { snapshotTS =>
      Store.collection(idx, terms, snapshotTS).countT
    }
  }

  /** Returns all tasks with the given name that were created by the given account.
    */
  def getAllByAccount(
    accountID: AccountID,
    name: String,
    executing: Boolean = true): PagedQuery[Iterable[Task]] = {
    val idx = NativeIndex.ExecutingTasksByAccountAndName(Database.RootScopeID)
    val terms =
      Vector(IndexTerm(accountID.toLong), IndexTerm(name), IndexTerm(executing))
    Query.snapshotTime flatMap { snapshotTS =>
      Store.collectDocuments(idx, terms, snapshotTS) { (v, ts) =>
        get(v.docID.as[TaskID], ts)
      }
    }
  }

  /** Returns the number of tasks eligible for execution, with the given maximum. This will stop counting if
    * the number of tasks is larger than said maximum.
    */
  def executingCountMax(accountID: AccountID, name: String, max: Int): Query[Int] = {
    require(max >= 0, s"Max count cannot be negative: $max")
    val idx = NativeIndex.ExecutingTasksByAccountAndName(Database.RootScopeID)
    val terms =
      Vector(Scalar(LongV(accountID.toLong)), Scalar(StringV(name)), Scalar(TrueV))
    Query.snapshotTime flatMap { snapshotTS =>
      // Split into pages of `max`, get the first page, and count it.
      Store
        .collection(idx, terms, snapshotTS, pageSize = max)
        .splitT(max)
        .initValuesT map { _.size }
    }
  }

  /** Dispatch a task.
    */
  def create(
    scopeID: ScopeID,
    name: String,
    host: HostID,
    msg: Data,
    parent: Option[TaskID],
    isOperational: Boolean = false,
    router: TaskRouter = TaskRouter): Query[Task] =
    Query.repo flatMap { repo =>
      if (!repo.service.runningHosts.contains(host)) {
        throw new IllegalArgumentException(
          s"$host is not a live member of the cluster!")
      }
      val tracer = GlobalTracer.instance
      val spanB = tracer
        .buildSpan(s"task.$name.create")
        .withKind(Producer)

      TaskColl.nextID().map(_.get) flatMap { id =>
        Database.forScope(scopeID) flatMap {
          case Some(db) =>
            def taskPriority(): Query[Int] =
              repo.hostFlag(EnableIndexBuildPriorities) flatMap { enabled =>
                if (enabled) {
                  router.customPriorityForTask(name, db.accountID)
                } else {
                  // If we don't have custom priorities enabled, use the default
                  Query.value(DefaultUserPriority)
                }
              }

            val prio = parent match {
              case None if isOperational =>
                Query.value((DefaultOperationalPriority, true))
              case None => taskPriority() map { (_, false) }
              case Some(id) =>
                Query.snapshotTime flatMap { snapshotTS =>
                  // NB. isOperational propogates to children, if the parent still
                  // exists.
                  Task.get(id, snapshotTS) flatMap {
                    case None if isOperational =>
                      Query.value((DefaultOperationalPriority, true))
                    case None => taskPriority() map { (_, false) }
                    case Some(t) =>
                      Query.value(
                        ((t.priority + 1) min Int.MaxValue, t.isOperational))
                  }
                }
            }

            prio flatMap { case (prio, operOverride) =>
              val span = spanB.start()
              val scope = tracer.activate(span)
              try {
                val ctx = if (span.isSampled) {
                  Some(span.context)
                } else {
                  None
                }
                val t = Task(
                  id = id,
                  host = host,
                  name = name,
                  scopeID = scopeID,
                  accountID = db.accountID,
                  priority = prio,
                  createdAt = Clock.time,
                  elapsedMillis = 0,
                  trace = ctx,
                  state = Task.Runnable(msg, parent),
                  validTS = Timestamp.MaxMicros,
                  isOperational = isOperational || operOverride
                )

                write(t)
              } finally {
                scope foreach { _.close() }
              }
            }
          case None =>
            throw new IllegalArgumentException(
              s"scope $scopeID is not a live database")
        }
      }
    }

  /** Dispatch a task on to the current local host, as determined by broadcast address.
    */
  def createLocal(
    scope: ScopeID,
    name: String,
    msg: Data,
    parent: Option[TaskID],
    isOperational: Boolean = false): Query[Task] =
    Query.repo flatMap { repo =>
      repo.service.localID match {
        case Some(id) =>
          create(scope, name, id, msg, parent, isOperational)
        case None => Query.fail(new IllegalStateException("no host ID"))
      }
    }

  /** Filters a Set[HostID] into a seq of hosts which are live
    */
  private[this] def filterLive(repo: RepoContext, set: Set[HostID]): Seq[HostID] =
    set.toSeq.filter {
      repo.service.hostService.isLive(_)
    }

  /** Dispatch a task to a single host at random.
    */
  def createRandom(
    scope: ScopeID,
    name: String,
    msg: Data,
    parent: Option[TaskID],
    isOperational: Boolean = false): Query[Task] = {
    Query.repo flatMap { repo =>
      val compute = filterLive(repo, repo.service.computeHosts)

      // If there are any compute-only hosts in this cluster, prefer to
      // send random tasks to them.
      val target = if (compute.nonEmpty) {
        Random.choose(compute)
      } else {
        // No compute-only hosts; pick any running host.
        Random.choose(filterLive(repo, repo.service.runningHosts))
      }

      create(scope, name, target, msg, parent, isOperational)
    }
  }

  /** Dispatch a range task ("table scan") to each host in the cluster, where
    * the task will be executed over each hosts' segments.
    */
  def createAllSegments(scope: ScopeID, name: String, parent: Option[TaskID])(
    fn: (Iterable[Segment], Int) => Data): Query[Iterable[Task]] =
    Query.repo flatMap { repo =>
      repo.keyspace.allSegmentsByHost.zipWithIndex map { case ((host, segs), idx) =>
        create(scope, name, host, fn(segs, idx), parent)
      } sequence
    }

  /** Dispatch a range task ("table scan") to each host in a random data replica.
    * TODO: as an improvement we can be a bit smart and pick a less busy
    * data replica instead of a random or choose the replica that is closest to
    * the users that initiated the task, assuming this is a optimization for
    * ExportDataTask.
    */
  def createRandomReplicaSegments(
    scope: ScopeID,
    name: String,
    parent: Option[TaskID])(
    fn: (Iterable[Segment], Int) => Data): Query[Iterable[Task]] =
    Query.repo flatMap { repo =>
      val replica = Random.choose(repo.service.dataReplicas.toSeq)

      repo.keyspace.replicaSegments(replica).zipWithIndex map {
        case ((host, segs), idx) =>
          create(scope, name, host, fn(segs, idx), parent)
      } sequence
    }

  /** Dispatch a range task ("table scan") to each host in the cluster, where
    * the task will be executed over each hosts' primary segments.
    */
  def createPrimarySegments(scope: ScopeID, name: String, parent: Option[TaskID])(
    fn: (Iterable[Segment], Int) => Data): Query[Iterable[Task]] =
    Query.repo flatMap { repo =>
      repo.keyspace.allSegmentsByPrimaryHost.zipWithIndex map {
        case ((host, segs), idx) =>
          create(scope, name, host, fn(segs, idx), parent)
      } sequence
    }

  /** Like createAllSegments, but each segment is labeled with whether it is primary
    * on the host in the list given to `fn`.
    */
  def createLabeledSegments(scope: ScopeID, name: String, parent: Option[TaskID])(
    fn: (Iterable[(Segment, Boolean)], Int) => Data): Query[Iterable[Task]] =
    Query.repo flatMap { repo =>
      repo.keyspace.labeledSegmentsByHost.zipWithIndex map {
        case ((host, segs), idx) =>
          create(scope, name, host, fn(segs, idx), parent)
      } sequence
    }

  private def checkUnblock(
    self: Task,
    parent: TaskID,
    state: State,
    elapsedMillis: Long) =
    Task.get(parent) flatMap {
      case Some(parent @ Children(tasks)) =>
        val tasksQ = tasks filterNot { _ == self.id } map { Task.get(_) } sequence

        val pendingQ = tasksQ collectT {
          case Some(t) if t.isPending => Query(Seq(t))
        }

        pendingQ flatMap { pending =>
          if (pending.nonEmpty) {
            write(
              self.copy(
                state = state,
                elapsedMillis = self.elapsedMillis + elapsedMillis))
          } else {
            Task.unblock(parent) flatMap { _ =>
              write(
                self.copy(
                  state = state,
                  elapsedMillis = self.elapsedMillis + elapsedMillis))
            }
          }
        }
      case _ =>
        write(
          self
            .copy(state = state, elapsedMillis = self.elapsedMillis + elapsedMillis))
    } flatMap { task =>
      timeComplete(task)
    }

  private def timeComplete(task: Task): Query[Task] =
    if (task.createdAt > Timestamp.Epoch) {
      Query.stats map { stats =>
        val key = s"Task.${task.statName}.Completed.Time"
        val duration = Clock.time.difference(task.createdAt)

        stats.timing(key, duration.toMillis)
        task
      }
    } else {
      Query.value(task)
    }

  /** Completes a task, unblocking its parent and putting it back on
    * the runq if applicable.
    */
  def complete(task: Task, data: Option[Data], elapsedMillis: Long): Query[Task] =
    task.state match {
      case Runnable(_, Some(p)) =>
        checkUnblock(task, p, Completed(data), elapsedMillis)
      case Paused(_, _, Some(p)) =>
        checkUnblock(task, p, Completed(data), elapsedMillis)
      case Forked(_, _, Some(p)) =>
        checkUnblock(task, p, Completed(data), elapsedMillis)
      case Blocked(_, _, Some(p)) =>
        checkUnblock(task, p, Completed(data), elapsedMillis)
      case _: Cancelled | _: Completed => Query.value(task)
      case Runnable(_, None) | Paused(_, _, None) | Forked(_, _, None) |
          Blocked(_, _, None) =>
        write(
          task.copy(
            state = Completed(data),
            elapsedMillis = task.elapsedMillis + elapsedMillis)) flatMap {
          timeComplete(_)
        }
    }

  /** Cancels a task, unblocking its parent and putting it back on the
    * runq if applicable.
    */
  def cancel(task: Task, data: Option[Data], elapsedMillis: Long): Query[Task] =
    task.state match {
      case Runnable(_, Some(p)) =>
        checkUnblock(task, p, Cancelled(data), elapsedMillis)
      case Paused(_, _, Some(p)) =>
        checkUnblock(task, p, Cancelled(data), elapsedMillis)
      case Forked(_, _, Some(p)) =>
        checkUnblock(task, p, Cancelled(data), elapsedMillis)
      case Blocked(_, _, Some(p)) =>
        checkUnblock(task, p, Cancelled(data), elapsedMillis)
      case _: Cancelled | _: Completed => Query.value(task)
      case Runnable(_, None) | Paused(_, _, None) | Forked(_, _, None) |
          Blocked(_, _, None) =>
        write(
          task.copy(
            state = Cancelled(data),
            elapsedMillis = task.elapsedMillis + elapsedMillis)) flatMap {
          timeComplete(_)
        }
    }

  def runnable(
    task: Task,
    msg: Data,
    parent: Option[TaskID],
    elapsedMillis: Long): Query[Task] =
    write(
      task.copy(
        state = Runnable(msg, parent),
        elapsedMillis = task.elapsedMillis + elapsedMillis))

  def paused(
    task: Task,
    reason: String,
    previous: State,
    parent: Option[TaskID],
    elapsedMillis: Long): Query[Task] =
    write(
      task.copy(
        state = Paused(reason, previous, parent),
        elapsedMillis = task.elapsedMillis + elapsedMillis))

  /** Moves `task` to Blocked until all `subtasks` reach Completed or Cancelled.
    * See `Task.join()` for the treatment of `msg`.
    *
    * However if `task` is already Forked and `subtasks` == its previous list,
    * remain forked.
    */
  def maybeBlock(
    task: Task,
    subtasks: Vector[TaskID],
    msg: Data,
    elapsedMillis: Long): Query[Task] = {
    val state = task.state match {
      case Forked(prev, _, _) if prev == subtasks =>
        Forked(subtasks, msg, task.parent)
      case _ =>
        Blocked(subtasks, msg, task.parent)
    }
    write(
      task.copy(state = state, elapsedMillis = task.elapsedMillis + elapsedMillis))
  }

  /** If `task` is currently Blocked, places it back on the runq by
    * moving it to Forked.
    */
  def unblock(task: Task): Query[Task] =
    task.state match {
      case Blocked(tasks, data, parent) =>
        write(task.copy(state = Forked(tasks, data, parent)))
      case _ => Query.value(task)
    }

  /** Removes `task` from its current runq and places it on the
    * provided host's runq, or the caller's local runq if no `to` is
    * provided.
    */
  def steal(task: Task, to: Option[HostID] = None): Query[Task] =
    Query.repo flatMap { repo =>
      to orElse repo.service.localID match {
        case Some(dest) =>
          write(task.copy(host = dest))
        case None =>
          throw new IllegalArgumentException(
            "Attempted to steal tasks without joining a cluster?")
      }
    }

  def addChild(task: Task, childID: TaskID): Query[Unit] =
    task.state match {
      case Task.Forked(ids, data, parent) =>
        Task.write(task.copy(state = Task.Forked(ids :+ childID, data, parent))).join
      case Task.Blocked(ids, data, parent) =>
        Task
          .write(task.copy(state = Task.Blocked(ids :+ childID, data, parent)))
          .join
      case _ =>
        val msg =
          s"Task is not in `Task.Forked` or `Task.Blocked` state, can't add a child to it."
        Query.fail(new IllegalArgumentException(msg))
    }

  /** Executes `steal()` for each task in `from`'s runq as of `snapshotTS`. */
  def drain(from: HostID, to: Option[HostID] = None): Query[Int] =
    getRunnableByHost(from).foldLeftValuesMT(0) { case (acc, taskID) =>
      steal(taskID, to) map { _ => acc + 1 }
    }

  def remove(id: TaskID): Query[Version.Deleted] =
    TaskColl.internalDelete(id)

  def dryRun(taskID: TaskID, executor: Executor)(
    implicit ctl: AdminControl): Query[State] =
    executor.tryTask(taskID) match {
      case Success(state: State) => Query.value(state)
      case Success(obj) =>
        Query.fail(new RuntimeException(s"Dry run failed with: $obj."))
      case Failure(ex) => Query.fail(ex)
    }

  def dryRun(taskID: TaskID)(implicit ctl: AdminControl): Query[State] = {
    Query.repo flatMap { repo =>
      repo.service.executor.value match {
        case Some(Success(Some(executor))) => dryRun(taskID, executor)
        case Some(Failure(t)) =>
          Query.fail(new RuntimeException("No task executor available.", t))
        case _ => Query.fail(new RuntimeException("No task executor available."))
      }
    }
  }

  def write(task: Task): Query[Task] =
    Query.snapshotTime flatMap { snapshotTime =>
      val ttl = if (task.state.isCompleted) {
        Some(snapshotTime + TTL)
      } else {
        None
      }

      val data = Data(
        HostField -> task.host,
        NameField -> task.name,
        ScopeField -> Some(task.scopeID),
        PriorityField -> task.priority,
        CreatedAtField -> task.createdAt,
        ElapsedField -> task.elapsedMillis,
        TraceField -> task.trace,
        StateField -> task.state,
        AccountField -> task.accountID,
        OperationalField -> task.isOperational,
        Version.TTLField -> ttl
      )

      TaskColl.insert(task.id, data) map apply
    }

  private def legacyState(v: Version): State =
    // cancellation always takes precedence
    if (v.data(CancelledField) getOrElse false) {
      Task.Cancelled()
    } else if (v.data(CompletedField)) {
      Task.Completed()
    } else if (v.data(ForkedField) getOrElse false) {
      Task.Forked(v.data(TasksField), v.data(DataField), v.data(ParentField))
    } else {
      Task.Runnable(v.data(DataField), v.data(ParentField))
    }
}
