package fauna.model.tasks

import fauna.atoms._
import fauna.lang.{ AdminControl, TimeBound, Timestamp }
import fauna.lang.syntax._
import fauna.model._
import fauna.repo.cassandra.CassandraService
import fauna.repo.query.Query
import fauna.repo.service.RepairFailed
import fauna.repo.store._
import fauna.storage.{ ScanBounds, Selector }
import fauna.storage.doc._
import java.util.concurrent.TimeoutException
import scala.annotation.unused
import scala.util.Random

/**
  * Runs through all repair tasks, starting with the canonical data in Versions.
  */
object Repair {

  object Effect {

    implicit val EffectT = FieldType.SumCodec[String, Effect](
      Field[String]("effect"),
      "commit" -> FieldType.Empty(Commit),
      "nocommit" -> FieldType.Empty(NoCommit))

    def apply(task: Task) =
      task.data.getOrElse(EffectField, Commit)
  }

  sealed abstract class Effect {

    def isDryRun: Boolean =
      this match {
        case Commit   => false
        case NoCommit => true
      }
  }

  // commit the effects of repair to storage
  case object Commit extends Effect

  // do not commit the effects of repair to storage, i.e. "dry run"
  case object NoCommit extends Effect

  object Mode {

    implicit val ModeT = FieldType.SumCodec[String, Mode](
      Field[String]("mode"),
      "full" -> FieldType.Empty(Full),
      "replication" -> FieldType.Empty(Replication),
      "model" -> FieldType.Empty(Model))

    def apply(task: Task): Mode =
      task.data.getOrElse(ModeField, Full)
  }

  sealed abstract class Mode {

    def dataOnly: Boolean =
      this match {
        case Model              => true
        case Full | Replication => false
      }
  }

  // repair both replication and data model
  case object Full extends Mode

  // repair replication, do not repair data model
  case object Replication extends Mode

  // repair data model, do not repair replication
  case object Model extends Mode

  val TaskVersion = 1

  // journal entries associated with repair jobs will be filed under
  // this tag
  val EntryTag = "repair"

  // each journal entry keeps a pointer to its root task for repair
  // status queries
  val TaskField = Field[TaskID]("task_id")

  val RangesField = Field[Vector[Segment]]("ranges")
  val StateField = Field[String]("state")

  // each RootTask keeps a pointer to its journal entry for
  // completion/cancellation
  val EntryField = Field[JournalEntryID]("entry_id")

  val EffectField = Field[Effect]("effect")

  val ModeField = Field[Mode]("mode")

  val FilterScopeField = Field[Option[ScopeID]]("filter_scope")

  case object RootTask extends Type("repair-join", TaskVersion) {

    override def isStealable(t: Task) = false

    def pendingState(snapshotTS: Timestamp) =
      JournalEntry.readByTag(EntryTag, snapshotTS) flatMapValuesT { entry =>
        Task.get(entry.data(TaskField), snapshotTS) map {
          case None => Seq.empty
          case Some(task) =>
            val stage = task.data(StateField) match {
              case "start" | "join-replication" => "replication"
              case "lookups" | "join-lookups" | "join"  => "lookups"
            }

            val forkedRemaining = task match {
              case Task.Children(tasks) => tasks.size
              case _                    => 0
            }

            Seq((stage, forkedRemaining))
        }
      }

    def step(task: Task, snapshotTS: Timestamp): Query[Task.State] =
      task.data(StateField) match {
        case "start" =>
          if (Mode(task).dataOnly) { // skip replication repair
            Query.value(
              Task.Runnable(task.data.update(StateField -> "lookups"), None))
          } else {
            task.fork {
              val filterScope = task.data(FilterScopeField)
              val restore = RestoreReplicationIncremental.create(
                task.id,
                Effect(task),
                filterScope)

              restore map {
                (_, task.data.update(StateField -> "join-replication"))
              }
            }
          }

        case "join-replication" =>
          task.joinThen() {
            case Left(_) => Query.value(Task.Cancelled())
            case Right(data) =>
              Query(Task.Runnable(data.update(StateField -> "lookups"), None))
          }

        case "lookups" =>
          task.fork {
            val filterScope = task.data(FilterScopeField)
            RepairLookups.create(task.id, Effect(task), filterScope) map {
              (_, task.data.update(StateField -> "join-lookups"))
            }
          }

        case "join-lookups" =>
          task.joinThen() {
            case Left(_) => cancel(task)
            case Right(data) =>
              Query(Task.Runnable(data.update(StateField -> "reverse"), None))
          }

        case "reverse" =>
          task.fork {
            val effect = Effect(task)
            val filterScope = task.data(FilterScopeField)
            RepairReverseLookups.create(task.id, effect, filterScope) map {
              lookups =>
              (lookups,task.data.update(StateField -> "join"))
            }
          }

        case "join" =>
          task.joinThen() {
            case Right(_) =>
              JournalEntry.remove(task.data(EntryField)) map { _ =>
                Task.Completed()
              }
            case Left(_) => cancel(task)
          }
      }

    /**
      * Create a new repair task, if none exists as of snapshotTS.
      */
    def createOpt(
      snapshotTS: Timestamp,
      effect: Effect,
      mode: Mode,
      filterScope: Option[ScopeID] = None): Query[Option[Task]] = {
      val data = Data(
        StateField -> "start",
        EffectField -> effect,
        ModeField -> mode,
        FilterScopeField -> filterScope)

      val t = JournalEntry.readByTag(EntryTag, snapshotTS).nonEmptyT
      t flatMap {
        case true => Query.none
        case false =>
          Task.createLocal(
            Database.RootScopeID,
            name,
            data,
            None,
            isOperational = true) flatMap { task =>
            JournalEntry.writeLocal(EntryTag, Data(TaskField -> task.id)) flatMap {
              case None =>
                Query.fail(
                  new IllegalStateException("cannot create repair journal entry"))
              case Some(entry) =>
                Task.runnable(
                  task,
                  task.data.update(EntryField -> entry.id),
                  None,
                  0) map { Some(_) }
            }
          }
      }
    }

    override def cancelAll(router: TaskRouter): Query[Unit] =
      Task.getAllRunnableAndPaused() selectT { _.name == name } foreachValueT { task =>
        router.cancel(task, None) flatMap { _ =>
          JournalEntry.remove(task.data(EntryField))
        }
      }

    private def cancel(task: Task) =
      JournalEntry.remove(task.data(EntryField)) map { _ =>
        Task.Cancelled()
      }
  }

  case object RestoreReplicationIncremental
      extends Type("repair-replicas-incremental", TaskVersion) {
    // replication will be repaired up to this point in time
    private val SnapshotField = Field[Timestamp]("snapshot_time")

    // the replica randomly chosen to coordinate repair
    private val ReplicaField = Field[String]("replica")

    // the list of hosts in the chosen replica at the beginning of the
    // repair
    private val InitialField = Field[Vector[HostID]]("initial")

    // the host currently coordinating repair
    private val CurrentField = Field[HostID]("current")

    // the segments replicated by the current coordinator host which
    // remain to be repaired
    private val SegmentsField = Field[Vector[Segment]]("segments")

    // the list of hosts in the chosen replica which have completed
    // repair for their replicated segments
    private val RepairedField = Field[Vector[HostID]]("repaired")

    // the list of CF names to be targeted (optional, will default to "Versions" & "_RowTimestamps_")
    private val CFNamesField = Field[Vector[String]]("cf_names")

    //FIXME: dig out of query context or provide via construction
    private def service = CassandraService.instance

    private val log = getLogger

    override def isStealable(t: Task) = false

    def step(task: Task, snapshotTS: Timestamp): Query[Task.State] =
      task.data(StateField) match {
        case "start" =>
          val replica = task.data(ReplicaField)

          checkReplica(task, replica) flatMap {
            case Left(state) => Query.value(state)
            case Right(_) =>
              val hosts = service.hostsInReplica(replica)
              val data = task.data.update(
                StateField -> "validate",
                InitialField -> hosts.toVector,
                RepairedField -> Vector.empty)
              Query.value(Task.Runnable(data, task.parent))
          }
        case "validate" =>
          val replica = task.data(ReplicaField)
          val init = task.data(InitialField).toSet
          val done = task.data(RepairedField).toSet
          val ts = task.data(SnapshotField)

          Query.repo flatMap { repo =>
            target(task, replica, init, done) flatMap {
              case Left(state) => Query.value(state)
              case Right(host) =>
                val timeout = repo.repairTimeout.bound
                val segments = task.data.getOrElse(
                  SegmentsField,
                  repo.keyspace.segmentsForHost(host).toVector)
                val seg = segments.head
                val filterScope = task.data(FilterScopeField)
                val cfNames = task.data.getOrElse(CFNamesField, Vector("Versions", "_RowTimestamps_"))

                repair(host, seg, ts, timeout, filterScope, cfNames) map { _ =>
                  val progress = segments.tail

                  val data = if (progress.isEmpty) {
                    log.info(s"Repair complete for $host.")
                    task.data
                      .remove(CurrentField)
                      .remove(SegmentsField)
                      .update(RepairedField -> (done + host).toVector)
                  } else {
                    repo.stats.set(
                      "Repair.Replication.Segments.Remaining",
                      progress.size)
                    log.info(
                      s"Repair complete for $seg on $host. ${progress.size} segments remaining.")
                    task.data.update(
                      CurrentField -> host,
                      SegmentsField -> progress)
                  }

                  Task.Runnable(data, task.parent)
                } recover {
                  case ex @ (_: TimeoutException | _: RepairFailed) =>
                    repo.stats.incr("Repair.Replication.Segments.Split")
                    val segs = seg.split(seg.midpoint)
                    log.warn(
                      s"Repair failed for $seg on $host, splitting segment into $segs. (${ex.getMessage})")
                    // attempt to progress by splitting the current
                    // segment, and retrying
                    val data = task.data.update(
                      CurrentField -> host,
                      SegmentsField -> (segs ++ segments.tail).toVector)
                    Task.Runnable(data, task.parent)
                }
            }
          }
      }

    def create(
      parent: TaskID,
      @unused effect: Effect,
      filterScope: Option[ScopeID]): Query[Iterable[Task]] = {
      val replica = Random.choose(service.dataReplicas.toSeq)
      Query.snapshotTime flatMap { snapshotTS =>
        val data = Data(
          StateField -> "start",
          ReplicaField -> replica,
          SnapshotField -> snapshotTS,
          FilterScopeField -> filterScope)

        Task.createRandom(Database.RootScopeID, name, data, Some(parent)) map {
          Seq(_)
        }
      }
    }

    def create(
      filterScope: Option[ScopeID],
      cfNames: Vector[String])(implicit @unused ctl: AdminControl): Query[Iterable[Task]] = {
      val replica = Random.choose(service.dataReplicas.toSeq)
      Query.snapshotTime flatMap { snapshotTS =>
        val data = Data(
          StateField -> "start",
          ReplicaField -> replica,
          SnapshotField -> snapshotTS,
          FilterScopeField -> filterScope,
          CFNamesField -> cfNames)

        Task.createRandom(Database.RootScopeID, name, data, None) map {
          Seq(_)
        }
      }
    }

    // Provided with a replica, a set of initial hosts in the cluster,
    // and a set of hosts which have been repaired, find the next
    // target host to be repaired. If the cluster topology has
    // changed, move the state machine back one step.
    private def target(
      task: Task,
      replica: String,
      init: Set[HostID],
      done: Set[HostID]) =
      checkReplica(task, replica) flatMapT { _ =>
        val hosts = service.hostsInReplica(replica)

        Query.repo map { repo =>
          if (init forall { hosts.contains(_) }) {
            // if the replica added a new partition, it cannot be in
            // `done`, therefore we will pick it up naturally as a
            // candidate here
            val candidates = hosts -- done

            if (candidates.isEmpty) {
              // repair complete!
              repo.stats.incr("Repair.Replication.Complete")
              log.info(s"Repair complete for $replica.")
              Left(Task.Completed())
            } else {
              repo.stats.set("Repair.Replication.Hosts.Remaining", candidates.size)
              val host =
                task.data.getOrElse(CurrentField, Random.shuffle(candidates).head)
              Right(host)
            }
          } else {
            // lost a host, try again in the same replica
            val data = task.data.update(StateField -> "start")

            repo.stats.incr("Repair.Replication.Restart")
            log.warn("Repair detected a topology change. Restarting...")
            Left(Task.Runnable(data, task.parent))
          }
        }
      }

    private def checkReplica(
      task: Task,
      replica: String): Query[Either[Task.State, Unit]] =
      if (service.dataReplicas contains replica) {
        Query.value(Right(()))
      } else {
        // lost our replica, try again.
        val replica = Random.choose(service.dataReplicas.toSeq)
        val data = task.data.update(StateField -> "start", ReplicaField -> replica)

        Query.repo map { repo =>
          repo.stats.incr("Repair.Replication.Restart")
          log.warn("Repair detected a topology change. Restarting...")
          Left(Task.Runnable(data, task.parent))
        }
      }

    private def repair(
      host: HostID,
      segment: Segment,
      snapshotTS: Timestamp,
      timeout: TimeBound,
      filterScope: Option[ScopeID],
      cfNames: Vector[String]) = {
      def repairCF(cf: String) =
        Query.repo flatMap { repo =>
          Query.future {
            repo.keyspace.repair(
              host,
              Database.RootScopeID,
              cf,
              segment,
              snapshotTS,
              timeout,
              filterScope)
          }
        }
      cfNames.map{ repairCF(_) }.join
    }

  }

  sealed trait RepairSubtask { self: Type =>

    override final def isStealable(t: Task) = false

    def backgrounded[T](q: Query[T]): Query[T] = {
      for {
        snapshotTS <- Query.snapshotTime
        repoCtx <- Query.repo
        result <- Query.future {
          // NB. Set background priority and disables backup reads.
          // Repair must read from local replica only in order to fix
          // its inconsistencies. This is safe considering versions
          // and row timestamps are restored from outside of query
          // boundaries (via TransferService). Local replicas should
          // be able to deterministically rebuild their state once
          // versions and timestamps are consistent.
          repoCtx.forRepair.result(q, snapshotTS)
        }
      } yield result.value
    }

    def create(
      ranges: Iterable[Segment],
      effect: Repair.Effect,
      filterScope: Option[ScopeID]): Data =
      Data(
        RangesField -> ranges.toVector,
        EffectField -> effect,
        FilterScopeField -> filterScope)

    def create(
      parent: TaskID,
      effect: Repair.Effect,
      filterScope: Option[ScopeID]): Query[Iterable[Task]] =
      Task.createAllSegments(Database.RootScopeID, name, Some(parent)) { (segs, _) =>
        create(segs, effect, filterScope)
      }
  }

  case object RepairLookups
      extends PaginateType[Segment]("repair-lookups", TaskVersion)
      with RepairSubtask
      with VersionSegmentMVTPaginator {

    def step(task: Task, snapshotTS: Timestamp): Query[Task.State] = {
      // The only two collections indexed into Lookups are Database
      // and Key.
      val colls = Set(DatabaseID.collID, KeyID.collID)

      val selector = task.data(FilterScopeField) match {
        case Some(scope) => Selector.from(scope, colls)
        case None        => Selector.Schema(colls)
      }

      val iter = LookupRepair.VersionTask(snapshotTS, Effect(task))
      val q = paginate(
        task,
        RangesField,
        "Lookup.Repair",
        iter,
        selector)
      backgrounded(q)
    }
  }

  case object RepairReverseLookups
      extends Type("repair-reverse-lookups", TaskVersion)
      with RepairSubtask {

    def step(task: Task, snapshotTS: Timestamp): Query[Task.State] = {
      val q = foreach(task, RangesField, "Lookup.Repair.Ranges.Remaining") { rng =>
        val filterScope = task.data(FilterScopeField)

        LookupRepair
          .LookupTask(snapshotTS, Effect(task), filterScope)
          .run(LookupStore.localScan(ScanBounds(rng)))
      }
      backgrounded(q)
    }
  }
}
