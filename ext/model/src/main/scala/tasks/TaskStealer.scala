package fauna.model.tasks

import fauna.atoms.{ HostID, TaskID }
import fauna.flags.EnableComputeOnlyTaskStealing
import fauna.lang.{ RandomSample, TimeBound }
import fauna.lang.syntax._
import fauna.model.Task
import fauna.repo.{ WeightedGraveyard, WorkStealing }
import fauna.repo.cassandra.CassandraService
import fauna.repo.query.Query
import fauna.trace.{ GlobalTracer, Sampler }
import java.util.concurrent.ConcurrentSkipListSet
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext
import scala.util.Random

class TaskStealer(
  exec: TaskExecutor,
  stealTimeout: FiniteDuration,
  service: CassandraService,
  sampler: Sampler = Sampler.Default,
  enabled: Boolean = true)
    extends WorkStealing(exec) {

  val StealTimeout = stealTimeout

  private[this] val tracer = GlobalTracer.instance.withSampler(sampler)
  protected val unhealthyHostsWithNoTasks = new ConcurrentSkipListSet[HostID]()

  val stealSize = exec.stealSize
  val unhealthyStealSize = exec.unhealthyStealSize

  /** Returns only compute hosts since we only support stealing tasks from compute nodes.
    */
  def stealCandidatesQ: Query[Seq[HostID]] = {
    // Prioritize stealing from departed hosts, proportional to the
    // time since their departure.
    for {
      repo                <- Query.repo
      computeOnlyStealing <- repo.hostFlag(EnableComputeOnlyTaskStealing)
    } yield {
      val hostsToConsider = if (computeOnlyStealing) {
        computeEndpoints.toIndexedSeq
      } else {
        service.hosts.toIndexedSeq
      }
      if (service.leftHosts.nonEmpty) {
        val graveyard = new WeightedGraveyard(
          hostsToConsider,
          service.graveyard
            .filterNot { case (host, _) =>
              unhealthyHostsWithNoTasks.contains(host)
            },
          Task.TTL
        )
        Seq(RandomSample.choose(graveyard))
      } else {
        Random.shuffle(hostsToConsider.toSeq)
      }
    }
  }

  def isStealable(task: TaskID): Query[Boolean] =
    Query.snapshotTime flatMap { ts =>
      Task.get(task, ts) map {
        case None => false
        case Some(t) =>
          exec.router.isStealable(t)
      }
    }

  override def attemptSteal()(implicit ec: ExecutionContext): Query[Boolean] =
    if (enabled) {
      super.attemptSteal()
    } else {
      Query.False
    }

  def steal(task: TaskID): Query[Unit] =
    Query.snapshotTime flatMap { ts =>
      Task.get(task, ts) flatMapT {
        Task.steal(_) map { Some(_) }
      } map { _ => () }
    }

  def recvSteal(
    host: HostID,
    candidates: Vector[TaskID],
    deadline: TimeBound): Vector[TaskID] =
    tracer.withSpan("task.steal") {
      // NB. Racing with the task executor can cause the candidate list to be
      // processed before task stealer can grab them.
      val stealable = exec.processing.tryMarkStealing(candidates)

      def stealQ =
        Query.snapshotTime flatMap { ts =>
          Query.value(stealable) selectMT { id =>
            Task.get(id, ts) flatMap {
              case None    => Query.value(false)
              case Some(t) =>
                // Double-/triple-check this is a good steal candidate
                val mine = service.localID.contains(t.host)
                val stealable = t.isRunnable && exec.router.isStealable(t)

                if (mine && stealable) {
                  Task.steal(t, Some(host)) map { _ => true }
                } else {
                  Query.value(false)
                }
            }
          }
        }

      try {
        val stolen =
          exec.run(exec.repo, stealQ, deadline.timeLeft, "Steal")(_ => ())(
            Vector.empty)
        exec.repo.stats.count(s"TaskExecutor.Stolen.HealthyTaken", stolen.size)
        stolen.toVector
      } finally {
        exec.processing.unmark(stealable)
      }
    }
}
