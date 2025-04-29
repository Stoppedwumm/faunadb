package fauna.repo

import fauna.atoms._
import fauna.flags.EnableComputeOnlyTaskStealing
import fauna.lang.syntax._
import fauna.lang.TimeBound
import fauna.repo.cassandra.CassandraService
import fauna.repo.query.Query
import java.util.concurrent.ConcurrentSkipListSet
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext
import scala.util.{ Random, Success }

/**
  * Work-stealing cooperates with task execution to prevent a single
  * task executing on multiple hosts concurrently, while attemping to
  * maximize utilization of task executors in the cluster. It
  * accomplishes this in two ways:
  *
  * 1. Idle task executors attempt to find work sitting
  *    un-/under-executed in other healthy hosts' queues, or work
  *    assigned to hosts leaving the cluster.
  *
  * 2. Busy task executors reject steal attempts for tasks currently
  *    being executed.
  */
abstract class WorkStealing(exec: Executor) {

  private[this] lazy val service = CassandraService.instance

  private[this] val log = getLogger()

  protected def unhealthyHostsWithNoTasks: ConcurrentSkipListSet[HostID]

  /**
    * The number of tasks stolen per attempt.
    */
  val TasksPerAttempt = 1

  /**
    * The maximum amount of time given to steal from a
    * presumed-healthy host. This value affects the executor's
    * responsiveness to new tasks.
    */
  val StealTimeout: FiniteDuration

  /** The current node cluster membership from which tasks may be stolen.
    */
  def stealCandidatesQ: Query[Seq[HostID]]

  def stealSize: Int
  def unhealthyStealSize: Int

  /** A subset of cluster members recently known to be healthy.
    * When looking for healthy hosts to steal work from, we want to limit this set to compute nodes as those are
    * the only nodes that we support stealing tasks from.
    */
  def healthyEndpoints: Query[Set[HostID]] =
    service.healthChecker.healthyEndpoints.flatMap { healthyHostIds =>
      Query.repo.flatMap { repo =>
        repo.hostFlag(EnableComputeOnlyTaskStealing).map { computeOnlyTaskStealing =>
          if (computeOnlyTaskStealing) {
            healthyHostIds intersect computeEndpoints
          } else {
            healthyHostIds
          }
        }
      }
    }

  /** Returns the set of compute nodes in the cluster, this falls back to returning the set of 'data' nodes if there
    * are no compute replicas. This is because if there are no compute replicas that means that the data nodes are
    * acting as compute and data.
    */
  def computeEndpoints: Set[HostID] =
    if (service.computeReplicas.nonEmpty) {
      service.computeHosts
    } else {
      service.dataHosts
    }

  /** Certain tasks must be executed on the host on which they are
    * scheduled - cache shootdowns, in particular. This method
    * determines whether the provided task may be executed on any
    * host.
    */
  def isStealable(task: TaskID): Query[Boolean]

  /**
    * Re-assigns the given task to the caller's runq.
    */
  def steal(task: TaskID): Query[Unit]

  /**
    * Receives a steal attempt from another `host`, returning tasks
    * which have been successfully re-assigned to that host.
    */
  def recvSteal(host: HostID, ids: Vector[TaskID], deadline: TimeBound): Vector[TaskID]

  /**
    * Initiates a steal attempt, returning true if any tasks were
    * stolen and false otherwise.
    */
  def attemptSteal()(implicit ec: ExecutionContext): Query[Boolean] =
    (healthyEndpoints, stealCandidatesQ) par { (healthy, stealCandidates) =>
      val candidates = stealCandidates filterNot {
        _ == service.localID.get
      }

      // once a host has left, it will no longer be participating in
      // health checks; it will be a candidate here until it is reaped
      // from the graveyard.
      val unhealthy = candidates filterNot {
        healthy.contains(_)
      }

      log.info(
        s"${exec.name}: Finding someone to steal from. Found ${candidates.size} nodes, of which ${unhealthy.size} are unhealthy")
      if (unhealthy.nonEmpty) {
        val target = Random.choose(unhealthy)
        // rip tasks from their cold, dead hands
        log.info(
          s"${exec.name}: Found an unhealthy node, ripping tasks from $target's cold, dead hands")
        unhealthySteal(target)
      } else {
        service.taskService.value match {
          case Some(Success(Some(svc))) if candidates.nonEmpty =>
            // attempt a coordinated steal from any member of the
            // cluster
            val target = candidates.head

            log.info(s"${exec.name}: Checking if we can steal from $target")
            exec.runQueue(target, stealSize).initValuesT flatMap { tasks =>
              // If there is a single task on the target runQ, it
              // will be processing at full speed. Don't bother
              // trying to steal it.
              if (tasks.size > 1) {
                Query.repo flatMap { repo =>
                  repo.stats.incr("TaskExecutor.StealAttempts.Healthy")
                  log.info(
                    s"${exec.name}: $target has tasks, attempting steal (deadline: $StealTimeout).")
                  Query.future {
                    svc.attemptSteal(tasks.toSeq, target, StealTimeout.bound) map {
                      _.nonEmpty
                    }
                  }
                }
              } else {
                log.info(s"${exec.name}: $target has no tasks, returning")
                Query.False
              }
            }
          case _ => Query.False
        }
      }
    }

  protected def unhealthySteal(host: HostID): Query[Boolean] = {
    val runQ = exec.runQueue(host, unhealthyStealSize).takeT(TasksPerAttempt).flattenT

    val stealQ = runQ flatMap { values =>
      if (values.isEmpty) {
        unhealthyHostsWithNoTasks.add(host)
      }
      
      values map { task =>
        Query.repo flatMap { repo =>
          repo.stats.incr(s"${exec.name}.StealAttempts.Unhealthy")

          isStealable(task) flatMap {
            if (_) {
              steal(task) map { _ =>
                repo.stats.incr(s"${exec.name}.Stolen.Unhealthy")
                true
              }
            } else {
              Query.False
            }
          }
        }
      } sequence
    }

    stealQ map { _.find { _ == true } getOrElse false }
  }
}
