package fauna.repo

import fauna.atoms.HostID
import fauna.cluster.Membership
import fauna.exec.{ BackOff, LoopThreadService }
import fauna.lang.{ Logger, Timestamp, Timing }
import fauna.logging.ExceptionLogging
import fauna.repo.query.Query
import fauna.repo.store.HealthCheckStore
import fauna.stats.StatsRecorder
import fauna.trace.{ Attributes, GlobalTracer }
import java.util.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.util.control.NonFatal

/**
  * This is a convenience trait, which provides a common
  * backoff-when-unhealthy strategy when mixed in.
  */
trait HealthCheck extends ExceptionLogging {
  def name: String
  def repo: RepoContext
  def log: Logger
  def backOff: BackOff

  /**
    * Backs off if the local host is unhealthy.
    */
  def localHealthCheck() =
    healthCheck("local health check", repo.isLocalHealthy)

  /**
    * Backs off if any hosts in this cluster are unhealthy.
    */
  def clusterHealthCheck() =
    healthCheck("cluster health check", repo.isClusterHealthy)

  /**
    * Backs off if any hosts in a data replica are unhealthy.
    */
  def storageHealthCheck() =
    healthCheck("storage health check", repo.isStorageHealthy)

  private def healthCheck(reason: String, healthy: => Boolean) = {
    val tracer = GlobalTracer.instance
    tracer.withSpan("healthcheck") {
      if (!healthy) {
        val timing = Timing.start
        log.info(s"$name: Delaying due to backpressure from $reason.")

        backOff { _ => BackOff.Operation.breakIf(healthy) }

        val total = timing.elapsedMillis
        log.info(s"$name: Delayed ${total.millis} due to backpressure from $reason.")
        tracer.activeSpan foreach {
          _.addAttribute(Attributes.HealthCheck.BackOff, total)
        }
        repo.keyspace.stats.timing(s"$name.BackOff.Time", total)
      }
    }
  }
}

final class HealthChecker(
  hostID: HostID,
  membership: Membership,
  stats: StatsRecorder,
  minPersistedTS: => Timestamp,
  dataReplicas: => Set[String],
  hintGlobalPersistedTS: Timestamp => Unit) {

  private[this] def all[T](f: Map[HostID, Timestamp] => T): Query[Option[T]] =
    Query.defer {
      HealthCheckStore.getAll flatMap { hostsTs => Query.some(f(hostsTs)) }
    }

  private[this] def minGlobalPersistedTimestamp: Query[Timestamp] =
    all { announced =>
      val drs = dataReplicas
      membership.hosts map { host =>
        announced.get(host).fold(Timestamp.Epoch) { ep =>
          if (ep != Timestamp.MaxMicros) {
            ep
          } else {
            // Treat Timestamp.MaxMicros with a grain of salt. It was emitted by a
            // node in a compute-only replica, but...
            membership.getReplica(host).fold(Timestamp.MaxMicros) { rname =>
              if (drs contains rname) {
                /// ... if its replica became a data replica since it last
                // emitted a health-check timestamp, then we know this value is
                // stale so we need to treat it as if it were missing (thus,
                // prevent log truncation temporarily.) It'll emit a more
                // accurate timestamp on its next health check anyway.
                Timestamp.Epoch
              } else {
                Timestamp.MaxMicros
              }
            }
          }
        }
      } min
    } map {
      // Can't yet decide what cluster are we in. Don't accidentally truncate log.
      _ getOrElse Timestamp.Epoch
    }

  /**
    * Returns the set of health endpoints within this cluster.
    */
  def healthyEndpoints: Query[Set[HostID]] =
    all { _.keySet } map { _ getOrElse Set.empty }

  /**
    * Returns true if all hosts in this cluster are among the healthy
    * endpoints.
    */
  val isClusterHealthy: Query[Boolean] =
    healthyEndpoints map { eps =>
      val healthy = membership.hosts forall { eps contains _ }

      val stat = if (healthy) {
        "HealthCheck.Cluster.Passed"
      } else {
        "HealthCheck.Cluster.Failed"
      }

      stats.incr(stat)
      healthy
    } recoverWith {
      case _: UninitializedException => Query.False
    }

  /**
    * Returns true if all hosts in a data replica are among the
    * healthy endpoints.
    */
  val isStorageHealthy: Query[Boolean] =
    healthyEndpoints map { eps =>
      val storage = dataReplicas flatMap { membership.hostsInReplica(_) }
      val healthy = storage forall { eps contains(_) }

      val stat = if (healthy) {
        "HealthCheck.Storage.Passed"
      } else {
        "HealthCheck.Storage.Failed"
      }

      stats.incr(stat)
      healthy
    } recoverWith {
      case _: UninitializedException => Query.False
    }

  /**
    * Returns true if the local host is among the healthy endpoints.
    */
  val isLocalHealthy: Query[Boolean] =
    healthyEndpoints map { eps =>
      val healthy = eps.contains(hostID)

      val stat = if (healthy) {
        "HealthCheck.Local.Passed"
      } else {
        "HealthCheck.Local.Failed"
      }

      stats.incr(stat)
      healthy
    } recoverWith {
      case _: UninitializedException => Query.False
    }

  def mkUpdater(repo: RepoContext): HealthCheckUpdater =
    new HealthCheckUpdater(repo)

  /**
    * Updaters are responsible for emitting a write through the
    * transaction log via the HealthCheckStore to prove to the rest of
    * the cluster that this host is healthy.
    *
    * It will also call the `hintPersistedTimestamp` callback with an
    * observation of the minimum persisted timestamp across all
    * members in this cluster. The hinted timestamp is used to control
    * transaction log truncation. See
    * `PipelineCtx.hintGlobalPersistedTimestamp()`.
    */
  final class HealthCheckUpdater(repo: RepoContext)
      extends LoopThreadService("Cluster HealthCheck") {
    private def markHealthy(): Unit =
      repo.runSynchronously(
        HealthCheckStore.insert(hostID, minPersistedTS),
        repo.healthCheckTimeout)

    private def hintPersisted(): Unit = {
      val q = minGlobalPersistedTimestamp foreach hintGlobalPersistedTS
      repo.runSynchronously(q, repo.healthCheckTimeout)
    }

    protected def loop() = {
      var healthy = false
      try {
        markHealthy()
        hintPersisted()
        healthy = true
      } catch {
        case _: TimeoutException =>
          () // Silence local timeouts. Peers will notice that this host isn't hinting.

        case NonFatal(ex) =>
          logException(ex)
      }

      val period = if (healthy) {
        repo.healthCheckHealthyPeriod
      } else {
        repo.healthCheckUnhealthyPeriod
      }
      Thread.sleep(period.toMillis)
    }
  }
}
