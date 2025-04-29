package fauna.repo

import fauna.atoms._
import fauna.cluster.topology._
import fauna.cluster.Membership
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.net.util.FutureSequence
import fauna.stats.StatsRecorder
import fauna.storage.StorageEngine
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.Future

class StorageCleaner(
  storage: StorageEngine,
  membership: Membership,
  topologies: Topologies,
  sequencer: FutureSequence,
  stats: StatsRecorder = StatsRecorder.Null)
    extends ExceptionLogging {

  private[this] val logger = getLogger
  private[this] val running = new AtomicBoolean

  // On startup, we don't know what this node owns, so presume it owns the
  // whole ring in its replica. That way, we'll definitely do an initial
  // cleanup (except if this node really owns the whole ring.)
  @volatile private[this] var lastCleanedOwnership = Vector(
    OwnedSegment(Segment.All, membership.self))

  def needsCleanup(cf: Option[String]): Boolean =
    topologies.snapshot.getTopology(membership.replica) map {
      case ReplicaTopology(current, _, _) =>
        val self = membership.self
        val curr = SegmentOwnership.toOwnedSegments(current)
        val mine = curr filter { _.host == self } map { _.segment }

        cf match {
          case None     => storage.needsCleanup(mine)
          case Some(cf) => storage.needsCleanup(cf, mine)
        }
    } getOrElse false

  def scheduleCleanup(): Unit =
    if (running.compareAndSet(false, true)) {
      logger.info(s"Storage cleanup scheduled.")
      // A shared sequencer is used to ensure cleanup is not running concurrently
      // with TopologyReconciler that might be updating the current segments for
      // this node.
      logException(sequencer(Future(cleanup()))) andThen { t =>
        if (t.isFailure) stats.incr("Storage.Cleanup.Failure")
        running.set(false)
      }
    } else {
      logger.warn(s"Storage cleanup is already running.")
    }

  private def cleanup(): Unit =
    topologies.snapshot.getTopology(membership.replica) foreach {
      case rt @ ReplicaTopology(current, _, _) =>
        require(rt.isStable, "cannot safely cleanup storage with unstable topology")

        val self = membership.self
        val curr = SegmentOwnership.toOwnedSegments(current)
        val needsCleanup =
          OwnedSegment.differentHosts(lastCleanedOwnership, curr) exists {
            _.host == self
          }

        if (needsCleanup) {
          stats.incr("Storage.Cleanup.Started")

          try {
            stats.time("Storage.Cleanup.Time") {
              logger.info("Storage cleanup starting.")
              // There are segments in lastCleanedOwnership belonging to this host
              // that are not there anymore in current. A cleanup is warranted.
              val owned = curr collect {
                case OwnedSegment(seg, `self`) => seg
              }

              // Not using forall eagerly as we want to run as many of cleanups as possible
              val results = storage.columnFamilyNames map {
                storage.cleanup(_, owned)
              }

              if (results forall identity) {
                // FIXME: would be nice to have a measure of the space that was freed up
                logger.info("Storage cleanup completed successfully.")
                lastCleanedOwnership = curr
              } else {
                logger.info("Storage cleanup completed with some failures.")
              }
            }
          } finally {
            stats.incr("Storage.Cleanup.Ended")
          }
        } else {
          logger.info("Storage cleanup is unnecessary. Skipping.")
        }
    }
}
