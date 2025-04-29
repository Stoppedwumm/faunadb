package fauna.repo.service

import fauna.atoms.HostID
import fauna.cluster.{ FailureDetector, Membership }
import fauna.cluster.topology.Topologies
import fauna.exec.{ ImmediateExecutionContext, LoopThreadService }
import fauna.flags.{
  EnableRingAvailabilityCheck,
  EnableRingAvailabilityCheckPeriodMinutes,
  HostFlags,
  HostProperties,
  Service => FFClient
}
import fauna.lang.clocks.Clock
import fauna.repo.service.ringAvailability.RingLocations
import fauna.stats.StatsRecorder
import fauna.tx.transaction.Partitioner
import scala.concurrent.duration._
import scala.concurrent.Future

/** Ring Availability Service is responsible for emitting metrics that depict
  * overall availability of a Location on the Ring.
  */
final class RingAvailabilityService(
  failureDetector: FailureDetector,
  membership: Membership,
  partitioner: => Partitioner[_, _],
  stats: StatsRecorder,
  topologies: Topologies,
  flagsProperties: () => HostProperties,
  flagsClient: FFClient
) extends LoopThreadService("Ring Availability") {

  private def hostFlags(): Future[HostFlags] = {
    implicit val ec = ImmediateExecutionContext
    val props = flagsProperties()
    flagsClient.getUncached[HostID, HostProperties, HostFlags](props) map { flags =>
      flags.getOrElse(HostFlags(props.id))
    }
  }

  override protected def loop(): Unit = {
    implicit val ec = ImmediateExecutionContext
    val flagsFut = hostFlags() map { flags =>
      (
        flags.get(EnableRingAvailabilityCheck),
        flags.get(EnableRingAvailabilityCheckPeriodMinutes))
    }

    flagsFut foreach { case (check, period) =>
      if (check && membership.isMember) {
        val availability = RingLocations.percentAvailable(
          dataReplicas = topologies.snapshot.replicaNames.toSet,
          hostsByReplica = membership.hostsInReplica,
          isAlive = failureDetector.isAlive,
          segmentsByHost = partitioner.segments(_, pending = false)
        )

        stats.set("RingAvailability.Percent", availability)
      }
      Clock.sleepUninterruptibly(period.minutes)
    }
  }
}
