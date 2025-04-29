package fauna.cluster

import fauna.atoms._
import fauna.cluster.topology.{ OwnedSegment, ReplicaTopology, SegmentOwnership, Topologies }
import fauna.cluster.workerid.{ WorkerID, WorkerIDs }
import fauna.lang.Timestamp

sealed trait HostStatus
object HostStatus {
  case object Up extends HostStatus
  case object Down extends HostStatus
}

case class NodeInfo(
  status: HostStatus,
  state: String,
  ownership: Float,
  ownershipGoal: Float,
  hostID: HostID,
  hostName: Option[String],
  replica: String,
  workerID: WorkerID,
  persistedTimestamp: Option[Timestamp])

object ClusterStatus {
  def status(
    membership: Membership,
    workerIDs: WorkerIDs,
    topologies: Topologies,
    fd: FailureDetector,
    announcements: Map[HostID, Timestamp]): Seq[NodeInfo] = {

    val b = Seq.newBuilder[NodeInfo]

    val replicaMap =
      // Capture replica names for all segment-owning hosts (including removed ones)
      (topologies.snapshot.replicaTopologies flatMap { case (r, t) =>
        (t.current map { _.host } distinct) filter { _ != HostID.NullID } map { _ -> r }
      } toMap) ++
      // Override with membership information for live hosts
      (membership.hosts map { m => (m, membership.getReplica(m)) } collect { case (m, Some(r)) => (m, r) })

    replicaMap foreach { case (hostID, replica) =>
      val hostInfo = membership.hostInformation(hostID)
      val hostName = hostInfo map { _.hostName }
      val status = if (fd.isAlive(hostID)) {
        HostStatus.Up
      } else {
        HostStatus.Down
      }

      b += NodeInfo(
        status,
        if (hostInfo exists { !_.leaving }) "live" else "removed",
        ownership(hostID, replica, topologies, { _.current } ),
        ownership(hostID, replica, topologies, { _.pending } ),
        hostID,
        hostName,
        replica,
        workerIDs.hostWorkerID(hostID),
        announcements.get(hostID))
    }

    b.result()
  }

  def ownership(hostID: HostID, replica: String, topologies: Topologies, segf: ReplicaTopology => Vector[SegmentOwnership]): Float =
    topologies.snapshot.getTopology(replica) map { t =>
      val owned = OwnedSegment.segmentsForHost(SegmentOwnership.toOwnedSegments(segf(t)), hostID)
      Segment.ringFraction(owned)
    } getOrElse 0.0f
}
