package fauna.cluster

import fauna.atoms.HostID
import fauna.cluster.topology.{
  OwnedSegment,
  Topologies
}
import fauna.lang.syntax._
import fauna.net.HostService
import scala.concurrent.{ ExecutionContext, Future }

/**
  * Encapsulates the coordination logic for removing a node from the cluster.
  */
class NodeRemoval(
  membership: Membership,
  topologies: Topologies,
  hostService: HostService)(implicit ec: ExecutionContext) {

  private[this] val log = getLogger

  @volatile private[this] var running = true

  setupForceRemovalListener()

  def stop(): Unit = {
    running = false
  }

  /**
    * Removes a node from the cluster.
    *
    * @param hostID the host id of the node to be removed
    * @param force when true, consider the host permanently gone
    * @return a Future that represents the completion of the removal operation
    * @throws IllegalArgumentException if the operation can not be completed for the node
    */
  def removeNode(hostID: HostID, force: Boolean): Future[Unit] =
    membership.getReplica(hostID) match {
      case Some(replicaName) =>
        val ts = topologies.snapshot
        val replicaExists = ts.getTopology(replicaName).nonEmpty
        if (replicaExists && membership.liveHostsInReplica(replicaName) == Set(hostID)) {
          illegalArg(
            s"Removing $hostID would leave its data replica $replicaName empty. Update the replica to compute type first.")
        } else if (force) {
          log.warn(s"Membership: Removing $hostID by force.")
          // when forced, consider the host permanently failed and
          // remove it immediately
          membership.remove(hostID, force = true)
        } else if (!canSafelyRemove(hostID)) {
          illegalArg(cantSafelyRemoveMsg(hostID))
        } else {
          // This method only marks the host as leaving in the membership. A separate listener
          // (see setupNodeRemovalListener below) ensures that a leaving node that owns no segments
          // and is not an irreplaceable log node will be removed.
          membership.markLeaving(hostID)
        }
      case _ => Future.unit
    }

  private def canSafelyRemove(hostID: HostID) =
    hostService.isLive(hostID) || membership.canSafelyRemove(hostID)

  private def cantSafelyRemoveMsg(hostID: HostID) =
    s"Node $hostID can only be removed with forced removal."

  /**
    * When a host is forcibly removed, replica topology will be left
    * with segments owned by that host which must be reassigned, as
    * the removed host may have been the last remaining replica for
    * those segments.
    */
  private def setupForceRemovalListener(): Unit = {
    def onChange: Future[Boolean] = {
      val gone = membership.leftHosts
      val topo = topologies.snapshot.replicaTopologies

      val fs = topo map {
        case (replica, topo) =>
          val segs = topo.currentSegments
          val orphans = segs collect {
            case OwnedSegment(seg, host) if gone(host) => seg
          }

          topologies.announceOrphans(replica, orphans)
      }


      fs.join map { _ => running }
    }

    membership.subscribeWithLogging(onChange)
  }

  private def illegalArg(msg: String) =
    Future.failed(new IllegalArgumentException(msg))
}
