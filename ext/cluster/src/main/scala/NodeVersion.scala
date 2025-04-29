package fauna.cluster

import fauna.atoms.HostID

case class NodeVersion(
  hostID: HostID,
  replica: Option[String],
  hostName: Option[String],
  buildVersion: Option[String])

object NodeVersion {
  def versions(buildVersions: Map[HostID, String], membership: Membership): Seq[NodeVersion] = {
    membership.hosts map { hostID =>
      val replica = membership.getReplica(hostID)
      val hostName = membership.getHostName(hostID)

      NodeVersion(hostID, replica, hostName, buildVersions.get(hostID))
    } toSeq
  }
}
