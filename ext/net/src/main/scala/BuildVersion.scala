package fauna.net

import fauna.atoms.HostID
import fauna.codex.cbor.CBOR
import fauna.exec.Timer
import fauna.net.bus.SignalID
import fauna.net.gossip.GossipAdaptor
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

case class GossipVersionInfo(hostID: HostID, hostVersion: String)

object GossipVersionInfo {
  implicit val codec = CBOR.TupleCodec[GossipVersionInfo]
}

object BuildVersion {
  private[BuildVersion] val GossipVersionInterval = 1.minute
}

class BuildVersion(val buildVersion: String, signal: SignalID, hostID: HostID, gossip: GossipAdaptor) {
  private val _hostVersions = new ConcurrentHashMap[HostID, String]

  registerHostVersion(hostID, buildVersion)

  def registerHostVersion(id: HostID, version: String) = _hostVersions.put(id, version)

  def hostVersions: Map[HostID, String] = _hostVersions.asScala.toMap

  private val handler = gossip.handler[GossipVersionInfo](signal) {
    case (_, GossipVersionInfo(hostId, hostVersion)) =>
      registerHostVersion(hostId, hostVersion)
      Future.unit
  }

  Timer.Global.scheduleRepeatedly(BuildVersion.GossipVersionInterval, !handler.isClosed) {
    gossip.send(signal, GossipVersionInfo(hostID, buildVersion))
  }

  def stop(): Unit =
    handler.close()
}
