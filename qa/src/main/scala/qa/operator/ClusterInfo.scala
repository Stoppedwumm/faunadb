package qa.operator

import fauna.codex.json.{ JS, JSValue }
import java.net.InetAddress
import java.time.Instant
import java.util.UUID
import scala.util.Try

/**
  * Fauna node status.
  */
sealed trait NodeStatus

/**
  * Available node statuses.
  */
object NodeStatus {
  case object Up extends NodeStatus
  case object Down extends NodeStatus

  def parse(str: String): NodeStatus =
    str match {
      case "up"   => Up
      case "down" => Down
    }
}

/**
  * Fauna node info.
  * @param status
  * @param state
  * @param workerID
  * @param address
  * @param ownership
  * @param ownershipGoal
  * @param hostID
  * @param logSegment
  * @param logSegmentLeader
  * @param persistedTimestamp
  */
case class NodeInfo(
  status: NodeStatus,
  state: String,
  address: Option[InetAddress],
  workerID: Option[String],
  ownership: Float,
  ownershipGoal: Float,
  hostID: Option[UUID],
  logSegment: Option[String],
  logSegmentLeader: Boolean,
  persistedTimestamp: Option[Instant]) {

  val isUp = status == NodeStatus.Up
  val isValid = isUp && address.isDefined && hostID.isDefined
  val isPersisted = persistedTimestamp.isDefined
  val isLive = state == "live"
}

/**
  * Factory for [[NodeInfo]] instances.
  */
object NodeInfo {

  def extract(json: JSValue): NodeInfo = {
    val status = NodeStatus.parse((json / "status").as[String])
    val state = (json / "state").as[String]
    val address = Try(InetAddress.getByName((json / "address").as[String])).toOption
    val workerID = (json / "worker_id").asOpt[String]
    val ownership = (json / "ownership").as[Float]
    val ownershipGoal = (json / "ownership_goal").as[Float]
    val hostID = Try(UUID.fromString((json / "host_id").as[String])).toOption
    val logSegment = (json / "log_segment").asOpt[String]
    val logSegmentLeader = (json / "log_segment_leader").as[Boolean]
    val timestamp = Try(
      Instant.parse((json / "persisted_timestamp").as[String])).toOption

    NodeInfo(
      status,
      state,
      address,
      workerID,
      ownership,
      ownershipGoal,
      hostID,
      logSegment,
      logSegmentLeader,
      timestamp)
  }
}

/**
  * Fauna replica mode.
  */
sealed trait ReplicaMode

/**
  * Available replica modes.
  */
object ReplicaMode {
  case object Data extends ReplicaMode
  case object Log extends ReplicaMode
  case object DataLog extends ReplicaMode

  def parse(str: String): ReplicaMode =
    str.stripPrefix("(").stripSuffix(")") match {
      case "data+log" => DataLog
      case "data"     => Data
      case "log"      => Log
    }
}

/**
  * Fauna replica info.
  * @param name
  * @param mode
  * @param nodes
  */
final case class ReplicaInfo(name: String, mode: ReplicaMode, nodes: Seq[NodeInfo]) {
  val size = nodes.size
}

/**
  * Cluster info.
  * @param replicas
  */
final case class ClusterInfo(replicas: Seq[ReplicaInfo]) {
  val size = replicas.size
}

/**
  * Factory for [[ClusterInfo]] instances.
  */
object ClusterInfo {

  def parse(str: String): ClusterInfo = extract(JS.parse(str))

  def parse(lines: Seq[String]): ClusterInfo = parse(lines.mkString("\n"))

  def extract(json: JSValue): ClusterInfo = {

    val replicas = {
      val nodes = (json / "nodes").as[Seq[JSValue]]

      (json / "replica_info").as[Seq[JSValue]].map { js =>
        val name = (js / "name").as[String]
        val mode = ReplicaMode.parse((js / "type").as[String])

        val ownNodes = nodes.collect {
          case js if (js / "replica").asOpt[String].contains(name) =>
            NodeInfo.extract(js)
        }

        ReplicaInfo(name, mode, ownNodes)
      }
    }

    ClusterInfo(replicas)
  }
}
