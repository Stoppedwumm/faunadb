package fauna.qa.operator

import fauna.codex.cbor.CBOR
import fauna.net.util.EncodedTimeBound
import fauna.qa._
import fauna.qa.net.OperatorRep
import fauna.qa.net.OperatorRep.Success
import fauna.qa.operator.Cmd.Admin
import java.net.InetAddress
import java.nio.file.Path
import qa.operator.{ ClusterInfo, NodeInfo, NodeStatus, ReplicaMode }
import scala.concurrent.duration._
import scala.util.Try

sealed abstract class Operation

object Operation {

  implicit val Codec: CBOR.Codec[Operation] =
    CBOR.SumCodec[Operation](
      CBOR.RecordCodec[Squelch],
      CBOR.RecordCodec[Pause],
      CBOR.RecordCodec[Repeat],
      CBOR.RecordCodec[SlackMsg],
      CBOR.RecordCodec[Annotate],
      CBOR.RecordCodec[Remote],
      CBOR.SingletonCodec(StartTraffic),
      CBOR.SingletonCodec(StopTraffic)
    )
}

/** Squelch messages are sent to Workers that we should or should not send requests to a Fauna instance. */
case class Squelch(flag: Boolean = true, hosts: Vector[Host] = Vector.empty)
    extends Operation {
  def apply(host: Host): Squelch = apply(Vector(host))
  def apply(hosts: Vector[Host]): Squelch = copy(hosts = hosts)
}

/** Pause messages delay the processing of any more Operations. */
case class Pause(duration: Duration) extends Operation

case class Repeat(ops: Vector[Operation], deadline: EncodedTimeBound)
    extends Operation

/** Send a slack message to the #qa-notify channel. */
case class SlackMsg(title: String, fields: Vector[(String, String)])
    extends Operation

object SlackMsg {
  def apply(title: String): SlackMsg = SlackMsg(title, Vector.empty)
}

/** Annotate metrics dashboard */
case class Annotate(
  title: String,
  fields: Vector[(String, String)],
  extraTags: Vector[(String, String)] = Vector.empty
) extends Operation

case object StartTraffic extends Operation
case object StopTraffic extends Operation

// Send a sequence of operations to a remote Operator
case class Remote(cmds: Vector[Cmd], on: Vector[Host]) extends Operation {
  def apply(host: Host) = copy(on = Vector(host))
  def apply(hosts: Vector[Host]) = copy(on = hosts)
}

// verification step
final case class Verify(
  cmd: Cmd,
  on: Vector[Host],
  reduce: Map[Host, OperatorRep] => Either[String, Unit])
    extends Operation

// available verifications
object Verify {

  private def parseClusterInfo(host: Host, rsp: Map[Host, OperatorRep]) =
    rsp.head._2 match {
      case Success(out, _) => ClusterInfo.parse(out)
      case _               => throw RunCmdFailedException(Admin.GetStatus, Vector(host))
    }

  private def fromAdminStatus(check: ClusterInfo => Boolean)(host: Host) =
    Verify(
      Admin.GetStatus,
      Vector(host),
      rsps =>
        Try {
          val ci = parseClusterInfo(host, rsps)
          assert(check(ci))
        }.toEither.left.map(_.toString))

  def assertClusterTopology(expected: Map[String, Int]) =
    fromAdminStatus { ci =>
      val topology = ci.replicas.map(ri => ri.name -> ri.size).toMap
      topology == expected
    } _

  def assertReplicaCount(expected: Int) =
    fromAdminStatus { ci =>
      ci.size == expected
    } _

  def assertReplicaModes(pairs: (String, ReplicaMode)*) =
    fromAdminStatus { ci =>
      pairs.forall { pair =>
        ci.replicas.exists(ri => ri.name == pair._1 && ri.mode == pair._2)
      }
    } _

  def assertNodeStatuses(pairs: (InetAddress, NodeStatus)*) =
    fromAdminStatus { ci =>
      val allNodes = for {
        replica <- ci.replicas
        nodes <- replica.nodes
      } yield nodes
      val nodeStatuses = allNodes.collect {
        case NodeInfo(status, _, Some(addr), _, _, _, _, _, _, _) => addr -> status
      }.toMap
      pairs.forall { pair => nodeStatuses.get(pair._1).contains(pair._2) }
    } _
}

object Remote {
  import Cmd._

  def apply(todos: Vector[Cmd], host: Host): Remote =
    Remote(todos, Vector(host))

  def apply(todo: Cmd, hosts: Vector[Host]): Remote =
    Remote(Vector(todo), hosts)

  def apply(todo: Cmd, host: Host): Remote =
    Remote(Vector(todo), Vector(host))

  def apply(todo: Cmd): Remote =
    Remote(Vector(todo), Vector.empty[Host])

  val PauseFauna = Vector(Squelch(), Remote(Proc.SignalFauna(19 /* SIGSTOP */ )))

  def StartNode(node: Host, useHttps: Boolean = false) =
    StartNodes(Vector(node), useHttps)

  def StartNodes(nodes: Vector[Host], useHttps: Boolean = false) =
    Vector(
      Remote(Vector(Proc.StartNode, Proc.WaitForPing(useHttps)), nodes),
      Squelch(false, nodes)
    )

  def StopNode(node: Host) =
    StopNodes(Vector(node))

  def StopNodes(nodes: Vector[Host]) =
    Vector(Squelch(true, nodes), Remote(Vector(Proc.StopNode), nodes))

  def RestartNode(node: Host, useHttps: Boolean = false) =
    RestartNodes(Vector(node), useHttps)

  def RestartNodes(nodes: Vector[Host], useHttps: Boolean = false) =
    Vector(
      Squelch(true, nodes),
      // Super-simplistic load balancer: pause for 30sec to drain connections
      Pause(30.seconds),
      Remote(Vector(Proc.RestartNode(useHttps)), nodes),
      Squelch(false, nodes)
    )

  def HangCores(nodes: Vector[CoreNode], duration: Duration) = {
    val hosts = nodes map { _.host }
    Vector(
      Squelch(true, hosts),
      Pause(duration),
      Remote(Proc.SignalFauna(19 /* SIGSTOP */ ), hosts)
    )
  }

  def KillCores(nodes: Vector[CoreNode]) = {
    val hosts = nodes map { _.host }
    Vector(Squelch(true, hosts), Remote(Proc.SignalFauna(9 /* SIGKILL */ ), hosts))
  }

  def Skew(delay: Duration) =
    Vector(apply(Time.DisableNTP), apply(Time.AdjustTime(delay.toMillis)))

  def Slow(delay: Duration, jitter: Duration) =
    apply(Network.PacketDelay(delay.toMillis, jitter.toMillis))

  def Flaky(percent: Int) =
    apply(Network.PacketLoss(percent))

  def Drop(ip: String) =
    apply(Network.DropByIP(ip))

  def Touch(file: Path) =
    apply(Etc.TouchFile(file))

  def EchoMsgs(hello: String) =
    apply(Etc.Echo(hello))

  def InitCluster(seed: CoreNode) =
    apply(Admin.InitCluster(seed.replica), seed.host)

  def ForceRemoveNode(target: Host, seed: Host) =
    Vector(
      Squelch(true, Vector(target)),
      Remote(
        Vector(Proc.StopNode, Admin.ForceRemoveNode(target.addr, seed.addr)),
        target
      )
    )

  def RemoveNode(target: Host, seed: Host) =
    Vector(
      Squelch(true, Vector(target)),
      Remote(Vector(Admin.RemoveNode(target.addr, seed.addr)), target)
    )

  def AddNode(target: CoreNode, seed: Host) =
    Vector(
      Remote(
        Vector(
          Cmd.CreateDirs,
          Proc.StartNode,
          Admin.JoinToCluster(seed.addr, target.replica)
        ),
        target.host
      ),
      Squelch(false, Vector(target.host))
    )

  def MoveNode(to: String) =
    apply(Admin.MoveNode(to))

  def UpdateReplica(rtype: String, replica: String*) =
    apply(Admin.UpdateReplica(rtype, replica.toVector))
}
