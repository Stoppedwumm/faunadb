package fauna.qa.generators.op

import fauna.qa._
import fauna.qa.operator._
import scala.concurrent.duration._

/** Run the cluster on a build for some period of time. Then upgrade a
  * single node and let it run. Then upgrade the rest of the cluster.
  */
class Canary(config: QAConfig) extends OpGenerator(config) {
  val curVer = config.getString("upgrade.current-version")
  val nextVer = config.getString("upgrade.next-version")

  override def setup(nodes: Vector[CoreNode]): QASetup = {
    val hosts = Hosts(nodes)
    val newNodes = defaultTopology(nodes)

    val ts = Vector.newBuilder[Operation]
    ts += Remote(Cmd.RPM.Remove, hosts)
    ts += Annotate("Install RPM", Vector("Version" -> curVer))
    ts += Remote(Cmd.RPM.Install(curVer), hosts)
    ts ++= buildCluster(newNodes)

    QASetup(ts.result(), newNodes)
  }

  def ops(nodes: Vector[CoreNode]): Vector[Operation] = {
    val ts = Vector.newBuilder[Operation]
    ts += StartTraffic
    ts += Pause(30 minutes)

    val canary = nodes.head
    val rest = nodes.tail

    def upgrade(node: CoreNode): Unit = {
      val host = node.host
      ts += Annotate(
        "Upgrade Node",
        Vector("Host" -> host.addr, "Version" -> nextVer)
      )

      ts += Remote(Cmd.Admin.UpdateStorage, host)

      ts ++= Remote.StopNode(host)
      ts += Remote(Cmd.RPM.Install(nextVer), host)
      ts ++= Remote.StartNode(host)
      ts += Pause(10 minutes)
    }

    upgrade(canary)

    ts += Pause(40 minutes)

    rest foreach { upgrade(_) }

    ts += Pause(30 minutes)
    ts += StopTraffic
    ts.result()
  }
}
