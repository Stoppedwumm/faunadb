package fauna.qa.generators.op

import fauna.qa._
import fauna.qa.operator._

/**
  * Downgrade the cluster from the one version to a previous version.
  */
class Downgrade(config: QAConfig) extends OpGenerator(config) {
  val curVer = config.getString("upgrade.current-version")
  val nextVer = config.getString("upgrade.next-version")

  override def setup(nodes: Vector[CoreNode]): QASetup = {
    val newNodes = defaultTopology(nodes)
    val hosts = Hosts(nodes)

    val ts = Vector.newBuilder[Operation]
    ts += Remote(Cmd.RPM.Remove, hosts)
    ts += Annotate("Install RPM", Vector("Version" -> nextVer))
    ts += Remote(Cmd.RPM.Install(nextVer), hosts)
    ts ++= buildCluster(newNodes)

    QASetup(ts.result(), newNodes)
  }

  def ops(nodes: Vector[CoreNode]): Vector[Operation] = {
    val ts = Vector.newBuilder[Operation]

    nodes foreach { node =>
      val host = node.host
      ts += Annotate(
        "Downgrade Node",
        Vector("Host" -> host.addr, "Version" -> curVer)
      )

      ts ++= Remote.StopNode(host)
      ts += Remote(Cmd.RPM.Downgrade(curVer), host)
      ts ++= Remote.StartNode(host)
    }

    ts.result()
  }
}
