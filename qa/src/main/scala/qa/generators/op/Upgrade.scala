package fauna.qa.generators.op

import fauna.qa._
import fauna.qa.operator._

/**
  * Upgrade the cluster from one version to another with zero
  * downtime. For a more severe upgrade strategy see `HardUpgrade`.
  *
  * NB: Note that should we ever test a new version where storage changes would
  * necessitate waiting for the topology to re-stabilise, we should convince
  * ourselves that only waiting for ping to come up is sufficient.
  */
class Upgrade(config: QAConfig, rolling: Boolean) {
  val curVer = config.getString("upgrade.current-version")
  val nextVer = config.getString("upgrade.next-version")

  def setup(nodes: Vector[CoreNode], newNodes: Vector[CoreNode])(
    build: Vector[CoreNode] => Vector[Operation]
  ): QASetup = {
    val hosts = Hosts(nodes)

    val ts = Vector.newBuilder[Operation]
    ts += Remote(Cmd.RPM.Remove, hosts)
    ts += Annotate(
      "Install RPM",
      Vector("Version" -> curVer),
      Vector("version" -> curVer)
    )
    ts += Remote(Cmd.RPM.Install(curVer), hosts)
    ts ++= build(newNodes)

    QASetup(ts.result(), newNodes)
  }

  def ops(nodes: Vector[CoreNode]): Vector[Operation] = {
    val ts = Vector.newBuilder[Operation]

    if (!rolling) {
      nodes foreach { node =>
        val host = node.host
        ts += Annotate("Upgrade Storage", Vector("Host" -> host.addr))
        ts += Remote(Cmd.Admin.UpdateStorage, host)
      }
    }

    nodes foreach { node =>
      val host = node.host
      ts += Annotate(
        "Upgrade Node",
        Vector("Host" -> host.addr, "Version" -> nextVer),
        Vector("version" -> nextVer)
      )

      if (rolling) {
        ts += Remote(Cmd.Admin.UpdateStorage, host)
      }

      ts ++= Remote.StopNode(host)
      ts += Remote(Cmd.RPM.Install(nextVer), host)
      ts ++= Remote.StartNode(host)
    }

    ts.result()
  }
}

class RollingUpgrade(config: QAConfig) extends OpGenerator(config) {
  val underlying = new Upgrade(config, true)

  override def setup(nodes: Vector[CoreNode]): QASetup =
    underlying.setup(nodes, defaultTopology(nodes))(buildCluster)

  def ops(nodes: Vector[CoreNode]): Vector[Operation] =
    underlying.ops(nodes)
}

class EnMasseUpgrade(config: QAConfig) extends OpGenerator(config) {
  val underlying = new Upgrade(config, false)

  override def setup(nodes: Vector[CoreNode]): QASetup =
    underlying.setup(nodes, defaultTopology(nodes))(buildCluster)

  def ops(nodes: Vector[CoreNode]): Vector[Operation] =
    underlying.ops(nodes)
}
