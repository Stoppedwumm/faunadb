package fauna.qa.generators.op

import fauna.qa._
import fauna.qa.operator._
import scala.concurrent.duration._

/**
  * Upgrade the cluster from one version to another all at once. For a
  * kinder, gentler upgrade strategy see `Upgrade`.
  *
  * NB: Note that should we ever test a new version where storage changes would
  * necessitate waiting for the topology to re-stabilise, we should convince
  * ourselves that only waiting for ping to come up is sufficient.
  */
class HardUpgrade(config: QAConfig) extends OpGenerator(config) {
  val curVer = config.getString("upgrade.current-version")
  val nextVer = config.getString("upgrade.next-version")

  override def setup(nodes: Vector[CoreNode]): QASetup = {
    val newNodes = defaultTopology(nodes)
    val hosts = Hosts(nodes)

    val ts = Vector.newBuilder[Operation]
    ts += Remote(Cmd.RPM.Remove, hosts)
    ts += Annotate(
      "Install RPM",
      Vector("Version" -> curVer),
      Vector("version" -> curVer)
    )
    ts += Remote(Cmd.RPM.Install(curVer), hosts)
    ts ++= buildCluster(newNodes)

    QASetup(ts.result(), newNodes)
  }

  def ops(nodes: Vector[CoreNode]): Vector[Operation] = {
    val hosts = Hosts(nodes)
    val ts = Vector.newBuilder[Operation]

    ts += Pause(10 minutes)
    ts += Annotate(
      "Hard Upgrade",
      Vector("Version" -> nextVer),
      Vector("version" -> nextVer)
    )

    nodes foreach { node =>
      ts += Remote(Cmd.RPM.Install(nextVer), node.host)
    }

    ts ++= Remote.RestartNodes(hosts, config.useHttps)
    ts.result()
  }
}
