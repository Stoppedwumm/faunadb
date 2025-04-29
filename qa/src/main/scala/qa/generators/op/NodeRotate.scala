package fauna.qa.generators.op

import fauna.qa._
import fauna.qa.operator._

/**
  * Drop a node from the cluster then add a new one
  */
class NodeRotate(config: QAConfig) extends OpGenerator(config) {

  def ops(nodes: Vector[CoreNode]): Vector[Operation] = {
    val ts = Vector.newBuilder[Operation]

    val Seq(seed, target) = config.rand.shuffle(nodes filter { _.isActive }).take(2)

    ts += Annotate("Remove Node", Vector("Target" -> target.toString))
    ts ++= Remote.ForceRemoveNode(target.host, seed.host)
    ts += Remote(Cmd.Proc.WaitForTopology, seed.host)
    ts += Annotate("Add Node", Vector("Target" -> target.toString))
    ts ++= Remote.AddNode(target, seed.host)
    ts += Remote(Cmd.Proc.WaitForTopology, seed.host)

    ts.result()
  }
}
