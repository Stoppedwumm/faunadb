package fauna.qa.generators.op

import fauna.qa._
import fauna.qa.operator._
import scala.concurrent.duration._

/**
  *  Core-182  Simulate expanding a cluster replica by replica
  *
  *    Start a full cluster
  *    Start traffic against the cluster
  *    Start a new node in an existing replica
  *    Join the node to the cluster.
  *
  * Final State:
  *  n/2 Clusters rounded up
  *     replica-0       starts:  1 node       --> Ends:   2 nodes
  *     replica-1       starts:  1 nodes      --> Ends:   2 nodes
  *     replica-2 to N  starts:  1 nodes      --> Ends:   1 node
  */
class AddNode(config: QAConfig) extends OpGenerator(config) {

  override def defaultTopology(nodes: Vector[CoreNode]): Vector[CoreNode] = {
    val baseClusterSize = nodes.size / 2 + nodes.size % 2

    val newNodes = Vector.newBuilder[CoreNode]

    // Carve off the current nodes for the cluster
    // need to setup the new replica name
    for (i <- 0 until baseClusterSize) {
      newNodes += nodes(i).copy(isActive = true, replica = s"replica-$i")
    }

    // Carve off the nodes for future use, with same replicas as above
    for (i <- baseClusterSize until nodes.size) {
      newNodes += nodes(i)
        .copy(isActive = false, replica = s"replica-${i - baseClusterSize}")
    }

    newNodes.result()
  }

  def ops(nodes: Vector[CoreNode]): Vector[Operation] = {
    val ts = Vector.newBuilder[Operation]

    val (active, inactive) = nodes partition { _.isActive }
    val seed = active.head.host

    inactive.take(2) foreach { target =>
      ts += Annotate("Add Node", Vector("Target" -> target.host.toString))
      ts ++= Remote.AddNode(target, seed)
      ts += Pause(2 minutes)
    }
    ts += Remote(Cmd.Proc.WaitForTopology, seed)

    ts.result()
  }
}
