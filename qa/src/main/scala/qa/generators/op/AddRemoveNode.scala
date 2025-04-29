package fauna.qa.generators.op

import fauna.qa._
import fauna.qa.operator._

/**
  * Core-186  Remove a node from a replica
  *
  * Start a full cluster
  * Stop a node
  * Clear its data directory
  * Start the node
  * Remove the dead node and join the new node
  *
  * Final State:
  * Two Replicas
  *     replica-0   starts:  2 nodes             --> Ends:   2 nodes
  *     replica-1   starts:  N-3 data nodes      --> Ends:   N-3 nodes (one node replaced)
  */

class AddRemoveNode(config: QAConfig) extends OpGenerator(config) {

  val r0 = "replica-0"
  val r1 = "replica-1"

  override def defaultTopology(nodes: Vector[CoreNode]): Vector[CoreNode] = {
    val baseClusterSize = 2

    val newNodes = Vector.newBuilder[CoreNode]

    // setup r0
    for (i <- 0 until baseClusterSize) {
      newNodes += nodes(i).copy(isActive = true, replica = r0)
    }

    // setup r1
    for (i <- baseClusterSize until nodes.size - 1) {
      newNodes += nodes(i).copy(isActive = true, replica = r1)
    }
    val idle = nodes.last.copy(isActive = false, replica = r1)

    newNodes.result() :+ idle
  }

  def ops(nodes: Vector[CoreNode]): Vector[Operation] = {

    val ts = Vector.newBuilder[Operation]

    val (active, inactive) = nodes partition { _.isActive }
    val seed = active.head.host
    // find the first host in r1, or the first non-seed host.
    val t = active find { _.replica == r1 } match {
      case Some(n) => n.host
      case None    => active.drop(1).head.host
    }
    val a = inactive.head
    ts += Annotate("Remove Node", Vector("Remove" -> t.addr))
    ts ++= Remote.ForceRemoveNode(t, seed)
    ts += Remote(Cmd.Proc.WaitForTopology, seed)
    ts += Annotate("Add Node", Vector("Add" -> a.addr))
    ts ++= Remote.AddNode(a, seed)
    ts += Remote(Cmd.Proc.WaitForTopology, seed)

    ts.result()
  }
}
