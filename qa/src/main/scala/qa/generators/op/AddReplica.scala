package fauna.qa.generators.op

import fauna.qa._
import fauna.qa.operator._

/**
  *  Core-181  Simulate adding a replica to an existing cluster
  *
  *      Start a cluster
  *         - Take half of the nodes and setup a 1 x N cluster
  *      Start traffic against the cluster
  *      Configure a new node with a new replica name
  *      Start node and join it to the cluster
  *      Update replication to bring node in to the clusterA
  *
  * Final State:
  *  n/2 Clusters rounded up
  *     replica-0       starts:  1 node       --> Ends:   1 nodes
  *     replica-1       starts:  1 nodes      --> Ends:   1 nodes
  *     replica-N/2^    starts:  1 nodes      --> Ends:   1 node
  *     replica-N/2^+1  starts:  0 nodes      --> Ends:   1 nodes
  *     replica-N/2^+2  starts:  0 nodes      --> Ends:   1 nodes
  */
class AddReplica(config: QAConfig) extends OpGenerator(config) {

  override def defaultTopology(nodes: Vector[CoreNode]): Vector[CoreNode] = {
    val baseClusterSize = nodes.size / 2 + nodes.size % 2

    val newHosts = Vector.newBuilder[CoreNode]

    // Carve off the current nodes for the cluster
    // need to setup the new replica name
    for (i <- 0 until baseClusterSize) {
      newHosts += nodes(i).copy(isActive = true, replica = s"replica-$i")
    }

    // Carve off the nodes for future use, with same replicas as above
    for (i <- baseClusterSize until nodes.size) {
      newHosts += nodes(i).copy(replica = s"replica-$i")
    }

    newHosts.result()
  }

  def ops(nodes: Vector[CoreNode]): Vector[Operation] = {
    val ts = Vector.newBuilder[Operation]

    val (active, inactive) = nodes partition { _.isActive }
    val addNodes = 2 min inactive.size
    val seed = active.head.host
    val addList = inactive.take(addNodes)

    addList foreach { target =>
      ts += Annotate("Add Node", Vector("Target" -> target.toString))
      ts ++= Remote.AddNode(target, seed)
      ts += Annotate("Add Replica", Vector("Replica" -> target.replica))
      ts += Remote.UpdateReplica("data+log", target.replica)(seed)
      ts += Remote(Cmd.Proc.WaitForTopology, seed)
    }

    ts.result()
  }
}
