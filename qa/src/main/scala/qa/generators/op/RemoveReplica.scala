package fauna.qa.generators.op

import fauna.qa._
import fauna.qa.operator._

/**
  * Remove a replica
  *
  * Start full cluster with a configurable number of replicas (default 3)
  * Start traffic against the cluster
  * Update replication to remove one replica
  * Stop nodes in the replica
  * Remove replica's nodes from the cluster
  */

class RemoveReplica(config: QAConfig) extends OpGenerator(config) {
  val repToRemove = "replica-1"

  val replicas =
    config.opt("remove-replica.replicas", _.toInt) getOrElse 3

  override def defaultTopology(hs: Vector[CoreNode]): Vector[CoreNode] = {
    val newNodes = Vector.newBuilder[CoreNode]
    for (i <- hs.indices) {
      val rep = s"replica-${i % replicas}"
      newNodes += hs(i).copy(isActive = true, replica = rep)
    }
    newNodes.result()
  }

  def ops(nodes: Vector[CoreNode]): Vector[Operation] = {

    val l = Vector.newBuilder[Operation]

    val seed = nodes.head.host
    val target = nodes.collect { case n if n.replica == repToRemove => n.host }

    l += Annotate("Remove Replica", Vector("Remove" -> repToRemove))
    l += Remote.UpdateReplica("compute", repToRemove)(seed)
    l += Remote(Cmd.Proc.WaitForTopology, seed)

    for (t <- target) {
      l += Annotate("Remove Node", Vector("Remove" -> t.addr))
      l ++= Remote.ForceRemoveNode(t, seed)
    }

    l += Remote(Cmd.Proc.WaitForTopology, seed)

    l.result()
  }
}
