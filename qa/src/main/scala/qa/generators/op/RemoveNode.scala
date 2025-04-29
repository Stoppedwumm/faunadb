package fauna.qa.generators.op

import fauna.qa._
import fauna.qa.operator._
import scala.collection.mutable.{ Set => MSet }

/**
  * Core-184  Remove a node from a replica
  *
  * Start a full cluster with 2 replicas
  * Start traffic against the cluster
  * Stop traffic to a node
  * Remove the node from the cluster
  * Stop traffic to another node
  * Remove the node from the cluster
  *
  */

class RemoveNode(config: QAConfig) extends OpGenerator(config) {

  override def defaultTopology(nodes: Vector[CoreNode]): Vector[CoreNode] = {
    val baseClusterSize = nodes.size / 2

    val newNodes = Vector.newBuilder[CoreNode]

    for (i <- 0 until baseClusterSize) {
      newNodes += nodes(i).copy(isActive = true, replica = "replica-0")
    }

    for (i <- baseClusterSize until nodes.size) {
      newNodes += nodes(i).copy(isActive = true, replica = "replica-1")
    }

    newNodes.result()
  }

  def ops(nodes: Vector[CoreNode]): Vector[Operation] = {

    val ts = Vector.newBuilder[Operation]

    val active = nodes filter { _.isActive }
    val target = active.take(2)
    val seed = active.drop(2).head.host

    val removed = MSet.empty[CoreNode]

    // Prophylactic cleanup.
    ts += Remote(
      Vector(Cmd.Admin.CleanStorage, Cmd.Proc.WaitForCleanup),
      active map { _.host })

    target foreach { t =>
      ts += Annotate("Remove Node", Vector("Target" -> t.addr))
      ts ++= Remote.RemoveNode(t.host, seed)
      ts += Remote(Cmd.Proc.WaitForTopology, seed)
      ts ++= Remote.ForceRemoveNode(t.host, seed)

      removed += t
      val remaining = (active.toSet filterNot removed) map { _.host } toVector

      ts += Remote(
        Vector(Cmd.Admin.CleanStorage, Cmd.Proc.WaitForCleanup),
        remaining)
    }

    ts.result()
  }
}
