package fauna.qa.generators.op

import fauna.qa._
import fauna.qa.operator._

/**
  * Remove a partition across all replicas. Then remove a full replica.
  */
class ContractCluster(config: QAConfig) extends OpGenerator(config) {

  def ops(nodes: Vector[CoreNode]): Vector[Operation] = {
    val ops = Vector.newBuilder[Operation]

    val active = nodes filter { _.isActive }
    val byReplica = active groupBy { _.replica }
    val partitions = byReplica map { _._2.head } toSet
    val seed = (active filterNot partitions).head
    val replicaNodes = byReplica collectFirst {
      case (replica, repnodes) if replica != seed.replica =>
        repnodes filterNot partitions
    } get

    // Remove all nodes, except the last node in the replica to remove.
    (partitions ++ replicaNodes.dropRight(1)) foreach { target =>
      ops += Annotate("Remove Node", Vector("Target" -> target.addr))
      ops ++= Remote.ForceRemoveNode(target.host, seed.host)
      ops += Remote(Cmd.Proc.WaitForTopology, seed.host)
    }

    // To remove the last node in a replica, we need to remove the replica first
    val last = replicaNodes.last
    ops += Annotate("Set Replica to compute", Vector("Replica" -> last.replica))
    ops += Remote(
      Cmd.Admin.UpdateReplica("compute", Vector(last.replica)),
      seed.host
    )
    ops += Annotate("Remove Node", Vector("Target" -> last.addr))
    ops ++= Remote.ForceRemoveNode(last.host, seed.host)

    ops.result()
  }
}
