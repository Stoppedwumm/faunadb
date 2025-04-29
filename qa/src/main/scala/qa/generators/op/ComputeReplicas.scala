package fauna.qa.generators.op

import fauna.qa._
import fauna.qa.operator._

/**
  * Split each replica in two: one compute only replica; and one data+log replica.
  * Data+log replicas are made inactive to prevent workloads hitting them.
  */
class ComputeReplicas(config: QAConfig) extends TimedRun(config) {

  override def defaultTopology(nodes: Vector[CoreNode]): Vector[CoreNode] =
    nodes groupBy { _.replica } flatMap {
      case (replica, nodes) =>
        val (head, tail) =
          nodes.splitAt((nodes.size / 2) + (nodes.size % 2))

        val compute =
          head map {
            _.copy(
              role = CoreNode.Role.Compute,
              replica = s"$replica-compute",
              isActive = true
            )
          }

        val datalog =
          tail map {
            _.copy(
              role = CoreNode.Role.DataLog,
              replica = s"$replica-datalog",
              isActive = false // prevent traffic
            )
          }

        compute ++ datalog
    } toVector

  // NB. Activate all nodes during cluster build so data+log nodes aren't ignored.
  override def buildCluster(nodes: Vector[CoreNode]): Vector[Operation] =
    super.buildCluster(nodes map { _.copy(isActive = true) })
}
