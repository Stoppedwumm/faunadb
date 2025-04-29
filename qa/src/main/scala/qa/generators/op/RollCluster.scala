package fauna.qa.generators.op

import fauna.qa._
import fauna.qa.operator._
import scala.concurrent.duration._

/**
  * Restart the entire cluster one node at a time.
  * Restart the cluster an entire replica at a time.
  * Restart the entire cluster simultaneously.
  * Stop the entire cluster, then start the entire cluster simultaneously.
  */
class RollCluster(config: QAConfig) extends OpGenerator(config) {

  def ops(nodes: Vector[CoreNode]): Vector[Operation] = {
    val ops = Vector.newBuilder[Operation]
    val active = nodes filter { _.isActive }

    ops += Annotate("Rolling Restart", Vector.empty)

    config.rand.shuffle(active) foreach { node =>
      ops ++= Remote.RestartNode(node.host, config.useHttps)
    }

    ops += Pause(5.minutes)

    active groupBy { _.replica } foreach {
      case (replica, nodes) =>
        ops += Annotate(
          "Replica Restart",
          Vector("Replica" -> replica)
        )
        ops ++= Remote.RestartNodes(Hosts(config.rand.shuffle(nodes)), config.useHttps)
    }

    ops += Pause(5.minutes)

    ops += Annotate("Cluster Restart", Vector.empty)

    ops ++= Remote.RestartNodes(Hosts(active), config.useHttps)

    ops += Pause(5.minutes)

    ops += Annotate("Cluster Hard Restart", Vector.empty)

    ops ++= Remote.StopNodes(Hosts(active))
    ops ++= Remote.StartNodes(Hosts(active))

    ops.result()
  }
}
