package fauna.qa.generators.op

import fauna.qa._
import fauna.qa.operator._
import scala.concurrent.duration._

/**
  * Triggers repair on a presumed-healthy cluster.
  */
class Repair(config: QAConfig) extends OpGenerator(config) {

  def ops(nodes: Vector[CoreNode]): Vector[Operation] = {
    val ops = Vector.newBuilder[Operation]

    ops += StartTraffic
    ops += Pause(5 minutes)

    val active = nodes filter { _.isActive }
    val coord = config.rand.shuffle(active).head

    ops += Annotate("Repair", Vector("Coordinator" -> coord.host.toString))
    ops += Remote(Cmd.Admin.Repair, coord.host)
    ops += Remote(Cmd.Proc.WaitForRepair, coord.host)

    ops += Pause(5.minutes)
    ops += StopTraffic

    ops.result()
  }
}

/**
  * Induces a replication fault by removing data from a single host,
  * then triggers repair.
  */
class DegradedRepair(config: QAConfig) extends OpGenerator(config) {

  def ops(nodes: Vector[CoreNode]): Vector[Operation] = {
    val ops = Vector.newBuilder[Operation]
    val active = nodes filter { _.isActive } toSet
    val target = config.rand.shuffle(active).head
    val coord = config.rand.shuffle(active).head
    val dir = config.getString("fauna.db.data-path")

    ops += Annotate("Stop Node", Vector("Target" -> target.toString))
    ops ++= Remote.StopNode(target.host)
    ops += Remote(
      Cmd.Raw(s"find $dir -name 'FAUNA-Versions*' -type f -delete"),
      target.host
    )
    ops ++= Remote.StartNode(target.host)
    ops += Pause(5.minutes)

    ops += Annotate("Repair", Vector("Coordinator" -> coord.host.toString))
    ops += Remote(Cmd.Admin.Repair, coord.host)
    ops += Remote(Cmd.Proc.WaitForRepair, coord.host)

    ops.result()
  }
}
