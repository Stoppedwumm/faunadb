package fauna.qa.generators.op

import fauna.lang.TimeBound
import fauna.qa._
import fauna.qa.operator._
import scala.concurrent.duration._

class KeepRolling(config: QAConfig) extends OpGenerator(config) {

  def ops(nodes: Vector[CoreNode]): Vector[Operation] = {
    val ops = Vector.newBuilder[Operation]
    val active = nodes filter { _.isActive }

    ops += Repeat(
      active flatMap { node =>
        Remote.RestartNode(node.host, config.useHttps) :+ Pause(3.minute)
      },
      TimeBound.Max // practically forever
    )

    ops.result()
  }
}
