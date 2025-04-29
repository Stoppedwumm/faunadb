package fauna.qa.generators.op

import fauna.qa._
import fauna.qa.operator._
import scala.concurrent.duration._

/**
  * TimedRun will initialize the system and run traffic against the cluster for the
  * configured amount of time.
  */
abstract class TimedRun(config: QAConfig) extends OpGenerator(config) {

  def ops(nodes: Vector[CoreNode]): Vector[Operation] =
    // See Commander.scala, OpRunner.run - two Pause(1.minutes) are added to every OpGen,
    // so subtracting the 2.minutes here.
    Vector(Pause((config.testDuration - 2.minutes) max Duration.Zero))
}
