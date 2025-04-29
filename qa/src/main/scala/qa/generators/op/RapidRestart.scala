package fauna.qa.generators.op

import fauna.lang.syntax._
import fauna.qa._
import fauna.qa.operator._

/**
  * Repeatedly restarts a single, randomly-chosen process for the
  * duration of the test.
  */
class RapidRestart(config: QAConfig) extends OpGenerator(config) {

  def ops(nodes: Vector[CoreNode]): Vector[Operation] = {
    val ts = Vector.newBuilder[Operation]
    val hosts = Vector.newBuilder[Host]
    val deadline = config.testDuration.bound

    val victim = config.rand.shuffle(nodes).head
    ts += Annotate(
      "Rapid Restart",
      Vector(
        "Victim" -> victim.toString,
        "Deadline" -> deadline.duration.toCoarsest.toString
      )
    )

    hosts += victim.host
    ts += Repeat(Remote.RestartNodes(hosts.result(), config.useHttps), deadline)

    ts.result()
  }
}
