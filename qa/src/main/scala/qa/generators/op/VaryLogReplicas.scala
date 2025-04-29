package fauna.qa.generators.op

import fauna.lang.syntax._
import fauna.qa._
import fauna.qa.operator._

/**
  * Turn replicas from data+log to data and back
  */
class VaryLogReplicas(config: QAConfig) extends OpGenerator(config) {

  val pause =
    QAConfig.parseDuration(config.getString("vary-log-replicas.pause"))
  val deadline = config.testDuration.bound

  def ops(nodes: Vector[CoreNode]): Vector[Operation] = {
    val active = nodes filter { _.isActive }
    val replicas = active map { _.replica } distinct
    val seed = active.head

    def mkLogConfigs(rtype: String, exclude: String): Vector[Operation] =
      replicas flatMap { r =>
        if (r != exclude) {
          Remote.UpdateReplica(rtype, r)(seed.host) :: Pause(pause) :: Nil
        } else {
          Nil
        }
      }

    val logConfigs =
      replicas flatMap { r =>
        mkLogConfigs("data", r) ++ mkLogConfigs("data+log", r)
      }

    Vector.empty :+
      Remote(Cmd.Proc.WaitForTopology, seed.host) :+
      Repeat(logConfigs, deadline)
  }
}
