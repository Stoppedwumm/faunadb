package fauna.qa.generators.op

import fauna.qa._
import fauna.qa.operator._
import scala.concurrent.duration._

/** skew() and strobe() are factory methods since they depend on multiple arguments,
  *  and making them abstract non-anonymous classes seemed a bit more awkward in
  *  Cockroach.scala.  Maybe we can revisit this.
  */
object ClockSkew {

  /** A ClockSkew adjusts the clock on all hosts by a fixed amount. */
  def skew(config: QAConfig, maxSkew: Duration) = new OpGenerator(config) {
    val inMillis = maxSkew.toMillis.toInt max 1

    def ops(nodes: Vector[CoreNode]): Vector[Operation] = {
      for {
        n <- nodes if n.isActive
        t <- Remote.Skew((config.rand.nextInt(inMillis) - (inMillis / 2)) millis)
      } yield t(n.host)
    }
  }

  def strobe(config: QAConfig, maxSkew: Duration, times: Int) =
    new OpGenerator(config) {
      val strobePause = 500 millis /*TODO: what does the cockroach test use? */

      def ops(nodes: Vector[CoreNode]): Vector[Operation] =
        (1 to times).toVector flatMap { _ =>
          Pause(strobePause) +: skew(config, maxSkew).ops(nodes)
        }
    }

}
