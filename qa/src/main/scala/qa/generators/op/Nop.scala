package fauna.qa.generators.op

import fauna.qa._
import fauna.qa.operator._
import scala.concurrent.duration._

/** A Nop does not mutate any part of the system. */
class Nop(config: QAConfig) extends OpGenerator(config) {
  val delay = 5 seconds

  val transactions = Vector(
    Remote.EchoMsgs("Hello, remote nodes!")
  )

  /* Nop is used for running unit tests, so let's override the default behaviour of tearing down and starting up
   * fresh Core instances by passing no Operations to run at the setup phase.
   */
  override def setup(nodes: Vector[CoreNode]): QASetup =
    QASetup(Vector.empty, nodes map { _.copy(isActive = true) })

  def ops(nodes: Vector[CoreNode]) =
    Pause(delay) +: onAll(transactions, Hosts(nodes))
}
