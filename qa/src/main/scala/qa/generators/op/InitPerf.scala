package fauna.qa.generators.op

import fauna.qa._

/** InitPerf initializes the cluster in a manner specific to
  *  the performance benchmark reports.
  */
class InitPerf(config: QAConfig) extends OpGenerator(config) {

  override def setup(nodes: Vector[CoreNode]): QASetup = {
    val newNodes = for {
      (dc, ns) <- nodes.groupBy { _.dc }.toVector
      (n, i) <- ns.zipWithIndex
    } yield n.copy(isActive = true)

    QASetup(buildCluster(newNodes), newNodes)
  }

  def ops(nodes: Vector[CoreNode]) = Vector.empty
}
