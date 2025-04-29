package fauna.cluster.topology

import fauna.tx.transaction.PartitionerProvider

abstract class TopologyPartitionerProvider[K, R, W](topologies: Topologies) extends PartitionerProvider[R, W] {

  @volatile private[this] var _partitioner = makePartitioner(topologies.snapshot)

  def partitioner = {
    val cp = _partitioner
    val tstate = topologies.snapshot
    if (cp.version == tstate.version) {
      cp
    } else {
      val p = makePartitioner(tstate)
      _partitioner = p
      p
    }
  }

  def makePartitioner(tstate: TopologyState): TopologyPartitioner[K, R, W]
}
