package fauna.cluster.test
import fauna.cluster.topology.{ SetReplicas, TopologyState }

class TopologyStateSpec extends Spec {
  val re = Vector.empty[String]
  val r13 = Vector("r1", "r3")
  val r123 = Vector("r1", "r2", "r3")
  val r1234 = Vector("r1", "r2", "r3", "r4")

  "initially added replicas are all log-and-data" in {
    TopologyState.empty
      .applyCmd(SetReplicas(r123, r123, re, re))
      .logReplicaNames should equal(r123)
  }


  "setting a replica to be non-log works" in {
    TopologyState.empty
      .applyCmd(SetReplicas(r123, r123, re, re))
      .applyCmd(SetReplicas(r123, r13, r123, r123))
      .logReplicaNames should equal(r13)
  }

  "setting a replica to be non-log and back works" in {
    TopologyState.empty
      .applyCmd(SetReplicas(r123, r123, re, re))
      .applyCmd(SetReplicas(r123, r13, r123, r123))
      .applyCmd(SetReplicas(r123, r123, r123, r13))
      .logReplicaNames should equal(r123)
  }

  "setting log state of a non-existent replica does nothing" in {
    TopologyState.empty
      .applyCmd(SetReplicas(r123, r123, re, re))
      .applyCmd(SetReplicas(r123, r1234, r123, r123))
      .logReplicaNames should equal(r123)
  }
}
