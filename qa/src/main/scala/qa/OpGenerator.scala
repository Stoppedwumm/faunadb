package fauna.qa

import fauna.qa.operator._
import scala.util.Try

/**
  * OpGenerator produces a series of operations to take on the cluster. There are two core phases: a setup phase
  * that should prepare the cluster for initial data. The second phase decides when to start traffic against the
  * cluster and starts producing operations against it. When all operations have completed the test is considered
  * done and the system will exit.
  */
abstract class OpGenerator(config: QAConfig) {

  /** ops is the sequence of Operations to run when the OperationRunner is in
    * "Running" state.  The operations it produces may depend on the how cluster was
    * initialized; hence, we are passed a ClusterState rather than relying on
    * the hostset in QAConfig.
    *
    * All OperationGenerators must implement this function. */
  def ops(nodes: Vector[CoreNode]): Vector[Operation]

  // Helper to configure a default topology (replica per DC, two log partitions)
  protected def defaultTopology(nodes: Vector[CoreNode]): Vector[CoreNode] =
    nodes map { _.copy(isActive = true) }

  // Helper to build a new cluster given a topology
  protected def buildCluster(nodes: Vector[CoreNode]): Vector[Operation] = {
    val v = Vector.newBuilder[Operation]

    // Shut down all Core processes, reset local data.
    v += Remote(Cmd.Proc.ClearNode, Hosts(nodes))

    val active = nodes filter { _.isActive }
    val seed = active find { _.role == CoreNode.Role.DataLog } getOrElse {
      throw new IllegalStateException("Topology has no data+log replica.")
    }

    // Rebuild the cluster.
    v += Remote(
      Vector(Cmd.CreateDirs, Cmd.Proc.StartNode, Cmd.Proc.WaitForAdmin),
      Hosts(active)
    )
    v += Remote.InitCluster(seed)

    // Join all non-seed nodes to the seed.
    active foreach { node =>
      if (node != seed) {
        v += Remote(Cmd.Admin.JoinToCluster(seed.addr, node.replica), node.host)
      }
    }

    // On the seed node, make all DCs replicas.
    val replicas = nodes filter { n => n.isActive && n.replica.nonEmpty }
    replicas.groupMap(_.role)(_.replica) foreach {
      case (role, replicas) =>
        v += Remote(Cmd.Admin.UpdateReplica(role.toString, replicas.distinct), seed.host)
    }

    // Once ping is available, the Tx pipeline is up
    v += Remote(
      Vector(
        Cmd.Proc.WaitForPing(config.useHttps),
        Cmd.Etc.Sleep(5),
        Cmd.Proc.WaitForTopology),
      Hosts(active)
    )

    val replicasStr = nodes.groupBy { _.replica }.collect {
      case (r, ns) =>
        val addrs = ns collect { case n if n.isActive => n.addr }
        s"$r: ${addrs.mkString("[", ", ", "]")}"
    }

    val nodesStr = nodes map { n =>
      val active = if (n.isActive) "active" else "inactive"
      s"${n.addr}:${n.port} dc:${n.dc} replica:${n.replica} $active"
    }

    v += Annotate(
      "Cluster Initialized",
      Vector(
        "Replicas" -> replicasStr.mkString("- ", "\n- ", ""),
        "Nodes" -> nodesStr.mkString("- ", "\n- ", "")
      )
    )

    v.result()
  }

  /**
    * Provide a list of Operations that will be used to initialize the cluster. A list of uninitialized nodes
    * will be provided. It's the responsibility of the implementor of this method to configure the blank
    * nodes.
    */
  def setup(nodes: Vector[CoreNode]): QASetup = {
    // By default set the replicas to the DC. Implementing tests should
    // change this based on their needs.
    val newNodes = defaultTopology(nodes)
    QASetup(buildCluster(newNodes), newNodes)
  }

  /** nRandom chooses `n` hosts. */
  def nRandom(nodes: Vector[CoreNode], n: Int): Vector[CoreNode] = {
    config.rand.shuffle(nodes).take(n)
  }

  /** Randomly chooses a host with `p` probability. */
  def randomly(nodes: Vector[CoreNode], p: Double): Vector[CoreNode] = {
    nodes filter { _ =>
      config.rand.nextDouble() <= p
    }
  }

  /** OnAll applies a collection of Remotes to all supplied hosts. */
  def onAll(ts: Vector[Remote], hosts: Vector[Host]): Vector[Remote] =
    for (t <- ts) yield t(hosts)
}

object OpGenerator {

  def byName(name: String, config: QAConfig) = {
    Try {
      val cls = Class.forName(s"fauna.qa.generators.op.$name")
      val ctor = cls.getDeclaredConstructor(classOf[QAConfig])
      ctor.newInstance(config).asInstanceOf[OpGenerator]
    }
  }
}
