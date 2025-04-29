package fauna.cluster.test

import fauna.atoms._
import fauna.cluster.Membership
import fauna.cluster.topology._
import fauna.exec.FaunaExecutionContext.Implicits._
import fauna.exec.AsyncSemaphore
import fauna.lang.syntax._
import fauna.net.HostService
import fauna.net.util.FutureSequence
import org.scalactic.source
import org.scalatest.time.SpanSugar
import scala.concurrent.Future

class NodeRemovalSpec extends Spec with SpanSugar {
  private def initalizeFirstNodeInCluster() =
    initializeFirstNode( "r1", Set.empty) { _.init() }

  private def initalizeFirstNodeInReplica(replicaName: String, seed: Node) =
    initializeFirstNode(replicaName, seed.membership.replicaNames) {
      _.join(seed.address)
    }

  private def initializeFirstNode(
    replicaName: String,
    existingReplicas: Set[String])(init: Membership => Future[Unit]) = {
    val (node, t, st) = makeTestNode(replicaName)
    await(init(node.membership))
    node.awaitActive()
    val newReps = (existingReplicas + replicaName).toVector
    val oldReps = existingReplicas.toVector
    await(node.topologies.setReplicas(newReps, newReps, oldReps, oldReps))
    (node, t, st)
  }

  private def initializeNextNodeInReplica(seed: Node) = {
    val (node, t, st) = makeTestNode(seed.membership.replica)
    await(node.membership.join(seed.address))
    node.awaitActive()
    (node, t, st)
  }

  private def makeTestNode(replicaName: String) = {
    val node = Node.create(replica = replicaName)
    val (t, st) = makeTopologyReconciler(node)
    (node, t, st)
  }

  private class PausingStreamer extends Streamer {
    val semaphore = AsyncSemaphore(1)

    def pause(): Future[Int] = semaphore.acquire()
    def unpause(): Unit = semaphore.release()

    override def streamSegment(host: HostID, seg: Segment, topoVersion: Long) =
      pause() flatMap { _ =>
        unpause()
        FutureTrue
      }
  }

  private def makeTopologyReconciler(n: Node) = {
    val hostService = new HostService {
      override def isLive(host: HostID) = true
      override def isLocal(host: HostID) = n.membership.getReplica(host) contains n.membership.replica
      override def isNear(host: HostID) = false
      override def subscribeStartsAndRestarts(f: HostID => Unit): Unit = ???
    }
    val streamer = new PausingStreamer
    val t = new TopologyReconciler(n.topologies, n.membership, n.workerIDs, streamer, hostService, FutureSequence())
    (t, streamer)
  }

  private def makeRepartitioner(n: Node) = {
    // Use only few locations per host and a single proposal to make it go faster
    val r = new Repartitioner(n.membership, n.workerIDs, n.topologies)
    r.start()
    r
  }

  def giveItTime[T](fun: => T)(implicit pos: source.Position): T =
    eventually(timeout(scaled(5 seconds)), interval(scaled(1 second))) {
      fun
    }

  "NodeRemoval should" - {
    "allow removal of a node, but reject removal of last node in data replica" in {
      // Build a 2-replica cluster, with node1 in r1 and node2 and node3 in r2
      val (node1, _, _) = initalizeFirstNodeInCluster()
      val r = makeRepartitioner(node1) // one is enough for the whole cluster
      val (node2, _, st2) = initalizeFirstNodeInReplica("r2", node1)
      val (node3, _, _) = initializeNextNodeInReplica(node2)
      giveItTime {
        node1.topologies.isStable("r1") contains(true) should equal (true)
        node1.topologies.isStable("r2") contains(true) should equal (true)
      }
      // pause streaming on node2 so we don't race its topology reconciler
      await(st2.pause())
      await(node1.removeNode(node3.id, false))

      // Confirm node3 is leaving
      giveItTime {
        node1.membership.hostInformation(node3.id) forall { _.leaving } should equal (true)
        node1.topologies.isStable("r2") contains(false) should equal (true)
      }

      // Can't remove node2 as node3 removal is pending
      assertThrows[IllegalArgumentException] {
        await(node1.removeNode(node2.id, false))
      }

      // Can't even force-remove node2
      assertThrows[IllegalArgumentException] {
        await(node1.removeNode(node2.id, true))
      }

      // Allow node2 to proceed with reconciliation
      st2.unpause()

      // node2 eventually takes over ownership of node3 segments
      giveItTime {
        node1.topologies.isStable("r2") contains(true) should equal (true)
      }

      // Force-remove node3 now.
      await(node1.removeNode(node3.id, true))
      node1.membership.hostInformation(node3.id) should equal(None)

      // Still can't remove node2 as it is the only node in its data replica
      assertThrows[IllegalArgumentException] {
        await(node1.removeNode(node2.id, false))
      }

      // Still can't even force-remove it
      assertThrows[IllegalArgumentException] {
        await(node1.removeNode(node2.id, true))
      }

      // Switch the replica to compute
      await(node1.topologies.setReplicas(Vector("r1"), Vector("r1"), Vector("r1", "r2"), Vector("r1", "r2")))

      // r2 topology was discarded
      node1.topologies.isStable("r2") should equal (None)

      // Now we can remove the node
      await(node1.removeNode(node2.id, false))
      // It is leaving
      node1.membership.hostInformation(node2.id) forall { _.leaving } should equal(true)

      // Even if the node is still leaving we can't move the node back to data
      assertThrows[IllegalArgumentException] {
        await(node1.topologies.setReplicas(Vector("r1", "r2"), Vector("r1", "r2"), Vector("r1"), Vector("r1")))
      }

      // We can call idempotent removal during this time, it won't throw
      await(node1.removeNode(node2.id, false))

      // Confirm the node is still in "leaving" state (so above exception was
      // thrown because r2 only has leaving nodes, not because it is empty).
      node1.membership.hostInformation(node2.id) forall { _.leaving } should equal(true)

      // We can call idempotent removal during this time, it won't throw
      await(node1.removeNode(node2.id, false))

      // Force-remove it
      await(node1.removeNode(node2.id, true))
      node1.membership.hostInformation(node2.id) should equal(None)

      // Trying to switch r2 to data will fail again, this time because the
      // replica does not exist anymore.
      assertThrows[IllegalArgumentException] {
        await(node1.topologies.setReplicas(Vector("r1", "r2"), Vector("r1", "r2"), Vector("r1"), Vector("r1")))
      }

      r.stop()
      Node.stopAll(node1, node2, node3)
    }

    "allow forced removal of an already leaving node" in {
      // Build a 2-replica cluster, with node1 in r1 and node2 and node3 in r2
      val (node1, _, _) = initalizeFirstNodeInCluster()
      val r = makeRepartitioner(node1) // one is enough for the whole cluster
      val (node2, _, st2) = initalizeFirstNodeInReplica("r2", node1)
      val (node3, _, _) = initializeNextNodeInReplica(node2)
      giveItTime {
        node1.topologies.isStable("r1") contains(true) should equal (true)
        node1.topologies.isStable("r2") contains(true) should equal (true)
      }
      // pause streaming on node2 so we don't race its topology reconciler
      await(st2.pause())
      await(node1.removeNode(node3.id, false))

      // Confirm node3 is leaving
      giveItTime {
        node1.membership.hostInformation(node3.id) forall { _.leaving } should equal (true)
        node1.topologies.isStable("r2") contains(false) should equal (true)
      }

      // Now force-remove it
      await(node1.removeNode(node3.id, true))
      // By the time that method returns, it should be gone
      node1.membership.hostInformation(node3.id) should equal (None)

      // Topology should stabilize
      st2.unpause()
      giveItTime {
        node1.topologies.isStable("r2") contains(true) should equal (true)
      }

      r.stop()
      Node.stopAll(node1, node2, node3)
    }
  }
}
