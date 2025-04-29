package fauna.cluster.test

import fauna.atoms._
import fauna.cluster.Membership
import fauna.cluster.topology._
import fauna.cluster.workerid.WorkerIDs
import fauna.exec.FaunaExecutionContext.Implicits._
import fauna.exec.Timer
import fauna.lang.syntax._
import fauna.net.HostService
import fauna.net.util.FutureSequence
import java.util.concurrent.TimeUnit
import org.scalatest.time.SpanSugar
import scala.concurrent.duration.Duration
import scala.concurrent.{ Future, Promise }

class TopologyReconcilerSpec extends Spec with SpanSugar {
  type TEST_NODE = (Node, TestTopologyReconciler, TestStreamer)

  private def initalizeFirstNodeInCluster() =
    initializeFirstNode("dc1", Set.empty) { _.init() }

  private def initalizeFirstNodeInReplica(replicaName: String, seed: Node) =
    initializeFirstNode(replicaName, seed.membership.replicaNames) {
      _.join(seed.address)
    }

  private def initializeFirstNode(
    replicaName: String,
    existingReplicas: Set[String])(init: Membership => Future[Unit]) = {
    val (node, tr, st) = makeTestNode(replicaName)
    await(init(node.membership))
    node.awaitActive()
    val newReps = (existingReplicas + replicaName).toVector
    val oldReps = existingReplicas.toVector
    await(node.topologies.setReplicas(newReps, newReps, oldReps, oldReps))
    (node, tr, st)
  }

  private def initializeNextNodeInReplica(
    seed: Node,
    st: Streamer = AlwaysSucceedsStreamer) = {
    val (node, tr2, st2) = makeTestNode(seed.membership.replica)
    st2.delegate = st
    await(node.membership.join(seed.address))
    node.awaitActive()
    (node, tr2, st2)
  }

  // Points to split the ring in 4 segments
  val l0 = Location.MinValue
  val l1 = Location(Long.MinValue / 2)
  val l2 = Location(0)
  val l3 = Location(Long.MaxValue / 2)

  private def initializeCluster() = {
    val (node1, tr1, st1) = initalizeFirstNodeInCluster()
    // Initially, it's all unassigned (assigned to null host ID)
    node1.topologies.getCurrentCovering(Segment.All) should equal(
      Seq(OwnedSegment(Segment.All, HostID.NullID)))
    // Equivalently, this needs to also show in segment availability
    node1.topologies.getUnclaimedParts(Segment.All) should equal(Seq(Segment.All))

    splitRing(
      Seq(
        SegmentOwnership(l0, node1.id),
        SegmentOwnership(l1, node1.id),
        SegmentOwnership(l2, node1.id),
        SegmentOwnership(l3, node1.id)
      ),
      Seq(
        OwnedSegment(Segment.All, node1.id)
      ),
      Seq(
        node1
      )
    )

    eventually {
      node1.topologies.getUnclaimedParts(Segment.All) should equal(Seq.empty)
    }

    // Nothing was streamed
    st1.streamed should equal(Seq.empty)

    (node1, tr1, st1)
  }

  private def splitRing(
    pending: Seq[SegmentOwnership],
    current: Seq[OwnedSegment],
    nodes: Seq[Node]) = {
    proposeTopology(nodes.head, pending)
    ringEventuallyStabilizesAs(current, nodes)
  }

  private def proposeTopology(node: Node, pending: Seq[SegmentOwnership]) = {
    val t = node.topologies
    val replica = node.membership.replica
    val prevPending = t.snapshot.replicaTopologies.collectFirst {
      case (r, ReplicaTopology(_, p, _)) if r == replica => p
    }.get
    await(t.proposeTopology(replica, pending, prevPending, Vector.empty))
  }

  private def ringEventuallyStabilizesAs(
    current: Seq[OwnedSegment],
    nodes: Seq[Node]) =
    eventually(timeout(scaled(20 seconds)), interval(scaled(1 second))) {
      // Eventually, they get reassigned.
      nodes foreach { n =>
        val t = n.topologies
        t.getCurrentCovering(Segment.All) should equal(current)
        t.isStable(n.membership.replica) should equal(Some(true))
      }
    }

  def claimRing(node: Node) =
    splitRing(Seq(SegmentOwnership(l0, node.id)),
              Seq(OwnedSegment(Segment.All, node.id)),
              Seq(node))

  private def splitRingInFour(node1: Node, node2: Node) =
    splitRing(
      Seq(
        SegmentOwnership(l0, node1.id),
        SegmentOwnership(l1, node2.id),
        SegmentOwnership(l2, node1.id),
        SegmentOwnership(l3, node2.id)
      ),
      Seq(
        OwnedSegment(Segment(l0, l1), node1.id),
        OwnedSegment(Segment(l1, l2), node2.id),
        OwnedSegment(Segment(l2, l3), node1.id),
        OwnedSegment(Segment(l3, l0), node2.id)
      ),
      Seq(
        node1,
        node2
      )
    )

  // Additional points to split the ring in 6 segments
  val l4 = Location(Long.MinValue / 3 * 2)
  val l5 = Location(Long.MinValue / 3)
  val l6 = Location(Long.MaxValue / 3)
  val l7 = Location(Long.MaxValue / 3 * 2)

  private def splitRingInSix(node1: Node, node2: Node, node3: Node) =
    splitRing(
      Seq(
        SegmentOwnership(l0, node1.id),
        SegmentOwnership(l4, node3.id),
        SegmentOwnership(l5, node2.id),
        SegmentOwnership(l2, node1.id),
        SegmentOwnership(l6, node3.id),
        SegmentOwnership(l7, node2.id)
      ),
      Seq(
        OwnedSegment(Segment(l0, l4), node1.id),
        OwnedSegment(Segment(l4, l5), node3.id),
        OwnedSegment(Segment(l5, l2), node2.id),
        OwnedSegment(Segment(l2, l6), node1.id),
        OwnedSegment(Segment(l6, l7), node3.id),
        OwnedSegment(Segment(l7, l0), node2.id)
      ),
      Seq(
        node1,
        node2,
        node3
      )
    )

  "TopologyReconciler" - {
    "in a newly initialized cluster" - {
      "first host should be able to claim the entire ring" in {
        val (node1, _, _) = initializeCluster()
        Node.stopAll(node1)
      }

      "a second host should be able to take some tokens over from the first one" in {
        // initialize node1 and take over the whole token range
        val (node1, _, st1) = initializeCluster()
        // Next, initialize node2
        val (node2, _, st2) = initializeNextNodeInReplica(node1)

        splitRingInFour(node1, node2)

        // Node 2 should've streamed its two segments from node1
        st2.streamed should equal(
          Seq(Segment(l1, l2), Segment(l3, l0)))

        // Node 1 shouldn't have streamed anything
        st1.streamed.isEmpty should equal(true)

        Node.stopAll(node2, node1)
      }

      "two hosts starting up at once should be able to claim the ring concurrently without streaming" in {
        val (node1, _, st1) = initalizeFirstNodeInCluster()
        val (node2, _, st2) = initializeNextNodeInReplica(node1)

        splitRingInFour(node1, node2)

        // Neither node should've streamed anything
        Seq(st1, st2) foreach { _.streamed.isEmpty should equal(true) }

        Node.stopAll(node2, node1)
      }

      "third host should be able to stream fragmented segments from first two hosts" in {
        val (node1, _, st1) = initalizeFirstNodeInCluster()
        val (node2, _, st2) = initializeNextNodeInReplica(node1)
        splitRingInFour(node1, node2)
        val (node3, _, st3) = initializeNextNodeInReplica(node1)

        // Propose a new pending ring. Make it so that Host 3 takes
        // over segments that overlap both Host1 and Host2 segments
        splitRingInSix(node1, node2, node3)

        // Nodes 1 and 2 should not have streamed anything
        Seq(st1, st2) foreach { _.streamed.isEmpty should equal(true) }

        // Node 3 streamed from both node1 and node2
        st3.streamed should equal(
          Seq(
            Segment(l4, l1),
            Segment(l1, l5),
            Segment(l6, l3),
            Segment(l3, l7)
          ))

        Node.stopAll(node3, node2, node1)
      }

      "should be able to reclaim whole replica from a removed node in a single pass" in {
        val (node1, _, st1) = initalizeFirstNodeInCluster()
        val (node2, _, _) = initializeNextNodeInReplica(node1)

        val r =
          new Repartitioner(node1.membership, node1.workerIDs, node1.topologies)
        r.start()

        def t = node1.topologies.snapshot

        eventually(timeout(scaled(20 seconds)), interval(scaled(1 second))) {
          if (!t.hasWholeReplicaAmong(Set("dc1"), Set(node1.id, node2.id))) {
            fail(s"dc1 is not whole: $t")
          }
        }
        t.hasWholeReplicaAmong(Set("dc1"), Set(node1.id)) should equal(false)

        val (node3, _, _) = initalizeFirstNodeInReplica("dc2", node1)
        eventually(timeout(scaled(20 seconds)), interval(scaled(1 second))) {
          t.hasWholeReplicaAmong(Set("dc2"), Set(node3.id)) should equal(true)
        }

        val ownedByNode2 =
          OwnedSegment.segmentsForHost(t.getTopology("dc1").get.currentSegments, node2.id)

        node2.stop()
        await(node1.removeNode(node2.id, force = false))

        eventually(timeout(scaled(20 seconds)), interval(scaled(1 second))) {
          t.hasWholeReplicaAmong(Set("dc1"), Set(node1.id)) should equal(true)
        }

        st1.streamed should equal(ownedByNode2)

        r.stop()
        Node.stopAll(node3, node1)
      }

      "should be able to add and remove nodes at the same time" in {
        val (node1, _, st1) = initalizeFirstNodeInCluster()
        val (node2, _, _) = initializeNextNodeInReplica(node1)
        val (node3, _, _) = initalizeFirstNodeInReplica("dc2", node1)
        val (node4, _, st4) = initializeNextNodeInReplica(node1)

        val r =
          new Repartitioner(node1.membership, node1.workerIDs, node1.topologies)
        r.start()

        def t = node1.topologies.snapshot

        eventually(timeout(scaled(20 seconds)), interval(scaled(1 second))) {
          t.hasWholeReplicaAmong(Set("dc1"), Set(node1.id, node2.id, node4.id)) should
            equal(true)
        }
        eventually(timeout(scaled(20 seconds)), interval(scaled(1 second))) {
          t.hasWholeReplicaAmong(Set("dc2"), Set(node3.id)) should equal(true)
        }

        val slow = new SlowStreamer(25.millis)
        st1.delegate = slow
        st4.delegate = slow
        val (node5, _, _) = initializeNextNodeInReplica(node1, slow)

        node1.removeNode(node2.id, force = false)

        val (node6, _, _) = initializeNextNodeInReplica(node1, slow)

        val (node7, _, _) = initializeNextNodeInReplica(node1, slow)

        node1.removeNode(node6.id, force = false)

        val (node8, _, _) = initializeNextNodeInReplica(node1, slow)

        node1.removeNode(node7.id, force = false)

        eventually(timeout(scaled(30 seconds)), interval(scaled(1 second))) {
          t.hasWholeReplicaAmong(
            Set("dc1"),
            Set(node1.id, node4.id, node5.id, node8.id)) should equal(true)
        }

        r.stop()
        Node.stopAll(node8, node7, node6, node5, node4, node3, node2, node1)
      }

      "should be able to add and remove replicas" in {
        val (node1, _, _) = initalizeFirstNodeInCluster()
        val (node2, _, _) = initializeNextNodeInReplica(node1)
        val (node3, _, _) = initalizeFirstNodeInReplica("dc2", node1)

        val r =
          new Repartitioner(node1.membership, node1.workerIDs, node1.topologies)
        r.start()

        def t = node1.topologies.snapshot

        eventually(timeout(scaled(20 seconds)), interval(scaled(1 second))) {
          t.hasWholeReplicaAmong(Set("dc1"), Set(node1.id, node2.id)) should
            equal(true)
        }
        eventually(timeout(scaled(20 seconds)), interval(scaled(1 second))) {
          t.hasWholeReplicaAmong(Set("dc2"), Set(node3.id)) should equal(true)
        }

        // Remove replica dc2
        await(node1.topologies.setReplicas(Seq("dc1"), Seq("dc1"), Seq("dc1", "dc2"), Seq("dc1", "dc2")))

        eventually(timeout(scaled(20 seconds)), interval(scaled(1 second))) {
          t.hasWholeReplicaAmong(Set("dc1"), Set(node1.id, node2.id)) should
            equal(true)
        }

        // Add replica dc2
        await(node1.topologies.setReplicas(Seq("dc1", "dc2"), Seq("dc1", "dc2"), Seq("dc1"), Seq("dc1")))

        eventually(timeout(scaled(20 seconds)), interval(scaled(1 second))) {
          t.hasWholeReplicaAmong(Set("dc1"), Set(node1.id, node2.id)) should
            equal(true)
        }
        eventually(timeout(scaled(20 seconds)), interval(scaled(1 second))) {
          t.hasWholeReplicaAmong(Set("dc2"), Set(node3.id)) should equal(true)
        }

        r.stop()
        Node.stopAll(node3, node2, node1)
      }

      "should be able to shrink a single-replica cluster" in {
        val (node1, _, st1) = initalizeFirstNodeInCluster()
        val (node2, _, st2) = initializeNextNodeInReplica(node1)
        val (node3, _, st3) = initializeNextNodeInReplica(node1)

        splitRingInSix(node1, node2, node3)

        def t = node1.topologies.snapshot

        // At this point, node3 owns some tokens
        t.hasWholeReplicaAmong(Set("dc1"), Set(node1.id, node2.id)) should
          equal(false)

        // remove but do not stop node3. Other nodes should be able to stream
        // from it as long as it's not stopped.
        await(node1.membership.remove(node3.id, force = false))

        splitRingInFour(node1, node2)

        t.hasWholeReplicaAmong(Set("dc1"), Set(node1.id, node2.id)) should
          equal(true)

        st1.streamed should equal(
          Seq(
            Segment(l4, l1),
            Segment(l6, l3),
          ))

        st2.streamed should equal(
          Seq(
            Segment(l1, l5),
            Segment(l3, l7),
          ))

        // Node 3 didn't stream anything, it was just a source
        st3.streamed should equal(Seq.empty)

        Node.stopAll(node3, node2, node1)
      }
    }

    "in two replicas" - {
      "streams there and back again" in {
        // DC1
        val (node1, _, st1) = initalizeFirstNodeInCluster()
        val (node2, _, st2) = initializeNextNodeInReplica(node1)
        val (node3, _, st3) = initializeNextNodeInReplica(node1)
        splitRingInSix(node1, node2, node3)

        // DC2
        val (node4, _, st4) = initalizeFirstNodeInReplica("dc2", node1)
        val (node5, _, st5) = initializeNextNodeInReplica(node4)
        splitRingInFour(node4, node5)

        // Nodes 1, 2, and 3 should not have streamed anything
        Seq(st1, st2, st3) foreach { _.streamed.isEmpty should equal(true) }

        // Nodes 4 and 5 should have streamed data from DC1
        st4.streamed should equal(
          Seq(
            Segment(l0, l4),
            Segment(l4, l1),
            Segment(l2, l6),
            Segment(l6, l3),
          ))

        st5.streamed should equal(
          Seq(
            Segment(l1, l5),
            Segment(l5, l2),
            Segment(l3, l7),
            Segment(l7, l0),
          ))

        // Reset streamed data
        st4.streamed = Seq.empty
        st5.streamed = Seq.empty

        // let's remove node3 and have DC1 nodes re-stream it from DC2. If we
        // didn't remove node3, it'd register as live, so DC1 hosts would just
        // stream from it. NOTE we didn't have to actually stop node3, our
        // mock streaming doesn't depend on it...
        await(node1.membership.remove(node3.id, force = false))

        Seq(node1, node2) foreach { _.awaitInactive(node3) }
        eventually(timeout(scaled(20 seconds)), interval(scaled(1 second))) {
          // Wait until both nodes saw it go
          node1.membership.hosts.contains(node3.id) should equal(false)
          node2.membership.hosts.contains(node3.id) should equal(false)
        }
        splitRingInFour(node1, node2)

        // They shouldn't be streaming from node3 anymore but rather from DC2 nodes
        st1.streamed should equal(
          Seq(
            Segment(l4, l1),
            Segment(l6, l3)
          ))

        st2.streamed should equal(
          Seq(
            Segment(l1, l5),
            Segment(l3, l7)
          ))

        // Nodes 4 and 5 should not have streamed anything
        Seq(st4, st5) foreach { _.streamed.isEmpty should equal(true) }

        Node.stopAll(node5, node4, node3, node2, node1)
      }

      "reacts to changed partition mid-streaming" in {
        // DC1
        val (node1, _, st1) = initalizeFirstNodeInCluster()
        val (node2, _, st2) = initializeNextNodeInReplica(node1)
        splitRingInFour(node1, node2)

        // DC2
        val (node3, _, st3) = initalizeFirstNodeInReplica("dc2", node1)
        val (node4, _, st4) = initializeNextNodeInReplica(node3)
        val (node5, _, st5) = initializeNextNodeInReplica(node3)

        val bst3 = new BlockOnSecondSegmentStreamer
        st3.delegate = bst3
        val bst4 = new BlockOnSecondSegmentStreamer
        st4.delegate = bst4
        val bst5 = new BlockOnSecondSegmentStreamer
        st5.delegate = bst5

        proposeTopology(node3,
                        Seq(
                          SegmentOwnership(l0, node3.id),
                          SegmentOwnership(l5, node4.id),
                          SegmentOwnership(l6, node5.id)
                        ))

        // Wait until all streamers hit the 2nd segment and block
        await(Future.sequence(Seq(bst3, bst4, bst5) map { _.blocked.future }))

        eventually(timeout(scaled(20 seconds)), interval(scaled(1 second))) {
          Seq(node3, node4, node5) foreach { n =>
            val t = n.topologies
            // The nodes only streamed one segment each
            t.getCurrentCovering(Segment.All) should equal(Seq(
              OwnedSegment(Segment(l0, l1), node3.id),
              OwnedSegment(Segment(l1, l5), HostID.NullID),
              OwnedSegment(Segment(l5, l2), node4.id),
              OwnedSegment(Segment(l2, l6), HostID.NullID),
              OwnedSegment(Segment(l6, l3), node5.id),
              OwnedSegment(Segment(l3, l0), HostID.NullID)
            ))
            // not stable as half the segments are missing
            t.isStable(n.membership.replica) should equal(Some(false))
          }
        }

        // Propose different topology now
        val newTopo = Seq(
          SegmentOwnership(l0, node4.id),
          SegmentOwnership(l5, node5.id),
          SegmentOwnership(l6, node3.id)
        )
        proposeTopology(node3, newTopo)

        // Ensure all nodes observed the new topology
        eventually {
          Seq(node3, node4, node5) foreach { n =>
            n.topologies.snapshot
              .getTopology(n.membership.replica) map { _.pending } should equal(
              Some(newTopo))
          }
        }

        // Release the streamers; make each stream fail; we should observe they weren't
        // re-enqueued despite this, as the pending ring changed.
        Seq(bst3, bst4, bst5) foreach { _.continue.success(false) }

        // The nodes completely claimed the new ranges
        ringEventuallyStabilizesAs(Seq(
                                     OwnedSegment(Segment(l0, l5), node4.id),
                                     OwnedSegment(Segment(l5, l6), node5.id),
                                     OwnedSegment(Segment(l6, l0), node3.id)
                                   ),
                                   Seq(node3, node4, node5))

        // Meanwhile, nothing happened in DC1
        Seq(st1, st2) foreach { _.streamed.isEmpty should equal(true) }

        val segs0 = Seq(
          Segment(l0, l1),
          Segment(l1, l5)
        )
        val segs1 = Seq(
          Segment(l5, l2),
          Segment(l2, l6)
        )
        val segs2 = Seq(
          Segment(l6, l3),
          Segment(l3, l0)
        )

        st3.streamed should equal(segs0 ++ segs2)
        st4.streamed should equal(segs1 ++ segs0)
        st5.streamed should equal(segs2 ++ segs1)

        Node.stopAll(node5, node4, node3, node2, node1)
      }
    }

    "should keep retrying streaming on failure" in {
      val (node1, _, _) = initalizeFirstNodeInCluster()
      claimRing(node1)
      val (node2, _, _) = initalizeFirstNodeInReplica("dc2", node1)
      claimRing(node2)
      val n3 = initalizeFirstNodeInReplica("dc3", node1)
      val (node3, tr3, st3) = n3

      st3.delegate = new SuccessPatternStreamer(
        Seq(false /*s1dc1*/, false /*s1dc2*/, false /*s2dc1*/, false /*s2dc2*/,
          true /*s1dc1*/, false /*s2dc1*/, false /*s2dc2*/, false /*s2dc1*/,
          true /*s2dc2*/ ))

      splitRing(Seq(SegmentOwnership(l0, node3.id), SegmentOwnership(l2, node3.id)),
                Seq(OwnedSegment(Segment.All, node3.id)),
                Seq(node3))
      val seg1 = Segment(l0, l2)
      val seg2 = Segment(l2, l0)
      val attempts = Seq(
        seg1, // try  first segment, DC1
        seg1, // try DC2
        seg2, // try second segment, DC1
        seg2, // try DC2
        // first round of retries
        seg1, // retry  first segment, DC1 (success)
        seg2, // retry second segment, DC1
        seg2, // retry second segment, DC2
        // second round of retries
        seg2, // retry second segment again, DC1
        seg2, // retry second segment, DC2 (success)
      )
      st3.streamed should equal(attempts)

      tr3.retryCyclesDetected should equal(3)

      Node.stopAll(node3, node2, node1)
    }

    "should keep retrying streaming when nodes are down" in {
      val (node1, _, _) = initalizeFirstNodeInCluster()
      claimRing(node1)
      val (node2, _, _) = initalizeFirstNodeInReplica("dc2", node1)
      claimRing(node2)
      val n3 = initalizeFirstNodeInReplica("dc3", node1)
      val (node3, tr3, st3) = n3

      tr3.setHostService(
        new LivenessPatternHostService(
          Seq(
            // seg 1, attempt 1
            false /*dc1*/, false /*dc2*/, // seg 2, attempt 1
            false /*dc1*/, false /*dc2*/, // seg 1, attempt 2
            true /*dc1*/, false /*dc2*/, // seg 2, attempt 2
            false /*dc1*/, false /*dc2*/, // seg 2, attempt 3
            false /*dc1*/, true /*dc2*/
          )))

      st3.delegate = new SuccessPatternStreamer(
        Seq(false /*s1dc1a1*/, false /*s1dc2*/, false /*s2dc1*/, false /*s2dc2*/,
          true /*s1dc1*/, false /*s2dc1*/, false /*s2dc2*/, true /*s2dc2*/ ))

      splitRing(Seq(SegmentOwnership(l0, node3.id), SegmentOwnership(l2, node3.id)),
                Seq(OwnedSegment(Segment.All, node3.id)),
                Seq(node3))
      val seg1 = Segment(l0, l2)
      val seg2 = Segment(l2, l0)
      val attempts = Seq(
        seg1, // try  first segment, DC1
        seg1, // try DC2
        seg2, // try second segment, DC1
        seg2, // try DC2
        // first round of retries
        seg1, // retry  first segment, DC1 (success)
        seg2, // retry second segment, DC1
        seg2, // retry second segment, DC2
        // second round of retries
        seg2 // retry second segment, DC2 (success)
      )
      st3.streamed should equal(attempts)

      tr3.retryCyclesDetected should equal(3)

      Node.stopAll(node3, node2, node1)
    }
  }

  class TestHostService(@volatile var delegate: HostService) extends HostService {
    def isLive(host: HostID) = delegate.isLive(host)
    def isLocal(host: HostID) = delegate.isLocal(host)
    def isNear(host: HostID) = delegate.isNear(host)
    def subscribeStartsAndRestarts(f: HostID => Unit) = delegate.subscribeStartsAndRestarts(f)
  }

  def makeTestNode(replica: String = "dc1"): TEST_NODE = {
    val node = Node.create(replica = replica)
    val streamer = new TestStreamer()
    val hostService = new TestHostService(new MembershipHostService(node.membership))
    val tr = new TestTopologyReconciler(node.topologies,
                                        node.membership,
                                        node.workerIDs,
                                        streamer,
                                        hostService)
    (node, tr, streamer)
  }

  class TestStreamer extends Streamer {
    @volatile var streamed = Seq.empty[Segment]
    @volatile var delegate: Streamer = AlwaysSucceedsStreamer

    def streamSegment(host: HostID, seg: Segment, version: Long) = {
      streamed = streamed :+ seg
      delegate.streamSegment(host, seg, version)
    }
  }

  object AlwaysSucceedsStreamer extends Streamer {
    def streamSegment(host: HostID, seg: Segment, version: Long) = FutureTrue
  }

  class SlowStreamer(delay: Duration) extends Streamer {

    def streamSegment(host: HostID, seg: Segment, version: Long) =
      Timer.Global.delay(delay)(FutureTrue)
  }

  class BlockOnSecondSegmentStreamer extends Streamer {
    @volatile var count = 0
    val blocked = Promise[Unit]()
    val continue = Promise[Boolean]()

    def streamSegment(host: HostID, seg: Segment, version: Long) = {
      count += 1
      if (count == 2) {
        blocked.success(())
        continue.future
      } else {
        FutureTrue
      }
    }
  }

  class SuccessPatternStreamer(@volatile var results: Seq[Boolean])
      extends Streamer {

    def streamSegment(host: HostID, seg: Segment, version: Long) = {
      val ret = results.headOption.getOrElse(true)
      results = results.drop(1)
      Future.successful(ret)
    }
  }

  class LivenessPatternHostService(@volatile var results: Seq[Boolean])
      extends HostService {

    def isLive(host: HostID) = {
      val ret = results.headOption.getOrElse(true)
      results = results.drop(1)
      ret
    }

    def isLocal(host: HostID) = false
    def isNear(host: HostID) = false
    def subscribeStartsAndRestarts(f: HostID => Unit): Unit = ()
  }

  /**
    * Emulates the production HostService in that it'll not consider removed
    * hosts to be live.
    */
  class MembershipHostService(membership: Membership) extends HostService {
    def isLive(host: HostID) = membership.hosts.contains(host)
    def isLocal(host: HostID) = membership.isLocal(host)
    def isNear(host: HostID) = false
    def subscribeStartsAndRestarts(f: HostID => Unit): Unit = ()
  }

  class TestTopologyReconciler(
    topologies: Topologies,
    membership: Membership,
    workerIDs: WorkerIDs,
    streamer: Streamer,
    hostService: TestHostService)
      extends TopologyReconciler(topologies,
                                 membership,
                                 workerIDs,
                                 streamer,
                                 hostService,
                                 FutureSequence()) {

    @volatile var retryCyclesDetected = 0

    def setHostService(hs: HostService): Unit =
      hostService.delegate = hs

    // stack the deck for deterministic test results
    override protected def shuffleSegments(rs: Seq[Segment]) = rs.sorted

    override def retryDelayOnRescheduledSegments = {
      retryCyclesDetected += 1
      Duration(0, TimeUnit.MILLISECONDS)
    }
  }
}
