package fauna.tx.test

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.net.HostService
import fauna.tx.transaction._
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._

object CoordinatorSpec {

  final case class TestHostService(
    local: Set[HostID],
    near: Set[HostID],
    dead: Set[HostID])
      extends HostService {
    def isLive(host: HostID) = !dead(host)
    def isLocal(host: HostID) = local(host)
    def isNear(host: HostID) = near(host)
    def subscribeStartsAndRestarts(f: HostID => Unit) = ()
  }

  final class TestPartitioner(writers: Set[HostID]) extends Partitioner[Int, Unit] {

    private var keysCovered = false // allow to mock result of txnCovered

    def mockTxnCovered[A](fn: => A): A = {
      keysCovered = true
      val result = fn
      keysCovered = false
      result
    }

    def hostsForWrite(write: Unit) = writers
    def txnCovered(hosts: Set[HostID], expr: Unit) = keysCovered

    // Unused API
    val version = 0L
    def replicas(seg: Segment) = ???
    def segments(host: HostID, pending: Boolean) = ???
    def segmentsInReplica(replica: String): Map[HostID, Vector[Segment]] = ???
    def primarySegments(host: HostID, pending: Boolean) = ???
    def isReplicaForSegment(seg: Segment, host: HostID) = ???
    def isReplicaForLocation(loc: Location, h: HostID, pending: Boolean) = ???
    def coversRead(read: Int) = ???
    def coversTxn(host: HostID, expr: Unit) = ???
    def hostsForRead(read: Int) = ???
  }

}

class CoordinatorSpec extends Spec {
  import Coordinator._
  import Coordinator.Outcome._
  import CoordinatorSpec._

  "Coordinator" - {

    "chooses and order log nodes" in {
      val hosts = Vector.fill(12) { HostID.randomID }

      // Topology:
      //            Seg 0  Seg 1   Seg 2   Seg 3
      // Replica A: host0, host1,  host2,  host3
      // Replica B: host4, host5,  host6,  host7
      // Replica C: host8, host9,  host10, host11
      val topology =
        Map(
          SegmentID(0) -> SegmentInfo(
            hosts = Vector(hosts(0), hosts(4), hosts(8)),
            state = Started(Epoch.MinValue),
            initRound = 0
          ),
          SegmentID(1) -> SegmentInfo(
            hosts = Vector(hosts(1), hosts(5), hosts(9)),
            state = Started(Epoch.MinValue),
            initRound = 0
          ),
          SegmentID(2) -> SegmentInfo(
            hosts = Vector(hosts(2), hosts(6), hosts(10)),
            state = Started(Epoch.MinValue),
            initRound = 0
          ),
          SegmentID(3) -> SegmentInfo(
            hosts = Vector(hosts(3), hosts(7), hosts(11)),
            state = Started(Epoch.MinValue),
            initRound = 0
          )
        )

      // Seg. 3 leader is unknown.
      val knownLeaders =
        TrieMap(
          SegmentID(0) -> hosts(0),
          SegmentID(1) -> hosts(5),
          SegmentID(2) -> hosts(10)
        )

      // Built from the perspective of host1. Presume neighbor replica B.
      val hostService =
        TestHostService(
          local = hosts.slice(0, 4).toSet,
          near = hosts.slice(4, 7).toSet,
          dead = Set(hosts(11))
        )

      val logNodes = preferredLogNodes(hosts(1), topology, knownLeaders, hostService)

      // Contain all but the dead nodes
      logNodes should contain.theSameElementsAs(hosts filter { _ != hosts(11) })

      // Start with hosts in segments where the leader is unknown
      logNodes.slice(0, 2) should contain.allOf(hosts(3), hosts(7))

      // Then prefer local leader
      logNodes(2) shouldBe hosts(0)

      // Then other known leaders, neighbor replica first
      logNodes.slice(3, 5) should contain.inOrder(hosts(5), hosts(10))

      // Then itself
      logNodes(5) shouldBe hosts(1)

      // Then other local hosts
      logNodes(6) shouldBe hosts(2)

      // Then hosts in a neighbor replica
      logNodes.slice(7, 9) should contain.allOf(hosts(4), hosts(6))

      // The remaning nodes
      logNodes.slice(9, 11) should contain.allOf(hosts(8), hosts(9))
    }

    "Applies" - {

      "data+log replicas" - {
        // Topology:
        // Replica A (data+log): host0, host1,  host2
        // Replica B (data+log): host3, host4,  host5
        //
        // Tests are written from the perspective of host0.
        val hosts = Vector.fill(6) { HostID.randomID }

        // Consider all replicas are data+log and all hosts are live
        val hostService =
          TestHostService(
            local = hosts.slice(0, 3).toSet,
            near = Set.empty,
            dead = Set.empty
          )

        // 2 hosts in each replica cover the writes
        val partitioner =
          new TestPartitioner(
            writers = Set(
              hosts(0),
              hosts(1),
              hosts(3),
              hosts(4)
            ))

        val txnTS = Timestamp.ofMicros(200)

        "computes transaction outcome when local replica is faster than remote" in {
          val applies =
            Applies[Unit, Int](partitioner, hostService, ())

          // Key got covered by the first host in the local replica
          applies.add(hosts(0), txnTS, 42).value shouldBe HasResult(txnTS, 42)

          partitioner.mockTxnCovered {
            // Key got covered by all nodes in the local replica
            applies.add(hosts(1), txnTS, 42).value shouldBe
              KeysCovered(txnTS, 42, allLocals = true)

            // New acks don't change the outcome
            applies.add(hosts(3), txnTS, 42) shouldBe empty

            // All writers have completed
            applies.add(hosts(4), txnTS, 42).value shouldBe AllNodes(txnTS, 42)
          }
        }

        "computes transaction outcome when remote replica is faster than local" in {
          val applies =
            Applies[Unit, Int](partitioner, hostService, ())

          // Key got covered by the first host in the remote replica first
          applies.add(hosts(3), txnTS, 42).value shouldBe HasResult(txnTS, 42)

          // A new ack from local replica don't change the outcome
          applies.add(hosts(0), txnTS, 42) shouldBe empty

          partitioner.mockTxnCovered {
            // Keys were covered by the remote replica
            applies.add(hosts(4), txnTS, 42).value shouldBe
              KeysCovered(txnTS, 42, allLocals = false)

            // All writers have completed
            applies.add(hosts(1), txnTS, 42).value shouldBe AllNodes(txnTS, 42)
          }
        }

        "don't wait for dead nodes in the local replica" in {
          val applies =
            Applies[Unit, Int](
              partitioner,
              hostService.copy(dead = Set(hosts(1))),
              ()
            )

          // Key got covered by the first host in the local replica
          applies.add(hosts(0), txnTS, 42).value shouldBe HasResult(txnTS, 42)

          partitioner.mockTxnCovered {
            // Key got covered by another host in a remote replica
            applies.add(hosts(4), txnTS, 42).value shouldBe
              KeysCovered(txnTS, 42, allLocals = true)

            // New acks don't change the outcome
            applies.add(hosts(3), txnTS, 42) shouldBe empty
          }
        }

        "detects TS mismatches" in {
          val applies =
            Applies[Unit, Int](
              partitioner,
              hostService.copy(dead = Set(hosts(1))),
              ()
            )

          val missmatchTS = txnTS + 10.millis

          // Key got covered by the first host in the local replica
          applies.add(hosts(0), txnTS, 42).value shouldBe HasResult(txnTS, 42)

          // Got a new ack with a different TS
          applies.add(hosts(4), missmatchTS, 42).value shouldBe
            MismatchedResults(
              Vector(
                (hosts(0), txnTS, 42),
                (hosts(4), missmatchTS, 42)
              )
            )

          // New acks don't change the outcome
          applies.add(hosts(3), txnTS, 42) shouldBe empty
        }

        "detects result mismatches" in {
          val applies =
            Applies[Unit, Int](
              partitioner,
              hostService.copy(dead = Set(hosts(1))),
              ()
            )

          // Key got covered by the first host in the local replica
          applies.add(hosts(0), txnTS, 42).value shouldBe HasResult(txnTS, 42)

          // Got a new ack with a different TS
          applies.add(hosts(4), txnTS, 8000).value shouldBe
            MismatchedResults(
              Vector(
                (hosts(0), txnTS, 42),
                (hosts(4), txnTS, 8000)
              )
            )

          // New acks don't change the outcome
          applies.add(hosts(3), txnTS, 42) shouldBe empty
        }
      }

      "data+log and compute replicas" - {
        // Topology:
        // Replica A (compute, neighbor of A): host0, host1,  host2
        // Replica B (data+log):               host3, host4,  host5
        // Replica C (data+log):               host6, host7,  host8
        //
        // Tests are written from the perspective of host0.
        val hosts = Vector.fill(9) { HostID.randomID }

        // Consider all replicas are data+log and all hosts are live
        val hostService =
          TestHostService(
            local = hosts.slice(0, 3).toSet,
            near = hosts.slice(3, 6).toSet,
            dead = Set.empty
          )

        // 2 hosts in each data+log replica cover the writes
        val partitioner =
          new TestPartitioner(
            writers = Set(
              hosts(3),
              hosts(4),
              hosts(6),
              hosts(7)
            ))

        val txnTS = Timestamp.ofMicros(200)

        "computes transaction outcome from neighbor replica" in {
          val applies =
            Applies[Unit, Int](partitioner, hostService, ())

          // Key got covered by the first host in the neighbor replica
          applies.add(hosts(3), txnTS, 42).value shouldBe HasResult(txnTS, 42)

          partitioner.mockTxnCovered {
            // Key got covered by all nodes in the neighbor replica
            applies.add(hosts(4), txnTS, 42).value shouldBe
              KeysCovered(txnTS, 42, allLocals = true)

            // New acks don't change the outcome
            applies.add(hosts(6), txnTS, 42) shouldBe empty

            // All writers have completed
            applies.add(hosts(7), txnTS, 42).value shouldBe AllNodes(txnTS, 42)
          }
        }
      }
    }
  }
}
