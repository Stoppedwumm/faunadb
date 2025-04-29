package fauna.cluster.test

import fauna.atoms.{ HostID, Location, Segment }
import fauna.cluster.{ HostInformation, ReplicaInfo }
import fauna.cluster.topology.{
  Repartitioner,
  ReplicaTopology,
  SegmentOwnership,
  TopologyPartitioner,
  TopologyStateV3
}
import fauna.storage.{ Txn, TxnRead }
import fauna.storage.cassandra.{ CassandraKeyExtractor, CassandraKeyLocator }
import io.netty.buffer.ByteBuf
import scala.util.Random

class TopologyPartitionerSpec extends Spec {
  "TopologyPartitioner" - {
    "replicas" - {
      "returns ordered list of adjacent sub segments" in {

        val topologyR1 = {
          val h1r1 = HostID.randomID
          val h2r1 = HostID.randomID
          val h3r1 = HostID.randomID

          ReplicaTopology.create(
            Repartitioner
              .newTopologyProposal(
                Set(h1r1, h2r1, h3r1),
                Set.empty,
                ReplicaTopology.Empty)
              .newPending
          )
        }

        val topologyR2 = {
          val h1r2 = HostID.randomID
          val h2r2 = HostID.randomID
          val h3r2 = HostID.randomID

          ReplicaTopology.create(
            Repartitioner
              .newTopologyProposal(
                Set(h1r2, h2r2, h3r2),
                Set.empty,
                ReplicaTopology.Empty)
              .newPending
          )
        }

        val topologyR3 = {
          val segs = Vector(SegmentOwnership(Location.MinValue, HostID.randomID))
          ReplicaTopology(segs, segs, Vector.empty)
        }

        def verifyForSegment(seg: Segment) = {
          val subSegments =
            TopologyPartitioner.replicas(
              Set(topologyR1, topologyR2, topologyR3),
              seg)

          subSegments.tail.foldLeft(subSegments.head) {
            case ((seg1, _), (seg2, hosts2)) =>
              // all sub segment are ordered and adjacent
              seg1.right should equal(seg2.left)
              // each sub segment has a host in both replicas
              hosts2 should have size 3
              (seg2, hosts2)
          }

          // first sub segment should also have a host in both replicas
          subSegments.head._2 should have size 3

          subSegments.head._1.left should equal(seg.left)
          subSegments.last._1.right should equal(seg.right)
        }

        verifyForSegment(Segment.All)
        verifyForSegment(Segment(Location.MinValue, Location.random()))
        verifyForSegment(Segment(Location.random(), Location.MinValue))

        val loc1 = Location.random()
        verifyForSegment(Segment(loc1, Location(loc1.token + 1)))

        val loc2 = Location.random()
        verifyForSegment(Segment(loc1 min loc2, loc1 max loc2))
      }

      "segmentsInReplica should cover entire ring" in {
        val host1 = HostID.randomID
        val host2 = HostID.randomID

        val random = new Random(0L)

        val topology =
          ReplicaTopology.create(
            Repartitioner
              .newTopologyProposal(
                Set(host1, host2),
                Set.empty,
                ReplicaTopology.Empty,
                random,
                locationsPerHost = 4)
              .newPending
          )

        val replicaInfo = new ReplicaInfo {
          def replicaNames: Set[String] = Set("replica1")
          def self: HostID = ???
          def hostInformation(host: HostID): Option[HostInformation] = ???
        }

        val partitioner = new TopologyPartitioner[ByteBuf, TxnRead, Txn](
          TopologyStateV3(Map("replica1" -> topology), Vector("replica1"), 1),
          replicaInfo,
          CassandraKeyLocator,
          CassandraKeyExtractor
        )

        partitioner.segmentsInReplica("replica2") should have size 0

        val segmentsByHost = partitioner.segmentsInReplica("replica1")
        segmentsByHost should have size 2

        segmentsByHost(host1) shouldBe Vector(
          Segment(Location(-2996308202189857655L), Location(4815349556090146136L)),
          Segment(Location(6227406868931904324L), Location(7661520242631645434L))
        )
        segmentsByHost(host2) shouldBe Vector(
          Segment(Location(-9223372036854775808L), Location(-2996308202189857655L)),
          Segment(Location(4815349556090146136L), Location(6227406868931904324L)),
          Segment(Location(7661520242631645434L), Location(-9223372036854775808L))
        )

        val normalized = Segment.normalize(segmentsByHost flatMap { case (_, segs) =>
          segs
        } toSeq)

        normalized should have size 1
        normalized.head shouldBe Segment.All
      }
    }
  }
}
