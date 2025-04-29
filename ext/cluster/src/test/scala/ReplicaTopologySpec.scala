package fauna.cluster.test

import fauna.atoms._
import fauna.cluster.topology._
import fauna.cluster.topology.ReplicaTopology.NullOwnership
import java.math.BigInteger
import java.util.UUID
import scala.util.Random

class ReplicaTopologySpec extends Spec {
  val id0 = HostID.NullID
  val id1 = HostID(new UUID(1L << 32, 0))
  val id2 = HostID(new UUID(2L << 32, 0))
  val id3 = HostID(new UUID(3L << 32, 0))

  val id1_start = Location.MinValue
  val id2_start = Location(Long.MinValue / 3)
  val id3_start = Location(Long.MaxValue / 3)

  "ReplicaTopology" - {
    "takes ownership in an empty current topology" - {
      "all of it" in {
        expectUpdated(NullOwnership,
                      Segment.All,
                      id1,
                      Vector(SegmentOwnership(Location.MinValue, id1)))
      }
      "left half of it" in {
        expectUpdated(NullOwnership,
                      Segment(Location.MinValue, Location(0)),
                      id1,
                      Vector(SegmentOwnership(Location.MinValue, id1),
                             SegmentOwnership(Location(0), id0)))
      }
      "right half of it" in {
        expectUpdated(NullOwnership,
                      Segment(Location(0), Location.MinValue),
                      id1,
                      Vector(SegmentOwnership(Location.MinValue, id0),
                             SegmentOwnership(Location(0), id1)))
      }
      "some of it in the middle" in {
        expectUpdated(NullOwnership,
                      Segment(Location(0), Location(1000)),
                      id1,
                      Vector(SegmentOwnership(Location(0), id1),
                             SegmentOwnership(Location(1000), id0)))
      }
    }

    "normalizes when taking ownership in current topology of" - {
      val populated = Vector(SegmentOwnership(id1_start, id1),
                             SegmentOwnership(id2_start, id2),
                             SegmentOwnership(id3_start, id3))

      "already owned segment" - {
        "all of it" in {
          expectUpdated(populated, Segment(id1_start, id2_start), id1, populated)
          expectUpdated(populated, Segment(id2_start, id3_start), id2, populated)
          expectUpdated(populated,
                        Segment(id3_start, Location.MinValue),
                        id3,
                        populated)
        }
        "on its left side" in {
          expectUpdated(populated,
                        Segment(id1_start, Location(id1_start.token + 100)),
                        id1,
                        populated)
          expectUpdated(populated,
                        Segment(id2_start, Location(id2_start.token + 100)),
                        id2,
                        populated)
          expectUpdated(populated,
                        Segment(id3_start, Location(id3_start.token + 100)),
                        id3,
                        populated)
        }
        "on its right side" in {
          expectUpdated(populated,
                        Segment(Location(id2_start.token - 100), id2_start),
                        id1,
                        populated)
          expectUpdated(populated,
                        Segment(Location(id3_start.token - 100), id3_start),
                        id2,
                        populated)
          expectUpdated(populated,
                        Segment(Location(Location.MinValue.token - 100),
                                Location.MinValue),
                        id3,
                        populated)
        }
        "in the middle of it" in {
          expectUpdated(populated,
                        Segment(Location(id1_start.token + 100),
                                Location(id2_start.token - 100)),
                        id1,
                        populated)
          expectUpdated(populated,
                        Segment(Location(id2_start.token + 100),
                                Location(id3_start.token - 100)),
                        id2,
                        populated)
          expectUpdated(populated,
                        Segment(Location(id3_start.token + 100),
                                Location(Location.MinValue.token - 100)),
                        id3,
                        populated)
        }
      }
      "segment adjacent to owned" - {
        "from the left" in {
          val start = Location(id2_start.token - 100)
          expectUpdated(populated,
                        Segment(start, id2_start),
                        id2,
                        Vector(SegmentOwnership(id1_start, id1),
                               SegmentOwnership(start, id2),
                               SegmentOwnership(id3_start, id3)))
        }
        "from the left overlapping" in {
          val start = Location(id2_start.token - 100)
          expectUpdated(
            populated,
            Segment(start, Location(id2_start.token + 100)),
            id2,
            Vector(SegmentOwnership(id1_start, id1),
                   SegmentOwnership(start, id2),
                   SegmentOwnership(id3_start, id3))
          )
        }
        "from the right" in {
          val end = Location(id3_start.token + 100)
          expectUpdated(populated,
                        Segment(id3_start, end),
                        id2,
                        Vector(SegmentOwnership(id1_start, id1),
                               SegmentOwnership(id2_start, id2),
                               SegmentOwnership(end, id3)))
        }
        "from the right overlapping" in {
          val end = Location(id3_start.token + 100)
          expectUpdated(
            populated,
            Segment(Location(id3_start.token - 100), end),
            id2,
            Vector(SegmentOwnership(id1_start, id1),
                   SegmentOwnership(id2_start, id2),
                   SegmentOwnership(end, id3))
          )
        }
        "in the middle of it" in {
          val start = Location(id2_start.token + 100)
          val end = Location(id3_start.token - 100)
          expectUpdated(
            populated,
            Segment(start, end),
            id3,
            Vector(SegmentOwnership(id1_start, id1),
                   SegmentOwnership(id2_start, id2),
                   SegmentOwnership(start, id3),
                   SegmentOwnership(end, id2),
                   SegmentOwnership(id3_start, id3))
          )
        }
      }
      "segment covering already owned" - {
        "exactly" in {
          expectUpdated(populated, Segment(id2_start, id3_start), id2, populated)
        }
        "extending to the left" in {
          val start = Location(id2_start.token - 100)
          expectUpdated(populated,
                        Segment(start, id3_start),
                        id2,
                        Vector(SegmentOwnership(id1_start, id1),
                               SegmentOwnership(start, id2),
                               SegmentOwnership(id3_start, id3)))
        }
        "extending to the right" in {
          val end = Location(id3_start.token + 100)
          expectUpdated(populated,
                        Segment(id2_start, end),
                        id2,
                        Vector(SegmentOwnership(id1_start, id1),
                               SegmentOwnership(id2_start, id2),
                               SegmentOwnership(end, id3)))
        }
        "extending to both sides" in {
          val start = Location(id2_start.token - 100)
          val end = Location(id3_start.token + 100)
          expectUpdated(populated,
                        Segment(start, end),
                        id2,
                        Vector(SegmentOwnership(id1_start, id1),
                               SegmentOwnership(start, id2),
                               SegmentOwnership(end, id3)))
        }
      }
    }

    "makes updates consistent with pending when" - {
      val cons1 = Vector(SegmentOwnership(Location.MinValue, id1),
                         SegmentOwnership(Location(0), id2),
                         SegmentOwnership(Location(100), id3))

      "exactly matching a segment" in {
        expectConsistent(cons1, Segment(Location(0), Location(100)), id2)
      }
      "being in the left of a segment" in {
        expectConsistent(cons1, Segment(Location(0), Location(50)), id2)
      }
      "being in the right of a segment" in {
        expectConsistent(cons1, Segment(Location(50), Location(100)), id2)
      }
      "being within a segment" in {
        expectConsistent(cons1, Segment(Location(20), Location(80)), id2)
      }

      val cons2 = Vector(SegmentOwnership(Location.MinValue, id1),
                         SegmentOwnership(Location(0), id2),
                         SegmentOwnership(Location(50), id2),
                         SegmentOwnership(Location(100), id3))

      "exactly matching two segments" in {
        expectConsistent(cons2, Segment(Location(0), Location(100)), id2)
      }
      "being in the left of two segment" in {
        expectConsistent(cons1, Segment(Location(0), Location(60)), id2)
      }
      "being in the right of two segment" in {
        expectConsistent(cons1, Segment(Location(40), Location(100)), id2)
      }
      "being within two segments" in {
        expectConsistent(cons1, Segment(Location(20), Location(80)), id2)
      }
    }

    "does not make updates inconsistent with pending when the segment" - {
      val cons3 = Vector(SegmentOwnership(Location.MinValue, id1),
                         SegmentOwnership(Location(0), id2),
                         SegmentOwnership(Location(100), id3))

      "only touches a matching segment" in {
        expectNotConsistent(cons3, Segment(Location(-100), Location(0)), id2)
      }
      "is entirely in a non-matching segment" in {
        expectNotConsistent(cons3, Segment(Location(-100), Location(-10)), id2)
      }
      "is only partially in a matching segment" in {
        expectNotConsistent(cons3, Segment(Location(-100), Location(50)), id2)
      }
      "is in two non-matching segments" in {
        expectNotConsistent(cons3, Segment(Location(-100), Location(50)), id3)
      }

      val cons4 = Vector(SegmentOwnership(Location.MinValue, id1),
                         SegmentOwnership(Location(0), id2),
                         SegmentOwnership(Location(100), id3),
                         SegmentOwnership(Location(200), id2))

      "contains a non-matching segment" in {
        expectNotConsistent(cons4, Segment(Location(0), Location(300)), id2)
        expectNotConsistent(cons4, Segment(Location(50), Location(300)), id2)
      }
    }

    "calculates missing segments" - {
      "on an empty current ring" - {
        "when all needs to be taken over" in {
          expectMissing(NullOwnership,
                        Vector(SegmentOwnership(Location.MinValue, id1)),
                        id1,
                        Vector(Segment.All))
        }
        "when left half needs to be taken over" in {
          expectMissing(NullOwnership,
                        Vector(SegmentOwnership(Location.MinValue, id1),
                               SegmentOwnership(Location(0), id2)),
                        id1,
                        Vector(Segment(Location.MinValue, Location(0))))
        }
        "when right half needs to be taken over" in {
          expectMissing(NullOwnership,
                        Vector(SegmentOwnership(Location.MinValue, id1),
                               SegmentOwnership(Location(0), id2)),
                        id2,
                        Vector(Segment(Location(0), Location.MinValue)))
        }
        "when some need to be taken over" in {
          expectMissing(
            NullOwnership,
            Vector(
              SegmentOwnership(Location.MinValue, id2),
              SegmentOwnership(Location(0), id1),
              SegmentOwnership(Location(100), id2),
              SegmentOwnership(Location(200), id1),
              SegmentOwnership(Location(300), id2)
            ),
            id1,
            Vector(
              Segment(Location(0), Location(100)),
              Segment(Location(200), Location(300))
            )
          )
        }
      }
      "on a populated ring" in {
        val populated = Vector(
          SegmentOwnership(Location.MinValue, id2),
          SegmentOwnership(Location(0), id1),
          SegmentOwnership(Location(100), id2),
          SegmentOwnership(Location(200), id3),
          SegmentOwnership(Location(300), id1),
          SegmentOwnership(Location(400), id2),
          SegmentOwnership(Location(500), id3)
        )

        val pending = Vector(
          SegmentOwnership(Location.MinValue, id3),
          SegmentOwnership(Location(0), id1),
          SegmentOwnership(Location(100), id3),
          SegmentOwnership(Location(280), id1),
          SegmentOwnership(Location(420), id2),
          SegmentOwnership(Location(520), id1),
          SegmentOwnership(Location(550), id2)
        )

        val expected = Vector(
          Segment(Location(280), Location(300)),
          Segment(Location(400), Location(420)),
          Segment(Location(520), Location(550))
        )

        expectMissing(populated, pending, id1, expected)
      }
    }
  }

  "OwnedSegment.rebalance should" - {
    "rebalance a topology" in {
      1 to 1000 foreach { _ =>
        // Random distribution
        val r = new Random()
        val hostIDs = 1 to 5 map { i => HostID(new UUID(0L, i)) }
        val ring = hostIDs flatMap { h =>
          1 to 128 map { _ => SegmentOwnership(Location(r.nextLong()), h) }
        } sorted

        val desiredSizes = Topologies.balancedReplicaSizes(hostIDs.toSet, hostIDs.toSet)
        val bySize = desiredSizes.groupBy { case (_, v) => v }
        // Should be equally divided to 2^64/5, with one host getting one token more.
        bySize(new BigInteger("3689348814741910323")).size should equal (4)
        bySize(new BigInteger("3689348814741910324")).size should equal (1)

        val balanced = OwnedSegment.balance(desiredSizes, SegmentOwnership.toOwnedSegments(ring.toVector))

        val perHost = balanced groupBy { _.host }
        perHost foreach { case (host, segs) =>
          val owns = segs.foldLeft(BigInteger.ZERO) { case (sum, seg) => sum.add(seg.segment.length) }
          owns should equal(desiredSizes(host))
        }
      }
    }

    "rebalance a topology with some removed hosts" in {
      1 to 100 foreach { _ =>
        // Random distribution
        val r = new Random()
        val hostIDs = 1 to 7 map { i => HostID(new UUID(0L, i)) }
        val ring = hostIDs flatMap { h =>
          1 to 128 map { _ => SegmentOwnership(Location(r.nextLong()), h) }
        } sorted

        val desiredSizes = Topologies.balancedReplicaSizes(hostIDs.toSet, hostIDs.toSet - hostIDs(2) - hostIDs(5))
        val bySize = desiredSizes.groupBy { case (_, v) => v }
        // Should be equally divided to 2^64/5, with one host getting one token more.
        // Two removed hosts should get zero.
        bySize(new BigInteger("3689348814741910323")).size should equal (4)
        bySize(new BigInteger("3689348814741910324")).size should equal (1)
        bySize(BigInteger.ZERO).size should equal (2)

        val balanced = OwnedSegment.balance(desiredSizes, SegmentOwnership.toOwnedSegments(ring.toVector))

        val perHost = balanced groupBy { _.host }
        perHost foreach { case (host, segs) =>
          val owns = segs.foldLeft(BigInteger.ZERO) { case (sum, seg) => sum.add(seg.segment.length) }
          owns should equal(desiredSizes(host))
        }
      }
    }
  }

  def expectUpdated(
    current: Vector[SegmentOwnership],
    seg: Segment,
    hostID: HostID,
    expected: Vector[SegmentOwnership]) =
    ReplicaTopology(current,
                    Vector(SegmentOwnership(Location.MinValue, hostID)),
                    Vector.empty).updateCurrent(seg, hostID).current should equal(
      expected)

  def expectConsistent(
    pending: Vector[SegmentOwnership],
    seg: Segment,
    hostID: HostID) = {
    val b = Seq.newBuilder[SegmentOwnership]
    b += SegmentOwnership(seg.left, hostID)
    if (!seg.right.isMin) {
      b += SegmentOwnership(seg.right, HostID.NullID)
    }
    ReplicaTopology(NullOwnership, pending, Vector.empty)
      .updateCurrent(seg, hostID)
      .current should equal(b.result())
  }

  def expectNotConsistent(
    pending: Vector[SegmentOwnership],
    seg: Segment,
    hostID: HostID) =
    ReplicaTopology(NullOwnership, pending, Vector.empty)
      .updateCurrent(seg, hostID)
      .current should equal(NullOwnership)

  def expectMissing(
    current: Vector[SegmentOwnership],
    pending: Vector[SegmentOwnership],
    hostID: HostID,
    expected: Vector[Segment]) =
    ReplicaTopology(current, pending, Vector.empty)
      .missingSegments(hostID) should equal(expected)
}
