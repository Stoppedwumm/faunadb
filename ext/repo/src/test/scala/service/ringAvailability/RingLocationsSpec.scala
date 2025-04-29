package fauna.repo.test.service.ringAvailability

import fauna.atoms.{ HostID, Location, Segment }
import fauna.repo.service.ringAvailability.RingLocations
import fauna.repo.test.Spec
import java.math.BigInteger

class RingLocationsSpec extends Spec {

  "RingLocations" - {
    val r1h1 = HostID.randomID
    val r1h2 = HostID.randomID
    val r2h1 = HostID.randomID
    val r2h2 = HostID.randomID
    val r3h1 = HostID.randomID
    val r3h2 = HostID.randomID

    val replicas = Set("r1", "r2", "r3")
    val hostsByReplica = Map(
      "r1" -> Set(r1h1, r1h2),
      "r2" -> Set(r2h1, r2h2),
      "r3" -> Set(r3h1, r3h2)
    )

    val ptc10 = Segment.RingLength.divide(BigInteger.valueOf(10)).longValue()

    /* The test topology contains:
     * - Two replicas that span the whole ring. The hosts overlap at -10% to 10% of
     * the ring, allowing us to isolate behavior around that segment.
     * - One replica that spans the -10% to +10% segment of the ring. These hosts
     * contain more segments just to test more segments in the sets.
     *
     * This configuration allows us to test overlapping and non-overlapping host
     * unavailability and ensure that we detect unavailability even when a replica
     * doesn't cover the entire keyspace, such as during a topology shift. */
    val segmentsMap = Map(
      // R1 spans the entire ring, Min to -10% and -10% to Max
      r1h1 -> Vector(
        Segment(Location(Long.MinValue), Location(-ptc10))
      ),
      r1h2 -> Vector(
        Segment(Location(-ptc10), Location(Long.MinValue))
      ),
      // R2 spans the entire ring, Min to +10% and +10% to Max
      r2h1 -> Vector(
        Segment(Location(Long.MinValue), Location(ptc10))
      ),
      r2h2 -> Vector(
        Segment(Location(ptc10), Location(Long.MinValue))
      ),
      // R3 spans -10% to ~0 and ~0 to +10% of the ring
      r3h1 -> Vector(
        // r3h1 contains exactly 10% of the ring
        Segment(Location(-ptc10), Location(-100)),
        Segment(Location(-75), Location(-50)),
        Segment(Location(-25), Location(0)),
        Segment(Location(25), Location(75))
      ),
      r3h2 -> Vector(
        // r3h2 contains exactly 10% of the ring
        Segment(Location(-100), Location(-75)),
        Segment(Location(-50), Location(-25)),
        Segment(Location(0), Location(25)),
        Segment(Location(75), Location(ptc10))
      )
    )

    "percentAvailable returns 100.0 when all hosts are alive" in {
      val isAlive = Function.const(true)(_: HostID)
      val result = RingLocations.percentAvailable(
        dataReplicas = replicas,
        hostsByReplica = hostsByReplica,
        isAlive = isAlive,
        segmentsByHost = segmentsMap
      )

      result should equal(100)
    }

    "percentAvailable returns expected percent when overlapping hosts are down" in {
      /* Ring availability by replica:
       * - R1 -> Min to -10%
       * - R2 -> 10% to Max
       * - R3 -> ~0 to 10% */
      val isAlive = (h: HostID) => {
        if (h == r1h1 || h == r2h2 || h == r3h2) true
        else false
      }

      val result = RingLocations.percentAvailable(
        dataReplicas = replicas,
        hostsByReplica = hostsByReplica,
        isAlive = isAlive,
        segmentsByHost = segmentsMap
      )

      result should equal(90)
    }

    "percentAvailable returns 100.0 when down hosts are not overlapping" in {
      /* Ring availability by replica:
       * - R1 -> Min to -10%
       * - R2 -> 10% to Max
       * - R3 -> -10% to 10% */
      val isAlive = (h: HostID) => {
        if (h == r1h2 || h == r2h1) false
        else true
      }

      val result = RingLocations.percentAvailable(
        dataReplicas = replicas,
        hostsByReplica = hostsByReplica,
        isAlive = isAlive,
        segmentsByHost = segmentsMap
      )

      result should equal(100)
    }

    "percentAvailable returns 0.0 when no hosts are alive" in {
      /* Ring availability by replica:
       * - R1 -> None
       * - R2 -> None
       * - R3 -> None */
      val isAlive = Function.const(false)(_: HostID)
      val result = RingLocations.percentAvailable(
        dataReplicas = replicas,
        hostsByReplica = hostsByReplica,
        isAlive = isAlive,
        segmentsByHost = segmentsMap
      )

      result should equal(0)
    }

    "percentAvailable returns expected percent when only replica with partial current ring is up" in {
      /* Ring availability by replica:
       * - R1 -> None
       * - R2 -> None
       * - R3 -> -10% to +10% */
      val isAlive = (h: HostID) => {
        if (h == r3h1 || h == r3h2) true
        else false
      }

      val result = RingLocations.percentAvailable(
        dataReplicas = replicas,
        hostsByReplica = hostsByReplica,
        isAlive = isAlive,
        segmentsByHost = segmentsMap
      )

      result should equal(20)
    }
  }
}
