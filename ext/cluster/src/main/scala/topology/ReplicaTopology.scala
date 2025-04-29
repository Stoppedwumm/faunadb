package fauna.cluster.topology

import fauna.atoms._
import fauna.codex.cbor.CBOR
import java.math.BigInteger
import java.util.Arrays
import scala.annotation.tailrec
import scala.collection.Searching._

object SegmentOwnership {
  implicit val codec = CBOR.TupleCodec[SegmentOwnership]

  /**
    * Returns a left-inclusive range [from, to) of segment ownership sequence indices
    * covering the particular segment. "from" can be -1 to indicate wraparound ownership.
    */
  private[topology] def covering(
    s: Vector[SegmentOwnership],
    seg: Segment): (Int, Int) = {
    val from = findIdx(s, seg.left)
    val to = if (seg.right == Location.MinValue) {
      s.length
    } else {
      find(s, seg.right).insertionPoint
    }
    (from, to)
  }

  def toOwnedSegments(s: Vector[SegmentOwnership]): Vector[OwnedSegment] =
    toOwnedSegments(s, if (s.head.from.isMin) 0 else -1, s.length)

  /**
    * Returns a sequence of owned segments based on a range of segment ownerships.
    * The range of ownerships is [from, to) where "from" can be -1 to indicate
    * wraparound ownership.
    */
  private[topology] def toOwnedSegments(s: Vector[SegmentOwnership], from: Int, to: Int): Vector[OwnedSegment] = {
    require(-1 <= from)
    require(from < to)
    require(to <= s.length)
    val b = Vector.newBuilder[OwnedSegment]
    var i = from
    if (i == -1) {
      // add a wraparound segment
      b += OwnedSegment(Segment(Location.MinValue, s(0).from), s.last.host)
      i = 0
    }
    var os = s(i)
    while (i < to) {
      val nextIdx = i + 1
      if (nextIdx < s.length) {
        val next = s(nextIdx)
        b += OwnedSegment(Segment(os.from, next.from), os.host)
        os = next
      } else {
        b += OwnedSegment(Segment(os.from, Location.MinValue), os.host)
        os = null
      }
      i = nextIdx
    }
    b.result()
  }

  // Removes any SegmentOwnership from the vector that is preceded by a
  // SegmentOwnership for the same host.
  def normalize(s: Vector[SegmentOwnership]): Vector[SegmentOwnership] = {
    if (s.isEmpty) {
      s
    } else {
      val b = Vector.newBuilder[SegmentOwnership]
      b.sizeHint(s.length)
      val it = s.iterator
      var so = it.next()
      b += so
      while (it.hasNext) {
        val nso = it.next()
        if (nso.host != so.host) {
          b += nso
          so = nso
        }
      }
      val s2 = b.result()

      // Coalesce wraparound segment
      val s3 = if (s2.length > 1 && s2.head.host == s2.last.host) {
        s2.tail
      } else {
        s2
      }

      // Canonical representation for single host owning the whole ring
      val s4 = if (s3.length == 1) {
        Vector(SegmentOwnership(Location.MinValue, s3.head.host))
      } else {
        s3
      }

      if (s4 == s) {
        s
      } else {
        s4
      }
    }
  }

  def hostForLocation(s: Vector[SegmentOwnership], l: Location): HostID = {
    val segIdx = findIdx(s, l)
    // Handle wraparound
    (if (segIdx == -1) s.last else s(segIdx)).host
  }

  private def findIdx(s: Vector[SegmentOwnership], l: Location) =
    find(s, l) match {
      case Found(idx)          => idx
      case InsertionPoint(idx) => idx - 1
    }

  private def find(s: Vector[SegmentOwnership], l: Location) =
    s.search(SegmentOwnership(l, HostID.NullID))
}

// Represents assignment of a single segment to a host within a replica.
// These objects are kept in a sequence, and represent ownership of a
// Segment stretching from "from" (inclusive) to the next SegmentOwnership's
// "from" (exclusive) in the sequence.
case class SegmentOwnership(from: Location, host: HostID) extends Ordered[SegmentOwnership] {
  def compare(other: SegmentOwnership) = from compare other.from
}

case class OwnedSegment(segment: Segment, host: HostID) {
  def claimed = host != HostID.NullID
}

object OwnedSegment {
  def segmentsForHost(segs: Vector[OwnedSegment], host: HostID): Vector[Segment] =
    segs collect {
      case os if os.host == host => os.segment
    }

  def toSegmentOwnership(segs: Vector[OwnedSegment]): Vector[SegmentOwnership] = {
    // Remove the wraparound segment
    val trimmed = if (segs.size > 1 && segs.head.host == segs.last.host) {
      segs.drop(1)
    }  else {
      segs
    }
    trimmed map { case OwnedSegment(Segment(left, _), host) => SegmentOwnership(left, host) }
  }

  def ensureValidTopology(ring: Vector[OwnedSegment]): Unit = {
    require(ring.nonEmpty)
    // Full range
    require(ring.head.segment.left == Location.MinValue)
    require(ring.last.segment.right == Location.MinValue)
    // Ring is full length
    require(ring.foldLeft(BigInteger.ZERO) { (sum, os) => sum.add(os.segment.length) } == Segment.RingLength)
    // Fully covering, non-overlapping, ordered
    ring.foldLeft(Location.MinValue.token) { (next, os) =>
      require(os.segment.left.token == next)
      os.segment.length.add(BigInteger.valueOf(next)).longValue()
    }
  }

  def differentHosts(
    these: Vector[OwnedSegment],
    those: Vector[OwnedSegment]): Vector[OwnedSegment] = {
    var _those = those
    these flatMap { thiz =>
      // drop those that don't intersect current segment
      _those = _those dropWhile { _.segment precedes thiz.segment }
      // intersect overlapping ones with different owner with the segment
      _those.iterator takeWhile { that =>
        !(thiz.segment precedes that.segment)
      } filter {
        _.host != thiz.host
      } flatMap {
        _.segment intersect thiz.segment
      } map { s =>
        OwnedSegment(s, thiz.host)
      }
    }
  }

  // Given a topology expressed as a vector of owned segments, it will return a
  // topology with same ownership, but with some segments split into multiple
  // smaller segments in order to ensure that every host has no less than segCount
  // segments. For every host that has less than segCount segments, it will
  // repeatedly split its largest segment in two equal halves, until it reaches
  // the desired count. If any host already has segCount or more segments, they
  // are not affected. This method is useful for converting a normalized
  // topology (or one computed from normalized) into a sufficiently denormalized
  // one that can be used as a pending topology (as Repartitioner will add
  // segments to pending topologies that don't have 128 segments per host.)
  def increaseSegmentation(topo: Vector[OwnedSegment], segCount: Int): Vector[OwnedSegment] = {
    ensureValidTopology(topo)
    val wraparoundHost = Option.when(topo.size > 1 && topo.head.host == topo.last.host)(topo.head.host)

    def hostSegCount(h: HostID) = {
      // If there's a wraparound segment, account for the fact it's
      // represented by two segments here, one at the end of the
      // ring and one at the start of the ring. The host owning the
      // wraparound segment thus has an extra segment in its count.
      if (wraparoundHost.contains(h)) {
        segCount + 1
      } else {
        segCount
      }
    }

    @tailrec
    def increaseSegmentation0(topo: Vector[OwnedSegment]): Vector[OwnedSegment] = {
      val topoByHost = topo.groupBy {
        _.host
      }

      // Find any host with < segCount tokens
      val minHost = topoByHost collectFirst {
        case (h, s) if s.size < hostSegCount(h)=> h
      }

      minHost match {
        case Some(h) =>
          // Find the largest segment
          val maxOwnedSeg = topoByHost(h) maxBy {
            _.segment.length
          }

          // Split it in two
          val maxSeg = maxOwnedSeg.segment
          val splitSegs = maxSeg.split(maxSeg.midpoint) map { OwnedSegment(_, h) }
          // Must be splittable
          if (splitSegs.sizeIs != 2) {
            throw new IllegalArgumentException(s"Can't increase segmentation of $minHost to $segCount")
          }

          // Replace it with the split, repeat
          val newRing = topo.patch(topo.indexOf(maxOwnedSeg), splitSegs, 1)
          // repeat
          increaseSegmentation0(newRing)

        case None =>
          topo // done!
      }
    }

    increaseSegmentation0(topo)
  }

  // Given a topology and a map of hosts to their desired ownership sizes in the
  // topology, transfers ownership of segments that own more than their desired
  // share to those owning less than their desired share. The algorithm is greedy,
  // trying to minimize the number of ownership changes by always trying to
  // transfer the largest segments possible.
  final def balance(desiredSizes: Map[HostID, BigInteger], topo: Vector[OwnedSegment]): Vector[OwnedSegment] = {
    ensureValidTopology(topo)
    // desired sizes add up to the full ring length
    require(desiredSizes.values.foldLeft(BigInteger.ZERO) { (sum, l) => sum.add(l) } == Segment.RingLength)

    // Add all hosts in topology not present in desiredSizes with zero ownership
    val unspecifiedHosts = (topo map { _.host } toSet) diff desiredSizes.keySet
    val allDesiredSizes = desiredSizes ++ (unspecifiedHosts map { h => (h, BigInteger.ZERO) })

    @tailrec
    def balance0(topo: Vector[OwnedSegment]): Vector[OwnedSegment] = {
      val currentSizes = topo groupBy { _.host } map { case (host, segs) =>
        (host, segs.foldLeft(BigInteger.ZERO) { (sum, seg) => sum.add(seg.segment.length) })
      }
      val hostDiffs = allDesiredSizes map { case (host, size) =>
        (host, currentSizes.getOrElse(host, BigInteger.ZERO).subtract(size))
      }

      val (maxHost, maxDiff) = hostDiffs maxBy { case (_, s) => s }
      val (minHost, minDiff) = hostDiffs minBy { case (_, s) => s }
      if (maxDiff == BigInteger.ZERO && minDiff == BigInteger.ZERO) {
        // We're done!
        topo
      } else if (maxDiff.compareTo(BigInteger.ZERO) > 0 && minDiff.compareTo(BigInteger.ZERO) < 0) {
        // We should transfer the amount of locations that is the desired size
        // for minHost to catch up to 0 diff, but no more than what would drop
        // maxHost below 0 diff.
        val maxTransferSize = minDiff.negate().min(maxDiff)

        // Find the largest maxHost-owned segment to take a bite out of. We could
        // use different strategies, such as "largest segment that's smaller or
        // equal to transfer size", or "smallest segment that's larger or equal
        // to transfer size", but always taking the largest gives the best chance
        // that we won't leave very narrow segments behind.
        val oseg = topo.iterator filter { _.host == maxHost } maxBy { _.segment.length }
        val seg = oseg.segment

        val newSegs = if (seg.length.compareTo(maxTransferSize) <= 0) {
          // Segment is not larger than maxTransferSize; reassign the whole segment.
          Seq(OwnedSegment(seg, minHost))
        } else {
          // Segment is larger than maxTransferSize; reassign right part of it.
          // We could choose whether we want to reassign left or right part of
          // it; right makes more sense because if a node was removed recently,
          // the maxHost's segment might've been extended to cover a removed
          // node's adjacent segment and in that case the right end of the
          // segment is less likely to also be owned by the maxHost in current
          // topology.
          val splitSegs = seg.split(Location(seg.bigRight.subtract(maxTransferSize).longValue()))
          require(splitSegs.sizeIs == 2)
          Seq(
            OwnedSegment(splitSegs.head, maxHost),
            OwnedSegment(splitSegs.last, minHost)
          )
        }

        balance0(
          topo.patch(topo.indexOf(oseg), newSegs, 1)
        )
      } else {
        // Cannot happen due to pre-requirements on desiredSizes unless there's a bug
        throw new AssertionError(s"Unexpected values maxDiff=$maxDiff minDiff=$minDiff")
      }
    }

    balance0(topo)
  }
}

object ReplicaTopology {
  implicit val codec = CBOR.TupleCodec[ReplicaTopology]

  // Represents the initial state of a ring (either pending or current) in
  // which no locations are owned by any host.
  val NullOwnership = Vector(SegmentOwnership(Location.MinValue, HostID.NullID))

  val Empty = ReplicaTopology(NullOwnership, NullOwnership, Vector.empty)

  // Throws if the ring is empty OR not ordered
  def validateRing(o: Vector[SegmentOwnership]): Location = {
    require(o.nonEmpty, "ring must not be empty")
    o.iterator.drop(1).foldLeft(Location.MinValue) { (prev, next) =>
      require(
        next.from > prev,
        "a ring element must be greater than the one preceding it")
      next.from
    }
  }

  def create(p: Vector[SegmentOwnership]): ReplicaTopology = {
    // By only doing transformations (updateCurrent) through the public API, we
    // ensure invariants, such as normalization in the current ring.
    val rt =
      SegmentOwnership.toOwnedSegments(p).foldLeft(ReplicaTopology(NullOwnership, p, Vector.empty)) {
        (r, os) =>
          r.updateCurrent(os.segment, os.host)
      }
    require(rt.missingSegments.isEmpty) // no data movement pending upon initialization
    rt
  }

  // If we're left with one host having ownership of the whole ring, normalize it to Location.MinValue.
  // This is not necessary, but avoids the need to express the whole ring as two segments.
  private[ReplicaTopology] def normalizeSingleOwnership(
    v: Vector[SegmentOwnership]) =
    if (v.length == 1 && !v.head.from.isMin) {
      Vector(SegmentOwnership(Location.MinValue, v.head.host))
    } else {
      v
    }
}

/**
  * Represents all assignments of segments to hosts in a replica.
  * It maintains two distinct assignments: a current one and a pending one.
  * When a change in topology is requested, the pending one is updated. As
  * nodes apply the changes to their locally owned data set, they'll gradually
  * update the current one. The current one will over time become equal to
  * pending, at which point the topology has stabilized. It is possible for
  * pending to change while current is catching up to it, in which case the
  * system will start to evolve current towards the new pending.
  *
  * Note that the information in these objects completely describes the
  * topology, and other representations can be derived from it in-memory by
  * dependents. For any particular token location, whichever host owns the range
  * containing it in current should be used for reads, while writes should be
  * directed to both the host owning its range in current and its range in
  * pending topology (they might be the same).
  *
  * Pending topology implicitly always covers the entire token range from
  * Location.MinValue to Location.MaxValue. It is presumed that the last segment
  * wraps around unless there is an explicit Location.MinValue token in it.
  * Current topology can be either empty or incomplete as a new replica is added
  * and is being populated. Unowned ranges in the current topology are represented
  * with HostID.NullID. Coverage never decreases, however, as it always evolves
  * towards the pending topology which is always fully covering.
  *
  * In addition to current and pending ring, it also maintains a list of reusable
  * tokens. These are tokens that used to belong to removed nodes. They can be
  * assigned to newly joining nodes to minimize data movement when nodes are
  * removed and new nodes join shortly thereafter (or the other way round).
  */
case class ReplicaTopology(
  current: Vector[SegmentOwnership],
  pending: Vector[SegmentOwnership],
  reusable: Vector[Location]) {
  import ReplicaTopology._

  validateRing(current)
  validateRing(pending)
  require(current == NullOwnership || (pending forall { _.host != HostID.NullID }))

  def pendingSegments: Vector[OwnedSegment] =
    SegmentOwnership.toOwnedSegments(pending)

  def currentSegments: Vector[OwnedSegment] =
    SegmentOwnership.toOwnedSegments(current)

  // True if pending is consistent with current; in other words,
  // missingSegments() would return an empty list for any host.
  val isStable: Boolean = {
    var cur = currentSegments
    pendingSegments forall { seg =>
      // drop those that don't intersect current segment
      cur = cur dropWhile { _.segment precedes seg.segment }
      // intersect overlapping ones with the segment
      cur.iterator takeWhile { s =>
        !(seg.segment precedes s.segment)
      } forall { _.host == seg.host }
    }
  }

  private def mkMap(v: Vector[SegmentOwnership]) = {
    val sorted = v.sortBy { _.from.token }
    (sorted.iterator.map { _.from.token } toArray, sorted.iterator map { _.host } toArray)
  }
  lazy val currentMap = mkMap(current)
  lazy val pendingMap = mkMap(pending)

  // function expects the 'map' to be sorted as per `mkMap`.
  private def mapLookup(m: (Array[Long], Array[HostID]), l: Location): HostID = {

    val (tokensArray, hostsArray) = m

    // https://docs.oracle.com/javase/8/docs/api/java/util/Arrays.html#binarySearch-long:A-long-
    // index of the search key, if it is contained in the array; otherwise, (-(insertion point) - 1).
    // The insertion point is defined as the point at which the key would be inserted into the array:
    //   the index of the first element greater than the key,
    //   or a.length if all elements in the array are less than the specified key.
    // Note that this guarantees that the return value will be >= 0 if and only if the key is found.
    val idx = Arrays.binarySearch(tokensArray, l.token)

    if (idx < -1) {
      // `-idx -1` maps e.g. -2 => 1; as per the documentation above, to determine the insertion point
      // next we do another `- 1`, as the relevant value is just to the left of the insertion point
      hostsArray(-idx - 1 - 1)
    } else if (idx == -1) {
      // if it would've inserted at the beginning of the array, then the host at the end
      // is the one owning this token (given the wrap-around behavior)
      hostsArray.last
    } else {
      // idx >= 0
      hostsArray(idx)
    }
  }

  def currentHost(l: Location): HostID = mapLookup(currentMap, l)

  def pendingHost(l: Location): HostID = mapLookup(pendingMap, l)

  def updateCurrent(seg: Segment, hostID: HostID) =
    // Only update current if that brings it closer to pending, that is the
    // segment must be assigned to the target host in pending already.
    // FIXME: we could be more lenient and instead of rejecting the request
    // calculate a set of segments that partially cover the passed segment and
    // are consistent with pending. That would allow partially salvaging some
    // data movement work that happened in midst of a change in pending, if in a
    // happily coincidence it is partially consistent with it. This is admittedly
    // not a high priority concern.
    if (isConsistentWithPending(seg, hostID)) {
      addToCurrent(seg, hostID)
    } else {
      this
    }

  /**
    * Calculates missing segments for a particular host. These are the segments
    * that should belong to the host based on the pending ring, but that don't
    * yet belong to the host based on the current ring.
    * @param hostID the host to calculate missing segments for
    */
  def missingSegments(hostID: HostID): Vector[Segment] =
    OwnedSegment.segmentsForHost(missingSegments, hostID)

  /**
    * Calculates all missing segments in the replica. These are the segments
    * that have different ownership in the pending and the current ring.
    */
  def missingSegments: Vector[OwnedSegment] =
    OwnedSegment.differentHosts(pendingSegments, currentSegments)

  private def addToCurrent(seg: Segment, hostID: HostID) = {
    def span(s: Vector[SegmentOwnership], l: Location) =
      s.splitAt(
        s.search(SegmentOwnership(l, HostID.NullID))
          .insertionPoint)

    // Add a min token for the duration of this calculation
    def addMinToken(v: Vector[SegmentOwnership]) =
      if (!v.head.from.isMin) {
        require(v.length > 1) // otherwise we're missing normalizeSingleOwnership somewhere
        SegmentOwnership(Location.MinValue, v(v.length - 1).host) +: v
      } else {
        v
      }

    val Segment(left, right) = seg
    val (before, notBefore) =
      span(addMinToken(normalizeSingleOwnership(current)), left)
    val untilEnd = right.isMin
    val (overlap, after) = if (untilEnd) {
      (notBefore, Vector.empty)
    } else {
      span(notBefore, right)
    }

    // Start building new current ring with the unmodified prefix
    var newCurrent = before

    val lastBefore = before.lastOption

    // Normalize: only append new range start marker if its host is different
    // from one before it.
    if (lastBefore forall { _.host != hostID }) {
      newCurrent = newCurrent :+ SegmentOwnership(left, hostID)
    }

    // Previous segment partially overlapped by the new segment from the right.
    if (!untilEnd && (after.headOption forall { _.from != right })) {
      // lastBefore will be used when the new segment is completely
      // contained in the segment defined by it (so overlaps are empty).
      // Filter normalizes the topology: if the segment following this
      // segment is also owned by the same host, they're merged.
      (overlap.lastOption orElse lastBefore) filter {
        _.host != hostID
      } map {
        _.copy(from = right)
      } foreach { so =>
        newCurrent = newCurrent :+ so
      }
    }

    // drop first element from after if its host is the same to normalize the topology
    val normalizedAfter =
      if ((after.headOption map { _.host }) == (newCurrent.lastOption map { _.host })) {
        after.drop(1)
      } else {
        after
      }
    newCurrent = newCurrent ++ normalizedAfter

    // Drop an explicit Location.MinValue ownership if it's implicit in the wraparound from last ownership
    def removeMinToken(v: Vector[SegmentOwnership]) =
      if (v.length > 1 && v.head.from.isMin && v.head.host == v(v.length - 1).host) {
        v.drop(1)
      } else {
        v
      }

    copy(current = normalizeSingleOwnership(removeMinToken(newCurrent)))
  }

  /**
    * Returns all segments in this replica topology's current ring that
    * intersect the passed-in segment. Since the ring coverage is complete,
    * the set of intersecting segments is guaranteed to fully cover the segment.
    * @param seg the segment
    * @return all segments that at least partially cover this segment.
    */
  def currentCovering(seg: Segment): Vector[OwnedSegment] =
    SegmentOwnership.covering(current, seg) match {
      case (from, to) => SegmentOwnership.toOwnedSegments(current, from, to)
    }

  private def isConsistentWithPending(seg: Segment, hostID: HostID): Boolean = {
    val (from, to) = SegmentOwnership.covering(pending, seg)
    require(to > from)
    SegmentOwnership.toOwnedSegments(pending, from, to) forall { _.host == hostID }
  }
}
