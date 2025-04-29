package fauna.cluster.topology

import fauna.atoms._
import fauna.cluster.ReplicaInfo
import fauna.tx.transaction._
import scala.collection.mutable.ListBuffer

object TopologyPartitioner {

  private final class Cursor(segments: Vector[OwnedSegment]) {
    private[this] var _segments = segments
    private[this] var _currHost: HostID = _
    private[this] var _nextLocation: Option[Location] = _

    private[this] def next(): Unit = {
      _currHost = _segments.head.host
      _segments = _segments.tail
      _nextLocation = _segments.headOption map { _.segment.left }
    }
    next()

    def currentHost: HostID = _currHost
    def nextLocation: Option[Location] = _nextLocation

    def skipPastLocation(location: Location): Unit =
      if (_nextLocation exists { _ >= location }) {
        next()
      }
  }

  def replicas(
    replicas: Set[ReplicaTopology],
    seg: Segment
  ): Seq[(Segment, Set[HostID])] = {
    val replicaCursors = replicas map { t => new Cursor(t.currentCovering(seg)) }
    val result = ListBuffer.empty[(Segment, Set[HostID])]

    @annotation.tailrec
    def loop(left: Location): Unit = {
      val nextLocations = replicaCursors map { _.nextLocation }
      val minimalNextLocation = nextLocations.flatten.minOption
      val hosts = replicaCursors map { _.currentHost }
      minimalNextLocation match {
        case Some(right) =>
          replicaCursors foreach { _.skipPastLocation(right) }
          result.addOne((Segment(left, right), hosts))
          loop(right)
        case None =>
          result.addOne((Segment(left, seg.right), hosts))
      }
    }

    loop(seg.left)

    result.toList
  }
}

class TopologyPartitioner[K, R, W](
  topology: TopologyState,
  replicaInfo: ReplicaInfo,
  keyLocator: KeyLocator[K, R],
  keyExtractor: KeyExtractor[K, R, W])
    extends Partitioner[R, W] {

  val version = topology.version

  private[this] val replicaTopology: Set[ReplicaTopology] =
    replicaInfo.replicaNames flatMap topology.getTopology

  private[this] def replicas(location: Location, pending: Boolean): Set[HostID] = {
    val b = Set.newBuilder[HostID]

    replicaTopology foreach { rt =>
      b += rt.currentHost(location)

      if (pending && !rt.isStable) {
        b += rt.pendingHost(location)
      }
    }

    b.result() filterNot { _ == HostID.NullID }
  }

  def replicas(seg: Segment): Seq[(Segment, Set[HostID])] =
    TopologyPartitioner.replicas(
      replicaTopology,
      seg
    )

  def segments(host: HostID, includePending: Boolean): Vector[Segment] = {
    val replica = replicaInfo.hostInformation(host) flatMap { hi =>
      topology.getTopology(hi.replica)
    }

    replica match {
      case None => Vector.empty
      case Some(t) =>
        val current = segmentsForHost(t.current, host)
        if (includePending && !t.isStable) {
          val pending = segmentsForHost(t.pending, host)
          Segment.normalize(current ++ pending)
        } else {
          current
        }
    }
  }

  def segmentsInReplica(replica: String): Map[HostID, Vector[Segment]] =
    topology.getTopology(replica) map { replicaTopology =>
      val segments = SegmentOwnership.toOwnedSegments(replicaTopology.current)
      segments.groupMap(_.host)(_.segment)
    } getOrElse Map.empty

  def primarySegments(host: HostID, includePending: Boolean): Vector[Segment] = {
    def primaryOwnership(ring: Iterable[SegmentOwnership]): Vector[Segment] = {
      val primaries = ring groupBy { _.from } map { case (_, owns) =>
        // we inherit from C* the concept of "primary" replica here:
        // the lexicographically first host at each location in a
        // common ring.
        owns minBy { _.host }
      }

      // groupBy does not seem to guarantee order preservation; sort Just In Case(TM)
      val sorted = primaries.toVector sortBy { _.from }
      segmentsForHost(sorted, host)
    }

    val ts = replicaTopology.toSeq

    if (includePending) {
      primaryOwnership(ts flatMap { t =>
        t.current ++ t.pending
      })
    } else {
      primaryOwnership(ts flatMap { _.current })
    }
  }

  private def segmentsForHost(s: Vector[SegmentOwnership], host: HostID) =
    OwnedSegment.segmentsForHost(SegmentOwnership.toOwnedSegments(s), host)

  def isReplicaForSegment(seg: Segment, host: HostID): Boolean =
    topologyForHostExists(host) { t =>
      t.currentCovering(seg) forall { _.host == host }
    }

  def isReplicaForLocation(
    loc: Location,
    host: HostID,
    includePending: Boolean): Boolean =
    topologyForHostExists(host) { t =>
      t.currentHost(loc) == host ||
      (includePending && !t.isStable && (t.pendingHost(loc) == host))
    }

  def hostsForRead(read: R): Set[HostID] = {
    val key = keyExtractor.readKey(read)
    replicas(keyLocator.locate(key), false)
  }

  def hostsForWrite(expr: W): Set[HostID] =
    hostSet(keyExtractor.writeKeys(expr), true)

  def coversRead(read: R): Boolean = {
    val host = replicaInfo.self
    topologyForHostExists(host) { t =>
      val k = keyExtractor.readKey(read)
      val l = keyLocator.locate(k)
      t.currentHost(l) == host
    }
  }

  def coversTxn(host: HostID, expr: W): Boolean =
    topologyForHostExists(host) { t =>
      val readsIter = keyExtractor.readKeysIterator(expr)
      (readsIter exists { k =>
        t.currentHost(keyLocator.locate(k)) == host
      }) || {
        val writes = keyExtractor.writeKeys(expr)
        writes exists { k =>
          val l = keyLocator.locate(k)
          (t.currentHost(l) == host) ||
          (!t.isStable && (t.pendingHost(l) == host))
        }
      }
    }

  def txnCovered(hosts: Set[HostID], expr: W): Boolean =
    keyExtractor.writeKeys(expr) forall { k =>
      val loc = keyLocator.locate(k)
      topology.replicaTopologies exists { case (_, rt) =>
        hosts contains rt.currentHost(loc)
      }
    }

  private def hostSet(keys: Iterable[K], pending: Boolean): Set[HostID] =
    keys.iterator flatMap { key =>
      replicas(keyLocator.locate(key), pending)
    } toSet

  private def topologyForHostExists(host: HostID)(
    f: ReplicaTopology => Boolean): Boolean =
    replicaInfo.hostInformation(host) flatMap { hi =>
      topology.getTopology(hi.replica)
    } exists f
}
