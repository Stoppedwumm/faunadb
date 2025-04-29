package fauna.cluster.topology

import fauna.atoms._
import fauna.cluster.ClusterService.LogAndState
import fauna.cluster._
import fauna.codex.cbor.CBOR
import fauna.lang.syntax._
import fauna.net.bus.SignalID
import fauna.stats.StatsRecorder
import java.math.BigInteger
import scala.concurrent.Future

object TopologyCommand {
  implicit val codec = CBOR.SumCodec[TopologyCommand](
    CBOR.DefunctCodec(Unused1),
    CBOR.TupleCodec[UpdateCurrentTopology],
    CBOR.DefunctCodec(Unused2),
    CBOR.DefunctCodec(Unused3),
    CBOR.TupleCodec[SetPendingTopology],
    CBOR.TupleCodec[SetReplicas]
  )
}

sealed trait TopologyCommand

// Placeholders for no longer used commands in SumCodec
case object Unused1 extends TopologyCommand
case object Unused2 extends TopologyCommand
case object Unused3 extends TopologyCommand

/**
  * Sets a new pending topology for a replica.
  * @param replica the replica
  * @param pending the new pending topology
  * @param prevPending the previous version of the pending topology that this pending
  *                       topology replaces.
  * @param reusable tokens that can be reused for newly joining nodes
  */
case class SetPendingTopology(
  replica: String,
  pending: Vector[SegmentOwnership],
  prevPending: Vector[SegmentOwnership],
  reusable: Vector[Location])
    extends TopologyCommand {
  ReplicaTopology.validateRing(pending)
}

// Updates the current topology with new segment ownerships. When a host is done
// assuming ownership for a range based on pending topology, it
// should update the current topology. This will signify that it should become
// the read authority for those segments in its replica.
case class UpdateCurrentTopology(
  replica: String,
  segments: Vector[Segment],
  hostID: HostID)
    extends TopologyCommand

case class SetReplicas(
  newReplicaNames: Vector[String],
  newLogReplicaNames: Vector[String],
  oldReplicaNames: Vector[String],
  oldLogReplicaNames: Vector[String])
    extends TopologyCommand

sealed trait TopologyState extends ServiceState[TopologyCommand, TopologyState] {
  def getTopology(replica: String): Option[ReplicaTopology]
  def replicaTopologies: Seq[(String, ReplicaTopology)]
  def logReplicaNames: Seq[String]
  def version: Long

  final def replicaNames: Seq[String] = replicaTopologies map { _._1 }

  /**
    * Returns true if at least one of replicas named in "replicaNames" has all
    * of its segments currently owned by hosts specified in "members". When
    * "members" are the current members of the cluster, such replica is
    * considered "whole", meaning all of its segments are owned by a cluster
    * member (that is, no segments are owned by a non-member, presumably
    * recently removed node).
    */
  final def hasWholeReplicaAmong(
    replicaNames: Set[String],
    members: Set[HostID]): Boolean =
    replicaTopologies exists {
      case (name, t) =>
        replicaNames.contains(name) && (t.current forall { so =>
          members.contains(so.host)
        })
    }
}

object TopologyState {
  implicit val replicaTopologyV1Codec = CBOR.TupleCodec[ReplicaTopologyV1]
  implicit val codec = CBOR.SumCodec[TopologyState](
    CBOR.TupleCodec[TopologyStateV1],
    CBOR.TupleCodec[TopologyStateV2],
    CBOR.TupleCodec[TopologyStateV3]
  )
  def empty: TopologyState = TopologyStateV3(Map.empty, Vector.empty, 0L)

  // Checks a replication specification for internal consistency.
  private[cluster] def checkValidReplicationSpec(dataReplicaNames: Seq[String], logReplicaNames: Seq[String]) = {
    if (logReplicaNames.isEmpty) {
      throw new IllegalArgumentException(s"Cluster must have at least one data+log replica.")
    }

    val logReplicasByRegion = logReplicaNames.groupBy { RegionName.fromReplicaName(_) }

    if (!logReplicasByRegion.contains(RegionName.DefaultName)) {
      throw new IllegalArgumentException(s"${RegionName.DefaultName.displayName.capitalize} must have at least one data+log replica.")
    }

    val dataReplicasByRegion = dataReplicaNames.groupBy { RegionName.fromReplicaName(_) }
    val regions = dataReplicasByRegion.keySet union logReplicasByRegion.keySet

    regions foreach { regionName =>
      val logReplicasInRegion = logReplicasByRegion.getOrElse(regionName, Seq.empty)
      if (logReplicasInRegion.isEmpty) {
        throw new IllegalArgumentException(s"${regionName.displayName.capitalize} must have at least one data+log replica.")
      } else {
        val dataReplicasInRegion = dataReplicasByRegion.getOrElse(regionName, Seq.empty)
        if (logReplicasInRegion exists { !dataReplicasInRegion.contains(_) }) {
          throw new IllegalArgumentException(s"data+log replicas in ${regionName.displayName} must be a subset of data replicas.")
        }
      }
    }
  }

  def findAnyLoglessRegion(logReplicaNames: Set[String], newLogReplicaNames: Set[String]): Option[RegionName] = {
    val existingLogRegions = logReplicaNames map { RegionName.fromReplicaName(_) }
    val newLogRegions = newLogReplicaNames map { RegionName.fromReplicaName(_) }
    (existingLogRegions diff newLogRegions).headOption
  }
}

// "V1" classes are only required to deserialize FaunaDB 2.5 persisted topology states
// If we're certain they're no longer in use anywhere, we can remove these.
case class ReplicaTopologyV1(
  current: Vector[SegmentOwnership],
  pending: Vector[SegmentOwnership])

case class TopologyStateV1(replicas: Map[String, ReplicaTopologyV1], version: Long)
    extends TopologyState {
  private[this] val stateV2 = {
    val replicasV2 = replicas map {
      case (n, ReplicaTopologyV1(c, p)) =>
        (n, ReplicaTopology(c, p, Vector.empty))
    }
    TopologyStateV2(replicasV2, version)
  }

  def applyCmd(cmd: TopologyCommand) = stateV2.applyCmd(cmd)
  def getTopology(replica: String) = stateV2.getTopology(replica)
  def replicaTopologies = stateV2.replicaTopologies
  def logReplicaNames = stateV2.logReplicaNames
}

case class TopologyStateV2(replicas: Map[String, ReplicaTopology], version: Long)
    extends TopologyState {

  private[this] val stateV3 =
    TopologyStateV3(replicas, replicas.keySet.toVector, version)

  def applyCmd(cmd: TopologyCommand) = stateV3.applyCmd(cmd)
  def getTopology(replica: String) = stateV3.getTopology(replica)
  def replicaTopologies = stateV3.replicaTopologies
  def logReplicaNames = stateV3.logReplicaNames
}

case class TopologyStateV3(replicas: Map[String, ReplicaTopology], logReplicaNames: Vector[String], version: Long)
    extends TopologyState {

  def applyCmd(cmd: TopologyCommand): TopologyState =
    cmd match {
      case SetReplicas(newReplicaNames, newLogReplicaNames, oldReplicaNames, oldLogReplicaNames) =>
        setReplicas(newReplicaNames, newLogReplicaNames, oldReplicaNames, oldLogReplicaNames)

      case SetPendingTopology(replica, pending, prevPending, reusable) =>
        withExistingReplica(replica, cmd) { topology =>
          if (topology.pending == prevPending) {
            copy(replicas = replicas.updated(
                   replica,
                   topology.copy(pending = pending, reusable = reusable)),
                 version = version + 1)
          } else {
            this
          }
        }

      case UpdateCurrentTopology(replica, segments, hostID) =>
        withExistingReplica(replica, cmd) { topology =>
          segments.foldLeft(this) { (t, s) =>
            t.copy(replicas =
                     replicas.updated(replica, topology.updateCurrent(s, hostID)),
                   version = version + 1)
          }
        }

      case Unused1 | Unused2 | Unused3 =>
        throw new AssertionError(cmd)
    }

  private def withExistingReplica(replica: String, cmd: TopologyCommand)(
    f: ReplicaTopology => TopologyStateV3) =
    getTopology(replica) match {
      case Some(topology) =>
        f(topology)
      case None =>
        getLogger.error(s"Received $cmd for non-existent replica")
        this
    }

  private def isValidReplicationSpec(replicaNames: Seq[String], logReplicaNames: Seq[String]) =
    try {
      TopologyState.checkValidReplicationSpec(replicaNames, logReplicaNames)
      true
    } catch {
      case _: IllegalArgumentException => false
    }

  private def setReplicas(
    newReplicaNames: Vector[String],
    newLogReplicaNames: Vector[String],
    oldReplicaNames: Vector[String],
    oldLogReplicaNames: Vector[String]) = {

    val current = replicas.keySet
    val currentLog = logReplicaNames.toSet
    val newLog = newLogReplicaNames.toSet

    val isStaleCommand =
      (oldReplicaNames.toSet != current) ||
      (oldLogReplicaNames.toSet != currentLog)

    def isIdempotentCommand =
      (newReplicaNames.toSet == current && newLog == currentLog)

    // Check if we need to ignore this SetReplicas command coming from the log.
    // If it's either stale, or it is internally inconsistent, or it results in
    // any existing region not having at least one log replica, we can't apply
    // it. We also skip applying it if it is idempotent, so we don't step the
    // version number unnecessarily.
    if (isStaleCommand ||
        !isValidReplicationSpec(newReplicaNames, newLogReplicaNames) ||
        isIdempotentCommand ||
        TopologyState.findAnyLoglessRegion(currentLog, newLog).isDefined) {
      this
    } else {
      val b = Map.newBuilder[String, ReplicaTopology]
      newReplicaNames foreach { r =>
        b += (r -> replicas.getOrElse(r, ReplicaTopology.Empty))
      }
      copy(replicas = b.result(), logReplicaNames = newLogReplicaNames, version = version + 1)
    }
  }

  def getTopology(replica: String) = replicas.get(replica)

  lazy val replicaTopologies = replicas.toSeq
}

object Topologies {

  def apply(
    config: ClusterServiceConfig,
    logSID: SignalID,
    stateSID: SignalID,
    membership: Membership,
    stats: StatsRecorder = StatsRecorder.Null) = {
    val logAndState = ClusterService[TopologyCommand].makeLogAndState(
      config,
      logSID,
      stateSID,
      "topology",
      TopologyState.empty,
      stats,
      Some(membership.logHandle))
    new Topologies(logAndState, membership)
  }

  def balancedReplicaSizes(hosts: Set[HostID], live: Set[HostID]): Map[HostID, BigInteger] = {
    // Add live hosts ownership, spread rounding when number of hosts is not a
    // multiple of ring length.
    val liveSizes = live.toSeq.sorted.foldLeft(Map.empty[HostID, BigInteger]) { (sizes, host) =>
      val allocatedSoFar = sizes.foldLeft(BigInteger.ZERO) { case(sum, (_, size)) => sum.add(size) }
      val nextAllocated = Segment.RingLength.multiply(BigInteger.valueOf(sizes.size + 1)).divide(BigInteger.valueOf(live.size))
      sizes.updated(host, nextAllocated.subtract(allocatedSoFar))
    }
    // Add leaving hosts with zero target ownership
    hosts.diff(live).foldLeft(liveSizes) { (sizes, host) => sizes.updated(host, BigInteger.ZERO) }
  }
}

sealed class Topologies(
  logAndState: LogAndState[TopologyCommand, TopologyState],
  membership: Membership)
    extends ClusterService[TopologyCommand, TopologyState](logAndState) {

  def snapshot = currState

  /**
    * Declares to the cluster that a replica should switch to a new topology.
    * When this proposal gets committed, hosts in the replica will start
    * repartitioning the data to conform to the new topology.
    * @param replicaName the name of the replica
    * @param topology the new topology
    * @param prevTopology previous topology
    * @param reusable reusable tokens
    * @return a future that completes when the proposal was committed and applied locally.
    */
  def proposeTopology(
    replicaName: String,
    topology: Seq[SegmentOwnership],
    prevTopology: Seq[SegmentOwnership],
    reusable: Vector[Location]) =
    currState.getTopology(replicaName) match {
      case Some(t) if t.pending == prevTopology =>
        proposeAndApply(SetPendingTopology(replicaName,
                                           topology.toVector,
                                           prevTopology.toVector,
                                           reusable))
      case _ =>
        Future.unit
    }

  def setReplicas(
    newDataReplicaNames: Seq[String],
    newLogReplicaNames: Seq[String],
    oldDataReplicaNames: Seq[String],
    oldLogReplicaNames: Seq[String]): Future[Unit] = {

    TopologyState.checkValidReplicationSpec(newDataReplicaNames, newLogReplicaNames)

    // All specified replicas must exist and must have non-leaving members.
    val existingReplicas = membership.replicaNames
    newDataReplicaNames foreach { rn =>
      if (!existingReplicas.contains(rn)) {
        throw new IllegalArgumentException(s"Replica $rn does not exist.")
      } else if (membership.liveHostsInReplica(rn).isEmpty) {
        throw new IllegalArgumentException(s"All nodes in replica $rn are leaving. It can't be a data replica.")
      }
    }

    val ts = currState

    // Can't make a region log-less once it had a log replica. By extension, can't
    // drop it entirely either (for now). NOTE: we enforce this instead of
    // "all regions must have a log replica" to allow for progressing the state
    // even in the unlikely case of two regions being added concurrently.
    // FIXME: we need to figure out how to allow removal of regions.
    // Note that topology state change also checks this criteria
    TopologyState.findAnyLoglessRegion(ts.logReplicaNames.toSet, newLogReplicaNames.toSet) foreach { regionName =>
      throw new IllegalArgumentException(s"${regionName.displayName.capitalize} must have at least one data+log replica.")
    }

    // Can't go from having at least one whole replica to not having one in any region.
    val members = membership.hosts
    val dataReplicaNamesSet = ts.replicaNames.toSet
    val newDataReplicaNamesSet = newDataReplicaNames.toSet
    membership.regions foreach { case (regionName, _) =>
      val existingDataReplicasInRegion =
        dataReplicaNamesSet filter regionName.containsReplica
      val newDataReplicasInRegion =
        newDataReplicaNamesSet filter regionName.containsReplica

      val isPotentialDataLoss =
        ts.hasWholeReplicaAmong(existingDataReplicasInRegion, members) &&
        !ts.hasWholeReplicaAmong(newDataReplicasInRegion, members)

      if (isPotentialDataLoss) {
        throw new IllegalArgumentException(
          s"Changing replica configuration would likely result in data loss in ${regionName.displayName}.")
      }
    }

    proposeAndApply(SetReplicas(
      newDataReplicaNames.toVector,
      newLogReplicaNames.toVector,
      oldDataReplicaNames.toVector,
      oldLogReplicaNames.toVector
    ))
  }

  /**
    * Declares to the cluster that particular segments are now fully owned by this host.
    * @param segments the segment now owned
    * @return a future that completes when the change was committed and applied locally.
    *         The caller should check upon completion if the change took effect. It could
    *         have been partially applied if the ownership of some segment is inconsistent
    *         with the pending ring.
    */
  def announceOwnership(segments: Seq[Segment]) =
    if (segments.isEmpty) {
      Future.unit
    } else {
      proposeAndApply(
        UpdateCurrentTopology(membership.replica,
                              segments.toVector,
                              membership.self))
    }

  /**
    * Declares that the particular segments have been orphaned by the
    * forced removal of a host from the given replica. These segments
    * must now be reassigned.
    */
  def announceOrphans(replica: String, segments: Vector[Segment]): Future[Unit] =
    if (segments.isEmpty) {
      Future.unit
    } else {
      proposeAndApply(
        UpdateCurrentTopology(replica,
                              segments,
                              HostID.NullID))
    }

  def getFirstClaimedIntersection(
    seg: Segment,
    replicaName: String): Option[OwnedSegment] =
    currState.getTopology(replicaName) flatMap { t =>
      t.currentCovering(seg) find { _.claimed } flatMap {
        case OwnedSegment(oseg, host) =>
          seg intersect oseg map { OwnedSegment(_, host) }
      }
    }

  def getUnclaimedParts(seg: Segment): Seq[Segment] = {
    // All segments covering seg in all replicas in the region
    val covering = currState.replicaTopologies flatMap {
      case (_, t) => t.currentCovering(seg)
    }
    // Only take segments that are claimed
    val claimed = covering filter { _.claimed }
    // subtract all claimed segments from seg, what's left are its unclaimed subsegments
    claimed.foldLeft(Seq(seg)) { (m, s) => // minuend, subtrahend
      m flatMap { _ diff s.segment }
    }
  }

  def isStable(replicaName: String): Option[Boolean] =
    currState.getTopology(replicaName) map { _.isStable }

  def pendingStatus: String = {
    val status = pendingStatusVerbose
    case class AggregateStats(locations: BigInteger, segments: Int) {
      def add(stats: PendingHostStats) =
        AggregateStats(locations.add(stats.locations), segments + stats.segments)
    }

    val aggStats = status.foldLeft(AggregateStats(BigInteger.ZERO, 0)) {
      case (agg, (_, statsMap)) =>
        statsMap.values.foldLeft(agg) { (agg2, stats) =>
          agg2.add(stats)
        }
    }
    status.size match {
      case 0 =>
        "No data movement is currently in progress."
      case r =>
        val ringPercentage = Segment.ringFraction(aggStats.locations) * 100 / r
        val hosts = status.foldLeft(0) { (s, m) =>
          s + m._2.size
        }
        val pct = "%4.2f%%".format(ringPercentage)
        s"$pct of tokens in ${aggStats.segments} segments moving to $hosts nodes in $r replicas."
    }
  }

  private case class PendingHostStats(locations: BigInteger, segments: Int)

  private def pendingStatusVerbose: Seq[(String, Map[HostID, PendingHostStats])] = {
    val missing = snapshot.replicaTopologies map {
      case (name, t) => (name, t.missingSegments)
    }
    missing flatMap {
      case (name, m) =>
        if (m.nonEmpty) {
          val perHostStats = m.groupBy { _.host } map {
            case (hostID, segs) =>
              val stats = segs.foldLeft(PendingHostStats(BigInteger.ZERO, 0)) {
                (stats, seg) =>
                  stats.copy(stats.locations.add(seg.segment.length),
                             stats.segments + 1)
              }
              (hostID, stats)
          }
          Vector((name, perHostStats))
        } else {
          Vector.empty
        }
    }
  }

  // Returns a map with assigned total segment ownership for all nodes in the
  // replica for a balanced topology. Evenly splits the ring size among all live
  // hosts in the replica and assigns zero ownership to all leaving hosts.
  private[this] def balancedReplicaSizes(replicaName: String): Map[HostID, BigInteger] = {
    val hosts = membership.hostsInReplica(replicaName)
    val live = membership.liveHostsInReplica(replicaName)
    Topologies.balancedReplicaSizes(hosts, live)
  }

  // Calculates the balanced version of the pending topology for the specified
  // replica, based on the current value of pending topology, but modified to
  // have the total owned segment sizes perfectly distributed across nodes
  // (some nodes might receive 1 extra token to account for rounding in case the
  // number of nodes is not an power of 2.) It uses a greedy algorithm that
  // strives to make the minimal number of modifications to the topology needed
  // for rebalancing.
  def balancedTopology(replicaName: String): Option[Vector[SegmentOwnership]] =
    currState.getTopology(replicaName) map { rt =>
      val balancedSizes = balancedReplicaSizes(replicaName)
      val balancedTopo = OwnedSegment.balance(balancedSizes,
        SegmentOwnership.toOwnedSegments(
          SegmentOwnership.normalize(rt.pending)))

      // Renormalize to merge any adjacent segments assigned to the same node. We could
      // do that in the OwnedSegment.balance method too but it's easier to run it
      // in a single pass afterwards than complicate its algorithm.
      val normalizedBalancedTopo =
      SegmentOwnership.toOwnedSegments(
        SegmentOwnership.normalize(
          OwnedSegment.toSegmentOwnership(balancedTopo)))

      // Re-segment the normalized balanced topology, so it can be used as a new pending.
      val segmentedTopo = OwnedSegment.increaseSegmentation(normalizedBalancedTopo, Location.PerHostCount)

      // Sanity checks
      OwnedSegment.ensureValidTopology(segmentedTopo)
      val finalSizes = segmentedTopo groupBy { _.host } map { case (host, segs) =>
        (host, Segment.sumLength(segs map { _.segment }))
      }

      // We ended up with the sizes we requested
      require(finalSizes forall { case (host, size) => balancedSizes.get(host) contains size })

      require(balancedSizes forall {
        case (host, size) if size.compareTo(BigInteger.ZERO)  > 0 => finalSizes.get(host) contains size
        case (host, size) if size.compareTo(BigInteger.ZERO) == 0 => !finalSizes.contains(host)
        case _ => false
      })

      val newPending = OwnedSegment.toSegmentOwnership(segmentedTopo)

      // More sanity checks, this time on SegmentOwnership converted representation
      val excessivelySegmented = newPending groupBy { _.host } collect {
        case (host, s) if s.size > Location.PerHostCount => host
      }

      require(excessivelySegmented.isEmpty,
        s"Can not rebalance without having excess segments for $excessivelySegmented")

      // All good
      newPending
    }

  // Used only by testing
  def getCurrentCovering(seg: Segment): Seq[OwnedSegment] =
    myTopology.get.currentCovering(seg)

  private def myTopology =
    currState.getTopology(membership.replica)
}
