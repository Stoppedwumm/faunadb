package fauna.cluster.topology

import fauna.atoms._
import fauna.cluster.{
  ClusterService,
  ClusterServiceConfig,
  Membership,
  ServiceState
}
import fauna.cluster.ClusterService.LogAndState
import fauna.codex.cbor.CBOR
import fauna.exec.AsyncEventSource
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.syntax._
import fauna.net.bus.SignalID
import fauna.stats.StatsRecorder
import fauna.tx.transaction._
import fauna.tx.transaction.RevalidatorRole._
import scala.annotation.tailrec
import scala.concurrent.Future
import scala.util.Random

/** The rules for log topology changes:
  *
  * There must be at least one log node.
  * A single replica will have as many segments as there are nodes, but not more than MaxSegments.
  * Only nodes in data-and-log replicas shall participate in the log.
  * Nodes will be added to a log segment if a node from their replica is not already part of the segment.
  * New segments will be created when every replica has a node that can be assigned to it, up to MaxSegments.
  * A segment can only exist if it can be replicated to every replica.
  */
object LogTopologyCommand {
  implicit val segStateCodec = CBOR.SumCodec[SegmentState](
    CBOR.SingletonCodec(Uninitialized),
    CBOR.TupleCodec[Started],
    CBOR.TupleCodec[Closed]
  )

  implicit val segInfoCodec = CBOR.TupleCodec[SegmentInfo]
  implicit val codec = CBOR.SumCodec[LogTopologyCommand](
    CBOR.TupleCodec[ProposeTopology],
    CBOR.TupleCodec[Revalidate]
  )
}

sealed trait LogTopologyCommand

case class ProposeTopology(topology: Map[SegmentID, SegmentInfo], version: Long)
    extends LogTopologyCommand

case class Revalidate(validUntil: Epoch, version: Long) extends LogTopologyCommand

sealed trait LogTopologyState
    extends ServiceState[LogTopologyCommand, LogTopologyState] {
  def topology: Map[SegmentID, SegmentInfo]
  def validUntil: Epoch
  def version: Long
}

object LogTopologyState {
  implicit val infoCodec = LogTopologyCommand.segInfoCodec
  implicit val codec =
    CBOR.SumCodec[LogTopologyState](CBOR.TupleCodec[LogTopologyStateV1])

  def empty: LogTopologyState =
    LogTopologyStateV1(Map.empty, 0, SegmentID(-1), Epoch.MinValue)
}

case class LogTopologyStateV1(
  topology: Map[SegmentID, SegmentInfo],
  version: Long,
  maxSegID: SegmentID,
  // Segment information is only valid until a certain epoch. This epoch should be
  // in a reasonable future of the current overall transaction pipeline epoch.
  // Transaction
  // pipeline must not process batches from an epoch later than the validity of this
  // state.
  // Further, starting and ending of segments only take place in the instant after
  // the
  // current validity. To illustrate, let's suppose the validity is set to 1000
  // epochs in
  // the future. Any log node that notices it being less can update it. When a new
  // segment
  // is started, it will be started at validUntil + 1, making sure that all data
  // nodes will
  // observe it in time. Log nodes can only start publishing batches once their
  // segment's
  // start epoch is reached and must finish publishing batches no later than their
  // end epoch
  // is reached.
  validUntil: Epoch)
    extends LogTopologyState {

  def applyCmd(cmd: LogTopologyCommand): LogTopologyState =
    cmd match {
      case ProposeTopology(`topology`, `version`) => this
      case ProposeTopology(t, `version`) =>
        val maxSeg = t.foldLeft(maxSegID) { case (ms, (s, _)) => ms max s }
        copy(topology = t, maxSegID = maxSeg, version = version + 1)
      case Revalidate(validUntil, `version`) if validUntil > this.validUntil =>
        copy(validUntil = validUntil, version = version + 1)
      case _ => this
    }
}

object LogTopology {
  // Permit no more than this many log segments in a cluster.
  val MaxSegments = 3

  // Probabilistically allow this many substitute revalidators at any given time
  private[LogTopology] val SubstituteRevalidatorsAllowed = 50

  def apply(
    config: ClusterServiceConfig,
    logSID: SignalID,
    stateSID: SignalID,
    topologies: Topologies,
    membership: Membership,
    stats: StatsRecorder = StatsRecorder.Null) =
    new LogTopology(
      ClusterService[LogTopologyCommand].makeLogAndState(
        config,
        logSID,
        stateSID,
        "log_topology",
        LogTopologyState.empty,
        stats,
        Some(membership.logHandle),
        dedicatedBusStream = true),
      topologies,
      membership
    )

  def newProposal(
    prevTopo: Seq[(SegmentID, SegmentInfo)],
    logReplicas: Seq[(String, Seq[HostID])],
    leaving: Set[HostID],
    maxSegID: SegmentID,
    validUntil: Epoch): Seq[(SegmentID, SegmentInfo)] = {

    // Can have at most as many non-condemned segments as the smallest log replica,
    // with an overall limit of MaxSegments.
    val maxSegs =
      // Note that currently leaving hosts are considered in the size of the log
      // replica
      logReplicas.foldLeft(MaxSegments) { case (m, (_, hs)) => m min hs.size }

    val currDataTopo =
      for {
        (replica, hosts) <- logReplicas
        host             <- hosts
      } yield (replica, host)

    val noDeadHostsTopo = prevTopo map { case (seg, si) =>
      val liveHosts = si.hosts filter { h =>
        currDataTopo exists { case (_, h2) => h == h2 }
      }
      (seg, si.copy(hosts = liveHosts))
    } filter { case (_, si) => si.hosts.nonEmpty }

    val (notEndedTopo, endedTopo) = noDeadHostsTopo partition { case (_, si) =>
      !si.isEnded
    }
    // Only leave largest maxSegs segments alive
    val (liveTopo, stopTopo) =
      notEndedTopo.sortBy { case (_, si) => -si.hosts.size }.splitAt(maxSegs)

    // stop already started extra segments, and simply discard those that
    // weren't even started yet
    val killedTopo = stopTopo collect {
      case (seg, si) if si.isStarted => (seg, si.endAfter(validUntil + 1))
    }

    val topo = (liveTopo ++ killedTopo ++ endedTopo) sortBy { case (seg, _) => seg }

    val currLogTopo = for {
      (segment, SegmentInfo(hosts, _, _)) <- topo
      host                                <- hosts
      replica <- logReplicas collectFirst { case (r, hs) if hs contains host => r }
    } yield (segment, replica, host)

    var newHosts = currDataTopo filterNot { case (_, host) =>
      (currLogTopo exists { _._3 == host }) || leaving.contains(host)
    }

    // Remove dead nodes
    var newLogTopo = currLogTopo filter { case (_, _, host) =>
      currDataTopo exists { r =>
        r._2 == host
      }
    }

    // Add unassigned nodes to segments that are not closed and are not yet
    // represented in the node's replica
    newLogTopo = newHosts.foldLeft(newLogTopo) { case (ntopo, (replica, host)) =>
      // sorted for determinism
      val bySegment = (ntopo groupBy { _._1 } toSeq) filter { case (seg, _) =>
        topo exists { case (seg2, si) => seg == seg2 && !si.isEnded }
      } sortBy { _._1 }
      val firstSeg = bySegment collectFirst {
        case (seg, hs) if !(hs exists { _._2 == replica }) => seg
      }
      firstSeg.fold(ntopo) { segment =>
        newHosts = newHosts filterNot { _ == (replica -> host) }
        ntopo :+ ((segment, replica, host))
      }
    }

    // Add more segments if possible
    @tailrec
    def addSegment(
      logTopo: Seq[(SegmentID, String, HostID)],
      hosts: Seq[(String, HostID)],
      maxSegID: SegmentID
    ): Seq[(SegmentID, String, HostID)] = {
      val byRep = hosts groupBy { _._1 }
      val segCount = (logTopo map { _._1 } toSet).size
      def hasFreeNodeInEveryReplica =
        logReplicas forall { case (r, _) =>
          byRep contains r
        }

      if (segCount < maxSegs && hosts.nonEmpty && hasFreeNodeInEveryReplica) {
        val nextSegID = SegmentID(maxSegID.toInt + 1)
        val newSegHosts = logReplicas map { case (r, _) =>
          (nextSegID, r, byRep(r).head._2)
        }
        val nextLogTopo = logTopo ++ newSegHosts
        val filteredHosts = hosts filterNot { case (_, h) =>
          newSegHosts exists { _._3 == h }
        }
        addSegment(nextLogTopo, filteredHosts, nextSegID)
      } else {
        logTopo
      }
    }
    newLogTopo = addSegment(newLogTopo, newHosts, maxSegID)

    (newLogTopo groupBy { _._1 } map { case (s, hs) =>
      val newHosts = (hs map { _._3 } toVector).sorted
      val newInfo = topo collectFirst {
        case (seg, si) if seg == s => si.withHosts(newHosts)
      } getOrElse SegmentInfo(newHosts, Uninitialized, SegmentInfo.FirstInitRound)
      (s, newInfo)
    } toSeq) sortBy { _._1 }
  }
}

class LogTopology(
  logAndState: LogAndState[LogTopologyCommand, LogTopologyState],
  topologies: Topologies,
  membership: Membership)
    extends ClusterService[LogTopologyCommand, LogTopologyState](logAndState) {
  service =>

  override def start() = {
    super.start()
    LogNodeProviderImpl.start()
    setupListeners()
  }

  override def stop(graceful: Boolean): Unit = {
    super.stop(graceful)
    LogNodeProviderImpl.stop()
  }

  @volatile private[this] var clusterSize = membership.hosts.size

  private def setupListeners(): Unit = {
    def onChange: Future[Unit] = {
      clusterSize = membership.hosts.size
      // Only submit log topology proposals if this node is a log node.
      // This prevents a race condition where a recently joined node did
      // not catch up on either membership or topology so it tries to
      // enforce a log topology based on stale data.
      def seesSelf = membership.getReplica(membership.self).isDefined
      def selfInLogReplica =
        topologies.snapshot.logReplicaNames.contains(membership.replica)
      if (seesSelf && selfInLogReplica) {
        currState match {
          case LogTopologyStateV1(topo, version, _, _) =>
            // Another safeguard: does the current topology look like it could've
            // been
            // produced by a recent membership state? Basically, all nodes in current
            // log
            // topology known in membership, either as current members or known to
            // have
            // recently left. If topology contains hosts that membership knows
            // nothing about
            // then it's safe to presume membership is stale. (membership.leftHosts
            // keeps
            // information about removed hosts for 30 days)
            def isRecentTopo = {
              val knownHosts = membership.hosts ++ membership.leftHosts
              topo forall { case (_, SegmentInfo(hs, _, _)) =>
                hs forall knownHosts.contains
              }
            }
            if (isRecentTopo) {
              proposeTopology(topo, version) flatMap { changed =>
                if (changed) {
                  onChange
                } else {
                  Future.unit
                }
              }
            } else {
              Future.unit
            }
        }
      } else {
        Future.unit
      }
    }

    ClusterService.subscribeWithLogging(membership, topologies) {
      onChange map { _ =>
        isRunning
      }
    }
  }

  private[this] object LogNodeProviderImpl extends LogNodeProvider {
    @volatile private[this] var logNodeInfo = makeLogNodeInfo()
    val callbacks = new AsyncEventSource

    def start(): Unit =
      service.subscribeWithLogging {
        logNodeInfo = makeLogNodeInfo()
        callbacks.signal()
        if (isRunning) FutureTrue else FutureFalse
      }

    def stop(): Unit =
      callbacks.stop()

    private class LogNodeInfoImpl(
      state: LogTopologyState,
      val regionSegments: Map[RegionID, Set[SegmentID]])
        extends LogNodeInfo {
      def topology = state.topology
      def validUntil = state.validUntil
      def version = state.version
    }

    private[this] def makeLogNodeInfo(): LogNodeInfoImpl = {
      val state = currState
      new LogNodeInfoImpl(state, makeRegionSegments(state))
    }

    // FIXME: remove all of this unnecessary data locality support.
    private def makeRegionSegments(
      state: LogTopologyState): Map[RegionID, Set[SegmentID]] = {
      val map = state.topology.groupMap { case (_, _) =>
        // All segments are in the default region.
        Some(RegionID.DefaultID)
      } { case (sid, _) =>
        sid
      }

      map flatMap { case (k, v) =>
        k map { (_, v.toSet) }
      }
    }

    def subscribeWithLogging(cb: => Future[Boolean]) = {
      val fut = callbacks.subscribe(cb)
      fut onComplete logException
      fut
    }

    def getLogNodeInfo: LogNodeInfo = logNodeInfo

    def removeSegments(segs: Iterable[SegmentID], currVersion: Long): Future[Unit] =
      currState match {
        case LogTopologyStateV1(currTopo, _, _, _) =>
          proposeTopology(currTopo -- segs, currVersion).unit
      }

    def revalidate(validUntil: Epoch, currVersion: Long): Future[Unit] =
      currState match {
        case LogTopologyStateV1(_, version, _, currValidUntil) =>
          if (version == currVersion && currValidUntil < validUntil) {
            proposeAndApply(Revalidate(validUntil, version))
          } else {
            Future.unit
          }
      }

    def startSegment(seg: SegmentID, start: Epoch, currVersion: Long): Future[Unit] =
      currState match {
        case LogTopologyStateV1(currTopo, version, _, validUntil) =>
          if (version == currVersion && start > validUntil) {
            currTopo.get(seg).fold(Future.unit) { si =>
              if (!si.isStarted) {
                proposeAndApply(
                  ProposeTopology(
                    currTopo.updated(seg, si.copy(state = Started(start))),
                    version))
              } else {
                Future.unit
              }
            }
          } else {
            Future.unit
          }
      }

    def closeSegment(seg: SegmentID, currVersion: Long): Epoch = {
      val LogTopologyStateV1(currTopo, version, _, validUntil) = currState
      if (version != currVersion) {
        throw new IllegalStateException(
          s"current log topology version $version != change version $currVersion")
      }
      val endEpoch = validUntil + 1
      currTopo.get(seg) match {
        case None => throw new IllegalStateException(s"no such segment $seg")
        case Some(si) =>
          if (currTopo.size == 1)
            throw new IllegalStateException("cannot close the only segment")
          si.state match {
            case Started(_) =>
              proposeAndApply(
                ProposeTopology(
                  currTopo.updated(seg, si.endAfter(endEpoch)),
                  version))
            case _ =>
              throw new IllegalStateException("cannot close segment that isn't open")
          }
      }
      endEpoch
    }

    def moveSegment(
      seg: SegmentID,
      from: HostID,
      to: HostID,
      currVersion: Long): Epoch = {
      val LogTopologyStateV1(currTopo, version, _, validUntil) = currState
      if (version != currVersion) {
        throw new IllegalStateException(
          s"current log topology version $version != change version $currVersion")
      }
      val endEpoch = validUntil + 1
      currTopo.get(seg) match {
        case None => throw new IllegalStateException(s"no such segment $seg")
        case Some(si) =>
          if (si.hosts.find(_ == from).isEmpty)
            throw new IllegalStateException(s"no host $from present in segment")
          val (fromR, toR) = (membership.getReplica(from), membership.getReplica(to))
          if (fromR != toR)
            throw new IllegalStateException(
              s"from and to hosts not in same replica: from in $fromR but to in $toR")
          // Short-circuit no-op moves.
          if (from != to) {
            si.state match {
              case Started(_) =>
                proposeAndApply(
                  ProposeTopology(
                    currTopo
                      .updated(
                        seg,
                        si.withHosts(si.hosts.filterNot(_ == from) :+ to)),
                    version))
              case _ =>
                throw new IllegalStateException(
                  "cannot close segment that isn't open")
            }
          }
      }
      endEpoch
    }

    def getRevalidatorRole: RevalidatorRole =
      if (replog.isLeader) {
        MainRevalidator
      } else if (
        Random.nextInt(clusterSize) < LogTopology.SubstituteRevalidatorsAllowed
      ) {
        SubstituteRevalidator
      } else {
        NotARevalidator
      }
  }

  val logNodeProvider: LogNodeProvider = LogNodeProviderImpl

  private def proposeTopology(
    topo: Map[SegmentID, SegmentInfo],
    version: Long): Future[Boolean] =
    currState match {
      case LogTopologyStateV1(currTopo, currVersion, maxSegID, validUntil) =>
        if (currVersion == version) {
          val replicas = topologies.snapshot.logReplicaNames.sorted map { r =>
            (r, membership.hostsInReplica(r).toSeq.sorted)
          }
          val sortedTopo = topo.toVector.sortBy { _._1 }
          val proposedTopo = LogTopology
            .newProposal(
              sortedTopo,
              replicas,
              membership.leavingHosts,
              maxSegID,
              validUntil)
            .toMap

          if (proposedTopo != currTopo) {
            proposeAndApply(ProposeTopology(proposedTopo, version)) flatMap { _ =>
              FutureTrue
            }
          } else {
            FutureFalse
          }
        } else {
          FutureFalse
        }
    }
}
