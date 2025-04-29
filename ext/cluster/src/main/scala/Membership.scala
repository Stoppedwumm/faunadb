package fauna.cluster

import fauna.atoms._
import fauna.cluster.ClusterService.LogAndState
import fauna.cluster.Membership.logger
import fauna.codex.cbor.CBOR
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.exec.Timer
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.net._
import fauna.net.bus._
import fauna.stats.StatsRecorder
import fauna.storage.ReplicaNameValidator
import fauna.tx.consensus._
import io.netty.util.Timeout
import scala.collection.mutable.{ Map => MMap }
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.Random

object MembershipCommand {
  implicit val codec = CBOR.SumCodec[MembershipCommand](
    CBOR.TupleCodec[IntroduceHost],
    CBOR.TupleCodec[RemoveHost],
    CBOR.TupleCodec[ForgetHosts],
    CBOR.TupleCodec[HostIsLeaving]
  )
}

/** Base class for all command messages used in the replicated log.
  */
sealed abstract class MembershipCommand

/** This message is written to the log by a node that receives a Hello request
  * from a new node introducing itself. It is also written to the log by a node
  * that needs to update its address or replica name. When reading this
  * message, the state machine will update its mapping of known hosts and
  * register the introduced host's HostID with its HostAddress with the message
  * bus.
  */
case class IntroduceHost(introduction: HostIntroduction, hostAddress: HostAddress)
    extends MembershipCommand

/** This message is written to the log to inform the cluster that a node has
  * been removed from the cluster.
  * @param hostID the ID of the host that was removed.
  * @param when the timestamp when the host was removed.
  */
case class RemoveHost(hostID: HostID, when: Timestamp) extends MembershipCommand

/** This message is written to the log to inform the cluster that a node identified
  * with hostID is ready to forget hosts that were removed up to and including "until".
  * @param hostID the ID of the host publishing its forget threshold.
  * @param until the host's forget threshold.
  */
case class ForgetHosts(hostID: HostID, until: Timestamp) extends MembershipCommand

/** This message is written to the log to inform the cluster that a node has
  * been marked as leaving in the cluster
  * @param hostID the ID of the host that is leaving.
  */
case class HostIsLeaving(hostID: HostID) extends MembershipCommand

sealed trait MembershipState
    extends ServiceState[MembershipCommand, MembershipState] {
  def knownHosts: Map[HostID, HostInformation]
  def graveyard: Vector[Tombstone]
  def forgetThreshold(hostID: HostID): Option[Timestamp]
  def regions: Map[RegionName, RegionID]

  final def isKnownDead(hostID: HostID) =
    graveyard exists { _.host == hostID }

  final def replicaCompatible(hostID: HostID, replica: String) =
    knownHosts.get(hostID) forall { _.replica == replica }

  final def informationMatches(hostID: HostID, hostName: String, replica: String) =
    knownHosts.get(hostID) exists {
      case HostInformation(addr, rep, _)
          if addr.hostName == hostName && rep == replica =>
        true
      case _ => false
    }
}

object MembershipState {
  implicit val hostInformationCodec = CBOR.RecordCodec[HostInformation]
  implicit val tombstoneCodec = CBOR.RecordCodec[Tombstone]
  implicit val codec = CBOR.SumCodec[MembershipState](
    CBOR.TupleCodec[MembershipStateV1],
    CBOR.TupleCodec[MembershipStateV2]
  )
  def empty: MembershipState =
    MembershipStateV2(
      Map.empty,
      Vector.empty,
      Map.empty,
      Map(RegionName.DefaultName -> RegionID.DefaultID),
      RegionID.DefaultID)
}

case class TombstoneExpirySpecification(
  expiry: Duration = 30.days,
  scanPeriod: Duration = 1.days) {
  def currentExpiry = Clock.time - expiry
}

object Membership {
  def apply(
    config: ClusterServiceConfig,
    logSID: SignalID,
    stateSID: SignalID,
    helloSID: SignalID,
    replica: String,
    stats: StatsRecorder = StatsRecorder.Null)(
    implicit tes: TombstoneExpirySpecification = TombstoneExpirySpecification()) = {

    new Membership(
      ClusterService[MembershipCommand].makeLogAndState(
        config,
        logSID,
        stateSID,
        "membership",
        MembershipState.empty,
        stats),
      config.bus,
      helloSID,
      replica)(tes)
  }

  val logger = getLogger()
}

/** In-memory representation of clusterwide known data about a host
  * @param hostAddress the host's address:port
  * @param replica the name of the replica the host belongs to
  * @param leaving if true, node is leaving. This means it should not own any
  *                segments and once all its segments are taken over, it should
  *                be removed from cluster.
  */
// FIXME: port is now ignored... how do we switch to just encoding hostname?
case class HostInformation(
  hostAddress: HostAddress,
  replica: String,
  leaving: Boolean = false) {
  def hostName = hostAddress.hostName
}

case class Tombstone(host: HostID, when: Timestamp, info: HostInformation)
    extends Ordered[Tombstone] {
  def compare(other: Tombstone): Int = when compare other.when
  def notNewerThan(expiry: Timestamp) = when <= expiry
}

case class MembershipStateV1(
  knownHosts: Map[HostID, HostInformation],
  graveyard: Vector[Tombstone],
  forgets: Map[HostID, Timestamp])
    extends MembershipState {
  val regions = Map(RegionName.DefaultName -> RegionID.DefaultID)

  def applyCmd(cmd: MembershipCommand): MembershipState =
    MembershipStateV2(knownHosts, graveyard, forgets, regions, regions.values.max)
      .applyCmd(cmd)

  override def forgetThreshold(hostID: HostID) = forgets.get(hostID)
}

case class MembershipStateV2(
  knownHosts: Map[HostID, HostInformation],
  graveyard: Vector[Tombstone],
  forgets: Map[HostID, Timestamp],
  regions: Map[RegionName, RegionID],
  maxRegionID: RegionID)
    extends MembershipState {

  def applyCmd(cmd: MembershipCommand): MembershipStateV2 =
    cmd match {
      case IntroduceHost(HostIntroduction(hostID, replica), hostAddress) =>
        if (!isKnownDead(hostID) && replicaCompatible(hostID, replica)) {
          val leaving = knownHosts.get(hostID) exists { _.leaving }
          val regionName = RegionName.fromReplicaName(replica)
          val newRegions = Map(regionName -> maxRegionID.next) ++ regions
          val newMaxRegionID = maxRegionID max newRegions.values.max
          copy(
            knownHosts = knownHosts.updated(
              hostID,
              HostInformation(hostAddress, replica, leaving)),
            forgets =
              if (forgets.contains(hostID)) forgets
              else forgets.updated(hostID, Timestamp.Epoch),
            regions = newRegions,
            maxRegionID = newMaxRegionID
          )
        } else {
          this
        }
      case RemoveHost(hostID, when) =>
        knownHosts.get(hostID).fold(this) { hi =>
          val regionName = RegionName.fromReplicaName(hi.replica)
          val newKnownHosts = knownHosts - hostID
          val newRegions =
            if (
              regionName.isDefault || newKnownHosts.values.exists { hi2 =>
                regionName.containsReplica(hi2.replica)
              }
            ) {
              regions
            } else {
              regions - regionName
            }
          copy(
            knownHosts = newKnownHosts,
            graveyard =
              (graveyard :+ Tombstone(hostID, when, knownHosts(hostID))).sorted,
            regions = newRegions
          ).updateForgets(forgets - hostID)
        }
      case ForgetHosts(hostID, until) =>
        if (knownHosts.contains(hostID)) {
          updateForgets(forgets.updated(hostID, until))
        } else {
          this
        }
      case HostIsLeaving(hostID) =>
        knownHosts.get(hostID) match {
          case Some(hi) if !hi.leaving =>
            copy(knownHosts = knownHosts.updated(hostID, hi.copy(leaving = true)))
          case _ =>
            this
        }
    }

  override def forgetThreshold(hostID: HostID) = forgets.get(hostID)

  private def updateForgets(newForgets: Map[HostID, Timestamp]) = {
    // quorum threshold, to protect against lone nodes with misconfigured clocks
    val threshold = newForgets.values.toSeq sortWith { (a, b) =>
      a > b
    } drop (newForgets.size / 2) headOption

    copy(
      forgets = newForgets,
      graveyard = threshold.fold(graveyard) { t =>
        graveyard dropWhile { _.notNewerThan(t) }
      })
  }
}

trait ReplicaInfo {
  def self: HostID
  def replicaNames: Set[String]
  def hostInformation(host: HostID): Option[HostInformation]
}

class Membership(
  logAndState: LogAndState[MembershipCommand, MembershipState],
  bus: MessageBus,
  helloSignalID: SignalID,
  val replica: String)(
  implicit tes: TombstoneExpirySpecification = TombstoneExpirySpecification())
    extends ClusterService[MembershipCommand, MembershipState](logAndState)
    with ReplicaInfo {

  logger.info(s"Membership Service: members on startup are: $hosts")

  @volatile private[this] var nextTombstoneScan: Option[Timeout] = None

  private[this] val helloHandler = Hello.makeHandler(bus, helloSignalID) {
    (from, intro, deadline) =>
      if (isMember) {
        state.sync(deadline) flatMap { _ =>
          introduceHost(intro, from.address.hostName, deadline)
        } map { _ =>
          val hostID = intro.hostID
          val cs = currState
          if (cs.informationMatches(hostID, from.address.hostName, intro.replica)) {
            IntroductionAccepted(knownHosts map { case (id, hi) =>
              (id, hi.hostAddress)
            } toVector)
          } else if (!cs.replicaCompatible(hostID, intro.replica)) {
            WrongReplica(cs.knownHosts.get(hostID) map {
              _.replica
            } getOrElse "<unknown>")
          } else if (cs.isKnownDead(hostID)) {
            AlreadyRemoved
          } else {
            // For completeness sake. Shouldn't happen.
            UnexpectedError("Host was introduced but was not found in the state.")
          }
        }
      } else {
        Future.successful(NotMember)
      }
  }

  def logHandle = ReplicatedLog.handle(replog)

  override def start() = {
    // We'll subscribe to our own changes to manage HostID->HostAddress registration
    // with the bus.
    // We need to do it once before start() to register already known persisted
    // HostAddress mappings.
    onChange
    super.start()
    // Subscribe to our own changes to manage future HostAddress mappings
    subscribeWithLogging(onChange)
    scanTombstones()
  }

  override def stop() = {
    nextTombstoneScan foreach { _.cancel() }
    helloHandler.close()
    super.stop()
  }

  private def onChange: Future[Boolean] = {
    val state = currState
    val addrs = MMap.empty[HostAddress, HostID]

    // Adds tombstones in time order. Address collisions will ensure
    // that the latest mapping will be preserved.
    state.graveyard foreach { case Tombstone(id, _, HostInformation(addr, _, _)) =>
      addrs += (addr -> id)
    }

    // Add live hosts last to clobber tombstones
    state.knownHosts foreach { case (id, HostInformation(addr, _, _)) =>
      addrs += (addr -> id)
    }

    addrs foreach { case (addr, id) =>
      bus.registerHostID(id, addr.copy(port = bus.hostAddress.port))
    }

    val knownIDs = state.knownHosts.keySet

    // Only process membership changes and bus mapping removals once this node is
    // known
    if (knownIDs contains self) {
      bus.setRegisteredHostIDs(addrs.values.toSet)

      val logMembers = replog.ring

      val adds = knownIDs diff logMembers
      val removes = logMembers diff knownIDs

      ((adds map replog.addMember) ++
        (removes map replog.removeMember)).join map { _ => isRunning }
    } else {
      Future.successful(isRunning)
    }
  }

  private def scanTombstones(): Future[Unit] = {
    val expiry = tes.currentExpiry
    val maxExpired = currState.graveyard takeWhile {
      _.notNewerThan(expiry)
    } lastOption
    val propose = maxExpired map { _.when } filterNot {
      currState.forgetThreshold(self) contains _
    } map { w =>
      proposeAndApply(ForgetHosts(self, w))
    } getOrElse Future.unit

    propose map { _ =>
      nextTombstoneScan = if (isRunning) {
        Some(Timer.Global.scheduleTimeout(tes.scanPeriod) { scanTombstones() })
      } else {
        None
      }
    }
  }

  private def knownHosts = currState.knownHosts

  /** Returns true when the local host a member of a cluster, and
    * false otherwise.
    */
  def isMember = hosts contains self

  /** Returns the set of host IDs of current cluster members.
    */
  def hosts = currState.knownHosts.keySet

  /** Returns the set of host IDs of recently left cluster members.
    */
  def leftHosts = currState.graveyard map { _.host } toSet

  /** Returns the set of departed hosts remaining in the graveyard, and
    * the timestamp of their departure.
    */
  def graveyard: Map[HostID, Timestamp] =
    currState.graveyard map { t => t.host -> t.when } toMap

  /** Returns the set of host IDs of currently leaving cluster members.
    */
  def leavingHosts = currState.knownHosts collect {
    case (id, hi) if hi.leaving => id
  } toSet

  /** Returns the ID for a given hostname.
    */
  def getHostID(name: String): Option[HostID] =
    currState.knownHosts collectFirst {
      case (id, info) if info.hostName == name => id
    }

  def getHostName(host: HostID): Option[String] =
    hostInformation(host) map { _.hostName }

  def addresses: Set[HostAddress] = {
    val builder = Set.newBuilder[HostAddress]
    currState.knownHosts foreach { case (_, info) =>
      val notSelf = localHostName exists { _ != info.hostName }
      if (!info.leaving && notSelf) {
        builder += info.hostAddress
      }
    }
    builder.result()
  }

  def localHostName: Option[String] =
    getHostName(self)

  def getReplica(host: HostID): Option[String] =
    hostInformation(host) map { _.replica }

  def localReplica: Option[String] =
    getReplica(self)

  /** Returns the IDs of the known hosts that are in the given replica.
    * This includes hosts that are in the process of leaving.
    */
  def hostsInReplica(replicaName: String): Set[HostID] =
    currState.knownHosts collect { case (id, HostInformation(_, `replicaName`, _)) =>
      id
    } toSet

  /** Returns the IDs of the known, non-leaving hosts that are in the given replica.
    */
  def liveHostsInReplica(replicaName: String): Set[HostID] =
    currState.knownHosts collect {
      case (id, HostInformation(_, `replicaName`, false)) => id
    } toSet

  /** Returns all known replica names that have at least one host in them.
    */
  def replicaNames: Set[String] =
    currState.knownHosts.values map { _.replica } toSet

  /** Returns whether the provided host is local, i.e. in the same
    * replica.
    */
  def isLocal(host: HostID) =
    hostInformation(host) exists { _.replica == replica }

  /** Returns the local host's ID.
    */
  def self = replog.self

  /** Initialize a new cluster, containing only the local host. The
    * local host must not already be a member of a cluster.
    */
  def init() =
    if (!isMember) {
      logger.info(
        s"Membership Service: Initializing with this node $self as the initial member...")
      replog.init() flatMap { _ =>
        introduceHost(selfIntroduction, bus.hostAddress.hostName, Duration.Inf)
      } map { _ =>
        logger.info("Membership Service: Initialized.")
      }
    } else {
      Future.failed(
        new IllegalStateException("Node is already a member of a cluster!"))
    }

  /** Join an existing cluster, using the provided seed host as the
    * initial entry point. The local host must not already be a member
    * of a cluster.
    */
  def join(seed: HostAddress) = {
    if (!isMember) {
      logger.info(
        s"Membership Service: this node $self is introducing itself to seed $seed")
      sendHello(Seq(seed)) flatMap { seed =>
        logger.info(
          s"Membership Service: this node $self is joining through seed ${idAndAddress(seed)}")
        subscribe {
          if (isMember) {
            logger.info("Membership Service: Joined.")
            FutureFalse
          } else {
            FutureTrue
          }
        }
      }
    } else {
      Future.failed(
        new IllegalStateException("Node is already a member of a cluster!"))
    }
  }

  def canSafelyRemove(hostID: HostID) = {
    def aloneMyself(hs: Set[HostID]) =
      hs.size == 1 && hs.contains(self)

    val currHosts = hosts
    val newHosts = hosts - hostID

    replog.isLeader || !aloneMyself(newHosts) || aloneMyself(currHosts)
  }

  /** Remove another cluster member from this cluster. Often, this is
    * used to eject a member after permanent failure.
    */
  def remove(hostID: HostID, force: Boolean) = {
    logger.info(s"Membership Service: Removing ${idAndAddress(hostID)}")

    val maybeReinit = if (force && !canSafelyRemove(hostID)) {
      // Special case of shrinking the cluster from 2 to 1 members, that
      // remaining member being ourselves and not a leader. This can't be done
      // safely, but we know that the departed leader isn't around anymore
      // since an operator is forcibly removing it. Reinitializing the log
      // does the trick. It'll add the requisite membership change messages.
      logger.warn(
        s"Membership Service: Removing ${idAndAddress(hostID)} with force while not safe, re-initializing the log.")
      replog.init()
    } else {
      Future.unit
    }

    maybeReinit flatMap { _ =>
      // Second precision is plenty enough. Makes CBOR serialized form smaller.
      val ts = Timestamp.ofSeconds(Clock.time.seconds)
      proposeAndApply(RemoveHost(hostID, ts)) map { _ =>
        logger.info(s"Membership Service: $hostID removed.")
      }
    }
  }

  def markLeaving(hostID: HostID) = {
    if (hostInformation(hostID) exists { !_.leaving }) {
      logger.info(s"Membership Service: Marking ${idAndAddress(hostID)} as leaving")
      proposeAndApply(HostIsLeaving(hostID)) map { _ =>
        logger.info(s"Membership Service: $hostID marked as leaving.")
      }
    } else {
      Future.unit
    }
  }

  /** Notify the cluster that the address of this node has changed. The local
    * node must be a member of the cluster already. The operation expresses
    * that it has been moved (with intact storage) to its current address.
    * It will try to contact all its known peers in random order,
    * repeatedly if needed until it successfully gets one peer to accept the
    * address update, or the operation times out, or some peer notifies this
    * host that it has been removed from the cluster, in which case it will
    * throw an exception. In the special case that this node is the only member
    * of the cluster (and thus has no peers), it'll simply commit the address
    * update to the log on its own.
    */
  def updateAddress(): Future[Unit] =
    if (!isMember) {
      Future.failed(
        new IllegalStateException(s"Node $self is not yet a member of a cluster!"))
    } else {
      def peerAddrs = {
        val peers = currState.knownHosts collect {
          case (id, _) if id != self => bus.getHostAddresses(id)
        } flatten

        val peerSeq = if (peers.isEmpty) {
          // No peers, we're the only member, so let's talk this through with
          // ourselves
          Seq(bus.hostAddress)
        } else {
          Random.shuffle(peers).toSeq
        }
        logger.info(
          s"Membership Service: trying to update address through peers: $peerSeq")
        peerSeq
      }
      sendHello(peerAddrs) map { seed =>
        logger.info(
          s"Membership Service: this node $self updated its address in the cluster through seed ${idAndAddress(seed)}")
      }
    }

  private def sendHello(peers: => Seq[HostAddress]) =
    Hello.sendHello(bus, helloSignalID, peers, selfIntroduction)

  private def selfIntroduction = HostIntroduction(self, replica)

  private def localHostInfoMatches(f: HostInformation => Boolean) =
    hostInformation(self) match {
      case None     => false
      case Some(hi) => f(hi)
    }

  /** Produces whether the supplied replica name matches what the Membership
    * service believes it to be.
    */
  def replicaNameMatches(replica: String): Boolean =
    localHostInfoMatches { _.replica == replica }

  /** Produces whether our host address matches what the Membership
    * service believes it to be.
    */
  def busAddressMatches: Boolean =
    localHostInfoMatches { _.hostAddress.hostName == bus.hostAddress.hostName }

  def hostInformation(host: HostID): Option[HostInformation] =
    currState.knownHosts.get(host)

  val regions: Map[RegionName, RegionID] =
    Map(RegionName.DefaultName -> RegionID.DefaultID)

  def getRegionID(replicaName: String) =
    regions.get(RegionName.fromReplicaName(replicaName))

  val selfRegion: Option[RegionID] = Some(RegionID.DefaultID)

  /** Returns the current region assignment for the given host, if any.
    */
  def getRegion(host: HostID): Option[RegionID] =
    getReplica(host) flatMap getRegionID

  private def introduceHost(
    introduction: HostIntroduction,
    hostName: String,
    deadline: Duration): Future[Unit] =
    if (
      currState.informationMatches(
        introduction.hostID,
        hostName,
        introduction.replica)
    ) {
      Future.unit
    } else if (!ReplicaNameValidator.isValid(introduction.replica)) {
      Future.failed(
        new IllegalArgumentException(
          ReplicaNameValidator.invalidNameMessage(introduction.replica)))
    } else {
      // In 2.6.0 and earlier, port is required in order to connect to the host.
      // For now, use the bus's configured port.
      // FIXME: remove port from this message.
      proposeAndApply(
        IntroduceHost(introduction, HostAddress(hostName, bus.hostAddress.port)),
        deadline)
    }

  private def idAndAddress(id: HostID) = {
    val addr = knownHosts.getOrElse(id, "address unknown")
    s"$id($addr)"
  }

  private def idAndAddress(desc: (Option[HostID], HostAddress)) = {
    val (idOpt, addr) = desc
    val id = idOpt.getOrElse("<unknown>")
    s"$id at $addr"
  }

}
