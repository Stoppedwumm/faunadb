package fauna.cluster.workerid

import fauna.atoms._
import fauna.cluster.{ ClusterService, ClusterServiceConfig, Membership, ServiceState }
import fauna.cluster.ClusterService.LogAndState
import fauna.codex.cbor.CBOR
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.syntax._
import fauna.net.bus.SignalID
import fauna.stats.StatsRecorder
import scala.concurrent.Future

object WorkerIDCommand {
  implicit val codec = CBOR.SumCodec[WorkerIDCommand](
    CBOR.TupleCodec[RequestWorkerIDs],
    CBOR.TupleCodec[ForgetWorkerIDs]
  )
}

sealed abstract class WorkerIDCommand

/**
  * With this message the cluster is asked to assign specific worker IDs to specific hosts.
  * The IDs can either be AssignedWorkerID or UnavailableWorkerID instances.
  * Requests for UnavailableWorkerID are honored if the host has no assigned worker ID yet.
  * Requests for AssignedWorkerID are honored if the host has no assigned worker ID yet and
  * the worker ID is not assigned to a different host yet.
  */
case class RequestWorkerIDs(ids: Vector[(HostID, WorkerID)]) extends WorkerIDCommand

/**
  * With this message, the cluster is instructed to forget about specified hosts because they were
  * observed to be removed from the membership cluster.
  */
case class ForgetWorkerIDs(hostIDs: Vector[HostID]) extends WorkerIDCommand

trait WorkerIDState extends ServiceState[WorkerIDCommand, WorkerIDState] {
  def workerIDs: Map[HostID, WorkerID]
  def workerID(hostID: HostID): WorkerID
  def assignedHosts: Iterable[HostID]
}

object WorkerIDState {
  implicit val codec = CBOR.SumCodec[WorkerIDState](
    CBOR.TupleCodec[WorkerIDStateV1]
  )
  def empty: WorkerIDState = WorkerIDStateV1(Map.empty)
}

case class WorkerIDStateV1(workerIDs: Map[HostID, WorkerID]) extends WorkerIDState {
  def workerID(hostID: HostID) = workerIDs.getOrElse(hostID, UnassignedWorkerID)

  def assignedHosts = workerIDs.collect { case (k, v) if v.idOpt.nonEmpty => k }

  def applyCmd(cmd: WorkerIDCommand) =
    cmd match {
      case RequestWorkerIDs(ids) =>
        ids.foldLeft(this) { case (s, (hostID, wid)) =>
          s.requestWorkerID(hostID, wid)
        }
      case ForgetWorkerIDs(hostIDs) =>
        copy(workerIDs = workerIDs -- hostIDs)
    }

  private def requestWorkerID(hostID: HostID, wid: WorkerID) =
    if (!workerIDs.contains(hostID) && (wid.idOpt forall { id => workerIDs.values forall { !_.idOpt.contains(id) } })) {
      copy(workerIDs = workerIDs.updated(hostID, wid))
    } else {
      this
    }
}

object WorkerIDs {
  // NB: maxWorkerID is normally equal to WorkerID.MaxValue. We only explicitly set it to lower value in tests.
  // There was some back and forth about whether to use a constructor parameter or a setter method; we settled
  // on constructor parameter.
  def apply(
    config: ClusterServiceConfig,
    logSID: SignalID,
    stateSID: SignalID,
    membership: Membership,
    stats: StatsRecorder = StatsRecorder.Null,
    minWorkerID: Int = WorkerID.MinValue,
    maxWorkerID: Int = WorkerID.MaxValue) =
    new WorkerIDs(
      ClusterService[WorkerIDCommand].makeLogAndState(
        config, logSID, stateSID, "worker_id", WorkerIDState.empty, stats, Some(membership.logHandle)),
      membership, minWorkerID, maxWorkerID)
}

class WorkerIDs(
    logAndState: LogAndState[WorkerIDCommand, WorkerIDState],
    membership: Membership,
    minWorkerID: Int,
    maxWorkerID: Int) extends ClusterService[WorkerIDCommand, WorkerIDState](logAndState) {

  require(minWorkerID >= WorkerID.MinValue, s"$minWorkerID must be >= ${WorkerID.MinValue}")
  // at least one worker ID available
  require(minWorkerID < WorkerID.MaxValue, s"$minWorkerID must be < ${WorkerID.MaxValue}")
  require(maxWorkerID >= WorkerID.MinValue, s"$maxWorkerID must be >= ${WorkerID.MinValue}")
  require(maxWorkerID <= WorkerID.MaxValue, s"$maxWorkerID must be <= ${WorkerID.MaxValue}")
  logWorkerID()

  override def start() = {
    super.start()
    membership.subscribeWithLogging(onMembershipChanged)
  }

  private[this] def self = replog.self

  /**
    * Returns a the worker ID of the specified host.
    * @param host the queried host
    * @return the queried host's worker ID.
    */
  def hostWorkerID(host: HostID) =
    currState.workerID(host)

  /**
    * Returns the worker ID of this host.
    * @return the queried host's worker ID.
    */
  def workerID =
    hostWorkerID(self)

  def assignedHosts = currState.assignedHosts

  private def onMembershipChanged: Future[Boolean] = {
    val knownHosts = currState.workerIDs.keySet
    for {
      _ <- forgetWorkerIDs(knownHosts diff membership.hosts)
      _ <- acquireWorkerIDs(membership.hosts diff knownHosts)
    } yield isRunning
  }

  private def acquireWorkerIDs(arrived: Iterable[HostID]): Future[Unit] = {
    val requests = makeWorkerIDRequests(arrived, currState)
    if (requests.nonEmpty) {
      proposeAndApply(RequestWorkerIDs(requests.toVector)) flatMap { _ =>
        if (arrived exists { _ == self }) {
          logWorkerID()
        }
        // Repeat in case some requests were rejected because they proposed a
        // worker ID that got assigned to a different node.
        acquireWorkerIDs(requests map { _._1})
      }
    } else {
      Future.unit
    }
  }

  private def makeWorkerIDRequests(hosts: Iterable[HostID], state: WorkerIDState) = {
    val takenIDs = new java.util.BitSet
    state.workerIDs.values foreach { _.idOpt foreach { takenIDs.set(_) } }
    hosts flatMap { host =>
      state.workerID(host) match {
        case UnassignedWorkerID =>
          // First try to find a free ID in the unreserved ID space
          val unext = takenIDs.nextClearBit((minWorkerID + maxWorkerID) / 2 + 1)
          // Next try to find a free ID in the reserved ID space
          val next = if (unext > maxWorkerID) takenIDs.nextClearBit(minWorkerID) else unext
          if (next > maxWorkerID) {
            Some((host, UnavailableWorkerID))
          } else {
            takenIDs.set(next)
            Some((host, AssignedWorkerID(next)))
          }
        case _ => None // already assigned
      }
    }
  }

  private def forgetWorkerIDs(departed: Set[HostID]): Future[Unit] =
    if (departed.nonEmpty) {
      proposeAndApply(ForgetWorkerIDs(departed.toVector))
    } else {
      Future.unit
    }

  private def logWorkerID() =
    currState.workerID(self) match {
      case AssignedWorkerID(id) =>
        getLogger.info(s"Worker ID: $id")
      case UnavailableWorkerID =>
        getLogger.error(s"No worker IDs available. The cluster reached the maximum size of ${maxWorkerID - minWorkerID + 1} nodes. This node should be removed from the cluster.")
      case UnassignedWorkerID => ()
  }
}
