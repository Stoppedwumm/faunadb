package fauna.tx.transaction

import fauna.atoms.HostID
import scala.concurrent.Future

object SegmentState {
  object HasStart {
    def unapply(s: SegmentState): Option[Epoch] =
      s match {
        case Started(start)   => Some(start)
        case Closed(start, _) => Some(start)
        case _                => None
      }
  }
}

sealed abstract class SegmentState

/** A segment is uninitialized if it hasn't started yet.
  */
case object Uninitialized extends SegmentState

/** A segment is started at the specified epoch.
  * @param start the starting epoch.
  */
case class Started(start: Epoch) extends SegmentState

/** A segment is closed, it was live between the specified
  * start epoch (inclusive) and end epoch (exclusive).
  * @param start the starting epoch.
  * @param end the ending epoch.
  */
case class Closed(start: Epoch, end: Epoch) extends SegmentState

object SegmentInfo {
  val NoInitRound = -1
  val FirstInitRound = 0
}

/** Describes the members and the state of a segment. A segment is live for
  * epochs "e" for which start <= e < end. If a segment was ended,
  * then "end" describes its removal epoch. A segment will only contain
  * non-empty (transaction bearing) batches for epochs for which it is live.
  * If "start" is None, the segment was not initialized yet.
  * If "end" is None, it means the segment was not ended yet.
  * "initVersion" is used in TxnPipeline.initLogNode() to reset ring building
  * when the membership of a not-yet-started segment changes.
  */
case class SegmentInfo(hosts: Vector[HostID], state: SegmentState, initRound: Int) {
  def contains(h: HostID) = hosts contains h

  def isLiveAt(ep: Epoch): Boolean =
    state match {
      case Uninitialized      => false
      case Started(start)     => start <= ep
      case Closed(start, end) => start <= ep && ep < end
    }

  def isEndedAt(ep: Epoch): Boolean =
    state match {
      case Closed(_, end) => end <= ep
      case _              => false
    }

  def isEnded =
    state match {
      case _: Closed => true
      case _         => false
    }

  def isStarted =
    state match {
      case Uninitialized => false
      case _             => true
    }

  def endAfter(ep: Epoch): SegmentInfo =
    state match {
      case Started(start) => copy(state = Closed(start, ep))
      case _ =>
        throw new IllegalStateException(
          s"Can not end a segment in $state state, only a started one.")
    }

  /** Creates a copy of this segment info with different hosts.
    * Additionally, if the hosts are different from current hosts
    * and the segment is not started yet, increments the init round.
    */
  def withHosts(hosts: Vector[HostID]): SegmentInfo =
    if (hosts == this.hosts) {
      this
    } else {
      copy(
        hosts = hosts,
        initRound = state match {
          case Uninitialized => initRound + 1
          case _             => initRound
        })
    }
}

/** Represents information about log topology.
  */
trait LogNodeInfo {

  /** Returns a map of all known log segments, keyed by their ID with
    * values describing the log segment: its member hosts and its
    * state.
    */
  def topology: Map[SegmentID, SegmentInfo]

  /** Log topology information has limited temporal validity. It is valid up to
    * and including the specified epoch. It must not be used before it was
    * revalidated for acting on log data after this epoch.
    */
  def validUntil: Epoch

  /** The current version of log topology information. Mostly used for CAS
    * update semantics of it. It increases monotonically with every update.
    */
  def version: Long
}

sealed abstract class RevalidatorRole

object RevalidatorRole {
  case object MainRevalidator extends RevalidatorRole
  case object SubstituteRevalidator extends RevalidatorRole
  case object NotARevalidator extends RevalidatorRole
}

/** A trait representing a service that provides log topology as well as
  * methods to effect changes to it.
  */
trait LogNodeProvider {

  /** Subscribe to changes in the topology. Subscription ends once the future
    * returned from callback completes with false.
    */
  def subscribeWithLogging(cb: => Future[Boolean]): Future[Unit]

  /** Returns the LogNodeInfo object representing the current log topology.
    */
  def getLogNodeInfo: LogNodeInfo

  /** Removes segments from the topology. Takes the version of LogNodeInfo the
    * caller read before deciding to make the change (used for OCC.) Only
    * segments that ended before current global minimal persisted timestamp are
    * safe to remove.
    */
  def removeSegments(segs: Iterable[SegmentID], currVersion: Long): Future[Unit]

  /** Revalidates the log topology until a new epoch (in the future compared to
    * its current validity limit). Revalidation should typically extend the
    * validity by few seconds (e.g. 8) and have the system revalidate
    * periodically. Takes the version of LogNodeInfo the caller read before
    * deciding to revalidate (used for OCC.)
    */
  def revalidate(validUntil: Epoch, currVersion: Long): Future[Unit]

  /** Marks the segment as started. Segments can start after their initial
    * members have all joined the segment's Raft ring. The start of the segment
    * must be beyond the current log topology validUntil value to ensure that
    * no observer can miss its start (by having to process a revalidation before
    * observing it.) Takes the version of LogNodeInfo the caller read before
    * deciding to call this method (used for OCC.)
    */
  def startSegment(seg: SegmentID, start: Epoch, currVersion: Long): Future[Unit]

  /** Closes the segment `seg`, which must exist and be started.
    * `currVersion` must match the current log topology version.
    * Returns the epoch at which the segment will be closed.
    * NB: Closing a segment will typically cause a new segment to be
    * opened according to the rules for log topology. This method's use
    * is "resetting" a bad segment by replacing it.
    */
  def closeSegment(seg: SegmentID, currVersion: Long): Epoch

  /** Moves the segment `seg` from `from` to `to`. The segment must exist
    * and be started. The source and destination nodes must be in the same
    * replica. `currVersion` must match the current log topology version.
    */
  def moveSegment(seg: SegmentID, from: HostID, to: HostID, currVersion: Long): Epoch

  /** To distribute the responsibility of revalidating the log topology, each
    * node locally calling this method will be told its role of being either the
    * main revalidator, a substitute revalidator, or not a revalidator at all.
    */
  def getRevalidatorRole: RevalidatorRole
}
