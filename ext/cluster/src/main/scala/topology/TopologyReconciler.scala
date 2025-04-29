package fauna.cluster.topology

import fauna.atoms._
import fauna.cluster.Membership
import fauna.cluster.workerid.WorkerIDs
import fauna.exec.FaunaExecutionContext.Implicits._
import fauna.exec.Timer
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.net.HostService
import fauna.net.util.FutureSequence
import fauna.stats.StatsRecorder
import fauna.tx.log.SequencedStateCallback
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.Random

/**
  * Trait for implementations of a streaming strategy. Allows the streaming
  * strategy to be separately specified and for TopologyReconciler to
  * be free of storage implementation details.
  */
trait Streamer {
  /**
    * Request streaming the segment from a host.
    * @param host the host to stream the segment from
    * @param seg the segment to stream
    * @param topoVersion the topology version used by the caller
    * @return a future that when completes with true, the segment was
    *         successfully streamed. If it completes with false, the
    *         streaming failed.
    */
  def streamSegment(host: HostID, seg: Segment, topoVersion: Long): Future[Boolean]
}

object TopologyReconciler {
  val RetryDelayOnRescheduledSegments = 5.seconds
  val AnnouncementCompletionWait = 5.seconds
}

/**
  * Describes missing segments for a node: those that are assigned to the node
  * in the pending ring for its replica, but not in its current ring. The node
  * needs to acquire data in those segments to become up to date with the
  * current ring.
  * @param segments the segments in pending ring missing from current ring
  * @param pending the pending ring this set is based on
  * @param version the version of the topology the pending ring was based on
  */
case class VersionedSegments(segments: Seq[Segment], pending: Vector[SegmentOwnership], version: Long) {
  def pendingMatches(p: Vector[SegmentOwnership]): Boolean =
    (p eq pending) || p == pending
}

class TopologyReconciler(
    topologies: Topologies,
    membership: Membership,
    workerIDs: WorkerIDs,
    streamer: Streamer,
    hostService: HostService,
    sequencer: FutureSequence,
    stats: StatsRecorder = StatsRecorder.Null) extends ExceptionLogging {

  import TopologyReconciler._

  // Is the reconciler running (not closed)?
  @volatile private[this] var running = true
  // pending topologies.announceOwnership futures
  private[this] var pendingAnnouncements = Seq.empty[Future[Unit]]

  private[this] val ssb = SequencedStateCallback(claimAllMissingSegments, sequencer)
  topologies.subscribeWithLogging(ssb())
  workerIDs.subscribeWithLogging(ssb())

  StatsRecorder.polling(30.seconds) {
    stats.set("Topology.TokensPendingRatio",
      myTopology._1.fold(0f) { t =>
        Segment.ringFraction(t.missingSegments(membership.self))
    })
  }

  def close(): Unit =
    running = false

  private def myTopology = {
    val s = topologies.snapshot
    (s.getTopology(membership.replica), s.version)
  }

  private def claimAllMissingSegments: Future[Boolean] =
    claimMissingSegments(None)

  private def claimMissingSegments(ovs: Option[VersionedSegments]): Future[Boolean] =
    if (running) {
      // Do we have a worker ID? (This node is live.)
      workerIDs.workerID.idOpt.fold(FutureTrue) { _ =>
        // Are we part of an active replica?
        val (topologyOpt, version) = myTopology
        topologyOpt.fold(FutureTrue) { topology =>
          val pending = topology.pending
          // Is ovs up to date?
          ovs filter { _.pendingMatches(pending) } map { vs =>
            // if vs.segments is empty, we're done
            vs.segments.headOption.fold(FutureTrue) { seg =>
              // claim first outstanding segment
              claimSegment(seg, vs.version) flatMap { leftoverSegs =>
                // Drop the processed segment but prepend any leftover unstreamed
                // but streamable subsegments so they're immediately attempted.
                // Note that claimSegment can fail to claim a segment if all
                // replicas that have a copy of the data failed to provide it
                // (e.g. because they're all down.) In that case this segment
                // will be dropped from newSegs without having been claimed,
                // allowing the reconciler to move on working on other segments.
                // To compensate for this, we force recomputing of missing
                // segments after the last segment was processed. Note that
                // claimSegment distinguishes between segments that are
                // available in some replica but all hosts are down, which is a
                // temporary condition remedied by recomputing, and segments
                // that aren't available in any replica. The latter is a
                // permanent condition and claimSegment will claim those
                // segments without streaming at the cost of potential data loss.
                val newSegs = leftoverSegs ++ vs.segments.drop(1)
                if (newSegs.nonEmpty) {
                  // Proceed with the updated segment list
                  claimMissingSegments(Some(VersionedSegments(newSegs, pending, vs.version)))
                } else {
                  // If we got to the end of the segment list, wait a bit, then
                  // recompute missing segments in case some segments that
                  // could not be streamed were dropped.
                  Timer.Global.delay(retryDelayOnRescheduledSegments)(claimAllMissingSegments)
                }
              }
            }
          } getOrElse {
            // There's no information on missing segments, or it is outdated.
            // We need to compute missing segments.
            synchronized {
              pendingAnnouncements = pendingAnnouncements filterNot { _.isCompleted }
            }

            if (pendingAnnouncements.nonEmpty) {
              // If there are any outstanding log commands, wait until they are
              // all applied to topology state. Timing out allows us to
              // periodically cycle and observe various termination criteria.
              Timer.Global.completedAfter(Future.sequence(pendingAnnouncements).unit, AnnouncementCompletionWait)(Future.unit) flatMap { _ =>
                // Retry.
                claimAllMissingSegments
              }
            } else {
              // No outstanding commands, compute missing segments and start
              // working on them.
              val vs = VersionedSegments(shuffleSegments(topology.missingSegments(membership.self)), pending, version)
              claimMissingSegments(Some(vs))
            }
          }
        }
      }
    } else {
      FutureFalse
    }

  protected def shuffleSegments(rs: Seq[Segment]) =
    Random.shuffle(rs)

  // Does a best-effort to claim ownership of the segment or at least a
  // subsegment of it. Returns a list of subsegments that were not claimed
  // through streaming, but potentially could still be streamed. E.g. the
  // whole segment to claim is not available from a single host, so there are
  // still outstanding subsegments that are likely available from another host.
  private def claimSegment(seg: Segment, version: Long): Future[Seq[Segment]] = {
    val replicas = topologies.snapshot.replicaNames

    val os = replicas flatMap { r =>
      topologies.getFirstClaimedIntersection(seg, r)
    }

    // order segments by live/local preference
    val hosts = hostService.preferredOrder(os map { _.host }).zipWithIndex.toMap
    val ordered = os.sortBy { seg => hosts(seg.host) }

    claimSegmentFromHosts(seg, ordered, version)
  }

  private def claimSegmentFromHosts(seg: Segment, hostsToTry: Seq[OwnedSegment], version: Long): Future[Seq[Segment]] = {
    hostsToTry.headOption match {
      // Checking for host liveness is a best-effort attempt to avoid
      // having to wait on streaming timeout if the stream host is down.
      case Some(OwnedSegment(streamSeg, streamHost)) =>
        // NOTE: streamSeg might be (and will likely be) just a subsegment
        // of the segment we're trying to stream. That is fine. Once it was
        // streamed, remaining unstreamed subsegments will be prepended to
        // the segment list and work will proceed with the next subsegment.
        streamer.streamSegment(streamHost, streamSeg, version) flatMap {
          case true =>
            announceOwnership(Seq(streamSeg))
            Future.successful(seg diff streamSeg)
          case false =>
            stats.incr("Topology.SegmentTransferFailed")
            // Failed to stream the segment, try next host
            claimSegmentFromHosts(seg, hostsToTry.drop(1), version)
        }
      case None =>
        // No more replicas to try, and not even part of the segment was streamed.
        // We need to check if any of its parts are unclaimed.
        val unclaimed = topologies.getUnclaimedParts(seg)
        if (unclaimed.nonEmpty) {
          // Claim ownership of all unclaimed subsegments.
          announceOwnership(unclaimed)
        }

        // This will stop further attempts on all remaining available
        // subsegments too in this iteration; they'll be re-queried when
        // the reconciler runs out of work to do, though.
        Future.successful(Seq.empty)
    }
  }

  private def announceOwnership(segments: Seq[Segment]): Unit = {
    stats.count("Topology.SegmentsAcquired", segments.size)
    val f = topologies.announceOwnership(segments)

    if (!f.isCompleted) {
      synchronized {
        pendingAnnouncements = pendingAnnouncements :+ f
      }
    }
  }

  protected def retryDelayOnRescheduledSegments = RetryDelayOnRescheduledSegments
}
