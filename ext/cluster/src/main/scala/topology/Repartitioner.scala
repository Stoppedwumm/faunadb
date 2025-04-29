package fauna.cluster.topology

import fauna.atoms._
import fauna.cluster.{ ClusterService, Membership }
import fauna.cluster.workerid.WorkerIDs
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.Service
import fauna.lang.syntax._
import fauna.storage.CassandraHelpers.ForbiddenEndLocation
import org.apache.commons.math3.stat.descriptive.moment.Variance
import scala.collection.Searching._
import scala.concurrent.Future
import scala.util.Random

/**
  * Listens to the changes in worker ID assignment and host membership, and
  * proposes changes to topology to ensure all live hosts own the necessary
  * number of tokens in their replicas and that no tokens belong to dead hosts.
  */
class Repartitioner(
  membership: Membership,
  workerIDs: WorkerIDs,
  topologies: Topologies)
    extends Service {
  private case class ObservedState(
    live: Set[HostID],
    dead: Set[HostID],
    replicaNames: Set[String]) {

    def needsAction(prev: ObservedState): Boolean =
      /*
       * 1. If the set of live hosts changes
       * 2. Some hosts got removed since last time. Removed hosts expiring
       *    from the "left" list is of no concern
       * 3. Need to keep the replica list correct for both add and drops,
       *    We could re-add a dropped replica and we need to see this change
       */
      live != prev.live ||
        dead.diff(prev.dead).nonEmpty ||
        replicaNames != prev.replicaNames
  }

  @volatile private[this] var running = false
  @volatile private[this] var lastObservedState: Option[ObservedState] = None
  def isRunning = running

  def start(): Unit = {
    if (!running) {
      running = true
      ClusterService.subscribeWithLogging(
        membership, // we need to observe hosts that have left
        workerIDs, // we need to observe worker ID assignment
        topologies // we need to observe new replicas being added
      )(onStateChanged)
    }
  }

  def stop(graceful: Boolean): Unit = {
    running = false
  }

  private def onStateChanged: Future[Boolean] = {
    // Only submit repartitioning proposals if this node can see its
    // own worker ID and is in replication. This prevents a race condition
    // where a recently joined node did not catch up on either membership
    // or topology so it tries to enforce a partitioning based on stale data.
    val action = if (workerIDs.workerID.idOpt.isDefined && topologies.snapshot.replicaNames.contains(membership.replica)) {
      val dead = membership.leftHosts ++ membership.leavingHosts
      val live = workerIDs.assignedHosts.toSet -- dead
      val replicaNames = topologies.snapshot.replicaTopologies map { _._1 }

      val observedState = ObservedState(live, dead, replicaNames.toSet)
      val needsAction = lastObservedState forall { last =>
        observedState.needsAction(last)
      }
      if (needsAction) {
        updateTopologies(live, dead, replicaNames) map { _ =>
          lastObservedState = Some(observedState)
        }
      } else {
        Future.unit
      }
    } else {
      Future.unit
    }
    action map { _ =>
      isRunning
    }
  }

  private def getTopology(replicaName: String) =
    topologies.snapshot.getTopology(replicaName)

  private def updateTopologies(
    live: Set[HostID],
    dead: Set[HostID],
    replicaNames: Seq[String]): Future[Unit] = {
    def updateNextReplica() = updateTopologies(live, dead, replicaNames.tail)

    // Decouple recursion from call stack
    def updateNextReplicaAsync() = Future.delegate {
      updateNextReplica()
    }

    replicaNames.headOption.fold(Future.unit) { replicaName =>
      getTopology(replicaName) match {
        case None =>
          // Replica disappeared while we worked? It can happen, go work on the next one
          updateNextReplicaAsync()
        case Some(r) =>
          // Live hosts in current replica
          val liveInReplica = live filter { host =>
            membership.getReplica(host).contains(replicaName)
          }

          val proposal = Repartitioner.newTopologyProposal(liveInReplica, dead, r)

          if (proposal.newPending.isEmpty) {
            // There are no hosts in the replica.
            if (proposal.oldPending.nonEmpty) {
              // If there used to be, but they are all removed now, note it. We
              // can't represent this in pending topology, so we simply won't do
              // anything. Operator can either eventually remove the replica or
              // add nodes to it.
              getLogger.debug(
                s"Not modifying pending topology for replica $replicaName as it has no hosts left in it.")
            }
            // Work on the next replica
            updateNextReplicaAsync()
          } else if (proposal.hasChanges) {
            // We actually changed something, so communicate it to the cluster
            // NOTE: r.pending is used instead of proposal.oldPending. They differ in their representation
            // of the empty ring.
            val fut = topologies.proposeTopology(replicaName,
                                                 proposal.newPending,
                                                 r.pending,
                                                 proposal.newReusables)
            fut flatMap { _ =>
              updateNextReplica()
            }
          } else {
            updateNextReplicaAsync()
          }
      }
    }
  }
}

case class RepartitioningProposal(
  newPending: Vector[SegmentOwnership],
  newReusables: Vector[Location],
  oldPending: Vector[SegmentOwnership]) {

  def hasChanges =
    newPending != oldPending

  def acceptedBy(t: ReplicaTopology) =
    t.pending == newPending

  lazy val variance =
    new Variance().evaluate(fractions.toArray)

  private[this] def fractions =
    if (newPending.isEmpty) {
      Seq.empty
    } else {
      val segLists = SegmentOwnership.toOwnedSegments(newPending).groupBy({
        _.host
      }).values

      segLists map { segs =>
        Segment.ringFraction(segs map { _.segment }).toDouble
      }
    }
}

object Repartitioner {
  private[this] val ProposalsToGenerate = 100

  def newTopologyProposal(
    live: Set[HostID],
    dead: Set[HostID],
    r: ReplicaTopology,
    rnd: Random = Random,
    locationsPerHost: Int = Location.PerHostCount) = {

    def createProposal =
      newSingleTopologyProposal(live, dead, r, rnd, locationsPerHost)

    var currProposal = createProposal
    (1 until ProposalsToGenerate) foreach { _ =>
      val newProposal = createProposal
      if (currProposal.variance > newProposal.variance) {
        currProposal = newProposal
      }
    }
    currProposal
  }

  private[this] def newSingleTopologyProposal(
    live: Set[HostID],
    dead: Set[HostID],
    r: ReplicaTopology,
    rnd: Random,
    locationsPerHost: Int) = {
    val ReplicaTopology(current, prevPending, prevReusables) = r

    val pending = if (prevPending != ReplicaTopology.NullOwnership) {
      prevPending filter { _.from != ForbiddenEndLocation }
    } else {
      Vector.empty
    }

    // Remove tokens for all dead hosts
    val (removed, ringWithRemovals) = pending partition { so =>
      dead.contains(so.host)
    }

    val validReusables = {
      val allowedReusables = prevReusables filter { _ != ForbiddenEndLocation }
      val mergedReusables =
        ((removed map { _.from }) ++ allowedReusables).distinct
      // Revalidate reusable tokens; the only ones that make sense to take into account are
      // those still owned by dead hosts.
      mergedReusables filter { t =>
        dead.contains(SegmentOwnership.hostForLocation(current, t))
      }
    }

    def isStable(so: SegmentOwnership) = {
      val h = SegmentOwnership.hostForLocation(current, so.from)
      h == so.host || dead.contains(h)
    }

    // Make sure live hosts have the required number of tokens by adding or
    // dropping them.
    val (newPending, newReusables) =
      live.foldLeft((ringWithRemovals, validReusables)) {
        case ((ring, reusables), host) =>
          // We call tokens "stable" if they were not disposed in favor of reusable tokens.
          val (stableRing, stableTokens) = {
            val tokens = ring.filter { _.host == host }

            // If we have reusable tokens and this host has some tokens that it
            // hasn't claimed yet and they belong to another live host, drop as
            // many as possible so they can be replaced with reusable tokens
            // (or they'll be favored for removal in case of
            // missingTokenCount < 0 branch below). This is merely an
            // optimization to bias it to take over dead ranges instead of live
            // ones.
            val disposableTokens = tokens filterNot isStable
            val disposedTokens =
              rnd.shuffle(disposableTokens).take(reusables.length).toSet

            val stableRing = ring filterNot disposedTokens.contains
            val stableTokens = tokens filterNot disposedTokens.contains
            (stableRing, stableTokens)
          }

          val missingTokenCount = locationsPerHost - stableTokens.length
          if (missingTokenCount > 0) {
            // Reuse tokens as well as generate unique new tokens
            val randomTokenStream = LazyList.continually(Location.random(rnd)) filter { _ != ForbiddenEndLocation }
            val tokenStream = rnd.shuffle(reusables).to(LazyList) ++ randomTokenStream
            val newTokens = tokenStream.distinct map {
              SegmentOwnership(_, host)
            } filter { so =>
              stableRing
                .search(so)
                .isInstanceOf[InsertionPoint]
            } take missingTokenCount
            val newLocs = newTokens map { _.from } toSet

            // Have to sort it after each host so ring.search above works correctly
            val nextRing =
              (stableRing ++ newTokens).sorted
            val nextReusables = reusables filterNot newLocs.contains
            (nextRing, nextReusables)
          } else if (missingTokenCount < 0) {
            // This normally only happens if TokensPerHost was changed between
            // code versions and a node was brought up with a new code version
            // with lower TokensPerHost value. In that case, randomly discard
            // excess tokens (but prefer discarding tokens that could still
            // cause data movement.
            val (moreStable, lessStable) = rnd.shuffle(stableTokens) partition isStable
            val toRemove = (lessStable ++ moreStable).take(-missingTokenCount).toSet
            (stableRing filterNot toRemove.contains, reusables)
          } else {
            (stableRing, reusables)
          }
      }

    RepartitioningProposal(newPending, newReusables, pending)
  }
}
