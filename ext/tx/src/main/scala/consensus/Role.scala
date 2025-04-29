package fauna.tx.consensus.role

import fauna.atoms.HostID
import fauna.lang.Timestamp
import fauna.logging.ExceptionLogging
import fauna.tx.consensus._
import fauna.tx.consensus.log._
import fauna.tx.consensus.messages._
import fauna.tx.log.TX
import java.util.concurrent.ThreadLocalRandom
import scala.util.control.NonFatal

import State._

sealed abstract class Role extends ExceptionLogging {
  def name: String
  def leader(state: State): Option[HostID]
  def isActive(state: State): Boolean

  def advance(state: State): Role
  def addProposals(state: State, after: Option[TX], es: Vector[Entry], meta: ProposalMeta): Role

  // Convenience logging function.
  def logInfo(state: State, msg: => String) =
    state.logInfo(name, msg)

  // Message Receipt

  def recv(state: State, sender: HostID, message: Message): Role =
    message match {
      case SyncProposals(_, tokens)      => recvSyncProposals(state, sender, tokens)
      case RequestProposals(_, tokens)   => recvRequestProposals(state, sender, tokens)
      case Proposals(_, after, es, meta) => recvProposals(state, sender, after, es, meta)

      case a @ Append(_, comm, acc, prev, es, token) => recvAppend(state, sender, comm, acc, prev, a.last, es, token)
      case Reinit(_, prev, ring, ringIdx)             => recvReinit(state, sender, prev, ring, ringIdx)
      case RequestAppend(_, after, count, token)     => recvRequestAppend(state, sender, after, count, token)
      case AcceptAppend(_, acc, sync)                 => recvAcceptAppend(state, sender, acc, sync)

      case Sync(_, token) => recvSync(state, sender, token)

      case CallElection(_, immediate) => recvCallElection(state, sender, immediate)
      case RequestVote(_, lastPos)    => recvRequestVote(state, sender, lastPos)
      case Vote(_, vote, votePos)     => recvVote(state, sender, vote, votePos)
      case Reset(_, leader)           => recvReset(state, sender, leader)

      case _: Unused => throw new AssertionError()
    }

  def recvSyncProposals(state: State, sender: HostID, tokens: Seq[Long]): Role
  def recvRequestProposals(state: State, sender: HostID, tokens: Seq[Long]): Role
  def recvProposals(state: State, sender: HostID, after: Pos, es: Entries, meta: ProposalMeta): Role

  def recvAppend(state: State, sender: HostID, comm: TX, acc: Pos, prev: Pos, last: Pos, entries: Entries, token: Long): Role
  def recvReinit(state: State, sender: HostID, prev: Pos, ring: Seq[HostID], ringIdx: TX): Role
  def recvRequestAppend(state: State, sender: HostID, after: Pos, count: Int, token: Long): Role
  def recvAcceptAppend(state: State, sender: HostID, acc: Pos, sync: Option[Long]): Role

  def recvCallElection(state: State, sender: HostID, immediate: Boolean): Role
  def recvRequestVote(state: State, sender: HostID, lastPos: Pos): Role
  def recvVote(state: State, sender: HostID, vote: HostID, votePos: Pos): Role
  def recvReset(state: State, sender: HostID, l: Option[HostID]): Role

  def recvSync(state: State, sender: HostID, token: Long) = {
    state.send(sender, AcceptAppend(state.term, state.log.flushedLastPos, Some(token)))
    this
  }

  // state helpers

  // If a node has a leader, detects no writes in flight, and
  // hasn't seen any calls for election, then we can skip the
  // heartbeat check.

  // In the case where there /is/ a write in the system, the
  // leader or follower that receives it will push it through.
  def step(state: State, t: Option[(HostID, Message)]): Role =
    t match {
      case None =>
        if (isActive(state)) {
          advance(state)
        } else {
          this
        }

      case Some((sender, msg)) =>
        (msg.term compare state.term).sign match {
          case -1 =>
            msg match {
              // We only need to send resets for messages from
              // follower-initiated messages. We allow leaders and candidates to
              // discover later terms via elections and appends.
              case _: SyncProposals | _: Proposals | _: RequestAppend | _: CallElection =>
                logInfo(state, s"Sending reset to $sender in response to message from earlier term. (Message: $msg)")
                state.send(sender, Reset(state.term, leader(state)))
              case _ => 
                logInfo(state, s"Ignoring message from $sender for earlier term. (Message: $msg)")
            }
            this
          case  0 =>
            recv(state, sender, msg)
          case  1 =>
            try {
              logInfo(state, s"Received message from $sender for later term. Resetting to Follower from $this. (Message: $msg)")
              state.reset(msg.term)
              Follower(state, Follower.LeaderOrElse(msg.leader, sender))
                .recv(state, sender, msg)
            } catch {
              case NonFatal(e) =>
                logInfo(state, s"Exception thrown when resetting state! $e")
                logException(e)
                Follower(state, Follower.LeaderOrElse(msg.leader, sender))
            } 
        }
    }

  def startElection(state: State, reason: String): Role = {
    if (!state.isMember) {
      throw new AssertionError(
        s"Cannot start election as non-ring member! Attempt reason: $reason")
    }

    state.stats.incrElections()

    state.reset(state.term.incr)

    logInfo(state, s"Becoming a candidate (reason=$reason)")
    val role = Candidate(Map.empty)

    if (state.ring.all == Set(state.self)) {
      // special case n & q == 1. have to elect self since we're not
      // waiting for another vote.
      role.recvVote(state, state.self, state.self, state.log.uncommittedLastPos)
    } else {
      role.advance(state)
    }
  }
}

object Leader {
  def initTerm(state: State) = {
    val afterIdx = state.log.uncommittedLastIdx
    val initIdx = afterIdx + 1
    val entry = List(Entry.Null(state.term, state.randomToken))

    state.addProposals(Some(afterIdx), entry, Seq(initIdx -> Timestamp.Max))
    state.appendAfter(afterIdx, entry)
    state.flush(Set.empty)
    state.send(state.peers, state.appendMsg(afterIdx, 1))

    // if this is a single node cluster, append above will have
    // comitted our append already.
    Leader(Some(initIdx) filter { _ > state.log.lastIdx })
  }
}

case class Leader(pendingMeta: Option[TX]) extends Role {
  def name: String = "LEADER"

  /**
   * N.B. This will need to handle the case where a new leader never takes over.
   * For now, we're only using this in the case of shutdown. If a new leader is
   * not elected before the current leader shuts down, a new one will be after.
   */
  def abdicate(state: State, reason: String) = {
    state.abdicate(reason)
    this
  }

  def leader(state: State) = Some(state.self)

  def isActive(state: State) = {
    (state.peers exists { state.accepted(_) != state.log.lastPos }) ||
    state.isActive
  }

  def advance(state: State) = {
    state.send(state.peers, state.heartbeatAppendMsg)
    processPending(state)
  }

  def addProposals(state: State, after: Option[TX], es: Vector[Entry], meta: ProposalMeta) =
    if (state.addProposals(after, es, meta).nonEmpty) {
      processPending(state)
    } else {
      this
    }

  // Message receipt

  def recvSyncProposals(state: State, sender: HostID, tokens: Seq[Long]) = {
    val missing = state.missingProposals(tokens).toVector
    state.send(sender, RequestProposals(state.term, missing take MaxProposals))
    this
  }

  def recvRequestProposals(state: State, sender: HostID, tokens: Seq[Long]) = {
    logInfo(state, s"Received RequestProposals from $sender, ignoring.")
    this
  }

  def recvProposals(state: State, sender: HostID, after: Pos, es: Entries, meta: ProposalMeta) =
    if (state.log.getTerm(after.idx) contains after.term) {
      addProposals(state, Some(after.idx), es, meta)
    } else {
      this
    }

  def recvAppend(state: State, sender: HostID, comm: TX, acc: Pos, prev: Pos, last: Pos, entries: Entries, token: Long) =
    throw new IllegalStateException(s"Multiple leaders elected for term ${state.term}!")

  def recvReinit(state: State, sender: HostID, prev: Pos, ring: Seq[HostID], ringIdx: TX) =
    throw new IllegalStateException(s"Multiple leaders elected for term ${state.term}!")

  def recvRequestAppend(state: State, sender: HostID, after: Pos, count: Int, token: Long) = {
    if (after.idx < state.log.prevIdx) {
      // Follower has fallen off the top of the log.
      logInfo(state, s"Received RequestAppend from $sender which has fallen of the top of the log.")
      val (ring, ringIdx) = state.log.committedRing
      state.send(sender, Reinit(state.term, state.log.prevPos, ring.all.toVector, ringIdx))
    } else {
      val msg = state.appendMsg(after.idx, count, token)
      if (count > 0) {
        logInfo(state, s"Responded to log sync RequestAppend($after, $count, $token) from $sender with $msg")
      }
      state.send(sender, msg)
    }

    this
  }

  def recvAcceptAppend(state: State, sender: HostID, acc: Pos, sync: Option[Long]) = {
    state.addAccept(acc, sender, sync)
    this
  }

  def recvCallElection(state: State, sender: HostID, immediate: Boolean) =
    reassertLeader(state, sender, "a call for election")

  def recvRequestVote(state: State, sender: HostID, lastPos: Pos) =
    if (state.isAbdicating) {
      val next = Follower(state, Follower.ObservedOrNull(state.peers))
      next.recvRequestVote(state, sender, lastPos)
    } else {
      reassertLeader(state, sender, "a request for vote")
    }

  private[this] def reassertLeader(state: State, sender: HostID, action: String) = {
    logInfo(state, s"Reasserting this node as the leader in response to $action from $sender.")
    state.send(state.peers + sender, state.heartbeatAppendMsg)
    this
  }

  def recvVote(state: State, sender: HostID, vote: HostID, votePos: Pos) = {
    logInfo(state, s"Received Vote from $sender, ignoring.")
    this
  }

  def recvReset(state: State, sender: HostID, l: Option[HostID]) = {
    logInfo(state, s"Received Reset from $sender, ignoring.")
    this
  }

  // helpers

  private def processPending(state: State): Role = {
    val pendingMeta0 = pendingMeta filter { _ > state.log.lastIdx }

    if (pendingMeta0.nonEmpty) {
      // If there is an uncommitted a member change or initial term
      // entry in the log, do not allow another at this point. Once
      // the previous change has been committed, it is safe proceed w/
      // the next. (See single-server member change algorithm, and
      // related bug described in
      // https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
      Leader(pendingMeta0)

    } else if (!state.isMember) {
      // no longer a ring member, delayed stepping down until the
      // remove was committed.

      abdicate(state, "removed")

    } else {

      // get entries to append
      val values = List.newBuilder[Entry]
      val memberChanges = List.newBuilder[Entry.MemberChange]
      val nulls = List.newBuilder[Entry.Null]

      state foreachPending {
        case e: Entry.MemberChange => memberChanges += e.withTerm(state.term)
        case e: Entry.Null         => nulls += e.withTerm(state.term)
        case e                     => values += e.withTerm(state.term)
      }

      val metaResult = nulls.result() ++ memberChanges.result()

      val entries = if (metaResult.nonEmpty) {
        // There is a member change event. We must ensure there is only one
        // membership change being processed at one time as a simple way to
        // guarantee overlapping quorums of any two transitional ring sets.
        val (rmSelf, otherMeta) = metaResult partition {
          case e: Entry.MemberChange if e.member == state.self => true
          case _ => false
        }

        // if the only proposed membership change is remove self and
        // self is the only ring member, drop it. Replicate a null entry
        // to clear it from the list of pending changes on any other
        // host.
        val metaEntry = otherMeta.headOption getOrElse {
          if (state.ring.size > 1) rmSelf.head else Entry.Null(state.term, rmSelf.head.token)
        }

        Seq(metaEntry)
      } else {
        values.result()
      }

      val afterIdx = state.log.uncommittedLastIdx
      val committed = state.log.lastIdx
      val accepted = state.log.flushedLastPos

      val maxEntriesPerAppend = 16
      var idx = afterIdx
      entries grouped(maxEntriesPerAppend) foreach { es =>
        // we can safely use the current term here, because by this
        // point we have already added the initial term entry to the log.
        val prev = Pos(idx, state.term)
        state.send(state.peers, Append(state.term, committed, accepted, prev, es.toVector))
        idx += es.length
      }

      if (entries.nonEmpty) {
        // append to log and broadcast accept
        state.appendAfter(afterIdx, entries)
        state.scheduleFlush(state.peers)
      }

      Leader(if (metaResult.isEmpty) None else Some(idx))
    }
  }
}

sealed trait BaseFollowerRecv extends Role {

  protected def sendReset(state: State, to: HostID): Role

  // Message Receipt

  // direct sender to leader
  def recvSyncProposals(state: State, sender: HostID, tokens: Seq[Long]) = {
    logInfo(state, s"Received SyncProposals from $sender, replying with Reset.")
    sendReset(state, sender)
  }

  def recvRequestProposals(state: State, sender: HostID, tokens: Seq[Long]) = {
    state.send(sender, state.proposalsMsg(tokens.toSet))
    this
  }

  def recvProposals(state: State, sender: HostID, after: Pos, es: Entries, meta: ProposalMeta) = {
    logInfo(state, s"Received Proposals from $sender, replying with Reset.")
    sendReset(state, sender)
  }

  def recvRequestAppend(state: State, sender: HostID, after: Pos, count: Int, token: Long) = {
    logInfo(state, s"Received RequestAppend from $sender, replying with Reset.")
    sendReset(state, sender)
  }

  def recvAcceptAppend(state: State, sender: HostID, acc: Pos, sync: Option[Long]) = {
    state.addAccept(acc, sender, sync)
    this
  }
}

case class Candidate(votes: Map[HostID, HostID], retryTimeout: Int = Follower.ActiveTimeout)
    extends Role with BaseFollowerRecv {
  def name: String = "CANDIDATE"

  def leader(state: State) = None

  def isActive(state: State) = true

  def advance(state: State) = 
    if (retryTimeout == 0) {
      startElection(state, "retry timeout = 0")
    } else {
      logInfo(state, s"Requesting votes from peers as a candidate in election. $retryTimeout ticks remaining.")
      state.send(state.peers, RequestVote(state.term, state.log.uncommittedLastPos))
      copy(retryTimeout = retryTimeout - 1, votes = votes + (state.self -> state.self))
    }
  
  def addProposals(state: State, after: Option[TX], es: Vector[Entry], meta: ProposalMeta) = {
    state.addProposals(after, es, meta)
    this
  }

  protected def sendReset(state: State, to: HostID) = this

  // Message Receipt

  def recvAppend(state: State, sender: HostID, comm: TX, acc: Pos, prev: Pos, last: Pos, entries: Entries, token: Long) =
    followLeader(state, sender)

  def recvReinit(state: State, sender: HostID, prev: Pos, ring: Seq[HostID], ringIdx: TX) =
    followLeader(state, sender)

  def recvCallElection(state: State, sender: HostID, immediate: Boolean) = {
    logInfo(state, s"Received CallElection from $sender, ignoring.")
    this
  }

  def recvRequestVote(state: State, sender: HostID, pos: Pos) = 
    if (state.isGreaterCandidate(sender, pos)) {
      // We are ahead of this peer. Tell the candidate we voted for ourselves.
      logInfo(state, s"Received a vote request from $sender/$pos as a candidate. We should be leader; we are ${state.self}/${state.log.uncommittedLastPos}.")
      state.send(sender, Vote(state.term, state.self, state.log.uncommittedLastPos))
      this
    } else {
      // Their log is ahead of ours, but we still must vote no. Delay our
      // election retry timeout so that if neither us nor them manages to make
      // quorum, they will restart faster than we will.
      logInfo(state, s"Received a vote request from $sender/$pos as a candidate. They should be leader; we are ${state.self}/${state.log.uncommittedLastPos}.")
      state.send(sender, Vote(state.term, state.self, state.log.uncommittedLastPos))
      copy(retryTimeout = Follower.PassiveTimeout)
    }

  def recvVote(state: State, sender: HostID, vote: HostID, votePos: Pos) = {
    val newVotes = votes + (sender -> vote)
    val yeas = newVotes.iterator collect { case (k, v) if v == state.self => k } toSet
    val nays = newVotes.iterator collect { case (k, v) if v != state.self => k } toSet

    def summarizeElection(total: Int, yeas: Int, nays: Int): String =
      s"received $total responses out of ${state.ring.size} voters: $yeas yeas and $nays nays"

    if (vote == state.self) {
      logInfo(state, s"Received yes vote from $sender.")
      if (state.ring.hasQuorum(yeas)) {
        logInfo(state, s"Election decided: this candidate won. ${summarizeElection(newVotes.size, yeas.size, nays.size)}")
        Leader.initTerm(state)
      } else {
        copy(votes = newVotes)
      }
    } else {
      // We check negative quorum here only to log an informative message.
      def maybeLogElectionFailure() = 
        if (state.ring.hasQuorum(nays)) {
          logInfo(state, s"Election decided: this candidate lost. ${summarizeElection(newVotes.size, yeas.size, nays.size)}")
        }

      // Factor out bits to shorten the logInfo lines.
      val lastPos = state.log.uncommittedLastPos
      val msgStart = s"Received no vote from $sender, who voted for $vote at $votePos"
      if (state.isGreaterCandidate(vote, votePos)) {
        logInfo(state, s"$msgStart, which is behind this log at $lastPos.")
        maybeLogElectionFailure()
        copy(votes = newVotes)
      } else {
        logInfo(state, s"$msgStart, which is ahead of this log at $lastPos. This peer will delay a next election attempt.")
        maybeLogElectionFailure()
        copy(votes = newVotes, retryTimeout = Follower.PassiveTimeout)
      }
    }
  }

  def recvReset(state: State, sender: HostID, senderLeader: Option[HostID]) = {
    logInfo(state, s"Received Reset from $sender, ignoring.")
    this
  }

  // helpers

  private def followLeader(state: State, leader: HostID) = {
    logInfo(state, s"Dropping candidacy. Becoming follower of leader $leader.")
    state.abdicated()
    state.send(leader, RequestAppend(state.term, state.log.uncommittedLastPos, 0))
    Follower(state, Follower.Leader(leader))
  }
}

object Follower {

  // Timeouts in ticks for followers to start calling for or starting an
  // election and candidates for retrying.
  //
  // The active timeout is used for followers receiving appends, and election
  // participants which have not detected a node with a higher position via a
  // RequestVote or Vote message.
  //
  // The passive timeout is used for election participants which have detected a
  // node with a higher position.
  // FIXME: This probably should be configurable 
  val ActiveTimeout = 2
  val PassiveTimeout = 7
  val SyncTimeout = 2

  @annotation.nowarn("cat=unused-params")
  def apply(state: State): Follower =
    Follower(Follower.Null)

  @annotation.nowarn("cat=unused-params")
  def apply(state: State, other: Follower.Obs): Follower =
    Follower(other)

  def apply(other: Follower.Obs): Follower = 
    Follower(other, Set.empty, -1, 0, ActiveTimeout)

  def apply(other: Follower.Obs, token: Long): Follower =
    Follower(other, Set.empty, token, SyncTimeout, ActiveTimeout)

  def apply(other: Follower.Obs, token: Long, syncTimeout: Int): Follower = 
    Follower(other, Set.empty, token, syncTimeout, ActiveTimeout)

  sealed trait Obs {
    def get: Option[HostID]
  }

  def LeaderOrElse(l: Option[HostID], alt: HostID) =
    l match {
      case Some(l) => Leader(l)
      case None    => Observed(alt)
    }

  def ObservedOrNull(set: Set[HostID]) =
    if (set.isEmpty) Null else Observed(set.head)

  case object Null extends Obs { def get = None }
  case class Leader(id: HostID) extends Obs {
    def get = Some(id)
  }
  case class Vote(id: HostID, lastPos: Pos) extends Obs { def get = Some(id) }
  case class Observed(id: HostID) extends Obs { def get = Some(id) }
}

case class Follower(
  observed: Follower.Obs,
  calls: Set[HostID],
  syncToken: Long,
  syncTimeout: Int,
  callTimeout: Int)
    extends Role with BaseFollowerRecv {
  def name: String = "FOLLOWER"

  private def isSyncInProgress = syncToken >= 0

  private def isSyncReply(token: Long) = isSyncInProgress && token == syncToken

  private def observedIsAhead(state: State) =
    observed.get.exists(state.accepted(_) > state.log.uncommittedLastPos)

  private def sendSyncMsg(to: HostID, state: State, pos: Pos, init: Option[String] = None) = {
    val token = ThreadLocalRandom.current.nextLong(Long.MaxValue)
    val size = init match {
      case Some(msg) =>
        logInfo(state, s"Initiating log sync at $pos ($token): $msg")
        0
      case None =>
        logInfo(state, s"Continuing log sync from $pos ($token).")
        MaxAppendEntries
    }
    state.send(to, RequestAppend(state.term, pos, size, token))
    token
  }

  def leader(state: State) = observed match {
    case Follower.Leader(i) => Some(i)
    case _                  => None
  }

  def isActive(state: State) =
    (leader(state).isEmpty && state.isMember) ||
    observedIsAhead(state) ||
    calls.nonEmpty ||
    state.isActive

  def advance(state: State) =
    if (calls contains state.self) {
      if (state.isMember && state.ring.hasQuorum(calls)) {
        startElection(state, "responding to own call")
      } else {
        state.stats.incrElectionCalls()
        logInfo(state, s"Calling for election to replace leader ${leader(state).getOrElse("<None>")}.")
        state.send(state.peers ++ observed.get, CallElection(state.term, immediate = false))
        this
      }
    } else {
      var role = this

      if (!isSyncInProgress) {
        observed.get foreach { obs =>
          // if the observed node's position is higher than ours, initiate a log sync.
          if (observedIsAhead(state)) {
            val pos = state.log.uncommittedLastPos 
            val token = sendSyncMsg(obs, state, pos, init = Some(s"Log state is behind leader at ${state.accepted(obs)}."))
            role = copy(syncToken = token, syncTimeout = Follower.SyncTimeout)
          } else {
            // otherwise probe the leader in order to get its latest accept state.
            state.send(obs, RequestAppend(state.term, state.log.uncommittedLastPos, 0))
          }

          // sync proposals if we have them.
          if (state.unavailableCommittedEntries < MaxBufferedAppends) {
            val b = Vector.newBuilder[Long]
            state foreachPending { e => b += e.token }
            val tokens = b.result() take MaxSyncTokens
            if (tokens.nonEmpty) state.send(obs, SyncProposals(state.term, tokens))
          }
        }
      } else {
        role = role.copy(syncTimeout = role.syncTimeout - 1)
        if (role.syncTimeout == 0) {
          role = role.copy(syncToken = -1)
          logInfo(state, s"Giving up on log sync $syncToken.")
        } else {
          logInfo(state, s"Log sync countdown ${role.syncTimeout} ticks remaining.")
        }
      }

      if (callTimeout > 0) {
        role = role.copy(callTimeout = role.callTimeout - 1)
      } else {
        role = role.copy(calls = role.calls + state.self)
      }

      role
    }

  def addProposals(state: State, after: Option[TX], es: Vector[Entry], meta: ProposalMeta) = {
    val added = state.addProposals(after, es, meta)
    if (added.nonEmpty) {
      state.send(observed.get, state.proposalsMsg(added, meta))
    }
    this
  }

  protected def sendReset(state: State, to: HostID) = {
    state.send(to, Reset(state.term, leader(state)))
    this
  }

  // Message Receipt

  def recvAppend(state: State, sender: HostID, comm: TX, lAcc: Pos, prev: Pos, last: Pos, entries: Entries, token: Long) = {
    state.abdicated()

    if (leader(state).exists { _ != sender }) {
      val msg = s"Follower received Append from $sender while it " +
        s"believes ${leader(state)} to be the leader for ${state.term}."
      val exn = new IllegalStateException(msg)
      logException(exn)
      if (state.isMember) {
        startElection(state, msg)
      } else {
        this
      }
    } else {

      // update our pov of the ring based on the leader's communication
      state.updateCommittedIdx(comm)
      state.addAccept(lAcc, sender, None)

      if (prev.idx < state.log.prevIdx) {
        // stale append. ignore since it falls before the head of the log.
        this

      } else if (prev.idx > state.log.uncommittedLastIdx) {
        // append is ahead of our log, save the append for later.
        state.bufferAppend(prev.idx, entries)
        Follower(Follower.Leader(sender), syncToken, syncTimeout)

      } else if (state.log.containsPos(prev)) {
        // our log is compatible with the leader's. accept the append

        // Check to see if we can skip appending
        val pos = if (state.log.containsPos(last)) {
          last
        } else {
          state.appendAfter(prev.idx, entries)
        }

        // We are caught up to the live append stream from the leader.
        val caughtUp = pos >= lAcc

        // broadcast accept if it will push forward the committed state
        val toNotify = if (caughtUp || pos.idx > state.globalCommittedIdx) {
          if (state.isMember) state.peers + sender else Set(sender)
        } else {
          Set.empty[HostID]
        }

        // flush and broadcast accept, if necessary
        if (pos.idx > state.log.flushedLastIdx) {
          state.scheduleFlush(toNotify)
        } else if (pos >= state.selfAcceptedPos) {
          // ensure we ourselves acknowledge reacceptance of our log
          state.addAccept(pos, state.self, None)
          state.send(toNotify, AcceptAppend(state.term, pos, None))
        }

        if (caughtUp) {
          // Clear out sync state.
          if (isSyncInProgress) {
            logInfo(state, s"Ending log sync $syncToken.")
          }
          Follower(Follower.Leader(sender))

        } else {
          if (isSyncReply(token)) {
            // response to RequestAppend. Keep 'em coming
            val token = sendSyncMsg(sender, state, pos)
            Follower(Follower.Leader(sender), token)
          } else {
            // redundant append or heartbeat, so do not react to it.
            Follower(Follower.Leader(sender), syncToken, syncTimeout)
          }
        }

      } else {
        // Cannot accept this append due to log conflict.

        if (isSyncReply(token)) {
          // If there is a conflict, just start over with our committed
          // position, which is guaranteed to be compatible.
          val pos = state.log.lastPos
          val token = sendSyncMsg(sender, state, pos, init = Some("Detected log conflict."))
          Follower(Follower.Leader(sender), token)
        } else {
          // This can happen when the live append stream conflicts with this
          // node's log, which can happen at the beginning of a term before the
          // local node has trimmed its copy of the log as part of the sync
          // process.
          logInfo(state, s"Live append stream log conflict.")
          this
        }
      }
    }
  }

  def recvReinit(state: State, sender: HostID, prev: Pos, ring: Seq[HostID], ringIdx: TX) = {
    logInfo(state, s"Received Reinit from $sender, abdicating.")
    state.abdicated()
    if (state.reinit(prev, ring.toSet, ringIdx)) {
      Follower(state, Follower.Leader(sender))
    } else {
      this
    }
  }

  def recvCallElection(state: State, sender: HostID, immediate: Boolean) =
    if (state.isMember && immediate) {
      val msg = s"Received call for immediate election from $sender"
      logInfo(state, msg)
      startElection(state, msg)
    } else {
      logInfo(state, s"Received call for election from $sender")
      copy(calls = calls + sender)
    }

  def recvRequestVote(state: State, sender: HostID, pos: Pos) =
    if (state.isMember) {
      observed match {
        case Follower.Leader(i) =>
          logInfo(state, s"Ignoring vote request from $sender. Already following leader $i.")
          // Sending a no vote (vote for leader) here. This will hint to the
          // sender that they cannot win as there is already a leader.
          state.send(sender, Vote(state.term, state.self, Pos.MaxValue))
          this

        case Follower.Vote(vote, votePos) =>
          // Already voted.
          if (vote == sender) {
            logInfo(state, s"Already voted for $sender. (Resending vote message.)")
          } else {
            logInfo(state, s"Not voting for $sender as already voted for $vote. (Notifying sender of prior vote.)")
          }
          state.send(sender, Vote(state.term, vote, votePos))
          copy(calls = calls + sender)

        case _ if pos >= state.log.uncommittedLastPos =>
          // they are a valid leader and we haven't voted yet. Vote for the requester.
          logInfo(state, s"Voting for $sender with log position of $pos.")
          state.send(sender, Vote(state.term, sender, pos))

          // Set our timeout for potentially triggering another election to
          // longer than the candidate's short delay, so we do not risk
          // competing with them if they fail to win this term's election.
          Follower(state, Follower.Vote(sender, pos)).copy(
            calls = calls + sender,
            callTimeout = Follower.PassiveTimeout)

        case _ =>
          logInfo(state, s"Cannot vote for $sender. This node's log position ${state.log.uncommittedLastPos} is higher than candidate's of $pos.")
          // Sending a "no" vote (vote for self) here. This will hint to the
          // sender that they cannot win this node and will back off on their
          // timeout. We preserve our timeout here so that if no candidate wins
          // we will trigger a new election first.
          state.send(sender, Vote(state.term, state.self, state.log.uncommittedLastPos))
          copy(calls = calls + sender)
      }
    } else {
      logInfo(state, s"Received RequestVote from $sender when not a member, ignoring.")
      copy(calls = calls + sender)
    }

  def recvVote(state: State, sender: HostID, vote: HostID, votePos: Pos) = {
    logInfo(state, s"Received Vote from $sender, ignoring.")
    this
  }

  def recvReset(state: State, sender: HostID, senderLeader: Option[HostID]) = {
    logInfo(state, s"Received Reset from $sender, current state ($observed, $senderLeader).")
    (observed, senderLeader) match {
      case (Follower.Leader(_), _)  => this
      case (_, Some(l))             =>
        state.abdicated()
        Follower(state, Follower.Leader(l))
      case (Follower.Null, _)       => Follower(state, Follower.Observed(sender))
      case (_, _)                   => this
    }
  }
}
