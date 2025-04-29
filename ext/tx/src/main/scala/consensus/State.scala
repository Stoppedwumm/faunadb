package fauna.tx.consensus

import fauna.atoms.HostID
import fauna.exec._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.tx.consensus.log._
import fauna.tx.consensus.messages._
import fauna.tx.log.{ EntriesIterator, LogClosedException, TX }
import java.util.PriorityQueue
import java.util.concurrent.{ ConcurrentHashMap, TimeoutException, ThreadLocalRandom }
import scala.collection.mutable.{ Map => MMap }
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

import State._

object State {
  type Accepts = Map[HostID, Pos]

  final val MaxAppendEntries = 16384
  final val MaxBufferedAppends = 512
  final val MaxSyncTokens = 2048
  final val MaxProposals = 1024
  // Empirically, single leaders, followers, and rolling restarts create ranges about
  // ~8K large without spending too much time scanning the log. 16K should be large
  // enough that normal cluster operations don't delay syncing proposals to much.
  final val MaxProposalsRange = 16000
  final val MaxMessageBytes = 4 * 1024 * 1024

  object Accepts {
    final def empty = MMap.empty[HostID, Pos].withDefaultValue(Pos.MinValue)
  }

  private def logger = getLogger()
}

abstract class TokenWaitersMap extends AsyncWaitersMap[Long, TX] {

  protected val timer = Timer.Global
  protected val pendingPromises = new ConcurrentHashMap[Long, PromiseRef]

  def size = pendingPromises.size

  protected def timeoutForKey(token: Long) =
    Future.failed(new TimeoutException(s"Timed out waiting for token $token."))

  protected def existingForKey(token: Long) = None
}

class PendingTokenWaitersMap extends TokenWaitersMap {
  def complete(token: Long, tx: TX): Unit =
    Option(pendingPromises.get(token)) foreach {
      _.trySuccess(tx)
    }

  def reject(token: Long, within: TX): Unit =
    Option(pendingPromises.get(token)) foreach {
      _.tryFailure(new RejectedEntryException(token, within))
    }
}

class SyncTokenWaitersMap extends TokenWaitersMap {
  protected val accsMap = new ConcurrentHashMap[Long, Map[HostID, Pos]]

  override def get(key: Long, within: Duration) = {
    implicit val ec = ImmediateExecutionContext
    val fut = super.get(key, within)
    fut ensure { accsMap.remove(key) }
    fut
  }

  def add(token: Long, id: HostID, pos: Pos, ring: Ring): Unit = {
    Option(pendingPromises.get(token)) foreach { r =>
      val accs = accsMap.compute(token, {
        case (_, null) => Map(id -> pos)
        case (_, m) => m + (id -> pos)
      })

      if (ring.hasQuorum(accs.keySet)) {
        r.trySuccess(accs.values.max.idx)
      }

      if (r.promise.isCompleted) {
        accsMap.remove(token)
      }
    }
  }
}

// `ring` is reinjected so that State's ring can be dependent on
// another log's ring
class State(
  _ring: => Ring,
  val log: ReplicatedLogStore,
  transport: ReplicatedLogTransport,
  val stats: ReplicatedLog.Stats,
  name: String,
  alwaysActive: Boolean) {

  @volatile private[this] var _globalCommittedIdx = log.lastIdx
  def globalCommittedIdx = _globalCommittedIdx

  private[this] val accepts = Accepts.empty
  def accepted(i: HostID) = accepts(i)
  def selfAcceptedPos = accepts(self)

  private[this] val appendBuffer =
    new PriorityQueue[(TX, Seq[Entry])](MaxBufferedAppends, _._1 compare _._1)

  private[this] case class Pending(entry: Entry, after: TX, within: TX, expiry: Timestamp)

  // State tracks pending entries which this node is aware of as they
  // move from *new* to *committed* through the following two mutable
  // maps. Each entry's identity is represented by its token.
  //
  // - Entries are/ added to `pending` via addProposals().
  // - Entries move from `pending` to `pendingInLog` as they pass
  //   through appendAfter(). If appendAfter() results in discarding
  //   messages from the log tail, affected entries are moved from
  //   `pendingInLog` back to `pending`
  // - Finally in `updateCommittedidx()`, each successfully committed
  //   entry is removed from `pendingInlog` and its promise marked as
  //   completed. Each entry in `pending` which cannot be committed
  //   because its within index has passed is removed and its promise
  //   is marked as failed.
  private[this] val pending = MMap.empty[Long, Pending]
  private[this] val pendingInLog = MMap.empty[TX, Pending]

  private[this] val pendingWaiters = new PendingTokenWaitersMap
  def waitForPending(token: Long, within: Duration) = pendingWaiters.get(token, within)

  private[this] val syncWaiters = new SyncTokenWaitersMap
  def waitForSync(token: Long, within: Duration) = syncWaiters.get(token, within)

  addAccept(log.flushedLastPos, self, None)

  override def toString = s"State($self, $ring, $term, ${log.lastPos}, p${pendingWaiters.size}, $accepts, $log)"

  def randomToken = ThreadLocalRandom.current.nextLong

  def close() = {
    val ex = new LogClosedException
    pendingWaiters.shutdown(ex)
    syncWaiters.shutdown(ex)
    log.close()
  }

  @volatile private[this] var _abdicating: Boolean = false
  def isAbdicating = _abdicating

  private[consensus] def abdicate(reason: String) = {
    _abdicating = true
    val lastPos = log.lastPos
    val potential = Random.shuffle(peers)
    val candidate = potential collectFirst {
      case id if accepted(id) >= lastPos => Set(id)
    }
    val candidates = candidate getOrElse potential
    logInfo("LEADER", s"Abdicating to $candidates: $reason.")
    send(candidates, CallElection(term, true))
  }

  private[consensus] def abdicated(): Unit = _abdicating = false

  // messaging

  def send(to: HostID, message: Message): Unit = {
    transport.send(to, message)
    stats.incrMessageSend()
  }

  def send(to: Iterable[HostID], msg: Message): Unit =
    to foreach { send(_, msg) }

  // ring state

  def self = log.self

  def ring = _ring

  def isMember = ring contains self

  def peers = ring.peers(self)

  // log state

  def term = log.term

  def isGreaterCandidate(cnd: HostID, cndPos: Pos) = {
    val cmp = log.uncommittedLastPos compare cndPos match {
      case 0 => self compare cnd
      case cmp => cmp
    }

    cmp > 0
  }

  def unavailableCommittedEntries =
    globalCommittedIdx diff log.lastIdx

  def isActive = alwaysActive || pendingWaiters.nonEmpty || syncWaiters.nonEmpty || isWriteUncommitted

  def isWriteUncommitted =
    if (pending.nonEmpty || log.uncommittedLastIdx != log.lastIdx) {
      true
    } else {
      log.lastPos != Pos(globalCommittedIdx, term)
    }

  def committedEntries(after: TX) = log.entries(after)

  def missingProposals(tokens: Seq[Long]) =
    tokens.iterator.filterNot { pending contains }.toSet

  def foreachPending(f: Entry => Unit) = {
    val lst = log.uncommittedLastIdx
    pending.values foreach { p =>
      if (p.within > lst) f(p.entry)
    }
  }

  // State updates

  private def setTerm(t: Term) = {
    log.setTerm(t)
    stats.recordTerm(t)
  }

  def resetRing() = log.resetRing()

  def reset(newTerm: Term) =
    if (term != newTerm) {
      setTerm(newTerm)
      accepts.clear()
      appendBuffer.clear()
    }

  def bufferAppend(after: TX, es: Seq[Entry]): Unit = {
    require(after > log.uncommittedLastIdx)
    appendBuffer.offer((after, es))
  }

  def appendAfter(after: TX, es: Seq[Entry]): Pos = {
    // If there are any buffered appends, then add those as well.
    // buffered appends are reset every term and so all come from the
    // current leader. So if they are not compatible yet, they will be
    // when the log itself is caught up.
    val b = Seq.newBuilder[Entry]
    b ++= es

    var lst = after + es.size
    var e = appendBuffer.peek
    while ((e ne null) && e._1 <= lst) {
      if (e._1 == lst) {
        b ++= e._2
        lst += e._2.size
      }

      assert(appendBuffer.poll eq e, "appendBuffer queue changed underneath appendAfter().")
      e = appendBuffer.peek
    }

    val entries = b.result()
    val pos = log.appendAfter(after, entries)

    pendingInLog foreach {
      case (idx, p) =>
        if (idx > after) {
          pendingInLog -= idx
          pending += (p.entry.token -> p)
        }
    }

    var idx = after
    entries foreach { e =>
      idx += 1
      pending.get(e.token) foreach { p =>
        pendingInLog(idx) = p
        pending -= e.token
      }
    }

    pos
  }

  def scheduleFlush(notify: Set[HostID]): Unit =
    transport.scheduleFlush(notify)

  def flush(notify: Set[HostID]): Unit = {
    val pos = log.flush()

    // FIXME: current term doesn't matter for accepts.
    val msg = AcceptAppend(term, pos, None)
    send(notify, msg)

    // FIXME: it's a little silly to do it this way but addAccept
    // needs to be handled on the message thread.
    transport.schedule(self, Future.successful(msg))
  }

  // If the new accepted position is within the current term, then
  // there's no possibility the sender of being truncated in the future.
  // However, if it is a prev term, then it's possible the log index
  // is still not valid. Pos ordering will still mean that the
  // accepted pos will be strictly ordered first, but it is not safe
  // to update the global committed index if the accepted term does
  // not equal the current term.
  def addAccept(pos: Pos, id: HostID, syncToken: Option[Long]) = {
    if (pos > accepts(id)) {
      accepts(id) = pos

      val globalPos = ring.minCommitted(accepts) getOrElse Pos.MinValue
      val committed = Some(globalPos) filter { _.term == term } getOrElse Pos.MinValue

      updateCommittedIdx(committed.idx)
    }

    syncToken foreach { syncWaiters.add(_, id, pos, ring) }
  }

  // Uses a slightly more conservative approach than vanilla Raft.
  // Only updates local committedIdx once an entry is in the log for
  // the current term. This ensures that there are no invalid log
  // entries from previous terms that have yet to be discarded,
  // because said entries must be discarded before appending any
  // entries for the current term.
  def updateCommittedIdx(idx: TX) = {
    _globalCommittedIdx = globalCommittedIdx max idx

    if (log.lastIdx < idx && log.flushedLastPos.term == term) {
      val committed = idx min log.flushedLastIdx

      stats.recordCommittedIdx(committed)
      stats.countCommits(committed.toLong - log.lastIdx.toLong)
      log.updateCommittedIdx(committed)

      var count = 0

      // GC committed pending
      pendingInLog foreach {
        case (tx, p) =>
          if (tx <= log.lastIdx) {
            pendingInLog -= tx
            pendingWaiters.complete(p.entry.token, tx)
          }
          count += 1
      }

      // GC rejected pending
      pending.values foreach { p =>
        if (p.within <= log.lastIdx) {
          pending -= p.entry.token
          pendingWaiters.reject(p.entry.token, p.within)
        }
        count += 1
      }

      stats.recordPending(count)
    }
  }

  def addProposals(after: Option[TX], es: Seq[Entry], meta: Seq[(TX, Timestamp)]) =
    if (after exists { log.uncommittedLastIdx.diff(_) > MaxProposalsRange }) {
      logInfo(
        "LEADER",
        "Dropping proposals. Follower is too outdated " +
          s"(after=${after.get}, uncommitedIdx=${log.uncommittedLastIdx})."
      )
      stats.incrProposalsDropped()
      Vector.empty
    } else {
      val lst = log.lastIdx
      val toAdd = {
        val (add, reject) = es.iterator
          .zip(meta)
          .partition { case (_, (w, _)) => w > lst }

        // we can't just drop these rejected entries on the floor, as they may have
        // waiters.
        reject foreach { case (e, (w, _)) => pendingWaiters.reject(e.token, w) }

        add.map { case (e, (w, ts)) => (e, w, ts) }.toSeq
      }

      val inLog = after match {
        case Some(a) =>
          log.getTokenIdx(
            toAdd.iterator.map { _._1.token }.toSet,
            a,
            log.uncommittedLastIdx)
        case None =>
          Map.empty[Long, TX]
      }

      val added = Vector.newBuilder[Entry]

      toAdd foreach { case (e, w, ts) =>
        // FIXME: check for expired timestamps
        if (!inLog.contains(e.token)) {
          added += e
          pending(e.token) = Pending(e, lst, w, ts)
        }
      }

      val result = added.result()
      if (result.nonEmpty) stats.incrAdds()

      result
    }

  def reinit(prev: Pos, ring: Set[HostID], ringIdx: TX) =
    if (prev <= log.uncommittedLastPos) {
      false
    } else {
      stats.incrReinits()
      log.reinit(prev, ring, ringIdx)
      updateCommittedIdx(prev.idx)
      true
    }

  // Message helpers

  def proposalsMsg(entries: Vector[Entry], meta: ProposalMeta) =
    Proposals(term, log.uncommittedLastPos, entries, meta)

  def proposalsMsg(tokens: Set[Long]): Proposals = {
    val entries = Vector.newBuilder[Entry]
    val meta = Vector.newBuilder[(TX, Timestamp)]

    var currBytes = 0

    tokens foreach { t =>
      pending.get(t) foreach { p =>
        val size = p.entry.bytesSize
        currBytes += size
        // always allow at least one entry.
        if (currBytes == size || currBytes <= MaxMessageBytes) {
          entries += p.entry
          meta += ((p.within, p.expiry))
        }
      }
    }

    proposalsMsg(entries.result(), meta.result())
  }

  def heartbeatAppendMsg: Message = {
    val committed = log.lastIdx
    val acc = selfAcceptedPos
    Append(term, committed, acc, acc, Vector.empty, -1)
  }

  def appendMsg(prevIdx: TX, count: Int, token: Long = -1): Message = {
    val prevTerm = log.getTerm(prevIdx) getOrElse log.prevPos.term
    val acc = selfAcceptedPos

    val iter = if (count <= 0) EntriesIterator.empty else {
      var currBytes = 0
      log.uncommittedEntries(prevIdx) take count map { _.get } takeWhile { e =>
        val size = e.bytesSize
        currBytes += size
        // always allow at least one entry.
        currBytes == size || currBytes <= MaxMessageBytes
      }
    }

    val entries = iter.toVector
    val prev = Pos(prevIdx, prevTerm)

    iter releaseAfter { _ =>
      Append(term, log.lastIdx, acc, prev, entries, token)
    }
  }

  private def msgWithPrefix(role: String, msg: => String): String =
    s"Consensus $name $term $role $self: $msg"

  def logInfo(role: String, msg: => String) =
    logger.info(msgWithPrefix(role, msg))

  def logWarn(role: String, msg: => String) =
    logger.warn(msgWithPrefix(role, msg))
}
