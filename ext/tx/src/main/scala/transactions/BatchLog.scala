package fauna.tx.transaction

import fauna.atoms._
import fauna.codex.cbor.CBOR
import fauna.exec.IdxWaitersMap
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.net.HostService
import fauna.tx.consensus.{ RejectedEntryException, ReplicatedLog }
import fauna.tx.log._
import scala.collection.immutable.SortedMap
import scala.collection.mutable.Builder
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal

import Batch._

object ScopeSubEntry {
  implicit val codec = CBOR.TupleCodec[ScopeSubEntry]
}

case class ScopeSubEntry(scope: ScopeID, txns: Vector[Txn])

object BatchLog {
  type Log = ReplicatedLog[(Epoch, Vector[ScopeSubEntry])]
  val Log = ReplicatedLog[(Epoch, Vector[ScopeSubEntry])]
}

/**
  * The BatchLog wraps a replicated log of submitted batches and
  * exposes normalized view of transactions per epoch. The rules are:
  *
  * - Every epoch has a batch, which may be empty.
  * - Epochs are observed in strict order.
  *
  * The requirements on batchlog input is more lax. Batches can be
  * submitted multiple times for an epoch or out of order, but the
  * normalization algorithm prevents consumers from seeing these
  * irregularities.
  *
  * Each log replica submits batch on a regularly scheduled heartbeat,
  * and tags the batch with its current perceived epoch according to
  * the node's local time. If there are no pending transactions, the
  * lognode will still add an empty batch in order to advance the
  * applied transaction time on consuming datanodes. (Currently only
  * the RAFT leader adds empty batches in order to cut down on log
  * chatter)
  *
  * The batchlog keeps a local buffer for the pending batch, and the
  * last seen epoch. On processing a log entry, the entry's batch is
  * added to the pending buffer, then if the entry's epoch is greater
  * than the last seen epoch, the batch buffer is flushed assigned the
  * current entry's epoch. The last seen epoch is set to the entry's
  * epoch, and the batch is made available to downstream datanodes.
  */
class BatchLog(
  val id: SegmentID,
  val log: BatchLog.Log,
  commitWindow: Int,
  cachedBatches: Int)(implicit ec: ExecutionContext)
    extends LogLike[Epoch, Vector[ScopeSubEntry]]
    with ExceptionLogging { self =>

  type E = LogEntry[Epoch, Vector[ScopeSubEntry]]

  private type VB[T] = Builder[T, Vector[T]]

  // indexing, caching

  // Index of Epochs to indexes of the last entry of that epoch.
  private val index = new TailIndex(TX.MinValue.toLong)

  @volatile private var cache = SortedMap.empty[Epoch, Vector[ScopeSubEntry]]
  private var cacheSize = 0

  private final class BatchesBuilder(v: VB[Txn] = Vector.newBuilder) {
    def addEvents(es: Vector[Txn]) =
      v ++= es

    def add(es: Vector[ScopeSubEntry]) =
      es foreach {
        case ScopeSubEntry(scope, txns) =>
          require(scope == ScopeID.RootID)
          v ++= txns
      }

    def result =
      Vector(ScopeSubEntry(ScopeID.RootID, v.result()))

    def clear() = v.clear()
  }

  @volatile private[this] var _prevEpoch = Epoch.MinValue
  private[this] val lastEpoch = new IdxWaitersMap(Epoch.MinValue)
  private[this] val batchBuf = new BatchesBuilder

  @volatile private[this] var canReinit = false

  subscribe(TX.MinValue)

  private[this] def subscribe(after: TX): Unit = {
    @volatile var recoverable = true
    @volatile var resubscribeAfter = after
    val subscription = log.subscribe(after, Duration.Inf) {
      case Log.Entries(_, es) =>
        while (es.hasNext) {
          val e = es.next()
          recoverable = false
          e.toOption foreach {
            case (epoch, scopes) =>
              batchBuf.add(scopes)

              if (epoch > lastEpoch.idx) {
                val batches = batchBuf.result

                // Add an index entry for this log entry since it is the
                // last of this batch.
                index.add(epoch.idx, e.idx.toLong)
                batchBuf.clear()

                // first entry, so initialize prevEpoch. Do not cache it
                // since it may be the initial partial batch meant to
                // kick off batch rollup.
                if (_prevEpoch == Epoch.MinValue) {
                  if (log.prevIdx == TX.MinValue) {
                    // log has never been truncated, so this really is
                    // the very first batch.
                    _prevEpoch = epoch - 1
                  } else {
                    _prevEpoch = epoch
                  }

                } else {
                  if (batches.nonEmpty) {
                    val c0 = cache + (epoch -> batches)

                    if (cacheSize < cachedBatches) {
                      cache = c0
                      cacheSize += 1
                    } else {
                      cache = c0 drop 1
                    }
                  }
                }

                lastEpoch.update(epoch)
              }
          }
          resubscribeAfter = e.idx
          recoverable = true
        }

        FutureTrue

      // log was reinitialized. This can only happen after every node is
      // past this position, so it is safe to reset intermediate state.
      case Log.Reinit(_) =>
        batchBuf.clear()
        cache = SortedMap.empty
        _prevEpoch = Epoch.MinValue
        FutureTrue

      case Log.Idle(_) => FutureTrue

      case Log.Closed => FutureFalse
    }

    subscription.failed foreach { t =>
      logException(t)
      // We only restart the subscription on nonfatal exceptions arising
      // in the underlying log's subscription/entry iteration mechanism.
      // This is a workaround for transient file reading exceptions that
      // sometime arise in the said mechanism. We use the "recoverable" flag to
      // specifically exclude restarting if the exception arose in this class'
      // code; we might be able to recover from some of those too but we don't
      // expect any and would conservatively rather not even attempt to.
      t match {
        case NonFatal(_) if recoverable => subscribe(resubscribeAfter)
        case _ =>
          getLogger().error("Can not restart BatchLog.subscribe loop", t)
      }
    }
  }

  def abdicate(reason: String): Unit =
    log.abdicate(reason)

  def close() = log.close()

  def isClosed = log.isClosed

  def isLeader = log.isLeader

  def isMember = log.isMember

  def shouldEmitEmpty =
    log.isLeader || !log.leader.exists(log.ring)

  def leader = log.leader

  def updateMembership(
    newMembers: Set[HostID],
    service: HostService): Future[Unit] = {
    def aloneMyself(hs: Set[HostID]) =
      hs.size == 1 && hs.contains(log.self)

    val curMembers = log.ring
    if (!log.isLeader && aloneMyself(newMembers) && !aloneMyself(curMembers)) {
      // Special case of shrinking the cluster from 2 to 1 members, the remaining
      // member being ourselves in a non-leader capacity. The only way to do this
      // is to reinitialize the log on the remaining single member. It'll add the
      // requisite membership change message(s) and then self-elect.
      //
      // This can't be done safely in the general Raft case, but we know that
      // the departed leader won't return in this special case due to the way
      // newMembers is externally (to this Raft log) derived from log topology
      // state: the members are drawn from cluster members. If a node stops being
      // a member in a log segment this means it's either removed from the
      // cluster, or its replica is no longer a log replica.
      //
      // * If it's removed from the cluster, it will never come back with the
      // same identity.
      //
      // * If its replica is no longer a log replica, the former leader could
      // come back if the replica is later reclassified again back to a log
      // replica. If it was up when replica was made non-log, then it will
      // abdicate, leave the ring, and delete its local copy of the log. If it
      // was down in between then the log reinitialization logic here for the
      // remaining node will kick in, and when the ex-leader later comes back up
      // (say by that time the replica was again log replica) it'll start out as
      // a follower, see this node as a leader with a higher term, and accept
      // its log.
      //
      // Note it is still possible that the old leader is active for a while
      // before it notices it needs to stop being a member. For this reason, it
      // is not safe to immediately reinitialize the log, we only set a flag here
      // marking that it is allowed to do so. The actual reinitialization will be
      // performed by tryReinit() invoked by the consensus stall detector
      // heartbeat in LogNode after the log segment failed to make progress for
      // some time.
      canReinit = true
      Future.unit
    } else {
      canReinit = false

      val dead = curMembers filterNot { service.isLive(_) }
      val adds = newMembers.diff(curMembers)
      val removes = curMembers.diff(newMembers)
      val zombies = removes.intersect(dead)

      // Only enact one change at a time.
      // If any current members are failed, remove them first if possible.
      // If all current members are alive, expand before contracting.
      if (zombies.nonEmpty) {
        log.removeMember(zombies.head) flatMap { _ =>
          updateMembership(newMembers, service)
        }
      } else if (adds.nonEmpty) {
        log.addMember(adds.head) flatMap { _ =>
          updateMembership(newMembers, service)
        }
      } else if (removes.nonEmpty) {
        log.removeMember(removes.head) flatMap { _ =>
          updateMembership(newMembers, service)
        }
      } else {
        Future.unit
      }
    }
  }

  def tryReinit(): Future[Boolean] =
    if (canReinit) {
      canReinit = false
      getLogger().warn(s"This node ${log.self} has reinitialized the log segment $id to make progress after it was the only node left in it.")
      log.init() flatMap { _ => FutureTrue }
    } else {
      FutureFalse
    }

  // Log interface

  def prevIdx = _prevEpoch

  def lastIdx = lastEpoch.idx

  def add(epoch: Epoch, evs: Vector[Txn]) = {
    val buf = new BatchesBuilder

    buf.addEvents(evs)

    log.add((epoch, buf.result), log.lastIdx + commitWindow) map {
      _ => true
    } recover {
      case _: RejectedEntryException => false
    }
  }

  def truncate(toIdx: Epoch): Unit = {
    if (toIdx > lastIdx) {
      throw new IllegalArgumentException(s"$toIdx is greater than log's lastIdx $lastIdx")
    }

    if (toIdx > prevIdx) {
      _prevEpoch = toIdx
      // keep the last entry of toIdx around, so that we can
      // recover prevEpoch from it when reopening the log.
      log.truncate(TX(index(_prevEpoch.idx)) - 1)
    }
  }

  def entries(after: Epoch): EntriesIterator[E] =
    entries(after, lastIdx)

  def entries(after0: Epoch, lst: Epoch): EntriesIterator[E] = {
    // must respect _prevEpoch in
    // order to not expose partial batches
    val after = after0 max _prevEpoch

    if (after >= lst) {
      EntriesIterator.empty
    } else {
      val c = cache
      val fr = after + 1

      if (c.nonEmpty && (fr >= c.head._1)) {
        val tail = c.rangeFrom(fr).iterator map { case (e, bs) => LogEntry(e, bs) }
        val iter = if (tail.nonEmpty) tail else Iterator(LogEntry(lst, Vector.empty))

        EntriesIterator.empty.delegateTo(iter)
      } else {
        // start log iteration with the last entry of `after`, in
        // order to find the correct start of the first batch.
        val afterIdx = TX(index(after.idx)) - 1

        var ep = Epoch.MinValue
        val buf = new BatchesBuilder

        val iter = log.entries(afterIdx) flatMap { e =>
          e.toOption flatMap {
            case (epoch, scopes) =>
              buf.add(scopes)

              if (epoch > ep) {
                val batches = buf.result
                buf.clear()

                val rv = if (batches.nonEmpty) {
                  Some(LogEntry(epoch, batches))
                } else {
                  None
                }

                ep = epoch
                rv
              } else {
                None
              }
          }
        } dropWhile {
          // Drop all entries with an epoch <= after. This has the
          // effect of dropping the partial entry used to kick off
          // batch rollup, and earlier entries retrieved due to the
          // coarseness of the tail index.
          _.idx <= after
        }

        if (iter.nonEmpty) iter else {
          iter.release()
          EntriesIterator.empty.delegateTo(Iterator(LogEntry(lst, Vector.empty)))
        }
      }
    }
  }

  def poll(after: Epoch, within: Duration) = lastEpoch.get(after + 1, within)

  def subscribe(after: Epoch, idle: Duration)(f: Log.Sink[Epoch, E])(implicit ec: ExecutionContext): Future[Unit] =
    poll(after, idle) flatMap {
      case true =>
        val lst = lastIdx // limit to committed entries
        val prev = prevIdx
        val es = entries(after, lst).toList

        if (prev > after) {
          f(Log.Reinit(prev)) flatMap {
            if (_) subscribe(prev, idle)(f) else Future.unit
          }
        } else {
          f(Log.Entries(after, es.iterator)) flatMap {
            if (_) subscribe(es.last.idx, idle)(f) else Future.unit
          }
        }

      case false =>
        f(Log.Idle(after)) flatMap {
          if (_) subscribe(after, idle)(f) else Future.unit
        }
    }
}
