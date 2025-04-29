package fauna.tx.transaction

import fauna.atoms._
import fauna.codex.cbor.CBOR
import fauna.exec.ImmediateExecutionContext
import fauna.lang.{ TimeBound, Timestamp, Timing }
import fauna.lang.syntax._
import fauna.net._
import fauna.net.bus.HandlerID
import fauna.stats.StatsRecorder
import fauna.trace.{ DeadlineExceeded, GlobalTracer }
import fauna.tx.log.{ Log, LogClosedException }
import java.util.concurrent.{ ConcurrentHashMap, LinkedBlockingQueue }
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicInteger }
import scala.annotation.unused
import scala.collection.mutable.{ Map => MMap }
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration._
import Batch._
import DataMessage._

object LogNode {

  private[transaction] final val MaxPipelinedEpochs = 4096

  private sealed abstract class Result(
    val committed: Boolean,
    val backoff: Boolean
  )
  private object Result {
    case object Committed extends Result(committed = true, backoff = false)
    case object Uncommitted extends Result(committed = false, backoff = false)
    case object Backoff extends Result(committed = false, backoff = true)

    @inline def apply(committed: Boolean) =
      if (committed) Committed else Uncommitted
  }

  private final case class PendingTxn(
    txn: Txn,
    occReads: Int,
    promise: Promise[Result] = Promise()
  )
}

final class LogNode[K, R, RV, W, WV](
  val ctx: PipelineCtx[K, R, RV, W, WV],
  val log: BatchLog,
  startEpoch: Epoch,
  seg: SegmentID,
  consensusStallRestartPeriod: Duration,
  stats: StatsRecorder)
    extends Node[K, R, RV, W, WV] {

  import ctx.codecs._
  import LogNode._

  private def config = ctx.config

  private[this] final val BatchesPerCall = 64
  // Conservatively allow 2 network round trip times before dropping heartbeats
  private[this] final val MaxPipelinedDuration = config.roundTripTime * 2
  private[this] final val MaxPipelinedHeartbeats = (MaxPipelinedDuration / config.logHeartbeatPeriod).toInt
  private[this] final val TruncateInterval = 30.seconds

  private implicit def ec = ctx.executionContext

  def abdicate(reason: String) =
    log.abdicate(reason)

  protected def onClose() = {
    heartbeat.stop()
    consensusHeartbeat.stop()

    // It's safe to invoke rejectAllPending and have it potentially race
    // with a still running heartbeat handler since they all interface
    // with the pending queue solely through .poll().
    rejectAllPending(None)
  }

  private val batchRequestors = new ConcurrentHashMap[HostID, AtomicBoolean]
  private val subscribers = new ConcurrentHashMap[HostID, Long]

  private def addSubscriber(host: HostID, acc: Epoch): Unit =
    subscribers.compute(host, (_, ep) => ep max acc.idx)

  private def removeSubscriber(host: HostID): Unit =
    subscribers.remove(host)

  private def foreachSubscriber(f: (HostID, Epoch) => Unit) =
    subscribers forEach { (host, ep) => f(host, Epoch(ep)) }

  // backoff mechanisms
  private val pipelinedBeats = new AtomicInteger
  private val occReadsLimiter =
    BurstLimiter
      .newBuilder()
      .withName("OCC Reads")
      .withPermitsPerSecond(config.maxOCCReadsPerSecond)
      .build()

  @volatile private[this] var lastTruncate = Timestamp.Epoch

  private[this] val pending =
    new LinkedBlockingQueue[PendingTxn](config.logMaxBatchSize)

  private[this] val heartbeat = Heartbeat(config.logHeartbeatPeriod, "LogNode") {
    val c = ctx.epochClock
    if (!isClosed && ctx.epochSynced) {
      val ts = c.time
      maybeTruncateLog(ts)

      if (pipelinedBeats.get >= MaxPipelinedHeartbeats) {
        ctx.stats.incrLogSkippedHeartbeats()
        rejectAllPending(None)
      } else {
        val epoch = Epoch(ts)
        if (epoch >= startEpoch) {
          if (isEpochValid(epoch)) {
            commitNextBatch(epoch)
          } else {
            rejectAllPending(Some(epoch))
          }
        }
      }
    }
  }

  private[this] val consensusHeartbeat = {
    val logNodeStart = Timing.start
    var previousLogLastIdxTS = Timestamp.Epoch
    val rsp = consensusStallRestartPeriod max 30.seconds
    if (rsp != consensusStallRestartPeriod) {
      getLogger().info("Too short period specified for consensus_stall_restart_period_seconds, overriding it to 30 seconds")
    }

    Heartbeat(10.seconds, "LogNode.ConsensusStall") {
      val ts = log.lastIdx.floorTimestamp
      if (ts == previousLogLastIdxTS) {
        log.log.logRoleAndState()
        if (ts + rsp < ctx.epochClock.time && logNodeStart.elapsed > 5.minutes) {
          /**
            * Note: this is a mitigation mechanism for LSE 12.
            * It terminates the JVM if the transaction log made no progress in 30 seconds.
            */
          getLogger().error(s"Consensus stall detected - exiting process to trigger restart.")
          System.exit(1)
        } else {
          // If the log segment is stalled because this node is the only node left in its
          // Raft ring, reinitialize the log to carry on. Note this will only kick in if
          // the node is the last one remaining _and_ it's been stalled for at least 10
          // seconds.
          log.tryReinit() foreach { reinited =>
            if (reinited) {
              stats.warning(
                "Log Segment Reinitialized",
                s"Node reinitialized the transaction log for segment ${seg.toInt} in order to make progress after it was the only node left in it.")
            }
          }
        }
      }
      previousLogLastIdxTS = ts
    }
  }

  private[this] def maybeTruncateLog(ts: Timestamp): Unit =
    if (ts > (lastTruncate + TruncateInterval)) {
      lastTruncate = ts
      // ensure we don't truncate a potentially partially applied Epoch
      val ep = Epoch(ctx.globalPersistedTimestamp) - 1
      val last = log.lastIdx
      if (ep <= last) {
        val prev = log.prevIdx
        log.truncate(ep)
        val newPrev = log.prevIdx
        if (newPrev != prev) {
          ctx.logger.info(s"LogNode: truncated log files for $seg from $prev to $newPrev; global persisted is $ep")
        }
      } else {
        ctx.logger.info(s"LogNode: not truncating log files for $seg as global persisted $ep is > log's last index $last")
      }
    }

  private[this] def isEpochValid(epoch: Epoch): Boolean = {
    val logNodeInfo = ctx.logNodeProvider.getLogNodeInfo
    epoch <= logNodeInfo.validUntil && (logNodeInfo.topology.get(seg) exists { si => !si.isEndedAt(epoch) })
  }

  private[this] def commitNextBatch(epoch: Epoch): Unit = {
    val commit = Promise[Result]()
    val b = Vector.newBuilder[Txn]

    var bytes = 0
    var occs = 0
    var p = pending.poll
    while (p ne null) {
      // must use ceil, in order to guarantee that t's expiry
      // falls after its assigned timestamp.
      val isExpired = p.txn.expiry <= epoch.ceilTimestamp

      if (isExpired) {
        p.promise.success(Result.Uncommitted)
      } else if (
        p.occReads > 0 &&
        !occReadsLimiter.tryAcquire(p.occReads).isSuccess &&
        p.occReads > config.occBackoffThreshold
      ) {
        ctx.logger.debug(
          s"Dropping txn due to occ rate limiter. # occ reads: ${p.occReads}")
        ctx.stats.incrLogOCCRateLimitRejections()
        p.promise.success(Result.Backoff)
      } else {
        b += p.txn
        bytes += p.txn.bytesSize
        occs += p.occReads
        p.promise.completeWith(commit.future)
      }

      p = if (bytes < config.maxBytesPerBatch) pending.poll else null
    }

    val txns = b.result()

    if (txns.nonEmpty || log.shouldEmitEmpty) {
      ctx.stats.recordOCCReadsPerBatch(occs)
      commit.completeWith(commitTxns(epoch, txns))
    }
  }

  private[this] def commitTxns(epoch: Epoch, txns: Vector[Txn]): Future[Result] = {
    val size = txns.size
    ctx.logger.debug(s"LogNode: Submitting $size transactions at $epoch")
    ctx.stats.recordLogBatchSize(size)

    pipelinedBeats.incrementAndGet()

    implicit val iec = ImmediateExecutionContext
    val addF = log.add(epoch, txns)

    addF.failed foreach {
      case _: LogClosedException =>
        () // Logs close as a normal course of business; don't log it.
      case ex => logException(ex)
    }

    addF map { Result(_) } ensure {
      ctx.stats.incrLogTotalBatches()
      pipelinedBeats.decrementAndGet()
    }
  }

  private[this] def rejectAllPending(epoch: Option[Epoch]): Unit = {
    // Either segment state is lagging behind our current commit epoch
    // (so we can't be sure our segment hasn't ended already), or our
    // segment has definitely ended by this epoch. In either case, we
    // must not commit any transactions as they'd either potentially or
    // definitely never get applied.

    // Reject all pending transactions. In case of state merely lagging
    // behind we needn't do this, but it's better to signal back to
    // coordinators and give them a chance to pick another, better
    // faring segment instead of hoping this one becomes operational
    // again before those transactions timed out. If our segment has
    // definitely ended, then we really should reject these.
    var p = pending.poll
    val rejected = Result.Uncommitted
    var cnt = 0
    while (p ne null) {
      p.promise.trySuccess(rejected)
      cnt += 1
      p = pending.poll
    }
    if (cnt > 0) {
      ctx.stats.addRejectedTxns(cnt)
    }

    // Keep adding empty batches from leader so data nodes can still
    // advance with transactions from other segments.
    if (log.shouldEmitEmpty) {
      epoch foreach {
        commitTxns(_, Vector.empty)
      }
    }
  }

  // Approximate epoch this log node is at, if the clock is synchronized
  def epoch =
    if (ctx.epochSynced) {
      Some(Epoch(ctx.epochClock.time))
    } else {
      None
    }

  // Ensures log is only subscribed to once epoch clock was
  // synchronized with the rest of the cluster.
  ctx.epochSyncedFuture foreach { _ =>
    if (!isClosed) {
      // subscribe from the most recent index. Since this is just
      // optimistically relaying entries, we don't need to bother w/ order
      // or Reinits. The data nodes must handle that.
      logException(log.subscribe(Epoch(ctx.epochClock.time), MaxPipelinedDuration) { ev =>
        if (isClosed) {
          FutureFalse
        } else {
          ev match {
            case Log.Entries(prev, es) =>
              ctx.stats.timeBroadcast(broadcast(prev, es.toVector))
            case Log.Idle(_) =>
              ctx.stats.incrLogBroadcastStalls()
              FutureTrue
            case _ =>
              FutureTrue
          }
        }
      })
    }
  }

  private[this] val filters = new ConcurrentHashMap[HostID, Set[DataFilter[W]]]()

  final def addFilter(host: HostID, filter: DataFilter[W]): Unit =
    filters.merge(host, Set(filter), { _ ++ _ })

  final def remFilter(host: HostID, filter: DataFilter[W]): Unit =
    filters.computeIfPresent(host, {
      case (_, fs) =>
        val next = fs - filter
        if (next.isEmpty) {
          null
        } else {
          next
        }
    })

  // Message Receipt

  private[transaction] def recvAdd(
    txn: Txn,
    occReads: Int,
    @unused prio: GlobalID,
    deadline: TimeBound): Future[Unit] = {

    val p = new PendingTxn(txn, occReads)
    val leaderHint = if (log.isLeader) None else log.leader

    def maybeAck(result: Result): Future[Unit] = {
      if (!result.committed) {
        ctx.stats.incrRejectedTxns()
      }
      if (deadline.hasTimeLeft) {
        ctx.ackTxn(txn, result.committed, result.backoff, leaderHint)
      } else {
        // Coordinator has gone away; don't bother sending an ACK
        Future {
          GlobalTracer.instance.activeSpan foreach { span =>
            span.setStatus(DeadlineExceeded(s"Transaction timed out before ACK. Deadline $deadline"))
            span.addAttribute("committed", result.committed)
          }
        }
      }
    }

    if (pending.offer(p)) {
      p.promise.future flatMap { maybeAck(_) }
    } else {
      maybeAck(Result.Uncommitted)
    }
  }

  private[transaction] def recvAcceptBatches(host: HostID,
                                             acc: Epoch,
                                             dataFilters: Vector[DataFilter[W]]): Future[Unit] = {
    filters.put(host, dataFilters.toSet)
    addSubscriber(host, acc)
    Future.unit
  }

  private[transaction] def recvGetBatches(host: HostID,
                                          recvSeg: SegmentID,
                                          pvers: Long,
                                          prev: Epoch,
                                          dataFilters: Vector[DataFilter[W]]): Future[Unit] =
    if (recvSeg != seg) {
      Future.unit

    } else if (prev >= log.lastIdx) {
      Future.unit

    } else if (prev < log.prevIdx) {
      val reinit = Reinit(seg, log.prevIdx, log.lastIdx)
      ctx.logger.info(s"LogNode: Sending $reinit to $host; it asked for batches from $prev.")
      ctx.dataSink(host).send(reinit).unit

    } else {
      filters.put(host, dataFilters.toSet)

      val part = ctx.partitioner

      if (part.version != pvers) {
        // Requestor's topology version is too old, we can't produce batches
        // for it. It'll eventually catch up to new topology and send us a
        // request with newer topology version.
        ctx.logger.debug(s"LogNode: Sending no batches to $host @ $pvers; topology version unavailable.")
        Future.unit
      } else {
        // Only ever service a single GetBatches request per host. A data node can
        // spam us with requests if its batchStallTime is shorter than the time it
        // takes us to prepare a batch.
        val reqMutex = batchRequestors.computeIfAbsent(host, _ => new AtomicBoolean())
        if (reqMutex.compareAndSet(false, true)) {
          try {
            val batches = Vector.newBuilder[Batch]
            val entries = log.entries(prev) take BatchesPerCall

            var last = prev
            var txnCount = 0

            entries foreach { e =>
              val bs = hostBatches(part, host, e, None)
              last = e.idx
              txnCount = (bs foldLeft txnCount) { _ + _.count }
              batches ++= bs
            }

            val batchesRes = batches.result()
            ctx.logger.debug(s"LogNode: Send ${batchesRes.size} batches ($txnCount transactions) to $host @ $pvers: $batchesRes")

            val msg = Batches(log.id, pvers, log.lastIdx, None, prev, last, batchesRes)

            ctx.dataSink(host).send(msg).unit ensure {
              entries.release()
            }
          } finally {
            reqMutex.set(false)
          }
        } else {
          // We are already servicing a GetBatches request for this host, so
          // ignore its repeated requests.
          Future.unit
        }
      }
    }

  private[transaction] def recvFilterAction(host: HostID,
                                            filter: DataFilter[W],
                                            isAdd: Boolean): Future[Unit] = {
    if (isAdd) {
      addFilter(host, filter)
    } else {
      remFilter(host, filter)
    }
    Future.unit
  }

  // helpers

  private def broadcast(prev: Epoch, entries: Vector[log.E]): Future[Boolean] = {
    val part = ctx.partitioner

    foreachSubscriber { (host, ep) =>
      if (prev.idx - ep.idx > MaxPipelinedEpochs) {
        removeSubscriber(host)
        ctx.logger.debug(s"LogNode: Dropped subscriber $host")
      }
    }

    var last = prev

    entries foreach { e =>
      // amortizes the cost of re-parsing expressions in hostBatches()
      val cache = Some(MMap.empty[HandlerID, W])

      foreachSubscriber { (host, _) =>
        val bs = hostBatches(part, host, e, cache)
        ctx.dataSink(host).send(Batches(log.id, part.version, e.idx, None, last, e.idx, bs))

        if (ctx.logger.isDebugEnabled) {
          var txnCount = 0
          var batchCount = 0

          bs foreach { b =>
            txnCount += b.count
            batchCount += 1
          }

          ctx.logger.debug(
            s"LogNode: Broadcast $batchCount batches ($txnCount transactions) to $host @ ${part.version}")
        }
      }

      last = e.idx

      ctx.stats.recordPartialEpochSendLatency(ctx.epochClock.time difference e.idx.ceilTimestamp)
    }

    FutureTrue
  }

  private def hostBatches(part: Partitioner[R, W],
                          host: HostID,
                          entry: log.E,
                          cache: Option[MMap[HandlerID, W]]) = {
    val fs = filters.getOrDefault(host, Set.empty)

    entry.get map {
      case ScopeSubEntry(_, txns) =>
        val filtered = Vector.newBuilder[Batch.Txn]

        var pos = 0
        txns foreach { t =>
          val w = cache match {
            case None        => CBOR.parse[W](t.expr)
            case Some(cache) =>
              cache.getOrElseUpdate(t.origin, CBOR.parse[W](t.expr))
          }

          if (fs.exists(_.covers(w)) || part.coversTxn(host, w)) {
            filtered += t.copy(pos = pos)
          } else {
            ctx.logger.debug(
              s"LogNode: Filtering transaction $t. " +
                s"$host does not cover this transaction at partitioner version ${part.version}")
          }

          pos += 1
        }

        Batch(entry.idx, ScopeID.RootID, filtered.result(), pos)
    }
  }
}
