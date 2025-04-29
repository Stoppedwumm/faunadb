package fauna.tx.transaction

import fauna.atoms._
import fauna.codex.cbor.CBOR
import fauna.exec.Timer
import fauna.lang.clocks.{ Clock, DelayTrackingClock }
import fauna.lang.syntax._
import fauna.lang.{
  AdminControl,
  AtomicFile,
  Timestamp,
  Timing
}
import fauna.logging.ExceptionLogging
import fauna.net.HostService
import fauna.net.bus._
import fauna.stats.StatsRecorder
import fauna.trace._
import fauna.tx.consensus._
import fauna.tx.transaction.Coordinator.Leadership
import fauna.tx.transaction.LogMessage._
import fauna.tx.transaction.RevalidatorRole._
import io.netty.buffer.{ ByteBuf, ByteBufAllocator }
import io.netty.util.Timeout
import java.nio.channels.FileChannel
import java.nio.file.{ NotDirectoryException, Path }
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.unused
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.Random

object TxnPipeline {
  private def epochsInSeconds(s: Int) =
    (s.seconds.toMillis / Epoch.MillisPerEpoch).toInt

  private[TxnPipeline] val LogSegmentsMaxValidity = epochsInSeconds(10)
  private[TxnPipeline] val LogSegmentsMinValidity = epochsInSeconds(6)
  private[TxnPipeline] val LogSegmentsCriticalValidity = epochsInSeconds(2)
  private[TxnPipeline] val MaxNonCriticalEpochs = LogSegmentsMinValidity - LogSegmentsCriticalValidity
  private[TxnPipeline] val MaxLogLeaveAttempts = 60
  val LogSegmentsRevalidationPeriod = 1.seconds

  def apply[K, R: CBOR.Codec, RV: CBOR.Codec, W: CBOR.Codec, WV: CBOR.Codec](
    config: PipelineConfig,
    hostService: HostService,
    logNodeProvider: LogNodeProvider,
    coordinatorNodes: => Iterable[HostID],
    isDataNode: => Boolean,
    keyExtractor: KeyExtractor[K, R, W],
    mismatchResetTxn: W => Option[W],
    partitionerProvider: PartitionerProvider[R, W],
    filterCodec: CBOR.Codec[DataFilter[W]],
    store: TransactionStorage[K, R, RV, W, WV],
    ec: ExecutionContext,
    bus: MessageBus,
    readClock: DelayTrackingClock,
    ackDelayClock: DelayTrackingClock,
    epochClock: Clock,
    backupRequestRatio: Double = 0.1,
    consensusStallRestartPeriod: Duration = Duration.Inf,
    stats: StatsRecorder = StatsRecorder.Null) =
    new TxnPipeline[K, R, RV, W, WV](
      config,
      bus.hostID,
      hostService,
      logNodeProvider,
      coordinatorNodes,
      isDataNode,
      keyExtractor,
      mismatchResetTxn,
      partitionerProvider,
      filterCodec,
      store,
      ec,
      bus,
      readClock,
      ackDelayClock,
      epochClock,
      backupRequestRatio,
      consensusStallRestartPeriod,
      stats)

  class Stats(val recorder: StatsRecorder) {
    def incrLogSkippedHeartbeats() = recorder.incr("Transaction.Log.Skipped.Heartbeats")

    def incrLogTotalBatches() = recorder.incr("Transaction.Log.Batches")

    def recordLogBatchSize(size: Int) = recorder.timing("Transaction.Log.Batch.Size", size)

    def incrRejectedTxns() = recorder.incr("Transaction.Rejected")

    def addRejectedTxns(count: Int) = recorder.count("Transaction.Rejected", count)

    def incrLogReadStalls() = recorder.incr("Transaction.Data.Log.Read.Stalls")

    def incrLogBroadcastStalls() = recorder.incr("Transaction.Log.Broadcast.Stalls")

    def incrLogOCCRateLimitRejections(): Unit =
      recorder.incr("Transaction.Log.OCCRateLimit.Rejections")

    def recordOCCReadsPerBatch(occReads: Int): Unit =
      recorder.count("Transaction.Log.Batches.OCCs", occReads)

    def recordPartialEpochSendLatency(elapsed: Duration) =
      recorder.timing("Transaction.Partial.Epoch.Send.Latency", elapsed.toMillis)

    def recordPartialEpochReceiveLatency(elapsed: Duration) =
      recorder.timing("Transaction.Partial.Epoch.Rcv.Latency", elapsed.toMillis)

    def recordFullEpochReceiveLatency(elapsed: Duration) =
      recorder.timing("Transaction.Full.Epoch.Rcv.Latency", elapsed.toMillis)

    def incrRemoteReadStalls() = recorder.incr("Transaction.Data.Remote.Read.Stalls")

    def incrRemoteReadCacheHits() = recorder.incr("Transaction.Data.Remote.Read.Cache.Hit")

    def incrRemoteReadCacheMisses() = recorder.incr("Transaction.Data.Remote.Read.Cache.Miss")

    def incrRemoteReadUncovered() = recorder.incr("Transaction.Data.Remote.Read.Uncovered")

    def incrRemoteReadForwardedTimeout() = recorder.incr("Transaction.Data.Remote.Read.Forwarded.Timeout")

    def incrRemoteReadRequestedTimeout() = recorder.incr("Transaction.Data.Remote.Read.Requested.Timeout")

    def recordRemoteReadLatency(elapsed: Duration) =
      recorder.timing("Transaction.Data.Remote.Read.Latency", elapsed.toMillis)

    /** Incremented when a batch is received by a DataNode for processing. */
    def incrBatchesReceived() = recorder.incr("Transaction.Batches.Received")

    /** Incremented when a transaction within a batch has been processed. */
    def incrProcessedTransactions() = recorder.incr("Transaction.Processed")

    /** Records the amount of time spent waiting to acquire a transaction
      * processing permit. See PipelineConfig.maxConcurrenTransactions.
      */
    def recordAcquireTimingElapsed(timing: Timing) =
      recorder.timing("Transaction.Acquire.Timing", timing.elapsedMillis)

    /** Records the amount of time spent processing a batch of transactions. */
    def recordBatchTimingElapsed(timing: Timing) =
      recorder.timing("Transaction.Batch.Timing", timing.elapsedMillis)

    /** Records the amount of time spent waiting for data dependencies to
      * apply in a sequenced batch of transactions.
      */
    def recordSequencedTransactionTiming(timing: Timing) =
      recorder.timing("Transaction.Sequence.Timing", timing.elapsedMillis)

    def incrAppliedTxns() = recorder.incr("Transaction.Applies")

    def recordApplyLatency(elapsed: Duration) =
      recorder.timing("Transaction.Apply.Latency", elapsed.toMillis)

    def recordApplyTimingElapsed(timing: Timing) =
      recorder.timing("Transaction.Apply.Timing", timing.elapsedMillis)

    def timeBroadcast[T](f: => Future[T]): Future[T] =
      recorder.timeFuture("Transaction.Broadcast.Duration")(f)

    def setReadClockDelay(delay: Int) = recorder.set("Transaction.Read.Clock.Delay", delay)

    def incrMismatchedResults() = recorder.incr("Transaction.Mismatched.Results")

    def incrLocalReadTimeout() = recorder.incr("Transaction.Local.Read.Timeout")

    def incrCoordinatorTimeouts(local: Boolean) =
      if (local) {
        recorder.incr("Transaction.Coordinator.Initial.Timeout")
      } else {
        recorder.incr("Transaction.Coordinator.Final.Timeout")
      }

    def incrQueueFull() = recorder.incr("Transaction.Queue.Full")

    def recordQueueProcessRate(rate: Double) =
      recorder.set("Transaction.Queue.Rate", rate)

    def recordConcurrentTransactionSemaphoreAvailability(
      availableTransactionExecutionSlots: Int): Unit =
      recorder.set(
        "Transaction.AvailabeExecutionSlots",
        availableTransactionExecutionSlots)

    def recordTransactionQueueSize(size: Int): Unit =
      recorder.set("Transaction.Queue.Size", size)

    def recordSequencedTransactionLength(size: Int): Unit =
      // timing is used for host local distribution
      recorder.timing("Transaction.SequencedTransactions", size)
  }

  /**
    * Quorum check implementation that also check if the responses received are
    * from the targeted SegmentID. Invalid SegmentIDs are filtered out before quorum check.
    *
    * @param segmentID target SegmentID expected from the leader response
    * @param hostIDs   target hostIDs.
    * @return [[QuorumCheck]] instance that can be used to check if the requests has quorum.
    */
  def segmentLeaderCheck(segmentID: SegmentID,
                         hostIDs: Set[HostID]): QuorumCheck[Leadership, HostID] = {
    responses: Seq[Leadership] => {
      val leaderIDs =
        responses collect {
          //also filter responses from the targeted SegmentID only.
          case Leadership.Leader(leaderHostID, leaderSegmentID) if leaderSegmentID == segmentID =>
            leaderHostID
        }
      Ring(hostIDs).quorumValue(leaderIDs)
    }
  }
}

class TxnPipeline[K, R: CBOR.Codec, RV: CBOR.Codec, W: CBOR.Codec, WV: CBOR.Codec](
  val config: PipelineConfig,
  val hostID: HostID,
  hostService: HostService,
  logNodeProvider: LogNodeProvider,
  coordinatorNodes: => Iterable[HostID],
  isDataNode: => Boolean,
  keyExtractor: KeyExtractor[K, R, W],
  mismatchResetTxn: MismatchResetTxnFactory[W],
  partitionerProvider: PartitionerProvider[R, W],
  filterCodec: CBOR.Codec[DataFilter[W]],
  store: TransactionStorage[K, R, RV, W, WV],
  executionContext: ExecutionContext,
  bus: MessageBus,
  readClock: DelayTrackingClock,
  ackDelayClock: DelayTrackingClock,
  epochClock: Clock,
  backupRequestRatio: Double,
  consensusStallRestartPeriod: Duration,
  statsRecorder: StatsRecorder) extends ExceptionLogging {

  import TxnPipeline._

  private implicit val ec = executionContext

  val ReplogTickDuration = config.roundTripTime

  val tracer = GlobalTracer.instance
  val stats = new TxnPipeline.Stats(statsRecorder)

  val ctx = PipelineCtx[K, R, RV, W, WV](
    config,
    hostID,
    hostService,
    logNodeProvider,
    keyExtractor,
    partitionerProvider,
    filterCodec,
    executionContext,
    epochClock,
    Timer.Global, // FIXME: perhaps use dedicated timer
    bus,
    readClock,
    ackDelayClock,
    backupRequestRatio,
    stats)

  import ctx.codecs._

  @volatile private[this] var _logNode: Option[LogNode[K, R, RV, W, WV]] = None

  val coordinator = new Coordinator(ctx, mismatchResetTxn)

  @inline private def decodeFilters(buf: ByteBuf): Vector[DataFilter[W]] =
    if (buf.isReadable) {
      CBOR.decode[Vector[DataFilter[W]]](buf)
    } else {
      Vector.empty
    }

  private[this] val logProtocolHandler = ctx.logHandler {
    case (from, GetBatches(seg, pvers, after, scope, filterBytes), _) =>
      _logNode.fold(Future.unit) {
        require(scope.contains(ScopeID.RootID))
        _.recvGetBatches(from.id, seg, pvers, after, decodeFilters(filterBytes))
      }
    case (from, AcceptBatches(_, acc, filterBytes), _) =>
      _logNode.fold(Future.unit) {
        _.recvAcceptBatches(from.id, acc, decodeFilters(filterBytes))
      }
    case (_, AddTxn(scope, txn, occReads, gid), deadline) =>
      tracer.withTraceContext("tx.add", txn.trace) {
        // Ack back the txn as not committed, so coordinator will retry.
        // The fact a coordinator addressed this node means there should
        // likely be a log node here, it just isn't initialized yet.
        _logNode.fold(ctx.ackTxn(txn, committed = false, backoff = false, None)) {
          require(scope == ScopeID.RootID)
          _.recvAdd(txn, occReads, gid, deadline)
        }
      }
    case (from, message: GetLeader, _) =>
      val response = _batchLog map { batchLog =>
        GetLeaderResponse(batchLog.id, batchLog.leader)
      }
      ctx.bus.sink(LogMessage.GetLeaderReplyProtocol, message.signalID.at(from)).send(response).unit
    case (_, LeaderNotification(seg, host), _) =>
      coordinator.markSegmentLeader(seg, host)
      Future.unit
    case (from, FilterAction(filterBytes, isAdd), _) =>
      _logNode.fold(Future.unit) {
        _.recvFilterAction(from.id, CBOR.decode[DataFilter[W]](filterBytes), isAdd)
      }
    case (_, Unused, _) => throw new AssertionError(Unused)
  }

  @volatile var dataNode = Option.empty[DataNode[K, R, RV, W, WV]]
  private[this] var streamCount = 0L

  @volatile private[this] var _batchLog: Option[BatchLog] = None
  @volatile private[this] var _initRound: Option[(SegmentID, Int)] = None

  private[this] var logLeaveAttempts = 0

  private def tryResetBatchLog() = {
    val ln = _logNode
    _logNode = None
    ln foreach { _.close() }

    // try to close the batchlog
    _batchLog foreach { bl =>
      // FIXME: under what conditions will all peers have gone
      // away?
      if (bl.isMember && bl.log.ring.size > 1 && (logLeaveAttempts < MaxLogLeaveAttempts)) {
        bl.log.leave()
        logLeaveAttempts += 1
      } else {
        ctx.logger.info(s"Closing batch log for segment ${bl.id}")
        bl.close()

        deleteBatchLogFiles(bl.id)

        _initRound = None
        _batchLog = None

        logLeaveAttempts = 0
      }
    }

    Future.unit
  }

  // Do an emergency, operator-intiated, unsafe truncation of the log.
  def unsafeTruncateLog(toIdx: Epoch): Unit =
    _batchLog match {
      case Some(log) => log.truncate(toIdx)
      case None => throw new IllegalStateException("txn log uninitialized")
    }

  // Only allow one log topology update at the time. They're all run from the loop in
  // updateState, so if one fizzes out it'll be retried next time if it's still
  // relevant. This is just an optimization, we could allow them to pile up.
  private[this] val logTopologyUpdate = new AtomicBoolean
  private[this] def updateLogTopology(f: LogNodeProvider => Future[Unit]): Unit =
    if (logTopologyUpdate.compareAndSet(false, true)) {
      f(logNodeProvider) ensure {
        logTopologyUpdate.set(false)
      }
    }

  private def maybeCreateDataNode(cause: String): Unit =
    if (dataNode.isEmpty) {
      ctx.logger.info(s"Initializing DataNode. (cause: $cause)")
      dataNode = Some(new DataNode(ctx, store))
    }

  StatsRecorder.polling {
    if (isDataNode) {
      val lat = store.appliedTimestamp
      if (lat != Timestamp.Epoch) {
        val now = ctx.epochClock.time
        ctx.stats.recordApplyLatency(now difference lat)
      }
    } else {
      ctx.stats.recordApplyLatency(Duration.Zero)
    }
  }

  private def updateState(): Future[Unit] = {
    synchronized {
      // do not start the data node if storage has been locked
      if (isDataNode && store.isUnlocked) {
        maybeCreateDataNode("state update")
      } else if (streamCount == 0) {
        dataNode foreach { dn =>
          dataNode = None
          dn.close()
        }
      }
    }

    val lni = logNodeProvider.getLogNodeInfo

    lni.topology find { case (_, si) => si contains hostID } match {
      // not a LogNode
      case None =>

        // close the service, and clean up current log
        tryResetBatchLog()

      case Some((segment, SegmentInfo(hosts, state, initRound))) =>
        val staleInitRound = readInitRound(segment) != initRound

        val batchLog = _batchLog match {
          case Some(bl) =>
            if (bl.id != segment) {
              ctx.logger.info(s"Assigned to new Log segment $segment")

              // break until batch log is cleaned up
              return tryResetBatchLog()
            } else if (staleInitRound) {
              ctx.logger.info(s"New initialization round $initRound for segment $segment")

              // break until batch log is cleaned up
              return tryResetBatchLog()
            }

            bl

          case None =>
            if (staleInitRound) {
              // Delete old batch log if we're in a different round of initialization now
              ctx.logger.info(s"New initialization round $initRound for segment $segment")
              deleteBatchLogFiles(segment)
            }

            // Make sure we have a batch log open and record the initialization round
            val bl = openBatchlog(segment)
            _batchLog = Some(bl)
            writeInitRound(segment, initRound)
            ctx.logger.info(s"Opening batch log for $segment, initRound=$initRound")
            bl
        }

        state match {
          // Segment is not yet started
          case Uninitialized =>

            // Is this the initializer?
            if (hosts.head == hostID) {
              if (!batchLog.isMember) {
                ctx.logger.info(s"Initializing transaction log $segment.")
                batchLog.log.init().unit

              } else if (hosts forall { h => batchLog.log.ring.contains(h) }) {
                ctx.logger.info(s"All members joined; starting segment $segment.")
                logNodeProvider.startSegment(segment, lni.validUntil + 1, lni.version).unit

              } else {
                ctx.logger.info(s"Waiting for more members on segment $segment. hosts=$hosts ring=${batchLog.log.ring}")
                Future.unit
              }

            } else if (!batchLog.isMember) {
              ctx.logger.info(s"Joining not yet started transaction log $segment (${hosts.head}).")
              batchLog.log.join(hosts.head).unit
              Future.unit

            } else {
              ctx.logger.info(s"Joined not yet started segment $segment, waiting for it to start.")
              Future.unit
            }

          // Segment is started
          case SegmentState.HasStart(start) =>

            // If we're not joined into the raft ring, join it; this
            // node is likely a new arrival to an already started segment
            if (!batchLog.isMember) {
              Random.chooseOption(hosts filter { _ != hostID }) match {
                case Some(joinPeer) =>
                  ctx.logger.info(s"Joining already started transaction log $segment ($joinPeer)")
                  batchLog.log.join(joinPeer)
                  // Note: we aren't waiting on join, we just fire it off. initLogNode is driven
                  // by LogTopology changes, and it's ticking its validUntil, so it'll circle back
                  // to executing it again and eventually notice it joined. On the other hand, if
                  // we start a new initialization round, we don't want to be blocked on this join completing.
                  Future.unit

                case None =>
                  _logNode foreach { _.close() }
                  batchLog.close()
                  _batchLog = None
                  // This can not happen
                  throw new IllegalStateException(s"$hostID is the only host in started $segment, yet it is not member of its ring.")
              }
            } else {
              // Otherwise if we're joined into the raft ring, start
              // the log node
              if (_logNode.isEmpty) {
                ctx.logger.info(s"Starting transaction log service $segment.")
                val ln = new LogNode(ctx, batchLog, start, segment, consensusStallRestartPeriod, statsRecorder)
                _logNode = Some(ln)
              }

              batchLog.updateMembership(hosts.toSet, hostService)

              if (batchLog.id == lni.topology.keys.min) {
                val gpe = Epoch(ctx.globalPersistedTimestamp)
                val segsToRemove = lni.topology collect { case (seg, si) if si.isEndedAt(gpe) => seg }

                if (segsToRemove.nonEmpty) {
                  ctx.logger.info(s"Removing log segments $segsToRemove from topology as they ended (global persisted epoch=$gpe).")
                  updateLogTopology { _.removeSegments(segsToRemove, lni.version) }
                }
              }

              // Normally, log epoch > data epoch, except if the local clock is way behind.
              // We should be prepared for it, though.
              val dataEpoch = dataNode map { _.maxReceivedBatch } getOrElse Epoch.MinValue
              val logEpoch = _logNode flatMap { _.epoch } getOrElse Epoch.MinValue
              val maxEpoch = dataEpoch max logEpoch
              val epochsLeft = lni.validUntil.idx - maxEpoch.idx
              if (epochsLeft < LogSegmentsMinValidity) {
                def revalidate(): Unit =
                  updateLogTopology { _.revalidate(maxEpoch + LogSegmentsMaxValidity, lni.version) }

                logNodeProvider.getRevalidatorRole match {
                  case MainRevalidator =>
                    revalidate()
                  case SubstituteRevalidator =>
                    if (Random.nextInt(MaxNonCriticalEpochs) > (epochsLeft - LogSegmentsCriticalValidity)) {
                      revalidate()
                    }
                  case NotARevalidator => ()
                }
              }

              Future.unit
            }

          case Started(_) | Closed(_, _) =>
            Future.unit // XXX: what action should be taken here?
        }
    }
  }

  private def setupTopologyListener() = {

    def syncForRemoval(): Future[Boolean] = {
      startP.setDone()

      // If all currently ended segments in the region could be removed if we
      // nudged the global persisted timestamp by syncing the storage, then
      // do it.
      // FIXME: refine this once we have per scope segments
      val pe = Epoch(store.persistedTimestamp)
      val lae = Epoch(store.appliedTimestamp)

      if (isDataNode) {
        val mee = ctx.logTopology.foldLeft(Epoch.MinValue) {
          case (e, (_, SegmentInfo(_, Closed(_, end), _))) => e max end
          case (e, _) => e
        }

        if (pe < mee && mee <= lae) {
          store.sync()
        }
      }

      Future.successful(!isClosed)
    }

    logNodeProvider.subscribeWithLogging(syncForRemoval())
  }

  private[this] val startP = Promise[Unit]()
  def onStart: Future[Unit] = startP.future

  setupTopologyListener()

  @volatile private[this] var _closed = false
  def isClosed = _closed

  def close(grace: Duration): Unit = {
    _logNode foreach { _.abdicate("closing") }
    ctx.timer.delay(grace) {
      synchronized {
        if (!isClosed) {
          _closed = true
          coordinator.close()
          dataNode foreach { _.close() }
          logProtocolHandler.close()
          _logNode foreach { _.close() }
          _batchLog foreach { _.close() }
          revalidatorTask foreach { _.cancel() }
        }
      }
      Future.unit
    }
  }

  @volatile private[this] var revalidatorTask: Option[Timeout] = None

  updateStateAndScheduleNext()

  private def updateStateAndScheduleNext(): Unit =
    if (isClosed) {
      revalidatorTask = None
    } else {
      logException(updateState()) ensure {
        revalidatorTask = Some(ctx.timer.scheduleTimeout(LogSegmentsRevalidationPeriod) {
          updateStateAndScheduleNext()
        })
      }
    }

  // helpers

  private def openBatchlog(segment: SegmentID) = {
    implicit val ec = ctx.executionContext
    val statsPrefix = "Transaction.Log.Consensus"
    val transport = new MessageBusTransport("tx",
                                            bus,
                                            config.replicationSignal,
                                            config.busMaxPendingMessages,
                                            statsRecorder = statsRecorder,
                                            statsPrefix = statsPrefix,
                                            dedicatedBusStream = true)

    val replog = BatchLog.Log.open(
      transport,
      hostID,
      ctx.transactionLogPath,
      s"segment_${segment.toInt}",
      ReplogTickDuration,
      statsRecorder = statsRecorder,
      statsPrefix = statsPrefix,
      leaderStateChangeListener = { rl =>
        // isClosed is tested purely defensively
        if (rl.isLeader && !rl.isClosed) {
          sendLeaderNotifications(segment)
        }
      },
      alwaysActive = true,
      txnLogBackupPath = ctx.config.txnLogBackupPath
    )

    new BatchLog(segment, replog, config.logIdxCommitWindow, config.logCachedBatches)
  }

  private def sendLeaderNotifications(segment: SegmentID) =
    Future {
      val msg = LeaderNotification(segment, hostID)
      // FIXME: only CBOR encode it once and sendBytes to hosts
      coordinatorNodes foreach { host =>
        ctx.logSink(host).send(msg).unit
      }
    }

  private def deleteBatchLogFiles(segment: SegmentID): Unit = {
    ctx.logger.info(s"Deleting batch log files for segment $segment")
    val b = List.newBuilder[Path]
    try {
      ctx.transactionLogPath.search(s"segment_${segment.toInt}*") { b += _ }
      ctx.transactionLogPath.search(s"segment_init_id_${segment.toInt}*") { b += _ }
      b.result() foreach { _.delete() }
    } catch {
      case _: NotDirectoryException => ()
    }
  }

  private def initRoundFile(seg: SegmentID) = synchronized {
    val f = new AtomicFile(ctx.transactionLogPath / s"segment_init_id_${seg.toInt}")
    f.create { writeInitRoundToChannel(_, SegmentInfo.NoInitRound) }
    f
  }

  private def writeInitRoundToChannel(ch: FileChannel, initRound: Int): Unit =
    ByteBufAllocator.DEFAULT.ioBuffer(4) releaseAfter { buf =>
      buf.writeInt(initRound)
      buf.readAllBytes(ch)
    }

  private def readInitRound(seg: SegmentID): Int =
    _initRound match {
      case Some((s, rnd)) if s == seg => rnd
      case _ =>
        val rnd = initRoundFile(seg).read { ch =>
          ByteBufAllocator.DEFAULT.buffer releaseAfter { b =>
            b.writeBytes(ch, 4)
            b.readInt
          }
        }

        _initRound = Some((seg, rnd))
        rnd
    }

  private def writeInitRound(seg: SegmentID, initRound: Int): Unit = {
    initRoundFile(seg).write { writeInitRoundToChannel(_, initRound) }
    _initRound = Some((seg, initRound))
  }

  /** Minimium txn ts applied (but not necessarily flushed) to storage. */
  def minAppliedTimestamp: Timestamp =
    store.appliedTimestamp

  /**
    * Returns the transaction time of the earliest (i.e. minimum)
    * transaction durably persisted to storage on this host.
    *
    * This value informs the transaction log truncation process: the
    * log may be safely truncated at the minimum of _all_
    * minPersistedTimestamps within a cluster, guaranteeing that all
    * transaction up to that point have been durably persisted on all
    * hosts. `HealthChecker` propagates these values across the
    * cluster via the transaction log.
    *
    * If this host is not a data node, returns Timestamp.MaxMicros
    * (i.e. this host does not prevent log truncation).
    *
    * If storage is locked, returns Timestamp.Epoch to prevent log
    * truncation for eventual replay on the locked host.
    */
  def minPersistedTimestamp: Timestamp =
    dataNode match {
      case Some(_) if store.isLocked =>
        // A data node emitting Timestamp.Epoch for minPersistedTimestamp
        // will prevent log truncation; locked storage requires that log
        // trunction be disabled for the duration of repair/recovery for
        // replay after storage becomes unlocked
        Timestamp.Epoch

      case Some(dn) => dn.minPersistedTimestamp
      case None     => Timestamp.MaxMicros
    }

  def logLeaders: Future[Set[HostID]] = {
    val leaders =
      ctx.logTopology map {
        case (segmentID, segmentInfo) =>
          logSegmentLeader(segmentID, segmentInfo.hosts.toSet)
      }

    Future.sequence(leaders).map(_.flatten.toSet)
  }

  /**
    * Ask the coordinator to fetch the leader for the Segment at a hostID.
    */
  def getLeader(hostID: HostID): Future[Leadership] =
    coordinator.getLeader(hostID)

  /**
    * Fetches the quorum leader of a Segment at given hostID's.
    */
  def logSegmentLeader(segmentID: SegmentID, hostIDs: Set[HostID]): Future[Option[HostID]] = {
    //concurrently send requests to all hostID to fetch the leader.
    def getLeaderResponses = hostIDs map { getLeader(_) } toList

    val check = segmentLeaderCheck(segmentID, hostIDs)
    //perform quorum check as responses arrive and return result as soon as majority responses
    //result in a successful or unsuccessful quorum.
    Quorum.check(getLeaderResponses, check) recoverWith {
      case ex: TimeoutException =>
        //we do no response timeouts to the client. If there is a timeout return None so that
        //the request succeeds.
        getLogger().warn("Quorum check timeout", ex)
        Future.successful(None)
    }
  }

  def addOrReplaceStream(stream: DataStream[W, WV]): Unit = {
    val dn = synchronized {
      streamCount += 1
      maybeCreateDataNode("stream start")
      dataNode getOrElse {
        streamCount -= 1
        // Practically can't happen since API endpoint isn't brought up
        // and internal users don't start before CassandraService.started,
        // which is contingent on CassandraService.selfRegion.
        throw new IllegalStateException("Can't add stream as node's region is not yet known.")
      }
    }
    dn.addOrReplaceStream(stream)
  }

  def removeStream(stream: DataStream[W, WV]): Unit = {
    dataNode foreach { _.removeStream(stream) }
    synchronized {
      if (streamCount > 0) {
        streamCount -= 1
      } else {
        getLogger().warn("Unbalanced removeStream call")
      }
    }
  }

  /**
    * *** DANGER ***
    */
  def skipTransactions()(implicit @unused ctl: AdminControl): Option[Timestamp] =
    dataNode map { _ =>
      val lat = store.appliedTimestamp
      val inc = lat + 1.nano
      ctx.logger.warn(s"WARNING: SKIPPING TRANSACTIONS BETWEEN $lat AND $inc!")
      store.sync(TransactionStorage.UnsafeSyncMode(inc))
      inc
    }
}
