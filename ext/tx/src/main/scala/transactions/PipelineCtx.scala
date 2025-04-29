package fauna.tx.transaction

import fauna.atoms._
import fauna.codex.cbor.CBOR
import fauna.exec.Timer
import fauna.lang.clocks.{ Clock, DelayTrackingClock }
import fauna.lang.syntax._
import fauna.lang.{ TimeBound, Timestamp }
import fauna.net._
import fauna.net.bus._
import fauna.stats.StatsRecorder
import fauna.trace.GlobalTracer
import java.nio.file._
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future, Promise }

/**
  * Thrown by storage to indicate that no transactions may be applied
  * until an operator repairs this host's data and unlocks storage.
  */
class StorageLockedException extends Exception("Storage is locked")

object TransactionStorage {
  sealed trait SyncMode
  case object NormalSyncMode extends SyncMode
  final case class UnsafeSyncMode private[transaction](ts: Timestamp) extends SyncMode
}

trait TransactionStorage[K, R, RV, W, WV] extends AppliedTimestampProxy[K] {

  def readsForTxn(expr: W): Seq[R]

  /**
    * Performs a local read at the specified timestamp. This method allows
    * reading beyond current applied timestamp. It is only safe to read beyond
    * this if the caller can prove that there can be no pending writes with a
    * timestamp less than `ts` for the given read key.
    */
  def evalTxnRead(ts: Timestamp, read: R, deadline: TimeBound): Future[RV]

  /**
    * Provided with a set of read results and a write expression, applies any
    * write results relevant to this host to local storage.
    */
  def evalTxnApply(ts: Timestamp, reads: Map[R, RV], expr: W): Future[WV]

  /**
    * The last applied timestamp persisted to durable media.
    */
  def persistedTimestamp: Timestamp

  /**
    * Force storage to persist state to durable media. The persistedTimestamp is
    * updated to appliedTimestamp.
    */
  def sync(): Unit = sync(TransactionStorage.NormalSyncMode)

  def sync(syncMode: TransactionStorage.SyncMode): Unit

  /**
    * Lock storage. Disallowing transaction application.
    */
  def lock(): Unit

  /**
    * Unlock storage, (re-)allowing transaction application.
    */
  def unlock(): Unit

  /**
    * Storage is open for transaction application.
    */
  def isUnlocked: Boolean

  /**
    * Storage is not open for transaction application.
    */
  final def isLocked: Boolean = !isUnlocked
}

case class PipelineConfig(
  replicationSignal: SignalID,
  logSignal: SignalID,
  dataSignal: SignalID,
  rootPath: Path,
  maxBytesPerBatch: Int,
  busMaxPendingMessages: Int,
  txnLogBackupPath: Option[Path],
  maxTransactionsPerEpoch: Int = Epoch.NanosPerEpoch,
  roundTripTime: FiniteDuration = 500.millis,
  maxReadDelay: FiniteDuration = 5.seconds,
  maxWriteTimeout: FiniteDuration = 30.seconds,
  logMinBatchSize: Int = 10,
  logMaxBatchSize: Int = 10000,
  maxConcurrentTransactions: Int = 1024,
  maxOCCReadsPerSecond: Int = Int.MaxValue,
  occBackoffThreshold: Int = Int.MaxValue
) {

  val logHeartbeatPeriod = Epoch.MillisPerEpoch.millis

  /**
    * determines the number of RAFT log entries each batch must be
    * committed in.
    */
  // FIXME what is right value? should really be logical timestamp-based expiry.
  val logIdxCommitWindow = 6000

  /**
    * Number of cached batches at the tail of the log to retain in
    * memory. 1 batch per epoch, at an epoch window of 10ms, a cache
    * size of 500 will cache the last 5 seconds of transactions.
    */
  val logCachedBatches = 500

  /**
    * The amount of time a scope broker waits for a full batch for a
    * given epoch.
    */
  val batchStallTime = roundTripTime * 2

  /**
    * The maximum number of buffered received but un-merged batches
    * maintained by each datanode per log segment. This papers over any
    * time inconsistency between log segments.
    */
  val batchBufferSize = 1000
}

case class PipelineCtx[K, R: CBOR.Codec, RV: CBOR.Codec, W: CBOR.Codec, WV: CBOR.Codec](
  config: PipelineConfig,
  hostID: HostID,
  hostService: HostService,
  logNodeProvider: LogNodeProvider,
  keyExtractor: KeyExtractor[K, R, W],
  partitionerProvider: PartitionerProvider[R, W],
  filterCodec: CBOR.Codec[DataFilter[W]],
  executionContext: ExecutionContext,
  epochClock: Clock,
  timer: Timer,
  bus: MessageBus,
  readClock: DelayTrackingClock,
  ackDelayClock: DelayTrackingClock,
  backupRequestRatio: Double,
  stats: TxnPipeline.Stats) {

  object codecs {
    implicit val ReadCodec = implicitly[CBOR.Codec[R]]
    implicit val ReadValueCodec = implicitly[CBOR.Codec[RV]]
    implicit val WriteCodec = implicitly[CBOR.Codec[W]]
    implicit val WriteValueCodec = implicitly[CBOR.Codec[WV]]
    implicit val FilterCodec = filterCodec
  }

  /**
    * Protocol for messages from Log/Data nodes back to a Coordinator,
    * indicating whether a transaction was committed or applied.
    */
  val CoordProtocol = Protocol[CoordMessage]("tx.coord.reply")

  val LogProtocol = Protocol[LogMessage]("tx.log.request")
  val DataProtocol = Protocol[DataMessage]("tx.data.request")

  val transactionLogPath = config.rootPath / "transactions"
  val scopesPath = config.rootPath / "scopes"
  def scopeMetaPath(scope: ScopeID) = scopesPath / scope.toLong.toString / "metadata"
  def scopeDataPath(scope: ScopeID) = scopesPath / scope.toLong.toString / "data"

  val logger = getLogger(classOf[TxnPipeline[_, _, _, _, _]].toString)

  private[this] val _epochSynced = Promise[Unit]()

  def epochSynced = _epochSynced.isCompleted
  def markEpochSynced() = _epochSynced.setDone()
  def epochSyncedFuture: Future[Unit] = _epochSynced.future

  StatsRecorder.polling(10.seconds) {
    stats.setReadClockDelay(readClock.delay)
  }

  def partitioner: Partitioner[R, W] =
    partitionerProvider.partitioner

  def logSegmentFor(host: HostID): Option[SegmentID] =
    logTopology collectFirst {
      case (seg, hs) if hs contains host => seg
    }

  def logTopology: Map[SegmentID, SegmentInfo] =
    logNodeProvider.getLogNodeInfo.topology

  def orderedNodesForSegment(seg: SegmentID) =
    logTopology.get(seg).fold(Seq.empty[HostID]) { si =>
      hostService.preferredOrder(si.hosts)
    }

  // More efficient than orderedNodesForSegment drop N take 1. See HostService#preferredNode
  def nodeForSegment(seg: SegmentID, offset: Int = 0): Option[HostID] =
    logTopology.get(seg) flatMap { si =>
      hostService.preferredNode(si.hosts, offset)
    }

  // FIXME: reference counting on queued messages?
  def logHandler(f: (HostInfo, LogMessage, TimeBound) => Future[Unit]) =
    bus.handler(LogProtocol, config.logSignal)(f)

  // FIXME: reference counting on queued messages?
  def dataHandler(f: (HostInfo, DataMessage, TimeBound) => Future[Unit]) =
    bus.handler(DataProtocol, config.dataSignal)(f)

  def coordReplyHandler(f: (HostInfo, CoordMessage, TimeBound) => Future[Unit]) =
    bus.tempHandler(CoordProtocol, Duration.Inf)(f)

  def coordSink(dest: HandlerID) = bus.sink(CoordProtocol, dest)
  def logSink(host: HostID) = bus.sink(LogProtocol, config.logSignal.at(host))
  def dataSink(host: HostID) = bus.sink(DataProtocol, config.dataSignal.at(host))

  def ackTxn(
    txn: Batch.Txn,
    committed: Boolean,
    backoff: Boolean,
    leaderHint: Option[HostID])(implicit ec: ExecutionContext) = {
    val tracer = GlobalTracer.instance

    tracer.withTraceContext("tx.ack", txn.trace) {
      tracer.activeSpan foreach { span =>
        span.addAttribute("committed", committed)

        leaderHint map { hint =>
          span.addAttribute("leader_hint", hint.toString)
        }
      }

      coordSink(txn.origin)
        .send(CoordMessage.TxnCommitted(committed, backoff, leaderHint))
        .unit
    }
  }

  // FIXME: figure this out internally. gross.
  @volatile private var _globalPersistedTimestamp = Timestamp.Epoch
  def globalPersistedTimestamp = _globalPersistedTimestamp

  def hintGlobalPersistedTimestamp(ts: Timestamp) =
    if (_globalPersistedTimestamp < ts) {
      _globalPersistedTimestamp = ts
    }
}
