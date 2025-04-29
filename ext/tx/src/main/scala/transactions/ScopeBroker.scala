package fauna.tx.transaction

import fauna.atoms._
import fauna.codex.cbor.CBOR
import fauna.exec.AsyncSemaphore
import fauna.lang.{ TimeBound, Timestamp, Timing }
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.stats.StatsRecorder
import fauna.trace.GlobalTracer
import io.netty.buffer.Unpooled
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.ConcurrentHashMap
import scala.collection.immutable.ArraySeq
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.{ Failure, Success }

class ScopeBroker[K, R, RV, W, WV](
  service: DataNode[K, R, RV, W, WV],
  applySequencer: ApplySequencer[K]) extends ExceptionLogging {

  import LogMessage._
  import service.ctx.codecs._

  private implicit def ec = ctx.executionContext
  private def ctx = service.ctx
  private def store = service.store

  private val tracer = GlobalTracer.instance

  val queue = new ScopeTxnQueue(
    ctx.epochClock,
    ctx.stats,
    ctx.config.batchBufferSize,
    ctx.config.maxTransactionsPerEpoch)

  private[this] val semaphore = AsyncSemaphore(ctx.config.maxConcurrentTransactions)
  // Track the available slots we have for executing transactions
  StatsRecorder.polling(10.seconds) {
    ctx.stats.recordConcurrentTransactionSemaphoreAvailability(
      semaphore.availablePermits)
  }

  private[this] var logReadStalls = 0

  private[this] val streams = new AtomicReference[Set[DataStream[W, WV]]](Set.empty)
  private[this] val lastAcceptSendEpochs = new ConcurrentHashMap[Int, Long]()
  private[this] final val AcceptSendEpochs = LogNode.MaxPipelinedEpochs / 32 // Some small fraction of LogNode.MaxPipelinedEpochs

  private def sendFilterAction(filter: DataFilter[W], isAdd: Boolean): Unit =
    ctx.logTopology.keys foreach { seg =>
      ctx.nodeForSegment(seg, logReadStalls) foreach { node =>
        ctx.logSink(node).send(FilterAction(CBOR.encode(filter), isAdd))
      }
    }

  /**
    * Add or replace existing stream thus preserving the latest added as the live
    * feed. Preserving the latest added stream makes for predictable handling of
    * racing code at call-site.
    */
  final def addOrReplaceStream(stream: DataStream[W, WV]): Unit = {
    streams.updateAndGet { _ - stream + stream }
    stream.filter foreach { filter =>
      sendFilterAction(filter, isAdd = true)
    }
  }

  final def removeStream(stream: DataStream[W, WV]): Unit = {
    streams.updateAndGet { _ - stream }
    stream.filter foreach { filter =>
      sendFilterAction(filter, isAdd = false)
    }
  }

  private def encodedFilters = {
    val fs = streams.get
    if (fs.isEmpty) {
      Unpooled.EMPTY_BUFFER
    } else {
      val encodable = fs.view.flatMap { _.filter }.toVector
      if (encodable.nonEmpty) CBOR.encode(encodable) else Unpooled.EMPTY_BUFFER
    }
  }

  // NOTE: while it seems we could be skipping transactions here, it is not
  // happening, as the log is not truncated until data nodes have all checked
  // in that they are past a tx. The function is currently routed through the
  // health checker. It probably should be internalized into the transaction
  // pipelines machinery as it’s weirdly roundabout currently.
  //   Reinit can only be observed by newly joining nodes and they will be
  // streaming their initial data from other nodes first anyway.
  //   If log was truncated more eagerly, this node would need to re-copy its
  // data for this scope at a covered applied timestamp from another node, and
  // then play the log from that point.
  def reinit(host: HostID, seg: SegmentID, prev: Epoch): Future[Unit] = {
    ctx.logTopology.get(seg) match {
      case Some(SegmentInfo(_, SegmentState.HasStart(start), _)) =>
        queue.lastEpochs flatMap { segs =>
          segs.get(seg) match {
            case Some(lastSeen) if prev <= lastSeen =>
              ctx.logger.info(s"Received reinit for $seg from $host with $prev <= $lastSeen. Ignoring.")
              Future.unit
            case Some(lastSeen) =>
              if (lastSeen + 1 == start || ctx.partitioner.segments(ctx.hostID, false).isEmpty) {
                ctx.logger.info(s"Received transaction queue reinit of $seg from $host. Safely skipping from $lastSeen to $prev")
              } else {
                ctx.logger.warn(s"Received invalid transaction queue reinit of $seg from $host. Can't safely skip from $lastSeen to $prev; locking storage.")
                store.lock()
              }

              queue.reinit(seg, prev)
            case None =>
              ctx.logger.warn(s"Received reinit for unknown segment $seg from $host. Ignoring.")
              Future.unit
          }
        }
      case _ =>
        ctx.logger.warn(s"Received reinit for unknown or not-started segment $seg from $host. Ignoring.")
        Future.unit
    }
  }

  def updateSegments(sl: Map[SegmentID, SegmentInfo]): Future[Unit] =
    queue.updateSegments(sl, Epoch(applySequencer.maxAppliedTimestamp))

  def getBatches(host: HostID, segment: SegmentID, pvers: Long, epoch: Epoch): Unit =
    ctx.logSink(host).send(GetBatches(segment, pvers, epoch, Some(ScopeID.RootID), encodedFilters))

  def acceptBatches(segment: SegmentID, pvers: Long, epoch: Epoch): Unit = {
    def sendEpoch(last: Long, curr: Long) =
      if (curr - last >= AcceptSendEpochs) { curr } else { last }

    val eidx = epoch.idx
    if (lastAcceptSendEpochs.merge(segment.toInt, eidx, sendEpoch) == eidx) {
      val nodes = ctx.orderedNodesForSegment(segment)
      nodes take 2 foreach { ctx.logSink(_).send(AcceptBatches(pvers, epoch, encodedFilters)) }
    }
  }

  def start(): Unit = {
    // start the subscribe loop
    logException(pollQueue())
  }

  private def pollQueue(): Future[Unit] =
    if (service.isClosed) {
      Future.unit
    } else {
      queue.poll(ctx.config.batchStallTime) flatMap {
        case Some(batches) if batches.nonEmpty =>
          processBatches(batches) flatMap { _ => pollQueue() }

        case Some(_) =>
          ctx.logger.debug(s"DataNode: received no batches?!")
          pollQueue()

        case None =>
          // Queue stalled. Prime the pump.
          processBatchStall() flatMap { _ => pollQueue() }
      }
    }

  private def processBatchStall(): Future[Unit] = {
    ctx.stats.incrLogReadStalls()
    ctx.logger.debug(s"DataNode: Stalled batch queue")
    val latest = ctx.logTopology collect {
      case (s, SegmentInfo(_, SegmentState.HasStart(start), _)) => (s, start - 1)
    }

    queue.lastEpochs map { last =>
      val segs = latest ++ last

      ctx.logger.debug(s"DataNode: Getting batches from $segs")

      segs foreach {
        case (seg, ep) =>
          val node = ctx.nodeForSegment(seg, logReadStalls)

          ctx.logger.debug(s"DataNode: Chosen node for $seg: $node")

          node foreach { getBatches(_, seg, ctx.partitioner.version, ep) }
      }
      logReadStalls += 1
    }
  }

  private def processBatches(batches: Seq[Batch]): Future[Unit] = {
    var ceilTS = Timestamp.Epoch
    val txns = ArraySeq.newBuilder[Transaction[W]]
    val part = ctx.partitioner

    logReadStalls = 0

    if (ctx.logger.isDebugEnabled) {
      batches foreach { b =>
        ctx.logger.debug(s"DataNode: Processing batch $b")
      }
    }

    batches foreach { b =>
      ctx.stats.incrBatchesReceived()

      ceilTS = b.ceilTimestamp
      val fs = streams.get
      b.transactions[W] foreach { txn =>
        // It's possible that we already applied some of the txns in the batch.
        // Typically happens on restart if the last persisted TS is not on an
        // batch (epoch) boundary. We must not schedule these transactions
        // again, as they won't be able to complete their remote reads as
        // DataNode is discarding remote read results older than LAT.
        if (txn.ts > applySequencer.maxAppliedTimestamp) {
          // If we have data filters, we need to partition incoming transactions.
          // The filters will only process the transactions they require so we send
          // all through the filter. However, we check whether we cover a transaction
          // before trying to apply it to reduce the amount of work required to apply
          // transactions.
          if (fs.isEmpty) {
            txns += txn
          } else {
            // Transactions covered by this node dispatch results after the txn has
            // been applied to prevent duplicates due to contention.
            if (part.coversTxn(ctx.hostID, txn.expr)) {
              txns += txn
            } else {
              // Transactions that are not covered by this node has been forward to
              // it by the log nodes with the only purpose of dispatching their
              // results to streams. Since these writes are not covered, we can't
              // defer their dispatch. This is part of the legacy log-node stream
              // subscription algorithm.
              fs foreach { _.onUncovered(txn) }
            }
          }
        }
      }
    }

    def process0(txns: ArraySeq[Transaction[W]]): Future[Unit] = {
      val timing = Timing.start
      semaphore.acquire(txns.size) flatMap { num =>
        ctx.stats.recordAcquireTimingElapsed(timing)
        val (batch, rest) = txns splitAt num

        val maxTS = (batch foldLeft Timestamp.Epoch) { (_, t) =>
          processTxn(part, t)
          ctx.stats.incrProcessedTransactions()
          t.ts
        }

        ctx.stats.recordBatchTimingElapsed(timing)

        if (rest.isEmpty) {
          applySequencer.updateMaxAppliedTimestamp(ceilTS)
          Future.unit
        } else {
          applySequencer.updateMaxAppliedTimestamp(maxTS)
          process0(rest)
        }
      }
    }

    process0(txns.result())
  }

  private def processTxn(part: Partitioner[R, W], txn: Transaction[W]): Unit =
    tracer.withTraceContext("tx.apply", txn.trace) {
      val timing = Timing.start

      // Extract reads and read/write keys
      val readTS = txn.ts.prevNano
      val reads = store.readsForTxn(txn.expr)
      val localReads = reads filter { part.coversRead(_) }
      val isWriter = part.hostsForWrite(txn.expr) contains ctx.hostID
      val fs = streams.get

      if (ctx.logger.isDebugEnabled) {
        val rks = show((reads map ctx.keyExtractor.readKey).toSet)
        val wks = show(ctx.keyExtractor.writeKeys(txn.expr).toSeq)
        ctx.logger.debug(
          s"DataNode COVER: ${txn.ts}, $rks, $wks ${show(txn.expr)} (version=${part.version}")
      }

      // get futures for read results
      val readResultFs = if (isWriter) {
        val isRecovery = service.isInRecovery
        reads map { service.readBroker.registerRead(readTS, _, isRecovery) }
      } else {
        Nil
      }

      // wire up local reads for local use and/or broadcast
      if (localReads.nonEmpty) {

        // use the latest partitioner for choosing hosts to broadcast to.
        val part = ctx.partitioner
        val peers = part.hostsForWrite(txn.expr) filter { h =>
          h != ctx.hostID && ctx.hostService.isLocal(h)
        }

        localReads foreach { read =>
          applySequencer.join(ctx.keyExtractor.readKey(read)) foreach { _ =>
            // We can't give up here so, use maximum deadline.
            store.evalTxnRead(readTS, read, TimeBound.Max) foreach { result =>
              // If any peers are interested in this result, forward it
              // along.
              if (peers.nonEmpty) {
                ctx.logger.debug(
                  s"DataNode FORWARDING READ: ${txn.ts}, rts $readTS, $read, $result, $peers (version=${part.version})")
                service.sendReadResult(
                  peers,
                  readTS,
                  None,
                  read,
                  result,
                  part.version)
              } else if (ctx.logger.isDebugEnabled) {
                ctx.logger.debug(
                  s"DataNode: no local host covers read? ${txn.ts}, rts $readTS, $read, $result (version=${part.version})")
              }

              // If the local host is interested in this result, forward
              // it locally.
              if (isWriter) {
                ctx.logger.debug(
                  s"DataNode FORWARDING LOCALLY: ${txn.ts}, rts $readTS, $read, $result, ${ctx.hostID} (version = ${part.version})")
                service.readBroker.recvResult(
                  ctx.hostID,
                  readTS,
                  read,
                  result,
                  None,
                  part.version)
              }
            }
          }
        }
      }

      // apply transaction
      val applyF = if (isWriter) {
        val timing = Timing.start
        val writeKeys = ctx.keyExtractor.writeKeys(txn.expr).toSet

        // The txn is about to be executed (most likely in parallel). Feed them first
        // to the data-streams in the sequential block so that they are able to
        // maintain txn order when gathering the results of parallel txn execution.
        fs foreach { _.onSchedule(txn) }

        // sequence write
        // TODO: limit to local keys when it is reasonable to do so. (the
        // partitioner doesn't provide a locality check for a key, and it's not
        // worth adding just for this.)
        applySequencer.sequence(txn.ts, writeKeys, ctx.stats) {
          ctx.stats.recordSequencedTransactionTiming(timing)
          reads foreach { service.readBroker.readRegistered(readTS, _) }
          readResultFs.sequence flatMap { reads =>
            store.evalTxnApply(txn.ts, reads.toMap, txn.expr) map { result =>
              ctx.logger.debug(
                s"DataNode APPLY: ${txn.ts}, ${show(reads)} ${show(result)}")
              Some(result)
            }
          }
        }
      } else {
        FutureNone
      }

      // post-apply async operations
      applyF onComplete { t =>
        semaphore.release()
        logException(t)

        // under this condition, transactions cannot be applied to
        // storage, but exiting the process wouldn't allow for repair
        // and recovery; stop the data node and wait for operator
        // intervention.
        t match {
          case Failure(_: StorageLockedException) =>
            ctx.logger.warn("DataNode: storage locked. Stopping...")
            service.close()

          case Failure(_) =>
            fs foreach { _.onResult(txn, None) }

          case Success(rv) =>
            ctx.stats.incrAppliedTxns()
            ctx.stats.recordApplyTimingElapsed(timing)

            if (rv.isDefined && (txn.ts + ctx.config.maxWriteTimeout) > ctx.epochClock.time) {
              // FIXME: digest values are just placeholders for now.
              val msg = CoordMessage.TxnApplied(txn.ts, CBOR.encode(rv).toByteArray, Array.empty)
              ctx.coordSink(txn.origin).send(msg)
            }

            if (isWriter) {
              // The txn was executed by the writer. Notify the data-streams on
              // its result so that they can flush pending streams that were
              // previously scheduled.
              fs foreach { _.onResult(txn, rv) }
            }
        }
      }
    }
}
