package fauna.repo.service.stream

import fauna.atoms._
import fauna.cluster._
import fauna.codex.cbor._
import fauna.exec._
import fauna.lang._
import fauna.lang.clocks._
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.net._
import fauna.net.bus.{ MessageBus, Protocol, SignalID }
import fauna.net.bus.transport.{ FrameCodec, TCPStreamOpenFailure }
import fauna.repo._
import fauna.stats._
import fauna.storage._
import fauna.storage.index._
import fauna.storage.ops._
import fauna.tx.transaction._
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelFuture
import java.util.concurrent.atomic.{ AtomicReference, LongAdder }
import scala.concurrent.{ Future, TimeoutException }
import scala.concurrent.duration._
import scala.util.{ Failure, Success }
import scala.util.control.NoStackTrace

final case class TxnResult(
  txnTS: Timestamp,
  dispatchTS: Timestamp,
  writes: Vector[Write]
)

/** The stream service coordinator.
  *
  * Streams are composed by two main components: a stream sink, that subscribes with
  * a given stream key into a source host which is defined by the stream key's
  * locality; and a stream source which processes writes from the transaction
  * pipeline and forwards them to the interested sinks. See `Sink.Manager` and
  * `Source.Manager` for more information.
  *
  * The stream coordinator connects sinks and sources via the network bus, and
  * sources with the local transaction pipeline via a write stream subscription.
  *
  * Note that upon subscribing to a stream, the first `TxnResult` pushed by the
  * coordinator will have no writes. Its txn time mark the beginning of the stream.
  * No other txn results should have an empty writes set after that.
  */
object Coordinator {

  final class MaxOpenStreamsExceeded extends NoStackTrace

  final case class Config(
    maxOpenStreams: Int = 10_000,
    messageTimeout: FiniteDuration = 5.seconds,
    mergerTimeout: FiniteDuration = 10.seconds, // messageTimeout * 2
    syncInterval: FiniteDuration = 10.seconds,
    sourceTTL: FiniteDuration = 1.minute,
    sourceIdlePeriod: FiniteDuration = 300.millis,
    sourceMaxIdleOverhead: Int = 256,
    sourceMaxBuffer: Int = 16384,
    sinkMaxBuffer: Int = 4096,
    sinkMergeQueueSize: Int = 16384
  )

  final class Stats(val recorder: StatsRecorder = StatsRecorder.Null) {
    def setActiveStreams(n: Int) = recorder.set("Streams.Active", n)
    def incrStreamsStarted() = recorder.incr("Streams.Started")
    def incrStreamsClosed() = recorder.incr("Streams.Closed")
    def incrRejected() = recorder.incr("Streams.Rejected")
    def incrOrphanKeyHits() = recorder.incr("Streams.Orphaned")
    def incrTimeout() = recorder.incr("Streams.Timeout")
    def incrFailedSync() = recorder.incr("Streams.Sync.Failed")
  }

  // Message protocol

  sealed trait Message

  final case class Subscribe(
    key: StreamKey,
    generation: Sink.Generation,
    replyTo: SignalID)
      extends Message

  final case class Sync(
    generation: Sink.Generation,
    mask: Vector[Long],
    replyTo: SignalID)
      extends Message

  final case class Result(
    generation: Sink.Generation,
    lastSentTS: Map[StreamKey, Timestamp],
    minAppliedTS: Timestamp,
    transactionTS: Timestamp,
    dispatchTS: Timestamp,
    writes: Vector[Write])
      extends Message

  implicit val ResultCodec =
    CBOR.TupleCodec[Result]

  implicit val MessageCodec =
    CBOR.SumCodec[Message](
      CBOR.TupleCodec[Subscribe],
      ResultCodec,
      CBOR.TupleCodec[Sync]
    )

  implicit val MessageProtocol =
    Protocol[Message]("datanode.streams.message")

  final case class Subscribed(startTS: Timestamp, index: Source.Index)

  implicit val SubscribedCodec = CBOR.TupleCodec[Subscribed]

  implicit val SubscribeReply =
    Protocol.Reply[Subscribe, Subscribed]("datanode.streams.subscribe.reply")

  implicit val SyncReply =
    Protocol.Reply[Sync, Unit]("datanode.streams.sync.reply")
}

final class Coordinator(
  protocolSignal: SignalID,
  txnResultSignal: SignalID,
  bus: MessageBus,
  txnPipeline: TxnPipeline[_, TxnRead, _, Txn, StorageEngine.TxnResult],
  keyLocator: KeyLocator[ByteBuf, _],
  config: Coordinator.Config = Coordinator.Config(),
  stats: Coordinator.Stats = new Coordinator.Stats())
    extends ExceptionLogging {
  import Coordinator._

  private[this] val writeStream = new WriteStream(handleTxnWrites, stats.recorder)
  private[this] val highestLocalTxnTS = new AtomicReference(Clock.time)
  private[this] val totalStreams = new LongAdder()
  private[this] val log = getLogger()

  private[this] val mergers =
    new StreamMerger(
      config.sinkMaxBuffer,
      config.sinkMergeQueueSize,
      config.sourceIdlePeriod
    )

  private[this] val sinks =
    new Sink.Manager[StreamKey, TxnResult](
      locate,
      subscribe,
      syncWithSource,
      config.sinkMaxBuffer
    )

  private[this] val sources =
    new Source.Manager[StreamKey, Write](
      // NOTE: `txnPipeline.minAppliedTimestamp` is updated after *scheduling*
      // transactions. It may rush ahead of txn application as it relies on per key
      // LAT and the `ApplySequencer` to guarantee the linearizability of txn
      // application. Using the write stream view of the min applied timestamp by
      // default ensures a monotonic cursor that is not above any pending txn result.
      () => writeStream.minAppliedTS.getOrElse(txnPipeline.minAppliedTimestamp),
      forwardResultsToSink,
      config.sourceMaxBuffer,
      config.sourceTTL,
      config.sourceIdlePeriod,
      config.sourceMaxIdleOverhead
    )

  private[this] val protocolHandler =
    bus.handler(MessageProtocol, protocolSignal) {
      case (host, msg: Subscribe, deadline) =>
        recvSubscribe(host, msg.key, msg.generation, msg.replyTo, deadline)

      case (host, msg: Sync, deadline) =>
        recvSync(host, msg.generation, msg.mask, msg.replyTo, deadline)

      case (host, _: Result, _) =>
        log.warn(
          s"Received result message on a non-dedicated TCP stream from ${host.id}")
        Future.unit
    }

  private[this] val txnResultsHandler =
    bus.tcpStreamHandler(txnResultSignal) { (sourceHost, channel) =>
      channel.pipeline.addBefore(
        "channel syntax handler",
        "frame decoder",
        FrameCodec.newDecoder
      )

      implicit val ec = FaunaExecutionContext.Implicits.global
      def recvLoop(): Future[Unit] = {
        channel.recv() flatMap { buf =>
          buf releaseAfter { buf =>
            val msg = CBOR.decode(buf)(ResultCodec)
            recvResult(
              sourceHost,
              msg.generation,
              msg.lastSentTS,
              msg.minAppliedTS,
              msg.transactionTS,
              msg.dispatchTS,
              msg.writes
            )
            recvLoop()
          }
        }
      }
      logException(recvLoop())
    }

  txnPipeline.addOrReplaceStream(writeStream)
  writeStream.start()

  Timer.Global.scheduleRepeatedly(config.syncInterval, !protocolHandler.isClosed) {
    sinks.synchronize()
  }

  def activeStreams = totalStreams.sum.toInt

  def forDocument(
    scope: ScopeID,
    id: DocID,
    idlePeriod: Duration = Duration.Inf
  ): Observable[TxnResult] =
    filterEvents(
      forKey(StreamKey.DocKey(scope, id)),
      idlePeriod
    )

  def forIndex(
    cfg: IndexConfig,
    terms: Vector[IndexTerm],
    idlePeriod: Duration = Duration.Inf
  ): Observable[TxnResult] = {
    val streamKey =
      StreamKey.SetKey(
        cfg.scopeID,
        cfg.id,
        terms map { _.value }
      )

    if (!cfg.isPartitioned) {
      filterEvents(forKey(streamKey), idlePeriod)
    } else {
      def partitions =
        cfg.partitions(terms).view map { terms =>
          val key = StreamKey.SetKey(cfg.scopeID, cfg.id, terms map { _.value })
          forKey(key) map { StreamMerger.Input(key, _) }
        } toSet
      val merged = mergers.register(streamKey, partitions)
      filterEvents(merged, idlePeriod)
    }
  }

  private def forKey(key: StreamKey): Observable[TxnResult] = {
    if (activeStreams > config.maxOpenStreams) {
      stats.incrRejected()
      Observable.failed(new MaxOpenStreamsExceeded())
    } else {
      val obs = sinks.register(key)
      totalStreams.increment()
      stats.incrStreamsStarted()

      obs ensure {
        stats.incrStreamsClosed()
        totalStreams.decrement()
      }
    }
  }

  /** Empty results are used to communicate the stream's last seen txn time. One empty
    * txn result will ALWAYS be emitted at subscription time. Then, one empty result
    * will be emitted approximately at every `idlePeriod` interval. Note that an
    * infinite idle period effectively filters out all empty results after the first
    * one.
    */
  private def filterEvents(
    obs: Observable[TxnResult],
    idlePeriod: Duration
  ): Observable[TxnResult] = {
    // Always emit the first empty result.
    @volatile var lastEmpty: Timestamp = null

    obs transform {
      case (observer, txn) if lastEmpty eq null =>
        // Start event. Turns the first txn result seen into an idle notification.
        lastEmpty = txn.txnTS
        observer.onNext(txn.copy(writes = Vector.empty))

      case (observer, txn) if txn.writes.isEmpty =>
        // Idle event.
        if (txn.txnTS.difference(lastEmpty) > idlePeriod) {
          lastEmpty = txn.txnTS
          observer.onNext(txn)
        } else {
          Observer.ContinueF
        }

      case (observer, txn) =>
        // Normal event.
        observer.onNext(txn)
    }
  }

  // WriteStream handlers

  private def handleTxnWrites(txnTS: Timestamp, writes: Vector[Write]): Unit = {
    highestLocalTxnTS.accumulateAndGet(txnTS, _ max _)
    val writesByKey = StreamKey.groupWritesByKey(writes)
    sources.publish(txnTS, Clock.time, writesByKey)
  }

  // Sink handlers

  private def locate(key: StreamKey): HostID = {
    val rowKey = key.rowKey
    val read = TxnRead(rowKey, RegionID.DefaultID)
    val part = txnPipeline.ctx.partitionerProvider.partitioner
    val hosts = part.hostsForRead(read)

    txnPipeline.ctx.hostService.preferredNode(hosts) getOrElse {
      stats.incrOrphanKeyHits()
      val location = keyLocator.locate(rowKey)
      throw ClusterTopologyException(location)
    }
  }

  private def subscribe(
    key: StreamKey,
    sourceHostID: HostID,
    generation: Sink.Generation): Future[(Timestamp, Source.Index)] = {

    implicit val ec = ImmediateExecutionContext
    val busSink = bus.sink(MessageProtocol, protocolSignal.at(sourceHostID))

    busSink.request(
      Subscribe(key, generation, _),
      config.messageTimeout.bound) flatMap {
      case Some(reply) =>
        Future.successful((reply.startTS, reply.index))

      case None =>
        stats.incrTimeout()
        Future.failed(
          new TimeoutException("Timed out while waiting for stream subscription."))
    }
  }

  private def syncWithSource(
    sourceHostID: HostID,
    generation: Sink.Generation,
    mask: Vector[Long]): Future[Unit] = {

    implicit val ec = ImmediateExecutionContext
    val busSink = bus.sink(MessageProtocol, protocolSignal.at(sourceHostID))

    busSink.request(Sync(generation, mask, _), config.messageTimeout.bound) flatMap {
      case Some(_) => Future.unit
      case None =>
        log.warn(
          s"Failed to synchronize ${mask.size} streams with source host " +
            s"$sourceHostID at generation ${generation.toLong}. Streams " +
            s"depending on this host will be closed.")

        stats.incrFailedSync()
        Future.failed(
          new TimeoutException("Timed out while synchronizing with source."))
    }
  }

  private def recvResult(
    sourceHost: HostInfo,
    generation: Sink.Generation,
    lastSentTxn: Map[StreamKey, Timestamp],
    minAppliedTS: Timestamp,
    transactionTS: Timestamp,
    dispatchTS: Timestamp,
    writes: Vector[Write]): Unit = {

    val groupedWrites = StreamKey.groupWritesByKey(writes)

    lastSentTxn foreach { case (key, lastSentTS) =>
      val (txnTS, writes) = groupedWrites.get(key) match {
        case Some(ws) => (transactionTS, ws) // writes from current txn
        case None     => (minAppliedTS, Vector.empty) // idle notification
      }
      val result = TxnResult(txnTS, dispatchTS, writes)
      sinks.receive(sourceHost.id, generation, lastSentTS, txnTS, key, result)
    }
  }

  // Source handlers

  private def forwardResultsToSink(
    sinkHostID: HostID,
    generation: Sink.Generation,
    results: Source[StreamKey, Write]): Unit = {

    implicit val ec = ImmediateExecutionContext

    val channelF =
      bus.openTCPStreamWithConfig(
        txnResultSignal.at(sinkHostID),
        s"dedicated streaming connection with $sinkHostID"
      ) { ch =>
        ch.enableChannelSyntax()
        ch.pipeline.addBefore(
          "channel syntax handler",
          "frame encoder",
          FrameCodec.newEncoder
        )
        ch.closeFuture.addListener { _: ChannelFuture =>
          results.close()
        }
      }

    channelF andThen {
      case Success(ch) =>
        logException {
          results.output foreachAsyncF { out =>
            val res = Result(
              generation,
              out.lastSentTS,
              out.minAppliedTS,
              out.transactionTS,
              out.dispatchTS,
              out.values
            )

            val buf = ch.alloc.buffer()
            CBOR.encode(buf, res)(ResultCodec)
            ch.sendAndFlush(buf)
          } ensure {
            ch.closeQuietly()
          }
        }
      case Failure(ex: TCPStreamOpenFailure) =>
        logException(ex)
    }
  }

  private def recvSubscribe(
    sinkHost: HostInfo,
    key: StreamKey,
    generation: Sink.Generation,
    replyTo: SignalID,
    deadline: TimeBound): Future[Unit] = {

    sources.register(sinkHost.id, generation, key).fold(Future.unit) { index =>
      implicit val ec = ImmediateExecutionContext
      val busSink = bus.sink(SubscribeReply, replyTo.at(sinkHost.id))
      busSink.send(Subscribed(highestLocalTxnTS.get, index), deadline).unit
    }
  }

  private def recvSync(
    sinkHost: HostInfo,
    generation: Sink.Generation,
    mask: Vector[Long],
    replyTo: SignalID,
    deadline: TimeBound): Future[Unit] = {

    implicit val ec = ImmediateExecutionContext
    if (sources.synchronize(sinkHost.id, generation, mask)) {
      val busSink = bus.sink(SyncReply, replyTo.at(sinkHost.id))
      busSink.send((), deadline).unit
    } else {
      Future.unit
    }
  }

  def close(): Unit = {
    txnPipeline.removeStream(writeStream)
    writeStream.stop(graceful = true)
    protocolHandler.close()
    txnResultsHandler.close()
    sources.close()
    mergers.close()
    sinks.close()
  }
}
