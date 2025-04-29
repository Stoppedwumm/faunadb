package fauna.repo.service

import fauna.atoms._
import fauna.exec._
import fauna.lang.clocks.Clock
import fauna.net.bus._
import fauna.repo.query._
import fauna.repo.service.stream.{ Coordinator, TxnResult }
import fauna.repo.IndexConfig
import fauna.stats._
import fauna.storage._
import fauna.storage.index.IndexTerm
import fauna.storage.ir._
import fauna.tx.transaction._
import io.netty.buffer.ByteBuf
import io.netty.util.{
  AbstractReferenceCounted,
  IllegalReferenceCountException,
  ReferenceCounted
}
import java.io.Closeable
import java.util.concurrent.{ ConcurrentHashMap, CopyOnWriteArrayList }
import scala.concurrent.duration.Duration

trait StreamingService {

  def forDocument(
    scope: ScopeID,
    docID: DocID,
    idlePeriod: Duration = Duration.Inf
  ): Observable[TxnResult]

  def forIndex(
    cfg: IndexConfig,
    terms: Vector[IndexTerm],
    idlePeriod: Duration = Duration.Inf
  ): Observable[TxnResult]
}

final class StreamService(
  protocolSignal: SignalID,
  txnResultSignal: SignalID,
  bus: MessageBus,
  txnPipeline: TxnPipeline[_, TxnRead, _, Txn, StorageEngine.TxnResult],
  keyLocator: KeyLocator[ByteBuf, _],
  config: Coordinator.Config = Coordinator.Config(),
  recorder: StatsRecorder = StatsRecorder.Null)
    extends StreamingService
    with Closeable {

  private[this] val stats = new Coordinator.Stats(recorder)

  private[this] val logNodeStreams =
    new LogNodeStreamCoordinator(txnPipeline, stats)

  private[this] val dataNodeStreams =
    new Coordinator(
      protocolSignal,
      txnResultSignal,
      bus,
      txnPipeline,
      keyLocator,
      config,
      stats
    )

  StatsRecorder.polling {
    val logStreams = logNodeStreams.activeStreams
    val dataStreams = dataNodeStreams.activeStreams
    stats.setActiveStreams(logStreams + dataStreams)
  }

  def forDocument(
    scope: ScopeID,
    docID: DocID,
    idlePeriod: Duration = Duration.Inf
  ): Observable[TxnResult] =
    dataNodeStreams.forDocument(scope, docID, idlePeriod)

  def forIndex(
    cfg: IndexConfig,
    terms: Vector[IndexTerm],
    idlePeriod: Duration = Duration.Inf
  ): Observable[TxnResult] =
    dataNodeStreams.forIndex(cfg, terms, idlePeriod)

  // NB. Legacy log node streams are deprecated.
  def forMatch(
    scope: ScopeID,
    indexID: IndexID,
    terms: Vector[IRValue]): Observable[TxnResult] =
    logNodeStreams.forMatch(scope, indexID, terms)

  // NB. Legacy log node streams are deprecated.
  def forCollSet(
    scope: Option[ScopeID],
    collIDs: Set[CollectionID]): Observable[TxnResult] =
    logNodeStreams.forCollSet(scope, collIDs)

  def close(): Unit = {
    logNodeStreams.close()
    dataNodeStreams.close()
  }
}

/** Log node stream coordinator subscribe to transactions at the log node level.
  * As transactions get filtered by the log node, it uses the filter provided by the
  * subscription protocol to determine which transactions to send to the data node.
  * As the data node process transactions, it notifies subscribed data streams.
  */
private final class LogNodeStreamCoordinator(
  txnPipeline: TxnPipeline[_, _, _, Txn, StorageEngine.TxnResult],
  stats: Coordinator.Stats)
    extends Closeable {

  /** A mechanism for streaming writes. This is a ref counted class in order to match
    * a single StreamDef to a single WriteStream regardless of the number of
    * consumers requesting the same StreamDef.
    */
  private case class WriteStream(streamDef: StreamDef)
      extends AbstractReferenceCounted
      with DataStream[Txn, StorageEngine.TxnResult] {

    val filter = Some(streamDef) // Subscribe at log nodes via this data filter.

    private val publishers = new CopyOnWriteArrayList[Publisher[TxnResult]]()

    def subscribe(publisher: Publisher[TxnResult]): Unit =
      publishers.add(publisher)

    def onSchedule(txn: Transaction[Txn]): Unit = ()

    def onUncovered(txn: Transaction[Txn]): Unit =
      onResult(txn, Some(StorageEngine.TxnSuccess))

    def onResult(
      txn: Transaction[Txn],
      result: Option[StorageEngine.TxnResult]): Unit =
      if (streamDef.covers(txn.expr) && result.isDefined) {
        val (_, txnWrites) = txn.expr
        val writes = txnWrites filter { streamDef.covered(_) }

        if (writes.nonEmpty) {
          val res = TxnResult(txn.ts, Clock.time, writes)
          publishers forEach { pub =>
            if (!pub.publish(res)) {
              publishers.remove(pub)
            }
          }
        }
      }

    def touch(hint: Object): ReferenceCounted = this

    protected def deallocate(): Unit =
      txnPipeline.removeStream(this)

    def close(): Unit =
      publishers forEach { _.close() }
  }

  private[this] val streams = new ConcurrentHashMap[StreamDef, WriteStream]()

  def activeStreams: Int = streams.size()

  def forMatch(
    scope: ScopeID,
    indexID: IndexID,
    terms: Vector[IRValue]): Observable[TxnResult] =
    startStream(MatchFilter(scope, indexID, terms))

  def forCollSet(
    scope: Option[ScopeID],
    collID: Set[CollectionID]): Observable[TxnResult] =
    startStream(CollectionSetFilter(scope, collID.toVector))

  private def startStream(streamDef: StreamDef): Observable[TxnResult] =
    Observable.create { pub: Publisher[TxnResult] =>
      stats.incrStreamsStarted()

      var create: Boolean = false
      val stream = streams.compute(
        streamDef,
        {
          case (_, null) =>
            create = true
            WriteStream(streamDef)
          case (_, stream) =>
            try {
              stream.retain()
              stream
            } catch {
              case _: IllegalReferenceCountException =>
                create = true
                WriteStream(streamDef)
            }
        }
      )

      stream.subscribe(pub)

      if (create) {
        txnPipeline.addOrReplaceStream(stream)
      }

      () => {
        stats.incrStreamsClosed()
        stream.release()
      }
    }

  def close(): Unit =
    streams forEach { (_, stream) =>
      stream.close()
    }
}
