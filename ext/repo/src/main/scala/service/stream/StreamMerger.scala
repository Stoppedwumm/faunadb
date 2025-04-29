package fauna.repo.service.stream

import fauna.exec._
import fauna.lang._
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.storage.ops.Write
import io.netty.util.{
  AbstractReferenceCounted,
  IllegalReferenceCountException,
  ReferenceCounted
}
import java.util.concurrent.{ ConcurrentHashMap, ConcurrentSkipListMap }
import java.util.concurrent.atomic.AtomicMarkableReference
import scala.concurrent.duration.Duration
import scala.concurrent.Future
import scala.util.control.NoStackTrace

object StreamMerger {

  object SyncTimeout extends Exception with NoStackTrace

  final case class Input(partition: StreamKey.SetKey, txn: TxnResult) {
    def txnTS = txn.txnTS
    def writes = txn.writes
  }

  private lazy val log = getLogger()
}

/** Stream merger aggregates merged streams by their set key.
  *
  * Merged streams are composed of multiple partitions, each being an observable of
  * transaction results. The merger works in 2 phases: first, it waits for at least
  * one txn result per partition, choosing the highest seen txn time as the merged
  * stream txn time; then it merges txns after the chosen txn time into a shared
  * buffer that subscribers listen to.
  *
  * The merge queue is designed to be non-blocking. It synchronizes multiple
  * publishers via an atomic cursor so that competing threads can collaboratively
  * share the load of publishing a batch of merged txns downstream.
  */
final class StreamMerger(
  maxBufferSize: Int,
  maxMergeQueueSize: Int = Int.MaxValue,
  idlePeriod: Duration = Duration.Inf
) {
  import StreamMerger._

  private val mergers = new ConcurrentHashMap[StreamKey.SetKey, Merger]

  def register(
    key: StreamKey.SetKey,
    partitions: => Set[Observable[Input]]
  ): Observable[TxnResult] =
    mergers.compute(
      key,
      {
        case (_, null) =>
          new Merger(key, partitions)
        case (_, merger) =>
          try {
            merger.retain()
            merger
          } catch {
            case _: IllegalReferenceCountException =>
              new Merger(key, partitions)
          }
      }
    )

  def close(): Unit =
    mergers.values forEach { _.close() }

  private final class MergedTxn(private[this] val txnTS: Timestamp) {
    private[this] var dispatchTS = Timestamp.MaxMicros
    private[this] val writes = Vector.newBuilder[Write]

    def merge(txn: TxnResult): MergedTxn = {
      require(txnTS == txn.txnTS, "merging transactions with different timestamps")
      dispatchTS = dispatchTS.min(txn.dispatchTS)
      writes ++= txn.writes
      this
    }

    def result(): TxnResult =
      TxnResult(
        txnTS,
        dispatchTS,
        writes.result()
      )
  }

  private final class Merger(
    key: StreamKey.SetKey,
    partitions: Set[Observable[Input]])
      extends AbstractReferenceCounted
      with Observable[TxnResult]
      with Observer[Input] {

    private val buffer = new AsyncRingBuffer[TxnResult](maxBufferSize)
    private val maxTxnTSByPartition = new ConcurrentHashMap[StreamKey, Timestamp]
    private val mergedTxnByTS = new ConcurrentSkipListMap[Timestamp, MergedTxn]
    private val minFlushedCursor = new AtomicMarkableReference(Timestamp.Min, false)

    // Satisfy Observer[Input] trait.
    implicit val ec = FaunaExecutionContext.Implicits.global

    // NB. Subscribe to all partitions right away since that may take some time. This
    // is a best effort to reduce multi-partition stream's subscription time.
    private val cancenations = partitions map { _.subscribe(this) }

    def subscribe(observer: Observer[TxnResult]): Cancelable =
      buffer
        .ensure(release())
        .subscribe(observer)

    def onNext(input: Input): Future[Observer.Ack] = {
      maxTxnTSByPartition.compute(
        input.partition,
        {
          case (_, null)    => input.txnTS
          case (_, currMax) => input.txnTS.max(currMax)
        })

      val minFlushedTS = minFlushedCursor.getReference

      // Discard idle notifications: they are synthetically produced when flushing.
      if (input.writes.nonEmpty) {
        mergedTxnByTS
          .compute(
            input.txnTS,
            {
              case (_, null)   => new MergedTxn(input.txnTS).merge(input.txn)
              case (_, merged) => merged.merge(input.txn)
            })
      }

      if (minFlushedTS == Timestamp.Min) {
        if (maxTxnTSByPartition.size == partitions.size) {
          attemptToStartMergedStream()
        } else if (mergedTxnByTS.size > maxMergeQueueSize) {
          close(SyncTimeout)
        }
      } else {
        attemptToFlushMergedTxns()
      }

      Observer.ContinueF
    }

    private def attemptToStartMergedStream(): Unit = {
      val isFlushingMark = new Array[Boolean](1)
      val minFlushedTS = minFlushedCursor.get(isFlushingMark)
      if (isFlushingMark(0)) return // already flushing

      // Chooses the highest seen txn time to start the merged stream so that all
      // txns after it are guaranteed to be observed by the merger.
      var highestSeenTxnTS = Timestamp.Min
      maxTxnTSByPartition.values forEach { txnTS =>
        highestSeenTxnTS = highestSeenTxnTS.max(txnTS)
      }

      if (
        !minFlushedCursor.compareAndSet(minFlushedTS, highestSeenTxnTS, false, true)
      ) return // lost a race to flush

      // Publish an idle event to mark the stream starting time.
      buffer.publish(TxnResult(highestSeenTxnTS, Clock.time, Vector.empty))

      // Unmark flushing and attempt to flush txns that became ready in the meantime.
      minFlushedCursor.set(highestSeenTxnTS, false)
      attemptToFlushMergedTxns()
    }

    private def attemptToFlushMergedTxns(): Unit = {
      val isFlushingMark = new Array[Boolean](1)
      val minFlushedTS = minFlushedCursor.get(isFlushingMark)
      if (isFlushingMark(0)) return // already flushing

      // Chooses the minimum txn time across all partitions so that we're sure that
      // all txns prior have been seen by the merger.
      var minReadyTS = Timestamp.MaxMicros
      maxTxnTSByPartition.values forEach { txnTS =>
        minReadyTS = minReadyTS.min(txnTS)
      }

      // If min doesn't move forward we're stuck waiting for a partition. Check if
      // the merge queue is still within acceptable size.
      if (minReadyTS <= minFlushedTS) {
        if (mergedTxnByTS.size > maxMergeQueueSize)
          close(SyncTimeout)
        return
      }

      // All partitions are moving forward. Let's attempt to flush merged txns.
      if (!minFlushedCursor.compareAndSet(minFlushedTS, minReadyTS, false, true))
        return // lost a race to flush

      val iter =
        mergedTxnByTS
          .headMap(minReadyTS, /* inclusive */ true)
          .values
          .iterator

      // If nothing to flush, produce an idle event if the idle period has expired
      // since last flush. Otherwise, flush merged txns.
      if (!iter.hasNext) {
        if (minReadyTS.difference(minFlushedTS) > idlePeriod) {
          buffer.publish(TxnResult(minReadyTS, Clock.time, Vector.empty))
        }
      } else {
        do {
          val txn = iter.next().result()
          // NB. Exclude txns that race with the merged stream start time.
          if (txn.txnTS > minFlushedTS) {
            buffer.publish(txn)
          }
          iter.remove() // Clean up the merge txn from the queue.
        } while (iter.hasNext)
      }

      minFlushedCursor.set(minReadyTS, false) // Unmark flushing
    }

    def onError(err: Throwable): Unit = {
      err match {
        case Cancelable.Canceled => () // ignore...
        case other => log.error("Closing stream merger due to upstream error", other)
      }
      close(err)
    }

    def onComplete(): Unit = close()
    def deallocate(): Unit = close()
    def touch(hint: Object): ReferenceCounted = this

    def close(err: Throwable = null): Unit =
      synchronized {
        if (!buffer.isClosed) {
          mergers.remove(key, this)
          buffer.close(err)
          cancenations foreach { _.cancel() }
        }
      }
  }
}
