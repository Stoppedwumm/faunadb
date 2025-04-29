package fauna.repo.service.stream

import fauna.exec.LoopThreadService
import fauna.lang._
import fauna.stats.StatsRecorder
import fauna.storage._
import fauna.storage.ops._
import fauna.tx.transaction._
import io.netty.util.Recycler
import java.util.concurrent.ConcurrentSkipListMap

object WriteStream {

  private sealed trait Stage
  private object Stage {
    case object Pending extends Stage
    case object Executed extends Stage
    case object Discarded extends Stage
  }

  private final class PendingTxn private (handle: Recycler.Handle[PendingTxn]) {
    @volatile var stage: Stage = _
    @volatile var ts: Timestamp = _
    @volatile var writes: Vector[Write] = _

    def recycle(): Unit = {
      stage = null
      ts = null
      writes = null
      handle.recycle(this)
    }
  }

  private object PendingTxn {

    private[this] val pool = new Recycler[PendingTxn] {
      def newObject(handle: Recycler.Handle[PendingTxn]) =
        new PendingTxn(handle)
    }

    def pooled(ts: Timestamp, writes: Vector[Write]): PendingTxn = {
      val pending = pool.get()
      pending.stage = Stage.Pending
      pending.ts = ts
      pending.writes = writes
      pending
    }
  }
}

/** A data stream used to get all writes from the transaction pipeline. */
final class WriteStream(
  callback: (Timestamp, Vector[Write]) => Unit,
  stats: StatsRecorder = StatsRecorder.Null)
    extends LoopThreadService("WriteStreamListener")
    with DataStream[Txn, StorageEngine.TxnResult] {
  import WriteStream._

  private val pendingTxns = new ConcurrentSkipListMap[Timestamp, PendingTxn]

  /** The write stream's view of the min applied timestamp based on pending
    * transactions: as long as there are pending txns in the list, we presume all
    * txns below the first one are executed. Returns `None` if no txn are pending, to
    * which case `TxnPipeline.minAppliedTimestamp` should be used.
    */
  def minAppliedTS =
    Option(pendingTxns.firstEntry)
      .map { _.getValue.ts.prevNano }

  def filter = None // subscribe to data-node level
  def onUncovered(txn: Transaction[Txn]) = ()

  def onSchedule(txn: Transaction[Txn]) = {
    val (_, writes) = txn.expr
    val pending = PendingTxn.pooled(txn.ts, writes)
    pendingTxns.put(txn.ts, pending)
  }

  def onResult(txn: Transaction[Txn], result: Option[StorageEngine.TxnResult]) = {
    val pending = pendingTxns.get(txn.ts)
    require(pending ne null, s"txn result at ${txn.ts} was not previously scheduled")
    pending.stage = result match {
      case Some(StorageEngine.TxnSuccess)                      => Stage.Executed
      case Some(_: StorageEngine.RowReadLockContention) | None => Stage.Discarded
    }
    synchronized { notify() }
  }

  protected def loop(): Unit = {
    val first = pendingTxns.firstEntry
    if ((first eq null) || first.getValue.stage == Stage.Pending) {
      synchronized { wait() }
    } else {
      stats.timing("Streams.Transactions.Pending", pendingTxns.size)
      val iter = pendingTxns.tailMap(Timestamp.Min).values.iterator

      while (iter.hasNext) {
        val pending = iter.next()
        pending.stage match {
          case Stage.Executed  => callback(pending.ts, pending.writes)
          case Stage.Discarded => () // ignore
          case Stage.Pending   => return
        }
        iter.remove()
        pending.recycle()
      }
    }
  }
}
