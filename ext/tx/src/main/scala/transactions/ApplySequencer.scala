package fauna.tx.transaction

import fauna.codex.cbor.CBOR
import fauna.exec.{ IdxWaitersMap, ImmediateExecutionContext, Timer }
import fauna.lang.{ TimeBound, Timestamp }
import fauna.lang.syntax._
import io.netty.buffer.ByteBuf
import java.util.concurrent.{
  ConcurrentHashMap,
  ConcurrentSkipListMap,
  ThreadLocalRandom,
  TimeoutException
}
import scala.concurrent.{ Future, Promise }

/**
  * trait used by TransactionStorage to expose applied timestamp management.
  */
trait AppliedTimestampProxy[K] {

  private[transaction] val applySequencer = new ApplySequencer[K]

  /**
    * Moves forward max locally applied timestamp bound.
    */
  def updateMaxAppliedTimestamp(ts: Timestamp) =
    applySequencer.updateMaxAppliedTimestamp(ts)

  /**
    * Returns the minimum locally applied timestamp. Equal to
    * sequencer.maxAppliedTimestamp if no writes are pending.
    */
  def appliedTimestamp = applySequencer.appliedTimestamp

  /**
    * Returns the applied timestamp for `key`. Equal to
    * sequencer.maxAppliedTimestamp if no write to `key` is pending.
    */
  def appliedTimestamp(key: K) =
    applySequencer.appliedTimestamp(key)

  /** Wait for minimum applied timestamp to reach `ts`. */
  def waitForAppliedTimestamp(ts: Timestamp, within: TimeBound) =
    applySequencer.waitForAppliedTimestamp(ts, within)

  /** Wait for applied timestamp for `key` to reach `ts`. */
  def waitForAppliedTimestamp(key: K, ts: Timestamp, within: TimeBound) =
    applySequencer.waitForAppliedTimestamp(key, ts, within)
}

/**
  * Transaction application dependency sequencer.
  *
  * For a given batch of transactions, the ScopeBroker first wires up
  * transaction reads and application in timestamp order with the methods `join`
  * and `sequence`.
  *
  * Once all transactions in a batch have been registered, the ScopeBroker then
  * calls `updateMaxAppliedTimestamp` with the highest timestamp associated with
  * the batch, which unblocks readers to probe for a global applied timestamp or
  * a key specific one within the batch timestamp range.
  *
  * `updateMaxAppliedTimestamp` is also called in cases where transaction
  * application needs to be fast-forwarded, such as on node init.
  */
final class ApplySequencer[K] {

  import ApplySequencer._

  private[this] val log = getLogger

  private[this] val maxApplied = new IdxWaitersMap[Timestamp](Timestamp.Epoch)
  private[this] val pendingKeys = new ConcurrentHashMap[K, WriteChain]
  private[this] val pendingTimestamps = new ConcurrentSkipListMap[Timestamp, Promise[Unit]]

  override def toString = s"ApplySequencer($pendingKeys)"

  def keysSize = pendingKeys.size

  // ScopeBroker interface (transaction sequencing)

  /**
    * Returns the max possible locally applied timestamp.
    */
  def maxAppliedTimestamp: Timestamp = maxApplied.idx

  /**
    * Returns a future which is completed when all previously sequenced
    * transactions affecting `key` have completed.
    */
  def join(key: K): Future[Unit] =
    pendingKeys.get(key) match {
      case null   => Future.unit
      case writes => writes.completion.future
    }

  /**
    * Sets the max possible locally applied timestamp. This is safe to call by
    * the ScopeBroker AFTER sequencing all transactions in a batch with
    * `sequence()`
    */
  def updateMaxAppliedTimestamp(ts: Timestamp) = {
    log.debug(s"Updating Max Applied Timestamp to $ts")
    maxApplied.update(ts)
  }

  /**
    * Registers `keys` for a pending write, along with a global timestamp
    * promise and completes them all when `applyF` completes. Apply completion
    * is concurrent/threadsafe, and applies may complete in arbitrary real-time
    * order.
    */
  def sequence[T](ts: Timestamp, keys: Iterable[K], stats: TxnPipeline.Stats)(
    applyF: => Future[T]): Future[T] = {
    implicit val ec = ImmediateExecutionContext
    val p = Promise[Unit]()
    pendingTimestamps.put(ts, p)

    keys foreach { k =>
      pendingKeys.compute(
        k,
        (_, prev) => {
          val wc = new WriteChain(ts, Promise[Unit](), prev)
          if (
            ThreadLocalRandom
              .current()
              .nextDouble() <= ApplySequencer.TransactionChainSampleRate
          ) {
            stats.recordSequencedTransactionLength(wc.length)
          }
          wc
        }
      )
    }

    applyF andThen { _ =>
      pendingTimestamps.remove(ts)
      pendingTimestamps.lowerEntry(ts) match {
        case null  => p.success(())
        case entry => p.completeWith(entry.getValue.future)
      }

      keys foreach { k =>
        pendingKeys.computeIfPresent(k, (_, writes) => writes.resolved(ts))
      }
    }
  }

  // Applied timestamp read interface. Proxied for public consumption via
  // AppliedTimestampProxy. (See its documentation)

  def appliedTimestamp: Timestamp = {
    val maxApplied = maxAppliedTimestamp
    Option(pendingTimestamps.firstEntry).fold(maxApplied) { _.getKey.prevNano }
  }

  def appliedTimestamp(key: K): Timestamp = {
    val max = maxAppliedTimestamp
    pendingKeys.get(key) match {
      case null   => max
      case writes => writes.appliedTimestamp(max)
    }
  }

  def waitForAppliedTimestamp(ts: Timestamp, within: TimeBound): Future[Unit] = {
    implicit val ec = ImmediateExecutionContext
    def exception =
      new TimeoutException(s"Timed out waiting for appliedTimestamp to reach $ts")

    maxApplied.get(ts, within.timeLeft) flatMap { maxAppliedMet =>
      if (!maxAppliedMet) {
        throw exception
      }

      pendingTimestamps.floorEntry(ts) match {
        case null  => Future.unit
        case entry =>
          Timer.Global.timeoutFuture(entry.getValue.future, within.timeLeft, exception)
      }
    }
  }

  def waitForAppliedTimestamp(key: K, ts: Timestamp, within: TimeBound): Future[Unit] = {
    implicit val ec = ImmediateExecutionContext
    def exception = {
      val str = key match {
        case b: ByteBuf => CBOR.showBuffer(b)
        case _          => key
      }

      new TimeoutException(s"Timed out waiting for appliedTimestamp for key $str to reach $ts")
    }

    maxApplied.get(ts, within.timeLeft) flatMap { maxAppliedMet =>
      if (!maxAppliedMet) {
        throw exception
      }

      pendingKeys.get(key) match {
        case null   => Future.unit
        case writes =>
          Timer.Global.timeoutFuture(writes.waitFor(ts), within.timeLeft, exception)
      }
    }
  }
}

object ApplySequencer {

  val TransactionChainSampleRate = 0.01

  final class WriteChain(
    val ts: Timestamp,
    val completion: Promise[Unit],
    val prior: WriteChain) {

    val length: Int = if (prior == null) {
      1
    } else {
      1 + prior.length
    }

    /**
      * resolves the write at ts0. returns either a WriteChain or null, if all
      * writes <= ts0 are completed. This also ensure the associated entry in
      * the `pendingKeys` map is cleared.
      */
    def resolved(ts0: Timestamp): WriteChain =
      if (ts == ts0) {
        prior match {
          case null  => completion.trySuccess(())
          case prior => completion.completeWith(prior.completion.future)
        }

        prior
      } else {
        prior match {
          case null =>
            throw new IllegalStateException("Received completion for unregistered transaction!")

          case prior =>
            val prior0 = prior.resolved(ts0)
            if (prior0 eq prior) this else new WriteChain(ts, completion, prior0)
        }
      }

    @annotation.tailrec
    def appliedTimestamp(max: Timestamp): Timestamp = {
      val max0 = if (completion.isCompleted) max else ts.prevNano
      if (prior ne null) prior.appliedTimestamp(max0) else max0
    }

    @annotation.tailrec
    def waitFor(ts0: Timestamp): Future[Unit] =
      if (ts0 >= ts) {
        completion.future
      } else {
        if (prior ne null) prior.waitFor(ts0) else Future.unit
      }

    override def toString = s"($ts, $completion, $prior)"
  }
}
