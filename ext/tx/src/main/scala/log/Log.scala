package fauna.tx.log

import fauna.codex.cbor.CBOR
import io.netty.buffer.ByteBuf
import java.nio.file.Path
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

trait LogEntry[+I, +V] {
  def idx: I
  def toOption: Option[V]

  def isEmpty: Boolean
  def get: V

  def nonEmpty = !isEmpty
}

object LogEntry {
  private final case class BasicEntry[+I, +V](idx: I, get: V)
      extends LogEntry[I, V] {
    def isEmpty = false
    def toOption = Some(get)
  }

  def apply[I, V](idx: I, value: V): LogEntry[I, V] = BasicEntry(idx, value)

  def unapply[I, V](e: LogEntry[I, V]): Option[(I, Option[V])] =
    Some((e.idx, e.toOption))
}

object Log {
  type Sink[I, E] = SinkState[I, E] => Future[Boolean]

  val DefaultSubscribeIdle = 30.seconds

  sealed trait SinkState[+I, +E]
  final case class Entries[I, E](prevIdx: I, entries: Iterator[E])
      extends SinkState[I, E]
  final case class Idle[I](prevIdx: I) extends SinkState[I, Nothing]
  final case class Reinit[I](prevIdx: I) extends SinkState[I, Nothing]
  final case object Closed extends SinkState[Nothing, Nothing]

  def open[V: CBOR.Codec](
    logDir: Path,
    txnLogBackupPath: Option[Path],
    namePrefix: String = "BinaryLog",
    fileSize: Int = BinaryLogStore.DefaultFileSize) = {

    new BasicLog(
      BinaryLogStore.open[V](logDir, txnLogBackupPath, namePrefix, fileSize))
  }
}

trait LogLike[I, +V] {

  type E

  /**
    * The index immediately preceding the first log entry.
    */
  def prevIdx: I

  /**
    * the index of the last transaction in the log. In an empty log,
    * prevIdx == lastIdx.
    */
  def lastIdx: I

  /**
    * Returns a Seq of all entries within the log beginning with
    * afterIdx and ending with the log's lastIdx. Subsequent adds,
    * truncates, or discards will not be reflected in the returned
    * Seq.
    */
  def entries(afterIdx: I): EntriesIterator[E]

  /**
    * Listen for new log entries from from an index.
    * @returns true if one or more entries exist after `afterIdx`, false otherwise
    */
  def poll(afterIdx: I, within: Duration): Future[Boolean]

  /**
    * Subscribe to future updates to a log. The sink function should
    * return a Future[Boolean] to signal if it wants to continue
    * consuming the log: `true` indicates continue, `false` to stop.
    */
  def subscribe(afterIdx: I, idle: Duration = Log.DefaultSubscribeIdle)(f: Log.Sink[I, E])(implicit ec: ExecutionContext): Future[Unit]
}

trait Log[I, V] extends LogLike[I, V] {

  type E <: LogEntry[I, V]

  /**
    * Add an entry to the log. The TX of the entry is returned.
    */
  def add(t: V, within: Duration): Future[I]

  /**
    * Synchronizes this log. The last synced index is returned.
    */
  def sync(within: Duration): Future[I]

  /**
    * Opportunistically truncates entries up to and including toIdx
    * from the log. This does not guarantee that the entries will be
    * deleted.
    */
  // FIXME: Exclusively owned by some primary state machine, or should
  // be refactored into some position tracking on logs themselves.
  def truncate(toIdx: I): Unit

  /**
    * Close the log and free any vm resources.
    */
  def close(): Unit

  def isClosed: Boolean
}

/**
  * The interface for persistent storage for log data.
  *
  * THREAD SAFETY: add(), discard(), and reinit() should only be
  * accessed from the writer thread. All other methods may be accessed
  * from any thread.
  */
trait LogStore[I, V] extends LogLike[I, V] {

  type E <: LogEntry[I, V]

  // Writes

  /**
    * Index of the last uncommitted entry in the store.
    */
  def uncommittedLastIdx: I

  /**
    * Index of the last uncommitted but durably saved entry in the store.
    */
  def flushedLastIdx: I

  /**
    * Returns an iterator that includes entries after `after` and up
    * to `uncommittedLastIdx`.
    */
  def uncommittedEntries(after: I): EntriesIterator[E]

  /**
    * Adds entries. Returns the index of the last added entry. Ensures
    * all pending writes are durably saved.
    */
  def add(ts: Iterable[V]): I

  /**
    * Ensures all pending writes are durably saved. Returns the index
    * of the last flushed entry.
    */
  def flush(): I

  /**
    * Sets the store's committed index. updates `lastIdx` to `idx` and
    * publishes newly committed entries to subscribers. If `idx` is
    * less than the current `lastIdx`, this does nothing.
    *
    * Passing value for `idx` greater than `flushedLastIdx` is an
    * error.
    */
  def updateCommittedIdx(idx: I, meta: Option[ByteBuf] = None): Unit

  /**
    * Opportunistically truncates entries up to and including toIdx
    * from the log. This does not guarantee that the entries will be
    * deleted.
    */
  def truncate(toIdx: I, meta: Option[ByteBuf] = None): Unit

  /**
    * Discards all entries after afterIdx from the tail of the log.
    * After calling discard(), lastIdx will equal afterIdx. Calling
    * discard with an afterIdx < prevIdx throws an
    * IllegalStateException.
    */
  def discard(afterIdx: I): Unit

  /**
    * If idx is greater than last idx in log (committed or
    * uncommitted), resets the log to an empty log with a prevIdx =
    * idx.
    */
  def reinit(idx: I, meta: Option[ByteBuf] = None): Unit

  // metadata

  /**
    * Return associated metadata, saved. The caller is responsible for
    * releasing the buffer, if necessary.
    */
  def metadata: Option[ByteBuf]

  /**
    * Set log metadata
    */
  def metadata(bytes: ByteBuf): Unit

  /**
    * Close the log and free any vm resources.
    */
  def close(): Unit

  def isClosed: Boolean
}
