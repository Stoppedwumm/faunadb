package fauna.tx.log

import fauna.codex.cbor.CBOR
import fauna.exec.{ IdxWaitersMap, Timer }
import fauna.lang.AtomicFile
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.net.util.FutureSequence
import io.netty.buffer.ByteBufAllocator
import java.nio.channels.FileChannel
import java.nio.file._
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future, Promise }

trait LoggedState[I, V] extends ExceptionLogging {

  private[this] val logger = getLogger

  implicit protected def idxOrd: Ordering[I]
  implicit protected def ec: ExecutionContext

  def log: Log[I, V]

  def persistedIdx: I

  protected def applyEntry(e: LogEntry[I, V]): Unit

  protected lazy val currentIdxState = new IdxWaitersMap(persistedIdx)
  def currentIdx = currentIdxState.idx

  private val subscribeDone = Promise[Unit]()
  private val subscribeCount = new AtomicInteger(0)

  protected def subscribe() = {
    subscribeCount.incrementAndGet()
    log.subscribe(currentIdx, Duration.Inf) {
      case Log.Idle(prev)     => logIdle(prev)
      case Log.Entries(_, es) => logEntries(es)
      case Log.Reinit(idx)    => logReinit(idx)
      case Log.Closed         => logClosed() map { _ => false }
    } onComplete { rv =>
      if (subscribeCount.decrementAndGet() == 0) {
        subscribeDone.setDone()
      }
      logException(rv)
    }
  }

  def close(): Unit = {
    log.close()
    currentIdxState.shutdown(new LogClosedException)
    Await.result(subscribeDone.future, 10.seconds)
  }

  // core handlers

  @annotation.nowarn("cat=unused-params")
  protected def logIdle(prev: I): Future[Boolean] = FutureTrue

  protected def logEntries(entries: Iterator[LogEntry[I, V]]): Future[Boolean] = {
    val lastPersisted = persistedIdx
    val prev = currentIdx

    assert(idxOrd.gteq(prev, lastPersisted), s"persistedIdx $lastPersisted > currentIdx $prev")

    val lastIdx = applyEntries(prev, entries)
    currentIdxState.update(lastIdx)
    if (persistedIdx != lastPersisted) log.truncate(persistedIdx)
    FutureTrue
  }

  protected def applyEntries(prev: I, entries: Iterator[LogEntry[I, V]]): I = {
    var lastIdx = prev

    while (entries.hasNext) {
      val e = entries.next()
      if (idxOrd.gt(e.idx, prev)) {
        logger.trace(s"Applying log entry $e")
        applyEntry(e)
        lastIdx = e.idx
      } else {
        logger.trace(s"Ignoring log entry $e")
      }
    }

    lastIdx
  }

  protected def logReinit(idx: I): Future[Boolean] =
    throw new IllegalStateException("Missing log events! Underlying log was truncated.")

  protected def logClosed(): Future[Unit] = Future.unit

  // API

  /**
    * consistently sync underlying log (e.g. latest committed if it is
    * replicated), and then wait until the managed state is caught up
    * with the latest committed log entry.
    */
  def sync(within: Duration)(implicit ec: ExecutionContext) = {
    val deadline = getTimeBound(within)

    log.sync(within) flatMap {
      currentIdxState.get(_, deadline match {
        case Some(d) => d.timeLeft
        case None    => Duration.Inf
      })
    } map { handleSyncResult(_, within) }
  }

  /**
    * Wait until the managed state has applied up to a given log index.
    */
  def sync(idx: I, within: Duration)(implicit ec: ExecutionContext) =
    currentIdxState.get(idx, within) map { handleSyncResult(_, within) }

  /**
    * Wait until the managed state has applied up to the latest currently
    * committed index.
    */
  protected def syncCommitted() =
    sync(log.lastIdx, Duration.Inf)

  /**
    * Executes `cb` as log events occur, level-triggered. If no events
    * have occurred since the last execution, delays for `duration`
    * before re-checking. `cb` will continue until it returns false.
    * Consider using `SequencedStateCallback` as a wrapper around a
    * callback when it is passed to multiple state subscriptions and it
    * is not known whether it is thread safe.
    */
  def subscribe(cb: => Future[Boolean], duration: Duration = 5.seconds): Future[Unit] = {
    def subscribe0(idx: I): Future[Unit] = {
      log.poll(idx, duration) flatMap {
        case false =>
          if (log.isClosed) {
            Future.unit
          } else {
            subscribe0(idx)
          }
        case true  =>
          val cur = log.lastIdx
          // if the log poll() fired, _eventually_ the state will see
          // the change...
          currentIdxState.get(cur, Duration.Inf) flatMap {
            case true =>
              GuardFuture(cb) flatMap {
                case true  => subscribe0(cur)
                case false => Future.unit
              }

            // should never happen with Duration.Inf, but just in case...
            case false => subscribe0(idx)
          }
      }
    }

    syncCommitted() flatMap { _ =>
      cb flatMap {
        case true  => subscribe0(currentIdx)
        case false => Future.unit
      }
    } recover {
      case _: LogClosedException => ()
    }
  }

  // helpers

  private def handleSyncResult(success: Boolean, within: Duration): Unit =
    if (!success) throw new TimeoutException(s"Timed out waiting for state sync after $within")

  private def getTimeBound(d: Duration) =
    d match {
      case d: FiniteDuration => Some(d.bound)
      case _                 => None
    }
}

object SequencedStateCallback {
  def apply(cb: => Future[Boolean])(implicit ec: ExecutionContext): SequencedStateCallback =
    apply(cb, FutureSequence())

  def apply(cb: => Future[Boolean], fs: FutureSequence)(implicit ec: ExecutionContext): SequencedStateCallback =
    new SequencedStateCallback(cb, fs)
}

/**
  * Ensures that callback invocations are sequenced across subscriptions when
  * a callback is passed to multiple state subscriptions. Also ensures that if
  * the callback returns false to one subscription, it will end all of them
  * and it won't be invoked again from another one.
  */
class SequencedStateCallback(cb: => Future[Boolean], private[this] val fs: FutureSequence)(implicit ec: ExecutionContext) {
  @volatile private[this] var continue = true

  def apply(): Future[Boolean] =
    fs { invokeSequenced() }

  private def invokeSequenced() =
    if (continue) {
      cb flatMap { b =>
        if (b) {
          FutureTrue
        } else {
          continue = false
          FutureFalse
        }
      }
    } else {
      FutureFalse
    }
}

object SnapshottedState {
  def open[I: Ordering : CBOR.Codec, V, S: CBOR.Codec](
    log: Log[I, V],
    dir: Path,
    name: String,
    initIdx: I,
    initState: S,
    snapshotInterval: Int = 100,
    snapshotBeforeUpdate: Boolean = false)
    (f: (I, S, V) => S)
    (implicit ec: ExecutionContext) = {

    val file = AtomicFile(dir / name + ".state")

    file.create { c =>
      CBOR.encode((initIdx, initState)) releaseAfter { _.readAllBytes(c) }
    }

    new SnapshottedState(log, file, snapshotInterval, snapshotBeforeUpdate, f)
  }

  private[SnapshottedState] def InitialSyncLogThreshold = 10.seconds
}

class SnapshottedState[I: CBOR.Codec, V, S: CBOR.Codec](
  val log: Log[I, V],
  file: AtomicFile,
  snapshotInterval: Int,
  snapshotBeforeUpdate: Boolean,
  f: (I, S, V) => S)(
  implicit protected val idxOrd: Ordering[I],
  protected val ec: ExecutionContext)
    extends LoggedState[I, V] {

  implicit private[this] val snapCodec = implicitly[CBOR.Codec[(I, S)]]

  private[this] var snapshotCounter = 0
  @volatile private[this] var _state = file.read(decodeState)
  @volatile private[this] var _persistedIdx = _state._1
  @volatile private[this] var _observableState = _state._2

  subscribe()

  // To avoid linearization violations of observable state on restart,
  // only complete the constructor of this instance once its state
  // caught up to the last committed index of the log. The entries up
  // to log.lastIdx must be available locally, so this is expected to
  // have no difficulties completing.
  {
    val synced = syncCommitted()
    Timer.Global.scheduleTimeout(SnapshottedState.InitialSyncLogThreshold) {
    if (!synced.isCompleted) {
        getLogger.warn(s"Initial sync-to-committed for ${file.path} is taking longer than ${SnapshottedState.InitialSyncLogThreshold}.")
        synced foreach { _ =>
          getLogger.info(s"Initial sync-to-committed for ${file.path} completed.")
        }
      }
    }
    Await.result(synced, Duration.Inf)
  }

  def persistedIdx = _persistedIdx

  def get = _observableState

  override protected def applyEntries(prev: I, entries: Iterator[LogEntry[I, V]]) = {
    val lastIdx = super.applyEntries(prev, entries)

    if (snapshotBeforeUpdate) {
      save()
    }
    exposeState()

    lastIdx
  }

  protected def applyEntry(entry: LogEntry[I, V]) = {
    val next = if (entry.nonEmpty) {
      f(entry.idx, _state._2, entry.get)
    } else {
      _state._2
    }

    _state = (entry.idx, next)
    snapshotCounter += 1

    if (snapshotCounter % snapshotInterval == 0) {
      save()
    }
  }

  private def exposeState() =
    _observableState = _state._2

  override protected def logClosed() = {
    save()
    Future.unit
  }

  def save() = synchronized {
    val snap = _state
    val buf = CBOR.encode(snap)

    file.write { c =>
      buf releaseAfter { _ =>
        while (buf.isReadable) buf.readBytes(c, buf.readableBytes)
      }
    }

    _persistedIdx = snap._1
  }

  protected final def decodeState(c: FileChannel): (I, S) = {
    if (c.size > Int.MaxValue) {
      throw new IllegalStateException(s"State file size is greater than ${Int.MaxValue} bytes.")
    }

    ByteBufAllocator.DEFAULT.buffer releaseAfter { buf =>
      buf.writeAllBytes(c)
      CBOR.decode[(I, S)](buf)
    }
  }

  protected final def setPersistedState(f: Path, idx: I): Unit = synchronized {
    file.moveFrom(f)
    _persistedIdx = idx
  }

  protected final def setCurrentState(state: (I, S)): Unit = synchronized {
    _state = state
    currentIdxState.update(_state._1)
    exposeState()
  }
}
