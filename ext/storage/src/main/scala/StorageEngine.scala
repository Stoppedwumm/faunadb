package fauna.storage

import fauna.atoms._
import fauna.codex.cbor._
import fauna.codex.cbor.CBOR.showBuffer
import fauna.exec.{
  ImmediateExecutionContext,
  NamedThreadPoolExecutionContext
}
import fauna.lang._
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.net.Heartbeat
import fauna.net.bus._
import fauna.scheduler._
import fauna.stats.StatsRecorder
import fauna.storage.ops._
import fauna.trace.{ Aborted, GlobalTracer }
import fauna.tx.transaction.{
  KeyLocator,
  Partitioner,
  StorageLockedException,
  TransactionStorage
}
import io.netty.buffer.{ ByteBuf, ByteBufAllocator, Unpooled }
import io.netty.util.AsciiString
import java.io.File
import java.nio.file.Path
import java.util.{ Arrays, UUID }
import java.util.concurrent.{ ConcurrentHashMap, ThreadLocalRandom }
import java.util.concurrent.atomic.{ AtomicInteger, AtomicReference }
import org.apache.cassandra.io.sstable._
import scala.concurrent.{ blocking, ExecutionContext, Future }
import scala.concurrent.duration._
import scala.util.Try

object StorageEngine {

  // The frequency at which read operation deadlines are checked for
  // expiry. Expressed as a percentage from [0.0, 1.0].
  val DeadlineCheckPercentage = 0.1

  /**
    * The number of attempts to apply a mutation to a key (at a
    * transaction time) before giving up. See `applyWrites()` and
    * `recordFailure()`.
    */
  val ApplyFailureThreshold = 10

  /**
    * The number of apply failures acceptable since the last time
    * storage was unlocked or the process was restarted.
    */
  val TotalApplyFailureThreshold = 20

  /**
    * Attribute for traced spans which contains the number of writes
    * per applied transaction.
    */
  val AttrWritesSize = AsciiString.cached("storage.writes.size")

  /**
    * The type of the results of applying a transaction in this
    * storage engine.
    */
  sealed trait TxnResult

  /**
    * Indicates that all mutations within a transaction have been
    * successfully applied to storage, and LAT may be moved forward to
    * publish the transaction's effects.
    */
  case object TxnSuccess extends TxnResult

  /**
    * At least one mutation within a transaction depends upon the
    * value of another key - a dependent read - whose value has
    * changed since this transaction was added to the log. The
    * transaction's effects have _not_ been applied to storage, and
    * the coordinator must retry the transaction. LAT may be moved
    * forward, because the transaction had no effect.
    */
  case class RowReadLockContention(rowKey: Array[Byte], newTS: Timestamp)
      extends TxnResult {
    override def toString =
      s"RowReadLockContention(${showBuffer(Unpooled.wrappedBuffer(rowKey))}, $newTS)"

    override def equals(obj: scala.Any) =
      obj match {
        case RowReadLockContention(rk, ts) =>
          (newTS == ts) && Arrays.equals(rowKey, rk)
        case _ => false
      }

    override def hashCode() =
      Arrays.hashCode(rowKey) ^ newTS.hashCode()
  }

  object TxnResult {
    implicit val CBORCodec = CBOR.SumCodec[TxnResult](
      CBOR.SingletonCodec(TxnSuccess),
      CBOR.TupleCodec[RowReadLockContention])
  }

  /**
    * A slice is a request for a sequence of subsections - "slices" -
    * of a single row within a column family. The slices are defined
    * by Cell names.
    */
  final case class Slice(
    cf: String,
    ranges: Vector[(ByteBuf, ByteBuf)],
    order: Order,
    count: Int) {
    override def toString = {
      val rangeStrs = ranges map { r =>
        r._1.toHexString -> r._2.toHexString
      }
      s"Slice($cf, $rangeStrs, $order, $count)"
    }
  }

  final case class Config(
    self: HostID,
    rootPath: Path,
    locator: KeyLocator[ByteBuf, TxnRead],
    partitioner: () => Partitioner[TxnRead, Txn],
    concurrentReads: Int,
    concurrentTxnReads: Int,
    readScheduler: IOScheduler,
    syncPeriod: FiniteDuration,
    stats: StorageEngine.Stats,
    syncOnShutdown: Boolean,
    dualWriteIndexCFs: Boolean,
    openFileReserve: Double,
    unleveledSSTableLimit: Int,
    latWaitThreshold: FiniteDuration) {

    require(syncPeriod.toMillis.toInt > 0,
            "syncPeriod as millis must be between 0 and Int.MaxValue")
  }

  case class Stats(stats: StatsRecorder) {

    def recordReadTime(millis: Long): Unit =
      stats.timing("Storage.Read.Time", millis)

    def recordRangeReadTime(millis: Long): Unit =
      stats.timing("Storage.Range.Read.Time", millis)

    def recordWriteTime(millis: Long): Unit =
      stats.timing("Storage.Write.Time", millis)
    def recordCellsAdded(cells: Int) = stats.count("Storage.Cells.Added", cells)
    def recordCellsDeleted(cells: Int) = stats.count("Storage.Cells.Deleted", cells)
    def recordRowsDeleted(rows: Int) = stats.count("Storage.Rows.Deleted", rows)
    def recordBytesRead(bytes: Int) = stats.count("Storage.Bytes.Read", bytes)
    def recordCellsRead(cells: Int) = stats.count("Storage.Cells.Read", cells)

    def recordDeletedCellsRead(cells: Int) =
      stats.count("Storage.Deleted.Cells.Read", cells)

    def recordUnbornCellsRead(cells: Int) =
      stats.count("Storage.Unborn.Cells.Read", cells)

    def setSSTablesPendingForCF(cf: String, count: Int) =
      stats.set(s"Storage.SSTables.Pending.$cf", count)
    def setSSTablesPending(count: Int) = stats.set("Storage.SSTables.Pending", count)

    def setTombstoneRatioForCF(cf: String, ratio: Double): Unit =
      stats.set(s"Storage.Tombstone.Ratio.$cf", ratio)

    def setTombstonePurgePauses(n: Int) = stats.set("Storage.Tombstone.Pauses", n)

    // The time taken to flush memtables for a single CF to storage.
    def flushTime(millis: Long): Unit = stats.timing("Storage.Flush.Time", millis)

    // The time taken to wait for LAT to exceed some value, or timeout.
    def waitForLATGlobal(f: => Future[Unit]): Future[Unit] =
      stats.timeFuture("Storage.Wait.Time")(f)

    def waitForLATKey(f: => Future[Unit]): Future[Unit] =
      stats.timeFuture("Storage.Wait.Key.Time")(f)

    def applyFailure(): Unit =
      stats.incr("Storage.Apply.Failure")

    def applyFailureThresholdReached(): Unit =
      stats.incr("Storage.Apply.Failure.Threshold.Reached")

    def recordPersistedTimestamp(ts: Timestamp): Unit =
      stats.set("Storage.PersistedTimestamp", ts.millis)

    def recordLockStatus(lock: Lock): Unit =
      stats.set("Storage.Locked", lock.isLocked.compare(false))
  }

  sealed abstract class Lock {
    def isUnlocked: Boolean = !isLocked

    def isLocked: Boolean =
      this match {
        case Locked   => true
        case Unlocked => false
      }
  }
  case object Locked extends Lock
  case object Unlocked extends Lock

  implicit val lockCodec =
    CBOR.SumCodec[Lock](CBOR.SingletonCodec(Locked), CBOR.SingletonCodec(Unlocked))
}

trait StorageEngine
    extends TransactionStorage[ByteBuf, TxnRead, Timestamp, Txn, StorageEngine.TxnResult]
    with ExceptionLogging {

  val config: StorageEngine.Config

  final protected val logger: Logger = getLogger

  import StorageEngine._

  final protected val stats = config.stats

  private val readEC =
    NamedThreadPoolExecutionContext(
      "Storage.Read",
      config.concurrentReads,
      reporter = new FailureReporter {
        def report(thread: Option[Thread], ex: Throwable): Unit =
          ex match {
            case _: TooBusyException =>
              () // Silence rejected work items from QoS.
            case _ => FailureReporter.Default.report(thread, ex)
          }
      })

  private val txnReadEC =
    NamedThreadPoolExecutionContext("Storage.TxnRead", config.concurrentTxnReads)

  // FIXME: make this configurable
  private val writeEC = NamedThreadPoolExecutionContext("Storage.Write", 16)

  // FIXME: handle non-zero scopes
  private[this] val metadata = AtomicFile(
    config.rootPath / "scopes" / "0" / "metadata")

  metadata.create { c =>
    CBOR.encode(Timestamp.Epoch) releaseAfter { _.readAllBytes(c) }
    CBOR.encode(Unlocked) releaseAfter { _.readAllBytes(c) }
  }

  final protected val (persisted, lockState) = {
    val (ts, lock) = metadata.read { c =>
      ByteBufAllocator.DEFAULT.buffer releaseAfter { buf =>
        val data = buf.writeAllBytes(c)
        val ts = CBOR.decode[Timestamp](data)
        val lock = Try(CBOR.decode[Lock](data)) getOrElse Unlocked
        (ts, lock)
      }
    }

    stats.recordPersistedTimestamp(ts)
    stats.recordLockStatus(lock)
    updateMaxAppliedTimestamp(ts)
    (new AtomicReference(ts), new AtomicReference(lock))
  }

  /**
    * Maintains the number of observed failures applying mutations to
    * storage since the process has been up. When this value exceeds
    * TotalApplyFailureThreshold, the following occurs:
    *
    * - all applies stop
    * - storage is locked
    * - data node is closed
    * - log truncation stops
    *
    *  An operator must repair/recover storage before releasing the
    *  lock.
    */
  private[this] val totalFailures = new AtomicInteger(0)

  /**
    * Maintains the number of observed failures applying a mutation
    * for a key/transaction-time tuple. If the number of failures
    * exceeds ApplyFailureThreshold, the failed mutation(s) will be
    * presumed to be impossible to apply, recorded for posterity, and
    * skipped. See `applyWrites()`.
    */
  private[this] val applyFailures = new ConcurrentHashMap[(ByteBuf, Timestamp), Int]

  private val heartbeat = Heartbeat(config.syncPeriod, "Storage") {
    flushMemtables(appliedTimestamp, config.syncPeriod)
  }

  @volatile private[this] var _closed = false
  final def isClosed = _closed

  final def close() = synchronized {
    if (!isClosed) {
      if (config.syncOnShutdown) {
        sync()
      }
      _closed = true
      heartbeat.stop()
      writeEC.stop()
      readEC.stop()
      txnReadEC.stop()
    }
  }

  final def unlock(): Unit = {
    // NB. this is not a perfect dual of lock(), because heartbeats do
    // not resume. A restart is required after unlocking storage.
    lockState.set(Unlocked)
    updateMetadata(persisted.get, Unlocked)
  }

  final def lock(): Unit = {
    heartbeat.stop()
    updateMetadata(persisted.get, Locked)
    lockState.set(Locked)
  }

  final def isUnlocked: Boolean =
    lockState.get.isUnlocked

  /**
    * Force storage to persist state to durable media.
    *
    * When `UnsafeSyncMode` is provided, this will result in a
    * persistedTimestamp greater than the current appliedTimestamp. Upon
    * restarting the node, any pending transactions with a timestamp less than
    * or equal to the UnsafeSyncMode's timestamp will be skipped.
    *
    * `cfs` names the column families whose memtables should be synced. As a special
    * case, if the set is empty, all CFs will be synced.
    */
  final def sync(
    syncMode: TransactionStorage.SyncMode,
    cfs: Set[String] = Set.empty,
    updatePersistedTimestamp: Boolean = true): Unit =
    blocking {
      val ts = syncMode match {
        case TransactionStorage.UnsafeSyncMode(ts) => ts
        case TransactionStorage.NormalSyncMode     => appliedTimestamp
      }
      flushMemtables(cfs, ts, updatePersistedTimestamp, Duration.Zero)
    }

  // Sync all column families. The persistedTimestamp is updated to appliedTimestamp.
  final def sync(syncMode: TransactionStorage.SyncMode): Unit =
    sync(syncMode, Set.empty)

  /**
    * Returns the average compression ratio of SSTables in a given
    * column family.
    */
  def compressionRatio(cf: String): Double

  /**
    * Returns the name of every active column family in storage.
    */
  def columnFamilyNames: Set[String]

  final def persistedTimestamp =
    persisted.get

  // AppliedTimestampProxy overrides to inject stats

  override def waitForAppliedTimestamp(ts: Timestamp, within: TimeBound) =
    GlobalTracer.instance.withSpan("storage.lat_global_wait") {
      stats.waitForLATGlobal {
        super.waitForAppliedTimestamp(ts, within)
      }
    }

  override def waitForAppliedTimestamp(key: ByteBuf, ts: Timestamp, within: TimeBound) =
    GlobalTracer.instance.withSpan("storage.lat_key_wait") {
      val timing = Timing.start
      stats.waitForLATKey {
        implicit val ec = ImmediateExecutionContext
        super.waitForAppliedTimestamp(key, ts, within) map { _ =>
          val elapsed = timing.elapsedMillis

          if (elapsed.millis > config.latWaitThreshold) {
            logger.warn(
              s"Wait for per-key LAT exceeded threshold: " +
                s"waited $elapsed msecs. for ${CBOR.showBuffer(key)}")
          }
        }
      }
    }

  final def readRow(
    snapTime: Timestamp,
    key: ByteBuf,
    slices: Vector[Slice],
    priority: PriorityGroup,
    deadline: TimeBound,
    knownRowTS: Option[Timestamp] = None
  ): Future[Row] = {
    implicit val ec = readEC

    waitForAppliedTimestamp(key, snapTime, deadline) flatMap { _ =>
      config.readScheduler(priority, deadline) {
        Future {
          val tracer = GlobalTracer.instance

          tracer.withSpan("storage.read") {
            val rowTS =
              knownRowTS getOrElse {
                rowTimestamp(snapTime, key, deadline)
              }

            val cfs = slices map {
              case StorageEngine.Slice(cfname, ranges, order, count) =>
                (cfname -> rowCFSlice(
                  snapTime,
                  key,
                  cfname,
                  ranges,
                  order,
                  count,
                  deadline))
            }

            Row(cfs, rowTS)
          }
        }
      }
    }
  }

  /** Sequentially scans rows within the provided slice, returning up to
    * `count` cells.
    *
    * If the slice contains a starting cell (née column), the scan
    * will start from that cell in the first row of the slice.
    */
  final def fullScan(
    snapTime: Timestamp,
    slice: ScanSlice,
    selector: Selector,
    count: Int,
    priority: PriorityGroup,
    deadline: TimeBound): Future[(Vector[ScanRow], Option[(ByteBuf, ByteBuf)])] = {
    implicit val ec = readEC

    waitForAppliedTimestamp(snapTime, deadline) flatMap { _ =>
      config.readScheduler(priority, deadline) {
        Future {
          val tracer = GlobalTracer.instance

          tracer.withSpan("storage.scan") {
            scanSlice(snapTime, slice, selector, count, Int.MaxValue, deadline)
          }
        }
      }
    }
  }

  /** Sequentially scans rows within the provided slice, returning up to
    * `count` cells, each of which will be the first cell of its
    * respective row.
    *
    * NOTE: The cursor returned by this method will represent a cell
    * in the final row; resuming a sparseScan from this cursor will
    * re-read a cell from that row. Callers should adjust the cell
    * name to represent the largest cell name within the CF's schema
    * before resuming the scan. Unfortunately, the storage engine
    * doesn't have enough information to do this on a caller's behalf.
    */
  final def sparseScan(
    snapTime: Timestamp,
    slice: ScanSlice,
    selector: Selector,
    count: Int,
    priority: PriorityGroup,
    deadline: TimeBound): Future[(Vector[ScanRow], Option[(ByteBuf, ByteBuf)])] = {
    implicit val ec = readEC

    waitForAppliedTimestamp(snapTime, deadline) flatMap { _ =>
      config.readScheduler(priority, deadline) {
        Future {
          val tracer = GlobalTracer.instance

          tracer.withSpan("storage.scan") {
            scanSlice(snapTime, slice, selector, count, 1, deadline)
          }
        }
      }
    }
  }

  final def readsForTxn(expr: Txn) = expr._1.keys.toSeq

  final def evalTxnRead(snapTime: Timestamp, key: TxnRead, deadline: TimeBound) = {
    implicit val ec = txnReadEC
    Future(rowTimestamp(snapTime, key.rowKey, deadline))
  }

  final def evalTxnApply(
    ts: Timestamp,
    readResults: Map[TxnRead, Timestamp],
    expr: Txn) = {
    val (reads, writes) = expr

    val timestampBoundary = ts - Tables.RowTimestamps.TTL

    /**
      * Iterate over all dependent reads and take the "max" of all read lock contentions if any exist. This
      * ensures all nodes will return the same lock contention regardless of the order of the iterator.
      */
    val rv = reads.iterator.foldLeft(Option.empty[(TxnRead, Timestamp)]) {
      case (maxContended, (k, lockTS)) =>
        val currContended = readResults.get(k) match {
          case Some(rowTS) =>
            // Comparison used is LTE iso EQ, this allows reads to be OCC-ed against the snapshotTime at
            // which the query was evaluated. This allows omitting the rowTimestamp read during query evaluation.
            // The (first part of the) expression can thus be read as
            // """the last modification to the row happened before the snapshotTime, aka no more changes happened
            //    between the snapshotTime and the txn commit time."""
            //
            // RowTimestamp eviction is not guaranteed to be deterministic, therefore we must make
            // the comparison here a bit more lenient. Specifically rowTS may be larger than lockTS
            // on the condition that lockTS equals Epoch (aka was already evicted) and rowTS is
            // older than RowTimestamps.TTL.
            // This is guarantees strict serializability as long as
            // `transactionTime - snapshotTime < RowTimestamps.TTL`. (If violated there's a fallback to snapshot
            // isolation.)
            // Related to this: LogNodes will expire transaction if they've held them in memory for 30 seconds.
            // Additionaly Query execution should not take that long, although there is (currently)
            // no hard/realtime guarantee there.
            if ((rowTS <= lockTS) ||
              (lockTS < timestampBoundary && rowTS < timestampBoundary)) {
              None
            } else {
              Some(k -> rowTS)
            }

          // FIXME: None read should not be possible.
          case None => Some(k -> Timestamp.Min)
        }

        (maxContended, currContended) match {
          case (None, None)    => None
          case (None, Some(_)) => currContended
          case (Some(_), None) => maxContended
          case (Some((maxBuf, _)), Some((currBuf, _))) =>
            if ((maxBuf compareTo currBuf) > 0) {
              maxContended
            } else {
              currContended
            }
        }
    }

    // only write out locally covered mutations.
    val covered = writes filter { w =>
      val loc = config.locator.locate(w.rowKey)
      config
        .partitioner()
        .isReplicaForLocation(loc, config.self, true)
    }

    val tracer = GlobalTracer.instance

    tracer.withSpan("storage.apply") {
      rv match {
        case Some((key, ts)) =>
          tracer.activeSpan foreach { _.setStatus(Aborted("OCC check failed")) }
          Future.successful(RowReadLockContention(key.rowKey.toByteArray, ts))

        case None =>
          tracer.activeSpan foreach {
            _.addAttribute(AttrWritesSize, covered.size)
          }

          if (covered.nonEmpty) {
            applyWrites(ts, covered)
          } else {
            Future.successful(TxnSuccess)
          }
      }
    }
  }

  def hash(
    cf: String,
    segment: Segment,
    snapTime: Timestamp,
    deadline: TimeBound,
    maxDepth: Int,
    filterKey: ByteBuf => Boolean = _ => true)(
    implicit ec: ExecutionContext): Future[HashTree]

  /**
    * Yields a file transfer for the provided ring segments as of at
    * least `snapTime`, if any applicable data exists.
    */
  def prepareTransfer(
    cf: String,
    segments: Vector[Segment],
    snapTime: Timestamp,
    ctx: FileTransferContext,
    bus: MessageBus,
    deadline: TimeBound,
    threads: Int = 1)(
    implicit ec: ExecutionContext): Future[Option[FileTransferManifest]]

  /**
    * Consumes a file transfer by loading sstables into the provided
    * column family.
    */
  def receiveTransfer(session: UUID, cf: String, tmpdir: Path, deadline: TimeBound)(
    implicit ec: ExecutionContext): Future[Unit]

  /**
    * Provided a set of open sstables and the segments needed
    * transfer, returns the appropriate set of transfers.
    *
    * Stream plan computation may optionally be parallelized using the
    * provided number of threads.
    */
  def streamPlan(
    sstables: Seq[SSTableReader],
    segments: Seq[Segment],
    threads: Int = 1): Seq[CompressedTransfer]

  /**
    * Finds all valid sstables within a directory matching the
    * provided column family name, returning opened SSTableReaders in
    * batch mode.
    */
  def bulkLoad(cf: String, root: File, threads: Int = 1): Seq[SSTableReader]

  /** Returns true if any data exists in the keyspace outside of the
    * given segments.
    */
  def needsCleanup(keep: Seq[Segment]): Boolean =
    columnFamilyNames exists { needsCleanup(_, keep) }

  /** Returns true if any data exists in a column family outside of the
    * given segments.
    */
  def needsCleanup(cf: String, keep: Seq[Segment]): Boolean

  /**
    *  Cleanup permanently removes all data _NOT_ within the given
    *  segments from a column family.
    *
    * The column family must be currently active in the schema.
    *
    * Returns true if the entire cleanup was successful, false otherwise.
    */
  def cleanup(cf: String, keep: Seq[Segment]): Boolean

  final protected def maybeCheckDeadline(deadline: TimeBound): Unit =
    if (ThreadLocalRandom.current.nextDouble() < DeadlineCheckPercentage) {
      deadline.checkOrThrow()
    }

  // max row timestamp returned is provided `snapTime`
  final protected def rowTimestamp(
    snapTime: Timestamp,
    key: ByteBuf,
    deadline: TimeBound) = {
    val rng = Seq((Tables.RowTimestamps.encode(snapTime), Unpooled.EMPTY_BUFFER))
    val cell = rowCFSlice(snapTime,
                          key,
                          Tables.RowTimestamps.CFName,
                          rng,
                          Order.Descending,
                          1,
                          deadline).headOption

    cell.fold(Timestamp.Epoch) { c =>
      Tables.RowTimestamps.decode(c.name)
    }
  }

  protected[storage] def rowCFSlice(
    snapTime: Timestamp,
    key: ByteBuf,
    cfname: String,
    ranges: Seq[(ByteBuf, ByteBuf)],
    order: Order,
    count: Int,
    deadline: TimeBound): Vector[Cell]

  protected def scanSlice(
    snapTime: Timestamp,
    slice: ScanSlice,
    selector: Selector,
    count: Int,
    cellsPerRow: Int,
    deadline: TimeBound): (Vector[ScanRow], Option[(ByteBuf, ByteBuf)])

  // FIXME: should take scope or prio context in order to inform
  // writeEC, but we need to have better separation of db txn
  // pipelines for this to be meaningful.
  private def applyWrites(txnTime: Timestamp, writes: Seq[Write]) = {
    // can't apply transactions until repair and recovery are
    // complete; reject writes and wait for an operator.
    if (lockState.get.isLocked) {
      throw new StorageLockedException()
    }

    implicit val ec = writeEC
    val timing = Timing.start

    Future {
      val tsCellName = Tables.RowTimestamps.encode(txnTime)

      var cellsAdded = 0
      var cellsDeleted = 0
      var rowsDeleted = 0

      writes foreach { write =>
        val rk = write.rowKey
        try {
          applyMutationForKey(rk) { mut =>
            mut.add(Tables.RowTimestamps.CFName,
                    tsCellName,
                    Unpooled.EMPTY_BUFFER,
                    txnTime,
                    Tables.RowTimestamps.StorageEngineTTLSeconds)
            write.mutateAt(mut, txnTime)
          }

          cellsAdded += write.addOps
          cellsDeleted += write.removeOps
          rowsDeleted += write.clearRowOps
        } catch {
          case e: Throwable =>
            val total = totalFailures.incrementAndGet()
            val failures = applyFailures.merge((rk, txnTime), 0, (old, _) => old + 1)

            if (failures == ApplyFailureThreshold ||
                total > TotalApplyFailureThreshold) {

              /**
                * !!!!!! ALARM ALARM ALARM !!!!!!
                *
                * AN UNRECOVERABLE APPLY FAILURE HAS
                * OCCURRED. REPAIR IS REQUIRED.
                */
              stats.applyFailureThresholdReached()
              recordFailure(rk, txnTime, write, writes)

              // stop flushing, lock storage, inform the caller that
              // the data node must be stopped
              if (total > TotalApplyFailureThreshold) {
                lock()
                throw new StorageLockedException()
              }
            } else {
              stats.applyFailure()
              throw e
            }
        }

        applyFailures.remove((rk, txnTime))
      }

      stats.recordWriteTime(timing.elapsedMillis)
      stats.recordCellsAdded(cellsAdded)
      stats.recordCellsDeleted(cellsDeleted)
      stats.recordRowsDeleted(rowsDeleted)
      TxnSuccess
    }
  }

  final protected def updateMetadata(ts: Timestamp, lock: Lock): Unit = {
    metadata.write { c =>
      CBOR.encode(ts) releaseAfter { _.readAllBytes(c) }
      CBOR.encode(lock) releaseAfter { _.readAllBytes(c) }
    }
    stats.recordPersistedTimestamp(ts)
    stats.recordLockStatus(lock)
  }

  // Flush the memtables for column families in `cfs`. As a special case, an empty
  // set means flush all CFs.
  protected def flushMemtables(
    cfs: Set[String],
    appliedTimestamp: Timestamp,
    updatePersistedTimestamp: Boolean,
    totalTime: FiniteDuration): Unit

  // Flush all memtables and update the persisted timestamp.
  protected def flushMemtables(
    appliedTimestamp: Timestamp,
    totalTime: FiniteDuration
  ): Unit = flushMemtables(Set.empty, appliedTimestamp, true, totalTime)

  def applyMutationForKey(key: ByteBuf)(f: ops.Mutation => Unit): Unit

  /**
    * Records a failed mutation and the set of mutations within the
    * same transaction to a file for later inspection. This method may
    * not throw a non-fatal exception.
    *
    * File format is determined by the concrete implementation of this method.
    */
  protected[storage] def recordFailure(
    key: ByteBuf,
    txnTime: Timestamp,
    failure: Write,
    writes: Seq[Write]): Unit
}
