package fauna.repo.query

import fauna.atoms.{ AccountID, CollectionID, SchemaVersion, ScopeID }
import fauna.lang.{ TimeBound, Timestamp }
import fauna.lang.clocks.SystemDeadlineClock
import fauna.lang.syntax._
import fauna.repo.{
  TransactionKeyTooLargeException,
  TxnTooLargeException,
  TxnTooManyComputeOpsException,
  VersionTooLargeException
}
import fauna.stats.{ QueryMetrics, StatTags, StatsRecorder }
import fauna.storage.ops.{ NoopWrite, Write }
import fauna.storage.Tables
import fql.error.{ Diagnostic, Hint }
import io.netty.buffer.ByteBuf
import org.apache.cassandra.db.Keyspace
import scala.annotation.tailrec
import scala.collection.immutable.SeqMap
import scala.collection.mutable.{ ArrayBuffer, SeqMap => MSeqMap }
import scala.concurrent.duration._

final case class ReadRecord(
  cf: String,
  rowKey: ByteBuf,
  ts: Timestamp,
  doConcurrencyCheck: Boolean)

object State {
  val computeOpsLimit = {
    // FIXME: find a way push down this limit without adding fields
    //            to state or metrics.
    val mapped = sys.props.get("fauna.compute-ops-limit") map { _.toInt }
    val defaulted = mapped getOrElse 12000
    // We want to use the pre-normalized compute ops for limiting
    (defaulted * QueryMetrics.BaselineCompute).toInt
  }

  /** A container for compute and IO metrics captured during
    * evaluation of a Query.
    *
    * Metrics are not valid until/unless the Query either commits
    * successfully or fails due to contention. These two conditions
    * represent billable use - timeouts and internal errors do not.
    *
    * Contention will rollup metrics from each commit attempt.
    */
  final case class Metrics(
    val compute: Int = 0,
    private val documents: Int = 0,
    private val sets: Int = 0,
    private val creates: Int = 0,
    private val updates: Int = 0,
    private val deletes: Int = 0,
    private val inserts: Int = 0,
    private val removes: Int = 0,
    bytesRead: Int = 0,
    private val readOps: Int = 0,
    private val serialReadOps: Int = 0,
    private val cpuWallNanos: Long = 0,
    private val readWallNanos: Long = 0,
    private val lastClock: Long = SystemDeadlineClock.nanos,
    private val isReading: Boolean = false,
    private val databaseCreates: Int = 0,
    private val databaseUpdates: Int = 0,
    private val databaseDeletes: Int = 0,
    private val collectionCreates: Int = 0,
    private val collectionUpdates: Int = 0,
    private val collectionDeletes: Int = 0,
    private val roleCreates: Int = 0,
    private val roleUpdates: Int = 0,
    private val roleDeletes: Int = 0,
    private val keyCreates: Int = 0,
    private val keyUpdates: Int = 0,
    private val keyDeletes: Int = 0,
    private val functionCreates: Int = 0,
    private val functionUpdates: Int = 0,
    private val functionDeletes: Int = 0
  ) {

    // NOTE: the current (Sept. 2020) pricing model simply counts one
    // read op per document or set. The next pricing model will count
    // read ops using the byte size of each document or set read. To
    // accomodate both during the transition period, this class must
    // accumulate documents/sets and readOps. `QueryMetrics` will do
    // the needful with that input using whichever pricing model is in
    // play.

    private[query] def addCompute(n: Int): Metrics =
      if (compute + n > computeOpsLimit) {
        throw TxnTooManyComputeOpsException(compute + n, computeOpsLimit)
      } else {
        copy(compute = compute + n)
      }

    private[query] def incrDocuments(ops: Int): Metrics =
      copy(documents = documents + 1, readOps = readOps + ops)

    private[query] def addSets(n: Int, ops: Int): Metrics =
      copy(sets = sets + n, readOps = readOps + ops)

    private[query] def incrCreates(): Metrics =
      copy(creates = creates + 1)

    private[query] def incrUpdates(): Metrics =
      copy(updates = updates + 1)

    private[query] def incrDeletes(): Metrics =
      copy(deletes = deletes + 1)

    private[query] def incrInserts(): Metrics =
      copy(inserts = inserts + 1)

    private[query] def incrRemoves(): Metrics =
      copy(removes = removes + 1)

    private[query] def incrDatabaseCreates(): Metrics =
      copy(databaseCreates = databaseCreates + 1)

    private[query] def incrDatabaseUpdates(): Metrics =
      copy(databaseUpdates = databaseUpdates + 1)

    private[query] def incrDatabaseDeletes(): Metrics =
      copy(databaseDeletes = databaseDeletes + 1)

    private[query] def incrCollectionCreates(): Metrics =
      copy(collectionCreates = collectionCreates + 1)

    private[query] def incrCollectionUpdates(): Metrics =
      copy(collectionUpdates = collectionUpdates + 1)

    private[query] def incrCollectionDeletes(): Metrics =
      copy(collectionDeletes = collectionDeletes + 1)

    private[query] def incrRoleCreates(): Metrics =
      copy(roleCreates = roleCreates + 1)

    private[query] def incrRoleUpdates(): Metrics =
      copy(roleUpdates = roleUpdates + 1)

    private[query] def incrRoleDeletes(): Metrics =
      copy(roleDeletes = roleDeletes + 1)

    private[query] def incrKeyCreates(): Metrics =
      copy(keyCreates = keyCreates + 1)

    private[query] def incrKeyUpdates(): Metrics =
      copy(keyUpdates = keyUpdates + 1)

    private[query] def incrKeyDeletes(): Metrics =
      copy(keyDeletes = keyDeletes + 1)

    private[query] def incrFunctionCreates(): Metrics =
      copy(functionCreates = functionCreates + 1)

    private[query] def incrFunctionUpdates(): Metrics =
      copy(functionUpdates = functionUpdates + 1)

    private[query] def incrFunctionDeletes(): Metrics =
      copy(functionDeletes = functionDeletes + 1)

    // Be aware that `bytesRead` is taken as the read cache size by `commitReads`.
    private[query] def addBytesRead(n: Int): Metrics =
      copy(bytesRead = bytesRead + n)

    def merge(other: Metrics): Metrics = {
      if (compute + other.compute > computeOpsLimit) {
        throw TxnTooManyComputeOpsException(compute + other.compute, computeOpsLimit)
      }

      val residual = SystemDeadlineClock.nanos - other.lastClock

      Metrics(
        compute + other.compute,
        documents + other.documents,
        sets + other.sets,
        creates + other.creates,
        updates + other.updates,
        deletes + other.deletes,
        inserts + other.inserts,
        removes + other.removes,
        bytesRead + other.bytesRead,
        readOps + other.readOps,
        serialReadOps max other.serialReadOps,
        cpuWallNanos + other.cpuWallNanos + residual,
        readWallNanos + other.readWallNanos,
        lastClock,
        databaseCreates = databaseCreates + other.databaseCreates,
        databaseUpdates = databaseUpdates + other.databaseUpdates,
        databaseDeletes = databaseDeletes + other.databaseDeletes,
        collectionCreates = collectionCreates + other.collectionCreates,
        collectionUpdates = collectionUpdates + other.collectionUpdates,
        collectionDeletes = collectionDeletes + other.collectionDeletes,
        roleCreates = roleCreates + other.roleCreates,
        roleUpdates = roleUpdates + other.roleUpdates,
        roleDeletes = roleDeletes + other.roleDeletes,
        keyCreates = keyCreates + other.keyCreates,
        keyUpdates = keyUpdates + other.keyUpdates,
        keyDeletes = keyDeletes + other.keyDeletes,
        functionCreates = functionCreates + other.functionCreates,
        functionUpdates = functionUpdates + other.functionUpdates,
        functionDeletes = functionDeletes + other.functionDeletes
      )
    }

    def +(other: Metrics): Metrics =
      merge(other)

    def -(other: Metrics): Metrics =
      Metrics(
        compute - other.compute,
        documents - other.documents,
        sets - other.sets,
        creates - other.creates,
        updates - other.updates,
        deletes - other.deletes,
        inserts - other.inserts,
        removes - other.removes,
        bytesRead - other.bytesRead,
        readOps - other.readOps,
        // TODO: not convinced this makes sense. But it's only called for index
        // builds,
        //            so does it even matter?
        serialReadOps min other.serialReadOps,
        cpuWallNanos - other.cpuWallNanos,
        readWallNanos - other.readWallNanos,
        // FIXME: this makes _no_ sense, but see above.
        lastClock,
        databaseCreates = databaseCreates - other.databaseCreates,
        databaseUpdates = databaseUpdates - other.databaseUpdates,
        databaseDeletes = databaseDeletes - other.databaseDeletes,
        collectionCreates = collectionCreates - other.collectionCreates,
        collectionUpdates = collectionUpdates - other.collectionUpdates,
        collectionDeletes = collectionDeletes - other.collectionDeletes,
        roleCreates = roleCreates - other.roleCreates,
        roleUpdates = roleUpdates - other.roleUpdates,
        roleDeletes = roleDeletes - other.roleDeletes,
        keyCreates = keyCreates - other.keyCreates,
        keyUpdates = keyUpdates - other.keyUpdates,
        keyDeletes = keyDeletes - other.keyDeletes,
        functionCreates = functionCreates - other.functionCreates,
        functionUpdates = functionUpdates - other.functionUpdates,
        functionDeletes = functionDeletes - other.functionDeletes
      )

    /** Commit metrics accrued during successful transaction
      * processing.
      */
    def commitAll(stats: StatsRecorder, accountID: AccountID): Unit = {
      commitReads(stats, accountID)
      stats.count(QueryMetrics.WriteCreate, creates)
      stats.count(QueryMetrics.WriteUpdate, updates)
      stats.count(QueryMetrics.WriteDelete, deletes)
      stats.count(QueryMetrics.WriteInsert, inserts)
      stats.count(QueryMetrics.WriteRemove, removes)
      stats.count(QueryMetrics.SerialReadOps, serialReadOps)
      stats.count(QueryMetrics.ReadTime, readWallNanos.nanos.toMillis.toInt)
      stats.count(QueryMetrics.ComputeTime, cpuWallNanos.nanos.toMillis.toInt)
      stats.count(QueryMetrics.DatabaseCreate, databaseCreates)
      stats.count(QueryMetrics.DatabaseUpdate, databaseUpdates)
      stats.count(QueryMetrics.DatabaseDelete, databaseDeletes)
      stats.count(QueryMetrics.CollectionCreate, collectionCreates)
      stats.count(QueryMetrics.CollectionUpdate, collectionUpdates)
      stats.count(QueryMetrics.CollectionDelete, collectionDeletes)
      stats.count(QueryMetrics.RoleCreate, roleCreates)
      stats.count(QueryMetrics.RoleUpdate, roleUpdates)
      stats.count(QueryMetrics.RoleDelete, roleDeletes)
      stats.count(QueryMetrics.KeyCreate, keyCreates)
      stats.count(QueryMetrics.KeyUpdate, keyUpdates)
      stats.count(QueryMetrics.KeyDelete, keyDeletes)
      stats.count(QueryMetrics.FunctionCreate, functionCreates)
      stats.count(QueryMetrics.FunctionUpdate, functionUpdates)
      stats.count(QueryMetrics.FunctionDelete, functionDeletes)
    }

    /** Commit metrics accrued prior to a user-initiated abort.
      */
    def commitReads(stats: StatsRecorder, accountID: AccountID): Unit = {
      val tags = if (accountID != AccountID.Root) {
        StatTags(Set("account_id" -> accountID.toLong.toString))
      } else {
        StatTags.Empty
      }

      stats.count(QueryMetrics.Compute, compute, tags)
      stats.count(QueryMetrics.ReadDocument, documents)
      stats.count(QueryMetrics.ReadSet, sets)
      stats.count(QueryMetrics.ByteReadOps, readOps)
      stats.count(QueryMetrics.BytesRead, bytesRead)
      // NB. The `bytesRead` fileds ends up containing the per query read cache size.
      // See `QueryEvalContext.eval` and `State.recordCacheMiss`.
      stats.timing("Query.Read.Cache.Size", bytesRead)
    }

    def startRead(): Metrics = {
      val curr = SystemDeadlineClock.nanos
      val diff = curr - lastClock
      copy(cpuWallNanos = cpuWallNanos + diff, serialReadOps = serialReadOps + 1)
    }

    def stopRead(): Metrics = {
      val curr = SystemDeadlineClock.nanos
      val diff = curr - lastClock
      copy(readWallNanos = readWallNanos + diff)
    }
  }

  val logger = getLogger
}

object ReadsWrites {
  val empty = ReadsWrites(Vector.empty, SeqMap.empty, SeqMap.empty, Set.empty)
}

final case class ReadsWrites(
  reads: Vector[ReadRecord],
  mergeableWrites: SeqMap[ByteBuf, SeqMap[Write.MergeKey, Write]],
  nonMergableWrites: SeqMap[ByteBuf, Vector[Write]],
  unlimitedKeys: Set[ByteBuf]) {

  def hasReads: Boolean = reads.nonEmpty
  def hasWrites: Boolean = mergeableWrites.nonEmpty || nonMergableWrites.nonEmpty
  def isPure: Boolean = !hasReads && !hasWrites

  def writesSize: Int =
    mergeableWrites.foldLeft(0) { case (sum, (_, v)) => sum + v.size } +
      nonMergableWrites.foldLeft(0) { case (sum, (_, v)) => sum + v.size }

  // NB. Ordering here is relevant. See State.collectWrites(..) usage.
  def allWrites: Iterator[Write] =
    mergeableWrites.valuesIterator
      .flatMap { _.valuesIterator }
      .concat(
        nonMergableWrites.valuesIterator.flatten
      )

  // NB. Ordering here is relevant. See State.collectWrites(..) usage.
  def writesForRowKey(rowKey: ByteBuf): Iterator[Write] =
    mergeableWrites
      .getOrElse(rowKey, SeqMap.empty)
      .valuesIterator
      .concat(
        nonMergableWrites.getOrElse(rowKey, Vector.empty)
      )

  def merge(other: ReadsWrites, omitWrites: Boolean) = {
    def merge0[K, V](a: SeqMap[K, V], b: SeqMap[K, V])(combine: (V, V) => V) =
      if (a.isEmpty) b
      else if (b.isEmpty) a
      else {
        a ++ b.map { case (k, v) =>
          k -> a.get(k).fold(v) { combine(_, v) }
        }
      }

    if (omitWrites) {
      copy(reads = reads ++ other.reads)
    } else {
      ReadsWrites(
        reads ++ other.reads,
        merge0(mergeableWrites, other.mergeableWrites) { merge0(_, _)(Write.merge) },
        merge0(nonMergableWrites, other.nonMergableWrites) { _ ++ _ },
        unlimitedKeys ++ other.unlimitedKeys
      )
    }
  }

  def recordRead(rr: ReadRecord) =
    copy(reads = reads :+ rr)

  def addWrite(w: Write, limited: Boolean = true): (ReadsWrites, Int) = {
    w.mergeKey match {
      case Some(mergeKey) =>
        // `adjust` represents the effective delta generated by adding this write op.
        // In the case of a merge that means removing the original op from the
        // txn and adding the new merged write, otherwise it's the new op.
        var adjust = 0
        // forgive me, for what you are about to read is gross.
        val updated = mergeableWrites.updatedWith(w.rowKey) {
          case Some(origWrites) =>
            Some(origWrites.updatedWith(mergeKey) { opt =>
              val merged = opt map { orig =>
                val mw = Write.merge(orig, w)
                adjust = mw.numBytes - orig.numBytes
                mw
              }
              merged orElse {
                adjust = w.numBytes
                Some(w)
              }
            })
          case None =>
            adjust = w.numBytes
            Some(SeqMap(mergeKey -> w))
        }
        (
          copy(
            mergeableWrites = updated,
            unlimitedKeys = unlimitedKeys ++ Option.when(!limited)(w.rowKey)),
          adjust)
      case None =>
        val updated = nonMergableWrites.updatedWith(w.rowKey) {
          case Some(writes) => Some(writes :+ w)
          case None         => Some(Vector(w))
        }
        (
          copy(
            nonMergableWrites = updated,
            unlimitedKeys = unlimitedKeys ++ Option.when(!limited)(w.rowKey)),
          w.numBytes)
    }
  }

  /** Rewinds the set of writes for the given row key to the writes passed.
    *
    * NOTE: The set of writes given are presumed to have originated from the
    * `ReadsWrites` class. No attempts to ensure their consistency will be made by
    * the `rewind` method (like merging writes).
    */
  private[repo] def rewind(rowKey: ByteBuf, writes: Iterable[Write]): ReadsWrites = {
    val (mergeables, nonMergeables) =
      writes.view.partition { _.mergeKey.isDefined } match {
        case (mergeables, nonMergeables) =>
          val mergeables0 = mergeables.map { w => w.mergeKey.get -> w }.to(SeqMap)
          val nonMergeables0 = nonMergeables.toVector
          (mergeables0, nonMergeables0)
      }
    copy(
      mergeableWrites = mergeableWrites.updated(rowKey, mergeables),
      nonMergableWrites = nonMergableWrites.updated(rowKey, nonMergeables)
    )
  }

  /** Reset the writes in this state iff there has been no other writes when comparing
    * to the old `ReadsWrites` state. Fails otherwise.
    */
  private[repo] def resetWritesOrFail(
    old: ReadsWrites,
    next: ReadsWrites): ReadsWrites =
    if (
      mergeableWrites == old.mergeableWrites &&
      nonMergableWrites == old.nonMergableWrites
    ) {
      copy(
        mergeableWrites = next.mergeableWrites,
        nonMergableWrites = next.nonMergableWrites
      )
    } else {
      throw new IllegalStateException(
        "Can not reset writes after additional writes.")
    }

  def invalidatesReadsOf(other: ReadsWrites): Boolean = {
    if (other.reads.isEmpty) {
      false
    } else {
      if (!hasWrites) {
        false
      } else {
        other.reads exists { r =>
          mergeableWrites.contains(r.rowKey) || nonMergableWrites
            .contains(r.rowKey)
        }
      }
    }
  }
}

object CollectionWrites {
  val empty = CollectionWrites(Map.empty)
}

final case class CollectionWrites(inner: Map[CollectionID, CollectionWrite])
    extends AnyVal {
  def get(id: CollectionID): Option[CollectionWrite] = inner.get(id)
  def update(id: CollectionID, write: CollectionWrite): CollectionWrites =
    copy(inner = inner.updated(id, write))

  def merge(other: CollectionWrites) = CollectionWrites(inner ++ other.inner)

  def conflictsWith(other: CollectionWrites) = inner.exists { case (id, write) =>
    other.inner.get(id) match {
      case Some(otherWrite) => write != otherWrite
      case None             => false
    }
  }
}

sealed trait CollectionWrite {}

object CollectionWrite {
  // Was this recorded write to a doc within the collection?
  object ToDocument extends CollectionWrite
  // Was this recorded write to the collection definition document?
  object ToCollection extends CollectionWrite
}

final case class State(
  parent: Option[State],
  enabledConcurrencyChecks: Boolean,
  readsWrites: ReadsWrites,
  collectionWrites: CollectionWrites,
  cacheMisses: Int,
  stats: StatsRecorder,
  deadline: TimeBound,
  txnSizeLimitBytes: Int,
  txnSizeBytes: Int,
  readOnlyTxn: Boolean,
  flushLocalKeyspace: Set[Keyspace],
  serialReads: Int,
  unlimited: Boolean,
  metrics: State.Metrics = new State.Metrics,
  queryDiagnostics: Seq[Diagnostic] = Seq.empty,

  /** We store these separate from the other diagnostics because we want to de-dupe the perf hints that are identical
    * while we don't want to dedupe log/dbg diagnostics.
    * If the exact same hint is emitted for the same span, we don't need to display that multiple times.
    */
  performanceHints: Set[Hint] = Set.empty
) {

  def merge(other: State): State = merge(other, false)

  def merge(other: State, omitWrites: Boolean): State = {
    val fks = {
      // `Set.concat` does not recognize (and optimize) some often occurring
      // conditions relating to State.flushLocalKeyspace, so we special-case
      // it ourselves.
      val lteq = flushLocalKeyspace.sizeCompare(other.flushLocalKeyspace) <= 0
      if (lteq && flushLocalKeyspace.subsetOf(other.flushLocalKeyspace)) {
        other.flushLocalKeyspace
      } else if (!lteq && other.flushLocalKeyspace.subsetOf(flushLocalKeyspace)) {
        flushLocalKeyspace
      } else {
        flushLocalKeyspace ++ other.flushLocalKeyspace
      }
    }

    copy(
      enabledConcurrencyChecks =
        enabledConcurrencyChecks || other.enabledConcurrencyChecks,
      readsWrites = readsWrites.merge(other.readsWrites, omitWrites),
      collectionWrites = collectionWrites.merge(other.collectionWrites),
      cacheMisses = cacheMisses + other.cacheMisses,
      txnSizeBytes = mergeLimits(txnSizeBytes, other.txnSizeBytes, omitWrites),
      readOnlyTxn = readOnlyTxn || (!omitWrites && other.readOnlyTxn),
      flushLocalKeyspace = fks,
      serialReads = serialReads max other.serialReads,
      unlimited = unlimited || other.unlimited,
      metrics = metrics + other.metrics,
      queryDiagnostics = (queryDiagnostics, other.queryDiagnostics) match {
        case (qd1, Nil) => qd1
        case (Nil, qd2) => qd2
        case (qd1, qd2) => qd1 ++ qd2
      },
      performanceHints = if (other.performanceHints.isEmpty) {
        performanceHints
      } else if (performanceHints.isEmpty) {
        other.performanceHints
      } else {
        performanceHints ++ other.performanceHints
      }
    )
  }

  private[this] def mergeLimits(self: Int, other: Int, omitWrites: Boolean): Int = {
    val totalBytes = if (omitWrites) {
      self
    } else {
      self + other
    }
    if (totalBytes > txnSizeLimitBytes) {
      throw TxnTooLargeException(totalBytes, txnSizeLimitBytes, "bytes")
    }
    totalBytes
  }

  def commit(omitWrites: Boolean = false): State = parent match {
    case Some(p) => p.merge(this, omitWrites)
    case None    => this
  }

  def checkpoint =
    State(
      Some(this),
      enabledConcurrencyChecks,
      ReadsWrites.empty,
      CollectionWrites.empty,
      0,
      stats,
      deadline,
      txnSizeLimitBytes,
      0,
      true,
      flushLocalKeyspace,
      0,
      unlimited
    )

  /** Returns an iterator of merged writes in order of occurrence in the current state
    * branch -- mergeables first, then non-mergeable writes.
    */
  def allPending(): Iterator[Write] =
    collectWrites { _.allWrites }

  /** Returns an iterator of merged writes for the given row in order or occurrence
    * in the current state branch -- mergeables first, then non-mergeable writes.
    */
  def writesForRowKey(rowKey: ByteBuf): Iterator[Write] =
    collectWrites { _.writesForRowKey(rowKey) }

  private def collectWrites(fn: ReadsWrites => Iterator[Write]): Iterator[Write] = {
    @tailrec def writes(parent: Option[State], stack: List[State]): Iterator[Write] =
      parent match {
        case Some(state) => writes(state.parent, state :: stack)
        case None        => stack.iterator flatMap { state => fn(state.readsWrites) }
      }

    val iter = writes(parent, this :: Nil)
    val mergeables = MSeqMap.empty[Write.MergeKey, Write]
    val nonMergeables = ArrayBuffer.empty[Write]

    iter foreach { write =>
      write.mergeKey match {
        case None => nonMergeables += write
        case Some(mergeKey) =>
          mergeables.updateWith(mergeKey) {
            case None       => Some(write)
            case Some(prev) => Some(Write.merge(prev, write))
          }
      }
    }

    mergeables.valuesIterator.concat(nonMergeables)
  }

  def cacheMissesRolledUp = {
    @annotation.tailrec
    def rollupCacheMisses(s: State, sum: Int): Int = {
      s.parent match {
        case Some(p) => rollupCacheMisses(p, sum + s.cacheMisses)
        case None    => sum + s.cacheMisses
      }
    }

    rollupCacheMisses(this, 0)
  }

  def recordCacheMiss(bytesRead: Int) =
    copy(cacheMisses = cacheMisses + 1, metrics = metrics.addBytesRead(bytesRead))

  def recordRead(
    cf: String,
    rk: ByteBuf,
    ts: Timestamp,
    occCheckEnabled: Boolean = enabledConcurrencyChecks): State = {
    // ignoring cbor overhead, byte array + two longs
    val keyBytes = rk.readableBytes + 16
    val totalBytes = txnSizeBytes + keyBytes
    if (totalBytes > txnSizeLimitBytes && !readOnlyTxn) {
      throw TxnTooLargeException(totalBytes, txnSizeLimitBytes, "bytes")
    }
    copy(readsWrites =
      readsWrites.recordRead(ReadRecord(cf, rk, ts, occCheckEnabled)))
  }

  def addWrite(w: Write) = {
    // NB. This predicate is only meant to discard index rows when either their
    // row key or cell names are larger than C* limits. This usually happens on
    // background index builds. It should not discard versions in any
    // circumstance.
    if (w.canWrite) {
      val (added, adjust) = readsWrites.addWrite(w, limited = !unlimited)
      val totalBytes = txnSizeBytes + adjust
      w match {
        case Write.KeyTooLarge(bytes) => throw TransactionKeyTooLargeException(bytes)
        case Write.VersionTooLarge(bytes) =>
          throw VersionTooLargeException(bytes)
        case _ => ()
      }
      if (totalBytes > txnSizeLimitBytes) {
        throw TxnTooLargeException(totalBytes, txnSizeLimitBytes, "bytes")
      }
      copy(readsWrites = added, txnSizeBytes = totalBytes, readOnlyTxn = false)
    } else {
      stats.incr("Query.Write.Dropped")
      State.logger.debug(s"Dropping write $w")
      this
    }
  }

  def addNoopWrite(): State =
    if (!readsWrites.hasWrites) {
      addWrite(NoopWrite)
    } else {
      this
    }

  def addSchemaVersionOccCheck(scope: ScopeID, ver: Option[SchemaVersion]): State = {
    val rk = Tables.SchemaVersions.rowKey(scope)
    val ts = ver.getOrElse(SchemaVersion.Min).ts
    recordRead(Tables.SchemaVersions.CFName, rk, ts)
  }

  private[repo] def rewind(rowKey: ByteBuf, writes: Iterable[Write]): State =
    copy(readsWrites = readsWrites.rewind(rowKey, writes))

  private[repo] def resetWritesOrFail(prev: State, next: State): State =
    copy(readsWrites =
      readsWrites.resetWritesOrFail(prev.readsWrites, next.readsWrites))

  @tailrec
  def findCollectionWrite(id: CollectionID): Option[CollectionWrite] = {
    collectionWrites.get(id) match {
      case Some(w) => Some(w)
      case None =>
        parent match {
          case Some(p) => p.findCollectionWrite(id)
          case None    => None
        }
    }
  }

  def invalidatesReadsOf(other: State) =
    readsWrites.invalidatesReadsOf(other.readsWrites) ||
      collectionWrites.conflictsWith(other.collectionWrites)

  def addLocalKeyspaceFlush(keyspace: Keyspace): State =
    copy(flushLocalKeyspace = flushLocalKeyspace + keyspace)

  def startRead(): State =
    copy(metrics = metrics.startRead())

  def stopRead(): State =
    copy(metrics = metrics.stopRead())

  def isPure: Boolean = {
    @tailrec def isPure0(state: State): Boolean =
      state.readsWrites.isPure && (
        state.parent match {
          case Some(parent) => isPure0(parent)
          case None         => true
        }
      )
    isPure0(this)
  }

  /** Add a diagnostic to the Query State. These diagnostics are surfaced to the user after query evaluation.
    * These include log and dbg statements as well as hints.
    */
  def addDiagnostic(d: Diagnostic): State =
    d match {
      case h: Hint if h.hintType == Hint.HintType.Performance =>
        copy(performanceHints = performanceHints + h)
      case v => copy(queryDiagnostics = queryDiagnostics :+ v)
    }

  def addDiagnostics(diagnostics: Seq[Diagnostic]): State = {
    if (diagnostics.nonEmpty) {
      val nonHintDiagnostics = diagnostics.filterNot(_.isInstanceOf[Hint])
      val hintDiagnostics = diagnostics.collect { case h: Hint =>
        h
      }
      (nonHintDiagnostics, hintDiagnostics) match {
        case (Nil, hints) =>
          copy(performanceHints = performanceHints ++ hints)
        case (nonHints, Nil) =>
          copy(queryDiagnostics = queryDiagnostics ++ nonHints)
        case (nonHints, hints) =>
          copy(
            queryDiagnostics = queryDiagnostics ++ nonHints,
            performanceHints = performanceHints ++ hints
          )
      }
    } else {
      this
    }
  }

  def diagnostics: Seq[Diagnostic] =
    queryDiagnostics ++ performanceHints.toSeq
}
