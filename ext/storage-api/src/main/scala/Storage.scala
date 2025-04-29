package fauna.storage.api

import fauna.atoms.{ GlobalID, HostID, ScopeID }
import fauna.codex.cbor.CBOR
import fauna.flags.HostFlags
import fauna.lang.{ TimeBound, Timestamp }
import fauna.lang.clocks.Clock
import fauna.scheduler.{ PriorityGroup, PriorityProvider }
import fauna.stats.StatsRecorder
import fauna.storage.{ Order, StorageEngine, Tables }
import fauna.storage.api.scan.ElementScan
import fauna.storage.api.set.{
  SetHistory,
  SetSnapshot,
  SetSortedValues,
  SparseSetSnapshot
}
import fauna.storage.api.version.{ DocHistory, DocSnapshot }
import fauna.storage.ops.{ VersionAdd, Write }
import fauna.tx.transaction.TransactionStorage
import io.netty.buffer.ByteBuf
import scala.concurrent.{ ExecutionContext, Future }

/** An operation on storage done on behalf of a storage API client. */
sealed trait Op {

  /** The name of the operation, used mostly in metric names.
    * Generally, use toString instead.
    */
  def name: String
}

/** A read of stored data.
  *
  * == Implementation Notes ==
  *
  * Reads should be cacheable, therefore, subclasses should implement `equals` and
  * `hashCode`.
  *
  * Reads lift data from storage and might filter its output before handing it back
  * to call-site. When implementing a read op, make sure to report the following
  * metrics:
  *
  * 1. `[Name].TotalBytesRead`: the total number of bytes read from storage;
  * 2. `[Name].BytesInOutput`: the total number of bytes present in its output;
  * 3. `[Name].BytesBeforeSnapshot`: the number of bytes read before performing
  *    snapshot computation.
  *
  * Note that `Read.Result` reports `bytesRead`. This directly influences read ops
  * computation on the compute-node side. The reported number of bytes depends on the
  * read op implementation. Generally speaking, we should charge for bytes read above
  * MVT, therefore, non-snapshot read ops will report `BytesInOutput` as their
  * `bytesRead` count while snapshot reads will report `BytesBeforeSnapshot` instead.
  */
trait Read[R] extends Op {

  implicit def codec: CBOR.Codec[R]

  def scopeID: ScopeID

  // FIXME: `columnFamily` and `rowKey` are C* storage internals. They should not be
  // part of the public interface. For now, they are used by downstream modules:
  // * The StorageService uses the row key to locate the data in the cluster.
  // * Query evaluation records reads using the column family and row key.
  def columnFamily: String
  def rowKey: ByteBuf

  /** The read op's snapshot timestamp. */
  def snapshotTS: Timestamp

  /** Returns true if the given write op affects the result of this read op. */
  def isRelevant(write: Write): Boolean

  /** This method is used to possibly skip a read from storage.
    * Based on writes that have happened during query execution, it is
    * possible that a given read operation can skip going to storage
    * based on the pending writes.  This method expects all provided
    * writes to be relevant to the read op.  It will return an Option
    * that is present with the result if a trip to storage can be skipped and
    * none otherwise.  If none is returned, it is expected that the run
    * method be used to retrieve the result from storage.
    * The caller must provide the current schema version of the read's
    * scope, so that any versions returned have the correct schema version.
    */
  def skipIO(pendingWrites: Iterable[Write]): Option[R] = None

  /** Run the read op against storage. Account for the effects of the given write
    * intents by merging them with the derived read result.
    */
  private[api] def run(
    source: HostID,
    ctx: Storage.Context,
    priority: PriorityGroup,
    writes: Iterable[Write],
    deadline: TimeBound
  )(implicit ec: ExecutionContext): Future[R]

  override def toString: String =
    s"Read(${CBOR.showBuffer(rowKey)}"

  // Borrowed from query.Read's hash function.
  // The theory is we avoid enough collisions with only cf and row key.
  override def hashCode: Int = 4349 * columnFamily.hashCode * rowKey.hashCode
}

object Read {

  /** The result of a read operation. */
  trait Result {

    /** The time of the last transaction that modified the result. */
    def lastModifiedTS: Timestamp

    /** The time of the last transaction applied to storage. */
    def lastAppliedTS: Timestamp

    /** The total number of bytes read to perform the read request. */
    def bytesRead: Int
  }

  /** A logical cursor defined in descending order: `max >= min`. */
  abstract class Cursor[A, C <: Cursor[A, C]](implicit ord: Ordering[A]) {
    import ord._

    // Eventually, restore this sensible check.
    // require(max >= min, "out-of-order cursor")

    /** The cursor's upper bound. */
    def max: A

    /** The cursor's lower bound. */
    def min: A

    /** The read result's desired order. */
    def order: Order

    /** Create a new cursor with the new boundaries. */
    protected def withCopy(max: A = max, min: A = min): C

    /** Return a copy of this cursor starting at the given boundary (wrt. order). */
    private[api] final def next(from: A) = {
      val res =
        order match {
          case Order.Ascending  => withCopy(min = from)
          case Order.Descending => withCopy(max = from)
        }

      require(res.max >= res.min, "out-of-order next cursor")
      require(
        max >= res.max && res.min >= min,
        "next cursor must be a subset of the logical cursor."
      )

      res
    }

    override def toString =
      s"Cursor(max=$max, min=$min, $order)"
  }

  object Cursor {

    /** A read cursor that can be converted to its in-disk read boundaries.
      *
      * In-disk boundaries are returned in the correct order wrt. `order` with its
      * upper bound adjusted to cover the GC edge. Implementation MUST use the
      * `contains` function to determine if a resulting value is covered by the
      * cursor logical bounds.
      */
    abstract class OnDisk[A, C <: Cursor[A, C]](implicit ord: Ordering[A])
        extends Cursor[A, C] {
      import ord._

      require(validTS(max) >= Timestamp.Epoch, "can't read before the epoch")
      require(validTS(min) >= Timestamp.Epoch, "can't read before the epoch")

      /** Read the boundary's valid time. */
      protected def validTS(value: A): Timestamp

      /** Adjust the boundary's valid time. */
      protected def adjust(value: A, validTS: Timestamp, floor: Boolean): A

      /** Return true if the `value` is contained by the logical cursor. */
      private[api] final def contains(value: A) =
        max >= value && value >= min

      /** Convert this cursor's logic boundaries to its in-disk bounds wrt. order.
        *
        * Note that in-disk bounds must cover the GC edge so that reads can correctly
        * derive the GC root. Therefore, the logical read's upper bound is rewritten
        * to cover the GC edge. Reads may choose to truncate its lower-bound to the
        * MVT by setting the `truncated` argument to `true`.
        */
      private[api] final def inDiskBounds(
        mvt: Timestamp,
        truncated: Boolean = false) = {

        val upperBound =
          if (mvt > validTS(max)) {
            adjust(max, mvt, floor = false)
          } else {
            max
          }

        val lowerBound =
          if (truncated && validTS(min) < mvt) {
            // NB. Covering a single valid time at MVT cause out-of-order bounds on
            // set history due to how event are sorted in event order, thus, we set
            // its lower-bound to the previous micro. This is safe as long as read
            // ops use the `contains` function to discard values outside of this
            // cursor's logical bounds.
            adjust(min, mvt.prevMicro, floor = true)
          } else {
            min
          }

        val res =
          order match {
            case Order.Ascending  => (lowerBound, upperBound)
            case Order.Descending => (upperBound, lowerBound)
          }

        // Eventually, restore this sensible check.
        /* require( if (order == Order.Ascending) from <= to else from >= to,
         * "out-of-order bounds" ) */

        res
      }
    }
  }

  implicit val ReadCodec: CBOR.Codec[Read[_]] =
    CBOR.SumCodec[Read[_]](
      CBOR.TupleCodec[DocSnapshot],
      CBOR.TupleCodec[DocHistory],
      CBOR.TupleCodec[SetSnapshot],
      CBOR.TupleCodec[SetHistory],
      CBOR.TupleCodec[SparseSetSnapshot],
      CBOR.TupleCodec[HealthChecks],
      CBOR.TupleCodec[SchemaVersionSnapshot],
      CBOR.TupleCodec[Lookups],
      CBOR.TupleCodec[SetSortedValues]
    )
}

object Scan {

  // A scan result. For now, just a tag.
  trait Result
}

/** A scan operation. Scan operations differ from reads in that:
  * 1) Scans may return multiple rows (in the C* sense) from one operation.
  * 2) They are local to a data node and do not cross the network boundary.
  */
trait Scan[R] extends Op {

  /** The scan's snapshot timestamp. */
  def snapshotTS: Timestamp

  private[api] def run(
    ctx: Storage.Context,
    priority: PriorityGroup,
    deadline: TimeBound
  )(implicit ec: ExecutionContext): Future[R]
}

object Storage {

  /** A context for interaction with storage. */
  final class Context(
    val priorities: PriorityProvider,
    val engine: StorageEngine,
    val hostFlags: () => Future[Option[HostFlags]],
    val stats: StatsRecorder = StatsRecorder.Null
  )

  def apply(ctx: Context): Storage =
    new Storage(ctx)
}

/** A handle for running operations against storage in the context `ctx`. */
final class Storage private (ctx: Storage.Context) {

  /** Perform the read `op` against storage. */
  def read[R](
    source: HostID,
    globalID: GlobalID,
    op: Read[R],
    writes: Iterable[Write],
    deadline: TimeBound
  )(implicit ec: ExecutionContext): Future[R] = {
    ctx.priorities.lookup(globalID) flatMap { priority =>
      op.run(source, ctx, priority, writes, deadline)
    }
  }

  /** Perform the scan against storage. */
  def scan[R](
    globalID: GlobalID,
    op: Scan[R],
    deadline: TimeBound
  )(implicit ec: ExecutionContext): Future[R] = {
    ctx.priorities.lookup(globalID) flatMap { priority =>
      op.run(ctx, priority, deadline)
    }
  }

  /** A temporary API for deleting entries returned by the scan API.
    * This function is meant for exclusive use by non-transactional
    * garbage collection. The write is not transactional.
    *
    * Eventually, this API will be replaced by one that uses Fauna-level
    * tombstones. The improved API needs to:
    * * Make the writes transactional.
    * * Drop a Fauna-level tombstone, not a C* tombstone.
    * * Use a transaction timestamp at or just above the cell's current
    *   timestamp.
    * * Have a more proper but less cool name.
    */
  def zapEntry(cf: String, e: ElementScan.Entry): Unit =
    ctx.engine.applyMutationForKey(e.key.duplicate) { mut =>
      mut.delete(cf, e.cellName.duplicate, Clock.time)
    }

  /** A special API for overwriting a version. Useful, e.g., to locally update an
    * unmigrated version to its migrated form without using the transaction pipeline.
    * Don't use this unless you know what you're doing.
    */
  def overwriteVersion(add: VersionAdd): Unit = {
    // This API should not be used if the transaction TS isn't known or the schema
    // version is pending (sync migration).
    require(
      add.writeTS.isResolved,
      "tried to overwrite version using an unresolved timestamp")
    require(
      !add.schemaVersion.isPending,
      "tried to overwrite version using a pending schema version"
    )
    ctx.engine.applyMutationForKey(add.rowKey) { mut =>
      add.mutateAt(mut, add.writeTS.transactionTS)
    }
  }

  /** Force storage to persist the versions CF durably, for example, after a batch of
    * calls to `overwriteVersion`. Note that this does not update the persisted
    * timestamp.
    */
  def syncVersions(): Unit =
    ctx.engine.sync(
      TransactionStorage.NormalSyncMode,
      Set(Tables.Versions.CFName),
      updatePersistedTimestamp = false)
}
