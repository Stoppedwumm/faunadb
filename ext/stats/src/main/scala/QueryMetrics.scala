package fauna.stats

import scala.concurrent.duration._

object QueryMetrics {

  /** Baseline compute ops approximate the number of compute ops
    * performed in one CPU-millisecond. It is used as the divisor for
    * computing the number of _billable_ compute ops.
    */
  final val BaselineCompute = 50.0d

  /** The number of bytes written to storage which result in a single
    * billable write op.
    */
  final val BytesPerWriteOp = 1024

  /** The number of bytes read from storage which result in a single
    * billable read op.
    */
  final val BytesPerReadOp = 4096

  final val QueryTime = "Query.Time"
  final val RenderTime = "Query.Render.Time"

  final val BytesIn = "Query.Request.Bytes.In"
  final val BytesOut = "Query.Request.Bytes.Out"

  final val BytesRead = "Query.Bytes.Read"
  final val BytesWrite = "Query.Bytes.Write"

  final val Compute = "Query.Compute.Ops"
  final val ComputeTime = "Query.Compute.Time"

  final val ReadDocument = "Query.Read.Ops.Document"
  final val ReadSet = "Query.Read.Ops.Set"
  final val ByteReadOps = "Query.Read.Ops.Byte"
  final val SerialReadOps = "Query.Read.Ops.Serial"
  final val ReadTime = "Query.Read.Time"

  final val WriteCreate = "Query.Write.Ops.Create"
  final val WriteUpdate = "Query.Write.Ops.Update"
  final val WriteDelete = "Query.Write.Ops.Delete"
  final val WriteInsert = "Query.Write.Ops.Insert"
  final val WriteRemove = "Query.Write.Ops.Remove"
  final val ByteWriteOps = "Query.Write.Ops.Byte"

  final val TransactionsContended = "Storage.Transactions.Contended"
  final val TransactionsTooLarge = "Storage.Transactions.TooLarge"
  final val OCCReads = "Transaction.OCCReads"

  final val ExpectedTime = "Query.Time.Expected"
  final val ExpectedComputeTime = "Query.Compute.Time.Expected"
  final val ExpectedReadTime = "Query.Read.Time.Expected"
  final val ExpectedWriteTime = "Query.Write.Time.Expected"

  final val OverheadTime = "Query.Time.Overhead"

  final val TransactionLogTime = "Query.Transaction.Log.Time"

  final val RateLimitCompute = "Query.RateLimit.Compute"
  final val RateLimitRead = "Query.RateLimit.Read"
  final val RateLimitWrite = "Query.RateLimit.Write"
  final val RateLimitStream = "Query.RateLimit.Stream"

  final val DatabaseCreate = "Query.Database.Create"
  final val DatabaseUpdate = "Query.Database.Update"
  final val DatabaseDelete = "Query.Database.Delete"

  final val CollectionCreate = "Query.Collection.Create"
  final val CollectionUpdate = "Query.Collection.Update"
  final val CollectionDelete = "Query.Collection.Delete"

  final val RoleCreate = "Query.Role.Create"
  final val RoleUpdate = "Query.Role.Update"
  final val RoleDelete = "Query.Role.Delete"

  final val KeyCreate = "Query.Key.Create"
  final val KeyUpdate = "Query.Key.Update"
  final val KeyDelete = "Query.Key.Delete"

  final val FunctionCreate = "Query.Function.Create"
  final val FunctionUpdate = "Query.Function.Update"
  final val FunctionDelete = "Query.Function.Delete"

  final val ReadCacheHits = "Query.ReadCache.Hits"
  final val ReadCacheMisses = "Query.ReadCache.Misses"
  final val ReadCacheBytesLoaded = "Query.ReadCache.BytesLoaded"

  /** A shorthand for filtering reads, writes, and OCC checks.
    */
  final val BytesReadWrite =
    Set(QueryMetrics.BytesRead, QueryMetrics.BytesWrite, QueryMetrics.OCCReads)

  val Null =
    QueryMetrics(
      byteReadOps = 0,
      byteWriteOps = 0,
      computeOps = 0,
      queryTime = 0,
      queryBytesIn = 0,
      storageBytesWrite = 0,
      storageBytesRead = 0,
      txnRetries = 0,
      expectedComputeTime = 0,
      expectedReadTime = 0,
      expectedWriteTime = 0,
      expectedQueryTime = 0,
      readTime = 0,
      computeTime = 0,
      txnLogTime = None,
      rateLimitComputeHit = false,
      rateLimitReadHit = false,
      rateLimitWriteHit = false,
      databaseCreates = 0,
      databaseUpdates = 0,
      databaseDeletes = 0,
      collectionCreates = 0,
      collectionUpdates = 0,
      collectionDeletes = 0,
      documentCreates = 0,
      documentUpdates = 0,
      documentDeletes = 0,
      roleCreates = 0,
      roleUpdates = 0,
      roleDeletes = 0,
      keyCreates = 0,
      keyUpdates = 0,
      keyDeletes = 0,
      functionCreates = 0,
      functionUpdates = 0,
      functionDeletes = 0,
      readCacheHits = 0,
      readCacheMisses = 0,
      readCacheBytesLoaded = 0
    )

  def apply(
    queryTime: Long,
    stats: StatsRequestBuffer,
    minComputeOps: Long = 1,
    computeOverhead: FiniteDuration = 0.millis,
    readOverhead: FiniteDuration = 1.millis,
    writeOverhead: FiniteDuration = 1.millis,
    txnOverhead: FiniteDuration = 100.millis): QueryMetrics = {

    val writeOps =
      stats.countOrZero(WriteCreate) +
        stats.countOrZero(WriteUpdate) +
        stats.countOrZero(WriteDelete) +
        stats.countOrZero(WriteInsert) +
        stats.countOrZero(WriteRemove)

    val queryCompute = stats.countOpt(Compute).getOrElse(minComputeOps)
    val computeOps = if (queryCompute > 0L) {
      Math.ceil(queryCompute / BaselineCompute).toLong
    } else {
      0
    }

    val serialReadOps = stats.countOrZero(SerialReadOps)

    val expectedComputeTime = computeOps * computeOverhead.toMillis
    val expectedReadTime = serialReadOps * readOverhead.toMillis
    val expectedWriteTime =
      computeExpectedWriteTime(
        writeOps,
        writeOverhead.toMillis,
        txnOverhead.toMillis)
    val expectedQueryTime =
      expectedReadTime + expectedWriteTime + expectedComputeTime

    val databaseCreates = stats.countOrZero(DatabaseCreate)
    val databaseUpdates = stats.countOrZero(DatabaseUpdate)
    val databaseDeletes = stats.countOrZero(DatabaseDelete)
    val collectionCreates = stats.countOrZero(CollectionCreate)
    val collectionUpdates = stats.countOrZero(CollectionUpdate)
    val collectionDeletes = stats.countOrZero(CollectionDelete)
    val roleCreates = stats.countOrZero(RoleCreate)
    val roleUpdates = stats.countOrZero(RoleUpdate)
    val roleDeletes = stats.countOrZero(RoleDelete)
    val keyCreates = stats.countOrZero(KeyCreate)
    val keyUpdates = stats.countOrZero(KeyUpdate)
    val keyDeletes = stats.countOrZero(KeyDelete)
    val functionCreates = stats.countOrZero(FunctionCreate)
    val functionUpdates = stats.countOrZero(FunctionUpdate)
    val functionDeletes = stats.countOrZero(FunctionDelete)
    val documentCreates =
      stats.countOrZero(
        WriteCreate) - databaseCreates - collectionCreates - roleCreates - functionCreates - keyCreates
    val documentUpdates =
      stats.countOrZero(
        WriteUpdate) - databaseUpdates - collectionUpdates - roleUpdates - functionUpdates - keyUpdates
    val documentDeletes =
      stats.countOrZero(
        WriteDelete) - databaseDeletes - collectionDeletes - roleDeletes - functionDeletes - keyDeletes

    new QueryMetrics(
      stats.countOrZero(ByteReadOps),
      stats.countOrZero(ByteWriteOps),
      computeOps,
      queryTime,
      stats.countOrZero(BytesIn),
      stats.countOrZero(BytesWrite),
      stats.countOrZero(BytesRead),
      stats.countOrZero(TransactionsContended),
      expectedComputeTime,
      expectedReadTime,
      expectedWriteTime,
      expectedQueryTime,
      stats.countOrZero(ReadTime),
      stats.countOrZero(ComputeTime),
      stats.timingOpt(TransactionLogTime),
      stats.countOrZero(RateLimitCompute) > 0,
      stats.countOrZero(RateLimitRead) > 0,
      stats.countOrZero(RateLimitWrite) > 0,
      databaseCreates = databaseCreates,
      databaseUpdates = databaseUpdates,
      databaseDeletes = databaseDeletes,
      collectionCreates = collectionCreates,
      collectionUpdates = collectionUpdates,
      collectionDeletes = collectionDeletes,
      documentCreates = documentCreates,
      documentUpdates = documentUpdates,
      documentDeletes = documentDeletes,
      roleCreates = roleCreates,
      roleUpdates = roleUpdates,
      roleDeletes = roleDeletes,
      keyCreates = keyCreates,
      keyUpdates = keyUpdates,
      keyDeletes = keyDeletes,
      functionCreates = functionCreates,
      functionUpdates = functionUpdates,
      functionDeletes = functionDeletes,
      readCacheHits = stats.countOrZero(ReadCacheHits),
      readCacheMisses = stats.countOrZero(ReadCacheMisses),
      readCacheBytesLoaded = stats.countOrZero(ReadCacheBytesLoaded)
    )
  }

  def computeExpectedQueryTime(
    stats: StatsRequestBuffer,
    minComputeOps: Long = 1,
    computeOverhead: Long = 0,
    readOverhead: Long = 1,
    writeOverhead: Long = 1,
    txnOverhead: Long = 100): FiniteDuration = {

    val queryCompute = stats.countOpt(Compute).getOrElse(minComputeOps)
    val computeOps = if (queryCompute > 0L) {
      Math.ceil(queryCompute / BaselineCompute).toLong
    } else {
      0
    }
    val readOps = stats.countOrZero(QueryMetrics.SerialReadOps)
    val writeOps = math.min(1, stats.countOrZero(ByteWriteOps))

    val expectedComputeTime = computeOps * computeOverhead
    val expectedReadTime = readOps * readOverhead
    val expectedWriteTime =
      computeExpectedWriteTime(writeOps, writeOverhead, txnOverhead)
    (expectedComputeTime + expectedReadTime + expectedWriteTime).millis
  }

  /** Write ops overhead is measured as one unit of write overhead per write operation,
    * plus a flat overhead for the transaction.
    */
  def computeExpectedWriteTime(
    writeOps: Long,
    writeOverhead: Long,
    txnOverhead: Long): Long = {

    (writeOps * writeOverhead) + (math.min(1, writeOps) * txnOverhead)
  }
}

final case class QueryMetrics private (
  byteReadOps: Long,
  byteWriteOps: Long,
  computeOps: Long,
  queryTime: Long,
  queryBytesIn: Long,
  storageBytesWrite: Long,
  storageBytesRead: Long,
  txnRetries: Long,
  expectedComputeTime: Long,
  expectedReadTime: Long,
  expectedWriteTime: Long,
  expectedQueryTime: Long,
  readTime: Long,
  computeTime: Long,
  txnLogTime: Option[Long],
  rateLimitComputeHit: Boolean,
  rateLimitReadHit: Boolean,
  rateLimitWriteHit: Boolean,
  databaseCreates: Long,
  databaseUpdates: Long,
  databaseDeletes: Long,
  collectionCreates: Long,
  collectionUpdates: Long,
  collectionDeletes: Long,
  documentCreates: Long,
  documentUpdates: Long,
  documentDeletes: Long,
  roleCreates: Long,
  roleUpdates: Long,
  roleDeletes: Long,
  keyCreates: Long,
  keyUpdates: Long,
  keyDeletes: Long,
  functionCreates: Long,
  functionUpdates: Long,
  functionDeletes: Long,
  readCacheHits: Long,
  readCacheMisses: Long,
  readCacheBytesLoaded: Long
) {

  import QueryMetrics._

  /** Output metrics derived from per-query stats buffers to the given
    * recorder.
    */
  def toStats(stats: StatsRecorder) = {
    stats.timing(OverheadTime, queryTime - expectedQueryTime)
    stats.timing(ExpectedComputeTime, expectedComputeTime)
    stats.timing(ExpectedReadTime, expectedReadTime)
    stats.timing(ExpectedWriteTime, expectedWriteTime)
    stats.timing(ExpectedTime, expectedQueryTime)
  }
}
