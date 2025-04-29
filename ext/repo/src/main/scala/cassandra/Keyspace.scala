package fauna.repo.cassandra

import fauna.atoms._
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.{ TimeBound, Timestamp }
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.repo._
import fauna.repo.query._
import fauna.scheduler._
import fauna.stats.{ QueryMetrics, StatsRecorder }
import fauna.storage.{ Storage, StorageEngine, Tables, TxnRead }
import fauna.storage.api.{ Read => NewRead, Scan => NewScan }
import fauna.storage.cassandra.CassandraKeyLocator
import fauna.storage.index.{ IndexKey, IndexValue }
import fauna.storage.ops._
import io.netty.buffer.{ ByteBuf, Unpooled }
import scala.collection.mutable.{ HashMap => MMap }
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.Future
import scala.util.Try

case class SchemaException(msg: String) extends Exception(msg)

case class Keyspace(
  service: CassandraService,
  storage: Storage,
  clock: Clock,
  stats: StatsRecorder,
  backupReads: Boolean,
  backupReadRate: Double,
  replicaLocal: Boolean) {

  val logger = getLogger()

  // stats

  def withStats(newStats: StatsRecorder): Keyspace =
    copy(stats = newStats)

  def topologyVersion: Long =
    service.partitioner.partitioner.version

  /** Returns all of the segments replicated to this host. If called
    * for every host, will cover every location once for each replica in
    * the cluster.
    */
  def localSegments: Seq[Segment] =
    service.localID.toSeq flatMap { segmentsForHost(_) }

  /** Returns all local primary segments. In order to cover the whole ring,
    * this method must be called for all nodes in the cluster.
    */
  def localPrimarySegments: Seq[Segment] =
    service.localID.toSeq flatMap {
      service.partitioner.partitioner
        .primarySegments(_, false)
    }

  /** Returns all of the segments replicated to a host. If called for
    * every host, will cover every location once for each replica in
    * the cluster.
    */
  def segmentsForHost(id: HostID): Seq[Segment] =
    service.partitioner.partitioner.segments(id, false)

  /** Returns a mapping of hosts to their replicated segments. No
    * location will be repeated in the results, but the entire ring
    * will be represented.
    */
  def allSegmentsByPrimaryHost: Map[HostID, Seq[Segment]] =
    service.dataHosts map { id =>
      id -> service.partitioner.partitioner.primarySegments(id, false)
    } toMap

  /** Returns a mapping of hosts to their replicated segments,
    * according to the rules of `localSegments`.
    */
  def allSegmentsByHost: Map[HostID, Seq[Segment]] =
    service.dataHosts map { id =>
      id -> service.partitioner.partitioner.segments(id, false)
    } toMap

  /** Returns a mapping of hosts to their replicated segments. Segments are
    * labeled with whether they are primary for the host or not.
    */
  def labeledSegmentsByHost: Map[HostID, Seq[(Segment, Boolean)]] =
    service.dataHosts.map { id =>
      val primarySet =
        service.partitioner.partitioner.primarySegments(id, false).toSet
      id -> service.partitioner.partitioner.segments(id, false).map { segment =>
        (segment, primarySet.contains(segment))
      }
    }.toMap

  /** Returns all segments in the ring for the cluster, according to the rules of
    * `allSegmentsByPrimaryHost`.
    */
  def allSegments: Seq[Segment] =
    service.dataHosts.toSeq flatMap {
      service.partitioner.partitioner.primarySegments(_, false)
    }

  /** Returns all segments of the replica. */
  def replicaSegments(replica: String): Map[HostID, Vector[Segment]] = {
    service.partitioner.partitioner.segmentsInReplica(replica)
  }

  def locateKey(key: ByteBuf): Location =
    CassandraKeyLocator.locate(key)

  def locateDocument(scopeID: ScopeID, docID: DocID): Location = {
    val key = Tables.Versions.rowKeyByteBuf(scopeID, docID)
    locateKey(key)
  }

  def segmentForDocument(scopeID: ScopeID, docID: DocID): Try[Segment] = {
    val documentLocation = locateDocument(scopeID, docID)
    // detect warp around
    val endLocation = if (documentLocation == Location.MaxValue) {
      Location.MinValue
    } else {
      Location(documentLocation.token + 1)
    }
    // throws if start_location > end_location
    Try(Segment(documentLocation, endLocation))
  }

  def locateDocumentSegment(
    scopeID: ScopeID,
    docID: DocID): Future[Option[(Segment, Set[HostID])]] =
    Future.fromTry(segmentForDocument(scopeID, docID)).map { segment =>
      service.partitioner.partitioner
        .replicas(segment)
        .headOption
    }

  def segmentIsLocal(segment: Segment): Boolean =
    localSegments.exists { ls =>
      ls.contains(segment.left) && ls.contains(segment.right)
    }

  // Reads.

  def read[A <: NewRead.Result](
    pg: PriorityGroup,
    op: NewRead[A],
    writes: Iterable[Write],
    deadline: TimeBound) =
    service.storageService.read(
      pg.id,
      op,
      writes,
      backupReads,
      replicaLocal,
      deadline)

  def scan[A <: NewScan.Result](
    pg: PriorityGroup,
    op: NewScan[A],
    deadline: TimeBound) =
    service.newStorage.scan(pg.id, op, deadline)

  def repair(
    host: HostID,
    scope: ScopeID,
    cf: String,
    segment: Segment,
    snapTime: Timestamp,
    deadline: TimeBound,
    filterScope: Option[ScopeID]): Future[Unit] =
    service.repairService.repair(
      host,
      scope,
      cf,
      segment,
      snapTime,
      deadline,
      filterScope)

  def indexExists(
    key: IndexKey,
    value: IndexValue,
    snapTime: Timestamp,
    deadline: TimeBound): Future[(Boolean, Boolean)] =
    service.repairService.indexExists(key, value, snapTime, deadline)

  // write

  def execute(
    reads: Vector[ReadRecord],
    writes: Vector[Write],
    writeStats: Write.Stats,
    priorityGroup: PriorityGroup,
    deadline: TimeBound): Future[Option[(Timestamp, FiniteDuration)]] = {
    val readsMap = MMap.empty[TxnRead, Timestamp]
    // Map from key to CF that is required only to provide good contention errors.
    val cfMap = MMap.empty[ByteBuf, String]

    reads foreach { r =>
      // do not propagate reads which opt out of OCC.
      if (r.doConcurrencyCheck) {
        cfMap += r.rowKey -> r.cf
        val key = TxnRead(r.rowKey, RegionID.DefaultID)

        // generate OCC check map, and preemptively abort the transaction if
        // we managed to read the same row and got back different OCC
        // timestamps.
        readsMap.get(key) match {
          case None => readsMap += key -> r.ts
          case Some(prevTS) =>
            if (prevTS != r.ts) {
              throw ContentionException(r.cf, key.rowKey, prevTS max r.ts)
            }
        }
      }
    }

    if (writeStats.added > 0) {
      stats.count("Storage.Ops.Write", writeStats.added)
      stats.timing("Storage.Cells.Written", writeStats.added)
    }

    if (writeStats.removed > 0) {
      stats.count("Storage.Ops.Delete", writeStats.removed)
      stats.timing("Storage.Cells.Delete", writeStats.removed)
    }

    if (writeStats.cleared > 0) {
      stats.count("Storage.Ops.Delete.All", writeStats.cleared)
      stats.timing("Storage.Rows.Delete.All", writeStats.cleared)
    }

    stats.count(QueryMetrics.OCCReads, readsMap.size)
    stats.timing(QueryMetrics.OCCReads, readsMap.size)

    val reducedWrites = writes filter {
      case NoopWrite => false
      case _         => true
    }

    // skip the pipeline if it's a read only txn, or there are no
    // reads and the only writes are Noops.
    if (writes.isEmpty || (reducedWrites.isEmpty && readsMap.isEmpty)) {
      FutureNone
    } else {
      service.txnPipeline.coordinator.write(
        (readsMap.toMap, reducedWrites),
        priorityGroup.id,
        deadline) map {
        case (_, _, StorageEngine.RowReadLockContention(key, newTS)) =>
          val keyByteBuf = Unpooled.wrappedBuffer(key)
          cfMap.get(keyByteBuf) match {
            case Some(cf) => throw ContentionException(cf, keyByteBuf, newTS)
            case None =>
              throw new IllegalStateException(
                "contended key missing from transaction")
          }
        case (txnTime, elapsed, StorageEngine.TxnSuccess) => Some((txnTime, elapsed))
      }
    }
  }
}
