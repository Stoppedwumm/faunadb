package fauna.repo.service

import fauna.atoms._
import fauna.cluster._
import fauna.codex.cbor._
import fauna.exec._
import fauna.lang.{ TimeBound, Timestamp }
import fauna.lang.syntax._
import fauna.net.HostInfo
import fauna.net.bus._
import fauna.scheduler.PriorityGroup
import fauna.storage._
import fauna.storage.cassandra.ColumnFamilySchema
import fauna.storage.index._
import fauna.tx.transaction.{ KeyLocator, Partitioner }
import io.netty.buffer.{ ByteBuf, Unpooled }
import java.io.Closeable
import java.util.concurrent.TimeoutException
import scala.annotation.unused
import scala.concurrent.{ ExecutionContext, Future, Promise }

class RepairFailed(msg: String) extends Exception(msg)

object RepairService {
  object V2 {

    sealed abstract class Request
    // Replication repair
    case object Unused extends Request // used to be Validate
    case class Sync(source: HostID, scope: ScopeID, cf: String, segments: Vector[Segment], snapTime: Timestamp, replyTo: SignalID) extends Request
    case class ValidateOne(scope: ScopeID, cf: String, segment: Segment, snapTime: Timestamp, replyTo: SignalID, filterScope: Option[ScopeID]) extends Request

    // Logical/Data model repair
    case class IndexExists(key: IndexKey, value: IndexValue, snapTime: Timestamp, replyTo: SignalID) extends Request

    sealed abstract class Reply
    // Replication repair
    case class Conflicts(cf: String, sync: Vector[((HostID, HostID), Vector[Segment])]) extends Reply
    case class SyncComplete(source: HostID, scope: ScopeID, cf: String, segments: Vector[Segment]) extends Reply
    case class SyncFailed(source: HostID, scope: ScopeID, cf: String, segments: Vector[Segment]) extends Reply
    case class ValidateFailed(scope: ScopeID, cf: String, segment: Segment) extends Reply

    // Logical/Data model repair
    case class IndexExistence(sortExists: Boolean, histExists: Boolean) extends Reply

    implicit val RequestCodec = CBOR.SumCodec[Request](
      CBOR.DefunctCodec(Unused),
      CBOR.RecordCodec[Sync],
      CBOR.RecordCodec[ValidateOne],
      CBOR.RecordCodec[IndexExists])

    implicit val ReplyCodec = CBOR.SumCodec[Reply](
      CBOR.TupleCodec[Conflicts],
      CBOR.TupleCodec[SyncComplete],
      CBOR.TupleCodec[SyncFailed],
      CBOR.TupleCodec[ValidateFailed],
      CBOR.TupleCodec[IndexExistence])
  }
}

class RepairService(
  signal: SignalID,
  bus: MessageBus,
  partitioner: => Partitioner[TxnRead, _],
  membership: Membership,
  keyLocator: KeyLocator[ByteBuf, TxnRead],
  validationService: ValidationService,
  transferService: TransferService,
  store: StorageEngine,
  syncParallelism: Int,
  maxHashDepth: Int) extends Closeable {

  import RepairService._
  import StorageEngine._
  import Tables._

  private val RequestProtocolV2 = Protocol[V2.Request]("repair.request.v2")(V2.RequestCodec)
  private val ReplyProtocolV2 = Protocol.Reply[V2.Request, V2.Reply]("repair.reply.v2")

  private val handlerV2 = bus.handler(RequestProtocolV2, signal) {
    case (from, V2.ValidateOne(scope, cf, seg, ts, replyTo, filterScope), deadline) =>
      require(scope == ScopeID.RootID)
      validateOne(cf, seg, ts, replyTo.at(from.id), deadline, filterScope)
    case (from, V2.Sync(src, scope, cf, segs, ts, replyTo), deadline) =>
      require(scope == ScopeID.RootID)
      sync(src, cf, segs, ts, replyTo.at(from.id), deadline)
    case (from, V2.IndexExists(k, v, ts, replyTo), deadline) =>
      indexExistence(k, v, ts, replyTo.at(from.id), deadline)
    case (_, V2.Unused, _) => throw new AssertionError()
  }

  private val logger = getLogger

  def close() = {
    handlerV2.close()
  }

  /**
    * Sends a validation request to the provided host. The receiver
    * will validate the segment for the provided scope against
    * its peers and reply with a set of conflicts. The coordinator
    * will then issue appropriate transfer requests to resolve the
    * conflicts.
    */
  def repair(
    host: HostID,
    scope: ScopeID,
    cf: String,
    segment: Segment,
    snapTime: Timestamp,
    deadline: TimeBound,
    filterScope: Option[ScopeID])(implicit ec: ExecutionContext): Future[Unit] = {
    val src = bus.tempSource(ReplyProtocolV2)

    val msg = V2.ValidateOne(scope, cf, segment, snapTime, src.id.signalID, filterScope)
    bus.sink(RequestProtocolV2, signal.at(host)).send(msg, deadline).unit

    gather(src, host, scope, snapTime, deadline)
  }

  /**
    * Performs a replica-local check for the existence of a key/value
    * pair within both index column families at the provided snapshot
    * time.
    * 
    * Returns two boolean values, indicating whether the pair exists
    * in the SortedIndex CF and the HistoricalIndex CF, respectively.
    */
  def indexExists(
    key: IndexKey,
    value: IndexValue,
    snapTime: Timestamp,
    deadline: TimeBound)(implicit
    ec: ExecutionContext): Future[(Boolean, Boolean)] = {
    val rowKey = Unpooled.wrappedBuffer(Tables.Indexes.rowKey(key))

    val txnRead = TxnRead(rowKey, RegionID.DefaultID)
    val replicas = partitioner.hostsForRead(txnRead)

    // bad news bears
    if (replicas.isEmpty) {
      throw ClusterTopologyException(keyLocator.locate(rowKey))
    }

    val localReplica = membership.localReplica
    val host = replicas find { membership.getReplica(_) == localReplica }

    host match {
      case None =>
        throw new IllegalStateException(
          s"No host in replica $localReplica for $key.")
      case Some(host) if host == bus.hostID =>
        indexExistsLocal(rowKey, key, value, snapTime, deadline)
      case Some(host) =>
        indexExistsRemote(host, key, value, snapTime, deadline)
    }
  }

  private def gather(src: Source[V2.Reply], host: HostID, scope: ScopeID, snapTime: Timestamp, deadline: TimeBound)(implicit ec: ExecutionContext): Future[Unit] = {
    src.fold(1L, deadline.timeLeft) {
      case (pending, Source.Messages(rs)) =>

        try {
          val inflight = rs.foldLeft(pending) { (acc, reply) =>
            reply.value match {
              case V2.Conflicts(cf, todo) if todo.nonEmpty =>
                todo foreach {
                  case ((hostA, hostB), conflicts) =>

                    def sendSync(hostA: HostID, hostB: HostID): Unit = {
                      val msg = V2.Sync(hostB, scope, cf, conflicts, snapTime, src.id.signalID)
                      bus.sink(RequestProtocolV2, signal.at(hostA)).send(msg, deadline)
                    }

                    // We can't be certain how the two hosts differ, so
                    // we'll transfer in both directions and let
                    // compaction sort it out.
                    logger.info(s"$hostA and $hostB conflict for $scope $cf in ${conflicts.size} segments. Syncing.")
                    sendSync(hostA, hostB)
                    sendSync(hostB, hostA)
                }

                acc + (todo.size * 2) - 1
              case V2.Conflicts(cf, _) =>
                logger.info(s"${reply.from.id} found no conflicts for $cf.")
                acc - 1
              case V2.SyncComplete(src, scope, cf, segs) =>
                logger.info(s"${reply.from.id} sync with $src completed for $scope $cf in ${segs.size} segments.")
                acc - 1
              case V2.SyncFailed(src, scope, cf, segs) =>
                logger.warn(s"${reply.from.id} sync with $src failed for $scope $cf in ${segs.size} segments.")
                throw new RepairFailed(s"Repair sync failed for $host.")
              case V2.ValidateFailed(scope, cf, seg) =>
                logger.warn(s"${reply.from.id} validation failed for $scope $cf $seg")
                throw new RepairFailed(s"Repair validation failed for $host.")
              case msg: V2.IndexExistence =>
                throw new IllegalStateException(s"Unexpected message $msg.")
            }
          }

          if (inflight == 0) {
            Future.successful(Right(()))
          } else {
            Future.successful(Left(inflight))
          }
        } catch {
          case ex: RepairFailed => Future.failed(ex)
        }
      case _ =>
        val msg = s"Repair timed out for $host."
        Future.failed(new TimeoutException(msg))
    }
  }

  private def validateOne(cf: String, segment: Segment, snapTime: Timestamp, replyTo: HandlerID, deadline: TimeBound, filterScope: Option[ScopeID]): Future[Unit] = {
    implicit val ec = ImmediateExecutionContext

    validationService.validate(cf,
                               segment,
                               deadline,
                               maxHashDepth,
                               snapTime,
                               filterScope) flatMap { conflicts =>
      val cs = conflicts map {
        case (xs, os) => (xs, os.toVector)
      } toVector

      bus.sink(ReplyProtocolV2, replyTo).send(V2.Conflicts(cf, cs), deadline).unit
    } recoverWith {
      case _: TimeoutException =>
        bus.sink(ReplyProtocolV2, replyTo).send(V2.ValidateFailed(ScopeID.RootID, cf, segment)).unit
    }
  }

  private def sync(source: HostID, cf: String, segments: Vector[Segment], snapTime: Timestamp, replyTo: HandlerID, deadline: TimeBound): Future[Unit] = {
    implicit val ec = ImmediateExecutionContext

    segments.grouped(syncParallelism).foldLeft(Future.unit) {
      case (fut, segs) =>
        fut flatMap { _ =>
          // explicitly passing the global EC, as transferService.request might perform thread blocking operations.
          transferService.request(source, Some(cf), segs, snapTime, deadline)(FaunaExecutionContext.Implicits.global) map { result =>
            // this condition is likely to be a topology mismatch
            if (result.isEmpty || segs != result) {
              throw new TransferFailed(s"Sync failed requesting $segs $cf from $source.")
            }
          } recoverWith {
            case ex @ (_: TimeoutException | _: TransferFailed) =>
              logger.warn("RepairService synchronisation failed", ex)
              val msg = V2.SyncFailed(source, ScopeID.RootID, cf, segments)
              bus.sink(ReplyProtocolV2, replyTo).send(msg, deadline) flatMap { _ =>
                // do not continue syncing segments after a failure
                Future.failed(ex)
              }
          }
        }
    } flatMap { _ =>
      val msg = V2.SyncComplete(source, ScopeID.RootID, cf, segments)
      bus.sink(ReplyProtocolV2, replyTo).send(msg, deadline).unit
    } recover {
      // do not allow "expected" failures to propogate to the handler
      case _: TimeoutException | _: TransferFailed => ()
    }
  }

  private def indexExistsLocal(rowKey: ByteBuf, key: IndexKey, value: IndexValue, snapTime: Timestamp, deadline: TimeBound)(implicit ec: ExecutionContext): Future[(Boolean, Boolean)] = {
    def slice(enc: (IndexKey, IndexValue) => Predicate, schema: ColumnFamilySchema): Slice = {
      val pred = enc(key, value)
      val from = schema.encodePrefix(pred, Predicate.LTE) getOrElse Unpooled.EMPTY_BUFFER
      val to = schema.encodePrefix(pred, Predicate.GTE) getOrElse Unpooled.EMPTY_BUFFER
      Slice(schema.name, Vector((from, to)), count = 1, order = Order.Descending)
    }

    try {
      val slices = Vector(
        slice(SortedIndex.toKey, SortedIndex.Schema),
        slice(HistoricalIndex.toKey, HistoricalIndex.Schema))

      store.readRow(snapTime, rowKey, slices, PriorityGroup.Default, deadline) map { row =>
        var sortExists = false
        var histExists = false

        row.cfs foreach {
          case (SortedIndex.CFName, cells) if cells.nonEmpty => sortExists = true
          case (HistoricalIndex.CFName, cells) if cells.nonEmpty => histExists = true
          case _ => ()
        }

        (sortExists, histExists)
      }
    } catch {
      case _: ComponentTooLargeException =>
        logger.warn(s"RepairService ${rowKey.toHexString} component too large.")
        // the SetAdd which would have generated these index entries
        // must have been dropped due to size, so "drop" the set
        // repair op by pretending both cells exist; there's nothing
        // further to be done.
        Future.successful((true, true))
    }
  }

  private def indexExistsRemote(host: HostID, key: IndexKey, value: IndexValue, snapTime: Timestamp, deadline: TimeBound)(implicit ec: ExecutionContext): Future[(Boolean, Boolean)] = {
    val result = Promise[(Boolean, Boolean)]()

    def onReply(hi: HostInfo, reply: V2.Reply, @unused deadline: TimeBound) = {
      reply match {
        case V2.IndexExistence(sorted, historical) =>
          result.trySuccess((sorted, historical))
        case _ =>
          val msg = s"Unknown response $reply to IndexExists from ${hi.id}"
          result.tryFailure(new IllegalStateException(msg))
      }

      Future.unit
    }

    def onExpiry = {
      val msg = s"Timeout for index existence check on $key/$value."
      result.tryFailure(new TimeoutException(msg))
      Future.unit
    }

    val src = bus.tempHandler(ReplyProtocolV2, deadline.timeLeft)(onReply, onExpiry)

    val msg = V2.IndexExists(key, value, snapTime, src.id.signalID)
    bus.sink(RequestProtocolV2, signal.at(host)).send(msg, deadline)

    result.future ensure {
      src.close()
    }
  }

  private def indexExistence(
    key: IndexKey,
    value: IndexValue,
    snapTime: Timestamp,
    replyTo: HandlerID,
    deadline: TimeBound): Future[Unit] = {
    implicit val ec = ImmediateExecutionContext

    val rowKey = Unpooled.wrappedBuffer(Tables.Indexes.rowKey(key))
    val read = TxnRead(rowKey, RegionID.DefaultID)

    def exists(): Future[Unit] = {
      val part = partitioner

      if (!part.coversRead(read)) {
        Future.failed(new IllegalArgumentException(s"Index key $key not covered by this node."))
      } else {
        indexExistsLocal(rowKey, key, value, snapTime, deadline) flatMap {
          case (sortExists, histExists) =>
            if (partitioner.version == part.version) {
              bus.sink(ReplyProtocolV2, replyTo).send(V2.IndexExistence(sortExists, histExists), deadline).unit
            } else if (deadline.hasTimeLeft) {
              exists() // retry
            } else {
              Future.failed(new TimeoutException("Timed out waiting for topology to stabilize."))
            }
        }
      }
    }

    exists()
  }
}
