package fauna.repo.service

import fauna.atoms._
import fauna.codex.cbor._
import fauna.exec._
import fauna.lang._
import fauna.lang.syntax._
import fauna.net.bus._
import fauna.storage.{ HashTree, StorageEngine, Tables }
import fauna.tx.transaction.Partitioner
import io.netty.buffer.ByteBuf
import java.io.Closeable
import java.util.concurrent.TimeoutException
import scala.collection.mutable.{ Map => MMap, Set => MSet }
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }

trait SegmentValidationService {

  protected val minPersistedTS: Function0[Timestamp]

  def validate(
    cf: String,
    segment: Segment,
    deadline: TimeBound,
    maxDepth: Int,
    snapTime: Timestamp = minPersistedTS(),
    filterScope: Option[ScopeID] = None)(implicit ec: ExecutionContext)
    : Future[Seq[((HostID, HostID), Iterable[Segment])]]
}

object ValidationService {
  case class Request(
    scope: ScopeID,
    cf: String,
    segment: Segment,
    snapTime: Timestamp,
    replyTo: SignalID,
    maxDepth: Int,
    filterScope: Option[ScopeID])

  case class Reply(tree: HashTree)

  implicit val RequestCodec = CBOR.TupleCodec[Request]
  implicit val ReplyCodec = CBOR.TupleCodec[Reply]
}

final class ValidationService(
  signalID: SignalID,
  bus: MessageBus,
  partitioner: => Partitioner[_, _],
  storage: StorageEngine,
  val minPersistedTS: () => Timestamp)
    extends SegmentValidationService
    with Closeable {

  import ValidationService._

  private type ReplyMap = MMap[Segment, MSet[(HostID, HashTree)]]
  private def newReplyMap: ReplyMap = MMap.empty

  private val RequestProtocolV2 = Protocol[Request]("validation.request.v2")(RequestCodec)
  private val ReplyProtocolV2 = Protocol.Reply[Request, Reply]("validation.reply")

  private val handlerV2 = bus.handler(RequestProtocolV2, signalID) {
    case (from, Request(scope, cf, seg, ts, replyTo, maxDepth, filterScope), deadline) =>
      require(scope == ScopeID.RootID)
      recvValidate(cf, seg, ts, replyTo.at(from.id), deadline, maxDepth, filterScope)
  }

  private val logger = getLogger

  def close() = {
    handlerV2.close()
  }

  /**
    * Validation builds a hash tree based on the data within a
    * particular segment. Each process responds to the coordinator
    * with the resulting hash.
    *
    * After hashes have been received from everyone involved, the
    * coordinator computes the pair-wise difference between hash trees
    * to discover all segments on which two or more processes
    * disagree. The caller is then responsible for sending sync
    * requests for these segments.
    */
  def validate(
    cf: String,
    segment: Segment,
    deadline: TimeBound,
    maxDepth: Int,
    snapTime: Timestamp = minPersistedTS(),
    filterScope: Option[ScopeID] = None)(implicit ec: ExecutionContext)
    : Future[Seq[((HostID, HostID), Iterable[Segment])]] = {
    val src = bus.tempSource(ReplyProtocolV2)
    val msgs = partitioner.replicas(segment) flatMap {
      case (segment, hosts) =>
        hosts map { host =>
          logger.debug(s"Requesting validation of $cf $segment from $host.")

          val msg = Request(ScopeID.RootID, cf, segment, snapTime, src.id.signalID, maxDepth, filterScope)
          bus.sink(RequestProtocolV2, signalID.at(host)).send(msg, deadline)
        }
    }

    gather(src, msgs.size, deadline) map { diff(_) }
  }

  // Gathers `count` replies from `source` into a map from
  // (sub-)segment to host/tree pairs. Each tree within a segment is
  // then comparable to discover replication inconsistencies between
  // hosts.
  private def gather(source: Source[Reply], count: Long, deadline: TimeBound)(implicit ec: ExecutionContext): Future[ReplyMap] =
    source.fold((newReplyMap, 0), deadline.timeLeft) {
      case ((replies, n), Source.Messages(rs)) =>
        val rcvs = rs.foldLeft(0) { (acc, r) =>
          replies
            .getOrElseUpdate(r.value.tree.segment, MSet.empty)
            .add(r.from.id -> r.value.tree)
          acc + 1
        }

        val total = n + rcvs
        if (total == count) {
          Future.successful(Right(replies))
        } else {
          Future.successful(Left((replies, total)))
        }
      case _ =>
        Future.failed(new TimeoutException("Validation request timed out."))
    }

  // For each of the tree replies received, this computes the pairwise
  // difference between trees. Any pair of hosts which are
  // inconsistent are then returned with the list of segments on which
  // they disagree.
  private def diff(replies: ReplyMap): Seq[((HostID, HostID), Iterable[Segment])] = {
    val b = Seq.newBuilder[((HostID, HostID), Iterable[Segment])]

    replies.foreach { case (segment, trees) =>
      // Note that this will diff distinct combinations, but will
      // _not_ diff all possible permutations of those
      // combinations.
      //
      // Given hosts [A, B, C], this will diff [A, B], [A, C], [B,
      // C], but not [B, A], [C, A], or [C, B].
      trees.toSeq.combinations(2) foreach {
        case Seq((hostA, treeA), (hostB, treeB)) =>
          logger.info(
            s"Validating replication between $hostA and $hostB for $segment...")
          val diff = treeA diff treeB
          if (diff.nonEmpty) {
            logger.info(
              s"${diff.size} replication errors found between $hostA and $hostB for $segment.")
            b += (hostA, hostB) -> diff
          } else {
            logger.info(
              s"No replication errors found between $hostA and $hostB for $segment.")
          }
        case _ => () // Satisfy scalac.
      }
    }

    b.result()
  }

  private def recvValidate(
    cf: String,
    segment: Segment,
    snapTime: Timestamp,
    replyTo: HandlerID,
    deadline: TimeBound,
    maxDepth: Int,
    filterScope: Option[ScopeID]): Future[Unit] = {

    if (!partitioner.isReplicaForSegment(segment, bus.hostID)) {
      logger.warn(s"Received invalid validation request for $cf $segment from ${replyTo.host}.")
      Future.unit
    } else {
      logger.info(s"Received validation request for $cf $segment from ${replyTo.host}.")

      def filterKey(key: ByteBuf): Boolean =
        filterScope forall { _ =>
          Tables.decodeScope(cf, key) == filterScope
        }

      val hashF =
        storage.hash(cf, segment, snapTime, deadline, maxDepth, filterKey)(
          FaunaExecutionContext.Implicits.global)

      implicit val ec = ImmediateExecutionContext
      hashF transformWith {
        case Success(tree) =>
          logger.debug(
            s"Replying to validation request for $cf $segment from ${replyTo.host}.")
          bus.sink(ReplyProtocolV2, replyTo).send(Reply(tree), deadline).unit
        case Failure(ex) =>
          logger.warn(s"Validation request failed: $ex")
          Future.unit
      }
    }
  }
}
