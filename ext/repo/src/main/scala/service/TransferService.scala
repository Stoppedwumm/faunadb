package fauna.repo.service

import fauna.atoms._
import fauna.codex.cbor._
import fauna.exec._
import fauna.lang._
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.net.RateLimiter
import fauna.net.bus._
import fauna.stats.StatsRecorder
import fauna.storage.StorageEngine
import fauna.tx.transaction.Partitioner
import java.io.Closeable
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.TimeoutException
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try
import scala.util.control.NoStackTrace

class TransferFailed(msg: String) extends Exception(msg)

object TransferService {
  case class Request(
    scope: ScopeID,
    cf: String,
    segments: Vector[Segment],
    snapTime: Timestamp,
    replyTo: SignalID,
    session: Option[UUID] = None)

  sealed trait Reply

  case class Result(
    scope: ScopeID,
    cf: String,
    segments: Vector[Segment],
    manifest: Option[FileTransferManifest])
      extends Reply

  case object UnsupportedOperation extends Reply

  implicit val RequestCodec = CBOR.RecordCodec[Request]
  implicit val ReplyCodec = CBOR.SumCodec[Reply](
    CBOR.RecordCodec[Result],
    CBOR.SingletonCodec(UnsupportedOperation)
  )

  private val logger = getLogger()
}

final class UnsupportedOperationException extends NoStackTrace

class TransferService(
  signalID: SignalID,
  bus: MessageBus,
  partitioner: => Partitioner[_, _],
  storage: StorageEngine,
  tmpDirectory: Path,
  chunkSize: Int,
  enableTransferDigests: Boolean,
  planThreads: Int,
  limiter: RateLimiter,
  stats: StatsRecorder)
    extends Closeable
    with ExceptionLogging {

  import TransferService._

  private val RequestProtocol = Protocol[Request]("transfer.request.v2")(RequestCodec)
  private val ReplyProtocol = Protocol.Reply[Request, Reply]("transfer.reply.v2")

  private val handler = bus.handler(RequestProtocol, signalID) {
    case (from, Request(scope, cf, seg, ts, replyTo, session), deadline) =>
      implicit val ec = ImmediateExecutionContext

      require(scope == ScopeID.RootID)

      val sess = session match {
        case Some(id) => id
        case None =>
          val uuid = UUID.randomUUID()
          logger.info(
            s"Transfer [$uuid]: Received transfer request without a session ID. " +
              "Starting new session.")
          uuid
      }

      handleTransferRequest(sess, cf, seg, ts, from.id, deadline, planThreads) flatMap {
        case (res, cleanup) =>
          bus
            .sink(ReplyProtocol, replyTo.at(from.id))
            .send(res, deadline) transformWith cleanup
      }
  }

  def close() = {
    handler.close()
  }

  /**
    * Request segments of a column family from `source`. The source
    * will wait until its Last Applied Timestamp is at least
    * `snapTime` prior to sending any files. Once the transfer is
    * received, the local process will load the data into storage.
    *
    * Not all requested segments are guaranteed to be transfered - for
    * instance, if the source and requesting processes disagree on
    * their topology - the list of successfully-transferred segments
    * will be returned.
    */
  def request(
    source: HostID,
    cf: Option[String],
    segments: Vector[Segment],
    snapTime: Timestamp,
    deadline: TimeBound)(implicit ec: ExecutionContext): Future[Vector[Segment]] = {

    val session = UUID.randomUUID()
    val data = cf map { cf => s" of $cf" } getOrElse ""
    logger.info(
      s"Transfer [$session]: Beginning transfer request for $segments$data from $source.")

    val (sink, src) =
        (bus.sink(RequestProtocol, signalID.at(source)), bus.tempSource(ReplyProtocol))
    requestSegments(sink,
                    src,
                    session,
                    cf,
                    segments,
                    snapTime,
                    deadline)
  }

  private def requestSegments(sink: SinkCtx[Protocol[Request]],
                              src: Source[Reply],
                              session: UUID,
                              cf: Option[String],
                              segments: Vector[Segment],
                              snapTime: Timestamp,
                              deadline: TimeBound
                             )(implicit ec: ExecutionContext): Future[Vector[Segment]] = {

    logger.debug(s"requestSegments(${sink.dest} $src, $cf, $segments, $snapTime, $deadline)")

    // TODO: send session in Request
    val msgs = cf match {
      case Some(cf) => Seq(Request(ScopeID.RootID, cf, segments, snapTime, src.id.signalID))
      case None     =>
        storage.columnFamilyNames map { name =>
          Request(ScopeID.RootID, name, segments, snapTime, src.id.signalID)
        }
    }

    msgs foreach { msg => sink.send(msg, deadline) }

    src.fold((0, Seq.newBuilder[Vector[Segment]]), deadline.timeLeft) {
      case ((progress, results), Source.Messages(rs)) =>
        val fs = rs map {
          case Source.Message(from, _, Result(_, cf, segments, manifest)) =>
            results += segments
            manifest match {
              case Some(manifest) =>
                logger.info(s"Received transfer manifest for $cf $segments from ${from.id}. Beginning transfer...")

                val ctx = FileTransferContext(tmpDirectory,
                                              chunkSize = chunkSize,
                                              transferExpiry = deadline.timeLeft,
                                              enableDigests = enableTransferDigests,
                                              limiter = limiter,
                                              statsRecorder = stats,
                                              statsPrefix = "SegmentTransfer")
                receiveTransfer(session, cf, manifest, ctx, deadline) map { _ =>
                  logger.info(
                    s"Transfer [$session] $cf $segments from ${from.id} complete.")
                  stats.incr("Transfer.Complete")
                }
              case None =>
                logger.info(s"Transfer [$session]: " +
                  s"Received no transfer manifest for $cf $segments from ${from.id}. " +
                  "Nothing to transfer.")
                Future.unit
            }
          case Source.Message(_, _, UnsupportedOperation) =>
            logger.debug(
              s"Transfer [$session]: Received UnsupportedOperation reply, raising error to trigger fallback path")
            Future.failed(new UnsupportedOperationException())
        }

        Future.sequence(fs) map { _ =>
          val n = rs.size + progress
          if (n == msgs.size) {
            Right(results.result().fold(segments) { (a, b) => a filter (b contains) })
          } else {
            Left((n, results))
          }
        }
      case _ =>
        stats.incr("Transfer.Timeout")
        val data = cf map { cf => s" of $cf" } getOrElse ""
        val msg = s"Transfer [$session]: " +
          s"timed out requesting segments $segments$data from ${sink.dest.host}"
        Future.failed(new TimeoutException(msg))
    }
  }

  private def receiveTransfer(
    session: UUID,
    columnFamily: String,
    manifest: FileTransferManifest,
    ctx: FileTransferContext,
    deadline: TimeBound): Future[Unit] = {

    implicit val ec = FaunaExecutionContext.Implicits.global
    ctx.receive(bus, manifest) { tmpdir =>
      storage.receiveTransfer(session, columnFamily, tmpdir, deadline)
    }
  }

  private def handleTransferRequest(session: UUID,
                                    cf: String,
                                    segments: Vector[Segment],
                                    snapTime: Timestamp,
                                    host: HostID,
                                    deadline: TimeBound,
                                    threads: Int
                                   ): Future[(Reply, Try[Boolean] => Future[Unit])] = {

    val (owns, errs) =
      segments partition { seg => partitioner.isReplicaForSegment(seg, bus.hostID) }

    if (errs.nonEmpty) {
      logger.warn(
        s"Transfer [$session]: Received invalid transfer request for $errs from $host.")
    }

    if (owns.nonEmpty) {
      val ctx = FileTransferContext(
        tmpDirectory,
        transferExpiry = deadline.timeLeft,
        statsRecorder = stats,
        statsPrefix = "SegmentTransfer")

      logger.info(
        s"Transfer [$session]: Received transfer request for $cf $owns from $host. " +
          "Preparing manifest...")
      val manF = stats.timeFuture("Transfer.Prepare.Time") {
        storage.prepareTransfer(cf, owns, snapTime, ctx, bus, deadline, threads)(
          FaunaExecutionContext.Implicits.global)
      }

      implicit val ec = ImmediateExecutionContext
      manF map { manifest => (Result(ScopeID.RootID, cf, owns, manifest), { t =>
        // The protocol is for either:
        // 1) Reply is sent and FileTransferContext will close()
        // 2) Reply is not sent and manifest must close() here
        if (!t.toOption.contains(true)) {
          manifest foreach {
            ctx.close(bus, _)
          }
        }
        Future.unit
      }) }
    } else {
      stats.incr("Transfer.Failure")

      logger.error(
        s"Transfer [$session]: Received invalid transfer request for $segments from $host")

      Future.successful((Result(ScopeID.RootID, cf, owns, None), { _ => Future.unit }))
    }
  }
}
