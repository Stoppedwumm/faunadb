package fauna.repo

import fauna.atoms.{ HostID, ScopeID }
import fauna.codex.cbor._
import fauna.lang.clocks.DeadlineClock
import fauna.lang.syntax._
import fauna.lang.TimeBound
import fauna.net.bus._
import fauna.repo.service.{ RestartService, RestoreStatusService }
import fauna.storage.CompressedTransfer
import io.netty.channel.unix.Errors$NativeIoException
import java.util.concurrent.{ ConcurrentHashMap, TimeoutException }
import java.util.UUID
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.control.NoStackTrace

final class RestoreFailedException(message: String)
    extends Exception(message)
    with NoStackTrace

final class ReceiverRestartedException(message: String)
    extends Exception(message)
    with NoStackTrace

object Restore {

  val logger = getLogger()

  final case class RestoreRequest(
    scope: ScopeID,
    cf: String,
    manifest: FileTransferManifest,
    session: UUID,
    replyTo: SignalID)

  sealed abstract class RestoreReply
  final case object RestoreSuccess extends RestoreReply
  final case class RestoreFailure(message: String) extends RestoreReply

  implicit val RestoreRequestCodec = CBOR.TupleCodec[RestoreRequest]
  implicit val RestoreRequestProtocol = Protocol[RestoreRequest]("restore.request")

  implicit val RestoreReplyCodec = CBOR.SumCodec[RestoreReply](
    CBOR.SingletonCodec(RestoreSuccess),
    CBOR.RecordCodec[RestoreFailure])

  implicit val RestoreReplyProtocol =
    Protocol.Reply[RestoreRequest, RestoreReply]("restore.reply")

  trait Transfer[T <: Chunked] {
    def session: UUID
    def context: FileTransferContext

    /** Sends a set of transfers to another host.
      *
      * All transfers must remain open until the returned future is
      * completed.
      *
      * The caller is responsible for closing the Source, if the
      * returned future completes successfully.
      */
    def send(
      columnFamily: String,
      dest: HostID,
      transfers: Seq[T],
      restart: Promise[Unit])(
      implicit ec: ExecutionContext): Future[Source[RestoreReply]]
  }

  final class CompressedBusTransfer(
    val session: UUID,
    signalID: SignalID,
    bus: MessageBus,
    val context: FileTransferContext)
      extends Transfer[CompressedTransfer] {

    def send(
      columnFamily: String,
      dest: HostID,
      transfers: Seq[CompressedTransfer],
      restart: Promise[Unit])(
      implicit ec: ExecutionContext): Future[Source[RestoreReply]] = {
      val handle = context.prepare(
        bus,
        transfers map { t => t.filename -> t } toMap,
        autoclose = false)

      // Setup the source for load completions and the restart
      // listener before sending the restore request, just in case
      // either event happens closely following the request.
      val src = bus.tempSource(RestoreReplyProtocol)

      restart.future.onComplete { _ =>
        logger.info(
          s"Transfer [$session]: Restore of $columnFamily to $dest failed. " +
            s"$dest restarted during transfer.")

        handle.close(success = false)
      }

      val msg = RestoreRequest(
        ScopeID.RootID,
        columnFamily,
        handle.manifest,
        session,
        src.id.signalID)

      bus.sink(RestoreRequestProtocol, signalID.at(dest)).send(msg)

      handle.future map {
        // Transfer complete. Hand back the source to listen for load
        // completion.
        case true => src

        case false =>
          // The transfer failed; treat all failures uniformly as
          // timeouts, because we can't know how exactly the network
          // transfer failed.
          src.close()

          throw new TimeoutException(
            s"Transfer [$session]: Restore of $columnFamily to $dest timed out.")

      } ensure {
        handle.close()
      }
    }
  }
}

final class Restore[T <: Chunked](
  /** Used to report status back to the RestoreStatusService for the restore
    */
  restoreID: UUID,
  columnFamily: String,
  streamPlans: Map[HostID, Seq[T]],
  restartService: RestartService,
  transfer: Restore.Transfer[T],
  deadline: TimeBound,
  restoreStatusService: RestoreStatusService
)(implicit clock: DeadlineClock) {

  import Restore._
  import RestoreStatusService._

  // Ongoing transfers will register a promise here, which will be
  // completed if the FD observes the host restart.
  private[this] val restarts = new ConcurrentHashMap[HostID, Promise[Unit]]

  def run()(implicit ec: ExecutionContext): Future[Unit] = {
    val hostsStr = streamPlans.keysIterator mkString ", "

    logger.info(s"Transfer [${transfer.session}] " +
      s"Initiating restore of $columnFamily to ${streamPlans.size} hosts. ($hostsStr)")

    val handle = restartService.subscribeStartsAndRestarts { host =>
      restarts.computeIfPresent(
        host,
        { case (_, p) =>
          p.trySuccess(())
          null // Remove the mapping.
        })
    }

    val streams = streamPlans map { case (host, transfers) =>
      @volatile var retries = 0
      def loop(): Future[Unit] = {
        val restart = restarts.compute(
          host,
          {
            // If there is no restart promise, or it was completed
            // (i.e. we saw a restart), register the new
            // promise. Otherwise, keep the old promise and re-use
            // it. This allows for multiple restart-retry pairs during
            // the same restore session.
            case (_, p) if p eq null     => Promise[Unit]()
            case (_, p) if p.isCompleted => Promise[Unit]()
            case (_, p)                  => p
          }
        )

        restoreStatusService.updateRestoreState(
          restoreID,
          host,
          ColumnFamilyStatus(columnFamily, "InProgress", retries))
        transfer.send(columnFamily, host, transfers, restart) flatMap { src =>
          logger.info(
            s"Transfer [${transfer.session}] complete of $columnFamily to $host")
          // Transfer complete. Wait to hear back on the load into storage.
          // If we see a restart before we get a message back from the source,
          // we want to retry the transfer as the remote host won't continue the
          // load.
          val restartF: Future[Unit] =
            restart.future.flatMap { _ =>
              Future.failed(
                new ReceiverRestartedException(
                  s"host $host restart received during data load."
                ))
            }
          val srcF: Future[Unit] =
            src.subscribe(deadline.timeLeft) {
              case Source.Messages(Seq(msg)) =>
                msg match {
                  case Source.Message(_, _, RestoreSuccess) =>
                    logger.info(
                      s"Transfer [${transfer.session}]: Restore of $columnFamily to $host complete.")
                    restoreStatusService.updateRestoreState(
                      restoreID,
                      host,
                      ColumnFamilyStatus(columnFamily, "Complete", retries))
                    Future.successful(Some(()))

                  case Source.Message(_, _, RestoreFailure(msg)) =>
                    logger.info(
                      s"Transfer [${transfer.session}]: Restore of $columnFamily to $host failed ($msg)")
                    Future.failed(new RestoreFailedException(msg))
                }

              case Source.Messages(msgs) =>
                throw new IllegalStateException(
                  s"Transfer [${transfer.session}]: Expected a single message got $msgs.")

              case Source.Idle =>
                val msg = s"Transfer [${transfer.session}]: " +
                  s"Restore of $columnFamily to $host timed out. Load failed."

                Future.failed(new TimeoutException(msg))

              case Source.Done => Future.successful(Some(()))
            } map { ret =>
              transfers foreach { _.close() }
              ret
            }

          Future.firstCompletedOf(Seq(srcF, restartF)).ensure {
            restarts.remove(host)
            src.close()
          }
        } recoverWith {
          /** When restarts happen during live restores, we have seen the following error come through:
            * io.netty.channel.unix.Errors$NativeIoException: recvAddress(..) failed: Connection reset by peer
            * Because it wasn't previously captured in this list it would cause the restore to fail. In order to
            * account for that we have added the NativeIoException to our restart list.
            */
          case e @ (_: TimeoutException | _: ReceiverRestartedException |
              _: Errors$NativeIoException) if deadline.hasTimeLeft =>
            // NB. The loop header will register a new restart
            // promise, if this retry was caused by a restart.
            retries += 1
            restoreStatusService.updateRestoreState(
              restoreID,
              host,
              ColumnFamilyStatus(columnFamily, "InProgress", retries))
            logger.info(s"Transfer [${transfer.session}]: " +
              s"Restore of $columnFamily to $host failed, ${e.getMessage}. Retrying. Retry attempt $retries")
            transfers foreach { _.reset() }
            loop()

          case ex =>
            transfers foreach { _.close() }
            restarts.remove(host)
            restoreStatusService.updateRestoreState(
              restoreID,
              host,
              ColumnFamilyStatus(columnFamily, "Failed", retries))
            logger.error(s"Restore of $columnFamily to $host failed", ex)
            Future.failed(ex)
        }
      }

      loop()
    }

    streams.join ensure {
      restartService.unsubscribeStartsAndRestarts(handle)
    }
  }
}
