package fauna.repo.service

import fauna.atoms._
import fauna.codex.cbor._
import fauna.exec._
import fauna.lang.syntax._
import fauna.lang.{ Service, TimeBound }
import fauna.logging.ExceptionLogging
import fauna.net.bus._
import fauna.repo.Executor
import fauna.repo.service.stream.TxnResult
import fauna.stats.StatsRecorder
import fauna.storage.doc.FieldType
import fauna.storage.index.NativeIndexID
import java.util.concurrent.TimeoutException
import scala.concurrent.{ ExecutionContext, Future, Promise }

object TaskService {
  sealed trait Request

  /** Represents a steal attempt, which is requesting any of `ids` to be sent to `dest`.
    */
  case class Attempt(dest: HostID, ids: Vector[TaskID], replyTo: SignalID) extends Request

  sealed trait Reply
  case class Confirm(ids: Vector[TaskID]) extends Reply

  implicit val RequestCodec = CBOR.SumCodec[Request](
    CBOR.RecordCodec[Attempt])

  implicit val ReplyCodec = CBOR.SumCodec[Reply](
    CBOR.RecordCodec[Confirm])
}

class TaskService(
  signal: SignalID,
  bus: MessageBus,
  executor: Executor,
  streamService: StreamService,
  stats: StatsRecorder)
    extends Service
    with ExceptionLogging {

  import TaskService._

  private[this] val RequestProtocol = Protocol[Request]("task.request")
  private[this] val ReplyProtocol = Protocol.Reply[Request, Reply]("task.reply")

  private[this] val handler = bus.handler(RequestProtocol, signal) {
    case (from, Attempt(dest, ids, replyTo), deadline) =>
      recvSteal(dest, ids, replyTo.at(from.id), deadline)
  }

  private[this] val log = getLogger

  private[this] val terms = {
    // XXX: these should never equal None but I feel bad
    val host = FieldType.HostIDT.encode(bus.hostID).get
    val isRunnable = FieldType.BooleanT.encode(true).get

    Vector(host, isRunnable)
  }

  @volatile private[this] var _stream: Cancelable = _

  def isRunning: Boolean = _stream ne null

  private def subscribe(): Unit = {
    require(_stream eq null)

    implicit val ec = FaunaExecutionContext.Implicits.global
    val idx = NativeIndexID.PrioritizedTasksByCreatedAt
    val obs = streamService.forMatch(ScopeID.RootID, idx, terms)

    _stream = obs.subscribe(new Observer.Default[TxnResult] {
      override def onNext(txnRes: TxnResult) = {
        stats.incr("TaskExecutor.Wakeup")
        executor.wakeup(txnRes.txnTS)
        Observer.ContinueF
      }

      override def onError(err: Throwable) =
        err match {
          case Cancelable.Canceled => () // closing... ignore.
          case other               => logException(other)
        }
    })
  }

  def start(): Unit = subscribe()

  def stop(graceful: Boolean) = {
    handler.close()
    if (_stream ne null) {
      _stream.cancel()
      _stream = null
    }
  }

  /**
    * Attempts to steal the tasks given by `ids` from the host given
    * by `src` within the `deadline`. A task is stolen - i.e. moved
    * to this host's runq - if all of the following conditions are
    * met:
    *
    * - the task is currently in `src`'s runq
    * - the task is not currently running
    * - the task is runnable
    * - the task is stealable (i.e. not a cache shootdown)
    *
    * Returns the list of successfully stolen tasks.
    */
  def attemptSteal(ids: Seq[TaskID], src: HostID, deadline: TimeBound)(
    implicit ec: ExecutionContext): Future[Seq[TaskID]] =
    if (ids.nonEmpty) { // defense against pointless requests
      log.debug(s"Sending steal request to $src for $ids.")

      val result = Promise[Seq[TaskID]]()

      val request = bus.tempHandler(ReplyProtocol, deadline.timeLeft)({
        case (_, Confirm(ids), _) =>
          result.trySuccess(ids)
          Future.unit
      }, {
        val msg = s"Timed out attempting to steal $ids from $src within $deadline."
        result.tryFailure(new TimeoutException(msg))
        Future.unit
      })

      val msg = Attempt(bus.hostID, ids.toVector, request.id.signalID)
      bus.sink(RequestProtocol, signal.at(src)).send(msg, deadline).unit

      result.future ensure {
        request.close()
      }
    } else {
      log.warn(s"Attempted to steal nothing from $src.")
      Future.successful(Seq.empty)
    }

  private def recvSteal(dest: HostID, ids: Vector[TaskID], replyTo: HandlerID, deadline: TimeBound): Future[Unit] = {
    implicit val ec = ImmediateExecutionContext

    val stolen = executor.stealer.recvSteal(dest, ids, deadline)

    bus.sink(ReplyProtocol, replyTo).send(Confirm(stolen), deadline).unit
  }
}
