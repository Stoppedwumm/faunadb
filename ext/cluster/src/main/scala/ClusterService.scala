package fauna.cluster

import ClusterService._
import fauna.codex.cbor.CBOR
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.Service
import fauna.logging.ExceptionLogging
import fauna.net.bus.{ MessageBus, SignalID }
import fauna.stats.StatsRecorder
import fauna.tx.consensus.{
  MessageBusTransport,
  ReplicatedLog,
  ReplicatedState,
  ReplicatedStateTransport
}
import fauna.tx.log.SequencedStateCallback
import java.nio.file.Path
import scala.concurrent.Future
import scala.concurrent.duration._

trait ServiceState[C, S <: ServiceState[C, S]] {
  def applyCmd(cmd: C): S
}

case class ClusterServiceConfig(
  bus: MessageBus,
  busMaxPendingMessages: Int,
  tmpDirectory: Path,
  logDir: Path,
  tickPeriod: FiniteDuration,
  txnLogBackupPath: Option[Path])

object ClusterService {
  private[ClusterService] val StartupTimeout = 30.minutes

  case class LogAndState[C, S](log: ReplicatedLog[C], state: ReplicatedState[C, S])

  def apply[C: CBOR.Codec] = new LogAndStateBuilder[C]

  class LogAndStateBuilder[C: CBOR.Codec] {

    def makeLogAndState[S <: ServiceState[C, S]: CBOR.Codec](
      config: ClusterServiceConfig,
      logSID: SignalID,
      stateSID: SignalID,
      logName: String,
      initialState: => S,
      stats: StatsRecorder = StatsRecorder.Null,
      ringLog: Option[ReplicatedLog.Handle] = None,
      dedicatedBusStream: Boolean = false): LogAndState[C, S] = {

      val ClusterServiceConfig(bus,
                               busMaxPendingMessages,
                               tmpDirectory,
                               logDir,
                               tickPeriod,
                               txnLogBackupPath) = config
      val transport = new MessageBusTransport(logName,
                                              bus,
                                              logSID,
                                              busMaxPendingMessages,
                                              statsRecorder = stats,
                                              dedicatedBusStream = dedicatedBusStream)
      val log = ReplicatedLog[C].open(transport,
                                      bus.hostID,
                                      logDir,
                                      logName,
                                      tickPeriod,
                                      stats,
                                      logName,
                                      ringLog,
                                      txnLogBackupPath = txnLogBackupPath)
      val stateTransport = new ReplicatedStateTransport(logName, bus, stateSID, tmpDirectory)
      val state = ReplicatedState(log,
                                  stateTransport,
                                  logDir,
                                  logName,
                                  initialState,
                                  snapshotInterval = 1000) { (_, p, c) =>
        p.applyCmd(c)
      }

      LogAndState(log, state)
    }
  }

  /**
    * Safely subscribe to multiple cluster services. Ensures that callback
    * invocations are sequenced across subscriptions and the subscription
    * ends to all services when the callback returns false.
    */
  def subscribeWithLogging(services: ClusterService[_, _]*)(cb: => Future[Boolean]): Unit = {
    val ssb = SequencedStateCallback(cb)
    services.foreach { _.subscribeWithLogging(ssb()) }
  }
}

abstract class ClusterService[C, S <: ServiceState[C, S]](
  logAndState: LogAndState[C, S])
    extends Service
    with ExceptionLogging {

  @volatile private[this] var running = false

  protected def replog = logAndState.log
  protected def state = logAndState.state

  /**
    * Subscribes to state changes in this service. Executes `cb` if
    * a state change potentially occurred. It is possible for `cb` to be invoked
    * spuriously. Invoking `cb` will continue until it returns false.
    */
  def subscribe(cb: => Future[Boolean]) = state.subscribe(cb)

  /**
    * Same as subscribe() but adds exception logging to the returned future.
    */
  def subscribeWithLogging(cb: => Future[Boolean]) =
    logException(subscribe(cb))

  protected def syncOnStartup = true

  def start(): Unit =
    running = true

  def prepareStop(): Unit =
    replog.abdicate("stopping")

  def stop(graceful: Boolean): Unit = {
    state.close()
    running = false
  }

  def isRunning = running

  protected def currState = state.get

  protected def proposeAndApply(
    cmd: C,
    deadline: Duration = Duration.Inf): Future[Unit] =
    replog.add(cmd, deadline) flatMap { state.sync(_, deadline) }
}
