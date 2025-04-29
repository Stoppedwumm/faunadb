package fauna.tx.test

import fauna.atoms._
import fauna.exec.Timer
import fauna.net.bus._
import fauna.tx.consensus._
import fauna.tx.consensus.messages.Message
import java.io.PrintWriter
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.util.Random

/**
  * An unreliable transport based on HostAddressTransport.
  * @param MessageBus
  * @param Port ID
  * @param underlying the wrapped underlying transport
  * @param lossRate a value between 0.0 and 1.0 representing the ratio of dropped messages
  * @param repeatRate a value between 0.0 and 1.0 representing the ratio of non-dropped messages that will be repeated
  * @param msgDelay base delay in message delivery, in milliseconds
  * @param msgDelayJitter jitter in message delivery delay, in milliseconds
  * @param ec an execution context
  * @tparam I the type of node identifiers
  * @tparam V the type of log values
  */
class UnreliableTransport(
  bus: MessageBus,
  signalID: SignalID,
  lossRate: Double,
  repeatRate: Double,
  msgDelay: Int,
  msgDelayJitter: Int,
  debug: PrintWriter)(implicit ec: ExecutionContext)
    extends MessageBusTransport("unreliable", bus, signalID, 1024) {

  private[this] val random = new Random()
  @volatile private[this] var _suspend = false

  override def send(to: HostID, message: Message, after: FiniteDuration) =
    if (!_suspend && Random.nextDouble() >= lossRate) {
      sendDelayed(to, message, after)
      if (Random.nextDouble() < repeatRate) {
        sendDelayed(to, message, after)
      }
    }

  def suspend() = _suspend = true
  def resume() = _suspend = false

  private def sendDelayed(to: HostID, message: Message, after: FiniteDuration) = {
    val delay = randomDelay().millis + after
    if (delay.length <= 0) {
      sendAndLog(to, message)
    } else {
      Timer.Global.scheduleTimeout(delay) { sendAndLog(to, message) }
    }
  }

  private def sendAndLog(to: HostID, message: Message) = {
    debug.println(s"SEND ${bus.hostID} => $to: $message")
    super.send(to, message, Duration.Zero)
  }

  private def randomDelay() =
    if (msgDelayJitter <= 0) msgDelay else {
      msgDelay - (msgDelayJitter / 2) + random.nextInt(msgDelayJitter)
    }
}
