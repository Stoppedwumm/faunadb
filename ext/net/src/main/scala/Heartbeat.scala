package fauna.net

import fauna.lang.{ NamedPoolThreadFactory, Service }
import fauna.logging.ExceptionLogging
import java.util.concurrent.ScheduledThreadPoolExecutor
import scala.concurrent.duration._
import scala.util.control.NonFatal

object Heartbeat {
  def apply(
    tick: FiniteDuration,
    name: String = "")(
    listener: => Unit) = {
    val nameOpt = Some(name) filterNot { _.isEmpty }
    new Heartbeat(tick, nameOpt, listener)
  }
}

class Heartbeat(
  tick: Duration,
  namePrefix: Option[String],
  listener: => Unit)
    extends Service with ExceptionLogging {

  private[this] val name = Seq(namePrefix, Some("Heartbeat")).flatten.mkString("-")
  private[this] var executor: ScheduledThreadPoolExecutor = null

  private[this] val beat = new Runnable {
    def run =
      try {
        listener
      } catch {
        case NonFatal(e) => logException(e)
      }
  }

  start()

  // Satisfy Service

  def isRunning = synchronized { executor ne null }

  def start(): Unit =
    synchronized {
      if (!isRunning) {
        executor = new ScheduledThreadPoolExecutor(1, new NamedPoolThreadFactory(name, true))
        executor.setRemoveOnCancelPolicy(true)
        executor.scheduleAtFixedRate(beat, 0, tick.length, tick.unit)
      }
    }

  def stop(graceful: Boolean): Unit =
    synchronized {
      if (isRunning) {
        executor.shutdownNow()
        executor = null
      }
    }
}
