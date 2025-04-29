package fauna.exec

import fauna.lang.syntax._
import fauna.lang.{ Local, Service }
import fauna.logging.ExceptionLogging
import java.util.concurrent.TimeoutException
import scala.util.control.NonFatal

abstract class ThreadService(val name: String, priority: Int = Thread.NORM_PRIORITY)
    extends Service
    with ExceptionLogging {

  @volatile protected var _continue = false

  private val thread = new Thread(() => run(), name)

  thread.setDaemon(true)
  thread.setPriority(priority)
  thread.start

  protected def maybePause(): Unit =
    synchronized {
      while (!_continue) {
        wait()
      }
    }

  protected def run(): Unit

  def isRunning: Boolean = _continue

  def start(): Unit = {
    getLogger.info(s"Starting service $name")
    synchronized {
      if (!_continue) {
        _continue = true
        notify()
      }
    }
  }

  def stop(graceful: Boolean): Unit = {
    getLogger.info(s"Stopping service $name")
    _continue = false
    thread.interrupt()
  }
}

abstract class LoopThreadService(name: String, priority: Int = Thread.NORM_PRIORITY)
    extends ThreadService(name, priority) {

  protected def loop(): Unit

  protected def run(): Unit =
    while (true) {
      try {
        maybePause()

        // reset thread-local context before each iteration
        Local.clear()

        loop()
      } catch {
        case _: TimeoutException     => ()
        case _: InterruptedException => ()
        case e: Throwable =>
          logException(e)
          if (!NonFatal(e)) throw e
      }
    }
}
