package fauna.lang

import java.lang.management.ManagementFactory
import org.apache.logging.log4j.LogManager
import scala.concurrent.duration._

object Timing {
  @inline final def start = Timing(System.nanoTime)
}

final case class Timing(startNanos: Long) extends AnyVal {
  def elapsedNanos: Long = System.nanoTime - startNanos

  def elapsedMicros: Long = elapsedNanos / 1_000

  def elapsedMillis: Long = elapsedNanos / 1_000_000

  def elapsedSeconds: Long = elapsedMillis / 1_000

  def elapsed: FiniteDuration = elapsedNanos.nanos
}

object CPUTiming {

  def apply() = new CPUTiming

  // getThreadMXBean looks up a singleton through some reflection shenanigans, so we
  // want to cache this. "bean" is adorable.
  private[CPUTiming] val bean = ManagementFactory.getThreadMXBean

  // probe to see if CPU measurement is available
  val isCPUTimeAvailable = {
    if (bean.isThreadCpuTimeSupported) {
      bean.setThreadCpuTimeEnabled(true)
      true
    } else {
      // unfortunately, can't use our logging helpers here since it's the same
      // project
      val log = LogManager.getLogger(this.getClass.getName)
      log.warn(
        "CPU time tracking is not available. Query Eval timing metrics will not be accurate.")

      false
    }
  }
}

final class CPUTiming {
  private[this] var _nanos = 0L

  final def measure[T](f: => T): T = {
    if (!CPUTiming.isCPUTimeAvailable) {
      return f
    }

    val tid = Thread.currentThread.getId
    val start = CPUTiming.bean.getThreadCpuTime(tid)

    try {
      f
    } finally {
      _nanos += (CPUTiming.bean.getThreadCpuTime(tid) - start)
    }
  }

  def elapsedNanos: Long = _nanos

  def elapsedMicros: Long = elapsedNanos / 1_000

  def elapsedMillis: Long = elapsedNanos / 1_000_000

  def elapsed: FiniteDuration = elapsedNanos.nanos
}
