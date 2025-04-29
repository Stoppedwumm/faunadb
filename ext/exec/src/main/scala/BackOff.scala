package fauna.exec

import fauna.lang.Timing
import fauna.lang.syntax._
import scala.concurrent.duration._

// Controls an exponential sleep, beginning at init and not to exceed
// limit.
final class BackOff(init: FiniteDuration, limit: FiniteDuration) {
  private[this] val Multiple = 2
  private[this] val _init = init.max(1.millisecond)
  private[this] val _lim = limit.max(init)
  private[this] val log = getLogger

  @volatile private[this] var cur = _init

  def currentDuration: FiniteDuration = cur

  /**
    * Execute backoff intervals while `check` returns false, returning
    * once `check` returns true.
    *
    * `wakeup()` preempts the backoff interval, to cause a `check`
    * before a backoff interval is complete.
    */
  def apply(check: Boolean => BackOff.Operation): Unit = {
    var wakeup = false

    while (check(wakeup).isWait) {
      synchronized {
        try {
          val t = Timing.start

          log.debug(s"sleeping for up to $cur")

          this.wait(cur.toMillis)

          if (t.elapsed < cur) {
            wakeup = true
            log.debug(s"wakeup received after ${t.elapsed.toCoarsest} ($cur)")
          } else {
            wakeup = false
            log.debug(s"wakeup after ${t.elapsed.toCoarsest} ($cur)")
          }

          cur = _lim.min(cur * Multiple)
        } catch {
          case _: InterruptedException => ()
        }
      }
    }

    // reset the backoff interval after check() passes
    cur = _init
  }

  /** Notify any waiting threads in `backOff.apply()` that work may now be
    * available.
    */
  def wakeup(): Unit = synchronized {
    this.notifyAll()
  }
}

object BackOff {
  object Operation {
    @inline def breakIf(cond: Boolean): Operation = if (cond) Break else Wait
    @inline def waitIf(cond: Boolean): Operation = if (cond) Wait else Break

    final case object Break extends Operation {
      def isBreak: Boolean = true
    }
    final case object Wait extends Operation {
      def isBreak: Boolean = false
    }
  }

  sealed trait Operation {
    def isBreak: Boolean
    final def isWait: Boolean = !isBreak
  }
}
