package fauna.lang

import fauna.lang.clocks.DeadlineClock
import scala.concurrent.duration.FiniteDuration
import scala.language.implicitConversions

trait DurationSyntax {
  implicit def asRichFiniateDuration(l: FiniteDuration): DurationSyntax.RichFiniteDuration =
    DurationSyntax.RichFiniteDuration(l)
}

object DurationSyntax {
  case class RichFiniteDuration(d: FiniteDuration) extends AnyVal {
    def bound(implicit clock: DeadlineClock): TimeBound = TimeBound(d)
  }
}
