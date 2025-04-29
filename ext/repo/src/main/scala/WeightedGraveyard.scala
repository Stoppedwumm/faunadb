package fauna.repo

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.lang.clocks.Clock
import scala.concurrent.duration.FiniteDuration

/** Provided a set of live and recently departed hosts, yields hosts
  * in order proportional to the length of time since their departure.
  *
  * Any hosts in the graveyard which departed earlier than the
  * provided TTL will be filtered from the sequence.
  */
final class WeightedGraveyard(
  live: IndexedSeq[HostID],
  graveyard: Iterable[(HostID, Timestamp)],
  ttl: FiniteDuration)(implicit clock: Clock = Clock)
    extends IndexedSeq[(HostID, Double)] {

  require(live.sizeIs > 0, "at least one host must be alive.")

  private[this] val weighted: IndexedSeq[(HostID, Double)] = {
    val now = clock.time

    // The point beyond which we cease attempting steals from a
    // departed host.
    val horizon = now - ttl

    var max = 0L
    val indexed = graveyard collect {
      case (h, ts) if ts > horizon =>
        val delta = ts.millis - horizon.millis
        max = delta.max(max)
        h -> delta.toDouble
    } map { case (h, delta) =>
      h -> (delta / max)
    } toIndexedSeq

    indexed.sortBy { case (_, w) => w }
  }

  def length: Int = live.size + weighted.size

  def apply(index: Int): (HostID, Double) =
    if (index < weighted.size) {
      weighted(index)
    } else {
      (live(index - weighted.size), 1.0)
    }
}
