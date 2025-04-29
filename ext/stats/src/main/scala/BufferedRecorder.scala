package fauna.stats

import fauna.logging.ExceptionLogging
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.{ ConcurrentHashMap, ConcurrentLinkedDeque, ThreadLocalRandom }
import scala.concurrent.duration._

class BufferedRecorder(
  interval: FiniteDuration,
  timingsPerSecond: Int,
  recorder: StatsRecorder
) extends StatsRecorder
    with ExceptionLogging {

  private final class LongAdderMap[K] {
    private[this] val map = new ConcurrentHashMap[K, LongAdder]

    def count(key: K, value: Long) =
      map.computeIfAbsent(key, _ => new LongAdder).add(value)

    def foreach(f: (K, Long) => Unit) =
      map forEach { (k, v) =>
        f(k, v.sumThenReset)
      }
  }

  private final class ValueMap[V] {
    private[this] val map = new ConcurrentHashMap[String, V]

    def set(key: String, value: V) = map.put(key, value)

    def foreach(f: (String, V) => Unit) = {
      val iter = map.entrySet.iterator

      while (iter.hasNext) {
        val e = iter.next
        f(e.getKey, e.getValue)
        iter.remove()
      }
    }
  }

  private type StatEvent = (StatLevel, String, String, StatTags)

  private val counts = new LongAdderMap[String]
  private val countsTagged = new LongAdderMap[(String, StatTags)]
  private val doubles = new ValueMap[Double]
  private val strings = new ValueMap[String]
  private val events = new ConcurrentLinkedDeque[StatEvent]()

  private val timingsPerInterval = (interval * timingsPerSecond).toSeconds.toDouble max 1.0
  private val timingCounts = new LongAdderMap[String]
  private val timingSampleRates = new ConcurrentHashMap[String, Double]

  def incr(key: String) = count(key, 1)

  def decr(key: String) = count(key, -1)

  def count(key: String, value: Long, tags: StatTags = StatTags.Empty): Unit =
    if (tags.isEmpty) {
      counts.count(key, value)
    } else {
      countsTagged.count(key -> tags, value)
    }

  def set(key: String, value: Double): Unit = doubles.set(key, value)

  def set(key: String, value: String): Unit = strings.set(key, value)

  def timing(key: String, value: Long): Unit = {
    // record the fact we emitted a timing.
    // FIXME: These could themselves be emitted as a stat, but that's
    // out of scope
    timingCounts.count(key, 1)

    // Get the current sample rate for the key, or default to emitting all.
    val rate = timingSampleRates.getOrDefault(key, 1.0)
    val rnd = ThreadLocalRandom.current.nextDouble

    if (rnd < rate) {
      recorder.timing(key, value)
    }
  }

  def distribution(key: String, value: Long, tags: StatTags = StatTags.Empty): Unit =
    // NB. distributions are not aggregated per-process, so they must
    // be passed straight through, unbuffered.
    recorder.distribution(key, value, tags)

  def event(level: StatLevel, title: String, text: String, tags: StatTags): Unit =
    events.add((level, title, text, tags))

  StatsRecorder.polling(interval) {
    counts foreach { (k, v) =>
      if (v != 0) recorder.count(k, v.toInt)
    }
    countsTagged foreach { case ((k, tags), v) =>
      if (v != 0) recorder.count(k, v.toInt, tags)
    }
    doubles foreach { (k, v) =>
      recorder.set(k, v)
    }
    strings foreach { (k, v) =>
      recorder.set(k, v)
    }

    var size = events.size
    var e = events.poll
    while (size > 0 && (e ne null)) {
      val (level, title, text, tags) = e
      recorder.event(level, title, text, tags)
      size -= 1
      e = events.poll
    }

    // recalculate timing sample rates
    timingCounts foreach { (k, v) =>
      // calculate the rate based on the total number of timings
      // emitted by code.
      val rate = timingsPerInterval / (v max 1)

      if (rate >= 1.0) {
        // since 1.0 is the default, no need to keep it around.
        timingSampleRates.remove(k)
      } else {
        // if we need to limit sending timings downstream, set the rate.
        timingSampleRates.put(k, rate)
      }
    }
  }
}
