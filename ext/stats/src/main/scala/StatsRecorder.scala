package fauna.stats

import fauna.lang._
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import java.util.concurrent.Executors
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.control.NonFatal

final case class StatTags(tags: Set[(String, String)]) {
  import StatTags.RegexPattern

  require(tags forall { case (k, _) => RegexPattern.matches(k) })

  val str = tags.iterator map { case (k, v) => s"$k:$v" } mkString (",")

  val isEmpty = tags.isEmpty

  override def equals(other: Any): Boolean =
    other match {
      case o: StatTags if (o eq this) => true
      case o: StatTags                => tags equals o.tags
      case _                          => false
    }
}

object StatTags {
  val Empty = StatTags(Set.empty[(String, String)])

  /** Tags:
    *  - MUST start with a letter
    *  - MAY contain alphanumerics, underscores, minuses, colons,
    *    periods, slashes thereafter
    *  - MUST NOT end with a colon
    *
    * All tags will be converted to lowercase, and may be up to 200
    * characters long.
    */
  val RegexPattern = raw"\A[\p{Alpha}]+[\p{Alnum}_\-:.\\/]*[\p{Alnum}_\-.\\/]".r

  // Convenience conversion for passing a single "foo" -> "bar" tag to
  // a metric.
  implicit def tagsToST(kv: (String, String)) = StatTags(Set(kv))
}

sealed abstract class StatLevel(val str: String)

object StatLevel {
  case object Info extends StatLevel("info")
  case object Warning extends StatLevel("warning")
  case object Error extends StatLevel("error")
}

trait StatsRecorder {

  /** Counts represent the total number of occurrences of an event
    * during the sample interval, such as the number of connections
    * opened or the number of queries processed.
    *
    * This is _not_ a rate, which is normalized per second over the
    * length of the sample interval.
    */
  def count(key: String, value: Long, tags: StatTags = StatTags.Empty): Unit

  /** Equivalent to count(key, 1).
    */
  def incr(key: String): Unit

  /** Equivalent to count(key, -1).
    */
  def decr(key: String): Unit

  /** Set creates a snapshot of occurrences of an event during the
    * sample interval. The representative value is typically the last
    * value set during the sample interval. Set is used for continuous
    * values such as memory utilization or available disk space.
    */
  def set(key: String, value: Double): Unit
  def set(key: String, value: String): Unit

  final def set(key: String, value: Long): Unit = set(key, value.toDouble)

  /** Timings represent a distribution of values within a sample
    * interval, computed locally by each process. Timings are used for
    * latency or cardinality measurements when local distributions are
    * sufficient.
    */
  def timing(key: String, value: Long): Unit

  /** Distributions represent a distribution (hah) of values within a
    * sample interval, computed globally across all processes by the
    * APM. Distributions are used when the data represents a logical
    * metric, independent of the physical infrastructure, such as a
    * Service Level Objective.
    */
  def distribution(key: String, value: Long, tags: StatTags = StatTags.Empty): Unit

  /** Events are records of notable activity, and are useful for
    * correlating activities with metrics, similar to log lines.
    */
  def event(level: StatLevel, title: String, text: String, tags: StatTags): Unit

  /** Equivalent to event(Info...).
    */
  def info(title: String, text: String, tags: StatTags): Unit =
    event(StatLevel.Info, title, text, tags)
  def info(title: String, text: String): Unit = info(title, text, StatTags.Empty)

  /** Equivalent to event(Warning...).
    */
  def warning(title: String, text: String, tags: StatTags): Unit =
    event(StatLevel.Warning, title, text, tags)

  def warning(title: String, text: String): Unit =
    warning(title, text, StatTags.Empty)

  /** Equivalent to event(Error...).
    */
  def error(title: String, text: String, tags: StatTags): Unit =
    event(StatLevel.Error, title, text, tags)
  def error(title: String, text: String): Unit = error(title, text, StatTags.Empty)

  def time[T](key: String, keys: String*)(f: => T): T = {
    val t = Timing.start
    val rv = f
    val elapsedMillis = t.elapsedMillis
    timing(key, elapsedMillis)
    keys foreach { timing(_, elapsedMillis) }
    rv
  }

  def timeFuture[T](key: String)(f: => Future[T]): Future[T] = {
    implicit val ec = ExecutionContext.parasitic // XXX: can't access IEC
    val t = Timing.start
    GuardFuture(f) ensure {
      timing(key, t.elapsedMillis)
    }
  }

  def scoped(prefix: String): StatsRecorder =
    new ScopedRecorder(this, prefix)

  def filtered(whitelist: Set[String]): StatsRecorder =
    new FilteredRecorder(this, whitelist)

  override def toString: String =
    s"StatsRecorder"

  class ScopedRecorder(
    self: StatsRecorder,
    scope: String
  ) extends StatsRecorder {
    def prefixed(key: String): String = s"$scope.$key"

    def count(key: String, value: Long, tags: StatTags = StatTags.Empty): Unit =
      self.count(prefixed(key), value, tags)
    def incr(key: String): Unit = self.incr(prefixed(key))
    def decr(key: String): Unit = self.decr(prefixed(key))
    def set(key: String, value: Double): Unit = self.set(prefixed(key), value)
    def set(key: String, value: String): Unit = self.set(prefixed(key), value)
    def timing(key: String, value: Long): Unit = self.timing(prefixed(key), value)

    def distribution(
      key: String,
      value: Long,
      tags: StatTags = StatTags.Empty): Unit =
      self.distribution(prefixed(key), value, tags)

    def event(level: StatLevel, title: String, text: String, tags: StatTags): Unit =
      self.event(level, title, text, tags)

    override def toString: String =
      s"$self.$scope"
  }

  final class FilteredRecorder(self: StatsRecorder, whitelist: Set[String])
      extends StatsRecorder {

    @inline private[this] def ifAllowed[A](key: String)(fn: => A) =
      if (whitelist.contains(key)) fn

    def count(key: String, value: Long, tags: StatTags = StatTags.Empty) =
      ifAllowed(key) { self.count(key, value, tags) }
    def incr(key: String) = ifAllowed(key) { self.incr(key) }
    def decr(key: String) = ifAllowed(key) { self.decr(key) }
    def set(key: String, value: Double) = ifAllowed(key) { self.set(key, value) }
    def set(key: String, value: String) = ifAllowed(key) { self.set(key, value) }

    def timing(key: String, value: Long) =
      ifAllowed(key) { self.timing(key, value) }

    def distribution(key: String, value: Long, tags: StatTags = StatTags.Empty) =
      ifAllowed(key) { self.distribution(key, value, tags) }

    def event(level: StatLevel, title: String, text: String, tags: StatTags): Unit =
      ifAllowed(title) { self.event(level, title, text, tags) }
  }
}

trait DelegatingStatsRecorder extends StatsRecorder {
  def delegates: Iterable[StatsRecorder]

  def count(key: String, value: Long, tags: StatTags = StatTags.Empty) =
    delegates foreach { _.count(key, value, tags) }
  def incr(key: String) = delegates foreach { _.incr(key) }
  def decr(key: String) = delegates foreach { _.decr(key) }
  def set(key: String, value: Double) = delegates foreach { _.set(key, value) }
  def set(key: String, value: String) = delegates foreach { _.set(key, value) }
  def timing(key: String, value: Long) = delegates foreach { _.timing(key, value) }
  def distribution(key: String, value: Long, tags: StatTags = StatTags.Empty) =
    delegates foreach { _.distribution(key, value, tags) }

  def event(level: StatLevel, title: String, text: String, tags: StatTags): Unit =
    delegates foreach { _.event(level, title, text, tags) }
}

object StatsRecorder extends ExceptionLogging {

  // FIXME: Emit a warning on STDERR if Null stats is used.
  object Null extends StatsRecorder {
    def count(key: String, value: Long, tags: StatTags = StatTags.Empty): Unit = ()
    def incr(key: String): Unit = ()
    def decr(key: String): Unit = ()
    def set(key: String, value: Double): Unit = ()
    def set(key: String, value: String): Unit = ()
    def timing(key: String, value: Long): Unit = ()
    def distribution(
      key: String,
      value: Long,
      tags: StatTags = StatTags.Empty): Unit = ()

    def event(level: StatLevel, title: String, text: String, tags: StatTags): Unit =
      ()
  }

  case class Multi(delegates: Seq[StatsRecorder]) extends DelegatingStatsRecorder

  val DefaultPollInterval = 10.seconds

  private val pollService =
    Executors.newScheduledThreadPool(
      1,
      new NamedPoolThreadFactory("Stats Poller", true)
    )

  /** Trait used to cancel metrics pollers.
    */
  trait Cancelable {
    def cancel(): Unit
  }

  def polling(f: => Unit): Unit = polling(DefaultPollInterval)(f)

  def polling(interval: Duration)(f: => Unit): Cancelable =
    if (interval > Duration.Zero) {
      val ps = pollService.scheduleAtFixedRate(
        new Runnable {
          def run() =
            try f
            catch {
              case e: Throwable =>
                logException(e)
                if (!NonFatal(e)) throw e
            }
        },
        interval.length,
        interval.length,
        interval.unit)
      () => ps.cancel(false)
    } else { () =>
      ()
    }
}
