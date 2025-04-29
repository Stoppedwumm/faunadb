package fauna.stats

import com.yammer.metrics.core.{ Histogram => YHistogram, _ }
import com.yammer.metrics.reporting.AbstractPollingReporter
import com.yammer.metrics.Metrics
import java.util.regex.Pattern
import scala.jdk.CollectionConverters._

object Predicate extends MetricPredicate {
  val interestingCFs = Seq(
    "HistoricalIndex",
    "LookupStore",
    "RowTimestamps",
    "SortedIndex",
    "Versions"
  )

  private val pattern = {
    val cfPatterns = interestingCFs.flatMap { cf =>
      Seq(
        s"^ColumnFamily.FAUNA.$cf.AllMemtablesHeapSize$$",
        s"^ColumnFamily.FAUNA.$cf.LiveSSTableCount$$",
        // s"^ColumnFamily.FAUNA.$cf.MaxRowSize$$",
        // s"^ColumnFamily.FAUNA.$cf.MeanRowSize$$",
        // s"^ColumnFamily.FAUNA.$cf.PendingTasks$$",
        s"^ColumnFamily.FAUNA.$cf.SSTablesPerReadHistogram$$"
      )
    }

    val patterns = cfPatterns ++ Seq(
      "^Allocation.*",
      "^Cache.KeyCache.*",
      "^Compaction.*"
    )
    Pattern.compile(patterns mkString "|")
  }

  def matches(name: MetricName, metric: Metric): Boolean =
    pattern.matcher(nameToString(name)).matches

  def nameToString(name: MetricName): String = {
    val sb = new StringBuilder()
      .append(name.getType)
      .append('.')

    if (name.hasScope) {
      sb.append(name.getScope).append('.')
    }

    sb.append(name.getName).toString
  }

}

/** Pipes Yammer Metrics to a StatsRecorder.
  */
case class MetricsReporter(statsd: StatsRecorder)
    extends AbstractPollingReporter(Metrics.defaultRegistry, "statsd")
    with MetricProcessor[Long] {

  private val clock = Clock.defaultClock
  private val predicate = Predicate
  private val registry = Metrics.defaultRegistry
  private val jvm = new JVMMetrics

  def run: Unit = {
    process(clock.time / 1000)
  }

  def processCounter(name: MetricName, counter: Counter, epoch: Long): Unit = {
    statsd.count(Predicate.nameToString(name), counter.count.toInt)
    counter.clear()
  }

  /** N.B. we process gauges as strings primarily due to ease of integration with
    * StatsRecorder's interface. However, because Gauge's type param is unbounded, we
    * get a few funky things. Notably, Cassandra exports two histograms as
    * Gauge<long[]>.
    */
  def processGauge(name: MetricName, gauge: Gauge[_], epoch: Long): Unit =
    statsd.set(Predicate.nameToString(name), gauge.value.toString)

  def processHistogram(
    name: MetricName,
    histogram: YHistogram,
    epoch: Long
  ): Unit = {
    val str = Predicate.nameToString(name)
    sampling(str, histogram)
  }

  def processMeter(name: MetricName, meter: Metered, epoch: Long): Unit =
    statsd.set(Predicate.nameToString(name), meter.count)

  def processTimer(name: MetricName, timer: Timer, epoch: Long): Unit = {
    val str = Predicate.nameToString(name)
    processMeter(name, timer, epoch)
    sampling(str, timer)
  }

  private def process(epoch: Long): Unit = {
    registry.groupedMetrics(predicate).entrySet.asScala foreach { entry =>
      entry.getValue.entrySet.asScala foreach { subEntry =>
        Option(subEntry.getValue) map { metric =>
          try {
            metric.processWith(this, subEntry.getKey, epoch)
          } catch {
            case _: Throwable => () // ignored
          }
        }
      }
    }

    jvm.snap(statsd)
  }

  private def sampling(name: String, samples: Sampling) = {
    val snapshot = samples.getSnapshot
    statsd.set(name + ".p50", snapshot.getMedian)
    // statsd.set(name + ".75", snapshot.get75thPercentile)
    // statsd.set(name + ".p95", snapshot.get95thPercentile)
    // statsd.set(name + ".98", snapshot.get98thPercentile)
    statsd.set(name + ".p99", snapshot.get99thPercentile)
    statsd.set(name + ".p999", snapshot.get999thPercentile)
  }

}
