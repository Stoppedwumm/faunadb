package fauna.stats

import scala.concurrent.duration._

final case class StreamMetrics(
  events: Long,
  byteReadOps: Long,
  computeOps: Long,
  storageBytesRead: Long,
  remainingPeriod: FiniteDuration,
  filteredEvents: Long = 0
)

object StreamMetrics {

  /** Period equivalent to 1 compute op in millis. */
  val BaselinePeriodMillis = 60_000 // 1 compute op per minute

  /** The number of events processed by a stream. */
  val Events = "Streams.Events"

  def bufferedReporter: StatsRequestBuffer =
    new StatsRequestBuffer(
      Set(
        Events,
        QueryMetrics.Compute,
        QueryMetrics.BytesRead,
        QueryMetrics.ReadDocument,
        QueryMetrics.ReadSet,
        QueryMetrics.ByteReadOps
      ))

  def apply(
    stats: StatsRequestBuffer,
    period: FiniteDuration,
    truncated: Boolean): StreamMetrics = {

    val query = QueryMetrics(queryTime = 0, stats, minComputeOps = 0)
    val events = stats.countOrZero(Events)
    val (computeCost, discarded) = calculateComputeCost(period, truncated)

    StreamMetrics(
      events,
      query.byteReadOps,
      query.computeOps + computeCost,
      query.storageBytesRead,
      discarded)
  }

  def apply(
    events: Long,
    filteredEvents: Long,
    byteReadOps: Long,
    storageBytesRead: Long,
    computeOps: Long,
    period: FiniteDuration,
    truncated: Boolean
  ): StreamMetrics = {
    val (computeCost, discarded) = calculateComputeCost(period, truncated)
    StreamMetrics(
      events,
      byteReadOps,
      computeOps + computeCost,
      storageBytesRead,
      discarded,
      filteredEvents = filteredEvents)
  }

  /** Calculates the cost of keeping the stream open for a given period.
    * Reporting is done periodically so, the cost should be truncated during the
    * stream operation to avoid accumulating error due to rounding values over time.
    * When truncated, the call-site must take the remaining period into account for
    * the next call. Truncation should be disabled when closing a stream and
    * reporting its ending period. In that case, the cost will be rounded up.
    */
  private def calculateComputeCost(
    period: FiniteDuration,
    truncated: Boolean): (Long, FiniteDuration) = {

    /** We max at 0 ms here to defend against a race condition when calculating compute
      * metrics. Our metrics aggregation happens in a background future and so it is
      * possible for our regularly scheduled metrics reporting and the stream closing
      * to happen at right around the same time. When this happens, we will call report
      * on stream close. If those 2 futures get executed out of order it is possible
      * to end up with a negative period. When this happens, all of the compute has
      * been accounted for by the one that executed first so we can max this to 0
      * ms which won't end up adding compute.
      */
    val duration = period.toMillis.max(0)

    val (cost, remaining) =
      if (truncated) {
        (duration / BaselinePeriodMillis, duration % BaselinePeriodMillis)
      } else {
        (math.ceil(duration.toDouble / BaselinePeriodMillis).toLong, 0L)
      }

    (cost, remaining.millis)
  }
}
