package fauna.model.runtime.stream

import fauna.lang.Timestamp

object EventMetricsAggregate {
  def apply(): EventMetricsAggregate =
    new EventMetricsAggregate(
      0, 0, 0, 0, 0, 0, false, false
    )
}

final class EventMetricsAggregate private (
  var numEvents: Long,
  var numFilteredEvents: Long,
  var totalReadOps: Long,
  var totalStorageBytesRead: Long,
  var totalComputeOps: Long,
  var totalProcessingTime: Long,
  var rateLimitReadHit: Boolean,
  var rateLimitComputeHit: Boolean
) {

  def addEventMetrics(event: Event.Metrics): EventMetricsAggregate = {
    numEvents += 1
    incrEventMetrics(event)
  }

  def addFilteredEventMetrics(event: Event.Metrics): EventMetricsAggregate = {
    numFilteredEvents += 1
    incrEventMetrics(event)
  }

  private def incrEventMetrics(
    eventMetrics: Event.Metrics): EventMetricsAggregate = {
    totalReadOps += eventMetrics.readOps
    totalStorageBytesRead += eventMetrics.storageBytesRead
    totalComputeOps += eventMetrics.computeOps
    totalProcessingTime += eventMetrics.processingTime
    rateLimitReadHit ||= eventMetrics.rateLimitReadHit
    rateLimitComputeHit ||= eventMetrics.rateLimitComputeHit
    this
  }
}

object EventMetricsRecorder {
  def apply(report: (Timestamp, EventMetricsAggregate, Boolean) => Unit) =
    new EventMetricsRecorder(report)
}

final class EventMetricsRecorder private (
  report: (Timestamp, EventMetricsAggregate, Boolean) => Unit) {

  private var metricsAggregate = EventMetricsAggregate()

  /** Filtered event metrics are aggregated in between our status events
    * and sent along with those events.  We need a way to access these
    * metrics without impacting the full metrics aggregate that is sent
    * to the log.
    */
  private var filteredMetricsAggregate = EventMetricsAggregate()

  def recordEvent(event: Event.Metrics): Unit = {
    synchronized {
      metricsAggregate.addEventMetrics(event)
    }
  }

  def recordFilteredEvent(event: Event.Metrics): Unit = {

    /** We add to both here because we need to flush both separately.
      * The total aggregate is flushed when we write to the stream log,
      * where we want all metrics.
      * The filtered events aggregate is flushed when we send status events.
      */
    synchronized {
      metricsAggregate.addFilteredEventMetrics(event)
      filteredMetricsAggregate.addFilteredEventMetrics(event)
    }
  }

  def flushFilteredMetrics(): EventMetricsAggregate = {
    synchronized {
      val old = filteredMetricsAggregate
      filteredMetricsAggregate = EventMetricsAggregate()
      old
    }
  }

  def report(now: Timestamp, running: Boolean): Unit = {
    val old = synchronized {
      val old = metricsAggregate
      metricsAggregate = EventMetricsAggregate()
      old
    }
    report(now, old, running)
  }
}
