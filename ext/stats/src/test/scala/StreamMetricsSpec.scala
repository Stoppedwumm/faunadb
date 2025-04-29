package fauna.stats.test

import fauna.prop._
import fauna.stats._
import scala.concurrent.duration._

class StreamMetricsSpec extends Spec {

  def idleStream(duration: FiniteDuration, truncated: Boolean): StreamMetrics =
    StreamMetrics(new StatsRequestBuffer(), duration, truncated)

  prop("charge 1 compute op per 60 seconds") {
    for {
      duration <- Prop.int(0 to 120) // 2 minutes
      metrics = idleStream(duration.seconds, truncated = true)
      minutes = duration / 60
      remain = (duration % 60).seconds
    } {
      metrics.computeOps shouldBe minutes
      metrics.remainingPeriod shouldBe remain
    }
  }

  prop("round up when not truncated") {
    for {
      duration <- Prop.int(0 to 120) // 2 minutes
      metrics = idleStream(duration.seconds, truncated = false)
    } {
      metrics.computeOps shouldBe math.ceil(duration / 60d).toLong
      metrics.remainingPeriod shouldBe Duration.Zero
    }
  }
}
