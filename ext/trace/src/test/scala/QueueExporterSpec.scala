package fauna.trace.test

import fauna.stats._
import fauna.trace._

class QueueExporterSpec extends Spec {
  val exporter = new MemoryExporter
  var stats: StatsRequestBuffer = _

  before {
    exporter.reset()

    stats = new StatsRequestBuffer(
      Set(
        "Tracing.Spans.Dropped",
        "Tracing.Spans.Exported"))
  }

  "QueueExporter" - {
    "works" in {
      val count = 1024
      val tracer = SamplingTracer(1.0, Tracer.Stats(stats))
      val queue = new QueueExporter(count, Vector(exporter), tracer.stats)

      tracer.addExporter(queue)

      queue.isRunning should be (false)

      queue.start()
      queue.isRunning should be (true)

      for (i <- 1 to count) {
        val span = tracer.buildSpan("test").start()
        span.finish()

        eventually {
          exporter.finishedSpans.size should be (i)

          val fin = exporter.finishedSpans.last
          fin.traceID should equal (span.traceID)
          fin.spanID should equal (span.spanID)

          // only exported when a span leaves the process
          (stats.toJson / "Tracing.Spans.Exported").asOpt[Int] should be (None)
          (stats.toJson / "Tracing.Spans.Dropped").asOpt[Int] should equal (None)
        }
      }

      val span = tracer.buildSpan("test").start()
      span.finish()

      eventually {
        exporter.finishedSpans.size should be (count + 1)
      }

      queue.stop()

      eventually {
        queue.isRunning should be (false)
      }

      val drop = tracer.buildSpan("drop").start()
      drop.finish()

      exporter.finishedSpans.size should be (count + 1)
      (stats.toJson / "Tracing.Spans.Dropped").as[Int] should equal (1)
    }

  }
}
