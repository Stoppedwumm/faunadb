package fauna.trace.test

import fauna.stats.StatsRecorder
import fauna.trace._

class TracerSpec extends Spec {
  val exporter = new MemoryExporter
  val stats = Tracer.Stats(StatsRecorder.Null)

  before {
    exporter.reset()
  }

  "Tracer" - {
    "withSpan" - {
      "noops" in {
        val tracer = SamplingTracer(1.0, stats)
        tracer.addExporter(exporter)

        tracer.withSpan("noop") { }

        exporter.finishedSpans should have size 0
      }

      "opens a new child span" in {
        val tracer = SamplingTracer(1.0, stats)
        tracer.addExporter(exporter)

        val root = tracer.buildSpan("parent").start()

        val scope = tracer.activate(root)
        try {
          tracer.withSpan("child") { }
        } finally {
          scope foreach { _.close() }
        }

        exporter.finishedSpans should have size 2
      }

      "handles failures" in {
        val tracer = SamplingTracer(1.0, stats)
        tracer.addExporter(exporter)

        val root = tracer.buildSpan("parent").start()

        val scope = tracer.activate(root)
        try {
          tracer.withSpan("child") {
            throw new IllegalStateException("bad news")
          }
        } catch {
          case _: IllegalStateException => ()
        } finally {
          scope foreach { _.close() }
        }

        exporter.finishedSpans should have size 2

        exporter.finishedSpans.head.name.toString should equal ("child")
        exporter.finishedSpans.head.status.code should not equal (0) // non-zero is failure

        exporter.finishedSpans.last.name.toString should equal ("parent")
        exporter.finishedSpans.last.status.code should equal (0) // child status does not propagate up
      }
    }

    "withTraceContext" - {
      "noops" in {
        val tracer = SamplingTracer(1.0, stats)
        tracer.addExporter(exporter)

        tracer.withTraceContext("noop", None) { }

        exporter.finishedSpans should have size 0
      }

      "opens a new child span" in {
        val tracer = SamplingTracer(1.0, stats)
        tracer.addExporter(exporter)

        val root = tracer.buildSpan("parent").start()

        val scope = tracer.activate(root)
        try {
          tracer.withTraceContext(
            "child",
            scope flatMap { _.span map { _.context } }) {}
        } finally {
          scope foreach { _.close() }
        }

        exporter.finishedSpans should have size 2
      }

      "handles failures" in {
        val tracer = SamplingTracer(1.0, stats)
        tracer.addExporter(exporter)

        val root = tracer.buildSpan("parent").start()

        val scope = tracer.activate(root)
        try {
          tracer.withTraceContext(
            "child",
            scope flatMap { _.span map { _.context } }) {
            throw new IllegalStateException("bad news")
          }
        } catch {
          case _: IllegalStateException => ()
        } finally {
          scope foreach { _.close() }
        }

        exporter.finishedSpans should have size 2

        exporter.finishedSpans.head.name.toString should equal ("child")
        exporter.finishedSpans.head.status.code should not equal (0) // non-zero is failure

        exporter.finishedSpans.last.name.toString should equal ("parent")
        exporter.finishedSpans.last.status.code should equal (0) // child status does not propagate up
      }
    }
  }
}
