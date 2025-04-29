package fauna.trace.test

import fauna.trace._
import io.netty.util.AsciiString

class SpanSpec extends Spec {
  val exporter = new MemoryExporter
  val tracer = new SamplingTracer(Sampler.Default)

  tracer.addExporter(exporter)

  before {
    exporter.reset()
  }

  "Span" - {
    "drops oldest attributes" in {
      val span = tracer.buildSpan("test")
        .enableSampling()
        .start().asInstanceOf[RecordingSpan]

      for (i <- 0 until Span.MaxAttributes) {
        span.addAttribute(i.toString, i)
      }

      span.addAttribute("last", Span.MaxAttributes + 1)

      span.toData.attributes.keySet shouldNot contain (new AsciiString("0"))
      span.toData.attributes.keySet should contain (new AsciiString("last"))
    }

    "maintains the last status" in {
      val span = tracer.buildSpan("test")
        .enableSampling()
        .start().asInstanceOf[RecordingSpan]

      span.toData.status should equal (Status.Default)

      val cancelled = Cancelled("timeout")
      span.setStatus(cancelled)
      span.toData.status should equal (cancelled)

      val aborted = Aborted("oops")
      span.setStatus(aborted)
      span.toData.status should equal (aborted)
    }

    "finished spans are exported" in {
      val span = tracer.buildSpan("test")
        .enableSampling()
        .start()

      exporter.finishedSpans.size should equal (0)
      span.finish()
      exporter.finishedSpans.size should equal (1)

      val data = exporter.finishedSpans(0)
      data.traceID should equal (span.traceID)
      data.spanID should equal (span.spanID)
    }

    "unsampled spans are not exported" in {
      val span = tracer.buildSpan("test").start()

      span.isSampled should be (false)

      exporter.finishedSpans.size should equal (0)
      span.finish()
      exporter.finishedSpans.size should equal (0)
    }

    "sampled spans carry tracer resource information" in {
      val ip = new AsciiString("127.0.0.1")
      val t = new SamplingTracer(
        Sampler.Default,
        Resource(Resource.Labels.HostName -> ip))

      t.addExporter(exporter)

      val span = t.buildSpan("test")
        .enableSampling()
        .start()
      span.finish()

      val data = exporter.finishedSpans(0)
      val iter = data.resource.iterator

      iter.nonEmpty should be (true)
      iter.toSeq should contain (Resource.Labels.HostName -> ip)
    }

    "sampled spans capture thread information" in {
      val span = tracer.buildSpan("1")
        .enableSampling()
        .start()

      span.finish()

      exporter.finishedSpans should have size (1)
      exporter.finishedSpans.head.attributes.keySet should contain (Attributes.Thread.ID)
      exporter.finishedSpans.head.attributes.keySet should contain (Attributes.Thread.Name)
    }

    "span operations default to span names" in {
      val s1 = tracer.buildSpan("1")
        .enableSampling()
        .start()

      s1.finish()

      exporter.finishedSpans.size should equal (1)
      exporter.finishedSpans.head.operation should equal (new AsciiString("1"))

      exporter.reset()

      val s2 = tracer.buildSpan("2")
        .enableSampling()
        .start()

      s2.setOperation("do 2")
      s2.finish()

      exporter.finishedSpans.size should equal (1)
      exporter.finishedSpans.head.operation should equal (new AsciiString("do 2"))
    }
  }
}
