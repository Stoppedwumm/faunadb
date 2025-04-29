package fauna.exec.test

import fauna.exec._
import fauna.trace._
import io.netty.util.AsciiString
import org.scalatest.concurrent.Eventually
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

class ExecutionContextSpec extends Spec with Eventually {
  val exporter = new MemoryExporter
  val tracer = new SamplingTracer(Sampler.Default)

  tracer.addExporter(exporter)

  GlobalTracer.registerIfAbsent { (None, tracer) }

  implicit val ec = NamedThreadPoolExecutionContext("test")

  before {
    exporter.reset()
  }

  "ExecutionContext" - {
    "activates spans" in {
      var f: Future[Span] = null

      val span = tracer.buildSpan("f")
        .enableSampling()
        .start()

      val scope = tracer.activate(span)
      try {
        f = Future[Span] {
          tracer.activeSpan shouldNot be(None)
          tracer.activeSpan.get
        }
      } finally {
        scope foreach { _.close() }
      }

      val res = Await.result(f, 1.second)
      eventually(timeout(1.second)) {
        exporter.finishedSpans.size should equal (1)
      }

      val first = exporter.finishedSpans(0)
      first.traceID should equal (res.traceID)
      first.spanID should equal (res.spanID)
    }

    "pipelines" in {
      var f: Future[Future[Span]] = null

      val span = tracer.buildSpan("f")
        .enableSampling()
        .start()

      val scope = tracer.activate(span)
      try {
        f = Future[Future[Span]] {
          tracer.activeSpan shouldNot be(None)
          tracer.activeSpan.get.addAttribute("1", true)

          Future[Span] {
            tracer.activeSpan shouldNot be(None)
            tracer.activeSpan.get.addAttribute("2", true)
            tracer.activeSpan.get
          }
        }
      } finally {
        scope foreach { _.close() }
      }

      eventually(timeout(1.second)) {
        exporter.finishedSpans.size should equal (1)
      }

      val finished = exporter.finishedSpans(0)
      finished.traceID should equal (span.traceID)
      finished.spanID should equal (span.spanID)
      finished.attributes.get(new AsciiString("1")).value should equal (true)
      finished.attributes.get(new AsciiString("2")).value should equal (true)
    }

    "maps" in {
      var f: Future[Int] = null

      val span = tracer.buildSpan("f")
        .enableSampling()
        .start()

      val scope = tracer.activate(span)
      try {
        tracer.activeSpan.get should be(span)

        f = Future[Int] {
          val i = 0
          tracer.activeSpan shouldNot be(None)
          tracer.activeSpan.get should be (span)
          tracer.activeSpan.get.addAttribute("before", i)
          i
        } map { i =>
          val j = i + 1
          tracer.activeSpan.get should be (span)
          tracer.activeSpan.get.addAttribute("after", j)
          j
        }

        tracer.activeSpan.get should be(span)
      } finally {
        scope foreach { _.close() }
      }

      tracer.activeSpan should be (None)

      eventually(timeout(1.second)) {
        exporter.finishedSpans.size should equal (1)
      }

      val finished = exporter.finishedSpans(0)
      finished.traceID should equal (span.traceID)
      finished.spanID should equal (span.spanID)
      finished.attributes.get(new AsciiString("before")).value should equal (0)
      finished.attributes.get(new AsciiString("after")).value should equal (1)
    }

    "flatMaps" in {
      var f: Future[Int] = null

      val span = tracer.buildSpan("f")
        .enableSampling()
        .start()

      val scope = tracer.activate(span)
      try {
        tracer.activeSpan.get should be(span)

        tracer.withSpan("g") {
          f = Future[Int] { 0 }
        }

        f = f flatMap { i => Future[Int] { i + 1 } }

        tracer.activeSpan.get should be(span)
      } finally {
        scope foreach { _.close() }
      }

      tracer.activeSpan should be (None)

      eventually(timeout(1.second)) {
        exporter.finishedSpans.size should equal (2)
      }

      val finished = exporter.finishedSpans

      finished map { _.name } should contain theSameElementsAs Seq(new AsciiString("f"), new AsciiString("g"))
    }
  }
}
