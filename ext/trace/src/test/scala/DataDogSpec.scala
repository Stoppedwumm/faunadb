package fauna.trace.test

import fauna.codex.json.JS
import fauna.codex.json2.JSONWriter
import fauna.stats.StatsRecorder
import fauna.trace._
import fauna.trace.datadog.Format
import io.netty.buffer._
import scala.util.Random

class DataDogSpec extends Spec {

  // Fauna's JSON parser cannot read unsigned longs; never generate
  // more than 63 bit IDs.
  implicit object PositiveRandom extends Random {
    override def nextLong(): Long = {
      var v = super.nextLong()
      while (v < 0) {
        v = super.nextLong()
      }
      v
    }
  }

  case class ByteBufExporter(buf: ByteBuf)
      extends Exporter {

    def exportOne(data: Span.Data): Unit = {
      val writer = JSONWriter(buf)
      Format.writeSpan(writer, data)
    }
  }

  "DataDog" - {
    "format" in {
      val buf = Unpooled.buffer
      val tracer = SamplingTracer(1.0, Tracer.Stats(StatsRecorder.Null))
      tracer.addExporter(ByteBufExporter(buf))

      val span = tracer.buildSpan("test").start()
      span.addAttribute("name", "john")
      span.addAttribute("age", 20)
      span.addAttribute("height", 5.83)
      span.addAttribute("alive", true)
      span.finish()

      val json = JS.parse(buf)
      (json / "trace_id").as[Long] should equal (span.traceID.toLong)
      (json / "span_id").as[Long] should equal (span.spanID.id)
      (json / "parent_id").asOpt[Long] should equal (None)
      (json / "name").as[String] should equal ("test")
      (json / "service").as[String] should equal ("faunadb")
      (json / "type").as[String] should equal ("db")
      (json / "start").asOpt[Long] shouldNot equal (None)
      (json / "duration").asOpt[Long] shouldNot equal (None)

      (json / "meta" / "name").as[String] should equal ("john")
      (json / "meta" / "alive").as[String] should equal ("true")

      (json / "metrics" / "height").as[Double] should equal (5.83)
      (json / "metrics" / "age").as[Double] should equal (20.0)

      (json / "error").asOpt[Long] should equal (None)
    }

    "parent" in {
      val buf = Unpooled.buffer
      val tracer = SamplingTracer(1.0, Tracer.Stats(StatsRecorder.Null))
      tracer.addExporter(ByteBufExporter(buf))

      val parent = tracer.buildSpan("parent").start()

      val span = tracer.buildSpan("child")
        .withParent(parent)
        .start()
      span.finish()

      val json = JS.parse(buf)
      (json / "parent_id").as[Long] should equal (parent.spanID.id)
    }

    "error" in {
      val buf = Unpooled.buffer
      val tracer = SamplingTracer(1.0, Tracer.Stats(StatsRecorder.Null))
      tracer.addExporter(ByteBufExporter(buf))

      val span = tracer.buildSpan("oops").start()
      span.setStatus(PermissionDenied("not gonna do it"))
      span.finish()

      val json = JS.parse(buf)
      (json / "error").as[Long] should equal (1)
    }
  }
}
