package fauna.trace.datadog

import fauna.codex.json2.{ JSON, JSONWriter }
import fauna.lang.syntax._
import fauna.trace.{ OK, Span }
import io.netty.buffer.Unpooled
import io.netty.util.AsciiString

// docs: https://docs.datadoghq.com/api/?lang=bash#send-traces
object Format {
  private[this] val log = getLogger

  // The unique integer (64-bit unsigned) ID of the trace containing
  // this span.
  val TraceIDField = JSON.Escaped("trace_id")

  // The span integer (64-bit unsigned) ID
  val SpanIDField = JSON.Escaped("span_id")

  // The span integer ID of the parent span.
  val ParentIDField = JSON.Escaped("parent_id")

  // The span name. The span name must not be longer than 100
  // characters.
  val NameField = JSON.Escaped("name")

  // The resource you are tracing. The resource name must not be
  // longer than 5000 characters.
  val ResourceField = JSON.Escaped("resource")

  // The service you are tracing. The service name must not be longer
  // than 100 characters.
  val ServiceField = JSON.Escaped("service")
  val ServiceValue = JSON.Escaped("faunadb")

  // case-sensitive The type of request. The options available are
  // web, db, cache, and custom.
  val TypeField = JSON.Escaped("type")
  val TypeValue = JSON.Escaped("db")

  // The start time of the request in nanoseconds from the unix epoch.
  val StartField = JSON.Escaped("start")

  // The duration of the request in nanoseconds.
  val DurationField = JSON.Escaped("duration")

  // Set this value to 1 to indicate if an error occured. If an error
  // occurs, you should pass additional information, such as the error
  // message, type and stack information in the meta property.
  val ErrorField = JSON.Escaped("error")

  // A set of key-value metadata. Keys and values must be strings.
  val MetaField = JSON.Escaped("meta")

  // A set of key-value metadata. Keys must be strings. Values must be doubles.
  val MetricsField = JSON.Escaped("metrics")

  val StatusField = new AsciiString("status")

  def writeSpan(out: JSON.Out, data: Span.Data): Unit = {
    out.writeObjectStart()

    // DataDog claims to only support 64-bit trace IDs (UGH); use only
    // the low order bits.
    out.writeObjectField(TraceIDField, out.writeUnsignedNumber(data.traceID.toLong))

    out.writeObjectField(SpanIDField, out.writeUnsignedNumber(data.spanID.id))

    data.parentID foreach { parent =>
      out.writeObjectField(ParentIDField, out.writeUnsignedNumber(parent.id))
    }

    out.writeObjectField(NameField, out.writeString(data.name))
    out.writeObjectField(ServiceField, out.writeString(ServiceValue))
    out.writeObjectField(TypeField, out.writeString(TypeValue))
    out.writeObjectField(StartField, out.writeNumber(data.startTime.micros * 1000))
    out.writeObjectField(DurationField, out.writeNumber(data.finishNanos - data.startNanos))

    // DataDog refers to the operation (e.g. GET /foo) as a "resource"
    out.writeObjectField(ResourceField, out.writeString(data.operation))

    val metaB = Map.newBuilder[AsciiString, Array[Byte]]

    data.status match {
      case OK(_) => ()
      case _     =>
        out.writeObjectField(ErrorField, out.writeNumber(1))
    }

    metaB += StatusField -> data.status.description.getBytes("UTF-8")

    if (!data.attributes.isEmpty) {
      val metricsB = Map.newBuilder[AsciiString, Double]

      data.attributes forEach {
        case (k, Span.StringValue(v)) =>
          metaB += k -> v.array
        case (k, Span.BooleanValue(true)) =>
          metaB += k -> JSONWriter.TrueBytes
        case (k, Span.BooleanValue(false)) =>
          metaB += k -> JSONWriter.FalseBytes
        case (k, Span.LongValue(v)) =>
          metricsB += k -> v.toDouble
        case (k, Span.DoubleValue(v)) =>
          metricsB += k -> v
        case (k, Span.ArrayValue(_)) =>
          // XXX: add once DD supports the entirety of OpenTelemetry
          log.trace(s"DataDog does not support array attributes. Dropping $k")
      }

      val metrics = metricsB.result()

      if (metrics.nonEmpty) {
        writeMap(out, MetricsField, metrics,
          { v: Double => out.writeNumber(v) })
      }
    }

    val meta = metaB.result()

    writeMap(out, MetaField, meta, { v: Array[Byte] =>
      out.writeString(Unpooled.wrappedBuffer(v))
    })

    out.writeObjectEnd()
  }

  private def writeMap[T](
    out: JSONWriter,
    field: JSON.Escaped,
    data: Map[AsciiString, T],
    valueF: T => JSONWriter): JSONWriter =
    out.writeObjectField(field, {
      out.writeObjectStart()
      data foreach {
        case (k, v) => out.writeObjectField(k, valueF(v))
      }
      out.writeObjectEnd()
    })
}
