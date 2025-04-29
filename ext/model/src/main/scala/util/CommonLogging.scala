package fauna.model.util

import fauna.codex.json2._
import fauna.lang.syntax._
import fauna.stats.QueryMetrics
import fauna.trace._

object CommonLogging {

  /** List of global ids for the database path the query was executed for.
    * Meant to store encoded global ids, see Database.encodeGlobalID
    */
  final val GlobalIDFieldStr = "global_id_path"
  final val GlobalIDPathField = JSON.Escaped(GlobalIDFieldStr)

  object Stats {

    /** Top-level key for metrics accrued while processing.
      */
    final val StatsField = JSON.Escaped("stats")

    /** Total compute ops accrued during processing. Found in
      * "stats.query_compute_ops" in each log line.
      *
      * If in request context and the response code is 5xx, this is hard-coded
      * to 1.
      */
    final val ComputeOpsField = JSON.Escaped("query_compute_ops")

    /** Actual total compute ops accrued, regardless of response code. */
    final val ActualComputeOpsField = JSON.Escaped("query_actual_compute_ops")

    /** Total read ops accrued during processing, as computed by the "bytes per read
      * op" pricing model. Found in "stats.query_byte_read_ops" in each log
      * line.
      *
      * If in request context and the response code is 5xx, this is hard-coded
      * to 0.
      */
    final val ByteReadOpsField = JSON.Escaped("query_byte_read_ops")

    /** Actual total read ops accrued, regardless of response code. */
    final val ActualReadOpsField = JSON.Escaped("query_actual_read_ops")

    /** Total write ops accrued processing, as computed by the "bytes per write op"
      * pricing model. Found in "stats.query_byte_write_ops" in each log line.
      *
      * If in request context and the response code is 5xx, this is hard-coded
      * to 0.
      */
    final val ByteWriteOpsField = JSON.Escaped("query_byte_write_ops")

    /** Actual total write ops accrued, regardless of response code. */
    final val ActualWriteOpsField = JSON.Escaped("query_actual_write_ops")

    /** Total number of bytes written to storage while processing. Found in
      * "stats.storage_bytes_write" in each log line.
      */
    final val BytesWriteField = JSON.Escaped("storage_bytes_write")

    /** Total number of bytes read from storage while processing. Found in
      * "stats.storage_bytes_read" in each log line.
      */
    final val BytesReadField = JSON.Escaped("storage_bytes_read")

    def writeStats(out: JSONWriter, metrics: QueryMetrics, httpCode: Option[Int])(
      writeExtraStats: JSONWriter => Unit) = {
      out.writeObjectField(
        StatsField, {
          out.writeObjectStart()
          writeCommonStats(out, metrics, httpCode)
          writeExtraStats(out)
          out.writeObjectEnd()
        })
    }

    private def writeCommonStats(
      out: JSONWriter,
      metrics: QueryMetrics,
      httpCode: Option[Int]) = {

      val (billedComputeOps, billedReadOps, billedWriteOps) = httpCode match {
        case Some(code) if code >= 500 && code < 600 => (1L, 0L, 0L)
        case _ => (metrics.computeOps, metrics.byteReadOps, metrics.byteWriteOps)
      }

      out.writeObjectField(ComputeOpsField, out.writeNumber(billedComputeOps))
      out.writeObjectField(ByteReadOpsField, out.writeNumber(billedReadOps))
      out.writeObjectField(ByteWriteOpsField, out.writeNumber(billedWriteOps))

      out.writeObjectField(
        ActualComputeOpsField,
        out.writeNumber(metrics.computeOps))
      out.writeObjectField(ActualReadOpsField, out.writeNumber(metrics.byteReadOps))
      out.writeObjectField(
        ActualWriteOpsField,
        out.writeNumber(metrics.byteWriteOps))

      out.writeObjectField(
        BytesWriteField,
        out.writeNumber(metrics.storageBytesWrite))
      out.writeObjectField(BytesReadField, out.writeNumber(metrics.storageBytesRead))
    }
  }

  object Trace {

    /** Top-level key which contains correlation context metadata for
      * requests sampled by OpenTelemetry.
      */
    final val TraceField = JSON.Escaped("trace")

    /** `TraceID` for requests sampled by OpenTelemetry. Found in
      * "trace.trace_id".
      */
    final val TraceIDField = JSON.Escaped("trace_id")

    /** `SpanID` for requests sampled by OpenTelemetry. Found in
      * "trace.span_id".
      */
    final val SpanIDField = JSON.Escaped("span_id")

    /** `TraceFlags` for requests sampled by OpenTelemetry. Found in
      * "trace.flags".
      */
    final val FlagsField = JSON.Escaped("flags")

    /** Top-level key for correlation context metadata formatted for
      * DataDog.
      *
      * See https://docs.datadoghq.com/tracing/connect_logs_and_traces/opentelemetry/
      */
    final val DataDogField = JSON.Escaped("dd")

    /** Key for correlation context metadata formatted for
      * DataDog. Found in "trace.datadog".
      */
    final val TraceDataDogField = JSON.Escaped("datadog")

    def writeTrace(
      out: JSONWriter,
      traceContext: TraceContext,
      isTasksLog: Boolean = false) = {
      out.writeObjectField(
        TraceField, {
          out.writeObjectStart()

          out.writeObjectField(
            TraceIDField,
            out.writeString(traceContext.traceID.toHexString))
          out.writeObjectField(
            SpanIDField,
            out.writeString(traceContext.spanID.toHexString))
          out.writeObjectField(
            FlagsField,
            out.writeString(traceContext.flags.toHexString)
          )

          if (isTasksLog) {
            // FIXME: tasks.log has this info in `trace.datadog`, but the query
            // log has this in `dd`. Why?
            out.writeObjectField(
              TraceDataDogField, {
                out.writeObjectStart()
                writeDataDog(out, traceContext)
                out.writeObjectEnd()
              }
            )
          }

          out.writeObjectEnd()
        }
      )

      out.writeObjectField(
        DataDogField, {
          out.writeObjectStart()
          writeDataDog(out, traceContext)
          out.writeObjectEnd()
        }
      )
    }

    private def writeDataDog(out: JSONWriter, traceContext: TraceContext) = {
      // DataDog uses 64b trace IDs and does not hex encode. These "extra"
      // fields make correlation between logs and APM easier.
      out.writeObjectField(
        TraceIDField,
        out.writeString(traceContext.traceID.toLong.toUnsignedString)
      )

      out.writeObjectField(
        SpanIDField,
        out.writeString(traceContext.spanID.id.toUnsignedString)
      )
    }
  }
}
