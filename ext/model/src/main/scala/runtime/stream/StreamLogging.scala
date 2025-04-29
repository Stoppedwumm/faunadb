package fauna.model.stream

import fauna.codex.json.JSValue
import fauna.codex.json2._
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.model.util.CommonLogging
import fauna.repo.cassandra.CassandraService
import fauna.stats.StreamMetrics
import io.netty.buffer.ByteBufAllocator

object StreamLogging {

  /** Top-level key containing the replica name of the host processing the stream */
  final val ReplicaField = JSON.Escaped("replica")

  /** Top-level key generated from an IDSource for each event. This
    * field is used as the idempotency key when aggregating logs,
    * where at-least-once delivery may result in duplicate log
    * entries.
    */
  final val MessageIDField = JSON.Escaped("id")

  /** Top-level key with log the event timestamp in ISO-8601 format. */
  val TSField = JSON.Escaped("ts")

  /** Top-level key with the globally unique identifier of the stream. */
  val StreamIDField = JSON.Escaped("stream_id")

  /** Top-level key with the stream's authentication information */
  val AuthField = JSON.Escaped("auth")

  /** Top-level key for metrics accrued while processing latest events. */
  val StatsField = JSON.Escaped("stats")

  /** Sub-key of "stats" with the number of events processed since last log entry. */
  val EventsField = JSON.Escaped("stream_events")

  /** Sub-key of "stats" with the number of read ops consumed since last log entry.
    *  Compute by the "bytes per read op" pricing model.
    */
  val ByteReadOpsField = JSON.Escaped("query_byte_read_ops")

  /** Sub-key of "stats" with the number of compute ops consumed since last log entry */
  val ComputeOpsField = JSON.Escaped("query_compute_ops")

  /** Sub-key of "stats" with the number of bytes read since last log entry */
  val BytesReadField = JSON.Escaped("storage_bytes_read")

  /** Version tracking the fql version used for the stream. */
  val VersionField = JSON.Escaped("api_version")

  /** Number of filtered events processed since the last log entry.
    *  This is the number of log events that were processed but
    *  were filtered out.  These events were not seen by the user.
    */
  val FilteredEventsField = JSON.Escaped("filtered_events")

  private lazy val logger = getLogger("stream")
}

trait StreamLogging {
  import StreamLogging._

  protected def logEvent(
    messageID: Long,
    streamID: StreamID,
    auth: JSValue,
    metrics: StreamMetrics,
    version: String,
    globalIDPath: Seq[String]
  ): Unit = {

    val alloc = ByteBufAllocator.DEFAULT
    val buf = alloc.ioBuffer

    try {
      val out = JSONWriter(buf)
      out.writeObjectStart()

      out.writeObjectField(
        MessageIDField,
        // Cast as a string to avoid limited/varying precision
        // integers in log pipelines.
        out.writeString(messageID.toString))

      out.writeObjectField(
        ReplicaField,
        out.writeString(CassandraService.instance.replicaName))

      out.writeObjectField(
        CommonLogging.GlobalIDPathField, {
          out.writeArrayStart()
          globalIDPath foreach { out.writeString(_) }
          out.writeArrayEnd()
        })

      out.writeObjectField(TSField, out.writeString(Clock.time.toString))
      out.writeObjectField(StreamIDField, out.writeNumber(streamID.toLong))
      out.writeObjectField(VersionField, out.writeString(version))
      out.writeObjectField(
        AuthField, {
          out.writeDelimiter()
          auth.writeTo(buf, pretty = false)
        })
      writeMetrics(out, metrics)
      out.writeObjectEnd()
      logger.info(buf.toUTF8String)
    } finally {
      buf.release()
    }
  }

  private def writeMetrics(out: JSONWriter, m: StreamMetrics): Unit =
    out.writeObjectField(
      StatsField, {
        out.writeObjectStart()
        out.writeObjectField(EventsField, out.writeNumber(m.events))
        out.writeObjectField(FilteredEventsField, out.writeNumber(m.filteredEvents))
        out.writeObjectField(ByteReadOpsField, out.writeNumber(m.byteReadOps))
        out.writeObjectField(ComputeOpsField, out.writeNumber(m.computeOps))
        out.writeObjectField(BytesReadField, out.writeNumber(m.storageBytesRead))
        out.writeObjectEnd()
      }
    )
}
