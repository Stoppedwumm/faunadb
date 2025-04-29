package fauna.model.runtime.stream

import fauna.atoms.AccountID
import fauna.codex.json2.JSON
import fauna.lang.Timestamp
import fauna.model.runtime.fql2.serialization.{
  FQL2ValueEncoder,
  MaterializedValue,
  ValueFormat
}
import fauna.repo.query.State
import fauna.repo.values.Value
import fauna.stats.{ QueryMetrics, StatsRequestBuffer }
import io.netty.util.AsciiString

sealed abstract class EventType(override val toString: String)

object EventType {
  sealed abstract class Protocol(toString: String) extends EventType(toString)
  case object Status extends Protocol("status")
  case object Error extends Protocol("error")

  sealed abstract class Data(toString: String) extends EventType(toString)
  case object Add extends Data("add")
  case object Update extends Data("update")
  case object Remove extends Data("remove")
}

sealed trait Event {
  import Event._

  def eventType: EventType

  final def encode(out: JSON.Out, metrics: Metrics): Unit = {
    out.writeObjectStart()
    out.writeObjectField(Event.Field.Type, out.writeString(eventType.toString))
    encodeFields(out)
    out.writeObjectField(Field.Stats, metrics.encode(out))
    out.writeObjectEnd()
  }

  protected def encodeFields(out: JSON.Out): Unit
}

object Event {

  type Processed = ProcessedT[Event]

  object Processed {
    def apply[E <: Event](
      event: E,
      metrics: Metrics,
      cursor: Option[Value.EventSource.Cursor] = None
    ): ProcessedT[E] =
      Processed(Some(event), metrics, cursor)

    def apply[E <: Event](
      event: Option[E],
      metrics: Metrics,
      cursor: Option[Value.EventSource.Cursor]
    ): ProcessedT[E] =
      new ProcessedT(event, metrics, cursor)
  }

  final case class ProcessedT[+E <: Event](
    event: Option[E],
    metrics: Metrics,
    cursor: Option[Value.EventSource.Cursor]
  ) {

    def lift: Option[(Event, Metrics)] = event map { (_, metrics) }

    def toEither: Either[ProcessedT[ErrorEvent], ProcessedT[Event]] = {
      event match {
        case Some(err: ErrorEvent) => Left(Processed(err, metrics, cursor))
        case _                     => Right(this)
      }
    }
  }

  object Metrics {
    def apply(stateMetrics: State.Metrics, queryTimeMillis: Long): Metrics = {
      val stats = new StatsRequestBuffer()
      stateMetrics.commitAll(stats, AccountID.Root)
      val metrics = QueryMetrics(queryTimeMillis, stats)
      Metrics(
        readOps = metrics.byteReadOps,
        storageBytesRead = metrics.storageBytesRead,
        computeOps = metrics.computeOps,
        processingTime = metrics.queryTime,
        rateLimitReadHit = metrics.rateLimitReadHit,
        rateLimitComputeHit = metrics.rateLimitComputeHit
      )
    }
    val Empty = Metrics(0, 0, 0, 0, false, false)
  }

  final case class Metrics(
    readOps: Long,
    storageBytesRead: Long,
    computeOps: Long,
    processingTime: Long,
    rateLimitReadHit: Boolean,
    rateLimitComputeHit: Boolean
  ) {
    def encode(out: JSON.Out) = {
      out.writeObjectStart()
      out.writeObjectField(Field.ReadOps, out.writeNumber(readOps))
      out.writeObjectField(Field.StorageBytesRead, out.writeNumber(storageBytesRead))
      out.writeObjectField(Field.ComputeOps, out.writeNumber(computeOps))
      out.writeObjectField(Field.ProcessingTime, out.writeNumber(processingTime))
      out.writeObjectField(
        Field.RateLimitsField, {
          out.writeArrayStart()
          if (rateLimitComputeHit) {
            out.writeString(Field.RateLimitHitCompute)
          }
          if (rateLimitReadHit) {
            out.writeString(Field.RateLimitHitRead)
          }
          out.writeArrayEnd()
        }
      )
      out.writeObjectEnd()
    }
  }

  object Field {
    val Error = AsciiString.cached("error")
    val Abort = AsciiString.cached("abort")
    val Type = AsciiString.cached("type")
    val Timestamp = AsciiString.cached("txn_ts")
    val Cursor = AsciiString.cached("cursor")
    val Data = AsciiString.cached("data")
    val Code = AsciiString.cached("code")
    val Message = AsciiString.cached("message")
    val Stats = AsciiString.cached("stats")
    val ReadOps = AsciiString.cached("read_ops")
    val StorageBytesRead = AsciiString.cached("storage_bytes_read")
    val ComputeOps = AsciiString.cached("compute_ops")
    val ProcessingTime = AsciiString.cached("processing_time_ms")
    val RateLimitsField = AsciiString.cached("rate_limits_hit")
    val RateLimitHitCompute = AsciiString.cached("compute")
    val RateLimitHitRead = AsciiString.cached("read")
    val RateLimitHitWrite = AsciiString.cached("write")
  }

  final case class Status(ts: Timestamp) extends Event {
    def eventType = EventType.Status
    protected def encodeFields(out: JSON.Out) =
      out
        .writeObjectField(Field.Timestamp, out.writeNumber(ts.micros))
        .writeObjectField(
          Field.Cursor,
          out.writeString(Value.EventSource.Cursor(ts).toBase64)
        )
  }

  final case class Data(
    eventType: EventType.Data,
    data: MaterializedValue,
    cursor: Value.EventSource.Cursor
  ) extends Event {
    protected def encodeFields(out: JSON.Out) =
      out
        .writeObjectField(Field.Data, data.encodeTagged(out, cursor.ts))
        .writeObjectField(Field.Timestamp, out.writeNumber(cursor.ts.micros))
        .writeObjectField(Field.Cursor, out.writeString(cursor.toBase64))
  }

  sealed trait ErrorEvent extends Event {
    def eventType: EventType = EventType.Error

    def code: String
    def message: String

    protected def encodeFields(out: JSON.Out): Unit = {
      out.writeObjectField(
        Field.Error, {
          out.writeObjectStart()
          out
            .writeObjectField(Field.Code, out.writeString(code))
            .writeObjectField(Field.Message, out.writeString(message))
          encodeAbort(out)
          out.writeObjectEnd()
        }
      )
    }

    protected def encodeAbort(out: JSON.Out): Unit
  }

  object Error {
    object Code {
      val StreamReplayVolumeExceeded = "stream_replay_volume_exceeded"
      val StreamOverflow = "stream_overflow"
      val InternalError = "internal_error"
      val InvalidStreamStartTime = "invalid_stream_start_time"
      val PermissionLoss = "permission_loss"
    }
  }

  final case class Error(
    code: String,
    message: String
  ) extends ErrorEvent {
    protected def encodeAbort(out: JSON.Out): Unit = ()
  }

  final case class AbortError(
    code: String,
    message: String,
    ts: Timestamp,
    abortReturn: MaterializedValue)
      extends ErrorEvent {
    protected def encodeAbort(out: JSON.Out): Unit = {
      out.writeObjectField(
        Field.Abort,
        FQL2ValueEncoder.encode(ValueFormat.Tagged, out, abortReturn, ts))
    }
  }
}
