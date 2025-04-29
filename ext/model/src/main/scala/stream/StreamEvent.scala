package fauna.model.stream

import fauna.lang._
import fauna.storage.ops._

sealed trait StreamEvent {
  val eventType: EventType
}

object StreamEvent {

  def apply(txnTS: Timestamp, write: Write, isPartitioned: Boolean): StreamEvent =
    write match {
      case w: VersionAdd    => VersionAdded(txnTS, w)
      case w: VersionRemove => VersionRemoved(w)
      case w: DocRemove     => DocumentRemoved(w)
      case w: SetAdd        => SetAdded(w, isPartitioned)
      case w: SetRemove     => SetRemoved(w, isPartitioned)
      case other =>
        throw new IllegalArgumentException(s"Unexpected event $other")
    }
}

sealed abstract class ProtocolEvent(val eventType: ProtocolEventType)
    extends StreamEvent

final case class StreamStart(ts: Timestamp) extends ProtocolEvent(EventType.Start)

final case class StreamError(code: String, description: String)
    extends ProtocolEvent(EventType.Error)

object StreamError {

  val PermissionDenied =
    StreamError(
      "permission denied",
      "Authorization lost during stream evaluation."
    )

  val RequestTimeout =
    StreamError(
      "request timeout",
      "Request timed out while opening stream."
    )

  val TooManyRequests =
    StreamError(
      "too many requests",
      "Too many open streams."
    )

  def ServiceTimeout(reason: String) =
    StreamError("time out", reason)

  def InternalServerError(err: Throwable) =
    StreamError(
      "internal server error",
      s"${err.toString}\nPlease create a ticket at https://support.fauna.com"
    )
}

sealed abstract class VersionEvent(val eventType: VersionEventType)
    extends StreamEvent

object VersionAdded {

  def apply(txnTS: Timestamp, write: VersionAdd): VersionAdded =
    EventType(txnTS, write.writeTS.resolve(txnTS).validTS) match {
      case EventType.Version        => NewVersionAdded(write)
      case EventType.HistoryRewrite => HistoryRewrite(write)
    }
}

sealed abstract class VersionAdded(eventType: VersionEventType)
    extends VersionEvent(eventType) {
  val write: VersionAdd
}

final case class NewVersionAdded(write: VersionAdd)
    extends VersionAdded(EventType.Version)

final case class HistoryRewrite(write: VersionAdd)
    extends VersionAdded(EventType.HistoryRewrite)

final case class VersionRemoved(write: VersionRemove)
    extends VersionEvent(EventType.HistoryRewrite)

final case class DocumentRemoved(write: DocRemove)
    extends VersionEvent(EventType.HistoryRewrite)

sealed abstract class SetEvent(val eventType: SetEventType) extends StreamEvent {
  val write: SetWrite
  val isPartitioned: Boolean
}

final case class SetAdded(write: SetAdd, isPartitioned: Boolean)
    extends SetEvent(EventType.Set)

final case class SetRemoved(write: SetRemove, isPartitioned: Boolean)
    extends SetEvent(EventType.Set)
