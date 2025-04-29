package fauna.model.stream

import fauna.ast._
import fauna.lang._

sealed abstract class EventType(val name: String) {
  def toLiteral = StringL(name)
}
sealed abstract class ProtocolEventType(name: String) extends EventType(name)
sealed abstract class VersionEventType(name: String) extends EventType(name)
sealed abstract class SetEventType(name: String) extends EventType(name)

object EventType {
  case object Start extends ProtocolEventType("start")
  case object Error extends ProtocolEventType("error")
  case object Version extends VersionEventType("version")
  case object HistoryRewrite extends VersionEventType("history_rewrite")
  case object Set extends SetEventType("set")

  def apply(txnTS: Timestamp, validTS: Timestamp): VersionEventType =
    if (txnTS == validTS) {
      Version
    } else {
      HistoryRewrite
    }
}
