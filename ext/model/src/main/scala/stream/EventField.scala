package fauna.model.stream

sealed abstract class EventField(val name: String)

object EventField {
  case object Action extends EventField("action")
  case object Document extends EventField("document")
  case object Prev extends EventField("prev")
  case object Diff extends EventField("diff")
  case object Index extends EventField("index")

  val All = Set[EventField](Action, Document, Diff, Prev, Index)

  val Defaults = Set[EventField](Action, Document)

  val ByName = All map { f => (f.name, f) } toMap

  def apply(name: String): Option[EventField] = ByName.get(name)
}
