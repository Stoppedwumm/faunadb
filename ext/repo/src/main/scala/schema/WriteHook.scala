package fauna.repo.schema

import fauna.atoms.DocID
import fauna.repo.query.Query
import fauna.storage.doc.{ Data, Diff }
import fauna.storage.DocAction

final case class WriteHook(
  // if true, also apply to internal writes (Store.internal*)
  applyInternal: Boolean,
  fn: WriteHook.HookF) {
  def lift(t: (CollectionSchema, WriteHook.Event)) = fn.lift(t)
}

object WriteHook {
  type HookF =
    PartialFunction[(CollectionSchema, Event), Query[Seq[ConstraintFailure]]]

  def apply(fn: HookF): WriteHook = WriteHook(false, fn)

  object Event {
    def apply(id: DocID, prev: Option[Data], updated: Option[Data]) =
      ((prev, updated): @unchecked) match {
        case ((None, Some(d)))    => OnCreate(id, d)
        case ((Some(p), Some(u))) => OnUpdate(id, p, u)
        case ((Some(p), None))    => OnDelete(id, p)
      }
  }

  sealed trait Event {
    def id: DocID
    def action: DocAction
    def prevData = this match {
      case OnCreate(_, _)       => None
      case OnUpdate(_, prev, _) => Some(prev)
      case OnDelete(_, prev)    => Some(prev)
    }
    def newData = this match {
      case OnCreate(_, data)    => Some(data)
      case OnUpdate(_, _, data) => Some(data)
      case OnDelete(_, _)       => None
    }
    def diffOpt: Option[Diff] = prevData.map(newData.getOrElse(Data.empty).diffTo)
    def isCreate = action == DocAction.Create
    def isUpdate = action == DocAction.Update
    def isDelete = action == DocAction.Delete
  }

  case class OnCreate(id: DocID, data: Data) extends Event {
    def action = DocAction.Create
  }
  case class OnUpdate(id: DocID, prev: Data, data: Data) extends Event {
    def action = DocAction.Update
  }
  case class OnDelete(id: DocID, prev: Data) extends Event {
    def action = DocAction.Delete
  }
}
