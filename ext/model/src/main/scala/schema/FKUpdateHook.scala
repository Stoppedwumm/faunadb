package fauna.model.schema

import fauna.lang.syntax._
import fauna.repo.query.Query
import fauna.repo.schema.WriteHook

object FKUpdateHook {
  import ForeignKey._
  def apply(srcs: Src*) = WriteHook {
    case (_, WriteHook.OnCreate(_, _)) =>
      Query.value(Nil)
    case (_, WriteHook.OnDelete(_, _)) =>
      Query.value(Nil)
    case (schema, updateEvent: WriteHook.OnUpdate) =>
      val validations = for {
        newData <- updateEvent.newData.toSeq
        diff    <- updateEvent.diffOpt.toSeq
        src     <- srcs
        prev    <- diff.fields.get(src.path).toSeq
        next    <- newData.fields.get(src.path).toSeq
        target  <- src.targets
      } yield {
        target.loc.rename(
          schema.scope,
          target.path,
          prev,
          next
        )
      }
      validations.sequence.map(_ => List.empty)
  }
}
