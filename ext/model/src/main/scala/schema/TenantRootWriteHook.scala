package fauna.model.schema

import fauna.model.Database
import fauna.repo.query.Query
import fauna.repo.schema.{ CollectionSchema, ConstraintFailure, WriteHook }
import fauna.repo.schema.ConstraintFailure.TenantRootWriteFailure

object TenantRootWriteHook {
  val writeHook = WriteHook {
    case (schema, WriteHook.OnCreate(_, _)) =>
      processWrite(schema)
    case _ => Query.value(Nil)
  }

  private def processWrite(
    collectionSchema: CollectionSchema): Query[Seq[ConstraintFailure]] = {
    Database
      .forScope(collectionSchema.scope)
      .map(_.fold(false)(_.isCustomerTenantRoot)) map { isTenantRoot =>
      if (isTenantRoot) {
        Seq(TenantRootWriteFailure(collectionSchema.name))
      } else {
        Nil
      }
    }
  }
}
