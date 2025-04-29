package fauna.model.runtime.fql2.stdlib

import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.runtime.fql2._
import fauna.model.runtime.Effect
import fauna.model.schema.{ NativeIndex, SchemaCollection }
import fauna.repo.query.Query
import fauna.repo.values.Value
import fauna.repo.Store
import fauna.storage.index.IndexTerm
import fql.typer.Type

object DatabaseDefCompanion
    extends SchemaCollectionCompanion(
      SchemaCollection.Database,
      NativeDocPrototype(
        SchemaCollection.Database.name,
        docType = TypeTag.NamedDoc(s"${SchemaCollection.Database.name}Def"),
        nullDocType = TypeTag.NullDoc(s"${SchemaCollection.Database.name}Def")
      ),
      NamedNullDocPrototype
    ) {

  override protected def allDocuments(
    ctx: FQLInterpCtx,
    action: Effect.Action,
    args: Vector[Value] = Vector.empty,
    range: IndexSet.Range = IndexSet.Range.Unbounded
  ): Query[Result[ValueSet]] = {
    checkReadPermission(ctx, action) flatMapT { _ =>
      // FIXME: just replace with DatabaseByDisabled(false). We can't right
      // now because "enabled" databases are not necessarily indexed.
      val cfg = NativeIndex.DocumentsByCollection(ctx.scopeID)
      val terms = Vector(IndexTerm(collID.toDocID))

      // we need to fake a difference here since the database field is too
      // expensive get from docs.
      val disabledQ = {
        val cfg = NativeIndex.DatabaseByDisabled(ctx.scopeID)
        val terms = Vector(IndexTerm(true))
        Store
          .collection(cfg, terms.map(_.value), Timestamp.MaxMicros)
          .flattenT
          .map(ivs => ivs.iterator.map(_.docID).toSet)
      }

      disabledQ.map { disabled =>
        IndexSet(
          this,
          "all",
          args,
          cfg,
          terms,
          ctx.userValidTime,
          ctx.stackTrace.currentStackFrame,
          range,
          filter =
            Some((_, iv) => Query.value(!disabled.contains(iv.docID)))).toResult
      }
    }
  }

  override val selfType = TypeTag.Named(s"${coll.name}Collection")

  override lazy val docImplType = Type
    .Record(
      "name" -> Type.Str,
      "ts" -> Type.Time,
      "coll" -> selfType.staticType,
      "global_id" -> Type.Str,
      "priority" -> Type.Optional(Type.Number),
      "typechecked" -> Type.Optional(Type.Boolean),
      "protected" -> Type.Optional(Type.Boolean),
      "data" -> Type.Optional(Type.AnyRecord)
    )
    .typescheme
  override lazy val docCreateType = tt.Struct(
    "name" -> tt.Str,
    "priority" -> tt.Optional(tt.Number),
    "typechecked" -> tt.Optional(tt.Boolean),
    "protected" -> tt.Optional(tt.Boolean),
    "data" -> tt.Optional(tt.AnyStruct)
  )
}
