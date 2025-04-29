package fauna.model.runtime.fql2.stdlib

import fauna.atoms.{ GlobalKeyID, KeyID }
import fauna.auth.KeyLike
import fauna.lang.syntax._
import fauna.model.runtime.fql2.{
  FQLInterpCtx,
  QueryRuntimeFailure,
  Result,
  ValueOps,
  WriteBroker
}
import fauna.model.schema.PublicCollection
import fauna.model.Key
import fauna.model.LookupHelpers
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.repo.schema.DataMode
import fauna.repo.store.LookupStore
import fauna.repo.values.Value
import fauna.repo.Store
import fauna.storage.{ Remove, Unresolved }
import fauna.storage.doc.Diff
import fauna.storage.lookup.LiveLookup
import fql.typer.Type

object KeyPrototype extends AbstractDocPrototype(PublicCollection.Key.name) {

  overrideMethod("update" -> docType)("data" -> tt.AnyStruct) {
    (ctx, self, fields) =>
      update(ctx, self, fields)
  }

  overrideMethod("replace" -> docType)("data" -> tt.AnyStruct) {
    (ctx, self, fields) =>
      replace(ctx, self, fields)
  }

  overrideMethod("delete" -> nullDocType)() { (ctx, self) =>
    delete(ctx, self)
  }

  private def updateLookups(write: Query[Result[Version.Live]]) =
    write flatMapT { actual =>
      val lookups = LookupHelpers.lookups(actual)
      val addsQ = lookups map {
        LookupStore.add(_)
      }

      addsQ.join map { _ =>
        Result.Ok(actual)
      }
    }

  def create(ctx: FQLInterpCtx, fields: Value.Struct) =
    PublicCollection.Key(ctx.scopeID) flatMap { config =>
      val schema = config.Schema

      updateLookups(
        WriteBroker.createDocumentVers(
          ctx,
          KeyID.collID,
          DataMode.Default,
          fields
        )) flatMapT { actual =>
        val globalID = GlobalKeyID(actual.id.as[KeyID].toLong)
        val kl = KeyLike(globalID)

        val toUpdate = Diff(Key.HashField -> Some(kl.hashedSecret))
        Store.internalUpdate(schema, actual.id, toUpdate) map { vers =>
          // the returned token contains the secret, so we need to hack in a
          // version with that field.
          val toDisplay =
            vers.withData(
              vers.data
                .update(Key.SecretField -> Some(kl.toBase64), Key.HashField -> None))

          Result.Ok(Value.Doc(vers.id, versionOverride = Some(toDisplay)))
        }
      }
    }

  private def update(
    ctx: FQLInterpCtx,
    doc: Value.Doc,
    fields: Value.Struct
  ) =
    updateLookups(
      WriteBroker
        .updateDocumentVers(ctx, doc, DataMode.Default, fields)).mapT { _ =>
      doc
    }

  private def replace(
    ctx: FQLInterpCtx,
    doc: Value.Doc,
    fields: Value.Struct
  ) =
    PublicCollection.Key(ctx.scopeID) flatMap { config =>
      val schema = config.Schema

      Store.get(schema, doc.id) flatMap {
        case Some(prev) =>
          updateLookups(
            WriteBroker
              .replaceDocumentVers(ctx, doc, DataMode.Default, fields)) flatMapT {
            _ =>
              // replace needs to preserve hashed_secret field
              val toUpdate = Diff(Key.HashField -> prev.data(Key.HashField))

              Store.internalUpdate(schema, doc.id, toUpdate) map { _ =>
                doc.toResult
              }
          }

        case None =>
          QueryRuntimeFailure
            .DocumentNotFound(schema.name, doc, ctx.stackTrace)
            .toQuery
      }
    }

  private def delete(ctx: FQLInterpCtx, doc: Value.Doc) =
    WriteBroker.deleteDocument(ctx, doc) flatMapT { st =>
      val lookup = LiveLookup(
        GlobalKeyID(doc.id.as[KeyID].toLong),
        ctx.scopeID,
        doc.id,
        Unresolved,
        Remove)
      LookupStore.add(lookup) map { _ => st.toResult }
    }
}

object KeyCompanion
    extends IDedCollectionCompanion(PublicCollection.Key, KeyPrototype) {

  override lazy val docImplType = Type
    .Record(
      "id" -> Type.ID,
      "ts" -> Type.Time,
      "coll" -> selfType.staticType,
      "role" -> Type.Str,
      "database" -> Type.Optional(Type.Str),
      "ttl" -> Type.Optional(Type.Time),
      "secret" -> Type.Optional(Type.Str),
      "data" -> Type.Optional(Type.AnyRecord)
    )
    .typescheme

  override lazy val docCreateType = Type.Record(
    "role" -> Type.Str,
    "database" -> Type.Optional(Type.Str),
    "ttl" -> Type.Optional(Type.Time),
    "data" -> Type.Optional(Type.AnyRecord)
  )

  overrideStaticFunction("create" -> docType)("data" -> tt.Struct(docCreateType)) {
    (ctx, fields) => KeyPrototype.create(ctx, fields)
  }
}
