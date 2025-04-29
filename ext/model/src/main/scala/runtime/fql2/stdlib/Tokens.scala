package fauna.model.runtime.fql2.stdlib

import fauna.atoms.TokenID
import fauna.auth.TokenLike
import fauna.lang.syntax._
import fauna.model.{ Database, Token }
import fauna.model.runtime.fql2.{
  FQLInterpCtx,
  IndexSet,
  QueryRuntimeFailure,
  Result,
  ValueOps,
  WriteBroker
}
import fauna.model.runtime.Effect
import fauna.model.schema.{ NativeIndex, PublicCollection }
import fauna.repo.schema.DataMode
import fauna.repo.values.Value
import fauna.repo.Store
import fauna.storage.doc.Diff
import fauna.storage.index.IndexTerm
import fql.typer.Type

object TokenPrototype extends AbstractDocPrototype(PublicCollection.Token.name) {

  overrideMethod("replace" -> docType)("data" -> tt.AnyStruct) {
    (ctx, self, fields) =>
      replace(ctx, self, fields)
  }

  def create(ctx: FQLInterpCtx, fields: Value.Struct) = {
    Database.forScope(ctx.scopeID) flatMap {
      case Some(db) =>
        PublicCollection.Token(ctx.scopeID) flatMap { config =>
          val schema = config.Schema

          WriteBroker.createDocument(
            ctx,
            TokenID.collID,
            DataMode.Default,
            fields) flatMapT { actual =>
            val tl = TokenLike(actual.id.as[TokenID], db.globalID)

            val update = Diff(
              Token.HashField -> Some(tl.hashedSecret),
              Token.GlobalIDField -> db.globalID
            )

            Store.internalUpdate(schema, actual.id, update) map { vers =>
              // the returned token contains the secret, so we need to hack in a
              // version with that field.
              val toDisplay = vers.withData(
                vers.data.update(
                  Token.SecretField -> Some(tl.toBase64),
                  Token.HashField -> None))

              Result.Ok(Value.Doc(vers.id, versionOverride = Some(toDisplay)))
            }
          }
        }

      case None =>
        QueryRuntimeFailure.PermissionDenied(ctx.stackTrace).toQuery
    }
  }

  def replace(ctx: FQLInterpCtx, doc: Value.Doc, fields: Value.Struct) =
    PublicCollection.Token(ctx.scopeID) flatMap { config =>
      val schema = config.Schema

      Store.get(schema, doc.id) flatMap {
        case Some(prev) =>
          WriteBroker.replaceDocument(ctx, doc, DataMode.Default, fields) flatMapT {
            _ =>
              // replace needs to preserve hashed_secret & global_id fields
              val toUpdate = Diff(
                Token.HashField -> prev.data(Token.HashField),
                Token.GlobalIDField -> prev.data(Token.GlobalIDField)
              )

              Store.internalUpdate(schema, doc.id, toUpdate) map { vers =>
                Value.Doc(vers.id).toResult
              }
          }

        case None =>
          QueryRuntimeFailure
            .DocumentNotFound(schema.name, doc, ctx.stackTrace)
            .toQuery
      }
    }
}

object TokenCompanion
    extends IDedCollectionCompanion(PublicCollection.Token, TokenPrototype) {

  override lazy val docImplType = Type
    .Record(
      "id" -> Type.ID,
      "ts" -> Type.Time,
      "coll" -> TokenCompanion.selfType,
      "document" -> tt.AnyDoc.staticType,
      "secret" -> Type.Optional(Type.Str),
      "ttl" -> Type.Optional(Type.Time),
      "data" -> Type.Optional(Type.AnyRecord)
    )
    .typescheme
  override lazy val docCreateType = Type.Record(
    "id" -> Type.Optional(Type.ID),
    "document" -> Type.Union(Type.AnyDoc, Type.AnyNullDoc),
    "ttl" -> Type.Optional(Type.Time),
    "data" -> Type.Optional(Type.AnyRecord)
  )

  overrideStaticFunction("create" -> docType)("data" -> tt.Struct(docCreateType)) {
    (ctx, fields) => TokenPrototype.create(ctx, fields)
  }

  defStaticFunction("byDocument" -> docSetType)("document" -> tt.AnyDoc) {
    (ctx, document) =>
      Effect.Action.Function("byDocument").check(ctx, Effect.Read).flatMapT { _ =>
        val cfg = NativeIndex.TokenByDocument(ctx.scopeID)
        val terms = Vector(IndexTerm(document.id))
        IndexSet(
          this,
          "byDocument",
          Vector(document),
          cfg,
          terms,
          ctx.userValidTime,
          ctx.stackTrace.currentStackFrame
        ).toQuery
      }
  }
}
