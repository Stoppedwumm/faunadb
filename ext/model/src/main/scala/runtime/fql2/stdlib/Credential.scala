package fauna.model.runtime.fql2.stdlib

import fauna.atoms.CredentialsID
import fauna.model.runtime.fql2._
import fauna.model.runtime.Effect
import fauna.model.schema.PublicCollection
import fauna.model.Credentials
import fauna.repo.query.Query
import fauna.repo.values.{ Value, ValueType }
import fql.typer.Type

object CredentialPrototype
    extends AbstractDocPrototype(PublicCollection.Credential.name) {

  private val tokenType = TokenCompanion.docType

  defMethod("verify" -> tt.Boolean)("secret" -> tt.Str) { (ctx, self, secret) =>
    getCredential(ctx, self, Effect.Action.Function("verify")).flatMap { cred =>
      Value.Boolean(cred.matches(secret.value)).toQuery
    }
  }

  defMethod("login" -> tokenType)("secret" -> tt.Str) { (ctx, self, secret) =>
    login(ctx, self, secret.value, None)
  }

  defMethod("login" -> tokenType)("secret" -> tt.Str, "ttl" -> tt.Time) {
    (ctx, self, secret, ttl) =>
      login(ctx, self, secret.value, Some(ttl))
  }

  private def getCredential(
    ctx: FQLInterpCtx,
    id: Value.Doc,
    action: Effect.Action) =
    ReadBroker.get(ctx, id, action) map { res => Credentials(res.unsafeGet) }

  private def login(
    ctx: FQLInterpCtx,
    cred: Value.Doc,
    secret: String,
    ttl: Option[Value.Time]
  ): Query[Result[Value.Doc]] = {
    getCredential(ctx, cred, Effect.Action.Function("login")) flatMap { cred =>
      if (cred.matches(secret)) {
        val data = ttl match {
          case Some(ttl) =>
            Value.Struct(
              "document" -> Value.Doc(cred.documentID),
              "ttl" -> ttl
            )

          case None =>
            Value.Struct("document" -> Value.Doc(cred.documentID))
        }

        TokenPrototype.create(ctx, data)
      } else {
        QueryRuntimeFailure.InvalidSecret(ctx.stackTrace).toQuery
      }
    }
  }
}

object CredentialCompanion
    extends IDedCollectionCompanion(
      PublicCollection.Credential,
      CredentialPrototype) {

  override lazy val docImplType = Type
    .Record(
      "id" -> Type.ID,
      "ts" -> Type.Time,
      "coll" -> selfType.staticType,
      "document" -> tt.Ref(tt.AnyDoc).staticType,
      "data" -> Type.Optional(Type.AnyRecord)
    )
    .typescheme
  override lazy val docCreateType = tt.Struct(
    "id" -> tt.Optional(tt.ID),
    "document" -> tt.Ref(tt.AnyDoc),
    "password" -> tt.Optional(tt.Str),
    "data" -> tt.Optional(tt.AnyStruct)
  )

  defStaticFunction("byDocument" -> docRefType)(
    "document" -> tt.Union(tt.AnyDoc, tt.AnyNullDoc)) {

    case (ctx, doc: Value.Doc) =>
      Credentials.idByDocument(ctx.scopeID, doc.id) map {
        case Some(id) => Value.Doc(id.toDocID).toResult
        // FIXME: This -1 is gross, but necessary since we are looking up
        // by a secondary key. Once we revise `by*` method to return Doc | Null,
        //  this can go away.
        case None => Value.Doc(CredentialsID(-1).toDocID).toResult
      }
    case (ctx, doc) =>
      QueryRuntimeFailure
        .InvalidArgumentType(
          "document",
          ValueType.AnyDocType,
          doc.dynamicType,
          ctx.stackTrace)
        .toQuery
  }
}
