package fauna.model

import fauna.ast._
import fauna.atoms._
import fauna.auth.TokenLike
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.schema.{ NativeIndex, PublicCollection }
import fauna.repo._
import fauna.repo.cache.CacheKey
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.storage.api.set._
import fauna.storage.doc._
import fauna.storage.ir._

case class Token(
  id: TokenID,
  scopeID: ScopeID,
  globalID: GlobalDatabaseID,
  document: Option[DocID],
  hashedSecret: Option[String],
  deletedTS: Option[Timestamp],
  ttl: Option[Timestamp]) {

  def credentials =
    if (isDeleted) Query.none
    else {
      Query(document) flatMapT { Credentials.getByDocument(scopeID, _) }
    }

  def matches(tokenLike: TokenLike) =
    !isDeleted && (hashedSecret match {
      case Some(hashed) => tokenLike.matches(id, globalID, hashed)
      case _            => false
    })

  def isDeleted = deletedTS.isDefined
}

object Token {

  def apply(live: Version, latest: Version): Token = {
    // N.B. Legacy tokens doesn't have global_id field so lets default it to parent
    // scope id and leverage
    // fallback logic on Database.forGlobalID()
    val globalID =
      live.data.getOrElse(GlobalIDField, GlobalDatabaseID(live.parentScopeID.toLong))

    Token(
      live.id.as[TokenID],
      live.parentScopeID,
      globalID,
      live.data(DocumentField),
      live.data(HashField),
      if (latest.isDeleted) Some(latest.ts.validTS) else None,
      live.data(Version.TTLField)
    )
  }

  val SecretField = Field[Option[String]]("secret")
  val HashField = Field[Option[String]]("hashed_secret")
  val DocumentField = Field[Option[DocID]]("instance")
  val GlobalIDField = Field[GlobalDatabaseID]("global_id")

  object OwnerValidator extends Validator[Query] {
    val filterMask = MaskTree(DocumentField.path)

    override def validateData(data: Data): Query[List[ValidationException]] = {
      val inst = data.fields.get(DocumentField.path)

      inst match {
        case None =>
          Query(List(ValueRequired(DocumentField.path)))
        case _ => Query(Nil)
      }
    }
  }

  object GlobalIDValidator extends Validator[Query] {
    val filterMask = MaskTree(GlobalIDField.path)
  }

  val Validator =
    OwnerValidator +
      GlobalIDValidator +
      DocumentField.validator +
      HashField.validator +
      Version.TTLField.validator

  private case class CKey(domain: ScopeID, id: TokenID) extends CacheKey[Token] {
    def query = getLatest(domain, id)
  }

  /** Uncached get by TokenID */
  def getLatest(scope: ScopeID, id: TokenID): Query[Option[Token]] = {
    val liveQ = PublicCollection.Token(scope).getVersionLiveNoTTL(id)
    val latestQ = PublicCollection.Token(scope).getVersionNoTTL(id)

    (liveQ, latestQ) parT { (live, latest) => Query.some(Token(live, latest)) }
  }

  def getLatestCached(globalID: GlobalID, id: TokenID): Query[Option[Token]] =
    Query.timing("Token.Cached.Get") {
      Query.repo flatMap { repo =>
        val databaseQ = globalID match {
          case id @ GlobalDatabaseID(_) =>
            Database.forGlobalID(id)

          case ScopeID(id) =>
            // Tokens created after ENG-XXX will have scope != global_id.
            // Lets consider it as global_id and use the fallback logic inside
            // Database.forGlobalID()
            Database.forGlobalID(GlobalDatabaseID(id))

          case GlobalKeyID(_) =>
            Query.none
        }

        databaseQ flatMapT { db =>
          repo.cacheContext.keys.get(CKey(db.scopeID, id))
        }
      }
    }

  def get(scope: ScopeID, id: TokenID): Query[Option[Token]] =
    getLatest(scope, id) rejectT { _.isDeleted }

  def forTokenLike(token: TokenLike): Query[Option[Token]] =
    getLatestCached(token.domain, token.id) rejectT { tkn =>
      !tkn.matches(token) || tkn.isDeleted
    }

  def removeByDocument(
    scope: ScopeID,
    id: DocID,
    snapshotTS: Timestamp): Query[Unit] = {
    val terms = Vector(Scalar(DocIDV(id)))
    Store.collection(
      NativeIndex.TokenByDocument(scope),
      terms,
      snapshotTS) foreachValueT { token =>
      removeToken(scope, token.docID.as[TokenID])
    }
  }

  def removeToken(scope: ScopeID, id: TokenID): Query[Unit] =
    PublicCollection.Token(scope).internalDelete(id).join

  def invalidateCaches(scope: ScopeID, id: TokenID): Query[Unit] =
    Query.repo map { _.cacheContext.keys.invalidate(CKey(scope, id)) }

  // helpers

  def validationFailed(errs: List[ValidationException]) = List(
    ValidationError(errs, RootPosition))

  val loginFailed = List(AuthenticationFailed(RootPosition))
}
