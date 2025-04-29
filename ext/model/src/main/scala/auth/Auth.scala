package fauna.auth

import fauna.ast._
import fauna.atoms._
import fauna.lang._
import fauna.lang.syntax._
import fauna.model._
import fauna.net.http.{ AuthType, BasicAuth, BearerAuth }
import fauna.net.security.{ JWKProvider, JWT }
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.repo.service.rateLimits.OpsLimiter
import fauna.repo.service.rateLimits.PermissiveOpsLimiter
import fauna.repo.values.Value
import fauna.scheduler._
import fauna.storage.{ Create, DocAction }
import fauna.storage.doc._
import fauna.storage.index._
import fauna.util.BCrypt
import scala.annotation.unused
import scala.util.Success

/** With the introduction of JWT arose the need to explain how this new
  * feature works and how it integrate with the old key/token system.
  *
  * JWT stands for JSON Web Token and it's a JSON representing claims
  * about a subject. JWT tokens are encoded using base64 and is composed by 3
  * components separated by dots. The first part is the Header and contains
  * information about the token itself. The second part is the Payload and
  * contains a set of claims about a subject. The third part is the Signature
  * that validate the integrity of the token.
  *
  * JWT is issued by a third party provider that's is responsible to authenticate
  * the user (ie: verify a username/password), then a token is returned to the user
  * with a set of claims, that token can then be used to request access to many
  * different systems, as it carry all the information needed to identify the user.
  *
  * In order to our users use JWT with Fauna, they need to configure a Access Provider.
  *
  * The Access Providers contains relevant information needed to validate if the tokens
  * are authentic or not, such as:
  *
  * issuer - uniquely identify the provider and usually is associated with user's account in the provider.
  * jwks_uri - points to the public set of keys used to sign the token.
  * allowed_roles - a set of roles allowed to participate on JWT authentication process.
  * allowed_collections - a set of collections allowed to participate on JWT authentication process.
  *
  * JWT validation:
  *
  * To be considered valid a JWT need to have a set of valid claims
  *
  * "iss" - represents the issuer, ie: https://dev-xpto.auth0.com
  * "aud" - represents the audience, in Fauna this will represent the scope/database and should
  *         have the following pattern "https://db.fauna.com/db/<global_id>"
  *         global_id is an unique string that represents a database globally.
  * "exp" - indicates when the token will expire and then is not valid anymore.
  * "nbf" - indicates that the token will only be considered valid after this time.
  * "scope" - represents a document or role to determine the permissions the JWT will have
  *           for documents it should have the following pattern "@doc/<collectionName>/<id>"
  *           for roles it should have the following pattern "@role/<roleName>"
  *           scope is a space separated list of strings, but only one document or role can exist
  *           at the same time for the token to be considered valid
  *
  * If all the above fields are present and valid, then the Access Provider is consulted to get the jwks_uri
  * in order to get the public keys to validate the token signature.
  *
  * If the token signature is valid, then the Access Provider is consulted again to check if the "valid_roles"
  * or "valid_collections" contains the "scope" claim in the token.
  *
  * If it contains then the JWT is considered valid and the user allowed to execute the query.
  *
  * To enable JWT, users need to sent JWT tokens on "Authorization" header using "Bearer" type.
  *   ie: "Authorization: Bearer xxx.yyy.zzz"
  *
  * However, the old key/tokens are now allowed to be passed on "Authorization: Bearer" header, so in order to distinguish
  * between key/token and the new JWT tokens it checks if the token contains dots as the old key/tokens doesn't have it.
  *
  * Besides this changes, the good old key/tokens system keeps working the same way.
  */
object Auth {
  private[this] val AuthLookupMetric = "Query.Auth.Time"
  private[this] val JWTAuthMetric = "Query.JWT.Auth"
  private[this] val JWTAuthLookupMetric = "Query.JWT.Auth.Time"

  def lookup(
    secret: String,
    authScope: AuthScope = NullAuthScope): Query[Option[Auth]] =
    AuthLike.fromBase64(secret) match {
      case Some(key: KeyLike)     => keyAuth(key, authScope)
      case Some(token: TokenLike) => tokenAuth(token, authScope)
      case None                   => Query.none
    }

  def fromAuth(auth: String, rootKeys: List[String]): Query[Option[Auth]] =
    AuthParser(auth).parse() match {
      case Success((secret, authScope)) =>
        lookup(secret, authScope) orElseT rootAuth(secret, rootKeys, authScope)

      case _ =>
        Query.none
    }

  def fromJwt(token: String, jwkProvider: JWKProvider): Query[Option[Auth]] = {
    Query.timing(JWTAuthLookupMetric) {
      JWTToken.fromToken(token, jwkProvider) flatMapT { auth =>
        Query.stats map { stats =>
          stats.incr(JWTAuthMetric)
          Some(auth)
        }
      }
    }
  }

  /** Primary entry point into the authentication system for incoming
    * requests. See CoreApplication.
    *
    * Returns a successfully-authenticated instance of Auth, or None
    * if authentication failed/was denied.
    */
  def fromInfo(
    auth: AuthType,
    rootKeys: List[String],
    jwkProvider: JWKProvider): Query[Option[Auth]] =
    auth match {
      case BearerAuth(token, _) if AuthLike.isJWT(token) =>
        fromJwt(token, jwkProvider)
      case BearerAuth(token, _) => fromAuth(token, rootKeys)
      case BasicAuth(auth, _)   => fromAuth(auth, rootKeys)
      case _                    => Query.none
    }

  // For console purposes
  def forScope(id: ScopeID, key: Option[Key] = None)(
    implicit @unused ctl: ConsoleControl): Auth =
    mkAuth(id, ServerPermissions, key)

  def clientForScope(id: ScopeID, key: Option[Key] = None)(
    implicit @unused ctl: ConsoleControl): Auth =
    mkAuth(id, NullPermissions, key)

  def adminForScope(id: ScopeID, key: Option[Key] = None)(
    implicit @unused ctl: ConsoleControl): Auth =
    mkAuth(id, AdminPermissions, key)

  def changeRole(auth: Auth, role: Key.Role): Query[Auth] = {
    val permQ = role match {
      case Key.AdminRole          => Query.value(AdminPermissions)
      case Key.ServerRole         => Query.value(ServerPermissions)
      case Key.ServerReadOnlyRole => Query.value(ServerReadOnlyPermissions)
      case Key.ClientRole         => Query.value(NullPermissions)
      case custom @ Key.UserRoles(_) =>
        custom.roleIDs(auth.scopeID) flatMap {
          RolePermissions.lookup(auth.scopeID, _)
        }
    }

    permQ map { auth.withPermissions }
  }

  def isRoot(auth: Auth) =
    auth match {
      case RootAuth => true
      case _        => false
    }

  /** Tap into the cache to quickly determine if the given auth is still valid. */
  def revalidate(auth: Auth): Query[Boolean] = {
    def isTTLValid(ttlOpt: Option[Timestamp]): Query[Boolean] =
      Query.snapshotTime map { ts => ttlOpt forall { _ >= ts } }

    def revalidateKey(key: Key): Query[Boolean] =
      isTTLValid(key.ttl) flatMap { valid =>
        if (valid) {
          Key.forGlobalID(key.globalID) existsT { key == _ }
        } else {
          Query.False
        }
      }

    def revalidateToken(token: Token): Query[Boolean] =
      isTTLValid(token.ttl) flatMap { valid =>
        if (valid) {
          Token.getLatestCached(token.scopeID, token.id) existsT { token == _ }
        } else {
          Query.False
        }
      }

    def revalidateRoles(scope: ScopeID, perms: Permissions): Query[Boolean] =
      perms match {
        case perms: RoleBasedPermissions =>
          val rec = perms.context
          RoleEvalContext.lookup(scope, rec.roleIDs) map { _ == rec }
        case _ => Query.True
      }

    val authValidQ =
      auth match {
        case RootAuth => Query.True
        case login: LoginAuth =>
          login.source match {
            case RootLogin         => Query.True
            case KeyLogin(key)     => revalidateKey(key)
            case TokenLogin(token) => revalidateToken(token)
            case JWTLogin(jwt, _)  => isTTLValid(jwt.getExpiresAt.map(Timestamp(_)))
          }
      }

    authValidQ flatMap { authValid =>
      if (authValid) {
        revalidateRoles(auth.scopeID, auth.permissions)
      } else {
        Query.False
      }
    }
  }

  private def mkAuth(id: ScopeID, perms: Permissions, key: Option[Key]): Auth =
    key map {
      KeyAuth(id, Database.RootDatabase, Database.RootDatabase, _, perms)
    } getOrElse {
      SystemAuth(id, Database.RootDatabase, perms)
    }

  private def keyAuth(keyLike: KeyLike, scope: AuthScope) =
    Query.timing(AuthLookupMetric) {
      Query.snapshotTime flatMap { snapTs =>
        Key.forKeyLike(keyLike) rejectT { key =>
          key.ttl exists { snapTs >= _ }
        } flatMapT {
          scope.getKeyAuth(_)
        }
      }
    }

  private def tokenAuth(tokenLike: TokenLike, scope: AuthScope) =
    Query.timing(AuthLookupMetric) {
      Query.snapshotTime flatMap { snapTs =>
        Token.forTokenLike(tokenLike) rejectT { token =>
          token.ttl exists { snapTs >= _ }
        } flatMapT {
          scope.getTokenAuth(_)
        }
      }
    }

  private def rootAuth(secret: String, rootKeys: List[String], scope: AuthScope) = {
    Query.defer {
      if (rootKeys exists { BCrypt.check(secret, _) }) {
        scope.getRootAuth()
      } else {
        Query.none
      }
    }
  }
}

sealed trait Auth {

  def scopeID: ScopeID

  def priorityGroup: PriorityGroup

  def database: Database

  def limiter: OpsLimiter

  def permissions: Permissions

  def source: LoginSource

  def withPermissions(perms: Permissions): Auth

  final def accountID = database.accountID

  final def checkReadPermission(
    scope: ScopeID,
    id: DocID,
    snapshotTime: Option[Timestamp] = None): Query[Boolean] =
    permissions.checkAction(scopeID, ReadInstance(scope, id, snapshotTime), source)

  final def checkReadCollectionPermission(
    scope: ScopeID,
    id: CollectionID): Query[Boolean] =
    permissions.hasAction(scope, id.toDocID, ActionNames.Read)

  final def checkHistoryReadPermission(scope: ScopeID, id: DocID): Query[Boolean] = {

    permissions.checkAction(scopeID, HistoryRead(scope, id), source)
  }

  final def checkReadIndexPermission(
    scope: ScopeID,
    id: IndexID,
    terms: Vector[IndexTerm]): Query[Boolean] = {

    permissions.checkAction(scopeID, ReadIndex(scope, id, terms), source)
  }

  final def checkUnrestrictedReadPermission(
    scope: ScopeID,
    id: CollectionID): Query[Boolean] = {

    permissions.checkAction(scopeID, UnrestrictedCollectionRead(scope, id), source)
  }

  final def checkUnrestrictedReadPermission(
    scope: ScopeID,
    id: IndexID,
    terms: Vector[IndexTerm]): Query[Boolean] = {

    permissions.checkAction(scopeID, UnrestrictedIndexRead(scope, id, terms), source)
  }

  final def checkWritePermission(
    scope: ScopeID,
    id: DocID,
    prevVersion: Version.Live,
    newData: Diff): Query[Boolean] = {
    checkWritePermission(
      scope,
      id,
      prevVersion,
      prevVersion
        .withData(prevVersion.data.patch(newData))
        .withUnresolvedTS
    )
  }

  final def checkWritePermission(
    scope: ScopeID,
    id: DocID,
    prevVersion: Version.Live,
    newVersion: Version.Live): Query[Boolean] = {
    permissions.checkAction(
      scopeID,
      WriteInstance(scope, id, prevVersion, newVersion),
      source)
  }

  final def checkHistoryWritePermission(
    scope: ScopeID,
    id: DocID,
    ts: Timestamp,
    action: DocAction,
    data: Diff): Query[Boolean] = {

    permissions.checkAction(
      scopeID,
      HistoryWrite(scope, id, ts, action, data),
      source)
  }

  final def checkCreatePermission(
    scope: ScopeID,
    newVersion: Version.Live): Query[Boolean] = {

    permissions.checkAction(scopeID, CreateInstance(scope, newVersion), source)
  }

  // If createWithID returns false, then delegates to create + history write
  // permission to preserve existing permission behavior.
  final def checkCreateWithIDPermission(
    scope: ScopeID,
    newVersion: Version.Live): Query[Boolean] = {

    permissions
      .checkAction(scopeID, CreateInstanceWithID(scope, newVersion), source)
      .flatMap {
        case true => Query.value(true)
        case false =>
          Query.snapshotTime flatMap { ts =>
            (
              checkCreatePermission(scope, newVersion),
              checkHistoryWritePermission(
                scope,
                newVersion.id,
                ts,
                Create,
                Diff(newVersion.data.fields))) par { (a, b) =>
              Query.value(a && b)
            }
          }
      }
  }

  final def checkDeletePermission(scope: ScopeID, id: DocID): Query[Boolean] =
    permissions.checkAction(scopeID, DeleteInstance(scope, id), source)

  final def checkCallPermission(
    scope: ScopeID,
    id: UserFunctionID,
    args: Either[Literal, IndexedSeq[Value]]): Query[Boolean] = {

    permissions.checkAction(scopeID, CallFunction(scope, id, args), source)
  }

  final def checkReadSchemaPermission(scope: ScopeID): Query[Boolean] =
    permissions.checkAction(scopeID, ReadSchema(scope), source)

  final def checkWriteSchemaPermission(scope: ScopeID): Query[Boolean] =
    permissions.checkAction(scopeID, WriteSchema(scope), source)

  final def checkLoggingPermission(scope: ScopeID): Query[Boolean] =
    permissions.checkAction(scopeID, Logging(scope), source)

  final def isInScope(scope: ScopeID): Query[Boolean] =
    permissions.isInScope(scopeID, scope)
}

/** Represents the type of login the user is using.
  * ie: Token, Key or JWT
  */
sealed trait LoginSource

/** Indicate that user is logged in using a token
  */
case class TokenLogin(token: Token) extends LoginSource

/** Indicate that user is logged in using a key
  */
case class KeyLogin(key: Key) extends LoginSource

/** Indicate that user is logged in using a JWT token
  */
case class JWTLogin(jwt: JWT, payload: Literal) extends LoginSource

/** Indicate that user is the root user, usually only
  * tests and debug console use this.
  */
object RootLogin extends LoginSource

case object RootAuth extends Auth {
  val scopeID = Database.RootScopeID
  val database = Database.RootDatabase
  val limiter = PermissiveOpsLimiter
  val priorityGroup = PriorityGroup.Default
  val permissions = AdminPermissions
  val source = RootLogin
  def withPermissions(perms: Permissions) = this
}

/** Queries will execute within `database`, but if a scoped key was
  * provided, `authDatabase` will be the key's database.
  */
final case class LoginAuth(
  scopeID: ScopeID,
  database: Database,
  limiter: OpsLimiter,
  authDatabase: Database,
  permissions: Permissions,
  source: LoginSource,
  identity: Option[DocID]
) extends Auth {

  val priorityGroup = source match {
    case KeyLogin(key)  => key.priorityGroup
    case TokenLogin(_)  => database.priorityGroup
    case JWTLogin(_, _) => database.priorityGroup
    case RootLogin      => database.priorityGroup
  }

  def withPermissions(perms: Permissions) = copy(permissions = perms)
}

object LoginAuth {
  object Source {
    def unapply(auth: LoginAuth): Option[LoginSource] = Some(auth.source)
  }

  object Identity {
    def unapply(auth: LoginAuth): Option[DocID] = auth.identity
  }

  def get(
    scopeID: ScopeID,
    permissions: Permissions,
    source: LoginSource,
    authDatabase: Option[Database] = None,
    identity: Option[DocID] = None
  ): Query[Auth] = {
    for {
      db <- Database.forScope(scopeID).map {
        case Some(db) => db
        case None =>
          throw new IllegalStateException(
            s"Auth lookup for database $scopeID failed")
      }
      account <- db.account
    } yield LoginAuth(
      scopeID,
      db,
      account.limiter,
      authDatabase.getOrElse(db),
      permissions,
      source,
      identity)
  }
}

object SystemAuth {
  def apply(scopeID: ScopeID, database: Database, permissions: Permissions): Auth =
    LoginAuth(
      scopeID,
      database,
      PermissiveOpsLimiter,
      database,
      permissions,
      RootLogin,
      None)

  def get(
    scopeID: ScopeID,
    permissions: Permissions,
    authDatabase: Option[Database] = None): Query[Auth] =
    LoginAuth.get(scopeID, permissions, RootLogin, authDatabase = authDatabase)
}

object KeyAuth {
  def apply(
    scopeID: ScopeID,
    database: Database,
    authDatabase: Database,
    key: Key,
    permissions: Permissions): Auth =
    LoginAuth(
      scopeID,
      database,
      PermissiveOpsLimiter,
      authDatabase,
      permissions,
      KeyLogin(key),
      None)

  def get(
    scopeID: ScopeID,
    key: Key,
    permissions: Permissions,
    authDatabase: Option[Database] = None): Query[Auth] =
    LoginAuth.get(scopeID, permissions, KeyLogin(key), authDatabase = authDatabase)
}

object EvalAuth {
  def apply(scopeID: ScopeID, permissions: Permissions): Auth =
    LoginAuth(
      scopeID,
      Database.RootDatabase,
      PermissiveOpsLimiter,
      Database.RootDatabase,
      permissions,
      RootLogin,
      None)

  def apply(
    scopeID: ScopeID,
    permissions: Permissions,
    source: LoginSource,
    id: Option[DocID]): Auth =
    LoginAuth(
      scopeID,
      Database.RootDatabase,
      PermissiveOpsLimiter,
      Database.RootDatabase,
      permissions,
      source,
      id)

  def apply(scopeID: ScopeID): Auth =
    apply(scopeID, NullPermissions)

  def read(scope: ScopeID, source: LoginSource, id: Option[DocID]): Auth =
    apply(scope, InternalReadOnlyPermissions, source, id)
}
