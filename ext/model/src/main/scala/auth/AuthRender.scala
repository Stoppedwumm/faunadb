package fauna.auth

import fauna.ast._
import fauna.atoms._
import fauna.codex.json._
import fauna.lang._
import fauna.lang.syntax._
import fauna.model._
import fauna.net.http._
import fauna.net.security.JWT
import fauna.repo.query._

object AuthRender {

  def render(auth: Auth): Query[JSValue] =
    auth match {
      case null     => Query.value(JSNull)
      case RootAuth => Query.value(JSObject("key" -> "root"))

      case auth: LoginAuth =>
        auth.source match {
          case JWTLogin(_, _) =>
            val renderedDB = renderRef(auth.database.parentScopeID, auth.database.id)
            val renderedRoles = auth.permissions match {
              case InstancePermissions(_, ctx) =>
                renderRoles(auth.scopeID, ctx.roles map { _.id })
              case _ => Query.value(JSNull)
            }

            (renderedDB, renderedRoles) par { (db, role) =>
              Query.value(
                JSObject(
                  "jwt" -> true,
                  "scope_db" -> db,
                  "role" -> role
                ))
            }

          case KeyLogin(key) =>
            val renderedAuthDB = renderAuthDBRef(auth.database, auth.authDatabase)
            val renderedScopeDB =
              renderRef(auth.database.parentScopeID, auth.database.id)
            val renderedKey = renderRef(key.scopeID, key.id)
            val renderedRole = keyRole(key)

            val renderedID = auth.identity match {
              case Some(id) => renderRef(auth.scopeID, id)
              case _        => Query.value(JSNull)
            }

            (
              renderedAuthDB,
              renderedScopeDB,
              renderedRole,
              renderedKey,
              renderedID) par { (authDB, scopeDB, role, key, id) =>
              Query.value(
                JSObject(
                  "key" -> key,
                  "auth_db" -> authDB,
                  "scope_db" -> scopeDB,
                  "instance" -> id,
                  "role" -> role
                ))
            }

          case TokenLogin(token) =>
            val renderedDB = renderRef(auth.database.parentScopeID, auth.database.id)
            val renderedToken = renderRef(auth.scopeID, token.id)
            val renderedID = renderRef(auth.scopeID, token.document)
            val renderedRoles = auth.permissions match {
              case InstancePermissions(_, ctx) =>
                renderRoles(auth.scopeID, ctx.roles map { _.id })
              case _ => Query.value(JSNull)
            }

            (renderedDB, renderedToken, renderedID, renderedRoles) par {
              (db, token, id, roles) =>
                Query.value(
                  JSObject(
                    "scope_db" -> db,
                    "tokens" -> token,
                    "instance" -> id,
                    "role" -> roles
                  ))
            }

          case RootLogin =>
            val renderedAuthDB = renderAuthDBRef(auth.database, auth.authDatabase)
            val renderedScopeDB =
              renderRef(auth.database.parentScopeID, auth.database.id)

            (renderedAuthDB, renderedScopeDB) par { (authDB, scopeDB) =>
              Query.value(
                JSObject(
                  "key" -> "root",
                  "auth_db" -> authDB,
                  "scope_db" -> scopeDB
                ))
            }
        }
    }

  def render(auth: AuthType): Option[JSValue] =
    auth match {
      case BearerAuth(token, _) if AuthLike.isJWT(token) =>
        val jwt = JWT(token)
        Some(
          JSObject(
            "jwt" -> true,
            "audience" -> jwt.getAudience,
            "issuer" -> jwt.getIssuer))

      case BearerAuth(token, _) =>
        fromRaw(token)

      case BasicAuth(token, _) =>
        fromRaw(token)

      case _ => None
    }

  private def fromRaw(raw: String) =
    AuthLike.fromBase64(raw) match {
      case Some(key: KeyLike) => Some(JSObject("key" -> key.id.toLong.toString))
      case Some(token: TokenLike) =>
        Some(
          JSObject(
            "token" -> token.id.toLong.toString,
            "scope" -> token.domain.toLong.toString))
      case None => None
    }

  private def renderRef[T <: ID[T]: CollectionIDTag](
    scope: ScopeID,
    id: T): Query[JSValue] = renderRef(scope, id.toDocID)

  private def renderRef(scope: ScopeID, id: DocID): Query[JSValue] = {
    RenderContext.render(
      RootAuth,
      APIVersion.V21,
      Timestamp.Epoch,
      RefL(scope, id)) map { buf =>
      JSRawValue(buf.toUTF8String)
    }
  }

  private def renderRef(scope: ScopeID, id: Option[DocID]): Query[JSValue] =
    id match {
      case Some(id) => renderRef(scope, id)
      case None     => Query.value(JSNull)
    }

  private def renderAuthDBRef(scopeDB: Database, authDB: Database) =
    if (scopeDB.scopeID != authDB.scopeID) {
      renderRef(authDB.parentScopeID, authDB.id)
    } else {
      Query.value(JSNull)
    }

  private def renderRoles(scope: ScopeID, roles: Set[RoleID]): Query[JSArray] = {
    val rendered = roles map { renderRef(scope, _) }
    rendered.sequence map { JSArray(_: _*) }
  }

  private def keyRole(key: Key): Query[JSValue] =
    key.role match {
      case Key.AdminRole          => Query.value(JSString("admin"))
      case Key.ServerRole         => Query.value(JSString("server"))
      case Key.ServerReadOnlyRole => Query.value(JSString("server-readonly"))
      case Key.ClientRole         => Query.value(JSString("client"))
      case custom @ Key.UserRoles(_) =>
        custom.roleIDs(key.scopeID) flatMap {
          renderRoles(key.scopeID, _)
        }
    }
}
