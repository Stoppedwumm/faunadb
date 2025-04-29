package fauna.auth

import fauna.atoms._
import fauna.lang.syntax._
import fauna.model._
import fauna.repo.query.Query
import org.parboiled2._
import scala.util.Try

object AuthParser {
  val DocRef = "@doc/"
  val RoleRef = "@role/"
}

case class AuthParser(input: ParserInput) extends Parser {
  import AuthParser._

  def parse(): Try[(String, AuthScope)] =
    root.run()

  private def root: Rule1[(String, AuthScope)] = rule { auth ~ EOI }

  private def auth: Rule1[(String, AuthScope)] = rule {
    secret ~ authScope ~> ((_: String, _: AuthScope)) |
      secret ~ ':' ~ zeroOrMore(ANY) ~> ((_: String, NullAuthScope)) |
      secret ~> ((_: String, NullAuthScope))
  }

  private def authScope: Rule1[AuthScope] = rule {
    ':' ~ optional(database ~ ':') ~ builtin ~> (mkBuiltin(_, _)) |
      ':' ~ optional(database ~ ':') ~ custom ~> (mkCustom(_, _)) |
      ':' ~ optional(database ~ ':') ~ legacyRef ~> (mkDocRef(_, _)) |
      ':' ~ optional(database ~ ':') ~ docRef ~> (mkDocRef(_, _))
  }

  private def secret: Rule1[String] = rule {
    capture(oneOrMore(noneOf("/,()&=:")))
  }

  private def database: Rule1[String] = rule {
    capture(segment)
  }

  private def segment: Rule0 = rule {
    oneOrMore(noneOf(",()&=:"))
  }

  private def builtin: Rule1[Key.Role] = rule {
    "admin" ~ push(Key.AdminRole) |
      "server-readonly" ~ push(Key.ServerReadOnlyRole) |
      "server" ~ push(Key.ServerRole) |
      "client" ~ push(Key.ClientRole)
  }

  private def custom: Rule1[String] = rule {
    RoleRef ~ capture(segment)
  }

  private def legacyRef: Rule1[String] = rule {
    ("classes/" | "collections/") ~ capture(segment)
  }

  private def docRef: Rule1[String] = rule {
    DocRef ~ capture(segment)
  }

  private def mkDocRef(database: Option[String], ref: String): AuthScope =
    DocumentAuthScope(s"collections/$ref", split(database))

  private def mkBuiltin(database: Option[String], role: Key.Role): AuthScope =
    BuiltinRoleAuthScope(role, split(database))

  private def mkCustom(database: Option[String], role: String): AuthScope =
    CustomRoleAuthScope(role, split(database))

  private def split(database: Option[String]): List[String] =
    database map { _.split("/").toList } getOrElse List.empty
}

object AuthScope {

  def canSetScope(key: Key) =
    key.role match {
      case Key.AdminRole => true
      case _             => false
    }
}

sealed trait AuthScope {

  // Currently when a scope is specified on a token
  // we just ignore that scope and return an InstanceAuth,
  // ideally this implementation will be moved to a concrete
  // class to be handled accordingly.
  def getTokenAuth(token: Token): Query[Option[Auth]] = {
    token.document match {
      case Some(instance) =>
        Database.forScope(token.scopeID) flatMapT { db =>
          RuntimeEnv.Default.Store(token.scopeID).get(instance) flatMapT { v =>
            val source = TokenLogin(token)

            RoleEvalContext.lookup(token.scopeID, v.docID, source).flatMap {
              context =>
                db.account.map { account =>
                  val auth = LoginAuth(
                    token.scopeID,
                    db,
                    account.limiter,
                    db,
                    InstancePermissions(v.docID, context),
                    source,
                    Some(v.docID))

                  Some(auth)
                }
            }
          }
        }
      case _ => Query.none
    }
  }

  def getKeyAuth(key: Key): Query[Option[Auth]]
  def getRootAuth(): Query[Option[Auth]]
}

case object NullAuthScope extends AuthScope {

  def getKeyAuth(key: Key): Query[Option[Auth]] = {
    Database.forScope(key.scopeID) flatMapT { db =>
      val permQ = key.role match {
        case Key.AdminRole          => Query.value(AdminPermissions)
        case Key.ServerRole         => Query.value(ServerPermissions)
        case Key.ServerReadOnlyRole => Query.value(ServerReadOnlyPermissions)
        case Key.ClientRole         => Query.value(NullPermissions)
        case custom @ Key.UserRoles(_) =>
          custom.roleIDs(key.scopeID) flatMap {
            RolePermissions.lookup(key.scopeID, _)
          }
      }

      permQ.flatMap { perm =>
        db.account.map { account =>
          val auth = LoginAuth(
            key.scopeID,
            db,
            account.limiter,
            db,
            perm,
            KeyLogin(key),
            None)

          Some(auth)
        }
      }
    }
  }

  def getRootAuth(): Query[Option[Auth]] = {
    Query.some(RootAuth)
  }
}

case class DocumentAuthScope(ref: String, path: List[String]) extends AuthScope {
  import AuthScope._

  def getKeyAuth(key: Key): Query[Option[Auth]] = {
    val source = KeyLogin(key)

    val roleQ = if (path.nonEmpty && canSetScope(key)) {
      getScopeAuth(key.scopeID, source)
    } else if (path.isEmpty && canSetPermissionScope(key)) {
      getScopeAuth(key.scopeID, source)
    } else {
      Query.none
    }

    roleQ.flatMapT { case (db, origDB, docID, context) =>
      db.account.map { account =>
        Some(
          LoginAuth(
            db.scopeID,
            db,
            account.limiter,
            origDB,
            InstancePermissions(docID, context),
            source,
            Some(docID)))
      }
    }
  }

  def getRootAuth(): Query[Option[Auth]] = {
    getScopeAuth(Database.RootScopeID, RootLogin).flatMapT {
      case (db, origDB, id, context) =>
        db.account.map { account =>
          Some(
            LoginAuth(
              db.scopeID,
              db,
              account.limiter,
              origDB,
              InstancePermissions(id, context),
              RootLogin,
              Some(id)))
        }
    }
  }

  private def canSetPermissionScope(key: Key) =
    key.role match {
      case Key.AdminRole | Key.ServerRole => true
      case _                              => false
    }

  private def getScopeAuth(scopeID: ScopeID, source: LoginSource) = {
    Database.getSubScope(scopeID, path) flatMapT { db =>
      if (path.isEmpty) {
        getRoleEvalContext(db, source) mapT { case (docID, context) =>
          (db, db, docID, context)
        }
      } else {
        Database.forScope(scopeID) flatMapT { origDB =>
          getRoleEvalContext(db, source) mapT { case (docID, context) =>
            (db, origDB, docID, context)
          }
        }
      }
    }
  }

  private def getRoleEvalContext(db: Database, source: LoginSource) = {
    // FIXME: symbolizeStr requires auth, but we don't have one here
    val auth = EvalAuth(db.scopeID, AdminPermissions)
    val scopeQ = ModelData.symbolizeRefStr(auth, ref) collectT { case id =>
      id.collID match {
        case UserCollectionID(_) => Query.some(id)
        case _                   => Query.none
      }
    }

    val docIDQ = scopeQ flatMapT { docID =>
      RuntimeEnv.Default.Store(db.scopeID).get(docID) mapT { _ => docID }
    }

    docIDQ flatMapT { id =>
      RoleEvalContext.lookup(db.scopeID, id, source) map { context =>
        Some((id, context))
      }
    }
  }
}

abstract class AbstractRoleAuthScope extends AuthScope {
  import AuthScope._

  val path: List[String]

  def getKeyAuth(key: Key): Query[Option[Auth]] = {
    if (canSetScope(key)) {
      getScopePermission(key.scopeID) flatMapT { case (permissions, database) =>
        val origDBQ = if (path.isEmpty) {
          Query.value(Some(database))
        } else {
          Database.forScope(key.scopeID)
        }

        origDBQ.flatMapT { origDB =>
          database.account.map { account =>
            Some(
              LoginAuth(
                database.scopeID,
                database,
                account.limiter,
                origDB,
                permissions,
                KeyLogin(key),
                None))
          }
        }
      }
    } else {
      Query.none
    }
  }

  def getRootAuth(): Query[Option[Auth]] = {
    getScopePermission(Database.RootScopeID).flatMapT {
      case (permissions, database) =>
        database.account.map { account =>
          Some(
            LoginAuth(
              database.scopeID,
              database,
              account.limiter,
              database,
              permissions,
              RootLogin,
              None))
        }
    }
  }

  protected def getScopePermission(
    scopeID: ScopeID): Query[Option[(Permissions, Database)]]
}

case class CustomRoleAuthScope(role: String, path: List[String])
    extends AbstractRoleAuthScope {

  protected def getScopePermission(scopeID: ScopeID) = {
    Database.getSubScope(scopeID, path) flatMapT { db =>
      Role.idByNameActive(db.scopeID, role) flatMapT { roleID =>
        RolePermissions.lookup(db.scopeID, Set(roleID)) map { perms =>
          Some((perms, db))
        }
      }
    }
  }
}

case class BuiltinRoleAuthScope(role: Key.Role, path: List[String])
    extends AbstractRoleAuthScope {

  protected def getScopePermission(scopeID: ScopeID) = {
    Database.getSubScope(scopeID, path) flatMapT { db =>
      role match {
        case Key.AdminRole =>
          Query.some((AdminPermissions, db))

        case Key.ServerRole =>
          Query.some((ServerPermissions, db))

        case Key.ServerReadOnlyRole =>
          Query.some((ServerReadOnlyPermissions, db))

        case Key.ClientRole =>
          Query.some((NullPermissions, db))

        case _ =>
          Query.none
      }
    }
  }
}
