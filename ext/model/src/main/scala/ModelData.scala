package fauna.model

import fauna.ast._
import fauna.atoms._
import fauna.auth._
import fauna.lang.syntax._
import fauna.repo.query.Query

import RefParser._

object ModelData {

  // symbolization

  def symbolizeStr(auth: Auth, str: String): Query[Option[ScalarL]] =
    LegacyRefParser.Ref.parse(str) match {
      case Some(ref) => lookupRef(auth, ref)
      case None      => Query.some(StringL(str))
    }

  def symbolizeRefStr(auth: Auth, str: String): Query[Option[DocID]] =
    symbolizeStr(auth, str) collectT { case RefL(_, id) => Query.some(id) }

  // helpers

  def scoped(auth: Auth, scope: Option[RefScope.DatabaseRef]): Query[Option[ScopeID]] = {
    scope match {
      case None => Query.some(auth.scopeID)
      case Some(RefScope.DatabaseRef(name, subScope)) =>
        scoped(auth, subScope) flatMapT { Database.scopeByName(_, name) }
    }
  }

  def lookupRef(auth: Auth, ref: RefScope.Ref): Query[Option[ScalarL]] = {
    scoped(auth, ref.scope) flatMapT { scopeID =>
      ref match {
        case key: RefScope.KeyRef                => Query.some(RefL(scopeID, key.id))
        case RefScope.DatabaseRef(name, _)       => Database.idByName(scopeID, name) mapT { RefL(scopeID, _) }
        case inst: RefScope.ObjectInstanceRef    => lookupInstanceRef(auth, scopeID, inst) mapT { RefL(scopeID, _) }
        case cls: RefScope.CollectionRef         => lookupClassRef(scopeID, cls) mapT { RefL(scopeID, _) }
        case idx: RefScope.IndexRef              => lookupIndexRef(scopeID, idx) mapT { RefL(scopeID, _) }
        case udf: RefScope.UserFunctionRef       => lookupUDFRef(scopeID, udf) mapT { RefL(scopeID, _) }
        case role: RefScope.RoleRef              => lookupRoleRef(scopeID, role) mapT { RefL(scopeID, _) }
        case RefScope.AccessProviderRef(name, _) => AccessProvider.idByNameActive(scopeID, name) mapT { RefL(scopeID, _) }
      }
    }
  }

  def lookupClassRef(scope: ScopeID, ref: RefScope.CollectionRef) =
    ref match {
      case RefScope.NativeCollectionRef(collectionID, _) => Query.some(collectionID)
      case RefScope.UserCollectionRef(name, _)           => Collection.idByNameActive(scope, name)
    }

  def lookupIndexRef(scope: ScopeID, ref: RefScope.IndexRef) =
    Index.idByName(scope, ref.name)

  def lookupUDFRef(scope: ScopeID, ref: RefScope.UserFunctionRef) =
    UserFunction.idByNameActive(scope, ref.name)

  def lookupRoleRef(scope: ScopeID, ref: RefScope.RoleRef): Query[Option[RoleID]] =
    Role.idByNameActive(scope, ref.name)

  def lookupInstanceRef(auth: Auth, scope: ScopeID, ref: RefScope.ObjectInstanceRef): Query[Option[DocID]] =
    (auth, ref) match {
      case (LoginAuth.Source(TokenLogin(token)), RefScope.SelfRef(RefScope.TokenClassRef)) =>
        Query.some(token.id.toDocID)

      case (LoginAuth.Source(TokenLogin(token)), RefScope.SelfRef(RefScope.CredentialsClassRef)) =>
        token.credentials mapT { _.docID }

      case (LoginAuth.Identity(id), RefScope.SelfRef(clsRef)) =>
        lookupClassRef(scope, clsRef) collectT { case cid if cid == id.collID => Query.some(id) }

      case (_, RefScope.InstanceRef(Right(id), clsRef)) =>
        lookupClassRef(scope, clsRef) mapT { DocID(id, _) }

      case _ =>
        Query.none
    }
}
