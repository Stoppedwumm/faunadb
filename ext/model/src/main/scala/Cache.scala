package fauna.model

import fauna.atoms._
import fauna.auth._
import fauna.lang._
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.model._
import fauna.model.account.Account
import fauna.model.schema._
import fauna.repo.cache.CacheKey
import fauna.repo.query._
import fauna.repo.store._
import java.util.Objects
import scala.annotation.unused

/** The Cache module brokers all direct cache access for the model layer. */
object Cache extends ExceptionLogging {
  import CacheStore._

  private val log = getLogger()

  /** Invalidate non-version aware cache items.
    *
    * !!! ATTENTION !!!
    *
    * Every time a new resource is added here, make sure
    * `CassandraService.invalidateCollectionIDs` is updated accordingly, otherwise
    * `SchemaCacheInvalidationService` will filter out events of the unknown
    * collections and the cache won't be invalidated.
    */
  def invalidate(
    scope: ScopeID,
    docID: DocID,
    @unused txnTS: Timestamp): Query[Unit] =
    docID match {
      case DatabaseID(id) => Database.invalidateCaches(scope, id)
      case TokenID(id)    => Token.invalidateCaches(scope, id)
      case KeyID(id)      => Key.invalidateCaches(scope, id)
      case _              => Query.unit
    }

  def idByName[I <: ID[I]: IDCompanion](
    scope: ScopeID,
    name: String): Query[Option[SchemaItemView[I]]] = getItem(
    new ByNameKey(scope, name))

  /** A class for schema-aware keys that return the doc ID via its name lookup.
    *
    * NB: Case classes don't consider implicits in their equals/hashCode, so we
    * implement that by hand.
    */
  private class ByNameKey[I <: ID[I]: IDCompanion](
    val scope: ScopeID,
    val name: String)
      extends SchemaKey[SchemaItemView[I]] {
    def collID = implicitly[IDCompanion[I]].collID

    def query: Query[Option[SchemaItemView[I]]] =
      Query.timing("Cache.Load.IDByName") {
        SchemaNames.idByNameUncachedView[I](scope, name)
      }

    override def equals(obj: Any): Boolean = obj match {
      case o: ByNameKey[_] =>
        scope == o.scope && name == o.name && collID == o.collID
      case _ => false
    }
    override def hashCode = Objects.hash(scope, name, collID)
  }

  def idByAlias[I <: ID[I]: IDCompanion](
    scope: ScopeID,
    alias: String): Query[Option[SchemaItemView[I]]] = getItem(
    new ByAliasKey(scope, alias))

  /** A class for schema-aware keys that return the doc ID via its alias lookup.
    * Only valid for collections and functions.
    *
    * NB: Case classes don't consider implicits in their equals/hashCode, so we
    * implement that by hand.
    */
  private class ByAliasKey[I <: ID[I]: IDCompanion](
    val scope: ScopeID,
    val alias: String)
      extends SchemaKey[SchemaItemView[I]] {
    def collID = implicitly[IDCompanion[I]].collID

    def query: Query[Option[SchemaItemView[I]]] =
      Query.timing("Cache.load.IDByAlias") {
        SchemaNames.idByAliasUncachedView[I](scope, alias)
      }

    override def equals(obj: Any): Boolean = obj match {
      case o: ByAliasKey[_] =>
        scope == o.scope && alias == o.alias && collID == o.collID
      case _ => false
    }
    override def hashCode = Objects.hash(scope, alias, collID)
  }

  /** Guard a query from cache staleness based on the given partial predicate. If the
    * predicate returns true for the query result, it forces a refresh of the latest
    * schema version for the scope provided, thus causing a schema version OCC if the
    * cache is outdated.
    */
  def guardFromStalenessIf[A](scope: ScopeID, query: Query[A])(
    shouldRefresh: A => Boolean): Query[A] =
    query flatMap { result =>
      val refreshQ =
        if (shouldRefresh(result)) {
          refreshLastSeenSchema(scope)
        } else {
          Query.unit
        }
      refreshQ map { _ => result }
    }

  def getLastSeenSchema(scope: ScopeID): Query[Option[SchemaVersion]] =
    CacheStore.getLastSeenSchema(scope)

  def refreshLastSeenSchema(scope: ScopeID): Query[Unit] =
    CacheStore.refreshLastSeenSchema(scope)

  def schemaIndexStatusByScope(scope: ScopeID) =
    getItem(SchemaIndexStatusByScope(scope))

  private case class SchemaIndexStatusByScope(scope: ScopeID)
      extends SchemaKey[SchemaIndexStatus] {
    def query = Query.timing("Cache.Load.SchemaIndexStatusByScope") {
      SchemaIndexStatus.forScopeUncached(scope)
    }
  }

  /** Versioned cache items */

  // Name lookup

  def nameByID(scope: ScopeID, id: DocID) = getItem(NameByID(scope, id))

  private final case class NameByID(scope: ScopeID, id: DocID)
      extends SchemaKey[String] {
    def query = Query.timing("Cache.Load.NameByID") {
      id.collID match {
        case NamedCollectionID(_) =>
          SchemaNames.lookupNameUncached(scope, id)
        case _ =>
          log.warn(s"Attempted to read name of unnamed document $id");
          Query.none
      }
    }
  }

  // Databases

  def databaseByScope(scope: ScopeID) = getItem(DatabaseByScope(scope))

  private final case class DatabaseByScope(scope: ScopeID)
      extends SchemaKey[Database] {
    def query = Query.timing("Cache.Load.DatabaseByScope") {
      Database.getUncached(scope)
    }
  }

  // Collections

  def cacheCollectionForTesting(cfg: CollectionConfig) =
    Query.repo map { repo =>
      val cache = repo.cacheContext

      val ckey = CollByID(cfg.parentScopeID, cfg.id)
      val cval = OptionalSchemaItemView.Unchanged(Collection(cfg, None))
      cache.schema2.update(ckey, cval)

      val id1key = new ByNameKey[CollectionID](cfg.parentScopeID, cfg.name)
      val id2key = new ByAliasKey[CollectionID](cfg.parentScopeID, cfg.name)
      val idval = SchemaItemView.Unchanged(cfg.id)
      cache.schema2.update(id1key, idval)
      cache.schema2.update(id2key, idval)

      val nkey = new NameByID(cfg.parentScopeID, cfg.id.toDocID)
      val nval = cfg.name
      cache.schema2.update(nkey, nval)
    }

  def collByID(scope: ScopeID, id: CollectionID) =
    getItem(CollByID(scope, id))

  private case class CollByID(scope: ScopeID, id: CollectionID)
      extends SchemaKey[Collection.StagedView] {
    def query = Query.timing("Cache.Load.CollByID") {
      id match {
        case UserCollectionID(id) => Collection.getUncached(scope, id)
        case _ =>
          log.warn(
            s"Attempted to read invalid native collection $id in $scope from schema cache.")
          Query.none
      }
    }
  }

  def collIDByName(scope: ScopeID, name: String) =
    idByName[CollectionID](scope, name)

  def collIDByAlias(scope: ScopeID, alias: String) =
    idByAlias[CollectionID](scope, alias)

  // Indexes

  def indexByID(scope: ScopeID, id: IndexID) =
    getItem(IndexByID(scope, id))

  private case class IndexByID(scope: ScopeID, id: IndexID)
      extends SchemaKey[Index] {
    def query = Query.timing("Cache.Load.IndexByID") {
      id match {
        case UserIndexID(id) => Index.getUncached(scope, id)
        case _ =>
          log.warn(
            s"Attempted to read invalid native index $id in $scope from schema cache.")
          Query.none
      }
    }
  }

  def indexIDByName(scope: ScopeID, name: String) = idByName[IndexID](scope, name)

  def indexIDsByScope(scope: ScopeID) =
    getItem(IndexIDsByScope(scope)).map {
      case Some(ids) => ids
      case None =>
        logException(
          new IllegalStateException(s"Could not lookup indexes for scope $scope"))
        throw new IllegalStateException("could not lookup indexes")
    }

  private case class IndexIDsByScope(scope: ScopeID)
      extends SchemaKey[Iterable[IndexID]] {
    def query = Query.timing("Cache.Load.IndexIDsByScope") {
      IndexID.getAllUserDefined(scope).flattenT map { Some(_) }
    }
  }

  // Functions

  def functionByID(scope: ScopeID, id: UserFunctionID) =
    getItem(FunctionByID(scope, id))

  private case class FunctionByID(scope: ScopeID, id: UserFunctionID)
      extends SchemaKey[SchemaItemView[UserFunction]] {
    def query = Query.timing("Cache.Load.FunctionByID") {
      UserFunction.getItemUncached(scope, id)
    }
  }

  def functionIDByName(scope: ScopeID, name: String) =
    idByName[UserFunctionID](scope, name)

  def functionIDByAlias(scope: ScopeID, alias: String) =
    idByAlias[UserFunctionID](scope, alias)

  // Access Providers

  def accessProviderIDByName(scope: ScopeID, name: String) =
    idByName[AccessProviderID](scope, name)

  def accessProviderByIssuer(scope: ScopeID, iss: String) =
    getItem(AccessProviderByIssuer(scope, iss))

  private case class AccessProviderByIssuer(scope: ScopeID, iss: String)
      extends SchemaKey[AccessProvider] {
    def query = Query.timing("Cache.Load.AccessProviderByIssuer") {
      AccessProvider.getByIssuerUncached(scope, iss)
    }
  }

  // Roles

  def roleByID(scope: ScopeID, id: RoleID) =
    getItem(RoleByID(scope, id))

  private case class RoleByID(scope: ScopeID, id: RoleID)
      extends SchemaKey[SchemaItemView[Role]] {
    def query = Query.timing("Cache.Load.RoleByID") {
      Role.getItemUncached(scope, id)
    }
  }

  def roleIDByName(scope: ScopeID, name: String) = idByName[RoleID](scope, name)

  def roleMembershipByCollection(scope: ScopeID, id: Either[String, CollectionID]) =
    getItem(RoleMembershipByCollection(scope, id))

  private case class RoleMembershipByCollection(
    scope: ScopeID,
    id: Either[String, CollectionID])
      extends SchemaKey[Set[ParsedMembership]] {
    def query =
      Query.timing("Cache.Load.RoleMembershipByCollection") {
        RoleMembership.byCollectionUncached(scope, id)
      }
  }

  def roleECByRoles(scope: ScopeID, roles: Set[RoleID]) =
    getItem(RoleECByRoles(scope, roles))

  private case class RoleECByRoles(scope: ScopeID, roles: Set[RoleID])
      extends SchemaKey[RoleEvalContext] {
    def query =
      Query.timing("Cache.Load.RoleEvalContextByRoles") {
        RoleEvalContext.getECByRolesUncached(scope, roles)
      }
  }

  /** Maps `GlobalID` => `ScopeID`. Unversioned, because this never changes.
    */
  def scopeByGlobalID(globalID: GlobalDatabaseID): Query[Option[ScopeID]] = for {
    repo  <- Query.repo
    scope <- repo.cacheContext.schema.get(ScopeByGlobalID(globalID))
  } yield scope

  def invalidateScopeByGlobalID(globalID: GlobalDatabaseID): Query[Unit] =
    Query.repo.map { repo =>
      repo.cacheContext.schema.invalidate(ScopeByGlobalID(globalID))
    }

  private case class ScopeByGlobalID(globalID: GlobalDatabaseID)
      extends CacheKey[ScopeID] {
    def query =
      Database.lookupIDForGlobalID(globalID).flatMapT { case (parent, db) =>
        SchemaCollection.Database(parent).get(db).mapT { vers =>
          vers.data(Database.ScopeField)
        }
      }

    override val shouldHaveRegion: Boolean = false
  }

  // This `.get` is ok, because `getUncached` will always return `Some`.
  def accountByID(accountID: AccountID) = getItem(AccountByID(accountID)).map(_.get)

  private case class AccountByID(accountID: AccountID) extends CacheKey[Account] {
    def query = Account.getUncached(accountID).map(Some(_))
  }
}
