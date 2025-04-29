package fauna.model

import fauna.ast._
import fauna.atoms._
import fauna.auth.{ AdminPermissions, EvalAuth }
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.account.Account
import fauna.model.schema.{ NativeIndex, SchemaCollection }
import fauna.repo._
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.repo.store.{ CacheStore, LookupStore }
import fauna.scheduler._
import fauna.storage.api.set._
import fauna.storage.doc._
import fauna.storage.index.{ IndexTerm, IndexTuple }
import fauna.storage.ir._
import fauna.util._

/** A database is a logical grouping of objects in a FaunaDB cluster.
  * Databases are organized in a tree with a special root database.
  * Every database defines a scope, which is a logical group containing the
  * database and all of its descendants and objects they contain, including
  * databases, collections, indexes, etc.
  *
  * A database is managed much like other documents, but has immutable
  * history - i.e. users are not permitted to insert events with timestamps in
  * the past. Critically, this means that once a database has been deleted, it
  * cannot be resurrected.
  */
final case class Database(
  /** The ID of the document that contains this database's configuration.
    * The document is stored in the scope of this database's parent.
    */
  id: DatabaseID,

  /** The ID of the scope of this database's parent.
    *
    * The parent scope of the root database is the root's scope.
    */
  parentScopeID: ScopeID,

  /** The ID of the scope created by this database.
    * All the direct descendants of this database, be they databases,
    * collections, indexes, etc., will be part of this scope.
    */
  scopeID: ScopeID,

  /** The ID of the customer account associated with this database.
    * Child databases inherit the account ID of their parent, except
    * that the default account ID associated with the root database
    * may be overridden. It is only persisted for a database when the
    * value is set explicitly for the database, and not when the value
    * is inherited.
    *
    * This means that, for each customer, there is one database that creates
    * a scope containing all the customer's objects, and that database is
    * uniquely identified as the database with account ID set to the
    * customer's account ID.
    */
  accountID: AccountID,

  /** A globally-unique identifier for databases, used by JWT audience
    * claims. See JWTToken.
    */
  globalID: GlobalDatabaseID,

  /** The human-friendly name of this database, like "customers" or "spaceships".
    * It is unique within its parent's scope and is stored under `SchemaNames.NameField`
    * in the database's configuration.
    */
  name: String,

  /** The scope IDs of the ancestors of this database. */
  ancestors: Set[ScopeID],

  /** The Global ID path for this database. */
  globalIDPath: Seq[GlobalID],

  /** The named path to this database. This is needed for our query logs that we ship to customers. */
  namePath: Seq[String],

  /** The priority group of this database, which helps determine the
    * QoS of queries within this database's scope.
    */
  priorityGroup: PriorityGroup,

  /** Indicates if the database is the tenant root.
    * This will return false for our internally marked accounts.
    * We have marked some fauna internal databases with account ids so that we can
    * throttle them if need be.  This value will be false for those databases.
    */
  isCustomerTenantRoot: Boolean,

  /** Indicates if the database is a container database or not. A container
    * database cannot persist any collections, documents, or indexes. Write
    * permissions are enforced by the WriteAdaptor and can be found in
    * `IOConfig.scala` via the flag `allowedInContainer`.
    */
  isContainer: Boolean,

  /** Indicates this database is disabled. If a database is disabled it will
    * not be visible to the user and cannot receive queries.
    */
  disabled: Option[Boolean],

  /** Flag indicating that the database's schema is typechecked. */
  typechecked: Option[Boolean],

  /** Flag indicating that this database's schema is in protected mode. */
  isProtected: Option[Boolean],

  /** If set, pins the "active" schema to the state of schema as of the version
    * snapshot. Any change to schema afterwards is interpreted as staged.
    * (See SchemaStatus as well).
    */
  activeSchemaOverride: Option[SchemaVersion],

  /** The number of minimum valid time pins on this scope.
    * If zero, MVT is unpinned and collection MVTs advance as normal.
    * If > 0, MVT is pinned and collection MVTs do not advance past
    * the time pinned for the scope.
    */
  minimumValidTimePins: Int,

  /** The pinned minimum valid time. Populated if and only if
    * minimumValidTimePins > 0, in which case collections in this scope
    * will not advance MVT past this time.
    */
  minimumValidTimePinnedTime: Option[Timestamp],

  /** The timestamp when this database was deleted. */
  deletedTS: Option[Timestamp]) {

  def account = Account.get(accountID)

  def priority: Priority = Priority(priorityGroup.treeGroup.weight)

  def isDeleted: Boolean = deletedTS.isDefined

  def hasAncestor(scope: ScopeID): Boolean = ancestors contains scope
}

case class ScopeLoopException(scope: ScopeID)
    extends Exception(s"ScopeID loop detected for $scope")

object Database {

  def apply(live: Version, latest: Version, parent: Database): Database = {
    val scope = live.data(ScopeField)

    /** We filter out our internally marked accounts.  We do this because those databases
      * contain schema in them and need to be able to create/modify it.
      */
    val isCustomerTenantRoot =
      live.data(AccountIDField).exists(_.toLong > MaxInternalAccountID)

    val account = live.data(AccountIDField) getOrElse parent.accountID

    val priority = live.data(PriorityField) getOrElse Priority.Default
    // legacy databases won't have this field and didn't have containers
    val container = live.data(ContainerField) getOrElse false
    val ancestors = parent.ancestors + live.parentScopeID
    val tree = parent.priorityGroup.treeGroup
      .descendant((live.parentScopeID, live.id), priority.toInt)
    val group = PriorityGroup(scope, tree)

    // Prior to ENG-XXX, global_id was an ephemeral property of a
    // database, equal to its scope. If the field is undefined,
    // default to the scope.
    val globalID = live.data.getOrElse(GlobalIDField, GlobalDatabaseID(scope.toLong))
    val globalIDPath = parent.globalIDPath :+ globalID

    val disabled = live.data(DisabledField)

    val typechecked = live.data(TypecheckedField)

    // The root database is unprotected. Default to the parent's setting
    // if the child lacks one.
    val isProtected = live.data(ProtectedField)

    val name = SchemaNames.findName(live)
    val namePath = parent.namePath :+ name

    val activeSchemaVers = live.data(ActiveSchemaVersField).map(SchemaVersion(_))

    val mvtPins = {
      val pins = live.data(MVTPinsField).getOrElse(0)
      require(pins >= 0, s"scope $scope has negative MVT pins")
      pins
    }

    val mvtPinTS = {
      val ts = live.data(MVTPinTSField)
      require(
        (mvtPins == 0 && ts.isEmpty) || (mvtPins > 0 && ts.isDefined),
        s"scope $scope has incompatible pins ($mvtPins) and pin TS ($ts)")
      ts
    }

    Database(
      live.id.as[DatabaseID],
      live.parentScopeID,
      scope,
      account,
      globalID,
      name,
      ancestors,
      globalIDPath = globalIDPath,
      namePath = namePath,
      group,
      isCustomerTenantRoot = isCustomerTenantRoot,
      isContainer = container,
      disabled = disabled,
      typechecked = typechecked,
      isProtected = isProtected,
      activeSchemaOverride = activeSchemaVers,
      minimumValidTimePins = mvtPins,
      minimumValidTimePinnedTime = mvtPinTS,
      Option.when(latest.isDeleted)(latest.ts.validTS)
    )
  }

  // Validations

  val ScopeField = Field[ScopeID]("scope")
  val GlobalIDField = Field[GlobalDatabaseID]("global_id")
  val AccountField = Field[Option[Data]]("account")
  val AccountIDField = Field[Option[AccountID]]("account", "id")
  val PriorityField = Field[Option[Priority]]("priority")
  val ContainerField = Field[Option[Boolean]]("container")
  val DisabledField = Field[Option[Boolean]]("disabled")
  val TypecheckedField = Field[Option[Boolean]]("typechecked")
  val ProtectedField = Field[Option[Boolean]]("protected")
  val MVTPinsField = Field[Option[Int]]("mvt_pins")
  val MVTPinTSField = Field[Option[Timestamp]]("mvt_pin_ts")
  val ActiveSchemaVersField = Field[Option[Long]]("active_schema_version")
  val NativeSchemaVersField = Field[Option[Long]]("native_schema_version")
  // NB: There used to be a field called 'allow_external_access_providers'.
  //     Don't add a new field with that name.

  /** Databases which do not have an account associated with any
    * ancestor use this account.
    */
  val DefaultAccount = AccountID.Root

  /** Any account id that is less than or equal to this value is treated as an internal account.
    * The current implication of that is that these accounts are able to create schema in
    * their root account database.
    */
  val MaxInternalAccountID = 1023

  /** Databases are created with this native schema version.
    * TODO: move to native schema management file.
    *
    * None or V0: Pre-versioned state
    */
  val CurrentNativeSchemaVers = 0L

  val RootScopeID = ScopeID.RootID
  val RootDatabaseID = DatabaseID.RootID
  val RootDatabaseInstanceID = RootDatabaseID.toDocID
  val RootDatabase = Database(
    RootDatabaseID,
    RootScopeID,
    RootScopeID,
    DefaultAccount,
    GlobalDatabaseID.MinValue,
    "root",
    Set.empty,
    globalIDPath = Seq.empty,
    namePath = Seq.empty,
    PriorityGroup.Root,
    isCustomerTenantRoot = false,
    isContainer = false,
    disabled = None,
    typechecked = None,
    isProtected = None,
    None,
    0,
    None,
    None
  )

  object ScopeValidator extends Validator[Query] {
    val filterMask = MaskTree(ScopeField.path)
  }

  object GlobalIDValidator extends Validator[Query] {
    val filterMask = MaskTree(GlobalIDField.path)
  }

  val VersionValidator: Validator[Query] =
    Document.DataValidator +
      SchemaNames.NameField.validator[Query] +
      ScopeValidator +
      AccountField.validator[Query] +
      AccountIDField.validator[Query] +
      PriorityField.validator[Query] +
      ContainerField.validator[Query] +
      TypecheckedField.validator[Query] +
      ProtectedField.validator[Query] +
      ActiveSchemaVersField.validator[Query] +
      NativeSchemaVersField.validator[Query] +
      GlobalIDValidator

  def LiveValidator(ec: EvalContext) =
    VersionValidator +
      ReferencesValidator(ec) +
      ContainerStateValidator

  // Cache

  def encodeGlobalID(globalID: GlobalID): String =
    globalID match {
      case _: GlobalKeyID =>
        throw new IllegalArgumentException(
          s"globalID must be a ScopeID or GlobalDatabaseID, but received a $globalID")
      case id @ (ScopeID(_) | GlobalDatabaseID(_)) =>
        ZBase32.encodeLong(id.toLong)
    }

  def decodeGlobalID(encoded: String): Option[GlobalDatabaseID] = {
    try {
      Some(GlobalDatabaseID(ZBase32.decodeLong(encoded)))
    } catch {
      case _: InvalidZBase32Exception =>
        None
    }
  }

  def lookupIDForGlobalID(id: GlobalID): Query[Option[(ScopeID, DatabaseID)]] =
    Store.getLatestDatabaseID(id) mapT { case (parentScope, db) =>
      id match {
        case scope: ScopeID if parentScope == scope =>
          throw ScopeLoopException(scope)
        case _ => ()
      }

      (parentScope, db)
    }

  /** Uncached get by GlobalID
    */
  def getUncached(globalID: GlobalID): Query[Option[Database]] =
    lookupIDForGlobalID(globalID) flatMapT { case (parentScope, db) =>
      getUncached(parentScope, db)
    }

  /** Uncached get by DatabaseID
    */
  def getUncached(parentScope: ScopeID, id: DatabaseID): Query[Option[Database]] = {
    val parentQ = latestForScope(parentScope)
    val liveQ = SchemaCollection
      .Database(parentScope, lookupIndexes = false)
      .getVersionLiveNoTTL(id)
    val latestQ = SchemaCollection
      .Database(parentScope, lookupIndexes = false)
      .getVersionNoTTL(id)

    (parentQ, liveQ, latestQ) parT { (parent, live, latest) =>
      Query.some(Database(live, latest, parent))
    }
  }

  def latestForScope(scope: ScopeID): Query[Option[Database]] =
    if (scope == RootScopeID) {
      Query.some(RootDatabase)
    } else {
      Query.timing("Database.Scope.Cached.Get") {
        Cache.databaseByScope(scope)
      }
    }

  def forScope(scopeID: ScopeID): Query[Option[Database]] =
    latestForScope(scopeID) rejectT { _.isDeleted }

  def forScopeUncached(scope: ScopeID): Query[Option[Database]] =
    lookupIDForGlobalID(scope).flatMapT { case (parentScope, id) =>
      getUncached(parentScope, id)
    }

  /** Returns a realized Database object given an AccountID if:
    *
    *   - A database is associated with the given account
    *   - The database is not deleted
    *
    * This method will add the Database object to the schema cache, if
    * any exists.
    */
  def forAccount(accountID: AccountID): Query[Option[Database]] =
    scopeByAccountID(accountID) flatMapT { forScope(_) }

  /** Returns the latest live Version for a database for a given account */
  def liveVersionForAccount(accountID: AccountID): Query[Option[Version.Live]] =
    Database.forAccount(accountID).flatMapT { db =>
      SchemaCollection
        .Database(db.parentScopeID)
        .getVersionLiveNoTTL(db.id)
    }

  /** Returns a realized Database object given a GlobalDatabaseID if a
    * live database exists with:
    *
    *   - The given globalID OR
    *   - A scope equal to the globalID
    */
  def forGlobalID(globalID: GlobalDatabaseID): Query[Option[Database]] =
    latestForGlobalID(globalID) rejectT { _.isDeleted }

  def latestForGlobalID(globalID: GlobalDatabaseID): Query[Option[Database]] =
    if (globalID == GlobalDatabaseID.MinValue) {
      Query.some(RootDatabase)
    } else {
      Query.timing("Database.GlobalID.Cached.Get") {
        Cache.scopeByGlobalID(globalID).flatMap { scope =>
          latestForScope(scope.getOrElse(ScopeID(globalID.toLong)))
        }
      }
    }

  def isDeleted(scopeID: ScopeID): Query[Boolean] =
    latestForScope(scopeID) existsT { _.isDeleted }

  def isDisabled(scope: ScopeID): Query[Boolean] =
    foldAncestors(scope)(db => Query.value(db.disabled))
      .map(_.getOrElse(false))

  def isProtected(scope: ScopeID): Query[Boolean] =
    foldAncestors(scope)(db => Query.value(db.isProtected))
      .map(_.getOrElse(false))

  def isTypechecked(scope: ScopeID): Query[Boolean] =
    foldAncestors(scope)(db => Query.value(db.typechecked))
      .map(_.getOrElse(true))

  private def foldAncestors(scope: ScopeID)(
    lookup: Database => Query[Option[Boolean]]): Query[Option[Boolean]] =
    forScope(scope).flatMapT { db =>
      lookup(db).flatMap {
        case Some(v) => Query.some(v)
        case None    =>
          // Make sure to call `lookup` on the root, but then don't recurse.
          if (scope == ScopeID.RootID) {
            Query.none
          } else {
            Database.foldAncestors(db.parentScopeID)(lookup)
          }
      }
    }

  // FIXME: Consolidate these in Schema
  def idByName(scopeID: ScopeID, name: String): Query[Option[DatabaseID]] =
    Store.uniqueIDForKey(
      NativeIndex.DatabaseByName(scopeID),
      Vector(IndexTerm(name)),
      Timestamp.MaxMicros) mapT {
      _.as[DatabaseID]
    }

  def scopeByAccountID(accountID: AccountID): Query[Option[ScopeID]] = {
    val cfg = NativeIndex.DatabaseByAccountID()
    val terms = Vector(Scalar(accountID.toLong))
    Store
      .collection(
        cfg,
        terms,
        Timestamp.MaxMicros,
        IndexTuple.MaxValue,
        IndexTuple.MinValue,
        1,
        ascending = false)
      .headValueT
      .map {
        _.flatMap { v =>
          v.tuple.values.headOption flatMap { t =>
            t.value match {
              case LongV(id) => Some(ScopeID(id))
              case _         => None
            }
          }
        }
      }
  }

  def scopeByName(scopeID: ScopeID, name: String): Query[Option[ScopeID]] =
    idByName(scopeID, name) flatMapT { getUncached(scopeID, _) } mapT { _.scopeID }

  def getSubScope(parentScopeID: ScopeID, path: List[String]) = {
    def getSubScope0(db: Database, path: List[String]): Query[Option[Database]] = {
      path match {
        case Nil =>
          Query.some(db)

        case name :: rest =>
          Database.idByName(db.scopeID, name) flatMapT { dbID =>
            Database.getUncached(db.scopeID, dbID) flatMapT { childDb =>
              getSubScope0(childDb, rest)
            }
          }
      }
    }

    Database.forScope(parentScopeID) flatMapT {
      getSubScope0(_, path)
    }
  }

  def setActiveSchemaVersion(
    scope: ScopeID,
    id: DatabaseID,
    vers: Option[SchemaVersion]): Query[Unit] = {
    val diff = Diff(ActiveSchemaVersField -> vers.map(_.toMicros))
    SchemaCollection.Database(scope).internalUpdate(id, diff).join
  }

  /** Return the pinned MVT for `scope`.
    *
    * While MVT is pinned, collection MVTs will not advance past the pinned time.
    */
  def pinnedMVTForScope(scope: ScopeID): Query[Option[Timestamp]] =
    Database.forScope(scope) map {
      _.flatMap { _.minimumValidTimePinnedTime }
    }

  /** Add a pin to MVT in `scope` at `ts`, preventing the MVTs of collections in the
    * scope from advancing past `ts`. The MVT will be pinned at or before `ts`
    * (depending on the timestamp of other pins). The pin must be at or after the
    * latest MVT of any collection (including MVT offset), plus the time
    * for cache invalidation, to ensure MVT does not move back. In practice, if
    * you pin at the snapshot time then the MVT offset guarantees the pin is valid.
    *
    * Does not go through the cache, but also always writes to the database
    * document.
    */
  def addMVTPin(scope: ScopeID, ts: Timestamp): Query[Unit] =
    Database.getUncached(scope) flatMap {
      case None => Query.unit
      case Some(db) =>
        require(db.minimumValidTimePins >= 0, s"scope $scope has negative MVT pins")

        Query.repo.flatMap { repo =>
          CollectionID
            .getAllUserDefined(scope)
            .forallMT { id =>
              Collection.deriveMinValidTime(scope, id) map { colMVT =>
                // This is a configuration issue, and is checked at start, so
                // this requirement should be redundant.
                require(
                  repo.cacheContext.schema.refreshRate <= Collection.MVTOffset,
                  "schema cache refresh rate is too infrequent")
                colMVT + repo.cacheContext.schema.refreshRate <= ts
              }
            }
            .flatMap { pinTSBeforeAllMVT =>
              if (pinTSBeforeAllMVT) {
                SchemaCollection
                  .Database(db.parentScopeID)
                  .internalUpdate(
                    db.id,
                    Diff(
                      MVTPinsField -> Some(db.minimumValidTimePins + 1),
                      MVTPinTSField -> Some(db.minimumValidTimePinnedTime.fold(ts) {
                        _.min(ts)
                      })))
                  .join
              } else {
                throw new IllegalStateException(
                  s"tried to pin $scope at $ts, which is before at least one collection's MVT")
              }
            }
        }
    }

  /** Remove a pin from MVT in `scope`.
    *
    * Does not go through the cache, but also always writes to the database
    * document.
    */
  def removeMVTPin(scope: ScopeID): Query[Unit] =
    Database.getUncached(scope) flatMap {
      case None => Query.unit
      case Some(db) =>
        require(
          db.minimumValidTimePins > 0,
          s"scope $scope has no MVT pin to remove (${db.minimumValidTimePins})")

        SchemaCollection
          .Database(db.parentScopeID)
          .internalUpdate(
            db.id,
            Diff(
              MVTPinsField -> Some(db.minimumValidTimePins - 1),
              MVTPinTSField -> (if (db.minimumValidTimePins > 1)
                                  db.minimumValidTimePinnedTime
                                else None)))
          .join
    }

  /** Traverses the database hierarchy visiting leaves first and then back up the tree.
    */
  def foldDescendants[T](
    init: GlobalID,
    seed: T,
    maxDepth: Int = Int.MaxValue,
    validTS: Timestamp = Timestamp.MaxMicros
  )(fold: (T, ScopeID, DatabaseID) => Query[T]): Query[T] =
    Database.lookupIDForGlobalID(init) flatMap {
      case None           => Query.value(seed)
      case Some((sc, id)) => foldDescendants(sc, id, seed, maxDepth, validTS)(fold)
    }

  def foldDescendants[T](
    parentScope: ScopeID,
    id: DatabaseID,
    seed: T,
    maxDepth: Int,
    validTS: Timestamp
  )(fold: (T, ScopeID, DatabaseID) => Query[T]): Query[T] = {

    val term = Vector(Scalar(DocIDV(DatabaseID.collID.toDocID)))

    def go(depth: Int, acc: T, parentScope: ScopeID, dbID: DatabaseID): Query[T] = {
      val accQ =
        if (depth == 0) {
          Query.value(acc)
        } else {
          // The root DB is the exceptional case here
          val ownedScopeQ =
            if (parentScope == ScopeID.RootID && dbID == DatabaseID.RootID) {
              Query.value(Some(ScopeID.RootID))
            } else {
              SchemaCollection.Database(parentScope).get(dbID, validTS) mapT {
                _.data(Database.ScopeField)
              }
            }

          ownedScopeQ flatMap {
            case None => Query.value(acc)
            case Some(scope) =>
              val idx = NativeIndex.DocumentsByCollection(scope)
              val coll = Store.collection(idx, term, validTS)
              coll.foldLeftValuesMT(acc) { case (acc, elem) =>
                go(depth - 1, acc, scope, elem.docID.as[DatabaseID])
              }
          }
        }

      // fold in the current node after all its descendents are accumulated.
      accQ flatMap { fold(_, parentScope, dbID) }
    }

    go(maxDepth, seed, parentScope, id)
  }

  // Helper method to return a list of all live global IDs underneath a database
  // based on walking the db tree.
  def getLiveGlobalIDsInHierarchy(
    scope: ScopeID,
    id: DatabaseID): Query[Seq[(GlobalID, (ScopeID, DatabaseID))]] =
    foldDescendants(
      scope,
      id,
      Vector.empty[(GlobalID, (ScopeID, DatabaseID))],
      Int.MaxValue,
      Timestamp.MaxMicros
    ) { (lookups, scope, id) =>
      val foundQ = SchemaCollection
        .Database(scope)
        .versions(id)
        .foldLeftValuesT(Map.empty[GlobalID, Boolean]) { (acc, v) =>
          def deletedData = v.prevData().getOrElse(Data.empty)
          def liveData = v.data
          val data = if (v.isDeleted) deletedData else liveData
          val globals = LookupHelpers.globalIDsFromData(v.id, data)
          // acc comes last here so that the latest state of each lookup is
          // preserved.
          (globals.map { _ -> v.isDeleted }.toMap[GlobalID, Boolean]) ++ acc
        }

      foundQ.map { found =>
        // preserve live globals
        val versLookups = found.filterNot { _._2 }.keys
        lookups :++ (versLookups.map { _ -> ((scope, id)) })
      }
    }

  /** Moves database target to destination. If target and destination databases represents logically
    * the same database (ie. same globalID), it will be an error if restore argument is not True.
    *
    * FIXME: The semantics of this are weird.
    * 1. it should raise an exception, not return Result.
    * 2. move and restore functionality should be separate public functions.
    */
  def moveDatabase(
    target: Database,
    destination: Database,
    restore: Boolean,
    pos: Position): Query[R[VersionL]] = {
    if (target.scopeID == destination.scopeID) {
      val reason = "Source and destination have the same scopeID."
      val err = MoveDatabaseError(target.name, destination.name, reason, pos)
      return Query.value(Left(List(err)))
    }

    def moveDatabase0(parent: Database): Query[R[VersionL]] = {
      val moveAdaptor = new WriteAdaptor(DatabaseWriteConfig.Move)

      if (parent.isDeleted) {
        val reason = s"${parent.name} is not a live database."
        val err = MoveDatabaseError(target.name, parent.name, reason, pos)
        return Query.value(Left(List(err)))
      }

      Query.snapshotTime flatMap { ts =>
        SchemaCollection.Database(target.parentScopeID).get(target.id, ts) flatMap {
          case Some(version) =>
            val diff = Diff(version.data.fields)

            val dstEc = EvalContext.write(
              EvalAuth(parent.scopeID, AdminPermissions),
              ts,
              APIVersion.Default)

            moveAdaptor.create(dstEc, None, diff, pos) flatMapT { versLit =>
              val destination =
                Database(versLit.version, versLit.version, parent)

              Key.moveKeys(target, destination) map { _ =>
                Right(versLit)
              } recoverWith { case Key.MoveKeyException(reason) =>
                val err = MoveDatabaseError(target.name, parent.name, reason, pos)
                Query.value(Left(List(err)))
              }
            } flatMapT { versLit =>
              // clean the original database document only at the end
              // so the keys are moved correctly.
              SchemaCollection
                .Database(target.parentScopeID)
                .clearDocument(target.id)
                .map { _ => Right(versLit) }
            }

          case None =>
            val reason = s"${target.name} is not a live database."
            val err = MoveDatabaseError(target.name, parent.name, reason, pos)
            Query.value(Left(List(err)))
        }
      }
    }

    if (target.globalID == destination.globalID) {
      if (restore) {
        Query.snapshotTime flatMap { ts =>
          val deleteQ: Query[R[Unit]] = if (!destination.isDeleted) {
            val auth = EvalAuth(destination.parentScopeID, AdminPermissions)
            val ec = EvalContext.write(auth, ts, APIVersion.Default)
            val dbAdaptor = new WriteAdaptor(DatabaseWriteConfig.Default)

            dbAdaptor.delete(ec, destination.id.toDocID.subID, pos) map {
              case Right(_) => Right(())
              case Left(_) =>
                val reason = s"Failed to delete ${destination.name} for restore."
                val err =
                  MoveDatabaseError(target.name, destination.name, reason, pos)
                Left(List(err))
            }
          } else {
            Query.value(Right(()))
          }

          deleteQ flatMapT { _ =>
            val parentQ = Database.forScope(destination.parentScopeID) map {
              _.toRight[List[EvalError]] {
                val reason = s"Failed to lookup destination's parent database."
                val err =
                  MoveDatabaseError(target.name, destination.name, reason, pos)
                List(err)
              }
            }

            parentQ flatMapT { moveDatabase0(_) }
          }
        }
      } else {
        val reason = "Source and destination have the same globalID. Use restore."
        val err = MoveDatabaseError(target.name, destination.name, reason, pos)
        Query.value(Left(List(err)))
      }
    } else {
      moveDatabase0(destination)
    }
  }

  def delete(scopeID: ScopeID, dbID: DatabaseID, recursiveDelete: Boolean) = {
    Database.foldDescendants(
      scopeID,
      dbID,
      Option.empty[Version.Deleted],
      maxDepth = if (recursiveDelete) Int.MaxValue else 0,
      Timestamp.MaxMicros
    ) { (_, parentScope, id) =>
      // This code somewhat mirrors indexing logic with lookups. However,
      // lookups behavior does not map 1-1. NB `v` below does not contain a
      // diff, so its prevData will be wrong. We reconstruct it by fetching the
      // previous version and using it to generate a proper diff.
      val remQ = for {
        prev <- SchemaCollection.Database(parentScope).get(id)
        v    <- SchemaCollection.Database(parentScope).internalDelete(id)
      } yield v.withDiff(prev.map(p => v.data.diffTo(p.data)))

      remQ flatMap { v =>
        val lookups = LookupHelpers.lookups(v)
        val addsQ = lookups.map(LookupStore.add(_)).join
        addsQ map { _ => Some(v) }
      }
    }
  }

  def invalidateCaches(scopeID: ScopeID, id: DatabaseID): Query[Unit] =
    SchemaCollection.Database(scopeID).getVersionLiveNoTTL(id).flatMap {
      case Some(v) =>
        val scopes = Seq(v.data(ScopeField)) ++ v.diff.flatMap { d =>
          ScopeField.read(d.fields).toOption
        }
        val globals = Seq(v.data(GlobalIDField)) ++ v.diff.flatMap { d =>
          GlobalIDField.read(d.fields).toOption
        }

        Seq(
          globals.map { g => Cache.invalidateScopeByGlobalID(g) }.join,
          scopes.map { s => CacheStore.invalidateScope(s) }.join
        ).join

      case None => Query.unit
    }
}

// TODO paths for exceptions?
object ContainerStateValidator extends Validator[Query] {

  override protected def filterMask: MaskTree = MaskTree.empty

  override protected def validateData(data: Data): Query[List[ValidationException]] =
    Database.ContainerField.read(data.fields) match {
      // Exception
      case Left(_) =>
        Query(Nil)

      // Container flag not present
      case Right(None) =>
        Query(Nil)

      // Flag present. Time to go to disk.
      case Right(Some(flag)) =>
        Database.ScopeField.read(data.fields) match {
          // Database does not exist. Pass validation and let write adaptor logic
          // handle the create.
          case Left(_) => Query(Nil)

          // Database already exists. validate flag as an update.
          case Right(scopeID) => makeChange(flag, scopeID)
        }
    }

  private def makeChange(flag: Boolean, scopeID: ScopeID) =
    Database.forScope(scopeID) flatMap { optDb =>
      (flag, optDb) match {
        // db doesn't exist
        case (_, None) => Query.value(Nil) // validated elsewhere (RefValidator)

        // db/container => db
        case (false, Some(_)) => Query.value(Nil)

        case (true, Some(db)) =>
          // container => container
          if (db.isContainer) {
            Query.value(Nil)

            // db => container. Fails if db contains collections, indexes, or UDFs
          } else {
            checkForInvalidContents(scopeID)
          }
      }
    }

  private def checkForInvalidContents(scopeID: ScopeID) = {
    val colQ = CollectionID.getAllUserDefined(scopeID).isEmptyT
    val idxQ = IndexID.getAllUserDefined(scopeID).isEmptyT
    val udfQ = UserFunctionID.getAllUserDefined(scopeID).isEmptyT

    // checks for all three in parallel
    (colQ, idxQ, udfQ) par {
      case (true, true, true) => Query(Nil)
      case (_, _, _)          => Query(List(ContainerCandidateContainsData(Nil)))
    }
  }
}
