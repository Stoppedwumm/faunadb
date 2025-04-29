package fauna.model.schema

import fauna.atoms._
import fauna.auth.ServerPermissions
import fauna.lang._
import fauna.lang.syntax._
import fauna.model
import fauna.model.{ Cache, Index => ModelIndex, SchemaNames }
import fauna.model.runtime.fql2.{ GlobalContext, PostEvalHook }
import fauna.model.schema.index.{ CollectionIndex, NativeCollectionIndex }
import fauna.model.schema.NativeCollection.NativeIdxOps
import fauna.repo.query.Query
import fauna.repo.schema.{
  ConstraintFailure,
  DocIDSource,
  FieldSchema,
  Path,
  ScalarType,
  SchemaResult,
  SchemaType,
  WriteHook
}
import fauna.repo.schema.migration.MigrationList
import fauna.repo.schema.ScalarType._
import fauna.repo.schema.SchemaType._
import fauna.repo.store.CacheStore
import fauna.repo.values.Value
import fauna.repo.values.Value.Func.Arity
import fauna.repo.IndexConfig
import fauna.storage.{ Selector => RSelector }
import fauna.storage.doc.Data
import fauna.storage.ir.StringV
import fauna.storage.DocAction
import scala.collection.immutable.SeqMap
import scala.concurrent.duration._

/** Fauna has a number of built-in collections used to implement its features.
  * These are their IDs.
  */
object NativeCollectionID {
  val MinValue = CollectionID(0)
  val MaxValue = CollectionID(1023)

  val Database = DatabaseID.collID // 0
  val Collection = CollectionID.collID // 1
  val Key = KeyID.collID // 2
  val Token = TokenID.collID // 3
  val SampleData = SampleDataID.collID // 6
  val Index = IndexID.collID // 7
  val JournalEntry = JournalEntryID.collID // 8
  val Task = TaskID.collID // 9
  val Credentials = CredentialsID.collID // 10
  val UserFunction = UserFunctionID.collID // 11
  val Role = RoleID.collID // 12
  val AccessProvider = AccessProviderID.collID // 13
  val SchemaSource = SchemaSourceID.collID // 14

  lazy val All: Set[CollectionID] = {
    val idRange = MinValue.toLong to MaxValue.toLong
    val ids = idRange collect {
      CollectionID(_) match { case NativeCollectionID(id) => id.collID }
    }
    ids.toSet
  }

  /** Identifies the ID subtype for the native collection with given id. */
  def unapply(id: CollectionID): Option[IDCompanion[_]] = id match {
    case DatabaseID.collID       => Some(DatabaseID)
    case CollectionID.collID     => Some(CollectionID)
    case IndexID.collID          => Some(IndexID)
    case UserFunctionID.collID   => Some(UserFunctionID)
    case RoleID.collID           => Some(RoleID)
    case AccessProviderID.collID => Some(AccessProviderID)
    case KeyID.collID            => Some(KeyID)
    case TokenID.collID          => Some(TokenID)
    case CredentialsID.collID    => Some(CredentialsID)
    case JournalEntryID.collID   => Some(JournalEntryID)
    case SampleDataID.collID     => Some(SampleDataID)
    case SchemaSourceID.collID   => Some(SchemaSourceID)
    case TaskID.collID           => Some(TaskID)
    case _                       => None
  }

  /** Returns the (FQL1) name of the native collection. */
  def toName(id: CollectionID, apiVersion: APIVersion): Option[String] = id match {
    // schema collections
    case Database                                  => Some("databases")
    case Collection if apiVersion < APIVersion.V27 => Some("classes")
    case Collection                                => Some("collections")
    case Index                                     => Some("indexes")
    case UserFunction                              => Some("functions")
    case Role                                      => Some("roles")
    case AccessProvider                            => Some("access_providers")

    // public collections
    case Key         => Some("keys")
    case Token       => Some("tokens")
    case Credentials => Some("credentials")

    // internal collections
    case SampleData   => Some("sample_data")
    case JournalEntry => Some("journal_entry")
    case Task         => Some("tasks")
    case SchemaSource => Some("schema")
    case _            => None
  }

  /** Identifies the native collection from its (FQL1) name. */
  def fromName(name: String): Option[CollectionID] = name match {
    // schema collections
    case "databases"        => Some(Database)
    case "classes"          => Some(Collection)
    case "collections"      => Some(Collection)
    case "indexes"          => Some(Index)
    case "functions"        => Some(UserFunction)
    case "roles"            => Some(Role)
    case "access_providers" => Some(AccessProvider)

    // public collections
    case "keys"        => Some(Key)
    case "tokens"      => Some(Token)
    case "credentials" => Some(Credentials)

    // internal collections cannot be looked up by name
    case _ => None
  }
}

/** Native collections used for internal system state. */
object InternalCollectionID {

  /** Identifies the ID subtype for the internal collection with given id. */
  def unapply(id: CollectionID): Option[IDCompanion[_]] = id match {
    case SampleDataID.collID   => Some(SampleDataID)
    case JournalEntryID.collID => Some(JournalEntryID)
    case TaskID.collID         => Some(TaskID)
    case SchemaSourceID.collID => Some(SchemaSourceID)
    case _                     => None
  }
}

/** Native collections which are part of the public data model. */
object PublicCollectionID {

  /** Identifies the ID subtype for the public collection with given id. */
  def unapply(id: CollectionID): Option[IDCompanion[_]] = id match {
    case KeyID.collID           => Some(KeyID)
    case TokenID.collID         => Some(TokenID)
    case CredentialsID.collID   => Some(CredentialsID)
    case SchemaCollectionID(id) => Some(id)
    case _                      => None
  }
}

/** Native collections which store the User's schema definitions. */
object SchemaCollectionID {
  lazy val All: Set[CollectionID] = NativeCollectionID.All collect {
    case SchemaCollectionID(id) => id.collID
  }
  lazy val Selector = RSelector.Schema(All)

  /** Identifies the ID subtype for the schema collection with given id. */
  def unapply(id: CollectionID): Option[IDCompanion[_]] = id match {
    case DatabaseID.collID       => Some(DatabaseID)
    case IndexID.collID          => Some(IndexID)
    case CollectionID.collID     => Some(CollectionID)
    case UserFunctionID.collID   => Some(UserFunctionID)
    case RoleID.collID           => Some(RoleID)
    case AccessProviderID.collID => Some(AccessProviderID)
    case _                       => None
  }
}

/** Native collections which have string names instead of IDs. */
object NamedCollectionID {

  /** Identifies the ID subtype for the named collection with given id. */
  def unapply(id: CollectionID): Option[IDCompanion[_]] = id match {
    case DatabaseID.collID       => Some(DatabaseID)
    case IndexID.collID          => Some(IndexID)
    case CollectionID.collID     => Some(CollectionID)
    case UserFunctionID.collID   => Some(UserFunctionID)
    case RoleID.collID           => Some(RoleID)
    case AccessProviderID.collID => Some(AccessProviderID)
    case _                       => None
  }

  object AliasedCollectionID {

    /** Identifies the ID subtype for the aliased collections with given id. */
    def unapply(id: CollectionID): Option[IDCompanion[_]] = id match {
      case CollectionID.collID   => Some(CollectionID)
      case UserFunctionID.collID => Some(UserFunctionID)
      case _                     => None
    }
  }
}

/** Contains the static schema configuration for a built-in collection. */
sealed abstract trait NativeCollection[I <: ID[I]] {

  /** The type of ID used to refer to entities in this native collection. */
  def idCompanion: IDCompanion[I]

  protected implicit def _idCompanion = idCompanion
  protected implicit def _collIDTag = idCompanion.collIDTag

  /** The native indexes that are configured on this native collection. */
  def nativeIndexes: ScopeID => List[NativeCollectionIndex]

  /** The fields that are permissible in documents in this native collection. */
  def fields: ScopeID => Map[String, FieldSchema]

  /** The permissible type of arbitrary fields in documents in this collection. */
  def wildcard: Option[SchemaType]

  /** The source of IDs for this collection. */
  def idSource: DocIDSource

  /** The duration which history will be retained for documents in this collection. */
  def historyDuration: Duration

  /** The CollectionID for this collection. */
  def id = idCompanion.collID

  /** The name of this collection. */
  def name = idCompanion.typeName

  /** List of hooks to be executed when a document in the collection is written. */
  def writeHooks: List[WriteHook]

  /** List of revalidation hooks to be executed at the end of query evaluation. */
  def revalidationHooks: List[PostEvalHook.Factory]

  /** Retrieves the set of user-defined indexes configured on this native
    * collection.
    */
  def userIndexes(scope: ScopeID): Query[Iterable[IndexConfig]]

  /** Gets the full collection configuration for this native collection. */
  def apply(
    scope: ScopeID,
    lookupIndexes: Boolean = true): Query[CollectionConfig.WithID[I]] =
    applyImpl(scope, lookupIndexes).asInstanceOf[Query[CollectionConfig.WithID[I]]]

  protected def applyImpl(
    scope: ScopeID,
    lookupIndexes: Boolean = true): Query[CollectionConfig] = {
    val userIndexesQ = if (lookupIndexes) {
      userIndexes(scope)
    } else {
      Query.value(Nil)
    }

    val fieldsQ = GlobalContext.FlaggedTypeMembers.get(name) match {
      case None => Query.value(fields(scope))
      case Some(fieldFlags) =>
        model.Database.forScope(scope).flatMap {
          case None => Query.value(fields(scope))
          case Some(db) =>
            fieldFlags.foldLeft(Query.value(fields(scope))) {
              case (fields, (field, feat)) =>
                db.account.map(_.flags.get(feat)).flatMap {
                  case true  => fields
                  case false => fields.map(_.removed(field))
                }
            }
        }
    }

    for {
      userIndexes <- userIndexesQ
      fields      <- fieldsQ
    } yield CollectionConfig(
      scope,
      name,
      id,
      nativeIndexes(scope),
      userIndexes.toList,
      collIndexes = List.empty,
      checkConstraints = List.empty,
      historyDuration,
      Duration.Inf,
      documentTTLs = true,
      Timestamp.Epoch,
      fields,
      computedFields = Map.empty,
      computedFieldSigs = SeqMap.empty,
      definedFields = Map.empty,
      wildcard = None,
      idSource,
      writeHooks = if (lookupIndexes) writeHooks else List(disallowWriteHook),
      revalidationHooks,
      internalMigrations = MigrationList.empty,
      internalStagedMigrations = None
    )
  }

  private val disallowWriteHook = WriteHook { case (schema, _) =>
    throw new IllegalStateException(s"Invalid collection write to $schema")
  }
}

/** Native collections have static schema configuration, which is defined here.
  * The exception is index configuration: users may define custom indexes on
  * public collections.
  */
object NativeCollection {

  /** Returns the static configuration for the native collection with given ID. */
  def apply(id: CollectionID): NativeCollection[_] = id match {
    case NativeCollection(cfg) => cfg
    case _ => throw new IllegalArgumentException(s"Invalid NativeCollectionID: $id")
  }

  /** Looks up the static configuration for the native collection with given ID. */
  def unapply(id: CollectionID): Option[NativeCollection[_]] = id match {
    case InternalCollection(cfg) => Some(cfg)
    case PublicCollection(cfg)   => Some(cfg)
    case _                       => None
  }

  /** Looks up a native collection by name. */
  def unapply(name: String): Option[NativeCollection[_]] = name match {
    // Only public collections can be looked up by name.
    case PublicCollection(cfg) => Some(cfg)
    case _                     => None
  }

  /** helper for native index adaptation */
  implicit class NativeIdxOps(val cfg: IndexConfig.Native) extends AnyVal {
    def collIndexOn(fields: String*): NativeCollectionIndex = {
      val terms = fields.map { f =>
        CollectionIndex.Term(CollectionIndex.Field.Fixed(f), Some(false))
      }
      NativeCollectionIndex(cfg, terms.toVector, Vector.empty)
    }
  }
}

/** Stores the static schema configuration for an internal collection */
sealed abstract class InternalCollection[I <: ID[I]: IDCompanion](
  val nativeIndexes: ScopeID => List[NativeCollectionIndex],
  val fields: ScopeID => Map[String, FieldSchema],
  val wildcard: Option[SchemaType] = None,
  val idSource: DocIDSource = DocIDSource.Snowflake,
  val historyDuration: Duration)
    extends NativeCollection[I] {

  val idCompanion = implicitly[IDCompanion[I]]
  val writeHooks = Nil
  val revalidationHooks = Nil

  def userIndexes(scope: ScopeID): Query[Iterable[IndexConfig]] =
    // User-defined indexes cannot apply to internal collections.
    Query.value(List.empty[IndexConfig])
}

/** Internal collections are used for system state, and are inaccessible to users. */
object InternalCollection {

  /** Returns the static configuration for the internal collection with given ID. */
  def apply(id: CollectionID): InternalCollection[_] = id match {
    case InternalCollection(cfg) => cfg
    case _ =>
      throw new IllegalArgumentException(s"Invalid InternalCollectionID: $id")
  }

  /** Looks up the static configuration for the internal collection with given ID. */
  def unapply(id: CollectionID): Option[InternalCollection[_]] = id match {
    case JournalEntryID.collID => Some(JournalEntry)
    case SampleDataID.collID   => Some(SampleData)
    case SchemaSourceID.collID => Some(SchemaSource)
    case TaskID.collID         => Some(Task)
    case _                     => None
  }

  // No unapply(name: String) -- InternalCollections cannot be looked up by name.

  object JournalEntry
      extends InternalCollection[JournalEntryID](
        // TODO: define JournalEntry fields
        fields = scala.Function.const(Map.empty),
        nativeIndexes = { scope =>
          List(
            NativeCollectionIndex.Documents(scope),
            NativeCollectionIndex.Changes(scope),
            NativeIndex.JournalEntryByHost(scope).collIndexOn(),
            NativeIndex.JournalEntryByHostAndTag(scope).collIndexOn(),
            NativeIndex.JournalEntryByTag(scope).collIndexOn()
          )
        },
        historyDuration = 0.days
      )

  object SampleData
      extends InternalCollection[SampleDataID](
        // TODO: define SampleData fields
        fields = scala.Function.const(Map.empty),
        nativeIndexes = { scope =>
          List(
            NativeCollectionIndex.Documents(scope),
            NativeCollectionIndex.Changes(scope))
        },
        historyDuration = 0.days
      )

  object SchemaSource
      extends InternalCollection[SchemaSourceID](
        fields = scala.Function.const(
          Map(
            "name" -> Str,
            "ext" -> Union(Singleton("fsl"), Singleton("json")),
            "content" -> Str
          )),
        nativeIndexes = { scope =>
          List(
            NativeCollectionIndex.Documents(scope),
            NativeCollectionIndex.Changes(scope))
        },
        historyDuration = Duration.Inf,
        idSource = DocIDSource.Sequential(
          SchemaSourceID.MinValue.toLong,
          SchemaSourceID.MaxValue.toLong
        )
      )

  object Task
      extends InternalCollection[TaskID](
        // TODO: define Task fields
        fields = scala.Function.const(Map.empty),
        nativeIndexes = { scope =>
          List(
            NativeCollectionIndex.Documents(scope),
            NativeCollectionIndex.Changes(scope),
            NativeIndex.TaskByCompletion(scope).collIndexOn(),
            NativeIndex.ExecutingTasksByAccountAndName(scope).collIndexOn(),
            NativeIndex.PrioritizedTasksByCreatedAt(scope).collIndexOn()
          )
        },
        historyDuration = 1.days
      )
}

/** Stores the static schema configuration for a public collection */
sealed abstract class PublicCollection[I <: ID[I]: IDCompanion](
  val nativeIndexes: ScopeID => List[NativeCollectionIndex],
  val fields: ScopeID => Map[String, FieldSchema],
  val wildcard: Option[SchemaType] = None,
  val idSource: DocIDSource = DocIDSource.Snowflake,
  val historyDuration: Duration,
  val ttlDuration: Duration = Duration.Inf,
  val writeHooks: List[WriteHook] = Nil,
  val revalidationHooks: List[PostEvalHook.Factory] = Nil
) extends NativeCollection[I] {

  val idCompanion = implicitly[IDCompanion[I]]

  def userIndexes(scope: ScopeID): Query[List[IndexConfig]] =
    ModelIndex.getUserIndexes(scope, id) map { _.toList }
}

/** Native collections that are visible to the user. */
object PublicCollection {

  /** Returns the static configuration for the public collection with given ID. */
  def apply(id: CollectionID): PublicCollection[_] = id match {
    case PublicCollection(cfg) => cfg
    case _ => throw new IllegalArgumentException(s"Invalid PublicCollectionID: $id")
  }

  /** Looks up the static configuration for the public collection with given ID. */
  def unapply(id: CollectionID): Option[PublicCollection[_]] = id match {
    case KeyID.collID          => Some(Key)
    case TokenID.collID        => Some(Token)
    case CredentialsID.collID  => Some(Credential)
    case SchemaCollection(cfg) => Some(cfg)
    case _                     => None
  }

  /** Looks up a public collection by name. */
  def unapply(name: String): Option[PublicCollection[_]] = name match {
    case KeyID.typeName   => Some(Key)
    case TokenID.typeName => Some(Token)
    case CredentialsID.typeName | "Credentials" =>
      Some(Credential) // Alias for early v10 users.
    case SchemaCollection(coll) => Some(coll)
    case _                      => None
  }

  object Key
      extends PublicCollection[KeyID](
        fields = { scope =>
          Map(
            "role" -> Str,
            "database" -> FieldSchema(
              SchemaType.Optional(
                SchemaType.Union(
                  ScalarType.Str,
                  ScalarType.DocType(NativeCollectionID.Database, "Database"))),
              readFieldImp = KeysSchemaHooks.databaseDocToName(scope)
            ),
            "ttl" -> Optional(Time),
            "secret" -> FieldSchema(Optional(Str), readOnly = true),
            "hashed_secret" -> FieldSchema(Optional(Str), internal = true),
            "data" -> FieldSchema(Optional(AnyRecord))
          )
        },
        nativeIndexes = { scope =>
          List(
            NativeCollectionIndex.Documents(scope),
            NativeCollectionIndex.Changes(scope),
            NativeIndex.KeyByDatabase(scope).collIndexOn(".database"))
        },
        historyDuration = 0.days,
        writeHooks = List(KeysSchemaHooks.writeHook),
        revalidationHooks = {
          import Validators.NameValidationHook._

          List(
            Validators.NameValidationHook(Field(FieldType.Database)("database")),
            Validators.NameValidationHook(Field(FieldType.Role)("role"))
          )
        }
      )

  object Token
      extends PublicCollection[TokenID](
        fields = scala.Function.const(
          Map(
            "document" -> FieldSchema(
              AnyDoc,
              aliasTo = Some("instance"),
              validator = Validators.UserCollectionDoc),
            "secret" -> FieldSchema(Optional(Str), readOnly = true),
            "hashed_secret" -> FieldSchema(Optional(Str), internal = true),
            "global_id" -> FieldSchema(Optional(Long), internal = true),
            "ttl" -> FieldSchema(Optional(Time)),
            "data" -> FieldSchema(Optional(AnyRecord))
          )),
        nativeIndexes = { scope =>
          List(
            NativeCollectionIndex.Documents(scope),
            NativeCollectionIndex.Changes(scope),
            NativeIndex.TokenByDocument(scope).collIndexOn(".document"))
        },
        historyDuration = 0.days
      )

  object Credential
      extends PublicCollection[CredentialsID](
        fields = scala.Function.const(
          Map(
            "document" -> FieldSchema(
              AnyDoc,
              aliasTo = Some("instance"),
              validator = Validators.UserCollectionDoc),
            "password" -> FieldSchema(Optional(Str)),
            "hashed_password" -> FieldSchema(Optional(Str), internal = true)
          )),
        nativeIndexes = { scope =>
          List(
            NativeCollectionIndex.Documents(scope),
            NativeCollectionIndex.Changes(scope),
            NativeIndex.CredentialsByDocument(scope).collIndexOn(".document"))
        },
        historyDuration = 0.days,
        writeHooks = List(CredentialsSchemaHooks.writeHook)
      )
}

/** Stores the static schema configuration for a named schema collection. */
sealed abstract class SchemaCollection[I <: ID[I]: IDCompanion](
  nativeIndexes: ScopeID => List[NativeCollectionIndex],
  fields: ScopeID => Map[String, FieldSchema],
  wildcard: Option[SchemaType] = None,
  idSource: DocIDSource = DocIDSource.Snowflake,
  historyDuration: Duration = Duration.Inf,
  ttlDuration: Duration = Duration.Inf,
  foreignKeys: List[ForeignKey.Src] = Nil,
  writeHooks: List[WriteHook] = Nil,
  revalidationHooks: List[PostEvalHook.Factory] = Nil)
    extends PublicCollection[I](
      nativeIndexes,
      fields,
      wildcard,
      idSource,
      historyDuration,
      ttlDuration,
      writeHooks :+ SchemaCollection.updateSchemaVersHook :++ SchemaCollection
        .writeHooksForForeignKeys(foreignKeys),
      revalidationHooks :++ SchemaCollection.validationHookForForeignKeys(
        foreignKeys)
    ) {

  /** Retrieves the name of the entity in this collection with given ID. */
  def idByNameStaged(scope: ScopeID, name: String): Query[Option[I]] =
    Cache.idByName[I](scope, name).map(_.flatMap(_.staged))

  /** Retrieves the DocID for the named entity in this collection. */
  def docIDByNameStaged(scope: ScopeID, name: String): Query[Option[DocID]] =
    idByNameStaged(scope, name) mapT { _.toDocID }
}

/** Schema collections are public native collections that enable the user to
  * configure their database schema.
  */
object SchemaCollection {

  private val updateSchemaVersHook = WriteHook(
    applyInternal = true,
    fn = { case (schema, ev) =>
      val scopes = Set.newBuilder[ScopeID]

      if (schema.collID == DatabaseID.collID) {
        ev.action match {
          case DocAction.Create =>
            // On create:
            // - Bump the parent scope for name lookups.
            // - No need to bump the child scope, as there isn't anything to cache in
            //   there yet.
            scopes += schema.scope
          case DocAction.Delete =>
            // On delete:
            // - Bump the parent scope, to refresh name lookups.
            // - Bump the child scope, as all the cached items are now invalid.
            scopes += schema.scope
            scopes += ev.prevData.get.apply(model.Database.ScopeField)
          case DocAction.Update =>
            // On update:
            // - Bump the parent scope if the name changes (for name lookups).
            // - Bump the child scope always (strictly speaking, we don't need to
            //   bump it if only the `name` or `data` fields change, but for simplicity
            //   we always bump the child scope).
            val oldName = ev.prevData.flatMap(_.getOpt(SchemaNames.NameField))
            val newName = ev.newData.flatMap(_.getOpt(SchemaNames.NameField))

            if (newName != oldName) {
              scopes += schema.scope
            }

            scopes += ev.newData.get.apply(model.Database.ScopeField)
        }
      } else {
        scopes += schema.scope
      }

      scopes
        .result()
        .map(CacheStore.updateSchemaVersion)
        .join
        .map(_ => Nil)
    }
  )

  private def writeHooksForForeignKeys(
    foreignKeys: List[ForeignKey.Src]): Option[WriteHook] = {
    Option.when(foreignKeys.nonEmpty) {
      FKUpdateHook(foreignKeys: _*)
    }
  }

  private def validationHookForForeignKeys(
    foreignKeys: List[ForeignKey.Src]): Option[PostEvalHook.Factory] = {
    val filtered = foreignKeys.flatMap { _.forRevalidation }
    Option.when(filtered.nonEmpty) {
      Validators.FKValidationHook(filtered: _*)
    }
  }

  /** Returns the static configuration for the schema collection with given ID. */
  def apply(id: CollectionID): SchemaCollection[_] = id match {
    case SchemaCollection(cfg) => cfg
    case _ => throw new IllegalArgumentException(s"Invalid SchemaCollectionID: $id")
  }

  /** Looks up the static configuration for the schema collection with given ID. */
  def unapply(id: CollectionID): Option[SchemaCollection[_]] = id match {
    case DatabaseID.collID       => Some(Database)
    case CollectionID.collID     => Some(Collection)
    case IndexID.collID          => Some(Index)
    case UserFunctionID.collID   => Some(UserFunction)
    case RoleID.collID           => Some(Role)
    case AccessProviderID.collID => Some(AccessProvider)
    case _                       => None
  }

  /** Looks up a schema collection by name. */
  def unapply(name: String): Option[SchemaCollection[_]] = name match {
    case DatabaseID.typeName       => Some(Database)
    case IndexID.typeName          => Some(Index)
    case CollectionID.typeName     => Some(Collection)
    case UserFunctionID.typeName   => Some(UserFunction)
    case RoleID.typeName           => Some(Role)
    case AccessProviderID.typeName => Some(AccessProvider)
    case _                         => None
  }

  object Database
      extends SchemaCollection[DatabaseID](
        fields = scala.Function.const(
          Map(
            "name" -> FieldSchema(
              Str,
              validator = Validators.ValidIdentifierValidator),
            "scope" -> FieldSchema(Optional(Long), internal = true),
            "global_id" -> FieldSchema(
              Optional(Long),
              readOnly = true,
              readFieldImp = { globalID =>
                Query.value(Value.Str(model.Database.encodeGlobalID(
                  GlobalDatabaseID(globalID.as[Long]))))
              }),
            "priority" -> FieldSchema(Optional(Long)),
            "typechecked" -> FieldSchema(Optional(Boolean)),
            "protected" -> FieldSchema(Optional(Boolean)),
            "container" -> FieldSchema(Optional(Boolean), internal = true),
            // NB. API allows account to be written to iff ancestor scope does not
            // have an account ID set.
            "account" -> FieldSchema(Optional(AnyRecord)),
            "disabled" -> FieldSchema(Optional(Boolean), internal = true),
            "active_schema_version" -> FieldSchema(Optional(Long), internal = true),
            "native_schema_version" -> FieldSchema(Optional(Long), internal = true),
            "mvt_pins" -> FieldSchema(Optional(Int), internal = true),
            "mvt_pin_ts" -> FieldSchema(Optional(Time), internal = true),
            // `region_group` was allowed at some point, and has since been removed.
            // This is here to allow databases with this field to be updated.
            "region_group" -> FieldSchema(Optional(Str), internal = true),
            // `api_version` is a field from v4 that is no longer supported.
            "api_version" -> FieldSchema(Optional(Str), internal = true),
            "data" -> FieldSchema(Optional(AnyRecord))
          )
        ),
        nativeIndexes = { scope =>
          List(
            NativeCollectionIndex.Documents(scope),
            NativeCollectionIndex.Changes(scope),
            NativeIndex.DatabaseByName(scope).collIndexOn(".name"),
            NativeIndex.DatabaseByDisabled(scope).collIndexOn(".disabled"),
            NativeIndex.DatabaseByAccountID(scope).collIndexOn(".account.id")
          )
        },
        writeHooks = List(DatabaseSchemaHooks.writeHook),
        foreignKeys = {
          import ForeignKey._

          List(Src("name")(Target(Location.Key)("database")))
        },
        revalidationHooks = List(
          TypeEnvValidator.Hook
        )
      ) {

    override def userIndexes(scope: ScopeID): Query[List[IndexConfig]] =
      Query.value(Nil)
  }

  val ComputedField =
    SchemaType.ObjectType(
      SchemaType.StructSchema(
        Map.empty,
        Some(
          SchemaType.Record(
            "body" -> FieldSchema(
              Str,
              validator = Validators.LambdaParseValidator(Arity(1))
            ),
            "signature" -> FieldSchema(
              Optional(Str),
              validator = Validators.UserSigValidator
            )
          ))
      )
    )

  val DefinedField =
    SchemaType.ObjectType(
      SchemaType.StructSchema(
        Map.empty,
        Some(SchemaType.Record(
          "signature" -> FieldSchema(
            Str,
            validator = Validators.SchemaTypeExprValidator
          ),
          "default" -> FieldSchema(Optional(Str)),
          "backfill_value" -> FieldSchema(
            Any,
            internal = true,
            readOnly = true
          )
        ))
      ))

  val Migration = Union(
    Record(
      "backfill" -> Record(
        "field" -> Str,
        "value" -> FieldSchema(Str, validator = Validators.ExprParseValidator))),
    Record("drop" -> Record("field" -> Str)),
    Record("split" -> Record("field" -> Str, "to" -> Array(Str))),
    Record("move" -> Record("field" -> Str, "to" -> Str)),
    Record("add" -> Record("field" -> Str)),
    Record("move_conflicts" -> Record("into" -> Str)),
    Record("move_wildcard" -> Record("into" -> Str)),
    Record("add_wildcard" -> Record())
  )

  val MigrationsField = Array(Migration)

  val CheckConstraint =
    SchemaType.Record(
      "check" -> FieldSchema(
        SchemaType.Record(
          "name" -> FieldSchema(
            Str,
            validator = Validators.ValidIdentifierValidator),
          "body" -> FieldSchema(
            Str,
            validator = Validators.LambdaParseValidator(Arity(1)))
        ))
    )

  val UniqueConstraint =
    SchemaType.Record(
      CollectionIndexSchemaTypes.uniqueFieldName -> FieldSchema(
        Union(Array(Str), Array(Record("field" -> Str, "mva" -> Optional(Boolean)))),
        validator = IndexFieldValidator.uniqueConstraintValidator
      ),
      CollectionIndexSchemaTypes.statusFieldName -> FieldSchema(
        Optional(Str),
        readOnly = true
      )
    )

  object Collection
      extends SchemaCollection[CollectionID](
        fields = { scope =>
          Map(
            "name" -> FieldSchema(
              Str,
              validator = GlobalNamespaceValidator.globalNameValidator(
                scope
              )),
            "alias" -> FieldSchema(
              Optional(Str),
              validator = GlobalNamespaceValidator.globalAliasValidator(
                scope
              )
            ),
            // TODO: validate that the user-defined signature (if any) matches the
            // infered type of the provided lambda.
            "computed_fields" -> FieldSchema(
              SchemaType.Optional(ComputedField),
              validator = CollectionValidators.memberValidator(
                CollectionValidators.isNameReservedInDoc(_))),
            // TODO: Prevent dupes between `computed_fields` and `fields`.
            "fields" -> FieldSchema(
              SchemaType.Optional(DefinedField),
              validator = CollectionValidators.memberValidator(
                CollectionValidators.isNameReservedInDoc(_))),
            "migrations" -> FieldSchema(SchemaType.Optional(MigrationsField)),
            "wildcard" -> FieldSchema(
              SchemaType.Optional(Str),
              validator = Validators.UserSigValidator),
            "history_days" -> FieldSchema(
              Optional(Long),
              validator = Validators.NonNegativeValidator,
              default = Some(_ =>
                Query.value(
                  SchemaResult.Ok(
                    model.Document.DefaultRetainDays
                  )))),
            "ttl_days" -> FieldSchema(
              Optional(Long),
              validator = Validators.NonNegativeValidator),
            "document_ttls" -> FieldSchema(Optional(Boolean)),
            "indexes" -> FieldSchema(
              SchemaType.Optional(
                SchemaType.ObjectType(
                  SchemaType.StructSchema(
                    Map.empty,
                    Some(CollectionIndexSchemaTypes.UserDefinedIndexType)))),
              validator = CollectionValidators.memberValidator(
                CollectionValidators.isNameReservedInColl(_))
            ),
            "backingIndexes" -> FieldSchema(
              SchemaType.Optional(
                SchemaType.Array(CollectionIndexSchemaTypes.BackingIndexType)),
              internal = true
            ),
            "internal_signatures" -> FieldSchema(
              SchemaType.Optional(
                SchemaType.Record("doc" -> FieldSchema(SchemaType.Optional(Str)))),
              internal = true
            ),
            "internal_migrations" -> FieldSchema(
              Optional(
                Array(
                  Record(
                    "version" -> FieldSchema(Time),
                    "migration" -> FieldSchema(AnyRecord)
                  ))),
              internal = true),

            // NB: If you are changing this and the CheckConstraints feature flag
            // hasn't been retired, update the pruning in NativeCollection.apply.
            "constraints" -> FieldSchema(
              SchemaType.Optional(
                SchemaType.Array(
                  SchemaType.Union(CheckConstraint, UniqueConstraint)))
            ),
            "data" -> FieldSchema(Optional(AnyRecord)),
            "has_class_index" -> FieldSchema(Optional(Boolean), internal = true),
            "min_valid_time" -> FieldSchema(Optional(Time), internal = true)
          )
        },
        nativeIndexes = { scope =>
          List(
            NativeCollectionIndex.Documents(scope),
            NativeCollectionIndex.Changes(scope),
            NativeIndex.CollectionByName(scope).collIndexOn(".name"),
            NativeIndex.CollectionByAlias(scope).collIndexOn(".alias")
          )
        },
        idSource = DocIDSource.Sequential(
          UserCollectionID.MinValue.toLong,
          UserCollectionID.MaxValue.toLong),
        // NOTE: !!!! READ THIS !!!!
        //
        // Collection retention MUST BE INFINITE. Otherwise the collection's
        // persisted
        // min. valid time can be missing and, therefore, the derived MVT at a given
        // snapshot time will risk exposing previously dead versions.
        //
        // See `deriveMinValidTime` and `computeMinValidTimeFloor` (defined
        // on `model.Collection`) for details.
        historyDuration = Duration.Inf,
        foreignKeys = {
          import ForeignKey._

          List(
            // NB: These foreign keys are revalidated by the SchemaTypeValidator.
            Src("name")(
              Target(Location.Role, revalidate = false)("privileges", "resource")),
            Src("name")(
              Target(Location.Role, revalidate = false)("membership", "resource"))
          )
        },
        writeHooks = List(
          TenantRootWriteHook.writeHook,
          CollectionSchemaHooks.writeHook,
          DefaultFieldHooks.writeHook
        ),
        revalidationHooks = {
          List(
            TypeEnvValidator.Hook
          )
        }
      )

  object Index
      extends SchemaCollection[IndexID](
        // FIXME: these are only partially defined, and do not validate all
        // constraints.
        fields = scala.Function.const(
          Map(
            "name" -> FieldSchema(Str),
            "terms" -> FieldSchema(Any),
            "values" -> FieldSchema(Any),
            "source" -> FieldSchema(Any),
            "active" -> FieldSchema(Boolean),
            "partitions" -> FieldSchema(Optional(Long)),
            "unique" -> FieldSchema(Optional(Boolean)),
            "serialized" -> FieldSchema(Optional(Boolean)),
            "data" -> FieldSchema(Optional(AnyRecord)),
            "hidden" -> FieldSchema(Optional(Boolean), internal = true),
            "collection_index" -> FieldSchema(Optional(Boolean), internal = true)
          )),
        nativeIndexes = { scope =>
          List(
            NativeCollectionIndex.Documents(scope),
            NativeCollectionIndex.Changes(scope),
            NativeIndex.IndexByName(scope).collIndexOn(".name"))
        },
        idSource = DocIDSource
          .Sequential(UserIndexID.MinValue.toLong, UserIndexID.MaxValue.toLong)
      ) {

    override def userIndexes(scope: ScopeID): Query[List[IndexConfig]] =
      Query.value(Nil)
  }

  object UserFunction
      extends SchemaCollection[UserFunctionID](
        fields = { scope =>
          Map(
            "name" -> FieldSchema(
              Str,
              validator = GlobalNamespaceValidator.globalNameValidator(
                scope
              )),
            "alias" -> FieldSchema(
              Optional(Str),
              validator = GlobalNamespaceValidator.globalAliasValidator(
                scope
              )
            ),
            "role" -> FieldSchema(Optional(Str)),
            "body" -> FieldSchema(
              Union(V4Query, Str),
              validator = Validators.LambdaParseValidator(allowShortLambda = false)
            ),
            "signature" -> FieldSchema(
              Optional(Str),
              validator = Validators.UserSigValidator),
            "internal_sig" -> FieldSchema(Optional(Str), internal = true),
            "data" -> FieldSchema(Optional(AnyRecord))
          )
        },
        nativeIndexes = { scope =>
          List(
            NativeCollectionIndex.Documents(scope),
            NativeCollectionIndex.Changes(scope),
            NativeIndex.UserFunctionByName(scope).collIndexOn(".name"),
            NativeIndex.UserFunctionByAlias(scope).collIndexOn(".alias")
          )
        },
        foreignKeys = {
          import ForeignKey._

          List(
            // NB: These foreign keys are revalidated by the SchemaTypeValidator.
            Src("name")(
              Target(Location.Role, revalidate = false)("privileges", "resource"))
          )
        },
        writeHooks = List(
          UserFunctionSchemaHooks.writeHook,
          TenantRootWriteHook.writeHook
        ),
        revalidationHooks = {
          import Validators.NameValidationHook._

          // Validate that the function role is not being created as or
          // updated to admin when the user merely has server permissions.
          def FunctionPermissionsValidationHook =
            PostEvalHook.Revalidation("FunctionPermissionsValidation") {
              case (ctx, _, WriteHook.OnCreate(_, data))
                  if ctx.auth.permissions == ServerPermissions =>
                val errs = data.fields.get(List("role")) match {
                  case Some(StringV(role)) if role == "admin" =>
                    Seq(
                      ConstraintFailure.ValidatorFailure(
                        Path(Right("role")),
                        "Cannot create a function with admin permissions using the server role."))
                  case _ => Seq.empty
                }
                Query.value(errs)
              case (ctx, _, WriteHook.OnUpdate(_, prev, curr))
                  if ctx.auth.permissions == ServerPermissions =>
                val errs = prev.diffTo(curr).fields.get(List("role")) match {
                  case Some(StringV(role)) if role == "admin" =>
                    Seq(
                      ConstraintFailure.ValidatorFailure(
                        Path(Right("role")),
                        "Cannot grant a function admin permissions using the server role."))
                  case _ => Seq.empty
                }
                Query.value(errs)
              case _ => Query.value(Seq.empty)
            }

          List(
            Validators.NameValidationHook(Field(FieldType.Role)("role")),
            FunctionPermissionsValidationHook,
            TypeEnvValidator.Hook
          )
        }
      )

  val Membership = SchemaType.Record(
    "resource" -> FieldSchema(Str),
    "predicate" -> FieldSchema(
      Optional(Union(V4Query, Str)),
      validator = Validators.LambdaParseValidator(Arity(1)))
  )

  def ActionType(action: String, arity: Arity) =
    action -> FieldSchema(
      Optional(Union(Boolean, Str, V4Query)),
      validator = Validators.LambdaParseValidator(Some(arity)))

  val RoleAction = SchemaType.Record(
    ActionType("read", Arity(1)),
    ActionType("write", Arity(2, 3)),
    ActionType("create", Arity(1)),
    ActionType("create_with_id", Arity(1, 2)),
    ActionType("delete", Arity(1)),
    ActionType("history_write", Arity(4)),
    ActionType("history_read", Arity(1)),
    ActionType("unrestricted_read", Arity(1)),
    ActionType("call", Arity.Variable(0))
  )

  val Privilege = SchemaType.Record(
    "resource" -> FieldSchema(Str),
    "actions" -> FieldSchema(SchemaType.Optional(RoleAction))
  )

  object Role
      extends SchemaCollection[RoleID](
        fields = scala.Function.const(
          Map(
            "name" -> FieldSchema(Str, validator = Validators.RoleValidator),
            "privileges" -> FieldSchema(SchemaType.ZeroOrMore(Privilege)),
            "membership" -> FieldSchema(SchemaType.ZeroOrMore(Membership)),
            "data" -> FieldSchema(Optional(AnyRecord))
          )),
        nativeIndexes = { scope =>
          List(
            NativeCollectionIndex.Documents(scope),
            NativeCollectionIndex.Changes(scope),
            NativeIndex.RoleByName(scope).collIndexOn(".name"),
            // FIXME: we can't properly express the path in v10 because the we
            // index into an MVA'ed object.
            NativeIndex.RolesByResource(scope).collIndexOn()
          )
        },
        foreignKeys = List(
          ForeignKey.Src("name")(
            ForeignKey.Target(ForeignKey.Location.UserFunction)("role"),
            // NB: These foreign keys are revalidated by the SchemaTypeValidator.
            ForeignKey.Target(
              ForeignKey.Location.AccessProvider,
              revalidate = false
            )("roles"),
            ForeignKey.Target(
              ForeignKey.Location.AccessProvider,
              revalidate = false
            )("roles", "role"),
            ForeignKey.Target(ForeignKey.Location.Key)("role")
          )
        ),
        writeHooks = List(
          TenantRootWriteHook.writeHook,
          RoleSchemaHooks.writeHook
        ),
        revalidationHooks = List(
          PostEvalHook.Revalidation("MaxRoles") {
            case (_, cfg, ev) =>
              val data = ev.newData.getOrElse(Data.empty)
              model.Role.validateMaxRolesPerResource(cfg.parentScopeID, data) map {
                case Right(_) => Seq.empty
                case Left(err) =>
                  Seq(
                    ConstraintFailure.ValidatorFailure(
                      Path.Root,
                      s"The maximum number of roles per resource (${err.limit}) have been exceeded."
                    ))
              }
            case _ =>
              Query.value(Seq.empty)
          },
          TypeEnvValidator.Hook
        )
      )

  val AccessProviderRole = {
    val RoleWithPredicate = SchemaType.Record(
      "role" -> FieldSchema(Str),
      "predicate" -> FieldSchema(
        Union(V4Query, Str),
        validator = Validators.LambdaParseValidator(Arity(1)))
    )

    SchemaType.Union(Str, RoleWithPredicate)
  }

  def ZeroOrMore(tpe: SchemaType): SchemaType = Optional(Union(tpe, Array(tpe)))
  def OneOrMore(tpe: SchemaType): SchemaType = Union(tpe, Array(tpe))

  object AccessProvider
      extends SchemaCollection[AccessProviderID](
        fields = scala.Function.const(
          Map(
            "name" -> FieldSchema(
              Str,
              validator = Validators.ValidIdentifierValidator),
            "issuer" -> FieldSchema(Str),
            "jwks_uri" -> FieldSchema(Str, validator = Validators.UrlValidator),
            "roles" -> FieldSchema(
              SchemaType.ZeroOrMore(AccessProviderRole),
              validator = Validators.AccessProviderRolesValidator
            ),
            "audience" -> FieldSchema(Optional(Str), readOnly = true),
            "data" -> FieldSchema(Optional(AnyRecord))
          )),
        nativeIndexes = { scope =>
          List(
            NativeCollectionIndex.Documents(scope),
            NativeCollectionIndex.Changes(scope),
            NativeIndex.AccessProviderByName(scope).collIndexOn(".name"),
            NativeIndex.AccessProviderByIssuer(scope).collIndexOn(".issuer")
          )
        },
        writeHooks = List(
          TenantRootWriteHook.writeHook,
          AccessProvidersSchemaHooks.writeHook
        ),
        revalidationHooks = List(TypeEnvValidator.Hook)
      )
}
