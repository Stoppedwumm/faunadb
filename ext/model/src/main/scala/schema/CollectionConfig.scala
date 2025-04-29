package fauna.model.schema

import fauna.atoms._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.logging.ExceptionLogging
import fauna.model.{ Cache, Collection, SchemaNames }
import fauna.model.runtime.fql2.stdlib.CollectionCompanion
import fauna.model.runtime.fql2.FQLInterpCtx
import fauna.model.runtime.fql2.PostEvalHook
import fauna.model.schema.index.{
  CollectionIndex,
  NativeCollectionIndex,
  UniqueConstraintIndex,
  UserDefinedIndex
}
import fauna.repo.query.Query
import fauna.repo.schema.{
  CollectionSchema,
  DocIDSource,
  FieldSchema,
  ScalarType,
  SchemaType,
  WriteHook
}
import fauna.repo.schema.migration.MigrationList
import fauna.repo.IndexConfig
import fauna.storage.doc.Data
import fauna.storage.DocAction
import fql.ast.{ Name, Span, TypeExpr }
import fql.env.CollectionTypeInfo
import fql.typer.TypeShape
import scala.collection.{ SeqMap, View }
import scala.concurrent.duration.Duration
import scala.language.implicitConversions

/** Contains the schema configuration for a Collection.
  *
  * This class will eventually supplant/replace/merge with/etc. the
  * `model.Collection` object, once the transactional schema cache is a thing.
  * But in the meantime, we need a place to put FQL2 schema definitions.
  */
final case class CollectionConfig(
  /** The scope where this collection resides. */
  parentScopeID: ScopeID,
  /** The name of this collection. */
  name: String,
  /** The ID of this collection. */
  id: CollectionID,
  /** Native indexes that apply to this collection. */
  nativeIndexes: List[NativeCollectionIndex],
  /** User-defined indexes in the Index collection that apply to this collection. */
  indexConfigs: List[IndexConfig],
  /** User-defined indexes configured on this collection document (FQLX). */
  collIndexes: List[CollectionIndex],
  /** Check constraints on this collection. */
  checkConstraints: List[CheckConstraint],
  /** The duration that history will be retained for documents in this collection. */
  historyDuration: Duration,
  /** The duration that documents in this collection will be retained. */
  ttlDuration: Duration,
  /** Whether documents in this collection may have TTLs written. */
  documentTTLs: Boolean,
  /** The coll's minimum valid timestamp persisted across all its versions. */
  minValidTimeFloor: Timestamp,
  /** Schema definition for fields on documents in this collection. */
  fields: Map[String, FieldSchema],
  /** Computed fields for this collection. */
  computedFields: Map[String, ComputedField],
  /** Computed field sigs type that the TypeEnvValidator inferred */
  computedFieldSigs: SeqMap[String, TypeExpr],
  /** Defined fields for this collection. */
  definedFields: Map[String, DefinedField],
  /** Permitted type of wildcard fields living under data. */
  wildcard: Option[Wildcard],
  /** Source for document IDs. */
  idSource: DocIDSource,
  /** List of hooks which are executed when documents in the collection are written. */
  writeHooks: List[WriteHook],
  /** Optional revalidation hook that is executed at the end of query evaluation. */
  revalidationHooks: List[PostEvalHook.Factory],
  /** Internal migrations to apply on read */
  internalMigrations: MigrationList,
  /** Internal migrations for a staged schema. This will be a complete migration
    * list, so it will overlap with `internalMigrations`.
    */
  internalStagedMigrations: Option[MigrationList]
) extends ExceptionLogging {

  /** The CollectionSchema that represents this collection in interactions with
    * repo. Note that `wildcard` is applied inside the schema of `fields`, and
    * wildcard of the Schema describes wildcard fields allowed *at the top level*.
    *
    * Cf. apply(Collection).
    */
  val wc = id match {
    case NativeCollectionID(_) => None
    case _                     => CollectionConfig.LegacySchema.Wildcard
  }
  lazy val Schema = CollectionSchema(
    parentScopeID,
    id,
    name,
    internalMigrations,
    fields,
    computedFields.keys.toList,
    wc,
    indexes,
    idSource,
    writeHooks,
    ttlDuration
  )

  /** The v10 runtime value */
  lazy val companionObject = CollectionCompanion.createUncached(this)

  lazy val typeInfo = {
    val defined = definedFields.view
      .map { case (n, p) => n -> (p.expectedTypeExpr, p.hasDefault) }

    // make sure to get a type for all computed fields.
    val computed = computedFields.view
      .map { case (n, _) =>
        n -> computedFieldSigs.getOrElse(n, TypeExpr.Any(Span.Null))
      }

    val wild = wildcard.map(_.expectedTypeExpr)

    val indexes = collIndexes.view.collect { case i: UserDefinedIndex =>
      val terms = i.terms.map { t => (t.field.parsePath, t.isMVA) }
      i.name -> terms
    }

    CollectionTypeInfo.Checked(
      Name(name, Span.Null),
      None,
      documentTTLs,
      defined.to(SeqMap),
      computed.to(SeqMap),
      wild,
      indexes.to(SeqMap),
      TypeShape.TypeHint.UserCollection
    )
  }

  /** The list of v4 indexes that apply to this collection. */
  def indexes = nativeIndexes.map(_.config) ++ indexConfigs

  def userIndexes: View[UserDefinedIndex] = collIndexes.view.collect {
    case i: UserDefinedIndex => i
  }
  def uniqueIndexes: View[UniqueConstraintIndex] = collIndexes.view.collect {
    case i: UniqueConstraintIndex => i
  }

  def constraintViolationMessage(prev: Option[(DocID, Data)], action: DocAction) = {

    val desc = (prev, id) match {
      case (None, NativeCollectionID(_)) => s"$name"
      case (None, _)                     => s"document in collection `$name`"

      case (Some((_, prev)), NamedCollectionID(_)) =>
        s"$name `${prev(SchemaNames.NameField).toString}`"
      case (Some((id, _)), NativeCollectionID(_)) =>
        s"$name with id ${id.subID.toLong}"
      case (Some((id, _)), _) =>
        s"document with id ${id.subID.toLong} in collection `$name`"
    }

    s"Failed to $action $desc."
  }
}

object CollectionConfig {

  type IDTag[I <: ID[I]] = { type ID = I }
  type WithID[I <: ID[I]] = CollectionConfig with IDTag[I]

  def NotFound(scope: ScopeID, id: CollectionID) =
    CollectionConfig(
      parentScopeID = scope,
      name = "NotFoundCollection",
      id = id,
      nativeIndexes = Nil,
      indexConfigs = Nil,
      collIndexes = Nil,
      checkConstraints = Nil,
      historyDuration = Duration.Inf,
      ttlDuration = Duration.Inf,
      documentTTLs = true,
      minValidTimeFloor = Timestamp.Epoch,
      fields = Map.empty,
      computedFields = Map.empty,
      computedFieldSigs = SeqMap.empty,
      definedFields = Map.empty,
      wildcard = None,
      idSource = DocIDSource.Snowflake,
      writeHooks = Nil,
      revalidationHooks = Nil,
      internalMigrations = MigrationList.empty,
      internalStagedMigrations = None
    )

  // querying helpers
  implicit def nativeCfgToStore[I <: ID[I]: CollectionIDTag](
    cfg: CollectionConfig.WithID[I]): ScopedStore.Native[I] =
    new ScopedStore.Native(Query.value(cfg))

  implicit def qNativeCfgToStore[I <: ID[I]: CollectionIDTag](
    cfg: Query[CollectionConfig.WithID[I]]): ScopedStore.Native[I] =
    new ScopedStore.Native(cfg)

  @annotation.nowarn("cat=unused")
  implicit def schemaCfgToStore[I <: ID[I]: ID.SchemaMarker: CollectionIDTag](
    cfg: CollectionConfig.WithID[I]): ScopedStore.Schema[I] =
    new ScopedStore.Schema(Query.value(cfg))

  @annotation.nowarn("cat=unused")
  implicit def qSchemaCfgToStore[I <: ID[I]: ID.SchemaMarker: CollectionIDTag](
    cfg: Query[CollectionConfig.WithID[I]]): ScopedStore.Schema[I] =
    new ScopedStore.Schema(cfg)

  /** Legacy schema for FQL1 documents. */
  object LegacySchema {
    val Fields = Map("ttl" -> FieldSchema(SchemaType.Optional(ScalarType.Time)))
    val Wildcard: Option[SchemaType] = Some(ScalarType.Any)
  }

  /** Gets the collection config by ID. */
  def apply(scope: ScopeID, id: CollectionID): Query[Option[CollectionConfig]] =
    id match {
      case NativeCollectionID(_) => NativeCollection(id)(scope) map { Some(_) }
      case UserCollectionID(_)   => User(scope, id)
      case _                     => Query.none
    }

  def getUncached(
    scope: ScopeID,
    id: CollectionID,
    lookupIndexes: Boolean = true): Query[Option[CollectionConfig]] =
    id match {
      case NativeCollectionID(_) => NativeCollection(id)(scope) map { Some(_) }
      case UserCollectionID(_)   => User.getUncached(scope, id, lookupIndexes)
      case _                     => Query.none
    }

  def getStagedUncached(
    scope: ScopeID,
    id: CollectionID,
    lookupIndexes: Boolean = true): Query[Option[CollectionConfig]] =
    id match {
      case NativeCollectionID(_) => NativeCollection(id)(scope) map { Some(_) }
      case UserCollectionID(_) =>
        Collection.getUncached(scope, id, lookupIndexes).map { viewOpt =>
          for {
            view   <- viewOpt
            staged <- view.staged
          } yield staged.config
        }
      case _ => Query.none
    }

  def getForSyncIndexBuild(
    scope: ScopeID,
    id: CollectionID): Query[Option[CollectionConfig]] =
    id match {
      case NativeCollectionID(_) => NativeCollection(id)(scope) map { Some(_) }
      case UserCollectionID(_) =>
        Collection.getForSyncIndexBuild(scope, id).mapT { coll => coll.config }
      case _ => Query.none
    }

  /** Gets the collection config by name. In the case of a conflict, built-in
    * collections are returned in preference to user-defined ones. Use
    * `CollectionConfig.User(scope, name)` to get the user-defined collection.
    */
  def apply(ctx: FQLInterpCtx, name: String): Query[Option[CollectionConfig]] =
    name match {
      case NativeCollection(coll) => coll(ctx.scopeID) map { Some(_) }
      case _                      => User(ctx, name)
    }

  /** Configuration for user-defined collections. */
  object User {

    def apply(scope: ScopeID, id: CollectionID): Query[Option[CollectionConfig]] =
      Cache.collByID(scope, id).map { _.flatMap(_.active.map(_.config)) }

    /** Uncached get the collection configuration for the given scope and id.
      *
      * Note: index access is still cached as currently there are no storage
      * predicate that allows reading indexes by source without fetching all indexes
      * for the source's scope.
      */
    def getUncached(
      scope: ScopeID,
      id: CollectionID,
      lookupIndexes: Boolean = true): Query[Option[CollectionConfig]] =
      Collection.getUncached(scope, id, lookupIndexes).map { viewOpt =>
        for {
          view   <- viewOpt
          active <- view.active
        } yield active.config
      }

    /** Retrieves the configuration for the user-defined collection with given name or alias. */
    def apply(ctx: FQLInterpCtx, name: String): Query[Option[CollectionConfig]] =
      Collection
        .idByIdentifier(ctx, name)
        .flatMapT { id => apply(ctx.scopeID, id) }

    /** Native indexes configured on every user-defined collection. */
    def nativeIndexes(scope: ScopeID): List[NativeCollectionIndex] =
      NativeCollectionIndex.Documents(scope) ::
        NativeCollectionIndex.Changes(scope) :: Nil
  }
}
