package fauna.model

import fauna.ast._
import fauna.atoms._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.logging.ExceptionLogging
import fauna.model.runtime.fql2.FQLInterpCtx
import fauna.model.runtime.fql2.Result
import fauna.model.schema.{
  CheckConstraint,
  CollectionConfig,
  ComputedField,
  ComputedFieldData,
  DefinedField,
  DefinedFieldData,
  NativeCollectionID,
  SchemaCollection,
  SchemaItemView,
  SchemaTypeResolver,
  Wildcard
}
import fauna.model.schema.index.{
  CollectionIndex,
  CollectionIndexManager,
  UniqueConstraintIndex,
  UserDefinedIndex
}
import fauna.model.schema.OptionalSchemaItemView
import fauna.repo._
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.repo.schema.{
  DocIDSource,
  FieldSchema,
  Path,
  ScalarType,
  SchemaResult,
  SchemaType,
  WriteHook
}
import fauna.repo.schema.migration.MigrationCodec._
import fauna.repo.schema.migration.MigrationList
import fauna.storage.doc._
import fauna.storage.ir.IRValue
import fauna.util.ReferencesValidator
import fql.ast.{ Expr, SchemaTypeExpr, Src, TypeExpr }
import fql.parser.Parser
import fql.typer.{ Type, Typer }
import scala.collection.immutable.SeqMap
import scala.collection.View
import scala.concurrent.duration._

case class Collection(config: CollectionConfig, alias: Option[String])
    extends ExceptionLogging {

  def id = config.id
  def parentScopeID = config.parentScopeID
  def name = config.name
  def collIndexes = config.collIndexes
  def userIndexes: View[UserDefinedIndex] = config.userIndexes
  def uniqueIndexes: View[UniqueConstraintIndex] = config.uniqueIndexes
  def schemaVersion: SchemaVersion = config.Schema.schemaVersion

  id match {
    case NativeCollectionID(_) =>
      throw new IllegalStateException(
        s"Cannot create a Collection for a native collection $id")
    case _ => ()
  }
}

object Collection extends ExceptionLogging {

  type StagedView = OptionalSchemaItemView[Collection]

  /** A Collection's minimum valid time (MVT) is delayed by this amount
    * to allow for stable set cursors when "history_days" = 0.
    *
    * WARNING: This value cannot be arbitrarily changed! If MVT moves
    * backward, inconsistent history may be revealed!
    */
  val MVTOffset = 15.minutes

  case class InvalidNativeCollectionID(id: CollectionID, message: String)
      extends Exception(message)

  def apply(
    vers: Version,
    staged: Option[Version],
    minValidTimeFloor: Timestamp,
    lookupIndexes: Boolean): Query[Collection] = {

    def getIndexes(vers: Version): Seq[CollectionIndex] = {
      if (!lookupIndexes) {
        return Nil
      }

      val b = Seq.newBuilder[CollectionIndex]
      val mgr = CollectionIndexManager(
        vers.parentScopeID,
        vers.id.as[CollectionID],
        vers.data)

      // If for some reason this fails, log the issue but don't blow up and make
      // the collection unusable.
      def backingIndexForDef(index: CollectionIndexManager.IndexDefinition) =
        squelchAndLogException(
          Some(mgr.findBackingIndexOrError(index)),
          { case _: IllegalStateException =>
            None
          })

      for {
        idx     <- mgr.userDefinedIndexes.values
        backing <- backingIndexForDef(idx)
      } {
        b += UserDefinedIndex(
          indexID = backing.indexID,
          name = idx.name,
          terms = idx.terms,
          values = idx.values,
          queryable = idx.queryable.getOrElse(false),
          status = idx.status
        )
      }

      for {
        uc      <- mgr.uniqueConstraints
        backing <- backingIndexForDef(uc.toIndexDefinition)
      } {
        b += UniqueConstraintIndex(
          indexID = backing.indexID,
          terms = backing.terms,
          values = backing.values,
          status = backing.status
        )
      }

      b.result()
    }

    val name = SchemaNames.findName(vers)

    val id = vers.id.as[CollectionID]
    val indexes = getIndexes(vers)
    val stagedIndexes = staged match {
      case Some(vers) => getIndexes(vers)
      case None       => Nil
    }
    val computedFields = ComputedField.fromData(name, vers.data)
    val definedFields = DefinedField.fromData(name, vers.data)
    val wildcard = Wildcard.fromData(name, vers.data)
    val internalMigrations = vers.data(InternalMigrationsField)
    val internalStagedMigrations = staged.map { _.data(InternalMigrationsField) }

    // These include v4, v10, and staged v10 indexes.
    val getIndexConfigsQ = if (!lookupIndexes) {
      Query.value(Nil)
    } else {
      Index.getUserIndexes(vers.parentScopeID, id).mapT { config =>
        internalStagedMigrations match {
          case Some(stagedMigrations) =>
            (
              indexes.exists(_.indexID == config.id),
              stagedIndexes.exists(_.indexID == config.id)) match {
              // A staged index.
              case (false, true) =>
                StagedIndexer(
                  config,
                  stagedMigrations,
                  internalMigrations.latestVersion)
              // Other cases:
              // (true, false) - An index that is removed by the staged migrations.
              // (true, true) - An index that is not modified by the staged
              //                migrations.
              // (false, false) - A v4 index.
              //
              // In all these cases, we can just return the index config as is.
              case _ => config
            }

          // Fast path for no staged migrations.
          case None => config
        }
      }
    }

    // If document_ttls is not set, it defaults to true if there are no defined
    // fields and false otherwise.
    val docTTLs = vers.data(DocumentTTLsField).getOrElse(definedFields.isEmpty)

    for {
      indexConfigs <- getIndexConfigsQ
      fields       <- lookupFieldSchema(vers.parentScopeID, definedFields, wildcard)
    } yield Collection(
      CollectionConfig(
        parentScopeID = vers.parentScopeID,
        name = name,
        id = id,
        nativeIndexes = CollectionConfig.User.nativeIndexes(vers.parentScopeID),
        indexConfigs = indexConfigs.toList,
        collIndexes = indexes.toList,
        checkConstraints = CheckConstraint.fromData(vers.data),
        historyDuration =
          vers.data(RetainDaysField).fold(Duration.Inf: Duration)(_.saturatedDays),
        ttlDuration =
          vers.data(TTLField).fold(Duration.Inf: Duration)(_.saturatedDays),
        documentTTLs = docTTLs,
        minValidTimeFloor = minValidTimeFloor,
        fields = fields,
        computedFields = computedFields,
        computedFieldSigs = parseComputedFieldSigs(vers),
        definedFields = definedFields,
        wildcard = wildcard,
        idSource = DocIDSource.Snowflake,
        writeHooks = if (lookupIndexes) Nil else List(disallowWriteHook),
        revalidationHooks = Nil,
        internalMigrations = internalMigrations,
        internalStagedMigrations = internalStagedMigrations
      ),
      alias = SchemaNames.findAlias(vers)
    )
  }

  private val disallowWriteHook = WriteHook { case (schema, _) =>
    throw new IllegalStateException(s"Invalid collection write to $schema")
  }

  private def lookupFieldSchema(
    scope: ScopeID,
    definedFields: Map[String, DefinedField],
    wildcard: Option[Wildcard]) = for {
    fields <- definedFields.map { case (name, field) =>
      fieldSchema(
        scope,
        new Path(List(Right(name))),
        field.expectedTypeExpr,
        field.default.map(defaultStr =>
          Parser.expr(defaultStr, Src.Inline(field.src, defaultStr)) match {
            case fql.Result.Ok(expr) => expr
            case fql.Result.Err(err) =>
              throw new IllegalStateException(
                s"Invalid default value for field $name: $err")
          })
      ).map { name -> _ }
    }.sequence
    wildcard <- wildcard match {
      case Some(wc) =>
        lookupType(scope, wc.expectedType).map(Some(_))
      case None => Query.none
    }
  } yield Map(
    "data" -> FieldSchema(
      SchemaType.ObjectType(
        SchemaType.StructSchema(
          fields.toMap,
          wildcard = Wildcard.expectedSchemaType(wildcard, definedFields.nonEmpty),
          isUserData = true
        ))
    )) ++ CollectionConfig.LegacySchema.Fields

  def defaultQ(path: Path, body: Expr)(
    scope: ScopeID): Query[SchemaResult[IRValue]] =
    DefinedField.evalDefault(scope, path, body)

  private def fieldSchema(
    scope: ScopeID,
    path: Path,
    te: SchemaTypeExpr,
    default: Option[Expr]): Query[FieldSchema] = {
    te match {
      case SchemaTypeExpr.Object(fields, wildcard, _) =>
        for {
          fields <- fields.map { case (name, te, default) =>
            fieldSchema(
              scope,
              new Path(path.elements :+ Right(name.str)),
              te,
              default).map {
              name.str -> _
            }
          }.sequence

          wildcardTy = wildcard.map { w =>
            Typer().typeTSchemeUncheckedType(TypeExpr.Scheme(Seq.empty, w))
          }
          wildcard <- wildcardTy.map { ty => lookupType(scope, ty) }.sequence

        } yield FieldSchema(
          SchemaType
            .ObjectType(
              SchemaType
                .StructSchema(
                  fields.toMap,
                  wildcard = wildcard,
                  isUserData = true
                )
            ),
          default = default.map { expr => defaultQ(path, expr) }
        )

      case SchemaTypeExpr.Simple(te) =>
        val ty = Typer().typeTSchemeUncheckedType(TypeExpr.Scheme(Seq.empty, te))

        lookupType(scope, ty).map { ty =>
          FieldSchema(ty, default = default.map { expr => defaultQ(path, expr) })
        }
    }
  }

  private def lookupType(scope: ScopeID, ty: Type): Query[SchemaType] = {
    // Ideally, this would error out if it found a non-persistable type. However,
    // there are two outstanding cases that make that difficult:
    // 1. After creating a collection in a query, the collection config is
    //    immediately created. This is difficult to work with, because schema
    //    validation happens at the end of the query.
    // 2. If typechecking is off for a database, non-persistable types can be
    //    created. So we just type those as `Null`, which disallows anything.
    SchemaTypeResolver(ty)(scope).map(_.getOrElse(ScalarType.Null))
  }

  // Config Instance Fields

  val RetainDaysField = Field[Option[Long]]("history_days")
  val TTLField = Field[Option[Long]]("ttl_days")
  val DocumentTTLsField = Field[Option[Boolean]]("document_ttls")
  val BackingIndexesField = Field[Option[Vector[Data]]]("backingIndexes")
  val ComputeField =
    Field[Option[List[(String, ComputedFieldData)]]]("computed_fields")
  val MigrationsField = Field[Option[IRValue]]("migrations")
  val DefinedFields =
    Field[Option[List[(String, DefinedFieldData)]]]("fields")
  val WildcardField = Field[Option[String]]("wildcard")
  val IndexesField = Field[Option[Data]]("indexes")
  val ConstraintsField = Field[Option[Vector[Data]]]("constraints")
  val MinValidTimeFloorField = Field[Timestamp]("min_valid_time")
  val BackfillValue = Field[String]("backfill_value")
  val InternalSignaturesField =
    Field[Option[List[(String, String)]]]("internal_signatures")
  val InternalDocSignatureField =
    Field[Option[String]]("internal_signatures", "doc")
  val InternalMigrationsField = Field[MigrationList]("internal_migrations")

  private object RetainDaysSignValidator extends Validator[Query] {
    override def filterMask: MaskTree = MaskTree.empty

    override def validateData(data: Data): Query[List[ValidationException]] =
      Query.value {
        data.getOpt(RetainDaysField).fold(List.empty[ValidationException]) {
          case Some(hd) if hd < 0 => List(InvalidNegative(RetainDaysField.path))
          case _                  => Nil
        }
      }
  }

  val RetainValidator = RetainDaysField.validator[Query] + TTLField
    .validator[Query] + RetainDaysSignValidator

  val VersionValidator =
    Document.DataValidator + RetainValidator

  def LiveValidator(ec: EvalContext) =
    VersionValidator + ReferencesValidator(ec)

  val UserCollectionVersionValidator =
    VersionValidator + SchemaNames.NameField.validator[Query]

  def UserCollectionLiveValidator(ec: EvalContext) =
    UserCollectionVersionValidator + ReferencesValidator(ec)

  val DefaultData = Data(RetainDaysField -> Some(Document.DefaultRetainDays))

  // Legacy Fields

  // Internal field that used to mark indexed collections. All collections are
  // indexed by default now. This field is kept due to the need of hiding internal
  // fields from rendered collections.
  val LegacyCollectionIndexField = Field[Option[Boolean]]("has_class_index")

  // Access

  def getUncached(
    scope: ScopeID,
    id: CollectionID,
    lookupIndexes: Boolean = true): Query[Option[Collection.StagedView]] =
    SchemaCollection.Collection(scope).schemaVersState(id).flatMapT {
      case SchemaItemView.Unchanged(v) =>
        getUncached0(scope, id, v, None, lookupIndexes).map { c =>
          Some(OptionalSchemaItemView.Unchanged(c))
        }
      case SchemaItemView.Created(v) =>
        getUncachedStaged(scope, id, v, lookupIndexes).map { c =>
          Some(OptionalSchemaItemView.Created(c))
        }
      case SchemaItemView.Deleted(v) =>
        getUncached0(scope, id, v, None, lookupIndexes).map { c =>
          Some(OptionalSchemaItemView.Deleted(c))
        }
      case SchemaItemView.Updated(v1, v2) =>
        (
          getUncached0(scope, id, v1, Some(v2), lookupIndexes),
          getUncachedStaged(scope, id, v2, lookupIndexes)).par { (v1, v2) =>
          Query.some(OptionalSchemaItemView.Updated(v1, v2))
        }
    }

  private def getUncachedStaged(
    scope: ScopeID,
    id: CollectionID,
    v: Version,
    lookupIndexes: Boolean): Query[Option[Collection]] = {
    // If this staged item has been committed, and the schema is valid, we can
    // look it up. Otherwise, we don't know if the collection is valid, so we
    // cannot build a collection config for it.
    //
    // TODO: Lookup if the staged schema is valid here.
    if (v.ts.validTSOpt.isDefined) {
      getUncached0(scope, id, v, None, lookupIndexes).map(Some(_))
    } else {
      Query.none
    }
  }

  /** Reads a Collection for an inline index build.
    *
    * NB: This will read pending writes for a collection that has been written
    * in the current transaction. Ideally, we would disallow this, but right
    * now its required for inline index builds to see the latest migration state.
    */
  def getForSyncIndexBuild(
    scope: ScopeID,
    id: CollectionID): Query[Option[Collection]] =
    SchemaCollection.Collection(scope).schemaVersState(id).flatMapT { view =>
      view.staged match {
        case Some(staged) =>
          getUncached0(scope, id, staged, None, lookupIndexes = false)
            .map(Some(_))
        case None => Query.none
      }
    }

  /** Reads the collection at the snapshot time. This is used in type env
    * validation, to see if there are any v4 indexes on the staged collection.
    */
  def getUncachedAtSnapshot(
    scope: ScopeID,
    id: CollectionID): Query[Option[Collection]] = {
    for {
      snap <- Query.snapshotTime
      data <- SchemaCollection.Collection(scope).get(id, snap)
      coll <- data match {
        case Some(v) =>
          getUncached0(scope, id, v, None, lookupIndexes = true).map(Some(_))
        case None => Query.none
      }
    } yield coll
  }

  private def getUncached0(
    scope: ScopeID,
    id: CollectionID,
    active: Version,
    staged: Option[Version],
    lookupIndexes: Boolean) = {
    val mvtQ = active.data.getOpt(MinValidTimeFloorField) match {
      case None     => computeMinValidTimeFloor(scope, id)
      case Some(ts) => Query.value(ts)
    }

    for {
      mvt  <- mvtQ
      coll <- apply(active, staged, mvt, lookupIndexes)
    } yield coll
  }

  def get(scope: ScopeID, id: CollectionID): Query[Option[Collection]] =
    getView(scope, id) flatMapT { c => Query.value(c.active) }

  def getStaged(scope: ScopeID, id: CollectionID): Query[Option[Collection]] =
    getView(scope, id) flatMapT { c => Query.value(c.staged) }

  private def getView(
    scope: ScopeID,
    id: CollectionID): Query[Option[Collection.StagedView]] =
    id match {
      case NativeCollectionID(_) =>
        throw new IllegalStateException(
          s"Cannot create a Collection for a native collection $id")
      case UserCollectionID(_) => Cache.collByID(scope, id)
      case _                   => Query.none
    }

  def getAll(scope: ScopeID): PagedQuery[Iterable[Collection]] =
    CollectionID.getAllUserDefined(scope) collectMT { get(scope, _) }

  def getAllUncached(
    scope: ScopeID,
    lookupIndexes: Boolean = true): PagedQuery[Iterable[Collection]] =
    CollectionID.getAllUserDefined(scope) collectMT { id =>
      getUncached(scope, id, lookupIndexes).map(_.flatMap(_.active))
    }

  def getAllStaged(
    scope: ScopeID,
    lookupIndexes: Boolean = true): PagedQuery[Iterable[Collection]] =
    CollectionID.getAllUserDefined(scope) collectMT { id =>
      getUncached(scope, id, lookupIndexes).map(_.flatMap(_.staged))
    }

  def idByName(ctx: FQLInterpCtx, name: String): Query[Option[CollectionID]] =
    ctx.env.idByName[CollectionID](ctx.scopeID, name)

  def idByNameActive(scope: ScopeID, name: String): Query[Option[CollectionID]] =
    Cache.collIDByName(scope, name).map(_.flatMap(_.active))

  def idByNameStaged(scope: ScopeID, name: String): Query[Option[CollectionID]] =
    Cache.collIDByName(scope, name).map(_.flatMap(_.staged))

  def idByAlias(ctx: FQLInterpCtx, alias: String): Query[Option[CollectionID]] =
    ctx.env.idByAlias[CollectionID](ctx.scopeID, alias)

  def idByAliasActive(scope: ScopeID, alias: String): Query[Option[CollectionID]] =
    Cache.collIDByAlias(scope, alias).map(_.flatMap(_.active))

  def idByIdentifier(ctx: FQLInterpCtx, name: String): Query[Option[CollectionID]] =
    idByName(ctx, name).orElseT(idByAlias(ctx, name))

  def idByIdentifierActive(
    scope: ScopeID,
    name: String): Query[Option[CollectionID]] =
    idByNameActive(scope, name).orElseT(idByAliasActive(scope, name))

  def idByNameUncachedStaged(
    scope: ScopeID,
    name: String): Query[Option[CollectionID]] = {
    SchemaNames.idByNameStagedUncached[CollectionID](scope, name)
  }

  def idByAliasUncachedStaged(
    scope: ScopeID,
    alias: String): Query[Option[CollectionID]] = {
    SchemaNames.idByAliasStagedUncached[CollectionID](scope, alias)
  }

  def idByIdentifierUncachedStaged(
    scope: ScopeID,
    name: String): Query[Option[CollectionID]] =
    idByNameUncachedStaged(scope, name).orElseT(idByAliasUncachedStaged(scope, name))

  def exists(scope: ScopeID, id: CollectionID): Query[Boolean] =
    get(scope, id).map(_.isDefined)

  // Retention policy derivation

  /** Derive the transaction minimum valid time according to the given collection's
    * retention policy and the `MVTOffset`. See `deriveMinValidTimeNoOffset` and
    * `deriveMinValidTime` for details.
    */
  def deriveMinValidTime(scope: ScopeID, collID: CollectionID): Query[Timestamp] =
    deriveMinValidTimeNoOffset(scope, collID) map { mvt =>
      (mvt - MVTOffset) max Timestamp.Epoch
    }

  /** Derive the transaction minimum valid time according to the given collection's
    * retention policy. Versions stored by the collection with valid time older than
    * the minimum valid time are eligible for GC and may be discarded by read/write
    * paths. To prevent the re-exposure of dead versions upon changes to the
    * collection's retention policy, this method accounts for the persisted min valid
    * time foor. See `computeMinValidTimeFloor` for details.
    *
    * NOTE: do NOT use this method in the READ PATH. The read path relies on the
    * `MVTOffset` to preserve a minimum amount of history so that features like
    * consistent set cursors are work when history days is set to zero. Prefer to USE
    * `deriveMinValidTime` as much as possible.
    */
  def deriveMinValidTimeNoOffset(
    scope: ScopeID,
    collID: CollectionID
  ): Query[Timestamp] =
    for {
      snapshotTS <- Query.snapshotTime
      pinnedTS   <- Database.pinnedMVTForScope(scope)
      cfgOpt     <- CollectionConfig(scope, collID)
    } yield {
      // GC is disabled if the collection is not found.
      cfgOpt.fold(Timestamp.Epoch) { cfg =>
        val minTxnValidTime = snapshotTS - cfg.historyDuration
        val prepinMVT = minTxnValidTime.max(cfg.minValidTimeFloor)
        pinnedTS.fold(prepinMVT)(_.min(prepinMVT)).max(Timestamp.Epoch)
      }
    }

  /** Derive the given collection's minimum valid time floor. The minimum valid
    * time of a collection is a high water mark from which older versions are
    * eligible for garbage collection. To prevent changes to retention policies
    * exposing dead versions to later transactions, this function computes the
    * maximum (most recent) high water mark.
    *
    * TODO: Backfill MVT to existing collections.
    */
  def computeMinValidTimeFloor(scope: ScopeID, id: CollectionID): Query[Timestamp] =
    Query.snapshotTime flatMap { snapshotTS =>
      SchemaCollection
        .Collection(scope)
        .get(id)
        .mapT { curr =>
          // NB: Two things.
          // 1. For resolved timestamps, valid and transaction times are equal
          //    because collection history is immutable.
          // 2. Unresolved timestamps should be resolved at snapshot time to prevent
          //    transactions from considering their own writes as expired (txn ts is
          //    set to Timestamp.MaxMicros by coordinators).
          val changeTS = curr.ts.resolve(snapshotTS).validTS
          val mvtAfter =
            changeTS - curr
              .data(RetainDaysField)
              .fold(Duration.Inf: Duration)(_.saturatedDays)

          val floor =
            curr.prevVersion.fold(mvtAfter) { prev =>
              val mvtBefore =
                changeTS - prev
                  .data(RetainDaysField)
                  .fold(Duration.Inf: Duration)(_.saturatedDays)
              mvtAfter.max(mvtBefore)
            }

          // NB. There are some old collections in legacy data where `history_days`
          // is not present. The lack of a retention policy in means that history is
          // kept forever, therefore, fix the floor at MVT to match the behavior.
          floor.max(Timestamp.Epoch)
        }
        .getOrElseT(Timestamp.Epoch)
    }

  private def parseComputedFieldSigs(live: Version): SeqMap[String, TypeExpr] =
    live.data(InternalDocSignatureField) match {
      case None => SeqMap.empty
      case Some(sig) =>
        (Parser.typeSchemeExpr(sig): Result[TypeExpr.Scheme]) match {
          case Result.Ok(TypeExpr.Scheme(_, TypeExpr.Object(fields, _, _))) =>
            fields.map { case (n, te) => n.str -> te }.to(SeqMap)
          case Result.Ok(sch) =>
            squelchAndLogException {
              throw new IllegalStateException(
                s"unexpected non-type on internal signature on ${live.parentScopeID}, ${live.id}, $sch")
            }
            SeqMap.empty
          case Result.Err(e) =>
            squelchAndLogException {
              throw new IllegalStateException(
                s"Internal doc signature is invalid on ${live.parentScopeID}, ${live.id}, $e")
            }
            SeqMap.empty
        }
    }
}
