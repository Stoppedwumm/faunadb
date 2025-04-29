package fauna.model.runtime.fql2.stdlib

import fauna.atoms._
import fauna.lang.syntax._
import fauna.model.{ Cache, Collection, SchemaNames }
import fauna.model.runtime.fql2._
import fauna.model.runtime.fql2.ToString._
import fauna.model.runtime.Effect
import fauna.model.schema.{ NativeIndex, PublicCollection, SchemaCollection }
import fauna.model.schema.CollectionConfig
import fauna.repo.query.Query
import fauna.repo.schema.DataMode
import fauna.repo.values.Value
import fauna.repo.PagedQuery
import fauna.storage.index.IndexTerm
import fql.ast.{ Name, Span }
import fql.error.Hint
import fql.typer.{ Type, TypeScheme, TypeShape }
import scala.collection.immutable.ArraySeq

object CollectionCompanion {

  private lazy val log = getLogger()

  /** Construct a collection SO. Rather than calling this directly, the SO should
    * be retrieved from the CollectionConfig so that the same instance is always
    * used.
    */
  def createUncached(coll: CollectionConfig): CollectionCompanion =
    coll.id match {
      case DatabaseID.collID       => DatabaseDefCompanion
      case CollectionID.collID     => CollectionDefCompanion
      case UserFunctionID.collID   => FunctionDefCompanion
      case RoleID.collID           => RoleCompanion
      case AccessProviderID.collID => AccessProviderCompanion
      case KeyID.collID            => KeyCompanion
      case CredentialsID.collID    => CredentialCompanion
      case TokenID.collID          => TokenCompanion
      case UserCollectionID(_)     => UserCollectionCompanion.createUncached(coll)
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot create a CollectionCompanion for ${coll.id}")
    }
}

/** A companion object for a Fauna collection
  * We accept an optional nameOverride parameter here because there are times when we want
  * the companion object for a collection to show up under its alias name instead of the collection name.
  * e.g. Customer has a collection "Collection", we want that to show up under its alias
  */
abstract class CollectionCompanion(
  val collID: CollectionID,
  val prototype: AbstractDocPrototype,
  val nullPrototype: AbstractNullDocPrototype = NullDocPrototype)
    extends CompanionObject(prototype.collName, None) {
  import CollectionCompanion._

  override val selfType = TypeTag.Named(s"${collName}Collection")

  override val typeHint: Option[TypeShape.TypeHint] = Some(
    TypeShape.TypeHint.ServerCollection)

  lazy val typeShapes = Map(
    selfType.name -> typeShape,
    docType.name -> docTypeShape,
    nullDocType.name -> nullDocTypeShape)

  lazy val collName: String = prototype.collName
  lazy val docType: TypeTag[Value.Doc] = prototype.docType
  lazy val nullDocType: TypeTag[Value.Doc] = prototype.nullDocType
  lazy val docRefType: TypeTag[Value.Doc] = tt.Ref(docType)
  lazy val docSetType: TypeTag[Value.Set] = tt.Set(docType)

  protected lazy val docImplType: TypeScheme = Type.AnyRecord.typescheme
  protected lazy val docCreateType: Type = Type.AnyRecord
  protected lazy val docCreateDataType: Type = Type.AnyRecord

  // Doc and nulldoc are not persistable because they
  // must occur together in order to be persistable,
  // until we can guarantee referential integrity.
  private lazy val docTypeShape = {
    // FIXME: this is a dumb hack, but fixing properly is a real pain
    val collField = ("coll" -> selfType.typescheme)
    TypeShape(
      self = docType.typescheme,
      fields = prototype.typeShape.fields + collField,
      docType = TypeShape.DocType.Doc(collName),
      isPersistable = false,
      alias = Some(docImplType)
    )
  }
  private lazy val nullDocTypeShape = TypeShape(
    self = nullDocType.typescheme,
    fields = nullPrototype.typeShapeColl(selfType.typescheme).fields,
    docType = TypeShape.DocType.NullDoc(collName),
    isPersistable = false,
    alias = Some(Type.Null.typescheme)
  )

  private lazy val docPredType = tt.Function(ArraySeq(docType), tt.Boolean)

  protected def allDocuments(
    ctx: FQLInterpCtx,
    action: Effect.Action,
    args: Vector[Value] = Vector.empty,
    range: IndexSet.Range = IndexSet.Range.Unbounded
  ): Query[Result[ValueSet]] = {
    checkReadPermission(ctx, action) flatMapT { _ =>
      val cfg = NativeIndex.DocumentsByCollection(ctx.scopeID)
      val terms = Vector(IndexTerm(collID.toDocID))
      IndexSet(
        this,
        "all",
        args,
        cfg,
        terms,
        ctx.userValidTime,
        ctx.stackTrace.currentStackFrame,
        range).toQuery
    }
  }

  protected def checkReadPermission(
    ctx: FQLInterpCtx,
    action: Effect.Action): Query[Result[Unit]] =
    action.check(ctx, Effect.Read) flatMapT { _ =>
      ctx.auth.checkReadCollectionPermission(ctx.scopeID, collID) map { allowed =>
        // FIXME: turn this into a `PermissionDenied` error after validating that no
        // customers in production are hitting the warning.
        if (!allowed) {
          log.warn(
            s"Account ${ctx.auth.accountID} reads collection $collName ($collID) " +
              s"in ${ctx.scopeID} without read permissions.")
        }
        Result.Ok(())
      }
    }

  def contains(v: Value): Boolean =
    v match {
      case doc: Value.Doc => doc.id.collID == collID
      case _              => false
    }

  defStaticFunction("all" -> docSetType)() { (ctx) =>
    allDocuments(ctx, Effect.Action.Function("all"))
  }

  defStaticFunction("all" -> docSetType)("range" -> SetCompanion.rangeType) {
    (ctx, rangeArg) =>
      val rng = CollectionIndexMethod.collectRange(
        "range",
        collID,
        Vector.empty,
        Some(rangeArg),
        ctx.stackTrace)

      Query.value(rng).flatMapT {
        allDocuments(ctx, Effect.Action.Function("all"), Vector(rangeArg), _)
      }
  }

  defStaticFunction("where" -> docSetType)("pred" -> docPredType) { (ctx, pred) =>
    maybeEmitHint(ctx, "where", ctx.stackTrace.currentStackFrame).flatMap { _ =>
      allDocuments(ctx, Effect.Action.Function("where"))
        .flatMapT(SetPrototype.whereImpl(ctx, _, pred))
    }
  }

  defStaticFunction("firstWhere" -> tt.Optional(docType))("pred" -> docPredType) {
    (ctx, pred) =>
      maybeEmitHint(ctx, "firstWhere", ctx.stackTrace.currentStackFrame).flatMap {
        _ =>
          allDocuments(ctx, Effect.Action.Function("firstWhere"))
            .flatMapT(SetPrototype.firstWhereImpl(ctx, _, pred))
      }
  }

  defStaticFunction("create" -> docType)("data" -> tt.Struct(docCreateType)) {
    (ctx, fields) =>
      WriteBroker.createDocument(ctx, collID, DataMode.Default, fields)
  }

  defMethod("toString" -> tt.Str)() { (ctx, self) =>
    self.toDisplayString(ctx).map { str =>
      Value.Str(str).toResult
    }
  }

  private def maybeEmitHint(
    ctx: FQLInterpCtx,
    methodName: String,
    span: Span): Query[Unit] =
    if (ctx.performanceDiagnosticsEnabled) {
      ctx.emitDiagnostic(
        Hints.CollectionScan(
          collName,
          methodName,
          span
        )
      )
    } else {
      Query.unit
    }
}

/** A companion object for a native Schema (named) collections. */
abstract class SchemaCollectionCompanion(
  protected val coll: SchemaCollection[_],
  prototype: AbstractDocPrototype,
  nullPrototype: AbstractNullDocPrototype)
    extends CollectionCompanion(coll.id, prototype, nullPrototype) {

  override lazy val docRefType: TypeTag[Value.Doc] = tt.NamedRef(docType)

  def idByNameStaged(scope: ScopeID, name: String) =
    coll.docIDByNameStaged(scope, name)

  def nameByID(scope: ScopeID, id: DocID) =
    // This is coll.nameByID except generics are difficult.
    SchemaNames.lookupCachedName(scope, id)

  defStaticFunction("byName" -> docRefType)("name" -> tt.Str) { (ctx, name) =>
    Effect.Action.Function("byName").check(ctx, Effect.Read).flatMapT { _ =>
      idByNameStaged(ctx.scopeID, name.value) map {
        case Some(docID) =>
          Value.Doc(docID, Some(name.value), ctx.userValidTime).toResult
        case None =>
          Value.Doc.createNamed(collID, name.value, ctx.userValidTime).toResult
      }
    }
  }
}

/** An object that contains the static fauna provided resolvers for
  * user collections.  This intermediary is used to help us validate that
  * user index names don't conflict with any of our provided resolvers.
  */
abstract class IDedCollectionCompanion(
  collID: CollectionID,
  prototype: AbstractDocPrototype)
    extends CollectionCompanion(collID, prototype) {

  override lazy val docImplType = prototype.docImplType.typescheme
  override lazy val docCreateType = prototype.docCreateType
  override lazy val docCreateDataType = prototype.docCreateDataType
  lazy val refDocType = tt.Ref(docType)

  def this(coll: PublicCollection[_], prototype: AbstractDocPrototype) =
    this(coll.id, prototype)

  defStaticFunction("byId" -> docRefType)("id" -> tt.ID) { (ctx, id) =>
    Effect.Action.Function("byId").check(ctx, Effect.Read).flatMapT { _ =>
      Value.Doc(DocID(SubID(id.value), collID), None, ctx.userValidTime).toQuery
    }
  }

  defApply(refDocType)("id" -> tt.ID) { (ctx, id) =>
    Value.Doc(DocID(SubID(id.value), collID), None, ctx.userValidTime).toQuery
  }
}

object UserCollectionCompanion {
  def createUncached(coll: CollectionConfig, nameOverride: Option[String] = None) =
    coll.id match {
      case UserCollectionID(_) => new UserCollectionCompanion(coll, nameOverride)
      case _ =>
        throw new UnsupportedOperationException(
          s"Providing a name is only supported for UserCollectionCompanions, unsupported collection id ${coll.id}")
    }

  def lookup(ctx: FQLInterpCtx, name: String): Query[Option[CollectionCompanion]] =
    Collection
      .idByIdentifier(ctx, name)
      .flatMapT { id =>
        CollectionConfig(ctx.scopeID, id).mapT { config =>
          if (name != config.name) {
            new UserCollectionCompanion(config, Some(name))
          } else {
            config.companionObject
          }
        }
      }

  def getAll(scope: ScopeID): PagedQuery[Iterable[UserCollectionCompanion]] =
    collsToCompanions(Collection.getAll(scope))

  def getAllUncached(
    scope: ScopeID,
    lookupIndexes: Boolean = true): PagedQuery[Iterable[UserCollectionCompanion]] =
    collsToCompanions(Collection.getAllUncached(scope, lookupIndexes))

  def getAllStaged(
    scope: ScopeID,
    lookupIndexes: Boolean = true): PagedQuery[Iterable[UserCollectionCompanion]] =
    collsToCompanions(Collection.getAllStaged(scope, lookupIndexes))

  private def collsToCompanions(colls: PagedQuery[Iterable[Collection]])
    : PagedQuery[Iterable[UserCollectionCompanion]] = {
    colls.flatMapValuesT { coll =>
      Query.value(
        Seq(new UserCollectionCompanion(coll.config, Some(coll.name))) ++ coll.alias
          .map { alias =>
            new UserCollectionCompanion(coll.config, Some(alias))
          })
    }
  }

  trait StaticResolvers { self: IDedCollectionCompanion =>
    defField("definition" -> CollectionDefCompanion.docType) { (_, _) =>
      Query.value(Value.Doc(collID.toDocID))
    }

    defStaticFunction("createData" -> docType)(
      "data" -> tt.Struct(docCreateDataType)) { (ctx, fields) =>
      WriteBroker.createDocument(ctx, collID, DataMode.PlainData, fields)
    }
  }
}

/** A companion object for a user-defined collection.
  *
  * Note on cache behavior: since indexes are defined within the user collections,
  * attempting to read an index immediately after its creation can hit a stale cache
  * entry on the collection side where the index is not present yet. These and
  * similar lookups must be guarded against cache staleness. See `hasField` and
  * `get`.
  */
class UserCollectionCompanion private (
  val coll: CollectionConfig,
  nameOverride: Option[String])
    extends IDedCollectionCompanion(
      coll.id,
      UserDocPrototype(coll, nameOverride.getOrElse(coll.name)))
    with UserCollectionCompanion.StaticResolvers {
  import FieldTable.R

  override val typeHint: Option[TypeShape.TypeHint] = Some(
    TypeShape.TypeHint.UserCollection)

  override def hasField(ctx: FQLInterpCtx, self: Value, name: Name): Query[Boolean] =
    Cache.guardFromStalenessIf(coll.parentScopeID, super.hasField(ctx, self, name)) {
      hasField => !hasField // refresh cache if field is not found.
    }

  override def getField(
    ctx: FQLInterpCtx,
    self: Value,
    name: Name): Query[R[Value]] =
    Cache.guardFromStalenessIf(coll.parentScopeID, super.getField(ctx, self, name)) {
      // refresh cache if field is null.
      case R.Val(_: Value.Null) => true
      case _                    => false
    }

  override def getMethod(
    ctx: FQLInterpCtx,
    self: Value,
    name: Name) = // : Query[R[Option[NativeMethod[Value]]]] =
    Cache.guardFromStalenessIf(
      coll.parentScopeID,
      super.getMethod(ctx, self, name)) {
      // refresh cache if the method does not exist.
      case R.Val(None) => true
      case _           => false
    }

  coll.userIndexes.foreach { index =>
    import NativeFunction._

    // Convert static index types into tagged range/wo range variants. Assumes
    // CollectionTypeInfo produces the right shape.
    val (withoutRangeParams, withRangeParams) = {
      val Type.Intersect(sigs, _) = coll.typeInfo.indexMethodTypes(index.name)
      val params = sigs.map { s =>
        val Type.Function(ps, None, _, _) = s
        ps.collect { case (Some(n), ty) =>
          NameWithType(n.str, tt.Erased(ty))
        }.toIndexedSeq
      }
      val Seq(wor, wr) = params
      assert(wr.last.n == "range")
      (wor, wr)
    }

    defStaticFunction(index.name -> docSetType).params(withoutRangeParams) {
      (ctx, params) =>
        ctx.auth.checkReadCollectionPermission(coll.parentScopeID, coll.id) flatMap {
          case false =>
            QueryRuntimeFailure
              .PermissionDenied(
                ctx.stackTrace,
                s"Insufficient privileges to read from collection ${coll.name}.")
              .toQuery
          case true =>
            CollectionIndexMethod(this).impl(
              ctx,
              index.name,
              index,
              params,
              terms = params,
              range = None
            )
        }
    }

    defStaticFunction(index.name -> docSetType).params(withRangeParams) {
      (ctx, params) =>
        ctx.auth.checkReadCollectionPermission(coll.parentScopeID, coll.id) flatMap {
          case false =>
            QueryRuntimeFailure
              .PermissionDenied(
                ctx.stackTrace,
                s"Insufficient privileges to read from collection ${coll.name}.")
              .toQuery
          case true =>
            CollectionIndexMethod(this).impl(
              ctx,
              index.name,
              index,
              params,
              params.init,
              params.lastOption
            )
        }
    }
  }
}

// FIXME: these variants can possibly go away once we derive the impl type of
// a collection from the collconfig
object CollectionDefCompanion
    extends SchemaCollectionCompanion(
      SchemaCollection.Collection,
      NativeDocPrototype(
        SchemaCollection.Collection.name,
        docType = TypeTag.NamedDoc(s"${SchemaCollection.Collection.name}Def"),
        nullDocType = TypeTag.NullDoc(s"${SchemaCollection.Collection.name}Def")
      ),
      NamedNullDocPrototype
    ) {

  override val selfType = TypeTag.Named(s"${coll.name}Collection")

  lazy val computeType = tt.Struct(
    "body" -> tt.Str,
    "signature" -> tt.Optional(tt.Str)
  )
  lazy val definedType = tt.Struct(
    "signature" -> tt.Str,
    "default" -> tt.Optional(tt.Str)
  )

  lazy val termType = tt.Struct(
    "field" -> tt.Str,
    "mva" -> tt.Optional(tt.Boolean)
  )
  lazy val valueType = tt.Struct(
    "field" -> tt.Str,
    "mva" -> tt.Optional(tt.Boolean),
    "order" -> tt.Optional(tt.Union(tt.StrLit("asc"), tt.StrLit("desc")))
  )

  lazy val statusType =
    tt.Union(tt.StrLit("pending"), tt.StrLit("active"), tt.StrLit("failed"))

  lazy val indexType = tt.Struct(
    "terms" -> tt.Array(termType),
    "values" -> tt.Array(valueType),
    "queryable" -> tt.Boolean,
    "status" -> statusType
  )
  lazy val indexCreateType = tt.Struct(
    "terms" -> tt.Optional(tt.Array(termType)),
    "values" -> tt.Optional(tt.Array(valueType)),
    "queryable" -> tt.Optional(tt.Boolean)
  )

  lazy val uniqueConstraintType = tt.Struct(
    "unique" -> tt.Union(
      tt.Array(tt.Str),
      tt.Array(tt.Struct("field" -> tt.Str, "mva" -> tt.Optional(tt.Boolean)))),
    "status" -> statusType
  )
  lazy val uniqueConstraintCreateType = tt.Struct(
    "unique" -> tt.Union(
      tt.Array(tt.Str),
      tt.Array(tt.Struct("field" -> tt.Str, "mva" -> tt.Optional(tt.Boolean))))
  )

  lazy val checkConstraintType =
    tt.Struct("check" -> tt.Struct("name" -> tt.Str, "body" -> tt.Str))

  lazy val migrationType = Type.Union(
    Type.Record("backfill" -> Type.Record("field" -> Type.Str, "value" -> Type.Str)),
    Type.Record("drop" -> Type.Record("field" -> Type.Str)),
    Type.Record(
      "split" -> Type.Record("field" -> Type.Str, "to" -> Type.Array(Type.Str))),
    Type.Record("move" -> Type.Record("field" -> Type.Str, "to" -> Type.Str)),
    Type.Record("add" -> Type.Record("field" -> Type.Str)),
    Type.Record("move_conflicts" -> Type.Record("into" -> Type.Str)),
    Type.Record("move_wildcard" -> Type.Record("into" -> Type.Str)),
    Type.Record("add_wildcard" -> Type.Record())
  )

  override lazy val docImplType = Type
    .Record(
      "name" -> Type.Str,
      "alias" -> Type.Union(Type.Str, Type.Null),
      "computed_fields" -> Type.WildRecord(computeType),
      "fields" -> Type.WildRecord(definedType),
      "migrations" -> Type.Optional(Type.Array(migrationType)),
      "wildcard" -> Type.Str,
      "ts" -> Type.Time,
      "coll" -> selfType,
      "history_days" -> Type.Union(Type.Number, Type.Null),
      "ttl_days" -> Type.Union(Type.Number, Type.Null),
      "document_ttls" -> Type.Optional(Type.Boolean),
      "indexes" -> Type.WildRecord(indexType),
      // TODO: Add handling for unions.
      "constraints" -> Type.Array(Type.AnyRecord),
      "data" -> Type.Union(Type.AnyRecord, Type.Null)
    )
    .typescheme
  override lazy val docCreateType = Type
    .Record(
      "name" -> Type.Str,
      "alias" -> Type.Optional(Type.Str),
      "computed_fields" -> Type.Optional(Type.WildRecord(computeType)),
      "fields" -> Type.Optional(Type.WildRecord(definedType)),
      "migrations" -> Type.Optional(Type.Array(migrationType)),
      "wildcard" -> Type.Optional(Type.Str),
      "history_days" -> Type.Optional(Type.Number),
      "ttl_days" -> Type.Optional(Type.Number),
      "document_ttls" -> Type.Optional(Type.Boolean),
      "indexes" -> Type.Optional(Type.WildRecord(indexCreateType)),
      "constraints" -> Type.Optional(
        Type.Array(Type.Union(uniqueConstraintCreateType, checkConstraintType))),
      "data" -> Type.Optional(Type.AnyRecord)
    )

  object Deleted extends CompanionObject("Deleted", Some(this)) {
    override val selfType = TypeTag.Named("DeletedCollection")
    def contains(v: Value) = false
  }

  defField("Deleted" -> Deleted.selfType)((_, _) => Query.value(Deleted))

  defApply(tt.Any)("collection" -> tt.Str) { (ctx, collection) =>
    UserCollectionCompanion.lookup(ctx, collection.value).flatMap {
      case Some(col) => col.toQuery
      case None      =>
        // Check if they attempted to create that collection in this transaction.
        val hintsQ =
          Collection.idByNameUncachedStaged(ctx.scopeID, collection.value).map {
            case Some(_) =>
              Seq(
                Hint(
                  "A collection cannot be created and used in the same query.",
                  ctx.stackTrace.currentStackFrame))
            case None =>
              Seq.empty
          }

        hintsQ.map { hints =>
          Result.Err(
            QueryRuntimeFailure.InvalidArgument(
              "collection",
              s"No such user collection `${collection.value}`.",
              ctx.stackTrace,
              hints))
        }
    }
  }
}
