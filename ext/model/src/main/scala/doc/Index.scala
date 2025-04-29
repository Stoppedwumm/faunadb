package fauna.model

import fauna.ast._
import fauna.atoms._
import fauna.flags.{ MaxConcurrentBuilds, RunTasks }
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.account.Account
import fauna.model.runtime.fql2.{
  FQLInterpCtx,
  FQLInterpreter,
  FieldTable,
  ReadBroker,
  Result
}
import fauna.model.runtime.Effect
import fauna.model.schema.{
  CollectionConfig,
  ComputedField,
  ComputedFieldData,
  FieldPath,
  InternalCollectionID,
  NativeCollection,
  NativeCollectionID,
  NativeIndex,
  SchemaCollection
}
import fauna.model.tasks.IndexBuild
import fauna.repo._
import fauna.repo.doc._
import fauna.repo.query.Query
import fauna.repo.schema.migration.MigrationList
import fauna.repo.values.Value
import fauna.storage.doc._
import fauna.storage.index._
import fauna.storage.ir._
import fauna.util.ReferencesValidator
import fql.ast.{ Name, Span }
import scala.collection.immutable.Queue
import scala.collection.mutable.{ Map => MMap }
import scala.util.control.NoStackTrace

/** Thrown when the number of concurrent index builds has exceeded
  * `Index.MaxConcurrentBuilds`.
  */
final class IndexBuildLimitExceeded(val pending: Int, val limit: Long)
    extends NoStackTrace

object IndexBuildsDisabled extends NoStackTrace

/** This is the configuration object exposed to users in the API for
  * both index terms *and* values, though reversing an index term is
  * pointless.
  */
sealed abstract class TermPath

case class TermDataPath(path: List[Either[Long, String]]) extends TermPath
case class TermBindingPath(name: String) extends TermPath

/** Associates a set of source collections for an index with a list of lambda
  * function bindings used to collect the fields that belong in the index.
  *
  * Indexes may contain many bindings each for any subset of the source collections.
  *
  * @param sources - A set of underlying source collections that use the same bindings
  * @param fields - A list of lambda function bindings that fetch fields
  * @param computed - A list of computed fields that are bound as terms or values.
  */
case class SourceConfig(
  sources: IndexSources,
  fields: List[(String, QueryV)] = List.empty,
  computed: List[(String, ComputedFieldData)] = List.empty)

case class TermConfig(
  path: TermPath,
  reverse: Boolean,
  mva: Boolean,
  transform: Option[String]) {

  private type Path = List[Either[Long, String]]

  def extractor(
    ec: EvalContext,
    intp: FQLInterpCtx,
    isCollIndex: Boolean): TermExtractor =
    path match {
      case TermBindingPath(name)             => bindingExtractor(name)
      case TermDataPath(path) if isCollIndex => readBrokerDataExtractor(intp, path)
      case TermDataPath(path)                => legacyDataExtractor(ec, path)
    }

  private def mvaFlatten(v: IRValue) = {
    def flatten(v: IRValue): Vector[ScalarV] = v match {
      case v: ScalarV    => Vector(v)
      case ArrayV(elems) => elems.flatMap(flatten)
      case _             => Vector.empty
    }
    flatten(v)
  }

  private def bindingExtractor(name: String): TermExtractor = { (_, bindings) =>
    val terms = bindings.iterator flatMap {
      case (n, elems) if n == name =>
        val es = if (mva) {
          elems flatMap mvaFlatten
        } else {
          elems
        }
        es map { IndexTerm(_, reverse) }
      case _ => List.empty
    }
    Query(terms.toVector)
  }

  private def readBrokerDataExtractor(
    intp: FQLInterpCtx,
    path: Path): TermExtractor = { (vers, _) =>
    path match {
      // Special-case ID handling.
      case Right("id") :: path =>
        if (path.isEmpty) {
          Query.value(Vector(IndexTerm(DocIDV(vers.id), reverse)))
        } else {
          Query.value(Vector.empty)
        }

      case Right(start) :: path =>
        Query.incrCompute() flatMap { _ =>
          // Charge one compute op per version processed per extractor, as in FQL4.
          ReadBroker.extractField(intp, vers, Name(start, Span.Null)) flatMap {
            case FieldTable.R.Val(seed) =>
              ReadBroker
                .extractValue(intp, computed = false, seed, path)
                .map { vOpt =>
                  val irOpt = vOpt.flatMap(Value.toIR(_).toOption)
                  val irVec = if (mva) {
                    def flatten(v: IRValue): Vector[ScalarV] = v match {
                      case v: ScalarV    => Vector(v)
                      case ArrayV(elems) => elems.flatMap(mvaFlatten)
                      case _             => Vector.empty
                    }
                    irOpt.fold(Vector.empty[IRValue])(flatten)
                  } else {
                    irOpt.toVector
                  }

                  if (irVec.isEmpty) {
                    Vector(IndexTerm(NullV, reverse))
                  } else {
                    irVec.map(IndexTerm(_, reverse))
                  }
                }
            case _ => Query.value(Vector.empty)
          }
        }
      case _ => Query.value(Vector.empty)
    }
  }

  private def legacyDataExtractor(ec: EvalContext, path: Path): TermExtractor = {
    (version, _) =>
      val pathL = ArrayL(path.map { _.fold(LongL(_), StringL(_)) })
      val input = if (mva) {
        ObjectL("select_as_index" -> pathL, "from" -> VersionL(version))
      } else {
        ObjectL("select" -> pathL, "from" -> VersionL(version), "default" -> NullL)
      }

      val transformed = transform match {
        case None => input
        case Some(t) =>
          val v = ObjectL("var" -> StringL("x"))
          val lambda = ObjectL("lambda" -> StringL("x"), "expr" -> ObjectL(t -> v))
          ObjectL("map" -> lambda, "collection" -> input)
      }

      ec.parseAndEvalTopLevel(transformed) map {
        case Right(res) =>
          if (mva) {
            def flatten(v: Literal): List[ScalarL] = v match {
              case v: ScalarL    => List(v)
              case ArrayL(elems) => elems.flatMap(flatten)
              case _             => List(NullL)
            }
            flatten(res).iterator.map(Literal.toIndexTerm(_, reverse)).toVector
          } else {
            Vector(Literal.toIndexTerm(res, reverse))
          }
        case Left(_) => Vector.empty
      }
  }
}

/** Decoded representation of an index.
  *
  * !!! ATTENTION: !!!
  *
  * Currently, structural equality is broken for this class as terms and values are
  * implemented as lambda functions, which implement to referential equality instead,
  * thus forcing the same behaviour into the Index class.
  *
  * FIXME: implement proper structural equality.
  */
case class Index(
  id: IndexID,
  scopeID: ScopeID,
  name: String,
  sources: IndexSources,
  binders: CollectionID => Option[List[TermBinder]],
  terms: Vector[(TermExtractor, Boolean)],
  values: Vector[(TermExtractor, Boolean)],
  constraint: Constraint,
  isSerial: Boolean,
  override val partitions: Long,
  isActive: Boolean,
  isHidden: Boolean,
  override val isCollectionIndex: Boolean)
    extends IndexConfig

/** This indexer wraps another indexer, and applies an additional set of
  *  migrations to all the versions.
  *
  * It is only used for the live write path. Inline index builds will fetch a
  * `CollectionConfig` that has all staged migrations included.
  *
  * Note that this does not apply staged computed fields! Staged computed fields
  * are stored on the index document itself, so those are handled in the
  * `IndexConfig` directly.
  */
case class StagedIndexer(
  original: IndexConfig,
  stagedMigrations: MigrationList,
  activeSchemaVersion: SchemaVersion
) extends IndexConfig {
  def id = original.id
  def scopeID = original.scopeID
  def sources = original.sources
  def binders = original.binders
  def constraint = original.constraint
  def isSerial = original.isSerial
  override def partitions = original.partitions
  override def isCollectionIndex = original.isCollectionIndex

  def terms: Vector[(TermExtractor, Boolean)] = original.terms.map { case (t, r) =>
    extractor(t) -> r
  }
  def values: Vector[(TermExtractor, Boolean)] = original.values.map { case (t, r) =>
    extractor(t) -> r
  }

  private def extractor(t: TermExtractor): TermExtractor = { (vers, bindings) =>
    vers match {
      case live: Version.Live =>
        // NB: `live` will already be migrated to the active schema version by
        // `Store.get`, so migrate it up to the staged schema version.
        //
        // For example:
        //
        //     disk     -> Store.get ->  live  -> stagedMigrations.migrate ->  staged
        //       ^                         ^                                     ^
        // live.schemaVersion = 5  activeSchemaVersion = 10  stagedSchemaVersion = 15

        val stagedData = stagedMigrations.migrate(live.data, activeSchemaVersion)
        // This is the same as `Version.fromStorage`.
        val stagedDiff = live.diff.map { diff =>
          val prevData = live.data patch diff
          val prevStagedData =
            stagedMigrations.migrate(prevData, activeSchemaVersion)
          stagedData diffTo prevStagedData
        }

        t(live.withData(stagedData).withDiff(stagedDiff), bindings)

      case deleted: Version.Deleted =>
        // This is the same as `Version.fromStorage`.
        val stagedDiff = deleted.diff.map { diff =>
          val prevData = Data.empty patch diff
          val prevStagedData =
            stagedMigrations.migrate(prevData, activeSchemaVersion)
          Data.empty diffTo prevStagedData
        }

        t(deleted.withDiff(stagedDiff), bindings)
    }
  }
}

object Index {
  final type Binders = CollectionID => Option[List[TermBinder]]
  final type Terms = Vector[(TermExtractor, Boolean)]

  val log = getLogger

  val BuildSyncSize = 128

  def apply(
    vers: Version.Live,
    cfgs: Vector[SourceConfig],
    forSyncBuild: Boolean): Index = {

    val scope = vers.parentScopeID
    val ec = EvalContext.pure(scope, APIVersion.Default, "index bindings")

    // NB: Once computed fields are able to call other computed fields and pure UDFs,
    // this will stop working! Staged indexes need to fetch the staged env, not the
    // default env here.
    val env = if (forSyncBuild) RuntimeEnv.InlineIndexEnv else RuntimeEnv.Default

    val intp =
      new FQLInterpreter(ec.auth, ec.effectLimit, env, allowFieldComputation = false)

    val sources = parseSource(cfgs)
    val binders = parseBinders(ec, intp, cfgs)

    val isActive = vers.data(ActiveField)
    val isHidden = vers.data(HiddenField) getOrElse false
    val isCollectionIndex = vers.data(CollectionIndexField) getOrElse false
    // v10 indexes are always serialized, v4 indexes are not
    val isSerial = if (isCollectionIndex) {
      true
    } else {
      vers.data(SerialField) getOrElse false
    }

    val terms = vers.data(IndexByField) map { t =>
      (t.extractor(ec, intp, isCollectionIndex), t.reverse)
    }
    val values = vers.data(CoveredField) map { t =>
      (t.extractor(ec, intp, isCollectionIndex), t.reverse)
    }

    val constraint = if (vers.data(UniqueField).getOrElse(false)) {
      UniqueValues
    } else {
      Unconstrained
    }

    val partitions = partitionsFromData(vers.data)

    Index(
      vers.id.as[IndexID],
      scope,
      SchemaNames.findName(vers),
      sources,
      binders,
      terms,
      values,
      constraint,
      isSerial,
      partitions,
      isActive,
      isHidden,
      isCollectionIndex
    )
  }

  // The forcing unique indexes to a single partition has been live for a long time
  // and makes it such that we can not toggle the unique field of a partitioned
  // index.
  // When we toggle the unique field for an index document that has a
  // partitions value set, then we also toggle the index from being a single
  // partition to a multi partition index, which changes the index queries
  // that are sent and misses all results that had previously been added
  // to the index.
  def partitionsFromData(data: Data) =
    if (data(UniqueField).getOrElse(false)) {
      1L
    } else {
      data(PartitionsField) getOrElse 1L
    }

  implicit val PathElemType =
    FieldType[Either[Long, String]]("Long or String") {
      case Left(l)  => LongV(l)
      case Right(s) => StringV(s)
    } {
      case LongV(l)   => Left(l)
      case StringV(s) => Right(s)
    }

  val FieldField = Field.OneOrMore[Either[Long, String]]("field")
  val BindingField = Field[Option[String]]("binding")
  val ReverseField = Field[Option[Boolean]]("reverse")
  val MVAField = Field[Option[Boolean]]("mva")
  val TransformField = Field[Option[String]]("transform")

  private def termPath(path: TermPath) =
    path match {
      case TermBindingPath(name) => "binding" -> StringV(name)
      case TermDataPath(path) =>
        val b = Vector.newBuilder[IRValue]
        b.sizeHint(path.size)
        path foreach {
          case Left(l)  => b += LongV(l)
          case Right(s) => b += StringV(s)
        }

        "field" -> ArrayV(b.result())
    }

  implicit val TermFieldType = FieldType.validating[TermConfig]("Term") { cfg =>
    val b = List.newBuilder[(String, IRValue)]
    b += termPath(cfg.path)
    if (cfg.reverse) b += ("reverse" -> TrueV)
    if (!cfg.mva) b += ("mva" -> FalseV)
    cfg.transform.foreach { t => b += ("transform" -> StringV(t)) }
    MapV(b.result())
  } { case vs: MapV =>
    def getPath: Either[List[ValidationException], TermPath] =
      (BindingField.read(vs), FieldField.read(vs)) match {
        case (Right(Some(bp)), _) => Right(TermBindingPath(bp))
        case (_, Right(ff))       => Right(TermDataPath(ff.toList))

        case (Left(errs @ List(_: InvalidType)), _) => Left(errs)
        case (_, Left(errs @ List(_: InvalidType))) => Left(errs)
        case _ => Left(List(MissingField(FieldField.path, BindingField.path)))
      }

    def checkTransform(transform: Option[String]) =
      transform match {
        case Some("casefold") => Right(transform)
        case Some(v) => Left(List(InvalidTransformValue(TransformField.path, v)))
        case None    => Right(transform)
      }

    for {
      pp  <- getPath
      rev <- ReverseField.read(vs)
      mva <- MVAField.read(vs)
      t   <- TransformField.read(vs)
      tt  <- checkTransform(t)
    } yield {
      TermConfig(
        path = pp,
        reverse = rev.getOrElse(false),
        mva = mva.getOrElse(true),
        transform = tt)
    }
  }

  def getFilterMask(field: Field[Vector[TermConfig]]): MaskTree =
    MaskTree(field.path, BindingField.path) merge
      MaskTree(field.path, FieldField.path) merge
      MaskTree(field.path, ReverseField.path) merge
      MaskTree(field.path, MVAField.path) merge
      MaskTree(field.path, TransformField.path)

  implicit val ClassFieldType = new FieldType[IndexSources] {
    val vtype = IRType.Custom("SourceClass")

    def decode(value: Option[IRValue], path: Queue[String]) = value match {
      case Some(StringV("_"))             => Right(IndexSources.Custom)
      case Some(DocIDV(CollectionID(id))) => Right(IndexSources(id))
      case Some(ArrayV(vs)) =>
        val ids = vs.iterator
          .map({ v => FieldType.CollectionIDT.decode(Some(v), path) })
          .toVector
        ids.sequence.map { ids => IndexSources(ids toSet) }
      case Some(v) => Left(List(InvalidType(path.toList, vtype, v.vtype)))
      case None    => Left(List(ValueRequired(path.toList)))
    }

    def encode(src: IndexSources) = src match {
      case IndexSources.All =>
        throw new IllegalStateException(
          "wildcard index cannot include native collections")
      case IndexSources.Custom => Some(StringV("_"))
      case IndexSources.Limit(ids) =>
        if (ids.size == 1) {
          FieldType.CollectionIDT.encode(ids.head)
        } else {
          (ids map FieldType.CollectionIDT.encode).sequence.map { s =>
            ArrayV(s.toVector)
          }
        }
    }
  }

  val SourceBindingsField = Field[Option[List[(String, QueryV)]]]("fields")
  val SourceComputedField =
    Field[Option[List[(String, ComputedFieldData)]]]("computed")
  val ClassField = Field[IndexSources]("class")
  val CollectionField = Field[IndexSources]("collection")

  implicit val SourceFieldType = new FieldType[SourceConfig] {
    val vtype = IRType.Custom("SourceClass")

    def encode(src: SourceConfig) = src match {
      case SourceConfig(sources, bindings, computed) =>
        val classField = List("class" -> ClassFieldType.encode(sources).get)
        val fields =
          if (bindings.isEmpty) List.empty else List("fields" -> MapV(bindings))
        val comp =
          if (computed.isEmpty)
            List.empty
          else {
            val cfs = computed map { case (name, cf) =>
              name -> ComputedFieldData.ComputedFieldType.encode(cf).get
            }
            List("computed" -> MapV(cfs))
          }
        Some(MapV(classField ++ fields ++ comp))
    }

    def decode(value: Option[IRValue], path: Queue[String]) = {
      val res = value match {
        case Some(StringV("_")) => Right(SourceConfig(IndexSources.Custom))
        case Some(DocIDV(CollectionID(id))) =>
          Right(SourceConfig(IndexSources.Limit(Set(id))))
        case Some(vs: MapV) =>
          for {
            klass    <- classOrCollection(vs)
            bindings <- SourceBindingsField.read(vs)
            computed <- SourceComputedField.read(vs)
          } yield SourceConfig(
            klass,
            bindings getOrElse List.empty,
            computed getOrElse List.empty)
        case Some(v) => Left(List(InvalidType(path.toList, vtype, v.vtype)))
        case None    => Left(List(ValueRequired(path.toList)))
      }

      res match {
        // Disallow sources over the `Database` collection (except the `All` source,
        // but no one really uses that).
        case Right(cfg)
            if cfg.sources != IndexSources.All &&
              cfg.sources.contains(DatabaseID.collID) =>
          Left(List(InvalidReference(path.toList)))

        case v => v
      }
    }

    def classOrCollection(vs: MapV) =
      (ClassField.read(vs), CollectionField.read(vs)) match {
        // FIXME should switch on APIVersion but it's not currently accessible here
        case (classErr @ Left(_), Left(_)) => classErr
        case (klass, col)                  => klass orElse col
      }
  }

  val IndexByField = Field.ZeroOrMore[TermConfig]("terms")
  val CoveredField = Field.ZeroOrMore[TermConfig]("values")
  val SourceField = Field.OneOrMore[SourceConfig]("source")
  val UniqueField = Field[Option[Boolean]]("unique")
  val PartitionsField = Field[Option[Long]]("partitions")

  // Optional for backward compatibility
  val SerialField = Field[Option[Boolean]]("serialized")

  // FIXME: active would be safer as a latch - once true, always
  // true. A user might update it from true -> false, preventing
  // match().
  val ActiveField = Field[Boolean]("active")

  // this identifies new builtin indexes embedded on collections
  val CollectionIndexField = Field[Option[Boolean]]("collection_index")

  /** A marker on an Index to prevent it from appearing in the FQLv4
    * Indexes() collection. Note that FQLv10 doesn't allow users to
    * observe indexes directly, so this flag has no effect there.
    *
    * This is used during operational rebuilds of indexes while a
    * replacement index is being built.
    */
  val HiddenField = Field[Option[Boolean]]("hidden")

  def CacheIndexers(scope: ScopeID) = Map(
    CollectionID.collID -> NativeIndex.CollectionByName(scope).indexer,
    IndexID.collID -> NativeIndex.IndexByName(scope).indexer)

  val ConfigurationValidator = SourceField.validator[Query] +
    IndexByField.validator[Query](getFilterMask(IndexByField)) +
    CoveredField.validator[Query](getFilterMask(CoveredField))

  val VersionValidator =
    Document.DataValidator +
      SchemaNames.NameField.validator[Query] +
      ConfigurationValidator +
      ActiveField.validator[Query] +
      PartitionsField.validator[Query] +
      UniqueField.validator[Query] +
      SerialField.validator[Query] +
      HiddenField.validator[Query]

  val DefaultData = Data(
    ActiveField -> false,
    HiddenField -> None,
    SerialField -> Some(true),
    CollectionIndexField -> Some(false)
  )

  val NativeIndexMinID = IndexID(0)
  val NativeIndexMaxID = IndexID(32767)

  val UserIndexMinID = IndexID(32768)
  val UserIndexMaxID = IndexID.MaxValue

  // This value is intended to strike a balance between the read cost
  // of the union of all partitions, and the probability that each
  // (hashed) partition will reside on its own node.
  val MaxIndexPartitions = 8L

  // Class indexes - indexes with no terms, or a single "class" term -
  // are partitioned by default. This applies that policy.
  case object DefaultPartitionPolicy extends Validator[Query] {
    protected val filterMask = MaskTree.empty

    @inline private def withinBounds(p: Long): Boolean =
      p > 0 && p <= MaxIndexPartitions

    override protected def validatePatch(cur: Data, diff: Diff) = {
      val data = cur.patch(diff)
      val terms = IndexByField.read(data.fields)
      val parts = PartitionsField.read(diff.fields).toOption.flatten
      val curParts = PartitionsField.read(data.fields).toOption.flatten

      (curParts, parts) match {
        case (Some(c), Some(p)) if c == p && withinBounds(p) =>
          Query(Right(diff))

        case (None, Some(p)) if withinBounds(p) =>
          Query(Right(diff))

        case (_, Some(p)) =>
          Query(
            Left(
              List(OutOfBoundValue(PartitionsField.path, p, 1, MaxIndexPartitions))))

        case (Some(_), None) =>
          Query(Right(diff))

        case (None, None) =>
          terms match {
            case Right(terms) if terms.isEmpty =>
              Query {
                Right(
                  diff.update(
                    PartitionsField -> parts.orElse(Some(MaxIndexPartitions))))
              }
            case Right(
                  Vector(TermConfig(TermDataPath(List(Right("class"))), _, _, _))) =>
              Query {
                Right(
                  diff.update(
                    PartitionsField -> parts.orElse(Some(MaxIndexPartitions))))
              }
            case _ =>
              Query {
                Right(diff.update(PartitionsField -> Some(1L)))
              }
          }
      }
    }
  }

  case object SourceConfigValidator extends Validator[Query] {
    protected val filterMask = MaskTree.empty

    private def multipleClassBindings(srcs: Vector[SourceConfig]): Boolean = {
      val collectionIDs = srcs collect {
        case SourceConfig(IndexSources.Limit(ids), _, _) => ids
      }
      collectionIDs.groupBy(identity) exists { case (_, ids) => ids.lengthIs > 1 }
    }

    private def mixedWildcards(srcs: Vector[SourceConfig]): Boolean =
      (srcs.length > 1) &&
        (srcs forall { _.fields.isEmpty }) &&
        (srcs exists { _.sources == IndexSources.Custom })

    override protected def validatePatch(cur: Data, diff: Diff) = {
      val data = cur.patch(diff)

      SourceField.read(data.fields) match {
        case Left(errs) => Query(Left(errs))
        case Right(srcs) if srcs.count { _.sources == IndexSources.All } > 1 =>
          Query(Left(List(MultipleWildcards(SourceField.path))))
        case Right(srcs) if multipleClassBindings(srcs) =>
          Query(Left(List(MultipleClassBindings(SourceField.path))))
        case Right(srcs) if mixedWildcards(srcs) =>
          Query(Left(List(MixedWildcards(SourceField.path))))
        case Right(_) => Query(Right(diff))
      }
    }
  }

  case object SourceBindingsValidator extends Validator[Query] {
    protected val filterMask =
      MaskTree(SourceField.path) merge
        MaskTree(IndexByField.path) merge
        MaskTree(CoveredField.path)

    override protected def validateData(
      data: Data): Query[List[ValidationException]] =
      SourceField.read(data.fields) match {
        case Left(errs) => Query(errs)
        case Right(sources) =>
          val configs =
            (IndexByField.read(data.fields), CoveredField.read(data.fields)) match {
              case (Right(terms), Right(vals)) => terms ++ vals
              case (Right(terms), _)           => terms
              case (_, Right(vals))            => vals
              case _                           => Vector.empty
            }

          val bindings = configs.iterator collect {
            case TermConfig(TermBindingPath(path), _, _, _) => path
          } toSet

          val validations = List.newBuilder[Query[List[ValidationException]]]

          sources.iterator.zipWithIndex foreach { case (source, srcIdx) =>
            source.fields.iterator.zipWithIndex foreach {
              case ((name, query), fieldIdx) =>
                val path =
                  List("source", srcIdx.toString, "fields", fieldIdx.toString, name)

                validations += LambdaL(
                  Database.RootScopeID,
                  query).callMaxEffect map {
                  case Effect.Pure => Nil
                  case _           => List(InvalidSourceBinding(path))
                }

                if (!bindings.contains(name)) {
                  validations += Query(List(UnusedSourceBinding(path)))
                }
            }
          }

          validations.result().accumulate(List.empty[ValidationException]) { _ ++ _ }
      }
  }

  case object ActiveValidator extends Validator[Query] {
    override protected def filterMask = MaskTree(ActiveField.path)

    override protected def validatePatch(
      current: Data,
      diff: Diff): Query[Either[List[ValidationException], Diff]] =
      ActiveField.read(current.fields) match {
        case Left(errs)   => Query.value(Left(errs))
        case Right(false) => Query.value(Right(diff))
        // latch index active field
        case Right(true) => Query.value(Right(diff.update(ActiveField -> true)))
      }
  }

  def LiveValidator(ec: EvalContext) =
    IndexSpecificValidator + ReferencesValidator(ec)

  val IndexSpecificValidator =
    VersionValidator +
      DefaultPartitionPolicy +
      SourceConfigValidator +
      SourceBindingsValidator +
      ActiveValidator

  /** Returns an indexer containing all indexes that apply to the collection. */
  def getIndexer(scope: ScopeID, collID: CollectionID): Query[Indexer] =
    getIndexes(scope, collID) map {
      _.foldLeft[Indexer](Indexer.Empty) { _ + _.indexer }
    }

  /** Returns an indexer for all non-user-defined indexes which apply to the
    * collection.
    */
  def getNativeIndexer(scope: ScopeID, collID: CollectionID): Indexer =
    getNativeIndexes(scope, collID).foldLeft[Indexer](Indexer.Empty) {
      _ + _.indexer
    }

  /** Returns a list of all configured indexes that apply to the collection */
  def getIndexes(
    scope: ScopeID,
    collID: CollectionID): Query[Iterable[IndexConfig]] =
    for {
      userIndexes <- getUserIndexes(scope, collID)
      nativeIndexes = getNativeIndexes(scope, collID)
    } yield nativeIndexes ++ userIndexes

  /** Retrieves all user-defined indexes that apply to the collection. */
  def getUserIndexes(
    scope: ScopeID,
    collID: CollectionID): Query[Iterable[IndexConfig]] = collID match {
    case InternalCollectionID(_) => Query.value(List[IndexConfig]())
    case _                       => Index.getUserDefinedBySource(scope, collID)
  }

  /** Returns a list of all non-user-defined indexes which apply to the collection. */
  def getNativeIndexes(scope: ScopeID, collID: CollectionID): Iterable[IndexConfig] =
    collID match {
      case NativeCollectionID(_) =>
        NativeCollection(collID).nativeIndexes(scope).map(_.config)
      case _ =>
        CollectionConfig.User.nativeIndexes(scope).map(_.config)
    }

  def getConfig(scope: ScopeID, id: IndexID): Query[Option[IndexConfig]] =
    id match {
      case NativeIndexID(id) => Query.value(NativeIndex(scope, id))
      case UserIndexID(id)   => get(scope, id)
      case id =>
        log.error(s"Attempted to load invalid index document $id in $scope.")
        Query.none
    }

  def get(scope: ScopeID, id: IndexID): Query[Option[Index]] =
    id match {
      case UserIndexID(id) => Cache.indexByID(scope, id)
      case id =>
        Query.fail(
          new IllegalArgumentException(s"$id is not a user-defined index ID"))
    }

  def getUncached(
    scope: ScopeID,
    id: IndexID,
    forSyncBuild: Boolean = false): Query[Option[Index]] =
    SchemaCollection
      .Index(scope, lookupIndexes = false)
      .getVersionNoTTL(id)
      .flatMapT {
        case vers: Version.Live =>
          val cfgs = SourceField.read(vers.data.fields) match {
            case Left(List(_: ValueRequired)) =>
              Vector(SourceConfig(IndexSources.Custom))
            case Left(errs)  => throw errs.head
            case Right(srcs) => srcs
          }

          Query.some(Index(vers, cfgs, forSyncBuild))
        case _ => Query.none
      }

  def idByName(scope: ScopeID, name: String): Query[Option[IndexID]] =
    Cache.indexIDByName(scope, name).map(_.flatMap(_.active))

  /** Retrieve the indexes that bind to a given source. */
  def getUserDefinedBySource(
    scope: ScopeID,
    source: CollectionID): Query[Iterable[Index]] =
    Cache
      .indexIDsByScope(scope)
      .flatMapT { Index.get(scope, _) map { _.to(Iterable) } }
      .selectT { _.binders(source).isDefined }

  /** Retrieve the indexes that bind to a given source without cache access. */
  def getUserDefinedBySourceUncached(
    scope: ScopeID,
    source: CollectionID): Query[Iterable[Index]] =
    IndexID
      .getAllUserDefined(scope)
      .collectMT(getUncached(scope, _).selectT(_.binders(source).isDefined))
      .flattenT

  /** Start an async. build task for the given Index, if the Database
    * has tasks enabled and the number of active builds for the
    * associated account is not above the MaxConcurrentBuilds feature
    * flag.
    *
    * Setting `isOperational` to true will prevent the user from being
    * billed for this build.
    */
  def build(
    scope: ScopeID,
    id: IndexID,
    isOperational: Boolean = false): Query[TaskID] =
    Account.flagForScope(scope, RunTasks).flatMap {
      case false => throw IndexBuildsDisabled
      case true =>
        Database.forScope(scope) flatMap {
          case Some(db) if db.accountID == Database.DefaultAccount =>
            IndexBuild.RootTask
              .create(scope, id, isOperational = isOperational)
              .map { _.id }

          case Some(db) =>
            for {
              pending <- Task.executingCount(db.accountID, IndexBuild.RootTask.name)
              limit   <- db.account.map(_.flags.get(MaxConcurrentBuilds))
              task    <- IndexBuild.RootTask.create(scope, id)
            } yield {
              if (pending >= limit) {
                throw new IndexBuildLimitExceeded(pending, limit)
              } else {
                task.id
              }
            }

          case None =>
            throw new IllegalStateException(s"scope $scope not found.")
        }
    }

  def markActive(
    scope: ScopeID,
    id: IndexID,
    snapshotTS: Timestamp,
    active: Boolean = true): Query[Option[Version]] = {
    SchemaCollection.Index(scope).get(id, snapshotTS).flatMapT { idx =>
      if (idx.data(ActiveField) == active) {
        Query.some(idx)
      } else {
        val diff = Diff(ActiveField -> active)
        SchemaCollection.Index(scope).internalUpdate(id, diff).map(Some(_))
      }
    }
  }

  // Helpers.

  private def mkBinders(
    ec: EvalContext,
    intp: FQLInterpCtx,
    fields: List[(String, QueryV)],
    computed: List[(String, ComputedFieldData)]): List[TermBinder] = {

    val binders = List.newBuilder[TermBinder]

    fields foreach { case (name, query) =>
      val lambda = LambdaL(ec.scopeID, query)

      val binder = { version: Version =>
        ec.evalLambdaApply(lambda, VersionL(version), RootPosition) map {
          case Right(ArrayL(elems)) =>
            elems.iterator map {
              case _: UnresolvedRefL => NullV
              case elem: ScalarL     => elem.irValue.asInstanceOf[ScalarV]
              case _                 => NullV
            } toVector
          case Right(_: UnresolvedRefL) => Vector.empty
          case Right(elem: ScalarL)     => Vector(elem.irValue.asInstanceOf[ScalarV])
          case _                        => Vector.empty
        }
      }

      binders += (name -> binder)
    }

    computed foreach { case (path, cfData) =>
      // The collection name is only used for error reporting. Because we don't emit
      // errors from index builds, we can insert a dummy value here.
      val cf = ComputedField(path, ComputedField.UnknownCollectionName, cfData)

      val binder = { version: Version =>
        version match {
          case version: Version.Live =>
            val doc = Value.Doc(version.id, versionOverride = Some(version))
            // If computed term .x uses computed field .y in its body, the user
            // can change the definition of y and silently corrupt the index.
            // Thus using y in x must be disallowed, or else we need to track
            // transitive computed field dependencies. Similarly, using the
            // document's ts field, which may not be known at index time,
            // must be disallowed.
            cf.eval(intp, doc) flatMap {
              case _: Result.Err =>
                Query.value(Vector.empty)
              case Result.Ok(v) =>
                val rest = FieldPath(path).fold(_ => Nil, _.tail)
                ReadBroker.extractValue(intp, computed = true, v, rest) map {
                  case Some(v) => Value.toIR(v).fold(_ => Vector.empty, Vector(_))
                  case None    => Vector.empty
                }
            }
          case _: Version.Deleted =>
            Query.value(Vector.empty)
        }
      }

      binders += (path -> binder)
    }

    binders.result()
  }

  private def parseSource(configs: Vector[SourceConfig]) =
    if (configs exists { _.sources == IndexSources.Custom }) {
      IndexSources.Custom
    } else {
      val ids = configs.iterator collect {
        case SourceConfig(IndexSources.Limit(ids), _, _) => ids
      }
      IndexSources.Limit(ids.flatten.toSet)
    }

  private def parseBinders(
    ec: EvalContext,
    intp: FQLInterpCtx,
    configs: Vector[SourceConfig]): Binders = {
    import IndexSources._

    val bySource = MMap.empty[CollectionID, List[TermBinder]]
    var wildcards = Option.empty[List[TermBinder]]

    configs foreach {
      case SourceConfig(All, _, _) =>
        // NB. `IndexSources.All` is reserved for internal usage. Its handling is
        // done at the `Index.Native.binders` implementation.
        ()

      case SourceConfig(Custom, fields, computed) =>
        wildcards = Some(mkBinders(ec, intp, fields, computed))

      case SourceConfig(Limit(ids), fields, computed) =>
        ids foreach { bySource.update(_, mkBinders(ec, intp, fields, computed)) }
    }

    collID =>
      bySource.get(collID) orElse {
        collID match {
          case UserCollectionID(_) => wildcards
          case _                   => None
        }
      }
  }
}
