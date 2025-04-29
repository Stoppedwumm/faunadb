package fauna.model.runtime.fql2

import fauna.atoms.{ AccountID, ScopeID }
import fauna.auth.EvalAuth
import fauna.flags.{ BooleanValue => FFBoolean, EvalV4FromV10, Feature }
import fauna.lang.syntax._
import fauna.model.runtime.fql2.stdlib.{
  CollectionCompanion,
  Global,
  GlobalResolvers,
  UserCollectionCompanion
}
import fauna.repo.query.Query
import fauna.repo.values.Value
import fql.ast.FreeVars
import fql.env.TypeEnv
import fql.parser.Tokens
import fql.typer.{ TypeScheme, TypeShape }
import fql.typer.Type

/** Module for lookup of a database's global FQL value context. */
object GlobalContext {

  /** Used to limit the number of collections returned when returning the
    * full environment
    */
  private val EnvironmentCollectionLimit = 1000

  /** Used to limit the number of functions returned when returning the
    * full environment
    */
  private val EnvironmentFunctionLimit = 1000

  /** A map of feature-flagged members by type or collection.
    *
    * For example, to exclude the `compute` field from the collection `Collection`,
    * set the following map to: `Map("Collection" -> Map("computed_fields" -> FF))`.
    *
    * NOTE: for collections' documents, disabled fields will fail at runtime. For types,
    * an additional runtime check in the implementation is required.
    */
  val FlaggedTypeMembers: Map[String, Map[String, Feature[AccountID, FFBoolean]]] =
    Map(
      GlobalResolvers.FQL.selfType.name -> Map("evalV4" -> EvalV4FromV10)
    )

  // used in tests, so not private
  def filterDisabledTypeMembers(intp: FQLInterpCtx, env: TypeEnv): Query[TypeEnv] = {
    intp.account.map { account =>
      val filteredShapes = env.typeShapes.map { case (name, shape) =>
        FlaggedTypeMembers.get(name) match {
          case None => (name, shape)
          case Some(mflags) =>
            val filteredMembers = shape.fields.flatMap { case (name, sch) =>
              Option.when(mflags.get(name).fold(true)(account.flags.get(_))) {
                (name, sch)
              }
            }
            (name, shape.copy(fields = filteredMembers))
        }
      }
      env.copy(typeShapes = filteredShapes)
    }
  }

  def lookupUserValue(ctx: FQLInterpCtx, name: String): Query[Option[Value]] = {
    def collQ = UserCollectionCompanion.lookup(ctx, name)
    def funQ = UserFunction.lookup(ctx, name)
    collQ.orElseT(funQ)
  }

  def lookupSingletonOrUDF(ctx: FQLInterpCtx, name: String): Query[Option[Value]] =
    Query
      .value(GlobalResolvers.singletons.get(name))
      .orElseT(lookupUserValue(ctx, name))

  def lookupSingletonOrUDFActive(
    scope: ScopeID,
    name: String): Query[Option[Value]] = {
    // Create an interpreter that will read the active schema. This is necessary for
    // role predicates.
    val intp = new FQLInterpreter(EvalAuth(scope))
    lookupSingletonOrUDF(intp, name)
  }

  // dynamic env lookups need to return the transitive set of definitions for
  // types which appear in its definition. unfortunately we're not very lazy in
  // that we eagerly populate the dependency tree of a type structure even if
  // the query doesn't use all of it. We ought to either simplify this and just
  // load/cache the entire db env, or make walking the tree truly lazy, which
  // most likely means needing to thread Query through the typer, somehow.
  def lookupEnv(
    intp: FQLInterpCtx,
    vars: FreeVars,
    tvars: FreeVars): Query[TypeEnv] = {
    val allNames = vars.names.concat(tvars.names.flatMap(allGlobalsForType))
    val stdlib = Global.StaticEnv.globalTypes.keys.toSet
    lookupGlobals(intp, allNames, stdlib).map(_ ++ Global.StaticEnv)
  }

  /** Looks up the given type names. This is somewhat hacky, because we don't
    * have a good way to map type names back to global names.
    */
  private def lookupTypes(
    intp: FQLInterpCtx,
    types: Iterable[String],
    current: Set[String]): Query[TypeEnv] =
    lookupGlobals(intp, types.flatMap(allGlobalsForType), current)

  // Looks up the given global names by looking for a collection or function with the
  // given name. Any names in `current` will be skipped.
  //
  // If a collection or function is found, the internal signature of that function
  // (or the internal signature of all computed fields) will be checked as well, and
  // this will recurse to `lookupTypes` to find those types as well.
  private def lookupGlobals(
    ctx: FQLInterpCtx,
    names: Iterable[String],
    current: Set[String]): Query[TypeEnv] = {
    names
      .filterNot(current.contains(_))
      .map { name =>
        def collQ =
          UserCollectionCompanion.lookup(ctx, name).mapT { companion =>
            val freeNames = companion.typeShapes.values.flatMap(_.alias match {
              case Some(alias) => alias.raw.freeTypeVars
              case None        => Set.empty
            })
            val ty = collectionCompanionToGlobalType(companion)
            (ty, freeNames)
          }

        def funQ =
          UserFunction.lookup(ctx, name).mapT { companion =>
            val freeNames = companion.signature.raw.freeTypeVars
            val ty = userFunctionToGlobalType(companion)
            (ty, freeNames)
          }

        collQ.orElseT(funQ)
      }
      .sequence
      .flatMap { lookups =>
        val globalTypes = Map.newBuilder[String, TypeScheme]
        val shapes = Map.newBuilder[String, TypeShape]

        lookups.flatten.foreach { case (gt, _) =>
          globalTypes += (gt.name -> gt.typeScheme)
          shapes ++= gt.dependencyTypeShapes
        }

        val e =
          TypeEnv(globalTypes = globalTypes.result(), typeShapes = shapes.result())

        val freeTypes = lookups.flatten
          .flatMap(_._2.view)
          .filterNot(current.contains)
          .toSet
        if (freeTypes.isEmpty) {
          Query.value(e)
        } else {
          // If this happens, it means the signature of a function or a computed
          // field contains types that we didn't just lookup. So recurse and go look
          // those up as well.
          lookupTypes(ctx, freeTypes, current ++ names).map { env =>
            env ++ e
          }
        }
      }
  }

  /** For functions, the type name is the function name, so we just look those
    *  up. For collections, there are three types we need to look for:
    * - The doc type (`Foo`).
    * - The null doc type (`NullFoo`).
    * - The collection companion type (`FooCollection`).
    *
    * These type names are all found be appending a prefix or suffix to the
    * collection name. So we attempt to find those by stripping the
    * prefix/suffix off the type name (if it has it), and looking up the type
    * after removing the prefix.
    */
  private def allGlobalsForType(ty: String): Iterable[String] = {
    val types = Set.newBuilder[String]

    // The doc name is the same as the type.
    types += ty

    // Strip `Null` from `NullFoo`
    if (ty.startsWith("Null") && ty.length > 4) {
      types += ty.drop(4)
    }

    // Strip `Collection` from `FooCollection`
    if (ty.endsWith("Collection") && ty.length > 10) {
      types += ty.dropRight(10)
    }

    types.result()
  }

  /** This method is used by our environment endpoint to return the complete environment
    * for a given scope.  This environment is used for auto completions when writing
    * fql queries.
    */
  def completeEnv(intp: FQLInterpCtx): Query[TruncatedEnvironment] = {
    (
      userCollectionsAsGlobals(intp.scopeID),
      userFunctionsAsGlobals(intp.scopeID)) par { (collTypes, funcTypes) =>
      val globalTypes = Map.newBuilder[String, TypeScheme]
      val shapes = Map.newBuilder[String, TypeShape]

      collTypes
        .concat(funcTypes)
        .take(EnvironmentCollectionLimit)
        .concat(funcTypes.take(EnvironmentFunctionLimit))
        .filterNot(gt => Global.StaticEnv.globalTypes.contains(gt.name))
        .foreach { gt =>
          globalTypes += (gt.name -> gt.typeScheme)
          shapes ++= gt.dependencyTypeShapes
        }

      val unfilteredEnv = Global.StaticEnv.copy(
        globalTypes = globalTypes.result() ++ Global.StaticEnv.globalTypes,
        typeShapes = shapes.result() ++ Global.StaticEnv.typeShapes)

      filterDisabledTypeMembers(intp, unfilteredEnv).map { typeEnv =>
        TruncatedEnvironment(
          typeEnv = typeEnv,
          collectionsTruncated = collTypes.length > EnvironmentCollectionLimit,
          functionsTruncated = funcTypes.length > EnvironmentFunctionLimit
        )
      }
    }
  }

  case class TruncatedEnvironment(
    typeEnv: TypeEnv,
    collectionsTruncated: Boolean,
    functionsTruncated: Boolean)

  private def userCollectionsAsGlobals(scope: ScopeID): Query[Seq[GlobalType]] = {
    UserCollectionCompanion
      .getAll(scope)
      .selectT(c => Tokens.isValidIdent(c.name))
      .takeT(EnvironmentCollectionLimit + 1)
      .flattenT
      .map { _.map(collectionCompanionToGlobalType(_)) }
  }

  private def userFunctionsAsGlobals(scope: ScopeID): Query[Seq[GlobalType]] = {
    UserFunction
      .getAll(scope)
      .selectT(f => Tokens.isValidIdent(f.name))
      .takeT(EnvironmentFunctionLimit + 1)
      .flattenT
      .map { _.map(userFunctionToGlobalType(_)) }
  }

  private case class GlobalType(
    name: String,
    typeScheme: TypeScheme,
    dependencyTypeShapes: Map[String, TypeShape])

  private def collectionCompanionToGlobalType(
    collectionCompanion: CollectionCompanion): GlobalType =
    GlobalType(
      collectionCompanion.name,
      collectionCompanion.selfType.staticType.typescheme,
      collectionCompanion.typeShapes
    )

  /** @param name this is typically the name of the function, but could also
    *             be an alias.
    */
  private def userFunctionToGlobalType(userFunction: UserFunction): GlobalType =
    GlobalType(
      userFunction.name,
      typeScheme = TypeTag
        .NamedUserFunction(userFunction.signature.raw match {
          case ty: Type => ty
          case other    => sys.error(s"function signature is not a type: $other")
        })
        .typescheme,
      Map.empty
    )
}
