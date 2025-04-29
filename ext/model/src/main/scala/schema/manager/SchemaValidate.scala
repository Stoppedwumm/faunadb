package fauna.model.schema.manager

import fauna.atoms.{ CollectionID, RoleID, ScopeID, UserFunctionID }
import fauna.lang.syntax._
import fauna.model._
import fauna.model.runtime.fql2.{ FQLInterpreter, Result => FQL2Result }
import fauna.model.runtime.fql2.stdlib.Global
import fauna.model.schema._
import fauna.model.schema.fsl.SourceFile
import fauna.model.schema.migration.MigrationConverter
import fauna.repo.query.Query
import fauna.repo.Store
import fauna.storage.index.IndexTerm
import fql.ast._
import fql.env.{ DatabaseSchema, SchemaTypeValidator }
import fql.error.Error
import fql.migration.MigrationValidator
import fql.schema.{ Diff, SchemaDiff }
import fql.typer.Typer
import scala.collection.mutable.{ Map => MMap }

case class ValidateResult(
  before: Seq[SourceFile.FSL],
  after: Seq[SourceFile.FSL],
  diffs: Seq[Diff]) {
  def beforeMap = before.view.map { file => file.src -> file.content }.toMap
  def afterMap = after.view.map { file => file.src -> file.content }.toMap
}

trait SchemaValidate { self: SchemaManager =>
  import Result.{ Err, Ok }

  // Returns `true` if the collection contains no versions.
  //
  // This function has special OCC semantics: if the collection is empty,
  // this function registers an OCC check asserting emptiness. If it is not
  // empty, no OCC check is done. This balances avoiding contention with
  // needing to know the collection is empty when applying a schema change
  // using looser checks.
  def isCollectionEmpty(scope: ScopeID, id: CollectionID): Query[Boolean] = {
    val q = Store
      .sortedIndex(
        NativeIndex.DocumentsByCollection(scope),
        Vector(IndexTerm(id)),
        pageSize = 1)
      .map { versions => versions.value.isEmpty }

    for {
      isEmpty <- Query.disableConcurrencyChecks { q }
      _       <- if (isEmpty) q else Query.unit
    } yield isEmpty
  }

  /** Given a set of source files, compute the diff between them and the schema of the
    * given scope, with the given override mode. The returned `Result` contains all
    * errors found, or all computed changes based on the proposed schema files.
    *
    * If `staged = true` is passed, the proposed files will be compared against
    * the currently staged files. This is only used for `fauna schema diff`, and
    * should not be used to validate any writes! Writes should always replace
    * staged schema, by passing `staged = false`.
    */
  def validate(
    scope: ScopeID,
    submittedFiles: Seq[SourceFile.FSL],
    renames: SchemaDiff.Renames = Map.empty,
    overrideMode: Boolean = true,
    staged: Boolean = false
  ): Query[Result[ValidateResult]] = isTenantRoot(scope).flatMap {
    case true =>
      Query.value(
        Err(
          SchemaError.Validation(
            "Schema can not be created in your account root."
          )))

    case false =>
      for {
        status <- SchemaStatus.forScope(scope)
        existingFiles <-
          if (staged || !status.hasStaged) {
            // The `staged` flag does nothing if there is no staged schema, so we
            // _must_ regenerate sources here. This is so that `update` behaves
            // correctly for an active push, when there is no staged schema.
            SchemaSource.stagedFSLFiles(scope)
          } else {
            // When comparing against the active schema, we don't need to regenerate
            // sources.
            SchemaSource.activeFSLFiles(scope)
          }

        // Compute the new complete set of files.
        resultFiles =
          if (overrideMode) {
            submittedFiles
          } else {
            submittedFiles ++ (existingFiles.filterNot(s =>
              submittedFiles.exists(_.filename == s.filename)))
          }

        res <- parseAndValidateFSLItems(resultFiles) match {
          case Result.Ok(resultItems) =>
            val existingItems = existingFiles.view.flatMap {
              // Don't fail when parsing existing files. They might be broken and
              // users are trying to patch them.
              _.parse() getOrElse Seq.empty
            }.toSeq

            val withDefaultsQ =
              if (existingItems.isEmpty) {
                Query.value(existingItems)
              } else {
                fillDefaults(scope, existingItems, staged)
              }

            withDefaultsQ flatMap { existingItems =>
              val diffs =
                SchemaDiff.diffItems(
                  existingItems,
                  resultItems,
                  renames
                )

              val resultDiffs = if (overrideMode) {
                diffs
              } else {
                val knownFiles = submittedFiles.view.map { _.filename }.toSet
                // Don't remove items outside of the files proposed on override.
                diffs filter {
                  case Diff.Remove(item) => knownFiles.contains(item.span.src.name)
                  case _                 => true
                }
              }

              validateDiffs(scope, resultItems, resultDiffs, staged).map {
                case fql.Result.Ok(diffs) =>
                  Result.Ok(ValidateResult(existingFiles, resultFiles, diffs))
                case fql.Result.Err(errs) =>
                  Result.Err(
                    SchemaError
                      .SchemaSourceErrors(
                        errs.map(SchemaError.FQLError),
                        resultFiles) :: Nil)
              }
            }
          case Result.Err(e) =>
            Query.value(
              Result.Err(SchemaError.SchemaSourceErrors(e, resultFiles) :: Nil))
        }
      } yield res
  }

  private def isTenantRoot(scope: ScopeID): Query[Boolean] =
    Database.forScope(scope) map { _.fold(false)(_.isCustomerTenantRoot) }

  private def fillDefaults(
    scope: ScopeID,
    existingItems: Iterable[SchemaItem],
    useStaged: Boolean
  ): Query[Seq[SchemaItem]] =
    existingItems.map {
      case item: SchemaItem.Collection =>
        item.historyDays match {
          case Member.Default.Set(_, _) => Query.value(item)
          case Member.Default.Implicit(_, _, _) =>
            val collQ =
              if (useStaged) {
                Collection
                  .idByNameStaged(scope, item.name.str)
                  .flatMapT(Collection.getStaged(scope, _))
              } else {
                Collection
                  .idByNameActive(scope, item.name.str)
                  .flatMapT(Collection.get(scope, _))
              }

            collQ map {
              case Some(coll) =>
                val days =
                  if (coll.config.historyDuration.isFinite) {
                    coll.config.historyDuration.toDays
                  } else {
                    Long.MaxValue
                  }
                item.copy(
                  historyDays = Member.Default.Implicit(
                    Member.Kind.HistoryDays,
                    Config.Long(days, Span.Null),
                    isDefault = days == Document.DefaultRetainDays
                  ))
              case None =>
                throw new IllegalStateException(
                  s"Collection ${item.name.str} not found.")
            }
        }
      case other => Query.value(other)
    }.sequence

  private def validateDiffs(
    scope: ScopeID,
    schemaItems: Iterable[SchemaItem],
    resultDiffs: Seq[Diff],
    useStaged: Boolean
  ): Query[fql.Result[Seq[Diff]]] =
    for {
      isEnvTypechecked <- FQLInterpreter.isEnvTypechecked(scope)

      v4Functions <- SchemaCollection.UserFunction(scope).flatMap { coll =>
        UserFunctionID
          .getAllUserDefined(scope)
          .collectMT { id =>
            coll.get(id).flatMapT { vers =>
              val name = SchemaNames.findName(vers)
              val defn = vers.data(UserFunction.BodyField)

              if (defn.isRight) {
                Query.some(name)
              } else {
                Query.none
              }
            }
          }
          .flattenT
      }
      v4Roles <- SchemaCollection.Role(scope).flatMap { coll =>
        RoleID
          .getAllUserDefined(scope)
          .collectMT { id =>
            coll.get(id).flatMapT { vers =>
              val name = SchemaNames.findName(vers)

              if (Role.isV10Role(vers.data)) {
                Query.none
              } else {
                Query.some(name)
              }
            }
          }
          .flattenT
      }

      emptyCollections <- resultDiffs
        .map {
          case Diff.Modify(item: SchemaItem.Collection, _, _, _) =>
            // This is cached, so use the old name.
            val idQ =
              if (useStaged) {
                Collection.idByNameStaged(scope, item.name.str)
              } else {
                Collection.idByNameActive(scope, item.name.str)
              }
            idQ flatMap {
              case Some(id) =>
                isCollectionEmpty(scope, id).map {
                  case true  => Some(item.name.str)
                  case false => None
                }
              case None =>
                throw new IllegalStateException(
                  s"Collection ${item.name.str} not found.")
            }

          case _ => Query.none
        }
        .sequence
        .map(_.flatten.toSet)

      db = DatabaseSchema.fromItems(schemaItems, v4Functions, v4Roles)
      res = SchemaTypeValidator
        .validate(Global.StaticEnv, db, isEnvTypechecked)
        .map(_.environment)

      r <- res match {
        case fql.Result.Ok(env) =>
          validateMigrations(scope, env.newTyper(), resultDiffs, emptyCollections)
        case res: fql.Result.Err =>
          Query.value(res)
      }
    } yield {
      r
    }

  // Helper for monad-munging that disregards OK results and concatenates all errors
  // into one mega-error.
  private def gatherErrors[T](
    qs: Seq[Query[FQL2Result[T]]]): Query[fql.Result[Unit]] = {
    def gatherErrors0(rs: Seq[FQL2Result[T]]): fql.Result[Unit] = {
      val errs = rs flatMap { r: FQL2Result[T] =>
        r match {
          case FQL2Result.Err(err) => err.errors
          case FQL2Result.Ok(_)    => Seq.empty
        }
      }
      if (errs.isEmpty) {
        fql.Result.Ok(())
      } else {
        fql.Result.Err(errs.toList)
      }
    }
    qs.sequence map gatherErrors0
  }

  // Helper for monad-munging that pushes errors into the query monad.
  private def flattenResult[T](
    res: fql.Result[Query[fql.Result[T]]]): Query[fql.Result[T]] =
    res match {
      case e: fql.Result.Err => Query.value(e)
      case fql.Result.Ok(q)  => q
    }

  private def validateMigrations(
    scope: ScopeID,
    typer: Typer,
    diffs: Seq[Diff],
    emptyCollections: Set[String]): Query[fql.Result[Seq[Diff]]] = {
    MigrationConverter(scope, ignoreBackfillErrors = true).flatMap { converter =>
      val errs = List.newBuilder[Error]

      val diffsWithMigrations = diffs.map { diff =>
        val res: Query[fql.Result[Diff]] = diff match {
          case diff @ Diff.Add(coll: SchemaItem.Collection) =>
            val validations = MigrationValidator.validate(typer, coll).map { ms =>
              gatherErrors(ms.map(converter.fromFSL))
            }
            flattenResult(validations).map { _.map { _ => diff: Diff } }
          case diff @ Diff.Modify(
                from: SchemaItem.Collection,
                to: SchemaItem.Collection,
                _,
                _) =>
            // Empty collections are the same as new collections.
            if (emptyCollections.contains(to.name.str)) {
              val validations = MigrationValidator.validate(typer, to).map { ms =>
                gatherErrors(ms.map(converter.fromFSL))
              }
              flattenResult(validations).map { _.map { _ => diff: Diff } }
            } else {
              val migrationsR = MigrationValidator.validate(typer, from, to)
              migrationsR match {
                case e @ fql.Result.Err(_) => Query.value(e)
                case fql.Result.Ok(migrations) =>
                  flattenResult(migrationsR.map(schemaMigrations =>
                    gatherErrors(schemaMigrations.map(converter.fromFSL)))).map {
                    _.map { _ => diff.copy(migrations = migrations) }
                  }
              }
            }

          // Non-collections remain unchanged
          case _ => Query.value(fql.Result.Ok(diff))
        }

        res.map {
          case fql.Result.Ok(updated) => updated
          case fql.Result.Err(err) =>
            errs ++= err
            diff
        }
      }

      diffsWithMigrations.sequence map { ds =>
        val errors = errs.result()
        if (errors.nonEmpty) {
          fql.Result.Err(errors)
        } else {
          fql.Result.Ok(ds)
        }
      }
    }
  }

  // Adapted from SchemaItemConverter.
  private class DupeList(val kind: String) {
    private[this] val seen = MMap.empty[String, String]

    def check(name: String, fn: String) = {
      seen.get(name) match {
        case Some(oldfn) =>
          Some(
            SchemaError.Validation(
              s"Duplicate $kind `$name` in $fn (originally seen in $oldfn)"))
        case None =>
          seen += name -> fn
          None
      }
    }
  }

  /** Validates the collection of FSL files. A collection is valid if:
    * - Every file parses.
    * - There are no duplicate items (same type and name).
    */
  private def parseAndValidateFSLItems(
    files: Seq[SourceFile.FSL]
  ): Result[Seq[SchemaItem]] = {

    val colAndFnSet = new DupeList("collection or function")
    val roleSet = new DupeList("role")
    val apSet = new DupeList("access provider")

    val results = files map { file =>
      val res: Result[Seq[SchemaItem]] = file.parse()

      res flatMap { items =>
        val errs = items.view.flatMap { item =>
          val dupList = item.kind match {
            case SchemaItem.Kind.Collection | SchemaItem.Kind.Function => colAndFnSet
            case SchemaItem.Kind.Role                                  => roleSet
            case SchemaItem.Kind.AccessProvider                        => apSet
          }
          dupList.check(item.name.str, file.filename)
        }.toList

        if (errs.nonEmpty) {
          Err(errs)
        } else {
          Ok(items)
        }
      }
    }

    Result.collectFailures(results) map { _.flatten }
  }
}
