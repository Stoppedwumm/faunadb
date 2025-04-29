package fauna.model.schema

import fauna.atoms._
import fauna.lang.{ CPUTiming, Timestamp }
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.model._
import fauna.model.runtime.fql2.{
  FQLInterpreter,
  PostEvalHook,
  QueryRuntimeFailure,
  Result => RuntimeResult
}
import fauna.model.runtime.fql2.stdlib.Global
import fauna.model.schema.fsl.{ SourceFile, SourceGenerator }
import fauna.model.schema.index.{ CollectionIndex, CollectionIndexManager }
import fauna.model.schema.migration.MigrationConverter
import fauna.model.tasks.MigrationTask
import fauna.model.Cache
import fauna.repo.query.Query
import fauna.repo.schema.{ ConstraintFailure, Path, WriteHook }
import fauna.repo.schema.migration.{ Migration, MigrationCodec }
import fauna.repo.schema.migration.MigrationCodec._
import fauna.repo.IndexConfig
import fauna.storage.doc.Diff
import fauna.storage.ir._
import fql.ast.{ Name, Span, Src }
import fql.env.CollectionTypeInfo
import fql.error.Error
import fql.migration.MigrationValidator
import fql.schema.SchemaDiff
import fql.typer.Typer
import fql.Result
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Deadline

/** Validates the database type environment */
object TypeEnvValidator {
  val Hook: PostEvalHook.Factory = (ctx, config, ev) => {

    // WARNING: TypeEnvValidators are merged across collections
    // so a given TypeEnvValidator can end up with a set of events
    // that don't belong to the original collection config it was
    // created with.
    // Pulling the scope here is fine because they will have the same
    // scope but if the TypeEnvValidator needs to be broadened to use
    // the collection config that will need to be accounted for here
    // and in the merge method.
    TypeEnvValidator(
      config.parentScopeID,
      Map(ev.id -> (ev, ctx.stackTrace)),
      ctx.stackTrace)
  }

  // TODO: can drop along with the old env validator components
  val MetaFieldsTExprs = CollectionTypeInfo.MetaFields.map { case (n, ty) =>
    Name(n, Span.Null) -> Typer().valueToExpr(ty)
  }
}

// FIXME: Use FKValidationHook against dependency lists of globals + types
// stored on schema def objects (collections, functions, roles, etc.)
final case class TypeEnvValidator(
  scope: ScopeID,
  changed: Map[DocID, (WriteHook.Event, FQLInterpreter.StackTrace)],
  stackTrace: FQLInterpreter.StackTrace)
    extends PostEvalHook
    with ExceptionLogging {
  type Self = TypeEnvValidator
  val key = Some(scope)

  // Convenience alias for describing Collections.
  type CollInfo = (CollectionID, String)
  // Convenience alias for describing UserFunctions. There's nothing fun about it.
  type FunInfo = (UserFunctionID, String, LambdaWrapper.Src)

  def merge(other: TypeEnvValidator) =
    other.copy(changed = changed ++ other.changed, stackTrace = other.stackTrace)

  type ValidatorFn = (
    Typer,
    Map[DocID, (WriteHook.Event, FQLInterpreter.StackTrace)]
  ) => Query[(List[Error], List[ConstraintFailure])]

  def run(): Query[RuntimeResult[Unit]] =
    runWithTyper(deriveMigrations, write = true)

  def dryRun(): Query[RuntimeResult[Unit]] =
    runWithTyper(deriveMigrations, write = false)

  def runWithTyper(
    f: ValidatorFn,
    write: Boolean = true): Query[RuntimeResult[Unit]] = {
    for {
      state <- Query.state
      deadline = state.deadline.timeLeft.fromNow
      isEnvTypechecked <- FQLInterpreter.isEnvTypechecked(scope)

      res <- runWithTyper0(deadline, isEnvTypechecked, write, f)
    } yield res
  }

  private def runWithTyper0(
    deadline: Deadline,
    isEnvTypeChecked: Boolean,
    write: Boolean,
    f: ValidatorFn): Query[RuntimeResult[Unit]] = {
    Query.timing(FQLInterpreter.EnvTypingTimeMetric) {
      (collDefsQ, funDefsQ, v4RolesQ, Query.stats) par {
        (colls, funDefs, v4Roles, stats) =>
          val cpuTiming = CPUTiming()

          implicit val deadline0 = deadline

          val errsQ = validateTypeEnv(
            colls,
            funDefs,
            v4Roles,
            isEnvTypeChecked,
            cpuTiming,
            write)
            .flatMap {
              case (ctx, Result.Ok(typer)) =>
                f(typer, changed).flatMap {
                  case (Nil, Nil) =>
                    // Even if typechecking is disabled, we want to make sure users
                    // aren't creating collections that will cause type environment
                    // conflicts.
                    val collValidator = CollectionDerivedTypeValidator(scope, colls)

                    val derivedTypeErrors = changed.values
                      .filter(_._1.id.collID == CollectionID.collID)
                      .flatMap { case (writeEvent, stackTrace) =>
                        collValidator.validateDerivedTypeNameConflicts(
                          writeEvent,
                          stackTrace.currentStackFrame
                        )
                      }

                    Query.value(ctx -> (derivedTypeErrors, Nil))

                  case migrationErrs => Query.value(ctx -> migrationErrs)
                }

              case (ctx, Result.Err(e)) => Query.value(ctx -> (e, Nil))
            }

          stats.timing(
            FQLInterpreter.EnvTypingCPUTimeMetric,
            cpuTiming.elapsedMillis)

          errsQ.map { case (ctx, (errs, fails)) =>
            if (errs.isEmpty && fails.isEmpty) {
              Result.Ok(())
            } else {
              mkFailure(ctx, errs, fails)
            }
          }
      }
    }
  }

  private def deriveMigrations(
    typer: Typer,
    changed: Map[DocID, (WriteHook.Event, FQLInterpreter.StackTrace)]
  ): Query[(List[Error], List[ConstraintFailure])] = {
    changed.keys
      .map {
        case CollectionID(id) =>
          SchemaCollection.Collection(scope).get(id).flatMap {
            case Some(_) => updateCollectionMigrations(typer, id)

            // Ignore deleted collections
            case None => Query.value((Nil, Nil))
          }

        case _ => Query.value((Nil, Nil))
      }
      .sequence
      .map {
        _.fold((Nil, Nil)) { case ((accErrs, accFails), (errs, fails)) =>
          (accErrs ++ errs, accFails ++ fails)
        }
      }
  }

  private def updateCollectionMigrations(
    typer: Typer,
    id: CollectionID): Query[(List[Error], List[ConstraintFailure])] =
    SchemaStatus.forScopeUncached(scope).flatMap { status =>
      status.activeSchemaVersion match {
        case Some(active) =>
          SchemaCollection.Collection(scope).get(id, active.ts).flatMap {
            // If the collection existed at the time of the staged schema push,
            // re-derive migrations.
            case Some(_) =>
              for {
                snap <- Query.snapshotTime
                prevMigrations <- MigrationDeriver
                  .derive(scope, typer, id, active.ts, snap)
                _ = {
                  if (prevMigrations.errors.nonEmpty) {
                    throw new IllegalStateException(
                      s"Failed to derive migrations for $id: ${prevMigrations.errors}")
                  }
                }

                newMigrations <- MigrationDeriver
                  .derive(scope, typer, id, active.ts, Timestamp.MaxMicros)

                _ <- applyMigrations(
                  id,
                  newMigrations.oldMigrations,
                  newMigrations.newMigrations)
                fails <- CollectionIndexManager
                  .updateStaged(
                    scope,
                    id,
                    prevMigrations = prevMigrations.newMigrations,
                    newMigrations = newMigrations.newMigrations,
                    active)

                v4IndexFails <-
                  if (
                    prevMigrations.newMigrations.nonEmpty || newMigrations.newMigrations.nonEmpty
                  ) {
                    checkV4Indexes(scope, id)
                  } else {
                    Query.value(Nil)
                  }
              } yield (newMigrations.errors, fails ++ v4IndexFails)

            // If the schema is new, we just assert the collection is empty, because
            // it was added in this staged schema push.
            case None =>
              for {
                empty <- SchemaManager.isCollectionEmpty(scope, id)

                // We still need to create backing indexes.
                fails <- CollectionIndexManager.updateStaged(
                  scope,
                  id,
                  prevMigrations = Seq.empty,
                  newMigrations = Seq.empty,
                  active)
              } yield {
                if (!empty) {
                  throw new IllegalStateException("staged collections must be empty")
                }

                (Nil, fails)
              }
          }

        case None =>
          for {
            snap <- Query.snapshotTime
            res <- MigrationDeriver
              .derive(scope, typer, id, snap, Timestamp.MaxMicros)

            _ <- applyMigrations(id, res.oldMigrations, res.newMigrations)
            _ <-
              if (res.newMigrations.nonEmpty) {
                MigrationTask.updateFromCommit(scope, id)
              } else {
                Query.unit
              }
            fails <- CollectionIndexManager
              .updateUnstaged(scope, id, migrations = res.newMigrations)

            v4IndexFails <-
              if (res.newMigrations.nonEmpty) {
                checkV4Indexes(scope, id)
              } else {
                Query.value(Nil)
              }
          } yield (res.errors, fails ++ v4IndexFails)
      }
    }

  private def applyMigrations(
    id: CollectionID,
    oldMigrations: ArrayV,
    newMigrations: Seq[Migration]
  ): Query[Unit] =
    if (newMigrations.isEmpty) {
      Query.unit
    } else {
      // Manually append these, because:
      // - The `MigrationList` structure can't be modified. Its optimized for reads,
      //   and appending to it would be O(N) or so, so adding to an ArrayV will be a
      //   lot cheaper.
      // - We don't know the new schema version yet, so we need to insert a
      //   `TransactionTimeV`, which will get rendered into correct `TimeV`. This can't
      //   be done with the `MigrationList` structure.
      val allMigrations = ArrayV(oldMigrations.elems ++ newMigrations.map { m =>
        MapV(
          MigrationCodec.MigrationField.path.head -> m.encode,
          MigrationCodec.SchemaVersionField.path.head -> TransactionTimeV(false))
      })

      for {
        _ <- SchemaCollection
          .Collection(scope)
          .internalUpdate(
            id,
            Diff(
              MapV(Collection.InternalMigrationsField.path.head -> allMigrations)))
      } yield ()
    }

  private def checkV4Indexes(
    scope: ScopeID,
    id: CollectionID): Query[List[ConstraintFailure]] =
    for {
      activeColl <- Collection.getUncached(scope, id).map(_.flatMap(_.active))

      // Fetch the collection at the snapshot time to avoid pending writes getting in
      // the way of this check. We would fetch it at Timestamp.MaxMicros, but the
      // index configs lookup is cached, so it'll return stale results if we don't
      // check the collection at the snapshot time.
      snapColl <- Collection.getUncachedAtSnapshot(scope, id)
    } yield {
      val activeIndexes =
        activeColl.fold(List.empty[CollectionIndex])(_.config.collIndexes)
      val stagedIndexes =
        snapColl.fold(List.empty[CollectionIndex])(_.config.collIndexes)

      // This list of index configs will contain:
      // - v4 indexes
      // - v10 indexes
      // - staged v10 indexes
      //
      // We want to disallow indexes if there are any v4 indexes
      // present.
      snapColl.fold(List.empty[IndexConfig])(_.config.indexConfigs).flatMap {
        index =>
          val existsActive = activeIndexes.exists(_.indexID == index.id)
          val existsStaged = stagedIndexes.exists(_.indexID == index.id)

          (existsActive, existsStaged) match {
            case (false, false) =>
              List(
                ConstraintFailure.ValidatorFailure(
                  Path.empty,
                  "Cannot create migrations on a collection with v4 indexes"))

            case _ => Nil
          }
      }
    }

  // FIXME: Pass `deadline` through to the schema type validator.
  @annotation.nowarn("cat=unused-params")
  private def validateTypeEnv(
    colls: Seq[CollInfo],
    funDefs: Seq[FunInfo],
    v4Roles: Seq[String],
    isEnvTypechecked: Boolean,
    cpuTiming: CPUTiming,
    write: Boolean
  )(implicit deadline: Deadline): Query[(Map[Src.Id, String], Result[Typer])] = {

    import fql.env._

    def validateEnv0(files: Iterable[SourceFile.FSL]) = {
      val errs = new ListBuffer[Error]
      val items = files.flatMap { file =>
        file.parse() match {
          case Result.Ok(it) => it
          case Result.Err(e) =>
            errs ++= e
            Seq.empty
        }
      }

      if (errs.isEmpty) {
        val stdlib = Global.StaticEnv

        // Right(_) is for a v4 function body.
        val v4Functions = funDefs.collect { case (_, name, Right(_)) => name }
        val schema = DatabaseSchema.fromItems(items, v4Functions, v4Roles)

        cpuTiming.measure {
          SchemaTypeValidator.validate(stdlib, schema, isEnvTypechecked)
        }
      } else {
        Result.Err(errs.toList)
      }
    }

    def updateSchema(updates: Iterable[InternalUpdate]) = {
      val collToId = colls.map { c => c._2 -> c._1 }.toMap
      // Only track v10 functions with Left(_).
      val funcToId = funDefs.collect { case (id, name, Left(_)) => name -> id }.toMap

      for {
        collConfig <- SchemaCollection.Collection(scope)
        collSchema = collConfig.Schema
        funcConfig <- SchemaCollection.UserFunction(scope)
        funcSchema = funcConfig.Schema
        _ <- updates.map {
          case CollectionUpdate(name, computedFields) =>
            updateInternalCollSig(collToId(name), computedFields)

          case FunctionUpdate(name, internalSig) =>
            updateInternalFuncSig(funcToId(name), internalSig)
        }.join
      } yield ()
    }

    for {
      _ <- Cache.refreshLastSeenSchema(scope)
      files <-
        if (write) {
          SchemaSource.getStagedUncached(scope).mapT { _.file }
        } else {
          // If we're in dry-run mode, go ahead and generate blank schema and patched
          // schema, and compare results.
          for {
            derived <- SourceGenerator.deriveSchema(scope)
            patched <- SchemaSource.patchedSchema(scope)
          } yield {
            val derivedItems = derived.flatMap { file =>
              file.parse() match {
                case Result.Ok(it) => it
                case Result.Err(e) =>
                  throw new IllegalStateException(
                    s"derived schema produced invalid syntax: $e")
              }
            }
            val patchedItems = patched.flatMap { file =>
              file.parse() match {
                case Result.Ok(it) => it
                case Result.Err(e) =>
                  throw new IllegalStateException(
                    s"patched schema produced invalid syntax: $e")
              }
            }

            // TODO: Structual equality would be better here, but diff works well
            // enough.
            val diff =
              SchemaDiff.diffItems(derivedItems, patchedItems, renames = Map.empty)

            if (diff.isEmpty) {
              derived
            } else {
              throw new IllegalStateException(
                s"derived schema does not match patched schema:\n$derivedItems\n$patchedItems")
            }
          }
        }

      ctx = files.map { f => f.src -> f.content }.toMap
      res = validateEnv0(files)
      res2 <- res match {
        case fql.Result.Ok(env) =>
          if (write) {
            updateSchema(env.updates).map { _ =>
              Result.Ok(env.environment.newTyper())
            }
          } else {
            Query.value(Result.Ok(env.environment.newTyper()))
          }
        case res: Result.Err => Query.value(res)
      }
    } yield (ctx, res2)
  }

  private def updateInternalCollSig(
    collID: CollectionID,
    sig: Option[String]): Query[Unit] = {
    SchemaCollection.Collection(scope).get(collID).flatMap {
      case Some(doc) =>
        val oldInternalSig = doc.data(Collection.InternalDocSignatureField)

        if (oldInternalSig == sig) {
          Query.unit
        } else {
          SchemaCollection
            .Collection(scope)
            .internalUpdate(
              collID,
              Diff(Collection.InternalDocSignatureField -> sig)
            )
            .join
        }

      case None =>
        throw new IllegalStateException(s"collection $collID not found")
    }
  }

  private def updateInternalFuncSig(
    funcID: UserFunctionID,
    sig: String): Query[Unit] = {
    SchemaCollection.UserFunction(scope).get(funcID).flatMap {
      case Some(doc) =>
        val oldInternalSig = doc.data(UserFunction.InternalSigField)

        if (oldInternalSig == Some(sig)) {
          Query.unit
        } else {
          SchemaCollection
            .UserFunction(scope)
            .internalUpdate(
              funcID,
              Diff(UserFunction.InternalSigField -> Some(sig))
            )
            .join
        }

      case None =>
        throw new IllegalStateException(s"function $funcID not found")
    }
  }

  // currently we don't do anything to _validate_ collections here. We just
  // look them up to populate the type env.
  private def collDefsQ: Query[Seq[CollInfo]] =
    CollectionID
      .getAllUserDefined(scope)
      .collectMT { id =>
        SchemaCollection.Collection(scope).get(id).mapT { vers =>
          val name = SchemaNames.findName(vers)
          SchemaNames.findAlias(vers) match {
            case Some(alias) => Seq((id, name), (id, alias))
            case None        => Seq((id, name))
          }
        }
      }
      .flattenT
      .map(_.flatten)

  // this is really dumb right now and grabs the entire list of UDFs to check them.
  private def funDefsQ: Query[Seq[FunInfo]] =
    UserFunctionID
      .getAllUserDefined(scope)
      .collectMT { id =>
        SchemaCollection.UserFunction(scope).get(id).mapT { vers =>
          val name = SchemaNames.findName(vers).toString
          (id, name, vers.data(UserFunction.BodyField))
        }
      }
      .flattenT

  private def v4RolesQ: Query[Seq[String]] = {
    RoleID
      .getAllUserDefined(scope)
      .collectMT { id =>
        SchemaCollection.Role(scope).get(id).flatMapT { vers =>
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

  // FIXME: It's currently more work to pin down a specific expression
  // failure above with the write call which produced it. Leaving that undone
  // for now. Until then, report the failure on the last write (as represented
  // by, the Span captured by the TypeEnvValidator.)
  private def mkFailure(
    ctx: Map[Src.Id, String],
    errs: Iterable[Error],
    fails: Seq[ConstraintFailure]): RuntimeResult[Unit] =
    if (errs.nonEmpty || fails.nonEmpty) {
      val desc = errs
        .flatMap(_.renderWithSource(ctx).trim().split("\n"))
        .map(l => s"    $l")
        .mkString("\n")
      val msg = s"Invalid database schema update.\n$desc"
      QueryRuntimeFailure
        .DatabaseSchemaEnvViolation(msg, stackTrace)
        .copy(constraintFailures = fails)
        .toResult
    } else {
      RuntimeResult.Ok(())
    }
}

case class MigrationResult(
  oldMigrations: ArrayV,
  newMigrations: Seq[Migration],
  errors: List[Error] = Nil)

object MigrationResult {
  def err(e: List[Error]) = MigrationResult(ArrayV.empty, Nil, e)
}

object MigrationDeriver {
  def derive(
    scope: ScopeID,
    typer: Typer,
    id: CollectionID,
    prev: Timestamp,
    next: Timestamp): Query[MigrationResult] = for {
    empty <- SchemaManager.isCollectionEmpty(scope, id)
    prev  <- SchemaCollection.Collection(scope).get(id, prev).mapT(_.data)
    next  <- SchemaCollection.Collection(scope).get(id, next).map(_.get.data)

    res <- (empty, prev) match {
      // Empty collections and new collections don't need migrations.
      case (true, _) | (_, None) =>
        val updatedSchema = SchemaTranslator.translateCollection(scope, next)
        Query.value(MigrationValidator.validate(typer, updatedSchema) match {
          case Result.Ok(_)  => MigrationResult(ArrayV.empty, Nil)
          case Result.Err(e) => MigrationResult.err(e)
        })

      case (_, Some(p)) =>
        val prevSchema = SchemaTranslator.translateCollection(scope, p)
        val updatedSchema = SchemaTranslator.translateCollection(scope, next)

        MigrationValidator.validate(typer, prevSchema, updatedSchema) match {
          case Result.Ok(migrations) =>
            val oldMigrations =
              p.fields.get(Collection.InternalMigrationsField.path) match {
                case Some(v) => v.asInstanceOf[ArrayV]
                case None    => ArrayV.empty
              }

            MigrationConverter(scope).flatMap { converter =>
              migrations.map(converter.fromFSL).sequenceT.map {
                case RuntimeResult.Ok(newMigrations) =>
                  MigrationResult(oldMigrations, newMigrations.flatten)

                case RuntimeResult.Err(e) => MigrationResult.err(e.errors.toList)
              }
            }

          case Result.Err(e) => Query.value(MigrationResult.err(e))
        }
    }
  } yield res
}
