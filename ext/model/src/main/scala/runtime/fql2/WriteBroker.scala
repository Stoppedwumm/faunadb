package fauna.model.runtime.fql2

import fauna.atoms._
import fauna.lang.syntax._
import fauna.model.runtime.fql2.stdlib.SchemaCollectionCompanion
import fauna.model.runtime.Effect
import fauna.model.schema.{ CheckConstraint, CollectionConfig, NativeCollectionID }
import fauna.model.schema.index.CollectionIndex
import fauna.model.Index
import fauna.repo.{
  IndexRow,
  Store,
  UniqueConstraintViolation,
  UnretryableException,
  WriteFailure,
  WriteFailureException
}
import fauna.repo.doc.Version
import fauna.repo.query.{ CollectionWrite, QState, Query }
import fauna.repo.schema._
import fauna.repo.schema.ConstraintFailure._
import fauna.repo.values.{ Value, ValueType }
import fauna.storage.doc.{ Data, Diff }
import fauna.storage.index.NativeIndexID
import fauna.storage.DocAction
import scala.util.control.NoStackTrace

/** Manages writes to documents, applying permissions and validating write
  * arguments based on the collection schema.
  *
  * The general steps of the write process are:
  * - Check that the current auth has the correct permissions to peform the
  *   action (create, update, delete, or history modification).
  * - Validate the write against the collection's public schema.
  * - Convert the write w/ the collections public-private write adaptor.
  * - Validate the write against the collection's private schema.
  * - Send write to the Store to be persisted.
  */
object WriteBroker {
  // FIXME: we have the CollectionConfig in CollectionSO's now, so this is
  // unnecessary.
  def get(scope: ScopeID, id: CollectionID): Query[WriteBroker] = {
    CollectionConfig(scope, id) flatMap {
      case Some(coll) => Query.value(new WriteBroker(coll))
      case _ =>
        Query.fail(new IllegalStateException(s"Collection $id not found on $scope"))
    }
  }

  private def versToDoc(
    ctx: FQLInterpCtx,
    collID: CollectionID,
    vers: Version.Live): Query[Result[Value.Doc]] =
    CollectionConfig(ctx.scopeID, collID).flatMap {
      case Some(config) =>
        config.companionObject match {
          case s: SchemaCollectionCompanion =>
            s.nameByID(ctx.scopeID, vers.id).map { name =>
              // Name should always be Some, so explode if it's not.
              Value.Doc(vers.id, Some(name.get)).toResult
            }
          case _ => Value.Doc(vers.id, None).toQuery
        }
      case None =>
        Query.fail(
          new IllegalStateException(s"No collection ${collID} found for ${vers.id}"))
    }

  // Plugs into mapWriteResult so check constraint eval errors
  // cause the query to fail. Not meant for other uses.
  private final case class CheckConstraintEvalException(
    name: String,
    err: QueryFailure)
      extends UnretryableException(
        s"Error evaluating check constraint `$name`: ${err.failureMessage}")
      with NoStackTrace

  private def evalCheckConstraints(
    ctx: FQLInterpCtx,
    checks: List[CheckConstraint],
    doc: Value.Doc
  ): Query[Result[Unit]] = {
    val results = checks.map { ch =>
      ch.eval(ctx, doc) map { (ch.name, _) }
    }.sequence

    results.flatMap { rs =>
      val firstErr = rs
        .find {
          case (_, Result.Err(_)) => true
          case _                  => false
        }
        .map { case (n, e) => (n, e.errOption.get) }

      if (firstErr.nonEmpty) {
        val (name, err) = firstErr.get
        Query.fail(CheckConstraintEvalException(name, err))
      } else {
        val fails = rs flatMap {
          case (_, Result.Ok(Value.True)) =>
            None
          case (name, Result.Ok(Value.False)) =>
            Some(CheckConstraintFailure.Rejected(name))
          case (name, Result.Ok(Value.Null(_))) =>
            Some(CheckConstraintFailure.Rejected(name))
          case (name, Result.Ok(_)) =>
            Some(CheckConstraintFailure.BadReturn(name))
          case _ =>
            None
        }

        if (fails.isEmpty) {
          Query.value(Result.Ok(()))
        } else {
          Query.fail(
            WriteFailureException(WriteFailure.SchemaConstraintViolation(fails)))
        }
      }
    }
  }

  def createDocumentVers(
    ctx: FQLInterpCtx,
    id: CollectionID,
    mode: DataMode,
    struct: Value.Struct
  ): Query[Result[Version.Live]] =
    get(ctx.scopeID, id).flatMap {
      _.createDocument(ctx, mode, struct)
    }

  def updateDocumentVers(
    ctx: FQLInterpCtx,
    doc: Value.Doc,
    mode: DataMode,
    fields: Value.Struct
  ): Query[Result[Version.Live]] =
    get(ctx.scopeID, doc.id.collID).flatMap {
      _.updateDocument(ctx, doc, mode, fields)
    }

  def replaceDocumentVers(
    ctx: FQLInterpCtx,
    doc: Value.Doc,
    mode: DataMode,
    fields: Value.Struct
  ): Query[Result[Version.Live]] =
    get(ctx.scopeID, doc.id.collID).flatMap {
      _.replaceDocument(ctx, doc, mode, fields)
    }

  def createDocument(
    ctx: FQLInterpCtx,
    id: CollectionID,
    mode: DataMode,
    struct: Value.Struct
  ): Query[Result[Value.Doc]] =
    createDocumentVers(ctx, id, mode, struct).flatMapT {
      versToDoc(ctx, id, _)
    }

  def updateDocument(
    ctx: FQLInterpCtx,
    doc: Value.Doc,
    mode: DataMode,
    fields: Value.Struct
  ): Query[Result[Value.Doc]] =
    updateDocumentVers(ctx, doc, mode, fields).mapT { _ => doc }

  def replaceDocument(
    ctx: FQLInterpCtx,
    doc: Value.Doc,
    mode: DataMode,
    fields: Value.Struct
  ): Query[Result[Value.Doc]] =
    replaceDocumentVers(ctx, doc, mode, fields).mapT { _ => doc }

  def deleteDocument(
    ctx: FQLInterpCtx,
    doc: Value.Doc
  ): Query[Result[Value.Doc]] =
    get(ctx.scopeID, doc.id.collID).flatMap {
      _.deleteDocument(ctx, doc)
    }
}

class WriteBroker(config: CollectionConfig) {
  @inline private def collectionSchema = config.Schema
  @inline private def scope = config.parentScopeID

  // Get the ID set in `struct`, or generate a new one.
  // Plain data id fields don't count.
  // Returns (ID, true if ID found in `struct`).
  // TODO: Should eventually be easier to validate `struct` against a bona fide
  //            schema specifying restrictions on the id field, rather than this.
  private def getOrMakeID(mode: DataMode, struct: Value.Struct.Full)
    : Query[Result[(DocID, Boolean, Value.Struct.Full)]] = {
    val idPath = (Path.RootPrefix :+ "id").toPath
    val idType = SchemaType.Union(ScalarType.ID, ScalarType.Str, ScalarType.Long)
    struct.fields.lift("id") match {
      case Some(Value.Null(_)) if mode != DataMode.PlainData =>
        Query.fail(
          WriteFailureException(
            WriteFailure.SchemaConstraintViolation(
              Seq(MissingField(idPath, idType, "id")))))
      case Some(v) if mode != DataMode.PlainData =>
        TypeTag.ID.cast(v) match {
          case Some(id) =>
            if (id.value >= 0) {
              val withoutId = Value.Struct(struct.fields - "id")

              Query.value(
                Result.Ok((DocID(SubID(id.value), config.id), true, withoutId)))
            } else {
              Query.fail(
                WriteFailureException(
                  WriteFailure.SchemaConstraintViolation(
                    Seq(ValidatorFailure(idPath, "id must be nonnegative")))))
            }
          case None =>
            // NB: `v` should previously (in the caller) have been translated to IR
            // as part of `struct`.
            val prov = SchemaType.valuePreciseType(Value.toIR(v).toOption.get)
            Query.fail(
              WriteFailureException(
                WriteFailure.SchemaConstraintViolation(
                  Seq(TypeMismatch(idPath, idType, prov)))))
        }
      case _ =>
        collectionSchema.nextID.flatMap {
          case None =>
            Query.fail(
              WriteFailureException(WriteFailure
                .MaxIDExceeded(collectionSchema.scope, collectionSchema.collID)))
          case Some(id) => Query.value(Result.Ok((id, false, struct)))
        }
    }
  }

  private def checkComputedFieldConflicts(
    mode: DataMode,
    struct: Value.Struct.Full): Query[Unit] =
    if (mode == DataMode.Default && config.computedFields.nonEmpty) {
      // NB: Computed fields are restricted to the top level.
      val conflicts =
        config.computedFields.keySet.intersect(struct.fields.keySet).toSeq
      if (conflicts.nonEmpty) {
        Query.fail(
          WriteFailureException(
            WriteFailure.SchemaConstraintViolation(
              conflicts.map(ConstraintFailure.ComputedFieldOverwrite(_)))))
      } else {
        Query.unit
      }
    } else {
      Query.unit
    }

  /** Create a document with the supplied struct. */
  def createDocument(
    ctx: FQLInterpCtx,
    mode: DataMode,
    struct: Value.Struct
  ): Query[Result[Version.Live]] = {
    Query.snapshotTime flatMap { snapTS =>
      requireWriteEffect(ctx, Effect.Action.Function("create")) {
        val writeQ =
          ReadBroker.materializeStruct(ctx, struct).flatMap { structWithId =>
            checkComputedFieldConflicts(mode, structWithId) flatMap { _ =>
              getOrMakeID(mode, structWithId).flatMapT {
                case (id, isProvidedID, structWithoutId) =>
                  structToIR(structWithoutId, ctx.stackTrace).flatMapT { ir =>
                    val ver = Version.Live(
                      scope,
                      id,
                      collectionSchema.schemaVersion,
                      Data(
                        collectionSchema.toInternalData(
                          SchemaOp.Create(collectionSchema, mode, ir),
                          snapTS
                        )))
                    checkCreatePermission(ctx, ver, isProvidedID).flatMapT { _ =>
                      checkCollectionDocWrite(ctx, id).flatMapT { _ =>
                        Store.create(
                          collectionSchema,
                          id,
                          mode,
                          Data(ir),
                          checkIdExists = isProvidedID) map { version =>
                          val ev =
                            WriteHook.Event(version.id, None, Some(version.data))
                          scheduleRevalidationHooks(ctx, ev)
                          Result.Ok(version)
                        } andThen {
                          Query.incrCreates()
                        }
                      }
                    }
                  }
              } flatMapT { vers =>
                val check = if (config.checkConstraints.nonEmpty) {
                  WriteBroker.versToDoc(ctx, config.id, vers) flatMapT {
                    WriteBroker
                      .evalCheckConstraints(ctx, config.checkConstraints, _)
                  }
                } else {
                  Query.value(Result.Ok(()))
                }
                check mapT { _ => vers }
              }
            }
          }
        mapWriteResult(
          writeQ,
          config.constraintViolationMessage(None, DocAction.Create),
          ctx.stackTrace
        )
      }
    }
  }

  /** Update a document by patching its existing data with the supplied struct. */
  def updateDocument(
    ctx: FQLInterpCtx,
    doc: Value.Doc,
    mode: DataMode,
    fields: Value.Struct
  ): Query[Result[Version.Live]] =
    Query.snapshotTime flatMap { snapTS =>
      requireWriteEffect(ctx, Effect.Action.Function("update")) {
        ReadBroker.materializeStruct(ctx, fields) flatMap { fields =>
          updateOrReplace(ctx, mode, doc, fields) { (prevVersion, diff) =>
            checkUpdatePermission(
              ctx,
              doc.id,
              prevVersion,
              prevVersion
                .withData(
                  collectionSchema.transformToData(
                    SchemaOp
                      .Update(collectionSchema, mode, diff.fields, prevVersion.data),
                    snapTS
                  ))
                .withUnresolvedTS
            ).flatMapT { _ =>
              checkCollectionDocWrite(ctx, doc.id).flatMapT { _ =>
                Store.externalUpdate(collectionSchema, doc.id, mode, diff) map {
                  Result.Ok(_)
                }
              }
            }.flatMapT { vers =>
              WriteBroker.evalCheckConstraints(
                ctx,
                config.checkConstraints,
                doc
              ) mapT { _ =>
                vers
              }
            }
          }
        }
      }
    }

  /** Update a document by replacing its data with the supplied struct. */
  def replaceDocument(
    ctx: FQLInterpCtx,
    doc: Value.Doc,
    mode: DataMode,
    fields: Value.Struct
  ): Query[Result[Version.Live]] =
    Query.snapshotTime flatMap { snapTS =>
      requireWriteEffect(ctx, Effect.Action.Function("replace")) {
        ReadBroker.materializeStruct(ctx, fields) flatMap { fields =>
          updateOrReplace(ctx, mode, doc, fields) { (prevVersion, diff) =>
            checkUpdatePermission(
              ctx,
              doc.id,
              prevVersion,
              prevVersion
                .withData(collectionSchema.transformToData(
                  SchemaOp
                    .Replace(collectionSchema, mode, diff.fields, prevVersion.data),
                  snapTS
                ))
                .withUnresolvedTS
            ).flatMapT { _ =>
              checkCollectionDocWrite(ctx, doc.id).flatMapT { _ =>
                Store.replace(
                  collectionSchema,
                  doc.id,
                  mode,
                  Data(diff.fields)) map {
                  Result.Ok(_)
                }
              }
            }.flatMapT { vers =>
              WriteBroker.evalCheckConstraints(
                ctx,
                config.checkConstraints,
                doc
              ) mapT { _ =>
                vers
              }
            }
          }
        }
      }
    }

  /** Returns a shallow object with _id and collection */
  def deleteDocument(
    ctx: FQLInterpCtx,
    doc: Value.Doc
  ): Query[Result[Value.Doc]] = {

    requireWriteEffect(ctx, Effect.Action.Function("delete")) {
      checkDeletePermission(ctx, doc.id).flatMapT { _ =>
        Store.get(collectionSchema, doc.id) flatMap {
          case Some(prev) =>
            checkCollectionDocWrite(ctx, doc.id).flatMapT { _ =>
              val writeQ =
                Store.externalDelete(collectionSchema, doc.id).map {
                  deletedVersion =>
                    val ev = WriteHook.Event(doc.id, Some(prev.data), None)
                    scheduleRevalidationHooks(ctx, ev)
                    Result.Ok(deletedVersion)
                } andThen {
                  Query.incrDeletes()
                }
              mapWriteResult(
                writeQ,
                config.constraintViolationMessage(
                  Some((doc.id, prev.data)),
                  DocAction.Delete),
                ctx.stackTrace
              ).mapT { deletedVersion =>
                doc.copy(versionOverride = Some(deletedVersion))
              }
            }
          case None =>
            QueryRuntimeFailure
              .DocumentNotFound(collectionSchema.name, doc, ctx.stackTrace)
              .toQuery
        }
      }
    }
  }

  // Helpers.

  private def updateOrReplace(
    ctx: FQLInterpCtx,
    mode: DataMode,
    doc: Value.Doc,
    fields: Value.Struct.Full
  )(updater: (Version.Live, Diff) => Query[Result[Version.Live]])
    : Query[Result[Version.Live]] = {

    ReadBroker.get(ctx, doc, Effect.Action.AllFields).map(_.lift).flatMap {
      case Some(prev) =>
        structToIR(fields, ctx.stackTrace).flatMapT { ir =>
          val writeQ = checkComputedFieldConflicts(mode, fields) flatMap { _ =>
            updater(prev, Diff(ir)) mapT { updated =>
              val ev = WriteHook.Event(doc.id, Some(prev.data), Some(updated.data))
              scheduleRevalidationHooks(ctx, ev)
              updated
            } andThen {
              Query.incrUpdates()
            }
          }
          mapWriteResult(
            writeQ,
            config.constraintViolationMessage(
              Some((doc.id, prev.data)),
              DocAction.Update),
            ctx.stackTrace
          )
        }
      case None =>
        QueryRuntimeFailure
          .DocumentNotFound(collectionSchema.name, doc, ctx.stackTrace)
          .toQuery
    }
  }

  private def requireWriteEffect[V](ctx: FQLInterpCtx, action: Effect.Action)(
    query: Query[Result[V]]): Query[Result[V]] =
    action.check(ctx, Effect.Write).flatMapT { _ =>
      if (ctx.userValidTime.orElse(ctx.systemValidTime).isEmpty) {
        query
      } else {
        QueryRuntimeFailure.MustWriteNow(ctx.stackTrace).toQuery
      }
    }

  private def structToIR(
    struct: Value.Struct.Full,
    stackTrace: FQLInterpreter.StackTrace) =
    Value.toIR(struct) match {
      case Right(v) => Query.value(Result.Ok(v))
      case Left(fail) if fail.provided == ValueType.AnyLambdaType =>
        QueryRuntimeFailure(
          "invalid_type",
          "Anonymous functions cannot be stored. Use a string of FQL instead.",
          stackTrace).toQuery
      case Left(fail) =>
        QueryRuntimeFailure("invalid_type", fail.displayString, stackTrace).toQuery
    }

  private def mapWriteResult[T](
    q: Query[Result[T]],
    constraintViolationMessage: => String,
    stackTrace: FQLInterpreter.StackTrace): Query[Result[T]] =
    q.recoverWith {
      // Note that the check constraint name will be contained in the
      // span of the error.
      case WriteBroker.CheckConstraintEvalException(_, err: QueryFailure) =>
        err.toQuery
      case WriteFailureException(fail) =>
        fail match {
          case dcv: WriteFailure.DeleteConstraintViolation =>
            QueryRuntimeFailure
              .DeleteConstraintViolation(collectionSchema.name, dcv.id, stackTrace)
              .toQuery
          case dcv: WriteFailure.SchemaConstraintViolation =>
            QueryRuntimeFailure
              .SchemaConstraintViolation(
                constraintViolationMessage,
                dcv.failures,
                stackTrace)
              .toQuery
          case dcv: WriteFailure.InvalidID =>
            QueryRuntimeFailure
              .InvalidDocumentID(collectionSchema.name, dcv.id, stackTrace)
              .toQuery
          case dcv: WriteFailure.CreateDocIDExists =>
            QueryRuntimeFailure
              .DocumentIDExists(collectionSchema.name, dcv.id, stackTrace)
              .toQuery
          case dcv: WriteFailure.DocNotFound =>
            // TODO: Can this doc be named?
            QueryRuntimeFailure
              .DocumentNotFound(collectionSchema.name, Value.Doc(dcv.id), stackTrace)
              .toQuery
          // TODO: handle max id exceeded with better error code
          case _: WriteFailure.MaxIDExceeded =>
            QueryRuntimeFailure(
              "tmp_write_failed",
              fail.toString,
              stackTrace).toQuery
        }
      case ucv: UniqueConstraintViolation =>
        mapUniqueConstraintError(ucv, stackTrace)
    }

  private def mapUniqueConstraintError[T](
    ucv: UniqueConstraintViolation,
    stackTrace: FQLInterpreter.StackTrace): Query[Result[T]] = {
    def buildUCF(idx: CollectionIndex) = {
      val fieldPaths = idx.terms.map { t => new Path(t.fieldPath) }.toList
      Query.value(UniqueConstraintFailure(fieldPaths))
    }

    ucv.indexes
      .map {
        // native index
        case (_, NativeIndexID(id), _, _) =>
          config.nativeIndexes.find(_.indexID == id.id) match {
            case Some(idx) => buildUCF(idx)
            case None      => Query.value(UniqueConstraintFailure(Nil))
          }

        // v10 index
        case (_, indexID: IndexID, _, _)
            if config.uniqueIndexes.exists(_.indexID == indexID) =>
          buildUCF(config.uniqueIndexes.find(_.indexID == indexID).get)

        // catch all, fql4 index
        case idx => mapFQL4IndexUniqueConstraintFailure(idx)
      }
      .sequence
      .map { constraintFailures =>
        QueryRuntimeFailure
          .SchemaConstraintViolation(
            "Failed unique constraint.",
            constraintFailures,
            stackTrace)
          .toResult
      }
  }

  private def mapFQL4IndexUniqueConstraintFailure(
    index: (ScopeID, IndexID, DocID, IndexRow)): Query[ConstraintFailure] = {
    Index.get(index._1, index._2).map {
      case Some(idx) => FQL4UniqueConstraintFailure(idx.name)
      case None      => UniqueConstraintFailure(List.empty)
    }
  }

  private def scheduleRevalidationHooks(
    ctx: FQLInterpCtx,
    ev: WriteHook.Event
  ): Unit =
    config.revalidationHooks.foreach { mkHook =>
      ctx.addPostEvalHook(mkHook(ctx, config, ev))
    }

  // perm check helpers

  private def checkCreatePermission(
    ctx: FQLInterpCtx,
    newVersion: Version.Live,
    hasProvidedID: Boolean
  ): Query[Result[Unit]] = {
    val idQ = if (hasProvidedID) {
      ctx.auth.checkCreateWithIDPermission(scope, newVersion)
    } else {
      Query.value(true)
    }

    val cQ = idQ.flatMap {
      case true  => ctx.auth.checkCreatePermission(scope, newVersion)
      case false => Query.value(false)
    }

    maybeFailPermCheck(cQ, ctx.stackTrace)
  }

  private def checkUpdatePermission(
    ctx: FQLInterpCtx,
    id: DocID,
    prevVersion: Version.Live,
    newVersion: Version.Live
  ): Query[Result[Unit]] =
    maybeFailPermCheck(
      ctx.auth.checkWritePermission(scope, id, prevVersion, newVersion),
      ctx.stackTrace)

  private def checkDeletePermission(
    ctx: FQLInterpCtx,
    id: DocID
  ): Query[Result[Unit]] =
    maybeFailPermCheck(ctx.auth.checkDeletePermission(scope, id), ctx.stackTrace)

  private def maybeFailPermCheck(
    check: Query[Boolean],
    stackTrace: FQLInterpreter.StackTrace): Query[Result[Unit]] =
    check.map {
      case true  => Result.Ok(())
      case false => Result.Err(QueryRuntimeFailure.PermissionDenied(stackTrace))
    }

  private def checkCollectionDocWrite(
    ctx: FQLInterpCtx,
    id: DocID
  ): Query[Result[Unit]] = {
    val collWrite = id.collID match {
      case NativeCollectionID(CollectionID) =>
        Some(CollectionID(id.subID.toLong) -> CollectionWrite.ToCollection)
      case UserCollectionID(_) =>
        Some(id.collID -> CollectionWrite.ToDocument)
      case _ => None
    }

    collWrite match {
      case Some((collID, write)) =>
        QState { state =>
          state.findCollectionWrite(collID) match {
            case Some(w) if w != write =>
              Result.Err(
                QueryRuntimeFailure.CollectionDocModify(ctx.stackTrace)) -> state
            case Some(_) => Result.Ok(()) -> state
            case None =>
              val updated = state.copy(collectionWrites =
                state.collectionWrites.update(collID, write))
              Result.Ok(()) -> updated
          }
        }

      case None => Query.value(Result.Ok(()))
    }
  }
}
