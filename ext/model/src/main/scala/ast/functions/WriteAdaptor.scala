package fauna.ast

import fauna.atoms._
import fauna.auth._
import fauna.lang.{ Page, Timestamp }
import fauna.lang.syntax._
import fauna.model._
import fauna.model.runtime.fql2.{ QueryCheckFailure, QueryRuntimeFailure, Result }
import fauna.model.runtime.fql2.FQLInterpreter
import fauna.model.runtime.fql2.ToString._
import fauna.model.schema.{
  CollectionConfig,
  NativeIndex,
  PublicCollection,
  SchemaCollection,
  SchemaError,
  SchemaStatus
}
import fauna.repo._
import fauna.repo.doc._
import fauna.repo.query.Query
import fauna.repo.schema.ConstraintFailure
import fauna.repo.schema.ConstraintFailure.ProtectedModeFailure
import fauna.repo.store.{ CacheStore, IDStore, LookupStore }
import fauna.repo.values.{ Value => V10Value }
import fauna.storage._
import fauna.storage.doc._
import fauna.storage.index._
import fauna.storage.ir._
import fauna.storage.lookup._
import fauna.util.{ ClientPasswordValidator, ServerPasswordValidator }
import scala.annotation.unused
import scala.util.control.NoStackTrace

// Connects check constraint evaluation errors to liftCV, so that
// check constraints fail writes. Not meant for other uses.
private final case class CheckConstraintEvalException(errs: Seq[EvalError])
    extends UnretryableException(s"Error evaluating check constraints")
    with NoStackTrace

// CVHelper (CV = Constraint Violation) recovers subqueries that fail
// constraints, including
// * unique constraints, and
// * check constraints.
object CVHelper {
  def liftCV[T](
    q: Query[T],
    pos: Position,
    isCreate: Boolean,
    isSchema: Boolean): Query[R[T]] =
    q map { Right(_) } recover {
      case UniqueConstraintViolation(idxs) if isCreate && isSchema =>
        val violations = idxs map { case (_, _, id, _) =>
          InstanceAlreadyExists(id, pos)
        }
        Left(violations)

      case UniqueConstraintViolation(idxs) =>
        val violations = idxs map { case (scope, id, _, row) =>
          val values = row.key.terms.map { v =>
            Literal(scope, v.value)
          }
          (RefL(scope, id), values)
        }
        Left(List(InstanceNotUnique(violations, pos)))

      case CheckConstraintEvalException(errs) =>
        // To match the behavior of V10, return the first abort, else
        // return all errors.
        val aborts = errs collect { case a: TransactionAbort => a }
        if (aborts.nonEmpty) {
          Left(List(aborts.head))
        } else {
          Left(errs.toList)
        }

      case WriteFailureException(WriteFailure.SchemaConstraintViolation(errs)) =>
        Left(errs.map { e =>
          SchemaValidationError(s"${e.label}: ${e.message}", pos)
        }.toList)
    }
}
import CVHelper._

object MaximumIDException
    extends Exception("Maximum sequential ID reached.")
    with NoStackTrace

sealed abstract class WriteConfig(val collectionID: CollectionID) {

  def config: IOConfig

  def defaultData: Data

  def versionValidator(ec: EvalContext, subID: SubID): Validator[Query]

  def liveValidator(ec: EvalContext, sub: Option[SubID]): Validator[Query]

  protected def prevForReplace(ec: EvalContext, data: Data): Data

  def prevVersions(
    ec: EvalContext,
    sub: SubID,
    isPartial: Boolean,
    pos: Position): Query[R[(Version.Live, Version.Live)]] = {

    val id = DocID(sub, collectionID)

    RuntimeEnv.Default.Store(ec.scopeID).get(id) map {
      case Some(vers) if isPartial =>
        Right((vers, vers))

      case Some(vers) =>
        val filtered = prevForReplace(ec, vers.data) merge
          config.writeProtectedData(vers.data)
        Right((vers, vers.withData(filtered)))

      case None =>
        Left(List(InstanceNotFound(Right(RefL(ec.scopeID, id)), pos)))
    }
  }

  def isSchema: Boolean = false

  def isCreatable: Boolean = true

  def isUpdatable(sub: SubID): Boolean = true

  def isDeletable(sub: SubID): Boolean = true

  def nextID(ec: EvalContext): Query[SubID] =
    IDStore.nextSnowflakeID(ec.scopeID, collectionID) map { _.subID }

  private def evalCheckConstraints(
    ec: EvalContext,
    id: DocID,
    pos: Position): Query[Unit] =
    CollectionConfig(ec.scopeID, collectionID).flatMap {
      case Some(col) if col.checkConstraints.nonEmpty =>
        val results = col.checkConstraints map { check =>
          check.evalV4(ec, id) flatMap {
            case Result.Ok(V10Value.True) =>
              Query.none
            case Result.Ok(V10Value.False) =>
              Query.some(
                CheckConstraintEvalError(
                  ConstraintFailure.CheckConstraintFailure.Rejected(check.name),
                  pos))
            case Result.Ok(V10Value.Null(_)) =>
              Query.some(
                CheckConstraintEvalError(
                  ConstraintFailure.CheckConstraintFailure.Rejected(check.name),
                  pos))
            case Result.Ok(_) =>
              Query.some(
                CheckConstraintEvalError(
                  ConstraintFailure.CheckConstraintFailure.BadReturn(check.name),
                  pos))
            // The final two cases mimic LambdaWrapper's FQLX-from-FQL4 evaluation.
            case Result.Err(e: QueryRuntimeFailure) =>
              e.abortReturn match {
                case Some(v) =>
                  // To handle this unusual case, it's easier to recreate an intp
                  // than it is to plumb it through evaluation.
                  val ctx = new FQLInterpreter(ec.auth, stackDepth = ec.stackDepth)
                  v.value.toDisplayString(ctx).map { message =>
                    Some(TransactionAbort(message, pos))
                  }
                case None =>
                  Query.some(TransactionAbort(e.message, pos))
              }
            case Result.Err(QueryCheckFailure(errors)) =>
              val errMsg = errors map { _.messageLine } mkString "\n"
              throw new IllegalStateException(
                s"Error calling check constraint `${check.name}`.\n$errMsg")
          }
        }
        results.sequence.flatMap { rs =>
          val errs = rs.flatten
          if (errs.isEmpty) {
            Query.unit
          } else {
            Query.fail(new CheckConstraintEvalException(errs))
          }
        }
      case _ => Query.unit
    }

  def write(
    ec: EvalContext,
    id: DocID,
    data: Data,
    isCreate: Boolean,
    pos: Position): Query[R[Version.Live]] = {
    val q = RuntimeEnv.Default.Store(ec.scopeID).insert(id, data, isCreate) andThen {
      evalCheckConstraints(ec, id, pos)
    }
    liftCV(q, pos, isCreate, isSchema)
  }

  def delete(
    ec: EvalContext,
    prev: Version,
    @unused pos: Position): Query[R[Version.Deleted]] = {
    RuntimeEnv.Default.Store(ec.scopeID).remove(prev.id) map { Right(_) }
  }

  def insertVersion(
    ec: EvalContext,
    id: DocID,
    validTS: Timestamp,
    action: DocAction,
    data: Data = Data.empty): Query[DocEvent] = {
    val store = RuntimeEnv.Default.Store(ec.scopeID)
    val actionQ = action match {
      case Create | Update => store.insertCreate(id, validTS, data)
      case Delete          => store.insertDelete(id, validTS)
    }
    actionQ map { _.event }
  }

  def removeVersion(
    ec: EvalContext,
    id: DocID,
    versionID: VersionID): Query[Unit] = {

    /** In the above insertVersion method, we treat both create and update inserts as a create.
      * We want to do the same here so that our removes our consistent with our inserts.
      */
    val versionToRemove = versionID.action match {
      case Update          => versionID.copy(action = Create)
      case Create | Delete => versionID
    }

    RuntimeEnv.Default.Store(ec.scopeID).removeVersion(id, versionToRemove)
  }

  protected def indexer(ec: EvalContext): Query[Indexer] =
    Index.getIndexer(ec.scopeID, collectionID)

  protected def indexer(scopeID: ScopeID): Query[Indexer] =
    Index.getIndexer(scopeID, collectionID)
}

sealed abstract class VersionedSchemaWriteConfig(collectionID: CollectionID)
    extends WriteConfig(collectionID) {

  override final def isSchema = true

  protected def failIfStagedSchema[T](ec: EvalContext, pos: Position)(
    f: => Query[R[T]]) =
    SchemaStatus.forScope(ec.scopeID).flatMap {
      case status if status.hasStaged =>
        Query.value(
          Left(
            List(SchemaValidationError(
              "Cannot update schema from FQL v4 if schema has been staged.",
              pos))))
      case _ => f
    }

  override def write(
    ec: EvalContext,
    id: DocID,
    data: Data,
    isCreate: Boolean,
    pos: Position): Query[R[Version.Live]] =
    failIfStagedSchema(ec, pos) {
      writeInternal(ec, id, data, isCreate, pos)
    }

  /** This override allows skipping the staged schema check in subclasses. This
    * is required for collection indexes.
    */
  protected def writeInternal(
    ec: EvalContext,
    id: DocID,
    data: Data,
    isCreate: Boolean,
    pos: Position): Query[R[Version.Live]] = {
    CacheStore.updateSchemaVersion(ec.scopeID).flatMap { _ =>
      writeNoSchemaUpdate(ec, id, data, isCreate, pos)
    }
  }

  protected def writeNoSchemaUpdate(
    ec: EvalContext,
    id: DocID,
    data: Data,
    isCreate: Boolean,
    pos: Position): Query[R[Version.Live]] =
    super.write(ec, id, data, isCreate, pos)

  override def delete(
    ec: EvalContext,
    prev: Version,
    pos: Position): Query[R[Version.Deleted]] =
    failIfStagedSchema(ec, pos) {
      CacheStore.updateSchemaVersion(ec.scopeID).flatMap { _ =>
        super.delete(ec, prev, pos)
      }
    }

  override def insertVersion(
    ec: EvalContext,
    id: DocID,
    validTS: Timestamp,
    action: DocAction,
    data: Data): Query[DocEvent] =
    CacheStore.updateSchemaVersion(ec.scopeID).flatMap { _ =>
      super.insertVersion(ec, id, validTS, action, data)
    }

  override def removeVersion(
    ec: EvalContext,
    id: DocID,
    versionID: VersionID): Query[Unit] =
    CacheStore.updateSchemaVersion(ec.scopeID).flatMap { _ =>
      super.removeVersion(ec, id, versionID)
    }
}

sealed abstract class FQLXFieldRetainingWriteConfig(collectionID: CollectionID)
    extends VersionedSchemaWriteConfig(collectionID) {
  def fqlxFields: List[List[String]]

  override def write(
    ec: EvalContext,
    id: DocID,
    data: Data,
    isCreate: Boolean,
    pos: Position): Query[R[Version.Live]] = {

    val updatedDataQ = RuntimeEnv.Default.Store(ec.scopeID).get(id) map {
      case Some(prev) =>
        val updatedFields = fqlxFields
          .collect {
            case field if prev.data.fields.get(field).isDefined =>
              (field -> prev.data.fields.get(field).get)
          }
          .foldLeft(data.fields) { (fields, entry) =>
            fields.update(entry._1, entry._2)
          }
        Data(updatedFields)
      case None => data
    }

    updatedDataQ flatMap { updatedData =>
      super.write(ec, id, updatedData, isCreate, pos)
    }
  }
}

object DatabaseWriteConfig {
  object Default
      extends DatabaseWriteConfig(DatabaseWriteIOConfig, recursiveDelete = true)
  object Move
      extends DatabaseWriteConfig(MoveDatabaseIOConfig, recursiveDelete = false)
}

sealed abstract class DatabaseWriteConfig(
  val config: IOConfig,
  recursiveDelete: Boolean = false)
    extends VersionedSchemaWriteConfig(DatabaseID.collID) {

  val defaultData = Data.empty

  def versionValidator(ec: EvalContext, sub: SubID) = Database.VersionValidator

  def liveValidator(ec: EvalContext, sub: Option[SubID]) =
    Database.LiveValidator(ec)

  protected def prevForReplace(ec: EvalContext, prev: Data): Data =
    Data(SchemaNames.NameField -> prev(SchemaNames.NameField))

  override def write(
    ec: EvalContext,
    id: DocID,
    raw: Data,
    isCreate: Boolean,
    pos: Position) = {

    val accountQ =
      if (!isCreate) {
        Query.value(raw)
      } else {
        val data = raw.update(
          Database.NativeSchemaVersField -> Some(Database.CurrentNativeSchemaVers))
        Database.forScope(ec.scopeID) flatMap {
          // parent has an account - this write cannot override it
          case Some(parent) if parent.accountID != Database.DefaultAccount =>
            Query.value(data.update(Database.AccountField -> None))

          case _ => Query.value(data)
        }
      }

    val dataQ = accountQ flatMap { data =>
      // we allocate new scope and global database ids if not present.
      // Prior to ENG-XXX, global_id was equal to scope.
      if (data.fields.get(Database.ScopeField.path).isDefined) {
        Query.value(Right((data, false)))
      } else {
        val global = data.fields.get(Database.GlobalIDField.path)

        Query.nextID flatMap { id =>
          val sID = ScopeID(id)

          // Preserve global_id, if it is present.
          val gidQ = global collect { case LongV(l) =>
            Query.value(l)
          } getOrElse {
            Query.nextID
          }

          gidQ map { gid =>
            val globalID = GlobalDatabaseID(gid)

            val patched = data.update(
              Database.ScopeField -> sID,
              Database.GlobalIDField -> globalID)

            Right((patched, true))
          }
        }
      }
    }

    dataQ flatMapT { case (data, addOCC) =>
      // This code somewhat mirrors indexing logic with lookups. However,
      // lookups behavior does not map 1-1. NB `v` below does not contain a
      // diff, so its prevData will be wrong. We reconstruct it by fetching the
      // previous version and using it to generate a proper diff for lookups
      // generation.
      SchemaCollection.Database(ec.scopeID).get(id.as[DatabaseID]).flatMap { prev =>
        super.write(ec, id, data, isCreate, pos).flatMapT { v =>
          val diff = prev.map { p => v.data.diffTo(p.data) }
          val v0 = v.withDiff(diff)
          val lookups = LookupHelpers.lookups(v0)
          val addsQ = lookups.map(LookupStore.add(_, addOCC)).join
          addsQ map { _ => Right(v0) }
        } andThen {
          if (isCreate) {
            Query.incrDatabaseCreates()
          } else {
            Query.incrDatabaseUpdates()
          }
        }
      }
    }
  }

  override def delete(
    ec: EvalContext,
    prev: Version,
    pos: Position): Query[R[Version.Deleted]] = {
    Database.delete(ec.scopeID, prev.id.as[DatabaseID], recursiveDelete) map {
      // sanity check that we are returning the delete of the original document.
      case Some(v) if v.id != prev.id =>
        throw new IllegalStateException(
          s"Deleting ${prev.parentScopeID} ${prev.id}, but returned a version for ${v.parentScopeID} ${v.id}.")
      case v =>
        Right(v.get)
    } andThen {
      Query.incrDatabaseDeletes()
    }
  }

  override protected def writeInternal(
    ec: EvalContext,
    id: DocID,
    data: Data,
    isCreate: Boolean,
    pos: Position): Query[R[Version.Live]] = {
    for {
      // Bump the parent scope if the name changes.
      db <- Database.getUncached(ec.scopeID, id.as[DatabaseID])
      oldName = db.map(_.name)
      newName = data(SchemaNames.NameField)
      _ <-
        if (oldName != Some(newName.toString)) {
          CacheStore.updateSchemaVersion(ec.scopeID)
        } else {
          Query.unit
        }

      // Always bump the child scope.
      childScope = data(Database.ScopeField)
      _ <- CacheStore.updateSchemaVersion(childScope)

      res <- writeNoSchemaUpdate(ec, id, data, isCreate, pos)
    } yield res
  }

  def disable(ec: EvalContext, id: DocID) = enable(ec, id, enable = false)

  def enable(
    ec: EvalContext,
    id: DocID,
    enable: Boolean = true): Query[R[Version]] = {
    prevVersions(ec, id.subID, isPartial = true, RootPosition) flatMapT {
      case (vers, _) =>
        val updated = if (enable) {
          vers.data.remove(Database.DisabledField)
        } else {
          vers.data.update(Database.DisabledField -> Some(true))
        }

        write(ec, id, updated, isCreate = false, RootPosition)
    }
  }
}

object CollectionWriteConfig
    extends FQLXFieldRetainingWriteConfig(CollectionID.collID) {

  def fqlxFields = List(
    Collection.BackingIndexesField,
    Collection.InternalSignaturesField,
    Collection.InternalMigrationsField,
    Collection.ComputeField,
    Collection.DefinedFields,
    Collection.MigrationsField,
    Collection.IndexesField,
    Collection.ConstraintsField,
    SchemaNames.AliasField
  ).map(_.path)

  /** There is a hard limit on the ids generated here, since they are used in every DocID
    * and must fit into a u16. Hence these are implemented as sequential ids, for which we
    * pay with more contention.
    */
  override def nextID(ec: EvalContext): Query[SubID] = {
    val idQ = IDStore.nextSequentialID(
      ec.scopeID,
      collectionID,
      UserCollectionID.MinValue.toLong,
      UserCollectionID.MaxValue.toLong)

    idQ flatMap {
      case Some(id) => Query.value(id.subID)
      case None     => Query.fail(MaximumIDException)
    }
  }

  val config = CollectionIOConfig
  val defaultData = Collection.DefaultData

  override def isUpdatable(sub: SubID) =
    CollectionID(sub.toLong) match {
      case UserCollectionID(_) => true
      case _                   => false
    }

  override def isDeletable(sub: SubID) =
    CollectionID(sub.toLong) match {
      case UserCollectionID(_) => true
      case _                   => false
    }

  def versionValidator(ec: EvalContext, sub: SubID) =
    CollectionID(sub.toLong) match {
      case UserCollectionID(_) => Collection.UserCollectionVersionValidator
      case _                   => Collection.VersionValidator
    }

  def liveValidator(ec: EvalContext, sub: Option[SubID]) = {
    val collectionID = sub map { v =>
      CollectionID(v.toLong)
    }

    collectionID match {
      case None | Some(UserCollectionID(_)) =>
        Collection.UserCollectionLiveValidator(ec)
      case _ => Collection.LiveValidator(ec)
    }
  }

  protected def prevForReplace(ec: EvalContext, prev: Data): Data =
    Data(
      SchemaNames.NameField -> prev(SchemaNames.NameField),
      Collection.RetainDaysField -> prev(Collection.RetainDaysField),
      Collection.MinValidTimeFloorField -> prev.getOrElse(
        Collection.MinValidTimeFloorField,
        Timestamp.Epoch)
    )

  /** Adjust the persisted MVT field of the collection, adding it to the
    * collection data if necessary. If the collection's scope has an MVT pin,
    * MVT will not be updated past the pinned time.
    *
    * Public so V10 can apply the same logic in a post-eval hook.
    */
  def updateMVT(scope: ScopeID, id: CollectionID, cur: Data): Query[Data] = {
    val curMVT = Collection.deriveMinValidTimeNoOffset(scope, id)
    val newMVT = Query.snapshotTime map { ts =>
      ts - cur(Collection.RetainDaysField)
        .getOrElse(Document.DefaultRetainDays)
        .saturatedDays
    }

    (Database.pinnedMVTForScope(scope), curMVT, newMVT) par { (pinnedTS, c, n) =>
      val prepinMVT = c.max(n)
      Query.value(
        cur.update(Collection.MinValidTimeFloorField -> pinnedTS.fold(prepinMVT)(
          _.min(prepinMVT))))
    }
  }

  // Returns the IDs of all indexes with `col` as their only source.
  private def indexesWithSoleSource(scope: ScopeID, col: CollectionID) =
    Index.getUserDefinedBySource(scope, col).map {
      _.flatMap { index =>
        index.sources match {
          case IndexSources.All | IndexSources.Custom =>
            List.empty
          case IndexSources.Limit(allCollections) =>
            if (allCollections.size == 1 && allCollections.contains(col)) {
              List(index.id)
            } else {
              List.empty
            }
        }
      }
    }

  // Checks data changes for violations of protected mode rules.
  private def checkProtectedFields(prev: Data, curr: Data) = {
    val errs = List.newBuilder[SchemaError.Validation]
    // * Cannot decrease or remove history_days.
    val prevHD = prev.getOrElse(Collection.RetainDaysField, None)
    val updHD = curr.getOrElse(Collection.RetainDaysField, None)
    (prevHD, updHD) match {
      case (Some(_), None) =>
        errs += SchemaError.Validation(
          ProtectedModeFailure.RemoveHistoryDays.message)
      case (Some(prev), Some(curr)) if curr < prev =>
        errs += SchemaError.Validation(
          ProtectedModeFailure.DecreaseHistoryDays.message)
      case _ => // Ok.
    }

    // Cannot decrease or add ttl_days.
    val prevTTL = prev.getOrElse(Collection.TTLField, None)
    val updTTL = curr.getOrElse(Collection.TTLField, None)
    (prevTTL, updTTL) match {
      case (None, Some(_)) =>
        errs += SchemaError.Validation(ProtectedModeFailure.AddTTLDays.message)
      case (Some(prev), Some(curr)) if curr < prev =>
        errs += SchemaError.Validation(ProtectedModeFailure.DecreaseTTLDays.message)
      case _ => // Ok.
    }

    errs.result()
  }

  // Validates that the update complies with protected mode,
  // Returns errors for all violations.
  private def checkUpdate(scope: ScopeID, prev: Version.Live, curr: Data) =
    Database.isProtected(scope) map { isProtected =>
      if (isProtected) {
        checkProtectedFields(prev.data, curr)
      } else {
        List.empty[SchemaError.Validation]
      }
    }

  override def write(
    ec: EvalContext,
    id: DocID,
    data: Data,
    isCreate: Boolean,
    pos: Position) = {

    val prevQ = SchemaCollection.Collection(ec.scopeID).get(id.as[CollectionID])
    val insertFn = super.write(ec, id, _, isCreate, pos)

    // FIXME: `(prevQ, insertQ) par { ... }` appears to be violating
    // serial order: the result of prevQ contains the write for insertQ
    prevQ flatMap { prevOpt =>
      val errsQ = prevOpt.fold(Query.value(List.empty[SchemaError.Validation])) {
        prev => checkUpdate(ec.scopeID, prev, data)
      }
      errsQ flatMap { errs =>
        if (errs.nonEmpty) {
          Query.value(Left(errs.map { err =>
            SchemaValidationError(err.message, pos)
          }))
        } else {
          updateMVT(ec.scopeID, id.as[CollectionID], data) flatMap {
            insertFn(_)
          }
        }
      }
    } andThen {
      if (isCreate) {
        Query.incrCollectionCreates()
      } else {
        Query.incrCollectionUpdates()
      }
    }
  }

  override def delete(ec: EvalContext, prev: Version, pos: Position) = {
    val cid = prev.id.as[CollectionID]
    lazy val w = WriteAdaptor(IndexID.collID)

    // This makes a v4 error for protected database. v10 errors are generated before
    // this function is called.
    val protectedQ = Database.isProtected(ec.scopeID).map { isProtected =>
      if (isProtected) {
        Left(List(
          SchemaValidationError(ProtectedModeFailure.DeleteCollection.message, pos)))
      } else {
        Right(())
      }
    }

    val indexQ = indexesWithSoleSource(ec.scopeID, cid).flatMap {
      _.map { id => w.delete(ec, id.toDocID.subID, RootPosition).join }.join.map {
        Right(_)
      }
    }

    protectedQ.flatMap {
      case Left(errs) => Query.value(Left(errs))
      case Right(())  => indexQ.flatMap { _ => super.delete(ec, prev, pos) }
    } andThen { Query.incrCollectionDeletes() }
  }
}

object IndexWriteConfig extends VersionedSchemaWriteConfig(IndexID.collID) {

  // not + 1 because of some page limit nonsense
  val SyncBuildFetchSize = Index.BuildSyncSize + 2
  val config = IndexIOConfig

  override def nextID(ec: EvalContext): Query[SubID] = nextID(ec.scopeID)

  def nextID(scopeID: ScopeID): Query[SubID] = {
    val idQ = IDStore.nextSequentialID(
      scopeID,
      collectionID,
      UserIndexID.MinValue.toLong,
      UserIndexID.MaxValue.toLong)

    idQ flatMap {
      case Some(id) => Query.value(id.subID)
      case None     => Query.fail(MaximumIDException)
    }
  }

  val defaultData = Index.DefaultData

  def versionValidator(ec: EvalContext, sub: SubID) = Index.VersionValidator

  def liveValidator(ec: EvalContext, sub: Option[SubID]) =
    Index.LiveValidator(ec)

  protected def prevForReplace(ec: EvalContext, prev: Data): Data =
    Data(
      SchemaNames.NameField -> prev(SchemaNames.NameField),
      Index.UniqueField -> prev(Index.UniqueField))

  /** Writes an index, and skips the staged schema check. This is required for
    * collection indexes.
    */
  override def writeInternal(
    ec: EvalContext,
    id: DocID,
    data: Data,
    isCreate: Boolean,
    pos: Position) =
    SchemaCollection.Index(ec.scopeID).get(id.as[IndexID]) flatMap {
      case None => Query.True
      case Some(prev) =>
        matchOrThrow(prev.data, data)
        Query.False
    } flatMap { build =>
      if (!build) {
        super.writeInternal(ec, id, data, isCreate, pos)
      } else {
        // Reading the collection for sync builds shouldn't pop rate limits.
        Query.unlimited(fetchForSyncIdxBuild(ec.scopeID, data)) flatMap { changes =>
          val sync = changes.isDefined
          val writeQ = super.writeInternal(ec, id, data, isCreate, pos)

          writeQ flatMapT { _ =>
            // NOTE: if a build is necessary to backfill this index,
            // do so regardless of the value of Index.ActiveField.
            // Users may set active = true to access partial results
            // while a (async) build is running, but that does not
            // change the necessity of executing the build.
            if (sync) {
              val idxQ =
                Index.getUncached(ec.scopeID, id.as[IndexID], forSyncBuild = true)
              val changesQ = Query.value(changes)

              // The sync build shouldn't pop rate limits.
              val build = Query.unlimited {
                (idxQ, changesQ).parT { (idx, changes) =>
                  val entries = changes.map(Store.build(_, idx.indexer))
                  entries.sequence.map(Some(_))
                }
              }

              (build, Query.stats).par { (_, stats) =>
                stats.incr("Index.SynchronousBuilds")

                val activeData = data.patch(Diff(Index.ActiveField -> true))
                super.writeInternal(ec, id, activeData, isCreate = false, pos)
              }
            } else {
              Index.build(ec.scopeID, id.as[IndexID]) flatMap { _ =>
                super.writeInternal(ec, id, data, isCreate = false, pos)
              }
            }
          }
        }
      }
    } recover { case e: ValidationException =>
      Left(List(ValidationError(List(e), pos)))
    }

  override def delete(ec: EvalContext, prev: Version, pos: Position) =
    Database.isProtected(ec.scopeID).flatMap { isProtected =>
      if (isProtected) {
        Query.value(
          Left(
            List(SchemaValidationError(
              ProtectedModeFailure.DeleteBackingIndex.message,
              pos))))
      } else {
        super.delete(ec, prev, pos)
      }
    }

  // Throws an IndexShapeChange exception if `prev` and `proposed` don't define the
  // same index.
  private def matchOrThrow(prev: Data, proposed: Data) = {
    val prevParts = Index.partitionsFromData(prev)
    val proposedParts = Index.partitionsFromData(proposed)

    if (prevParts != proposedParts) {
      throw IndexShapeChange.Partition(prevParts, proposedParts)
    }

    val prevFields = Index.ConfigurationValidator.select(prev)
    val proposedFields = Index.ConfigurationValidator.select(proposed)
    if (!prevFields.sameElements(proposedFields)) {
      throw IndexShapeChange.Fields(prevFields.diffTo(proposedFields))
    }
  }

  private def fetchForSyncIdxBuild(
    scopeID: ScopeID,
    data: Data): Query[Option[Iterable[Version]]] =
    data(Index.SourceField) match {
      case Vector(SourceConfig(IndexSources.Limit(collIDs), _, _)) =>
        val index = NativeIndex.DocumentsByCollection(scopeID)
        // For every collection involved as a source for this index,
        // count all docs. If there's too many, exit early. Otherwise
        // count document events and if there's too many, exit.
        val docsQ = Page.unfold((collIDs.toList, 0)) {
          case (Nil, _) =>
            Query.value((List.empty, None))

          case (_, total) if total > Index.BuildSyncSize =>
            Query.value((List.empty, None))

          case (cid :: cids, total) =>
            // NB: Count events with sorted index instead of historical index
            // because historical index truncates below MVT, which could
            // exclude older documents from the initial build.
            val pageQ =
              Store
                .sortedIndex(
                  index.indexer.config,
                  Vector(IndexTerm(cid)),
                  IndexValue.MaxValue,
                  IndexValue.MinValue,
                  SyncBuildFetchSize,
                  ascending = false
                )
                .takeT(SyncBuildFetchSize)
                .flattenT

            pageQ map { values =>
              (values, Option((cids, values.size + total)))
            }
        }

        docsQ.dropT(Index.BuildSyncSize).isEmptyT flatMap { notTooManyDocs =>
          if (notTooManyDocs) {
            val collMapQ = collIDs
              .map(CollectionConfig.getForSyncIndexBuild(scopeID, _))
              .sequence
              .map(_.collect { case Some(cc) => cc.id -> cc }.toMap)

            val versionsQ = collMapQ.flatMap { colls =>
              docsQ.flatMapValuesT { iTuple =>
                Store
                  .versions(
                    colls(iTuple.docID.collID).Schema,
                    iTuple.docID,
                    VersionID.MaxValue,
                    VersionID.MinValue,
                    SyncBuildFetchSize,
                    reverse = false)
                  .takeT(SyncBuildFetchSize)
                  .flattenT
              }
            }

            versionsQ.dropT(Index.BuildSyncSize).isEmptyT flatMap { canBuildSync =>
              if (canBuildSync) {
                versionsQ.flattenT map { Some(_) }
              } else {
                Query.none
              }
            }
          } else {
            Query.none
          }
        }
      case Vector(SourceConfig(IndexSources.Custom, _, _)) =>
        Collection.getAll(scopeID).isEmptyT map {
          if (_) {
            // HAX: no versions need to be indexed, but this avoids the index
            // build task
            Some(Nil)
          } else {
            None
          }
        }
      case _ =>
        Query.none
    }
}

object UserFunctionWriteConfig
    extends FQLXFieldRetainingWriteConfig(UserFunctionID.collID) {

  def fqlxFields = List(
    SchemaNames.AliasField
  ).map(_.path)

  val config = UserFunctionIOConfig

  val defaultData = Data.empty

  def versionValidator(ec: EvalContext, sub: SubID) =
    UserFunction.VersionValidator

  def liveValidator(ec: EvalContext, sub: Option[SubID]) =
    UserFunction.LiveValidator(ec)

  override def write(
    ec: EvalContext,
    id: DocID,
    data: Data,
    isCreate: Boolean,
    pos: Position) = {

    def validateFQL4UDFBody(body: LambdaWrapper.Src): List[EvalError] = {
      body match {
        case Left(_) =>
          List(
            SchemaValidationError("Invalid type provided for field `body`.", pos)
          )
        case Right(_) => List.empty
      }
    }

    // if a fql4 update is modifying the body we must ensure
    // 1. the body is of the QueryV type not the fqlx string type
    // 2. we remove the internal sig field if it is present
    val newBody = data(UserFunction.BodyField)
    val updatedQ = SchemaCollection
      .UserFunction(ec.scopeID)
      .get(id.as[UserFunctionID])
      .map {
        case Some(prev) =>
          val prevBody = prev.data(UserFunction.BodyField)

          if (newBody != prevBody) {
            val errs = validateFQL4UDFBody(newBody)
            if (errs.nonEmpty) {
              Left(errs)
            } else {
              Right(data.remove(UserFunction.InternalSigField))
            }
          } else {
            Right(data)
          }
        case None =>
          val errs = validateFQL4UDFBody(newBody)
          if (errs.nonEmpty) {
            Left(errs)
          } else {
            Right(data)
          }
      }

    updatedQ flatMap {
      case Left(errs) => Query.value(Left(errs))
      case Right(updatedData) =>
        super.write(ec, id, updatedData, isCreate, pos).andThen {
          if (isCreate) {
            Query.incrFunctionCreates()
          } else {
            Query.incrFunctionUpdates()
          }
        }
    }
  }

  override def delete(ec: EvalContext, prev: Version, pos: Position) = {
    super.delete(ec, prev, pos).andThen { Query.incrFunctionDeletes() }
  }

  protected def prevForReplace(ec: EvalContext, prev: Data): Data =
    Data(SchemaNames.NameField -> prev(SchemaNames.NameField))
}

object RoleWriteConfig extends VersionedSchemaWriteConfig(RoleID.collID) {

  val config = RoleIOConfig

  val defaultData = Data.empty

  def versionValidator(ec: EvalContext, subID: SubID): Validator[Query] =
    Role.VersionValidator

  def liveValidator(ec: EvalContext, sub: Option[SubID]): Validator[Query] =
    Role.LiveValidator(ec)

  protected def prevForReplace(ec: EvalContext, prev: Data): Data =
    Data(SchemaNames.NameField -> prev(SchemaNames.NameField))

  override def write(
    ec: EvalContext,
    id: DocID,
    data: Data,
    isCreate: Boolean,
    pos: Position) =
    super.write(ec, id, data, isCreate, pos) flatMapT { vers =>
      Role.validateMaxRolesPerResource(ec.scopeID, vers.data, pos) map {
        case Right(_)  => Right(vers)
        case Left(err) => Left(List(ValidationError(List(err), pos)))
      }
    } andThen {
      if (isCreate) {
        Query.incrRoleCreates()
      } else {
        Query.incrRoleUpdates()
      }
    }

  override def delete(ec: EvalContext, prev: Version, pos: Position) = {
    super.delete(ec, prev, pos).andThen { Query.incrRoleDeletes() }
  }
}

final class UserDocumentWriteConfig(collectionID: CollectionID)
    extends WriteConfig(collectionID) {

  val config: IOConfig = DocumentIOConfig

  val defaultData = Data.empty

  private val ServerValidator = ServerPasswordValidator(
    List("credentials", "hashed_password"),
    List("credentials", "password"))

  private val ClientValidator = ClientPasswordValidator(
    List("credentials", "hashed_password"),
    List("credentials", "password"),
    List("credentials", "current_password"))

  def versionValidator(ec: EvalContext, sub: SubID) =
    Document.VersionValidator

  def liveValidator(ec: EvalContext, sub: Option[SubID]) = {
    val pwValidator = ec.auth.permissions match {
      case ServerPermissions         => ServerValidator
      case ServerReadOnlyPermissions => ServerValidator
      case _                         => ClientValidator
    }

    Document.LiveValidator(ec) + pwValidator
  }

  // Adjust the TTL of a document based on the collection TTL.
  // Returns true if the collection TTL was applied to the document.
  private def adjustTTL(
    scopeID: ScopeID,
    colID: CollectionID,
    snapshotTS: Timestamp,
    data: Data): Query[Data] = {
    val colTTLOptQ = Collection.get(scopeID, colID) map {
      _.flatMap { col =>
        if (col.config.ttlDuration.isFinite) {
          Some(snapshotTS + col.config.ttlDuration)
        } else {
          None
        }
      }
    }
    colTTLOptQ flatMap { colTTLOpt =>
      (colTTLOpt, data(Version.TTLField)) match {
        case (Some(colTTL), None) =>
          Query.value(data.update((Version.TTLField, Some(colTTL))))
        case (_, Some(_)) =>
          // Record a stat only when the version had its TTL set explicitly.
          val statsQ =
            Database.forScope(scopeID) foreachT { db =>
              Query.stats foreach { stats =>
                val accountIDStr = db.accountID.toLong.toString
                stats.count("VersionTTL.Write", 1, "account_id" -> accountIDStr)
              }
            }
          statsQ map { _ => data }
        case _ => Query.value(data)
      }
    }
  }

  private def customWrite(
    ec: EvalContext,
    id: DocID,
    data: Data,
    isCreate: Boolean,
    pos: Position): Query[R[Version.Live]] = {

    val insertQ =
      adjustTTL(ec.scopeID, collectionID, ec.snapshotTime, data) flatMap { adjData =>
        // we must manually remove the "credentials" object, because it ends up being
        // allowed by Server/ClientValidator's filter mask.
        val d = Data(adjData.fields.remove(List("credentials")))
        super.write(ec, id, d, isCreate, pos)
      }

    insertQ flatMapT { version =>
      val r = data.fields.get(List("credentials")) match {
        case None    => Query(Right(()))
        case Some(d) =>
          // WARNING: escalating privs for credentials
          val auth = EvalAuth(ec.scopeID, ServerPermissions)
          val authEC = ec.copy(auth = auth)

          Credentials.getByDocument(ec.scopeID, id) flatMap {
            case None =>
              val creds = Diff(d.asInstanceOf[MapV])
                .update(Credentials.DocumentField -> id)

              WriteAdaptor(CredentialsID.collID).create(authEC, None, creds, pos)

            case Some(prev) =>
              val creds = Diff(d.asInstanceOf[MapV])

              // XXX: isPartial?
              WriteAdaptor(CredentialsID.collID)
                .update(authEC, prev.docID.subID, creds, true, pos)
          }
      }

      r mapT { _ => version }
    }
  }

  protected def prevForReplace(ec: EvalContext, prev: Data): Data = Data.empty

  override def write(
    ec: EvalContext,
    id: DocID,
    data: Data,
    isCreate: Boolean,
    pos: Position) = {

    // APIVersion.V5+: Verify that the collection existed prior to the current
    // transaction.
    //
    // NB. This implementation benefits from Cache's RYOW bypass in its absence
    // check. If the `Collection.get` API was not cached and preserved the RYOW
    // property, a query creating a collection and a document in the same
    // transaction would never miss.
    val collExistsQ = if (ec.apiVers < APIVersion.V5) {
      Query.value(true)
    } else {
      Collection.get(ec.scopeID, id.collID) map { _.isDefined }
    }

    collExistsQ flatMap {
      case false =>
        Query.value(Left(List(CollectionAndDocumentCreatedInSameTx(id.collID, pos))))
      case true =>
        collectionID match {
          case UserCollectionID(_) => customWrite(ec, id, data, isCreate, pos)
          case _                   => super.write(ec, id, data, isCreate, pos)
        }
    }
  }

  override def insertVersion(
    ec: EvalContext,
    id: DocID,
    validTS: Timestamp,
    action: DocAction,
    data: Data = Data.empty) = {
    val dataQ =
      action match {
        case Delete => Query.value(data)
        case Create | Update =>
          Query.snapshotTime flatMap {
            adjustTTL(ec.scopeID, id.collID, _, data)
          }
      }

    dataQ flatMap { super.insertVersion(ec, id, validTS, action, _) }
  }
}

object KeyWriteConfig {
  object Default extends KeyWriteConfig(KeyIOConfig)
  object Move extends KeyWriteConfig(MoveDatabaseIOConfig)
}

sealed abstract class KeyWriteConfig(val config: IOConfig)
    extends WriteConfig(KeyID.collID) {

  val defaultData = Data.empty

  override def versionValidator(ec: EvalContext, sub: SubID) =
    Key.VersionValidator + Key.SecretValidator(
      ec.scopeID,
      Some(GlobalKeyID(sub.toLong)))

  override def liveValidator(ec: EvalContext, sub: Option[SubID]) =
    Key.LiveValidator(ec) +
      Key.SecretValidator(ec.scopeID, sub map { case SubID(l) => GlobalKeyID(l) })

  override protected def prevForReplace(ec: EvalContext, prev: Data) =
    Data(Key.HashField -> prev(Key.HashField))

  override def write(
    ec: EvalContext,
    id: DocID,
    data: Data,
    isCreate: Boolean,
    pos: Position) = {
    val globalID = GlobalKeyID(id.as[KeyID].toLong)

    val toDisplay = if (data(Key.HashField).isDefined) {
      data
    } else {
      val kl = KeyLike(globalID)
      data.update(
        Key.SecretField -> Some(kl.toBase64),
        Key.HashField -> Some(kl.hashedSecret))
    }

    val toStore = toDisplay.update(Key.SecretField -> None)

    PublicCollection.Key(ec.scopeID).get(id.as[KeyID]) flatMap { prev =>
      super.write(ec, id, toStore, isCreate, pos) flatMapT { v =>
        val diff = prev map { p => v.data.diffTo(p.data) }
        val v0 = v.withDiff(diff)
        val lookups = LookupHelpers.lookups(v0)
        val addsQ = lookups map { LookupStore.add(_) }

        addsQ.join map { _ => Right(v0.withData(toDisplay)) }
      }
    } andThen {
      if (isCreate) {
        Query.incrKeyCreates()
      } else {
        Query.incrKeyUpdates()
      }
    }
  }

  override def delete(ec: EvalContext, prev: Version, pos: Position) =
    super.delete(ec, prev, pos) flatMapT { v =>
      val lookup = LiveLookup(
        GlobalKeyID(v.id.as[KeyID].toLong),
        ec.scopeID,
        v.id,
        v.ts,
        Remove)
      LookupStore.add(lookup) map { _ => Right(v) }
    } andThen { Query.incrKeyDeletes() }
}

object AccessProviderWriteConfig
    extends VersionedSchemaWriteConfig(AccessProviderID.collID) {

  val defaultData = Data.empty

  override val config = AccessProviderIOConfig

  override def versionValidator(ec: EvalContext, sub: SubID) =
    AccessProvider.VersionValidator

  override def liveValidator(ec: EvalContext, sub: Option[SubID]) =
    AccessProvider.LiveValidator(ec, sub)

  override protected def prevForReplace(ec: EvalContext, prev: Data) =
    Data(
      SchemaNames.NameField -> prev(SchemaNames.NameField),
      AccessProvider.IssuerField -> prev(AccessProvider.IssuerField),
      AccessProvider.JwksUriField -> prev(AccessProvider.JwksUriField),
      AccessProvider.RolesField -> prev(AccessProvider.RolesField)
    )

  override def write(
    ec: EvalContext,
    id: DocID,
    data: Data,
    isCreate: Boolean,
    pos: Position) = {

    val db = Database.forScope(ec.scopeID)
    db flatMap {
      case None => Query.fail(new RuntimeException("FIXME:"))
      case Some(db) =>
        val globalID = JWTToken.canonicalDBUrl(db)
        val patched = data.update(AccessProvider.AudienceField -> globalID)
        super.write(ec, id, patched, isCreate, pos)
    }
  }
}

object TokenWriteConfig extends WriteConfig(TokenID.collID) {

  val defaultData = Data.empty

  override val config = TokenIOConfig

  override def versionValidator(ec: EvalContext, sub: SubID) =
    Document.VersionValidator + Token.Validator

  override def liveValidator(ec: EvalContext, sub: Option[SubID]) =
    Document.LiveValidator(ec) + Token.Validator

  protected def prevForReplace(ec: EvalContext, prev: Data): Data =
    Data.empty

  override def write(
    ec: EvalContext,
    id: DocID,
    data: Data,
    isCreate: Boolean,
    pos: Position) = {

    val rawQ = if (data(Token.HashField).isDefined) {
      Query.some(data)
    } else {
      // FIXME: instead of lookup the database we could use
      // ec.auth.database,
      // but unfortunately Auth.forScope() always use RootDatabase breaking most of
      // the tests.
      Database.forScope(ec.scopeID) mapT { db =>
        val tl = TokenLike(id.as[TokenID], db.globalID)
        data.update(
          Token.SecretField -> Some(tl.toBase64),
          Token.HashField -> Some(tl.hashedSecret),
          Token.GlobalIDField -> db.globalID)
      }
    }

    rawQ flatMap {
      case Some(raw) =>
        val toStore = raw.update(Token.SecretField -> None)
        val toDisplay = raw.update(Token.HashField -> None)
        super.write(ec, id, toStore, isCreate, pos) mapT { _.withData(toDisplay) }
      case _ =>
        throw new IllegalStateException(
          "The database is gone before creating the token.")
    }
  }
}

object CredentialsWriteConfig extends WriteConfig(CredentialsID.collID) {

  val defaultData = Data.empty

  override val config = CredentialsIOConfig

  override def versionValidator(ec: EvalContext, sub: SubID) =
    Credentials.VersionValidator

  override def liveValidator(ec: EvalContext, sub: Option[SubID]) = {
    val pwValidator = ec.auth.permissions match {
      case ServerPermissions         => Credentials.ServerValidator
      case ServerReadOnlyPermissions => Credentials.ServerValidator
      case _                         => Credentials.ClientValidator
    }

    Document.LiveValidator(ec) +
      Credentials.DocumentField.validator +
      pwValidator
  }

  protected def prevForReplace(ec: EvalContext, prev: Data): Data =
    Data.empty
}

object WriteAdaptor {

  def apply(id: CollectionID): WriteAdaptor =
    id match {
      case DatabaseID.collID       => new WriteAdaptor(DatabaseWriteConfig.Default)
      case KeyID.collID            => new WriteAdaptor(KeyWriteConfig.Default)
      case TokenID.collID          => new WriteAdaptor(TokenWriteConfig)
      case CollectionID.collID     => new WriteAdaptor(CollectionWriteConfig)
      case IndexID.collID          => new WriteAdaptor(IndexWriteConfig)
      case CredentialsID.collID    => new WriteAdaptor(CredentialsWriteConfig)
      case UserFunctionID.collID   => new WriteAdaptor(UserFunctionWriteConfig)
      case RoleID.collID           => new WriteAdaptor(RoleWriteConfig)
      case AccessProviderID.collID => new WriteAdaptor(AccessProviderWriteConfig)
      case UserCollectionID(id) => new WriteAdaptor(new UserDocumentWriteConfig(id))
      case id: CollectionID =>
        throw new IllegalArgumentException(s"Unknown collection $id.")
    }
}

class WriteAdaptor(logic: WriteConfig) {

  private def liftPermissionDenied(scope: ScopeID, id: DocID, pos: Position)(
    allowed: Boolean): R[Unit] = {

    if (allowed) {
      Right(())
    } else {
      val ref = RefL(scope, id)
      Left(List(PermissionDenied(Right(ref), pos)))
    }
  }

  private def checkCreatePermission(
    ec: EvalContext,
    newVersion: Version.Live,
    pos: Position): Query[R[Unit]] = {

    ec.auth.checkCreatePermission(ec.scopeID, newVersion) map
      liftPermissionDenied(ec.scopeID, newVersion.id, pos)
  }

  private def checkCreateWithIDPermission(
    ec: EvalContext,
    newVersion: Version.Live,
    pos: Position): Query[R[Unit]] = {

    ec.auth.checkCreateWithIDPermission(ec.scopeID, newVersion) map
      liftPermissionDenied(ec.scopeID, newVersion.id, pos)
  }

  private def checkDeletePermission(
    ec: EvalContext,
    sub: SubID,
    pos: Position): Query[R[Unit]] = {

    val id = DocID(sub, logic.collectionID)

    ec.auth.checkDeletePermission(ec.scopeID, id) map
      liftPermissionDenied(ec.scopeID, id, pos)
  }

  private def checkWritePermission(
    ec: EvalContext,
    sub: SubID,
    prev: Version.Live,
    diff: Diff,
    pos: Position): Query[R[Unit]] = {

    val id = DocID(sub, logic.collectionID)

    ec.auth.checkWritePermission(ec.scopeID, id, prev, diff) map
      liftPermissionDenied(ec.scopeID, id, pos)
  }

  private def checkHistoryWritePermission(
    ec: EvalContext,
    sub: SubID,
    ts: Timestamp,
    action: DocAction,
    data: Diff,
    pos: Position): Query[R[Unit]] = {

    val id = DocID(sub, logic.collectionID)

    ec.auth.checkHistoryWritePermission(ec.scopeID, id, ts, action, data) map
      liftPermissionDenied(ec.scopeID, id, pos)
  }

  def getID(ec: EvalContext, sub: Option[SubID]): Query[DocID] =
    sub map { Query(_) } getOrElse logic.nextID(ec) map {
      DocID(_, logic.collectionID)
    }

  def create(
    ec: EvalContext,
    sub: Option[SubID],
    fullDiff: Diff,
    pos: Position): Query[R[VersionL]] = {

    val diff = logic.config.writeableDiff(fullDiff, isCreate = true)

    val precheckQ = if (logic.isCreatable) {
      sub match {
        case Some(subID) =>
          val ver = Version.Live(
            ec.scopeID,
            DocID(subID, logic.collectionID),
            // NB: This is not used in the actual write.
            SchemaVersion.Min,
            Data(diff.fields))
          val permQ = checkCreateWithIDPermission(ec, ver, pos)

          permQ flatMapT { _ =>
            val id = DocID(subID, logic.collectionID)
            RuntimeEnv.Default.Store(ec.scopeID).get(id) map {
              case Some(_) => Left(List(InstanceAlreadyExists(id, pos)))
              case None    => Right(subID)
            }
          }

        case None =>
          logic.nextID(ec).flatMap { subID =>
            val ver = Version.Live(
              ec.scopeID,
              DocID(subID, logic.collectionID),
              // NB: This is not used in the actual write.
              SchemaVersion.Min,
              Data(diff.fields))
            checkCreatePermission(ec, ver, pos).mapT { _ => subID }
          }
      }
    } else {
      Query(Left(List(InvalidCreateClassArgument(logic.collectionID, pos))))
    }

    precheckQ flatMapT { subID =>
      Database.forScope(ec.scopeID) flatMap {
        case Some(db) if db.isContainer && !logic.config.allowedInContainer =>
          Query.value(Left(List(InvalidObjectInContainer(pos))))
        case _ =>
          write(
            ec,
            Some(subID),
            diff,
            logic.defaultData,
            isCreate = true,
            pos) mapT {
            _.copy(isNew = true)
          }
      }
    } flatMapT { v =>
      Query.incrCreates() map { _ =>
        Right(v)
      }
    }
  }

  def update(
    ec: EvalContext,
    sub: SubID,
    fullDiff: Diff,
    isPartial: Boolean,
    pos: Position): Query[R[VersionL]] = {

    val verb = if (isPartial) "update" else "replace"

    if (logic.isUpdatable(sub)) {
      val diff = logic.config.writeableDiff(fullDiff, isCreate = false)
      val prevQ = logic.prevVersions(ec, sub, isPartial, pos at verb)

      prevQ flatMapT { case (originalVersion, filteredVersion) =>
        checkWritePermission(ec, sub, originalVersion, diff, pos) flatMapT { _ =>
          write(ec, Some(sub), diff, filteredVersion.data, isCreate = false, pos)
        }
      } flatMapT { v =>
        Query.incrUpdates() map { _ =>
          Right(v)
        }
      }
    } else {
      val id = DocID(sub, logic.collectionID)
      Query(Left(List(InvalidUpdateRefArgument(id, pos at verb))))
    }
  }

  // FIXME: This is a terrible hack to get around problems of
  // symbolization relying on the cache, when sometimes we don't, such
  // as for schema modifications.
  private def resymbolizeSchema(ec: EvalContext, map: MapV): Query[MapV] = {
    val ids = map collectValues {
      case (_, DocIDV(CollectionID(id)))   => id.toDocID
      case (_, DocIDV(IndexID(id)))        => id.toDocID
      case (_, DocIDV(UserFunctionID(id))) => id.toDocID
    }

    (ids map { id =>
      RuntimeEnv.Default.Store(ec.scopeID).isDeleted(id) flatMap {
        case false => Query.none
        case true =>
          RuntimeEnv.Default.Store(ec.scopeID).versions(id) findValueT { v =>
            !v.isDeleted
          } flatMapT { v =>
            val cfg = v.collID match {
              case IndexID.collID      => NativeIndex.IndexByName(ec.scopeID)
              case CollectionID.collID => NativeIndex.CollectionByName(ec.scopeID)
              case UserFunctionID.collID =>
                NativeIndex.UserFunctionByName(ec.scopeID)
              case _ =>
                throw new AssertionError(s"unexpected collection ${v.collID}")
            }

            Store.uniqueIDForKey(
              cfg,
              Vector(IndexTerm(StringV(v.data(SchemaNames.NameField).toString))),
              Timestamp.MaxMicros) mapT { newID =>
              id -> newID
            }
          }
      }
    }).sequence map { lookups =>
      val m = lookups.flatten.toMap

      map transformValues { case DocIDV(id) =>
        DocIDV(m.getOrElse(id, id))
      }
    }
  }

  private def write(
    ec: EvalContext,
    sub: Option[SubID],
    diff: Diff,
    prev: Data,
    isCreate: Boolean,
    pos: Position): Query[R[VersionL]] = {

    if (ec.validTimeOverride.isDefined) {
      Query.value(Left(List(InvalidWriteTime(pos))))
    } else {
      val validatorQ = Query(Right(logic.liveValidator(ec, sub)))
      val idQ = getID(ec, sub) map { Right(_) }
      val resymbolizedDiffQ = resymbolizeSchema(ec, diff.fields) map { m =>
        Right(Diff(m))
      }

      (validatorQ, idQ, resymbolizedDiffQ) parT { (validator, id, diff) =>
        liftPatch(validator.patch(prev, diff), pos) flatMapT { data =>
          logic.write(ec, id, data, isCreate, pos) mapT { VersionL(_) }
        }
      }
    }
  }

  def delete(ec: EvalContext, sub: SubID, pos: Position): Query[R[VersionL]] = {
    val id = DocID(sub, logic.collectionID)

    val validQ = Query {
      if (logic.isDeletable(sub)) {
        Right(())
      } else {
        Left(List(InvalidDeleteRefArgument(id, pos at "delete")))
      }
    }

    val permQ = checkDeletePermission(ec, sub, pos)
    val prevQ = RuntimeEnv.Default.Store(ec.scopeID).get(id) map {
      _ toRight List(InstanceNotFound(Right(RefL(ec.scopeID, id)), pos))
    }

    (prevQ, validQ, permQ) parT { (prev, _, _) =>
      logic.delete(ec, prev, pos) mapT { _ =>
        VersionL(prev)
      }
    } flatMapT { v =>
      Query.incrDeletes() map { _ =>
        Right(v)
      }
    }
  }

  def insertVersion(
    ec: EvalContext,
    sub: SubID,
    validTS: Timestamp,
    action: DocAction,
    fullDiff: Diff,
    pos: Position): Query[R[DocEventL]] = {

    val diff = logic.config.writeableDiff(fullDiff, isCreate = false)
    val id = DocID(sub, logic.collectionID)

    val permQ = checkHistoryWritePermission(ec, sub, validTS, action, diff, pos)
    val validatorQ = Query(Right(logic.versionValidator(ec, sub)))
    val resymbolizedDiffQ = resymbolizeSchema(ec, diff.fields) map { m =>
      Right(Diff(m))
    }

    val versionQ = (permQ, validatorQ, resymbolizedDiffQ) parT {
      (_, validator, diff) =>
        action match {
          case Delete =>
            liftCV(logic.insertVersion(ec, id, validTS, action), pos, false, false)
          case Create | Update =>
            liftPatch(validator.patch(Data.empty, diff), pos) flatMapT { data =>
              liftCV(
                logic.insertVersion(ec, id, validTS, action, data),
                pos,
                false,
                false)
            }
        }
    }

    versionQ flatMapT { v =>
      Query.incrInserts() map { _ =>
        Right(DocEventL(v))
      }
    }
  }

  def removeVersion(
    ec: EvalContext,
    sub: SubID,
    validTS: Timestamp,
    action: DocAction,
    pos: Position): Query[R[Literal]] = {

    val id = DocID(sub, logic.collectionID)
    val versionID = VersionID(validTS, action)

    // storage conflict resolution only allows one version per timestamp, so assume
    // at this level that we'll get the correct one for the timestamp.
    val permQ =
      RuntimeEnv.Default.Store(ec.scopeID).getVersion(id, validTS).flatMap { vOpt =>
        val fields = vOpt.map(_.data.fields).getOrElse(MapV.empty)
        checkHistoryWritePermission(ec, sub, validTS, action, Diff(fields), pos)
      }

    permQ flatMapT { _ =>
      logic.removeVersion(ec, id, versionID) map { _ =>
        Right(NullL)
      }
    } flatMap { v =>
      Query.incrRemoves() map { _ => v }
    }
  }

  // helpers

  def liftPatch(
    patch: Query[Either[List[ValidationException], Data]],
    pos: Position): Query[R[Data]] = {

    patch mapLeftT { errs =>
      List(ValidationError(errs, pos))
    }
  }
}
