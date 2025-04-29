package fauna.model.schema

import fauna.atoms._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model._
import fauna.model.runtime.fql2.{ FQLInterpCtx, FQLInterpreter }
import fauna.model.schema._
import fauna.model.schema.index.CollectionIndex
import fauna.model.schema.index.CollectionIndex.Status
import fauna.model.schema.index.CollectionIndexManager
import fauna.model.schema.index.NativeCollectionIndex
import fauna.model.schema.index.UniqueConstraintIndex
import fauna.model.schema.index.UserDefinedIndex
import fauna.model.schema.TypeEnvValidator
import fauna.model.tasks.MigrationTask
import fauna.model.Database
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.repo.schema.{ CollectionSchema, DataMode }
import fauna.repo.schema.migration.Migration
import fauna.repo.store.CacheStore
import fauna.repo.Store
import fauna.storage.index.IndexTerm
import fql.ast.display._
import fql.typer.Typer

sealed trait SchemaItemView[+A] {
  import SchemaItemView._

  def active: Option[A] = this match {
    case Unchanged(v)   => Some(v)
    case Created(_)     => None
    case Updated(v1, _) => Some(v1)
    case Deleted(v)     => Some(v)
  }

  def staged: Option[A] = this match {
    case Unchanged(v)   => Some(v)
    case Created(v)     => Some(v)
    case Updated(_, v2) => Some(v2)
    case Deleted(_)     => None
  }

  def mapSimplified[B](fn: A => B): SchemaItemView[B] =
    this match {
      case Unchanged(v)    => Unchanged(fn(v))
      case Created(v)      => Created(fn(v))
      case Deleted(v)      => Deleted(fn(v))
      case Updated(v1, v2) => Updated(fn(v1), fn(v2))
    }

  def flatMapSimplified[B](fn: A => Query[B]): Query[SchemaItemView[B]] =
    this match {
      case Unchanged(v) => fn(v).map(Unchanged(_))
      case Created(v)   => fn(v).map(Created(_))
      case Deleted(v)   => fn(v).map(Deleted(_))
      case Updated(v1, v2) =>
        (fn(v1), fn(v2)).par((v1, v2) => Query.value(Updated(v1, v2)))
    }
}

object SchemaItemView {
  final case class Unchanged[+A](v: A) extends SchemaItemView[A]
  final case class Created[+A](v: A) extends SchemaItemView[A]
  final case class Updated[+A](v1: A, v2: A) extends SchemaItemView[A]
  final case class Deleted[+A](v: A) extends SchemaItemView[A]

  def apply[A](v1: Option[A], v2: Option[A]) = (v1, v2) match {
    case (Some(v1), Some(v2)) if v1 == v2 => Unchanged(v1)
    case (Some(v1), Some(v2))             => Updated(v1, v2)
    case (None, Some(v2))                 => Created(v2)
    case (Some(v1), None)                 => Deleted(v1)
    case (None, None)                     => sys.error("unreachable")
  }

  def unapply[A](v: SchemaItemView[A]) = Some((v.active, v.staged))
}

// This is quite similar to SchemaItemView, but the `Created` and `Updated`
// classes store an `Option[A]`.
sealed trait OptionalSchemaItemView[+A] {
  import OptionalSchemaItemView._

  def active: Option[A] = this match {
    case Unchanged(v)   => Some(v)
    case Created(_)     => None
    case Updated(v1, _) => Some(v1)
    case Deleted(v)     => Some(v)
  }

  def staged: Option[A] = this match {
    case Unchanged(v)   => Some(v)
    case Created(v)     => v
    case Updated(_, v2) => v2
    case Deleted(_)     => None
  }
}

object OptionalSchemaItemView {
  final case class Unchanged[+A](v: A) extends OptionalSchemaItemView[A]
  final case class Created[+A](v: Option[A]) extends OptionalSchemaItemView[A]
  final case class Updated[+A](v1: A, v2: Option[A])
      extends OptionalSchemaItemView[A]
  final case class Deleted[+A](v: A) extends OptionalSchemaItemView[A]

  def unapply[A](v: OptionalSchemaItemView[A]) = Some((v.active, v.staged))
}

final case class SchemaStatus(
  activeSchemaVersion: Option[SchemaVersion]
) {
  def hasStaged = activeSchemaVersion.isDefined
}

final case class StagedIndex(coll: String, index: CollectionIndex) {
  def schemaStatus = index.status match {
    case Status.Building => SchemaIndexStatus.Pending
    case Status.Complete => SchemaIndexStatus.Ready
    case Status.Failed   => SchemaIndexStatus.Failed
  }

  def description = index match {
    case i: UniqueConstraintIndex =>
      s"unique constraint on ${i.terms.map(_.toConfig.display).mkString("[", ", ", "]")}"
    case i: UserDefinedIndex      => s"index ${i.name}"
    case _: NativeCollectionIndex => sys.error("unreachable")
  }
}

sealed trait SchemaIndexStatus {
  def asStr = this match {
    case SchemaIndexStatus.Pending => "pending"
    case SchemaIndexStatus.Ready   => "ready"
    case SchemaIndexStatus.Failed  => "failed"
  }

  def &(that: SchemaIndexStatus): SchemaIndexStatus = (this, that) match {
    case (SchemaIndexStatus.Failed, _)  => SchemaIndexStatus.Failed
    case (_, SchemaIndexStatus.Failed)  => SchemaIndexStatus.Failed
    case (SchemaIndexStatus.Pending, _) => SchemaIndexStatus.Pending
    case (_, SchemaIndexStatus.Pending) => SchemaIndexStatus.Pending
    case _                              => SchemaIndexStatus.Ready
  }
}

object SchemaIndexStatus {
  def fromStr(s: String): Option[SchemaIndexStatus] = s match {
    case "pending" => Some(SchemaIndexStatus.Pending)
    case "ready"   => Some(SchemaIndexStatus.Ready)
    case "failed"  => Some(SchemaIndexStatus.Failed)
    case _         => None
  }

  final case object Pending extends SchemaIndexStatus
  final case object Ready extends SchemaIndexStatus
  final case object Failed extends SchemaIndexStatus

  def forScope(scope: ScopeID): Query[Option[SchemaIndexStatus]] =
    Cache.schemaIndexStatusByScope(scope)

  def forScopeUncached(scope: ScopeID): Query[Option[SchemaIndexStatus]] = {
    SchemaStatus.forScope(scope).flatMap { status =>
      if (status.activeSchemaVersion.isDefined) {
        parseIndexStatus(scope).map { indexes =>
          Some(indexes.foldLeft(Ready: SchemaIndexStatus)(_ & _.schemaStatus))
        }
      } else {
        Query.none
      }
    }
  }

  def parseIndexStatus(scope: ScopeID): Query[Seq[StagedIndex]] = {
    CollectionID.getAllUserDefined(scope).flattenT.flatMap { collections =>
      collections
        .map { coll =>
          Cache.collByID(scope, coll).map { viewOpt =>
            val view = viewOpt.get

            (view.active, view.staged) match {
              case (active, Some(staged)) =>
                val activeIDs = active.fold(Set.empty[IndexID]) {
                  _.collIndexes.map(_.indexID).toSet
                }

                staged.collIndexes.view
                  // We only care about staged indexes.
                  .filter { index => !activeIDs.contains(index.indexID) }
                  .map { index => StagedIndex(staged.name, index) }

              case _ => Seq.empty
            }
          }
        }
        .sequence
        .map(_.flatten)
    }
  }
}

object SchemaStatus {

  type VersionView = SchemaItemView[Version.Live]

  def forScope(scope: ScopeID): Query[SchemaStatus] =
    Database.forScope(scope).map { db =>
      SchemaStatus(db.flatMap(_.activeSchemaOverride))
    }

  def forScopeUncached(scope: ScopeID): Query[SchemaStatus] =
    Database.getUncached(scope).map { db =>
      SchemaStatus(db.flatMap(_.activeSchemaOverride))
    }

  /** Only public for testing. Use `commit` or `abandon` instead.
    */
  def clearActiveSchema(scope: ScopeID): Query[Unit] =
    setActiveSchema(scope, None)

  /** Only public for testing. Use `pin` instead.
    */
  def pinActiveSchema(scope: ScopeID, vers: SchemaVersion) =
    setActiveSchema(scope, Some(vers))

  private def setActiveSchema(
    scope: ScopeID,
    vers: Option[SchemaVersion]): Query[Unit] =
    Database.forScope(scope).flatMap {
      case None =>
        throw new IllegalStateException(s"No database for scope $scope")
      case Some(db) =>
        CacheStore.updateSchemaVersion(scope).flatMap { _ =>
          Database.setActiveSchemaVersion(db.parentScopeID, db.id, vers)
        }
    }

  def activeOrSnapshot(scope: ScopeID): Query[Timestamp] =
    for {
      status <- forScope(scope)
      snap   <- Query.snapshotTime
    } yield status.activeSchemaVersion match {
      case Some(vers) => vers.ts
      case None       => snap
    }

  def getVersState(
    coll: CollectionSchema,
    id: DocID): Query[Option[SchemaStatus.VersionView]] = {
    getAbstract(coll) { validTS: Timestamp =>
      Store
        .getVersionNoTTL(coll, id, validTS)
        .collectT { case v: Version.Live => Query.some(v) }
    } { (a, b) => a.versionID == b.versionID }
  }

  def getAbstract[T](coll: CollectionSchema)(get: Timestamp => Query[Option[T]])(
    equal: (T, T) => Boolean): Query[Option[SchemaItemView[T]]] =
    (Query.snapshotTime, forScope(coll.scope)).par { (snap, status) =>
      val active = status.activeSchemaVersion match {
        case Some(sv) => sv.ts
        case None     => snap
      }

      (get(active), get(Timestamp.MaxMicros)).par {
        case (Some(active), Some(staged)) =>
          if (equal(active, staged)) {
            Query.some(SchemaItemView.Unchanged(active))
          } else {
            Query.some(SchemaItemView.Updated(active, staged))
          }

        case (None, Some(staged)) =>
          Query.some(SchemaItemView.Created(staged))

        case (Some(active), None) =>
          Query.some(SchemaItemView.Deleted(active))

        case (None, None) => Query.none
      }
    }

  /** Pins the schema at the snapshot time as the active schema version. Does
    * nothing if there is already a pinned schema version.
    */
  def pin(ctx: FQLInterpCtx): Query[SchemaVersion] = {
    SchemaStatus.forScope(ctx.scopeID).flatMap { status =>
      status.activeSchemaVersion match {
        case Some(vers) => Query.value(vers)
        case None =>
          for {
            // Pull a new schema version from the ether.
            version <- Query.snapshotTime.map(SchemaVersion(_))
            _       <- SchemaStatus.pinActiveSchema(ctx.scopeID, version)
          } yield version
      }
    }
  }

  /** Commits the staged schema in the given `ctx`.
    */
  def commit(ctx: FQLInterpCtx): Query[Result[Unit]] = {
    SchemaIndexStatus.forScope(ctx.scopeID).flatMap {
      case Some(SchemaIndexStatus.Ready) =>
        SchemaSource.stagedFSLFiles(ctx.scopeID).flatMap { staged =>
          SchemaManager
            .validate(ctx.scopeID, staged)
            .flatMapT { _ =>
              for {
                _ <- runCollectionUpdate(ctx) { (coll, migrations) =>
                  for {
                    _ <- CollectionIndexManager.commit(ctx.scopeID, coll, migrations)
                    _ <-
                      if (migrations.nonEmpty) {
                        MigrationTask.updateFromCommit(ctx.scopeID, coll)
                      } else {
                        Query.unit
                      }
                  } yield ()
                }

                _ <- SchemaStatus.clearActiveSchema(ctx.scopeID)
              } yield Result.Ok(())
            }
        }

      case Some(SchemaIndexStatus.Pending) =>
        SchemaError
          .Validation(
            "Cannot commit schema before it is in the `ready` state. Check schema status for details.")
          .toQuery

      case Some(SchemaIndexStatus.Failed) =>
        // I'm intentionally leaving this vauge, as this is typically a Bad Thing.
        // This could be the result of an index that is too complex, but usually an
        // operator would be enganged at this point. So I've left out any suggestion
        // to retry the push here, as we probably don't want that.
        SchemaError
          .Validation(
            "Cannot commit schema in the `failed` state. Check schema status for details.")
          .toQuery

      case None =>
        SchemaError.Validation("There is no staged schema to commit").toQuery
    }
  }

  /** Abandons the staged schema in the given `ctx`.
    */
  def abandon(ctx: FQLInterpCtx): Query[Result[Unit]] =
    SchemaStatus.forScope(ctx.scopeID).flatMap { status =>
      if (status.hasStaged) {
        for {
          // NB: The Collection collection is reverted entirely by
          // CollectionIndexManager, not by `revertSchema`.
          _ <- runCollectionUpdate(ctx) { (coll, migrations) =>
            CollectionIndexManager.abandon(ctx.scopeID, coll, migrations)
          }

          _ <- revertSchema(ctx)
          _ <- SchemaStatus.clearActiveSchema(ctx.scopeID)
        } yield Result.Ok(())
      } else {
        SchemaError.Validation("There is no staged schema to abandon").toQuery
      }
    }

  type CollectionUpdateFn = (CollectionID, Seq[Migration]) => Query[Unit]

  private def runCollectionUpdate(ctx: FQLInterpCtx)(
    f: CollectionUpdateFn): Query[Unit] = {
    val typeEnvValidator =
      TypeEnvValidator(
        ctx.scopeID,
        changed = Map.empty,
        stackTrace = FQLInterpreter.StackTrace.empty)

    for {
      collections <- CollectionID.getAllUserDefined(ctx.scopeID).flattenT

      res <- typeEnvValidator.runWithTyper { (typer, _) =>
        collections
          .map { updateCollection(ctx, typer, _, f) }
          .sequence
          .map { _ => (Nil, Nil) }
      }
      _ = if (res.isErr) {
        throw new IllegalStateException(s"commit failed in ${ctx.scopeID}: $res")
      }
    } yield ()
  }

  private def updateCollection(
    ctx: FQLInterpCtx,
    typer: Typer,
    coll: CollectionID,
    f: CollectionUpdateFn): Query[Unit] = {
    for {
      status <- SchemaStatus.forScopeUncached(ctx.scopeID)

      migrations <- MigrationDeriver.derive(
        ctx.scopeID,
        typer,
        coll,
        status.activeSchemaVersion.get.ts,
        Timestamp.MaxMicros)

      _ <- f(coll, migrations.newMigrations)
    } yield ()
  }

  // NB: This does not revert the Collection collection! CollectionIndexManager
  // reverts that.
  private def revertSchema(ctx: FQLInterpCtx): Query[Unit] = {
    for {
      active <- SchemaStatus.forScope(ctx.scopeID).map(_.activeSchemaVersion.get)

      _ <- revertCollection(active.ts, InternalCollection.SchemaSource(ctx.scopeID))
      _ <- revertCollection(active.ts, SchemaCollection.UserFunction(ctx.scopeID))
      _ <- revertCollection(active.ts, SchemaCollection.Role(ctx.scopeID))
      _ <- revertCollection(active.ts, SchemaCollection.AccessProvider(ctx.scopeID))
    } yield ()
  }

  private def revertCollection[I <: ID[I]: CollectionIDTag](
    activeTS: Timestamp,
    collQ: Query[CollectionConfig.WithID[I]]): Query[Unit] = {
    for {
      coll <- collQ

      terms = Vector(IndexTerm(coll.id.toDocID))
      idx = NativeIndex.ChangesByCollection(coll.parentScopeID)

      // Read sorted index directly, so that deleted docs get picked up.
      changed <- Store
        .sortedIndex(idx, terms)
        .takeWhileT(_.ts.validTS > activeTS)
        .flattenT

      _ <- changed.map { entry =>
        val id = entry.docID.as[I]

        for {
          active <- coll.get(id, activeTS)
          staged <- coll.get(id)
          _ <- (staged, active) match {
            // Doc was created in the staged update, delete it.
            case (Some(_), None) => coll.internalDelete(id)

            // Doc was updated in the staged update, revert it.
            case (Some(_), Some(active)) => coll.internalReplace(id, active.data)

            // Doc was deleted in the staged update, recreate it.
            //
            // NB: This path will only be hit for `SchemaSource`, as all other schema
            // items cannot be deleted, as that would break name lookups. Once this
            // is supported for schema documents, it will need to use
            // `internalCreate` instead of `create`, so that the internal fields are
            // written correctly.
            case (None, Some(active)) =>
              coll.create(id, DataMode.Default, active.data)

            // Doc was written multiple times, but the result is the same. Do
            // nothing.
            case (None, None) => Query.unit
          }
        } yield ()
      }.sequence
    } yield ()
  }
}
