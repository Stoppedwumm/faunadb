package fauna.model.schema

import fauna.ast.CollectionWriteConfig
import fauna.atoms.{ CollectionID, DocID, IndexID, ScopeID }
import fauna.lang.syntax._
import fauna.model.{ Collection, Database, Index }
import fauna.model.schema.index.{ CollectionIndex, CollectionIndexManager }
import fauna.model.tasks.MigrationTask
import fauna.repo.query.Query
import fauna.repo.schema.{ CollectionSchema, ConstraintFailure, Path, WriteHook }
import fauna.repo.Store
import fauna.storage.doc.{ Data, Diff }
import fauna.storage.index.IndexSources
import fql.ast.display._
import scala.collection.mutable.Builder

/** Hooks that are called when a collection is created, updated, or deleted.
  * - Manage FQL2 indexes on the collections. These indexes have an associated
  *   underlying index document that allows them to hook into the existing
  *   indexing system.  This object is used to manage the underlying indexes
  *   associated with a collection level index.
  * - Cancel migration tasks when a collection is deleted.
  * - If on, enforce protected mode: no deleting backing indexes.
  * - Validate that check constraint names are unique.
  * - Validate there are no redundant unique constraints.
  * - Validate that unique constraints have terms.
  * - Validate computed and defined fields don't share names.
  */
object CollectionSchemaHooks {

  val writeHook = WriteHook {
    case (schema, WriteHook.OnCreate(id, updated)) =>
      processUpdate(schema, id, Data.empty, updated) andThen {
        Query.incrCollectionCreates()
      }
    case (schema, WriteHook.OnUpdate(id, prev, updated)) =>
      processUpdate(schema, id, prev, updated) andThen {
        Query.incrCollectionUpdates()
      }
    case (schema, WriteHook.OnDelete(id, _)) =>
      processDelete(schema, id) andThen { Query.incrCollectionDeletes() }
  }

  def processDelete(
    schema: CollectionSchema,
    docID: DocID): Query[Seq[ConstraintFailure]] =
    for {
      errs <- deleteCollectionIndexes(schema, docID)
      // No need to check the feature flag before cancelling.
      _ <- MigrationTask.cancelForCollection(schema.scope, docID.as[CollectionID])
    } yield errs

  def processUpdate(
    schema: CollectionSchema,
    docID: DocID,
    prevData: Data,
    updatedData: Data) =
    for {
      withMVT <- CollectionWriteConfig.updateMVT(
        schema.scope,
        docID.as[CollectionID],
        updatedData)
      _ <- SchemaCollection
        .Collection(schema.scope)
        .internalUpdate(docID.as[CollectionID], Diff(withMVT.fields))
      isProtected <- Database.isProtected(schema.scope)
    } yield {
      val failures = Seq.newBuilder[ConstraintFailure]
      enforceProtectedMode(failures, isProtected, prevData, updatedData)
      validateCollection(failures, schema.name, updatedData)
      failures.result()
    }

  def markComplete(scope: ScopeID, index: IndexID): Query[Boolean] =
    updateIndexWithBuildState(scope, index, CollectionIndex.Status.Complete)

  def markFailed(scope: ScopeID, index: IndexID): Query[Boolean] =
    updateIndexWithBuildState(scope, index, CollectionIndex.Status.Failed)

  private def updateIndexWithBuildState(
    scopeID: ScopeID,
    indexID: IndexID,
    status: CollectionIndex.Status): Query[Boolean] = {
    Index.getUncached(scopeID, indexID) flatMap {
      case Some(index) if index.isCollectionIndex =>
        index.sources match {
          case IndexSources.Limit(collIDs) =>
            val updatesQ = collIDs map { collID =>
              SchemaCollection.Collection(scopeID) flatMap { config =>
                val schema = config.Schema

                Store.get(schema, collID.toDocID) flatMapT { ver =>
                  val coll = CollectionIndexManager(scopeID, collID, ver.data)
                    .updateIndexStatus(
                      indexID,
                      status
                    )
                  Store.internalUpdate(
                    schema,
                    collID.toDocID,
                    Diff(coll.toCollectionData)) map { _ => Some(true) }
                }
              }
            } sequence

            updatesQ map { updates =>
              updates contains Some(true)
            }

          case _ =>
            Query.False
        }

      case _ =>
        Query.False
    }
  }

  private def deleteCollectionIndexes(schema: CollectionSchema, docID: DocID) =
    SchemaCollection.Index(schema.scope) flatMap { indexConfig =>
      val indexSchema = indexConfig.Schema

      for {
        indexes <- Index.getUserDefinedBySource(
          indexSchema.scope,
          docID.as[CollectionID]
        )
        _ <- indexes.map { index =>
          // Only delete if the collection is the only
          // source of the index
          index.sources match {
            case IndexSources.Limit(colls) if colls.sizeIs == 1 =>
              Store.internalDelete(indexSchema, index.id.toDocID).join
            case _ =>
              Query.unit
          }
        }.join
        isProtected <- Database.isProtected(schema.scope)
      } yield {
        if (isProtected) {
          // Enforce protected mode: cannot delete collections.
          Seq(ConstraintFailure.ProtectedModeFailure.DeleteCollection)
        } else {
          Nil
        }
      }
    }

  private def enforceProtectedMode(
    failures: Builder[ConstraintFailure, Seq[ConstraintFailure]],
    isProtected: Boolean,
    prevData: Data,
    updatedData: Data) =
    if (prevData != Data.empty && isProtected) {
      // * Cannot decrease or remove history_days.
      val prevHD = prevData.getOrElse(Collection.RetainDaysField, None)
      val updHD =
        updatedData.getOrElse(Collection.RetainDaysField, None)
      (prevHD, updHD) match {
        case (Some(_), None) =>
          failures += ConstraintFailure.ProtectedModeFailure.RemoveHistoryDays
        case (Some(prev), Some(curr)) if curr < prev =>
          failures += ConstraintFailure.ProtectedModeFailure.DecreaseHistoryDays
        case _ => // Ok.
      }

      // * Cannot decrease or add ttl_days.
      val prevTTL = prevData.getOrElse(Collection.TTLField, None)
      val updTTL = updatedData.getOrElse(Collection.TTLField, None)
      (prevTTL, updTTL) match {
        case (None, Some(_)) =>
          failures += ConstraintFailure.ProtectedModeFailure.AddTTLDays
        case (Some(prev), Some(curr)) if curr < prev =>
          failures +=
            ConstraintFailure.ProtectedModeFailure.DecreaseTTLDays
        case _ => // Ok.
      }
    }

  // Performs extra validation of collection documents that didn't fit in elsewhere.
  private def validateCollection(
    failures: Builder[ConstraintFailure, Seq[ConstraintFailure]],
    name: String,
    data: Data) = {
    // Check for duplicate check constraint names.
    val names =
      CheckConstraint.fromData(data) groupBy { _.name }
    names foreach {
      case (name, grp) if grp.size > 1 =>
        failures += ConstraintFailure.ValidatorFailure(
          Path(Right("constraints")),
          s"Duplicate check constraint name '$name'")
      case _ => ()
    }

    // Check for empty and duplicate unique constraint paths.
    val uniques =
      CollectionIndexManager.UniqueConstraint.fromData(data) groupBy {
        _.fields.map(_.field.fieldPath)
      }
    if (uniques exists { _._1.isEmpty }) {
      failures += ConstraintFailure.ValidatorFailure(
        Path(Right("constraints")),
        "Unique constraints must have at least one term"
      )
    }
    uniques foreach {
      case (fields, grp) if grp.size > 1 =>
        val path = fields
          .map(p => fql.ast.Path.fromList(p).display)
          .mkString("[", ", ", "]")
        failures += ConstraintFailure.ValidatorFailure(
          Path(Right("constraints")),
          s"Duplicate unique constraint on path $path")
      case _ => ()
    }

    // Check for computed field names duplicating defined field names.
    val fs = DefinedField.fromData(name, data)
    val cfs =
      ComputedField.fromData(name, data)
    fs.keySet intersect cfs.keySet foreach { dupe =>
      failures += ConstraintFailure.ValidatorFailure(
        Path(Right("computed_fields")),
        s"Computed field '$dupe' collides with a defined field")
    }
  }
}
