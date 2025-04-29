package fauna.model.schema.index

import fauna.ast.{ EvalContext, IndexWriteConfig, MaximumIDException, RootPosition }
import fauna.atoms.{
  APIVersion,
  CollectionID,
  DocID,
  IndexID,
  SchemaVersion,
  ScopeID
}
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.{
  Index,
  SchemaNames,
  SourceConfig,
  TermBindingPath,
  TermConfig,
  TermDataPath
}
import fauna.model.schema.{
  CheckConstraint,
  CollectionIndexSchemaTypes,
  ComputedField,
  ComputedFieldData,
  SchemaCollection,
  SchemaStatus
}
import fauna.model.schema.index.CollectionIndexManager.IndexDefinition
import fauna.model.Database
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.repo.schema.{ CollectionSchema, ConstraintFailure }
import fauna.repo.schema.migration.Migration
import fauna.repo.values.{ Value, ValueDecoder }
import fauna.storage.doc.{ Data, Diff }
import fauna.storage.index.IndexSources
import fauna.storage.ir.{ ArrayV, IRValue, MapV }
import fql.ast.Span
import java.util.UUID
import scala.collection.immutable.SeqMap
import scala.collection.mutable.{ Map => MMap, Set => MSet }

object CollectionIndexManager {
  val IndexableTopLevelFields = Set("ts", "ttl")

  def commit(scope: ScopeID, coll: CollectionID, migrations: Seq[Migration]) =
    SchemaStatus.forScope(scope).flatMap { status =>
      status.activeSchemaVersion match {
        case Some(version) => commitStaged(scope, coll, migrations, version)
        case None =>
          throw new IllegalStateException(s"Schema was not staged in $scope")
      }
    }

  def abandon(scope: ScopeID, coll: CollectionID, migrations: Seq[Migration]) =
    SchemaStatus.forScope(scope).flatMap { status =>
      status.activeSchemaVersion match {
        case Some(version) => abandonStaged(scope, coll, migrations, version)
        case None =>
          throw new IllegalStateException(s"Schema was not staged in $scope")
      }
    }

  // This is the meat and potatoes of staged schema updates. Here is an example that
  // covers all the cases we need to handle:
  //
  // A -> B -> C -> D
  //      \          \
  //       -----------> E
  //
  // A: An old live schema.
  // B: The active schema.
  // C: An old staged schema.
  // D: The staged schema at the snapshot time.
  // E: The new staged schema that we are updating to.
  //
  // The goal of the current update is to move the collection from schema `B` to
  // schema `E`.
  //
  // Because we want to maintain indexes that are already build for schema `D`, we
  // need to run index field provenance twice: first with the migrations from schema
  // `B` to schema `D` (this is `prevMigrations`), and a second time for the
  // migrations from schema `B` to schema `E` (this is `newMigrations`). Once we have
  // index field provenance for both of these, we can roll the index shapes backwards
  // from `D` to `B`, and from `E` to `B`, and then compare to see which ones have
  // changed.
  def updateStaged(
    scope: ScopeID,
    coll: CollectionID,
    prevMigrations: Seq[Migration],
    newMigrations: Seq[Migration],
    active: SchemaVersion) = {
    for {
      snap <- Query.snapshotTime

      active  <- mgr(scope, coll, active.ts) // `B` in the example.
      staged  <- mgr(scope, coll, snap) // `D` in the example.
      updated <- mgr(scope, coll) // `E` in the example.

      (backingIndexes, changes) = computeStagedChanges(
        active,
        staged,
        updated,
        prevMigrations,
        newMigrations)

      // Step 1: Undo the index changes from the previous schema push.
      _ <- revertStagedChanges(active, staged, changes, prevMigrations)

      // Step 2: Build new indexes from schema `B` to schema `E`.
      backing <- changes.writeStaged(scope, coll, backingIndexes)
      _       <- updateStatuses(scope, coll, updated, backing)

      // Staged schema changes bypass protected mode.
    } yield Nil
  }

  private def commitStaged(
    scope: ScopeID,
    coll: CollectionID,
    migrations: Seq[Migration],
    active: SchemaVersion) = {
    for {
      active  <- mgr(scope, coll, active.ts) // `B` in the example.
      updated <- mgr(scope, coll) // `E` in the example.

      // Update and remove modified indexes from schema `B` to schema `E`.
      changes = computeChanges(active, updated, migrations)
      backing <- changes.writeCommit(
        scope,
        active.backingIndexes,
        updated.backingIndexes)
      _ <- updateStatuses(scope, coll, updated, backing)
    } yield ()
  }

  private def abandonStaged(
    scope: ScopeID,
    coll: CollectionID,
    migrations: Seq[Migration],
    active: SchemaVersion) = {
    for {
      activeMgr <- mgr(scope, coll, active.ts) // `B` in the example.
      stagedMgr <- mgr(scope, coll) // `E` in the example.

      // Delete any newly building indexes from schema `B` to schema `E`.
      changes = computeChanges(activeMgr, stagedMgr, migrations)
      _ <- changes.writeAbandon(scope, stagedMgr.backingIndexes)

      // This handles all 4 possible cases for each index:
      // - New indexes in the staged schema should be deleted.
      // - Indexes that have not changed should use the `stagedMgr` status (as they
      //   may have been built before the staged schema change, and finished building
      //   since the staged schema change).
      // - Indexes that _have_ been updated won't actually change shape until the
      //   staged schema is comitted, so we don't need to worry about those.
      // - Removed indexes should be re-added.
      //
      // Each of these 4 cases is handled by the following filters: new indexes are
      // removed in the first filter, unchanged/updated indexes are kept in the first
      // filter, and removed indexes are added back in the second filter.
      activeIds = activeMgr.backingIndexes.elems.view.map(_.indexID).toSet
      stagedIds = stagedMgr.backingIndexes.elems.view.map(_.indexID).toSet
      newBacking = BackingIndexList(stagedMgr.backingIndexes.filter { i =>
        activeIds.contains(i.indexID)
      }.elems ++ activeMgr.backingIndexes.filter { i =>
        !stagedIds.contains(i.indexID)
      }.elems)

      // We revert the collection to the active state here. This needs to be done in
      // this order, so that we can call `mgr()` above with the correct versions in
      // place, and then revert things afterwards.
      activeVers <- SchemaCollection.Collection(scope).get(coll, active.ts)
      _ <- activeVers match {
        case Some(vers) =>
          SchemaCollection
            .Collection(scope)
            .internalReplace(coll, vers.data)
            .flatMap { _ =>
              // Now that the doc is updated to the `active` state, we can write the
              // correct index statuses in.
              updateStatuses(scope, coll, activeMgr, newBacking)
            }
        case None =>
          SchemaCollection.Collection(scope).internalDelete(coll)
      }
    } yield ()
  }

  def updateUnstaged(
    scope: ScopeID,
    coll: CollectionID,
    migrations: Seq[Migration]) = {
    for {
      snap <- Query.snapshotTime

      prev    <- mgr(scope, coll, snap)
      updated <- mgr(scope, coll)

      changes = computeChanges(prev, updated, migrations)
      backing <- changes.write(scope, coll, prev.backingIndexes)
      _       <- updateStatuses(scope, coll, updated, backing)

      isProtected <- Database.isProtected(scope)
    } yield {
      if (isProtected) {
        // Enforce protected mode restrictions on update:
        // * Cannot cause a non-pending backing index to be deleted.
        if (changes.removeIndexes.nonEmpty) {
          List(ConstraintFailure.ProtectedModeFailure.DeleteBackingIndex)
        } else {
          Nil
        }
      } else {
        Nil
      }
    }
  }

  private def mgr(
    scope: ScopeID,
    coll: CollectionID,
    ts: Timestamp = Timestamp.MaxMicros) =
    SchemaCollection.Collection(scope).get(coll, ts).map { vers =>
      val data = vers.fold(Data.empty) { _.data }
      CollectionIndexManager(scope, coll, data)
    }

  def markNotQueryable(scope: ScopeID, coll: CollectionID) = {
    for {
      vers <- SchemaCollection.Collection(scope).get(coll)

      data = vers.fold(Data.empty) { _.data }
      originalMgr = CollectionIndexManager(scope, coll, data)

      // Clear out the `queryable` flag.
      updatedMgr = originalMgr.copy(userDefinedIndexes =
        originalMgr.userDefinedIndexes.map { case (name, index) =>
          name -> index.copy(queryable = Some(false))
        })

      changes = BackingIndexChanges.empty
      backing <- changes.write(scope, coll, originalMgr.backingIndexes)
      _       <- updateStatuses(scope, coll, updatedMgr, backing)
    } yield ()
  }

  def rebuildAll(scope: ScopeID, coll: CollectionID) = {
    for {
      vers <- SchemaCollection.Collection(scope).get(coll)

      data = vers.fold(Data.empty) { _.data }
      originalMgr = CollectionIndexManager(scope, coll, data)

      // Clear out the `queryable` flag, so that it gets overwritten in
      // `applyChanges`.
      updatedMgr = originalMgr.copy(userDefinedIndexes =
        originalMgr.userDefinedIndexes.map { case (name, index) =>
          name -> index.copy(queryable = None)
        })

      changes = rebuildChanges(originalMgr)
      backingChanges = originalMgr.computeBackingIndexChanges(changes)
      backing <- backingChanges.write(scope, coll, originalMgr.backingIndexes)
      _       <- updateStatuses(scope, coll, updatedMgr, backing)
    } yield ()
  }

  private def revertStagedChanges(
    active: CollectionIndexManager,
    staged: CollectionIndexManager,
    stagedChanges: BackingIndexChanges,
    prevMigrations: Seq[Migration]): Query[Unit] = {
    // Grab changes from active -> staged.
    val prevChanges = computeChanges(active, staged, prevMigrations)

    // Delete any of the indexes that were built. Updated and removed indexes won't
    // be modified, because they are still used by the active schema.
    prevChanges.buildIndexes
      .map { build =>
        // If this index is going to be updated, leave it be. This is the case where
        // we are re-using an index from a previous staged push, in which case we
        // should just leave it alone here.
        val bi = staged.backingIndexes.find(build.definition).get
        if (stagedChanges.updateIndexes.exists(_.backing.indexID == bi.indexID)) {
          Query.value(())
        } else {
          SchemaCollection.Index(active.scopeID).internalDelete(bi.indexID)
        }
      }
      .sequence
      .join
  }

  /** Updates the index statuses for a `CollectionIndexManager`, given a set of
    * new backing indexes.
    */
  private def updateStatuses(
    scope: ScopeID,
    coll: CollectionID,
    mgr: CollectionIndexManager,
    backing: BackingIndexList) = {
    // Copy over index statuses, and the new backing indexes list.
    val newMgr = mgr.copy(
      userDefinedIndexes = mgr.userDefinedIndexes.map { case (name, index) =>
        val bi = backing
          .find(index)
          .getOrElse(throw new IllegalStateException(
            s"Could not find backing index for $index in $backing ($scope, $coll)"))
        name -> index.copy(
          status = bi.status,
          queryable = index.queryable match {
            case Some(v) => Some(v)
            case None    => Some(bi.status == CollectionIndex.Status.Complete)
          })
      },
      uniqueConstraints = mgr.uniqueConstraints.map { case constraint =>
        val bi = backing
          .find(constraint.toIndexDefinition)
          .getOrElse(throw new IllegalStateException(
            s"Could not find backing index for ${constraint.toIndexDefinition} in $backing ($scope, $coll)"))
        constraint.copy(status = uniqueConstraintStatusFromIndexStatus(bi.status))
      },
      backingIndexes = backing
    )

    for {
      updatedVers <- SchemaCollection.Collection(scope).get(coll)
      updatedData = updatedVers.fold(Data.empty) { _.data }

      _ <- SchemaCollection
        .Collection(scope)
        .internalUpdate(
          coll,
          Diff(updatedData.fields.merge(newMgr.toCollectionData)))
    } yield ()
  }

  type IndexOrUnique = Either[UserIndexDefinition, UniqueConstraint]

  // These are the 4 possible states of each index:
  // - `buildIndexes` are indexes that are either new, or need to be rebuilt.
  // - `updateIndexes` are indexes whos data remains unchanged, but their
  //   definition have changed (ie, a field was renamed).
  // - `removeIndexes` are indexes that need to be removed.
  // - unchanged indexes are not tracked here, and they are indexes whos data
  //   and shape remain unchanged.
  case class Changes(
    buildIndexes: Seq[IndexBuild],
    updateIndexes: Seq[IndexUpdate],
    removeIndexes: Seq[IndexBuild]
  )

  object Changes {
    def empty = Changes(Seq.empty, Seq.empty, Seq.empty)
  }

  case class IndexBuild(
    indexes: Seq[UserIndexDefinition],
    unique: Option[UniqueConstraint]
  ) {
    assert(indexes.nonEmpty || unique.nonEmpty, "indexes or unique must be defined")

    // Return the unique index if it exists, so that unique indexes are built
    // correctly.
    //
    // TODO: Replace UserIndexDefinition with a case class, so we can clean
    // this up a bit.
    def definition =
      unique.map(_.toIndexDefinition).getOrElse(indexes.head)
  }

  case class IndexUpdate(
    fromIndex: Seq[UserIndexDefinition],
    fromUnique: Option[UniqueConstraint],
    toIndex: Seq[UserIndexDefinition],
    toUnique: Option[UniqueConstraint]
  ) {
    assert(
      fromIndex.nonEmpty || fromUnique.nonEmpty,
      "fromIndex or fromUnique must be defined")
    assert(
      toIndex.nonEmpty || toUnique.nonEmpty,
      "toIndex or toUnique must be defined")

    // The index and unique constraint should have the same shape, so just grab
    // either definition here.
    def fromDefinition =
      fromIndex.headOption.getOrElse(fromUnique.get.toIndexDefinition)
    def toDefinition =
      toIndex.headOption.getOrElse(toUnique.get.toIndexDefinition)
  }

  private def computeChanges(
    prev: CollectionIndexManager,
    mgr: CollectionIndexManager,
    migrations: Seq[Migration]): BackingIndexChanges = {

    val prov = new IndexFieldProvenance
    migrations.foreach(prov.migrate)

    val buildIndexesB = Seq.newBuilder[IndexBuild]
    val updateIndexesB = Seq.newBuilder[IndexUpdate]
    val removeIndexesB = Seq.newBuilder[IndexBuild]

    val activeOldShapes = MSet.empty[IndexFieldProvenance.IndexShape]

    val updatedShapes = IndexFieldProvenance.allShapes(mgr)
    val prevShapes = IndexFieldProvenance.allShapes(prev)

    updatedShapes.foreach { case (shape, indexes) =>
      val newIndexes = indexes.collect { case Left(index) => index }
      val newUniques = indexes.collect { case Right(unique) => unique }
      val newUnique = newUniques.nonEmpty
      if (newUniques.sizeIs > 1) {
        throw new IllegalStateException(
          s"Multiple unique constraints with the same shape: $newUniques")
      }

      prov.mapShape(shape) match {
        // An old shape was found for this index. This can happen if a field in an
        // index is renamed, or if no migrations were applied.
        case Some(oldShape) =>
          prevShapes.get(oldShape) match {
            case Some(oldIndexes) =>
              // This index existed previously. If its definition has changed, update
              // it, but do not rebuild it.
              activeOldShapes += oldShape
              val oldUnique = oldIndexes.collectFirst { case Right(_) => }.nonEmpty

              if (shape != oldShape || newUnique != oldUnique) {
                val oldUserIndexes = oldIndexes.collect { case Left(index) => index }
                val oldUniques = oldIndexes.collect { case Right(unique) => unique }
                if (oldUniques.sizeIs > 1) {
                  throw new IllegalStateException(
                    s"Multiple unique constraints with the same shape: $oldUniques")
                }

                updateIndexesB += IndexUpdate(
                  oldUserIndexes,
                  oldUniques.headOption,
                  newIndexes,
                  newUniques.headOption)
              }
            case None =>
              // This index is new, so build it.
              buildIndexesB += IndexBuild(newIndexes, newUniques.headOption)
          }

        // No old shape exists for this index. This either a new index, or an index
        // whos fields have been migrated. Build a new index, and remove the old one
        // if applicable (handled below).
        case None => buildIndexesB += IndexBuild(newIndexes, newUniques.headOption)
      }
    }

    prevShapes.foreach { case (oldShape, indexes) =>
      val oldIndexes = indexes.collect { case Left(index) => index }
      val oldUniques = indexes.collect { case Right(unique) => unique }
      if (oldUniques.sizeIs > 1) {
        throw new IllegalStateException(
          s"Multiple unique constraints with the same shape: $oldUniques")
      }

      // If no new indexes mapped to this old shape, remove it.
      if (!activeOldShapes.contains(oldShape)) {
        removeIndexesB += IndexBuild(oldIndexes, oldUniques.headOption)
      }
    }

    val changes = Changes(
      buildIndexes = buildIndexesB.result(),
      updateIndexes = updateIndexesB.result(),
      removeIndexes = removeIndexesB.result()
    )
    prev.computeBackingIndexChanges(changes)
  }

  private def computeStagedChanges(
    active: CollectionIndexManager,
    staged: CollectionIndexManager,
    updated: CollectionIndexManager,
    prevMigrations: Seq[Migration],
    newMigrations: Seq[Migration]): (BackingIndexList, BackingIndexChanges) = {

    val oldProv = new IndexFieldProvenance
    prevMigrations.foreach(oldProv.migrate)

    val newProv = new IndexFieldProvenance
    newMigrations.foreach(newProv.migrate)

    val buildIndexesB = Seq.newBuilder[IndexBuild]
    val updateIndexesB = Seq.newBuilder[WithBacking[IndexUpdate]]
    val removeIndexesB = Seq.newBuilder[WithBacking[IndexBuild]]

    // These are all the shapes that were staged, and can be mapped to the active
    // schema. We will try our best to re-use these shapes, as they are already built
    // as of the old staged schema.
    val stagedOldShapes =
      MMap.empty[IndexFieldProvenance.IndexShape, Seq[IndexOrUnique]]
    val stagedIndexesToKeep = List.newBuilder[BackingIndex]

    val activeOldShapes = MSet.empty[IndexFieldProvenance.IndexShape]

    val updatedShapes = IndexFieldProvenance.allShapes(updated)
    val stagedShapes = IndexFieldProvenance.allShapes(staged)
    val activeShapes = IndexFieldProvenance.allShapes(active)

    // Step 1: Find all the staged shapes that can be mapped to the active schema.
    stagedShapes.foreach { case (shape, indexes) =>
      oldProv.mapShape(shape) match {
        case Some(oldShape) => stagedOldShapes.put(oldShape, indexes)
        case None           => ()
      }
    }

    updatedShapes.foreach { case (shape, indexes) =>
      val newIndexes = indexes.collect { case Left(index) => index }
      val newUniques = indexes.collect { case Right(unique) => unique }
      val newUnique = newUniques.nonEmpty
      if (newUniques.sizeIs > 1) {
        throw new IllegalStateException(
          s"Multiple unique constraints with the same shape: $newUniques")
      }

      newProv.mapShape(shape) match {
        // An old shape was found for this index. This can happen if a field in an
        // index is renamed, or if no migrations were applied.
        case Some(oldShape) =>
          activeShapes.get(oldShape) match {
            case Some(oldIndexes) =>
              // This index existed previously. If its definition has changed, update
              // it, but do not rebuild it.
              activeOldShapes += oldShape
              val oldUnique = oldIndexes.collectFirst { case Right(_) => }.nonEmpty

              if (shape != oldShape || newUnique != oldUnique) {
                val oldUserIndexes = oldIndexes.collect { case Left(index) => index }
                val oldUniques = oldIndexes.collect { case Right(unique) => unique }
                if (oldUniques.sizeIs > 1) {
                  throw new IllegalStateException(
                    s"Multiple unique constraints with the same shape: $oldUniques")
                }

                val index = oldUserIndexes.headOption.getOrElse(
                  oldUniques.head.toIndexDefinition)

                updateIndexesB += WithBacking(
                  active.backingIndexes.find(index).get,
                  IndexUpdate(
                    oldUserIndexes,
                    oldUniques.headOption,
                    newIndexes,
                    newUniques.headOption))
              }
            case None =>
              stagedOldShapes.get(oldShape) match {
                case Some(oldIndexes) =>
                  val oldUserIndexes = oldIndexes.collect { case Left(index) =>
                    index
                  }
                  val oldUniques = oldIndexes.collect { case Right(unique) =>
                    unique
                  }

                  val index = oldUserIndexes.headOption.getOrElse(
                    oldUniques.head.toIndexDefinition)

                  // This index was in the previous staged schema! Go ahead and
                  // update it. Also, record this backing index, so that we keep it
                  // around for index status reasons.
                  val backing = staged.backingIndexes.find(index).get
                  stagedIndexesToKeep += backing

                  updateIndexesB += WithBacking(
                    backing,
                    IndexUpdate(
                      oldUserIndexes,
                      oldUniques.headOption,
                      newIndexes,
                      newUniques.headOption))

                case None =>
                  // This index is new, so build it.
                  buildIndexesB += IndexBuild(newIndexes, newUniques.headOption)
              }

          }

        // No old shape exists for this index. This either a new index, or an index
        // whos fields have been migrated. Build a new index, and remove the old one
        // if applicable (handled below).
        case None => buildIndexesB += IndexBuild(newIndexes, newUniques.headOption)
      }
    }

    activeShapes.foreach { case (oldShape, indexes) =>
      val oldIndexes = indexes.collect { case Left(index) => index }
      val oldUniques = indexes.collect { case Right(unique) => unique }
      if (oldUniques.sizeIs > 1) {
        throw new IllegalStateException(
          s"Multiple unique constraints with the same shape: $oldUniques")
      }

      // If no new indexes mapped to this old shape, remove it.
      if (!activeOldShapes.contains(oldShape)) {
        val remove = IndexBuild(oldIndexes, oldUniques.headOption)
        removeIndexesB += WithBacking(
          active.backingIndexes.find(remove.definition).get,
          remove)
      }
    }

    (
      BackingIndexList(active.backingIndexes.elems ++ stagedIndexesToKeep.result()),
      BackingIndexChanges(
        buildIndexes = buildIndexesB.result(),
        updateIndexes = updateIndexesB.result(),
        removeIndexes = removeIndexesB.result()
      ))
  }

  /** Returns a `Changes` that rebuilds everything.
    */
  private def rebuildChanges(mgr: CollectionIndexManager): Changes = {
    val shapes = IndexFieldProvenance.allShapes(mgr)

    val builds = shapes.map { case (_, shapeIndexes) =>
      val indexes = shapeIndexes.collect { case Left(index) => index }
      val uniques = shapeIndexes.collect { case Right(unique) => unique }
      if (uniques.sizeIs > 1) {
        throw new IllegalStateException(
          s"Multiple unique constraints with the same shape: $uniques")
      }

      IndexBuild(indexes, uniques.headOption)
    }.toSeq

    Changes(
      buildIndexes = builds,
      updateIndexes = Seq.empty,
      removeIndexes = builds
    )
  }

  def apply(
    scopeID: ScopeID,
    collectionID: CollectionID,
    data: Data): CollectionIndexManager = {
    val userDefinedIndexes = UserIndexDefinition.fromData(data)
    val backingIndexes = BackingIndex.fromData(data)
    val uniqueConstraints = UniqueConstraint.fromData(data)
    val checkConstraints = CheckConstraint.fromData(data)
    new CollectionIndexManager(
      scopeID,
      collectionID,
      userDefinedIndexes,
      backingIndexes,
      uniqueConstraints,
      checkConstraints
    )
  }

  sealed trait IndexShape {
    def terms: Vector[CollectionIndex.Term]
    def values: Vector[CollectionIndex.Value]
    def unique: Boolean
  }

  sealed trait IndexDefinition extends IndexShape {

    lazy val backingIndexValues =
      values.lastOption match {
        case Some(last) if last.isIDField && last.ascending =>
          // the definition covers ID ascending, which is compatible with the
          // native doc slot, so the backing index should not be build with an
          // extra ID slot.
          values.dropRight(1)

        case _ => values
      }

    def toBackingIndex(
      id: IndexID,
      status: CollectionIndex.Status = CollectionIndex.Status.Building) =
      BackingIndex(id, terms, backingIndexValues, unique, status)

    def matchesIndex(other: IndexDefinition): Boolean =
      terms.size == other.terms.size &&
        values.size == other.values.size &&
        terms.iterator.zip(other.terms).forall { case (a, b) => a.matches(b) } &&
        values.iterator.zip(other.values).forall { case (a, b) => a.matches(b) }

    def matchesBackingIndex(bi: BackingIndex): Boolean = {
      def termsM = terms.size == bi.terms.size &&
        terms.iterator.zip(bi.terms).forall { case (a, b) => a.matches(b) }

      def biValuesM =
        backingIndexValues.size == bi.values.size &&
          backingIndexValues.iterator.zip(bi.values).forall { case (a, b) =>
            a.matches(b)
          }

      // TODO: remove this variant when/if all index shapes are fixed
      def compatValuesM =
        values.size == bi.values.size &&
          values.iterator.zip(bi.values).forall { case (a, b) =>
            a.matches(b)
          }

      termsM && (biValuesM || compatValuesM)
    }
  }

  /** Represents the backing index that is used for customer index definitions
    * as well as unique constraints.  A single backing index can be used for
    * multiple customer index definitions/unique constraints if the definitions match.
    */
  case class BackingIndex(
    indexID: IndexID,
    terms: Vector[CollectionIndex.Term],
    values: Vector[CollectionIndex.Value],
    unique: Boolean,
    status: CollectionIndex.Status)
      extends IndexShape {

    def toData: MapV =
      Value.toIR(toValue) match {
        case Left(npv) =>
          throw new IllegalStateException(
            s"Received non persistable value: $npv when attempting to convert BackingIndex $this to a persistable value")
        case Right(v) => v
      }

    private def toValue: Value.Struct.Full = {
      val fields = SeqMap(
        CollectionIndexSchemaTypes.indexIDFieldName -> Value.Doc(indexID.toDocID),
        CollectionIndexSchemaTypes.termsFieldName -> Value.Array.fromSpecific(
          terms.map(_.toValue)),
        CollectionIndexSchemaTypes.valuesFieldName -> Value.Array.fromSpecific(
          values.map(_.toValue)),
        CollectionIndexSchemaTypes.uniqueFieldName -> Value.Boolean(unique),
        CollectionIndexSchemaTypes.statusFieldName -> Value.Str(status.asStr)
      )
      Value.Struct(fields)
    }
  }

  // Terms' and values' field may be prefixed with '.' but computed field
  // names aren't.
  private def dropDot(s: String) = if (s.startsWith(".")) s.drop(1) else s

  private def adjustField(
    cfOpt: Option[ComputedField],
    field: CollectionIndex.Field): CollectionIndex.Field =
    cfOpt match {
      case Some(cf) => CollectionIndex.Field.Computed(field.path, cf.body)
      case None     => CollectionIndex.Field.Fixed(field.path)
    }

  // Replace any reference to a computed field in a term with an updated
  // computed term referencing the current body (which may or may not be
  // the same as the term's body if it is computed). Or, if a computed
  // field has gone away, replace it as a term with a fixed term. This
  // allows the rest of the machinery to detect index shape changes caused
  // by computed field changes.
  private def adjustTerms(
    cfs: Map[String, ComputedField],
    terms: Vector[CollectionIndex.Term]): Vector[CollectionIndex.Term] =
    terms map { term =>
      val head = term.field.head.getOrElse(term.field.path)
      CollectionIndex.Term(
        adjustField(cfs.get(dropDot(head)), term.field),
        term.mvaOpt)
    }

  // The above, but for values.
  private def adjustValues(
    cfs: Map[String, ComputedField],
    values: Vector[CollectionIndex.Value]): Vector[CollectionIndex.Value] =
    values map { value =>
      val head = value.field.head.getOrElse(value.field.path)
      CollectionIndex.Value(
        adjustField(cfs.get(dropDot(head)), value.field),
        value.ascending,
        value.mvaOpt)
    }

  object BackingIndex {
    def fromData(data: Data): BackingIndexList = {
      val idxs =
        (Value.fromIR(data.fields, Span.Null) / "backingIndexes") match {
          case v: Value.Array => v.elems
          case _              => Seq.empty
        }

      val cfs = ComputedField.fromData(ComputedField.UnknownCollectionName, data)

      BackingIndexList(
        idxs
          .map(index => {
            val terms = (index / CollectionIndexSchemaTypes.termsFieldName)
              .asOpt[Vector[CollectionIndex.Term]]
              .getOrElse(Vector.empty)
            val values = (index / CollectionIndexSchemaTypes.valuesFieldName)
              .asOpt[Vector[CollectionIndex.Value]]
              .getOrElse(Vector.empty)
            BackingIndex(
              (index / CollectionIndexSchemaTypes.indexIDFieldName)
                .as[DocID]
                .as[IndexID],
              adjustTerms(cfs, terms),
              adjustValues(cfs, values),
              (index / CollectionIndexSchemaTypes.uniqueFieldName)
                .asOpt[Boolean]
                .getOrElse(false),
              CollectionIndex.Status
                .fromStr(
                  (index / CollectionIndexSchemaTypes.statusFieldName).as[String])
                .getOrElse(throw new IllegalStateException(
                  s"Invalid status for backing index: $index"))
            )
          })
          .toList)
    }
  }

  /** Represents the index object user can create/update via FQL-X interface.
    */
  case class UserIndexDefinition(
    terms: Vector[CollectionIndex.Term],
    values: Vector[CollectionIndex.Value],
    queryable: Option[Boolean],
    name: String,
    status: CollectionIndex.Status = CollectionIndex.Status.Building
  ) extends IndexDefinition {
    def unique: Boolean = false

    def toData: IRValue =
      Value.toIR(toValue) match {
        case Left(npv) =>
          throw new IllegalStateException(
            s"Received non persistable value: $npv when attempting to convert UserIndexDefinition $this to a persistable value")
        case Right(v) => v
      }

    private def toValue: Value.Struct.Full = {
      val fb = SeqMap.newBuilder[String, Value]
      fb.addOne(
        CollectionIndexSchemaTypes.queryableFieldName -> Value.Boolean(
          queryable.getOrElse(false)))
      fb.addOne(
        CollectionIndexSchemaTypes.statusFieldName -> Value.Str(status.asStr))
      if (terms.nonEmpty) {
        fb.addOne(
          CollectionIndexSchemaTypes.termsFieldName -> Value.Array.fromSpecific(
            terms.map(_.toValue)))
      }
      if (values.nonEmpty) {
        fb.addOne(
          CollectionIndexSchemaTypes.valuesFieldName -> Value.Array.fromSpecific(
            values.map(_.toValue)))
      }
      Value.Struct(fb.result())
    }
  }

  object UserIndexDefinition {
    import CollectionIndexSchemaTypes._

    def fromData(data: Data): Map[String, UserIndexDefinition] = {
      val idxs =
        (Value.fromIR(data.fields, Span.Null) / "indexes") match {
          case v: Value.Struct.Full => v
          case _                    => Value.Struct.empty
        }

      val cfs = ComputedField.fromData(ComputedField.UnknownCollectionName, data)

      idxs.fields.keys
        .map(name => {
          val index = idxs.fields(name)
          val terms = (index / termsFieldName)
            .asOpt[Vector[CollectionIndex.Term]]
            .getOrElse(Vector.empty)
          val values = (index / valuesFieldName)
            .asOpt[Vector[CollectionIndex.Value]]
            .getOrElse(Vector.empty)
          name -> UserIndexDefinition(
            adjustTerms(cfs, terms),
            adjustValues(cfs, values),
            (index / queryableFieldName).asOpt[Boolean],
            name,
            CollectionIndex.Status
              .fromStr((index / statusFieldName).asOpt[String].getOrElse("building"))
              .getOrElse(throw new IllegalStateException(
                s"Invalid status for backing index: $index"))
          )
        })
        .toMap
    }
  }

  /** Represents a unique constraint that is defined by the user.
    */
  case class UniqueConstraint(
    fields: Vector[CollectionIndex.Term],
    status: String = "pending"
  ) {
    def toIndexDefinition: IndexDefinition = new UniqueConstraintIndex(fields)

    def matchesIndexDefinition(index: IndexDefinition): Boolean = {
      toIndexDefinition == index
    }

    private def toValue: Value.Struct.Full = {
      val fb = SeqMap.newBuilder[String, Value]
      // If no properties require the complex format, return the simple format.
      // Simple format: { unique: ["foo", "bar"] }
      // Complex format: { unique: [{ field: "foo" }, { field: "bar" }] }
      val fvs = if (fields.exists { f => f.mvaOpt.nonEmpty }) {
        fields map { _.toValue }
      } else {
        fields map { f => Value.Str(f.field.path) }
      }
      fb.addOne(
        CollectionIndexSchemaTypes.uniqueFieldName -> Value.Array.fromSpecific(fvs))
      fb.addOne(CollectionIndexSchemaTypes.statusFieldName -> Value.Str(status))
      Value.Struct(fb.result())
    }

    def toData: IRValue = Value.toIR(toValue) match {
      case Left(npv) =>
        throw new IllegalStateException(
          s"Received non persistable value $npv when attempting to convert UniqueConstraint $this to a persistable value")
      case Right(v) => v
    }
  }

  object UniqueConstraint {
    def fromData(data: Data): List[UniqueConstraint] = {
      val constraints =
        (Value.fromIR(data.fields, Span.Null) / "constraints") match {
          case v: Value.Array => v.elems
          case _              => Seq.empty
        }

      val cfs = ComputedField.fromData(ComputedField.UnknownCollectionName, data)

      constraints
        .flatMap(cs => {
          (cs / CollectionIndexSchemaTypes.uniqueFieldName) match {
            case fs: Value.Array =>
              val unique = fs
                .asOpt[Vector[String]]
                .map({
                  _.map { f =>
                    CollectionIndex
                      .Term(CollectionIndex.Field.Fixed(f), None)
                  }
                })
                .getOrElse(fs
                  .asOpt[Vector[CollectionIndex.Term]]
                  .getOrElse(Vector.empty))
              Some(
                UniqueConstraint(
                  adjustTerms(cfs, unique),
                  (cs / CollectionIndexSchemaTypes.statusFieldName)
                    .asOpt[String] getOrElse "Pending"
                ))
            case Value.Null(_) =>
              // No unique constraints.
              None
            case _ =>
              throw new IllegalStateException(
                s"Unique constraint fields $cs has non-array value")
          }
        })
        .toList
    }
    implicit object UniqueConstraintDecoder extends ValueDecoder[UniqueConstraint] {
      def apply(value: Value): UniqueConstraint = {
        val fields =
          (value / CollectionIndexSchemaTypes.uniqueFieldName) match {
            case fs: Value.Array =>
              fs.asOpt[Vector[String]]
                .map({
                  _.map { f =>
                    CollectionIndex
                      .Term(CollectionIndex.Field.Fixed(f), None)
                  }
                })
                .getOrElse(fs
                  .asOpt[Vector[CollectionIndex.Term]]
                  .getOrElse(Vector.empty))
            case _ =>
              throw new IllegalStateException(
                s"Unique constraint $value has non-array fields")
          }
        val status =
          (value / CollectionIndexSchemaTypes.statusFieldName)
            .asOpt[String] getOrElse "Pending"
        UniqueConstraint(fields, status)
      }
    }
  }

  /** Represents an index that is used to support a customer provided unique constraint.
    */
  class UniqueConstraintIndex(
    override val terms: Vector[CollectionIndex.Term]
  ) extends IndexDefinition {
    def values: Vector[CollectionIndex.Value] = Vector.empty
    def unique: Boolean = true

    override def toString =
      s"UniqueConstraintIndex($terms)"
  }

  private def uniqueConstraintStatusFromIndexStatus(
    status: CollectionIndex.Status): String = {
    status match {
      case CollectionIndex.Status.Complete => "active"
      case CollectionIndex.Status.Failed   => "failed"
      case CollectionIndex.Status.Building => "pending"
    }
  }

  // A wrapper around a backing index and some data `T`.
  case class WithBacking[T](backing: BackingIndex, data: T)

  object BackingIndexChanges {
    def empty = BackingIndexChanges(Seq.empty, Seq.empty, Seq.empty)
  }

  /** After a `Changes` is built, this is built as a list of all the changes to
    * make to the backing indexes in a collection.
    */
  case class BackingIndexChanges(
    buildIndexes: Seq[IndexBuild],
    updateIndexes: Seq[WithBacking[IndexUpdate]],
    removeIndexes: Seq[WithBacking[IndexBuild]]) {

    // Writes the backing index changes, and produces a new `BackingIndexList`.
    //
    // This function mostly consists of two decoupled parts:
    // - Updating backing index documents.
    // - Building the new `BackingIndexList`.
    //
    // These two parts only depend on each other for the index IDs and statuses
    // from brand-new indexes.
    def write(
      scope: ScopeID,
      coll: CollectionID,
      prev: BackingIndexList): Query[BackingIndexList] = {
      for {
        // Fold over the builds so they run sequentially, taking distinct IDs.
        builtIndexes <- buildIndexes.foldLeft(Query.value(Seq.empty[BackingIndex])) {
          (indexesQ, build) =>
            for {
              indexes <- indexesQ
              index   <- createBackingIndex(scope, coll, build.definition)
            } yield {
              indexes :+ index
            }
        }

        _ <- updateIndexes.map { case WithBacking(backing, update) =>
          val diff = Diff(
            Index.IndexByField -> toIRTerms(update.toDefinition.terms),
            Index.CoveredField -> toIRValues(update.toDefinition.values),
            Index.UniqueField -> Some(update.toUnique.isDefined)
          )

          SchemaCollection.Index(scope).internalUpdate(backing.indexID, diff)
        }.sequence

        _ <- removeIndexes.map { case WithBacking(backing, _) =>
          SchemaCollection.Index(scope).internalDelete(backing.indexID)
        }.sequence
      } yield backingIndexList(prev, builtIndexes)
    }

    // Only writes the creates for new indexes, not the updates or removes.
    //
    // Returns the complete `BackingIndexList`, as if it had written the writes.
    def writeStaged(
      scope: ScopeID,
      coll: CollectionID,
      prev: BackingIndexList): Query[BackingIndexList] = {
      buildIndexes
        .map { build => createBackingIndex(scope, coll, build.definition) }
        .sequence
        .map { builtIndexes =>
          backingIndexList(prev, builtIndexes)
        }
    }

    // This is the second half of staged schema writes. This finds existing indexes
    // for `buildIndexes` (as `writeStaged` will have already built them). Then,
    // this function will write updates and removes.
    //
    // Note that the `staged` backing index list is in a partially-updated state.
    // That is to say, it has updated definitions for newly built indexes, but old
    // definitions for indexes that the active schema is using. So, this staged
    // backing indexes list should only be used to lookup newly built indexes.
    def writeCommit(
      scope: ScopeID,
      active: BackingIndexList,
      staged: BackingIndexList): Query[BackingIndexList] = {
      for {
        _ <- updateIndexes.map { case WithBacking(backing, update) =>
          val diff = Diff(
            Index.IndexByField -> toIRTerms(update.toDefinition.terms),
            Index.CoveredField -> toIRValues(update.toDefinition.values),
            Index.UniqueField -> Some(update.toUnique.isDefined)
          )

          SchemaCollection.Index(scope).internalUpdate(backing.indexID, diff)
        }.sequence

        _ <- removeIndexes.map { case WithBacking(backing, _) =>
          SchemaCollection.Index(scope).internalDelete(backing.indexID)
        }.sequence
      } yield {
        val builtIndexes = buildIndexes.map { build =>
          staged.find(build.definition).get
        }

        backingIndexList(active, builtIndexes)
      }
    }

    // This is the second half of staged schema writes. This finds existing indexes
    // for `buildIndexes` (as `writeStaged` will have already built them), and
    // deletes them. This effectively "undoes" the writes from `writeStaged`.
    //
    // Note that the `staged` backing index list is in a partially-updated state.
    // That is to say, it has updated definitions for newly built indexes, but old
    // definitions for indexes that the active schema is using. So, this staged
    // backing indexes list should only be used to lookup newly built indexes.
    def writeAbandon(scope: ScopeID, staged: BackingIndexList): Query[Unit] = {
      val builtIndexes = buildIndexes.map { build =>
        staged.find(build.definition).get
      }

      builtIndexes
        .map { backing =>
          SchemaCollection.Index(scope).internalDelete(backing.indexID)
        }
        .sequence
        .join
    }

    private def backingIndexList(
      prev: BackingIndexList,
      builtIndexes: Seq[CollectionIndexManager.BackingIndex]) = {

      val result = Seq.newBuilder[CollectionIndexManager.BackingIndex]

      // Case 1: New indexes.
      result.addAll(builtIndexes)

      prev.elems.foreach { bi =>
        updateIndexes.find(_.backing.indexID == bi.indexID) match {
          // Case 2: Updated indexes.
          case Some(update) =>
            result += update.backing.copy(
              terms = update.data.toDefinition.terms,
              values = update.data.toDefinition.values)

          case None =>
            removeIndexes.exists(_.backing.indexID == bi.indexID) match {
              // Case 3: Removed indexes.
              case true => ()

              // Case 4: Unchanged indexes.
              case false => result += bi
            }
        }
      }

      BackingIndexList(result.result())
    }

    private def createBackingIndex(
      scope: ScopeID,
      coll: CollectionID,
      index: IndexDefinition): Query[BackingIndex] = {
      for {
        indexSchema <- SchemaCollection.Index(scope).map(_.Schema)
        idOpt       <- indexSchema.nextID
        id <- idOpt match {
          case Some(id) => Query.value(id.as[IndexID])
          case None     => Query.fail(MaximumIDException)
        }
        bi = index.toBackingIndex(id)

        cfTerms = index.terms collect {
          case CollectionIndex
                .Term(CollectionIndex.Field.Computed(path, body), _) =>
            dropDot(path) -> ComputedFieldData(body, None)
        }
        cfValues = index.values collect {
          case CollectionIndex
                .Value(CollectionIndex.Field.Computed(path, body), _, _) =>
            dropDot(path) -> ComputedFieldData(body, None)
        }

        source =
          Vector(
            SourceConfig(
              IndexSources(coll),
              List.empty,
              (cfTerms ++ cfValues).toList))

        idxData = Data(
          Index.IndexByField -> toIRTerms(bi.terms),
          Index.CoveredField -> toIRValues(bi.values),
          Index.SourceField -> source,
          // Names are irrelevant for collection indexes.
          SchemaNames.NameField -> SchemaNames.Name(UUID.randomUUID().toString),
          Index.ActiveField -> false,
          Index.UniqueField -> Some(bi.unique)
        )

        indexVersion <- writeIndexQ(indexSchema, id, idxData, isCreate = true)
      } yield {
        val status = if (indexVersion.data(Index.ActiveField)) {
          CollectionIndex.Status.Complete
        } else {
          CollectionIndex.Status.Building
        }

        bi.copy(status = status)
      }
    }

    private def toIRTerms(terms: Vector[CollectionIndex.Term]): Vector[TermConfig] =
      terms.map { t =>
        t.field match {
          case _: CollectionIndex.Field.Fixed =>
            TermConfig(
              path = TermDataPath(t.fieldPath),
              reverse = false,
              mva = t.isMVA,
              transform = None)
          case _: CollectionIndex.Field.Computed =>
            TermConfig(
              path = TermBindingPath(dropDot(t.field.path)),
              reverse = false,
              mva = t.isMVA,
              transform = None
            )
        }
      }

    private def toIRValues(
      values: Vector[CollectionIndex.Value]): Vector[TermConfig] =
      values.map { v =>
        v.field match {
          case _: CollectionIndex.Field.Fixed =>
            TermConfig(
              path = TermDataPath(v.fieldPath),
              reverse = !v.ascending,
              mva = v.isMVA,
              transform = None)
          case _: CollectionIndex.Field.Computed =>
            TermConfig(
              path = TermBindingPath(dropDot(v.field.path)),
              reverse = !v.ascending,
              mva = v.isMVA,
              transform = None)
        }
      }

    private def writeIndexQ(
      indexSchema: CollectionSchema,
      indexID: IndexID,
      indexData: Data,
      isCreate: Boolean): Query[Version] =
      Index.IndexSpecificValidator
        .patch(indexData, Diff.empty)
        .flatMap {
          case Right(newIndexData) =>
            // FIXME: Use Store.create(indexSchema, indexID, indexData)
            // FIXME: Find a way to kickoff index build tasks
            // NB. In the following code path, only `ec.scopeID` is used, so the
            // reason for a pure context is blank.
            val ec = EvalContext.pure(
              indexSchema.scope,
              APIVersion.Default,
              reason = "index writes")
            IndexWriteConfig
              .writeInternal(
                ec = ec,
                id = indexID.toDocID,
                // This field will be removed by the IndexSpecificValidator, so add
                // it back.
                data =
                  newIndexData patch Diff(Index.CollectionIndexField -> Some(true)),
                isCreate = isCreate,
                pos = RootPosition
              )
              .flatMap {
                case Right(v) => Query.value(v)
                case Left(errs) =>
                  Query.fail(new IllegalStateException(
                    s"Failed to create underlying index, errs=[$errs]"))
              }

          case Left(validationExceptions) =>
            Query.fail(new IllegalStateException(
              s"Failed to translate v10 index to v4 index document $validationExceptions" +
                s"${validationExceptions.map(_.getMessage()).mkString("\n")}"))
        }
  }
}

case class BackingIndexList(elems: Seq[CollectionIndexManager.BackingIndex])
    extends AnyVal {
  def find(index: IndexDefinition) = elems.find(index.matchesBackingIndex(_))
  def findID(id: IndexID) = elems.find(_.indexID == id)

  def replace(
    id: IndexID,
    f: CollectionIndexManager.BackingIndex => CollectionIndexManager.BackingIndex) =
    BackingIndexList(elems.map { idx =>
      if (idx.indexID == id) {
        f(idx)
      } else {
        idx
      }
    })

  // Various collection functions, that just call into `Seq`.
  def filter(p: CollectionIndexManager.BackingIndex => Boolean) =
    BackingIndexList(elems.filter(p))
  def filterNot(p: CollectionIndexManager.BackingIndex => Boolean) =
    BackingIndexList(elems.filterNot(p))
  def foldLeft[T](z: T)(f: (T, CollectionIndexManager.BackingIndex) => T) =
    elems.foldLeft(z)(f)
  def map[T](f: CollectionIndexManager.BackingIndex => T): Seq[T] =
    elems.map(f)
  def :+(bi: CollectionIndexManager.BackingIndex) = BackingIndexList(elems :+ bi)
}

/** Class responsible for managing the indexes and unique constraints defined on a collection in FQLX.
  * All customer defined indexes and constraints must be backed by an underlying internal index.
  * When customer defined indexes and constraints are added, this class will ensure that the necessary
  * backing indexes are created/used.
  *
  * This class also has a reference to its collection's check constraints because it generates the
  * data for the constraints field.
  */
case class CollectionIndexManager(
  scopeID: ScopeID,
  collectionID: CollectionID,
  userDefinedIndexes: Map[String, CollectionIndexManager.UserIndexDefinition],
  backingIndexes: BackingIndexList,
  uniqueConstraints: List[CollectionIndexManager.UniqueConstraint],
  checkConstraints: List[CheckConstraint]
) {
  import CollectionIndexManager._

  /** This will update the status of the provided index.  This update will propagate
    * to any of the user defined indexes or unique constraints that are backed by the
    * index being updated.
    */
  def updateIndexStatus(
    indexID: IndexID,
    status: CollectionIndex.Status): CollectionIndexManager = {
    val backingIndex = backingIndexes
      .findID(indexID)
      .getOrElse(
        throw new IllegalStateException(
          s"Unable to find backing index for indexID: $indexID on collection: $collectionID"
        )
      )
      .copy(status = status)
    this.copy(
      backingIndexes = backingIndexes.replace(indexID, _ => backingIndex),
      userDefinedIndexes = userDefinedIndexes.values.map { idx =>
        val updatedIdx = if (idx.matchesBackingIndex(backingIndex)) {
          idx.copy(
            queryable = status match {
              case CollectionIndex.Status.Complete => Some(true)
              case CollectionIndex.Status.Failed   => Some(false)
              case _                               => idx.queryable
            },
            status = status
          )
        } else {
          idx
        }
        (idx.name -> updatedIdx)
      }.toMap,
      uniqueConstraints = uniqueConstraints.map { uc =>
        if (uc.toIndexDefinition.matchesBackingIndex(backingIndex)) {
          uc.copy(status = uniqueConstraintStatusFromIndexStatus(status))
        } else {
          uc
        }
      }
    )
  }

  def computeBackingIndexChanges(changes: Changes): BackingIndexChanges = {
    BackingIndexChanges(
      buildIndexes = changes.buildIndexes,
      updateIndexes = changes.updateIndexes.map { update =>
        WithBacking(backingIndexes.find(update.fromDefinition).get, update)
      },
      removeIndexes = changes.removeIndexes.map { remove =>
        WithBacking(backingIndexes.find(remove.definition).get, remove)
      }
    )
  }

  /** Method used to turn this portion of a collection into the IR storage format so that it can
    * be written.
    */
  def toCollectionData: MapV = {
    MapV(
      (
        "indexes",
        MapV(userDefinedIndexes.view.mapValues(uidx => uidx.toData).toList)),
      (
        "constraints",
        ArrayV(
          (uniqueConstraints.map(_.toData) ++ checkConstraints.map(
            _.toData)).toVector)),
      ("backingIndexes", ArrayV(backingIndexes.map(_.toData).toVector))
    )
  }

  def findBackingIndex(index: IndexDefinition): Option[BackingIndex] =
    backingIndexes.find(index)

  def findBackingIndexOrError(index: IndexDefinition): BackingIndex = {
    findBackingIndex(index)
      .getOrElse(throw new IllegalStateException(
        s"Could not find backing index for $index in $backingIndexes ($scopeID, $collectionID)"))
  }
}
