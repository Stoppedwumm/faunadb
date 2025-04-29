package fauna.model

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.model.schema.{
  InternalCollectionID,
  NativeCollectionID,
  SchemaCollection,
  SchemaItemView
}
import fauna.repo.query.Query
import fauna.storage.index.NativeIndexID
import scala.util.control.NoStackTrace

package tasks {

  /** Used for control flow to break out of Future context back into
    * the main loop of a Mapper thread when cluster topology indicates
    * this host owns no segments.
    */
  final class NoSegmentsOwnedException extends NoStackTrace

  /** Used for determining whether version and index data may be
    * removed following the deletion of a Database, Collection, or
    * Index.
    */
  sealed trait RetentionDecision
  object Retained extends RetentionDecision
  object Deleted extends RetentionDecision
  object Live extends RetentionDecision

  trait RetentionPolicy {
    def isDatabaseRetained(
      id: ScopeID,
      snapshotTS: Timestamp): Query[RetentionDecision]

    def isCollectionRetained(
      scopeID: ScopeID,
      id: CollectionID,
      snapshotTS: Timestamp): Query[RetentionDecision]

    def isIndexRetained(
      scopeID: ScopeID,
      id: IndexID,
      snapshotTS: Timestamp): Query[RetentionDecision]
  }

  object RetentionPolicy {
    object Default extends RetentionPolicy {

      def isDatabaseRetained(
        id: ScopeID,
        snapshotTS: Timestamp): Query[RetentionDecision] =
        Database.latestForScope(id) flatMap {
          case Some(db) if db.isDeleted && db.deletedTS.get < snapshotTS =>
            Query.value(Deleted)
          case Some(db) if db.isDeleted => Query.value(Retained)
          case Some(_)                  => Query.value(Live)
          case None =>
            Query.repo map { repo =>
              repo.stats.incr("Schema.Databases.Missing")

              // The safest choice is to consider these rows live, in
              // case this is caused by (e.g.) a replication error.
              Live
            }
        }

      def isCollectionRetained(
        scopeID: ScopeID,
        id: CollectionID,
        snapshotTS: Timestamp) =
        id match {
          // XXX: temporarily retain all internal collections.
          case InternalCollectionID(_) => Query.value(Retained)

          // Retain all native collection data.
          case NativeCollectionID(_) => Query.value(Live)

          case UserCollectionID(id) =>
            val colCol = SchemaCollection.Collection(scopeID)
            colCol.schemaVersState(id) flatMap {
              case Some(SchemaItemView.Deleted(_)) =>
                // The collection is live, though it is staged for deletion.
                Query.value(Live)
              case Some(_) =>
                Query.value(Live)
              case None =>
                // The collection is properly deleted.
                // Do not go through the cache to avoid expensive and
                // unnecessary MVT calculations for Collections with large
                // histories. The latest version is sufficient here.
                colCol.getVersion(id, snapshotTS) flatMap {
                  case Some(cls) if cls.isDeleted && cls.ts.validTS < snapshotTS =>
                    Query.value(Deleted)
                  case Some(cls) if cls.isDeleted => Query.value(Retained)
                  case Some(_)                    => Query.value(Live)
                  case None =>
                    Query.repo map { repo =>
                      repo.stats.incr("Schema.Collections.Missing")

                      // The safest choice is to consider these rows live, in
                      // case this is caused by (e.g.) a replication error.
                      Live
                    }
                }
            }

          case _ =>
            // This is a deprecated native collection; clean it up.
            Query.value(Deleted)
        }

      def isIndexRetained(scopeID: ScopeID, id: IndexID, snapshotTS: Timestamp) =
        id match {
          case NativeIndexID(_) =>
            Query.value(Live) // retain all native index data

          case UserIndexID(_) =>
            // NB: Although indexes can be staged for deletion, the backing
            // index is not deleted, and therefore this code correctly won't
            // delete any data in the backing index.
            //
            // Do not go through the cache to avoid unnecessary
            // materialization of sources. The latest version is
            // sufficient here.
            SchemaCollection.Index(scopeID).getVersion(id, snapshotTS) flatMap {
              case Some(idx) if idx.isDeleted && idx.ts.validTS < snapshotTS =>
                Query.value(Deleted)
              case Some(idx) if idx.isDeleted => Query.value(Retained)
              case Some(_)                    => Query.value(Live)
              case None =>
                Query.repo map { repo =>
                  repo.stats.incr("Schema.Indexes.Missing")

                  // The safest choice is to consider these rows live, in
                  // case this is caused by (e.g.) a replication error.
                  Live
                }
            }
          case _ =>
            // This is a deprecated native index; clean it up.
            Query.value(Deleted)
        }
    }
  }
}
