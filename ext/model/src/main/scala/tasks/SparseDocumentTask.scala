package fauna.model.tasks

import fauna.atoms._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.repo._
import fauna.repo.query.Query
import fauna.storage.api.version._
import scala.concurrent.duration._

object SparseDocumentTask {
  def apply(retainTime: Duration)(snapshotTS: Timestamp) =
    new SparseDocumentTask(retainTime, snapshotTS)
}

final class SparseDocumentTask(
  retainTime: Duration,
  snapshotTS: Timestamp,
  policy: RetentionPolicy = RetentionPolicy.Default)
    extends RangeIteratee[(ScopeID, DocID), StorageVersion] {

  private[this] val retentionTS = snapshotTS - retainTime

  def apply(row: (ScopeID, DocID)) = {
    val (scope, id) = row

    val dbQ = policy.isDatabaseRetained(scope, retentionTS)
    val collQ = policy.isCollectionRetained(scope, id.collID, retentionTS)

    // Always consider Database documents to be "Live". A deleted
    // Database document (and native index entries that point to it)
    // will be GC'ed by CollectionCompaction after its history
    // retention period has passed.
    val isDBDoc = id.collID == DatabaseID.collID

    Query.repo flatMap { repo =>
      (dbQ, collQ) par {
        case (Deleted, _) | (_, Deleted) if !isDBDoc =>
          repo.stats.incr("Document.Deleted")

          // Invariant: once deleted, a Collection or Database may not
          // be restored after the retention window. Ergo, any
          // documents therein may be removed without concern for
          // snapshot isolation.
          //
          // Associated index data is removed by SchemaDeletion.
          //
          // See CoreConfig.schema_retention_days and RecoverFunction.
          Store.clear(scope, id) map { _ => None }
        case (Retained, _) | (_, Retained) if !isDBDoc =>
          repo.stats.incr("Document.Retained")
          Query.none
        case _ =>
          repo.stats.incr("Document.Live")
          Query.none
      }
    }
  }
}
