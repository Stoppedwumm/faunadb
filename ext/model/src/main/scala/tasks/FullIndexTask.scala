package fauna.model.tasks

import fauna.lang.Timestamp
import fauna.repo._
import fauna.repo.query.Query
import fauna.storage.api.scan._
import scala.concurrent.duration._

object FullIndexTask {
  def apply(retainTime: Duration, tombstone: ElementScan.Entry => Query[Unit])(
    snapshotTS: Timestamp) =
    new FullIndexTask(retainTime, snapshotTS, tombstone = tombstone)
}

final class FullIndexTask(
  retainTime: Duration,
  snapshotTS: Timestamp,
  policy: RetentionPolicy = RetentionPolicy.Default,
  tombstone: ElementScan.Entry => Query[Unit])
    extends RangeIteratee[ElementScan.Entry, Unit] {

  private[this] val retentionTS = snapshotTS - retainTime

  def apply(entry: ElementScan.Entry) =
    Query.repo flatMap { repo =>
      policy.isCollectionRetained(
        entry.scope,
        entry.doc.collID,
        retentionTS) flatMap { decision =>
        decision match {
          case Live =>
            repo.stats.incr("Indexes.Elements.Live")
            Query.none
          case Retained =>
            repo.stats.incr("Indexes.Elements.Retained")
            Query.none
          case Deleted =>
            repo.stats.incr("Indexes.Elements.Deleted")
            // Invariant: once deleted, a Collection may not
            // be restored after the retention window. Ergo, any
            // documents therein may be removed without concern for
            // snapshot isolation.
            //
            // Associated document data is removed by
            // SparseDocumentTask.
            //
            // See CoreConfig.schema_retention_days and RecoverFunction.
            tombstone(entry) map { _ => None }
        }
      }
    }
}
