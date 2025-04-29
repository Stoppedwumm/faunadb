package fauna.model.tasks

import fauna.atoms._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.repo._
import fauna.repo.query.Query
import fauna.storage.api.scan._
import fauna.storage.index._
import fauna.storage.ir.DocIDV
import fauna.storage.Tables
import scala.concurrent.duration._

object SparseIndexTask {
  def apply(retainTime: Duration)(snapshotTS: Timestamp) =
    new SparseIndexTask(retainTime, snapshotTS)
}

final class SparseIndexTask(
  retainTime: Duration,
  snapshotTS: Timestamp,
  policy: RetentionPolicy = RetentionPolicy.Default)
    extends RangeIteratee[KeyScan.Entry, Unit] {

  private[this] val retentionTS = snapshotTS - retainTime

  def apply(row: KeyScan.Entry) =
    Query.repo flatMap { repo =>
      val dbQ = policy.isDatabaseRetained(row.scope, retentionTS)
      val indexQ = policy.isIndexRetained(row.scope, row.index, retentionTS)

      // For the  DocumentsByCollection and  ChangesByCollection native  index, clear
      // index entries based on the Collection retention decision.
      val docsByCollQ = row.index match {
        case NativeIndexID(
              NativeIndexID.DocumentsByCollection |
              NativeIndexID.ChangesByCollection) =>
          val (_, _, terms) = Tables.Indexes.decode(row.key.duplicate)

          terms.head match {
            case DocIDV(CollectionID(collID)) =>
              policy.isCollectionRetained(row.scope, collID, retentionTS)

            case term =>
              Query.fail(
                new IllegalStateException(
                  s"""Invalid term in DocumentsByCollection index for
                    scopeID=${row.scope}: '$term' is not a CollectionID."""))
          }

        case _ => Query.value(Live)
      }

      // Keep index entries that allow us to identify deleted Database documents.
      // These entries are needed by DocGC in order to clean up all documents in
      // the database & its children. The retained index entries will
      // be deleted by DocGC when the Database document itself is GC'ed.
      val isDbIndex = row.index match {
        case NativeIndexID(NativeIndexID.DatabaseByName)     => true
        case NativeIndexID(NativeIndexID.DatabaseByDisabled) => true
        case NativeIndexID(NativeIndexID.DocumentsByCollection) =>
          val (_, _, terms) = Tables.Indexes.decode(row.key.duplicate)
          terms.head == DocIDV(DatabaseID.collID.toDocID)
        case _ => false
      }

      if (isDbIndex) {
        repo.stats.incr("Indexes.Live")
        Query.none
      } else {
        (dbQ, indexQ, docsByCollQ) par {
          case (Deleted, _, _) | (_, Deleted, _) | (_, _, Deleted) =>
            repo.stats.incr("Indexes.Deleted")

            // Invariant: once deleted, an Index or Database may not
            // be restored after the retention window. Ergo, any
            // documents therein may be removed without concern for
            // snapshot isolation.
            //
            // Associated document data is removed by
            // SparseDocumentTask.
            //
            // See CoreConfig.schema_retention_days and RecoverFunction.
            Store.clear(row.key) map { _ => None }
          case (Retained, _, _) | (_, Retained, _) | (_, _, Retained) =>
            repo.stats.incr("Indexes.Retained")
            Query.none
          case _ =>
            repo.stats.incr("Indexes.Live")
            Query.none
        }
      }
    }
}
