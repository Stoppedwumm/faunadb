package fauna.model.tasks

import fauna.atoms._
import fauna.lang.syntax._
import fauna.model._
import fauna.lang.Timestamp
import fauna.repo._
import fauna.repo.doc._
import fauna.repo.query.Query
import fauna.storage.Conflict
import fauna.storage.ops.SetBackfill
import fauna.trace._

object IndexRepair {

  object VersionTask {

    def apply(effect: Repair.Effect)(snapshotTS: Timestamp) =
      new VersionTask(snapshotTS, effect)
  }

  /**
    * Responsible for adding missing index entries based on document versions.
    */
  final case class VersionTask(snapshotTS: Timestamp, effect: Repair.Effect)
      extends RangeIteratee[(ScopeID, DocID), Conflict[Version]] {

    def apply(rowID: (ScopeID, DocID)) = {
      val (scope, id) = rowID
      val dbQ = Database.isDeleted(scope)
      val classQ = Collection.exists(scope, id.collID)
      val indexerQ = Index.getIndexer(scope, id.collID)

      (dbQ, classQ, indexerQ) par {
        case (false, true, indexer) => Query.some(colIter(scope, id, indexer))
        case _                      => Query.none
      }
    }

    private def colIter(
      scope: ScopeID,
      id: DocID,
      indexer: Indexer): ColIteratee[Conflict[Version]] =
      ColIteratee {
        case None => Query.none
        case Some(page) =>
          Query.repo flatMap { repo =>
            val rebuilds = page filter { _.ts.validTS <= snapshotTS } map { res =>
              rebuild(repo, res.canonical, indexer)
            }

            rebuilds.accumulate(0) { _ + _ } map { added =>
              if (added > 0) {
                repo.stats.count("Index.Events.Added", added)
              }

              Some(colIter(scope, id, indexer))
            }
          }
      }

    /**
      * Given a Version and an Indexer, computes the necessary index
      * rows, and inserts any missing rows. Returns the number of
      * missing rows.
      */
    private def rebuild(
      repo: RepoContext,
      version: Version,
      indexer: Indexer): Query[Int] =
      indexer.rows(version) flatMap { rows =>
        val adds = rows map { row =>
          entryExists(repo, row) flatMap {
            case (true, true) => Query.value(0)
            case (snapExists, histExists) =>
              if (effect.isDryRun) {
                Query.value(1)
              } else {
                val op = SetBackfill(
                  row.key,
                  row.value,
                  missingSorted = !snapExists,
                  missingHistorical = !histExists)

                Query.write(op) map { _ =>
                  if (isTracingEnabled) {
                    traceMsg(
                      s"  REPAIR Indexes -> $row hist: $histExists snap: $snapExists")
                  }

                  1
                }
              }
          }
        }

        adds.accumulate(0) { _ + _ }
      }

    private def entryExists(
      repo: RepoContext,
      row: IndexRow): Query[(Boolean, Boolean)] =
      Query.future {
        repo.keyspace.indexExists(
          row.key,
          row.value,
          snapshotTS,
          repo.repairTimeout.bound)
      }

  }
}
