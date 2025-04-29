package fauna.model.tasks

import fauna.atoms._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model._
import fauna.repo._
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.repo.store._
import fauna.storage._
import fauna.storage.lookup._

object LookupRepair {

  def cleanup(lookup: LookupEntry, dryRun: Boolean): Query[Int] = {
    // we grab the head _and_ and the entry's version, so that we can detect
    // RowTombstone's via the absence of the head.
    val headQ = RuntimeEnv.Static
      .Store(lookup.scope)
      .versions(
        lookup.id,
        VersionID.MaxValue,
        VersionID.MinValue,
        1,
        false
      )
      .headValueT

    val versQ = RuntimeEnv.Static
      .Store(lookup.scope)
      .versions(lookup.id, lookup.versionID, VersionID.MinValue, 1, false)
      .headValueT

    def containsLookup(vers: Version) =
      LookupHelpers.lookups(vers).contains(lookup)

    (headQ, versQ) par {
      // not row deleted. specific version exists and generates the lookup
      case (Some(_), Some(vers)) if containsLookup(vers) => Query.value(0)

      // clean it
      case _ if dryRun => Query.value(1)
      case _           => LookupStore.remove(lookup) map { _ => 1 }
    }
  }

  object VersionTask {
    def apply(effect: Repair.Effect)(snapshotTS: Timestamp) =
      new VersionTask(snapshotTS, effect)
  }

  /** Responsible for adding missing lookup entries
    */
  case class VersionTask(snapshotTS: Timestamp, effect: Repair.Effect)
      extends RangeIteratee[(ScopeID, DocID), Version] {

    private def colIter(rowID: (ScopeID, DocID)): ColIteratee[Version] =
      ColIteratee {
        case None => Query.none
        case Some(page) =>
          val repairQs = page filter { _.ts.validTS <= snapshotTS } map { ver =>
            LookupHelpers
              .lookups(ver)
              .map { l => LookupStore.repair(l, effect.isDryRun) }
              .accumulate(0) { _ + _ }
          }

          val repairQ = repairQs.accumulate(0) { _ + _ }
          repairQ flatMap { count =>
            Query.repo map { repo =>
              if (count > 0) repo.stats.count("Lookup.Repair.Events.Added", count)
              Some(colIter(rowID))
            }
          }
      }

    def apply(rowID: (ScopeID, DocID)) = {
      val (scope, id) = rowID
      id.collID match {
        case DatabaseID.collID | KeyID.collID =>
          Database.isDeleted(scope) map {
            Option.unless(_) {
              colIter(rowID)
            }
          }

        case _ => Query.none
      }
    }
  }

  object LookupTask {
    def apply(effect: Repair.Effect)(snapshotTS: Timestamp) =
      new LookupTask(snapshotTS, effect, None)
  }

  /** Responsible for removing stale lookup entries
    */
  case class LookupTask(
    snapshotTS: Timestamp,
    effect: Repair.Effect,
    filterScope: Option[ScopeID])
      extends RangeIteratee[GlobalID, LookupEntry] {

    private def filterEntry(entry: LookupEntry): Boolean =
      filterScope.forall { _.compare(entry.globalID) == 0 } &&
        entry.ts.validTS <= snapshotTS

    private def colIter(gID: GlobalID): ColIteratee[LookupEntry] =
      ColIteratee { p =>
        Query(p) flatMapT { page =>
          Query.repo flatMap { repo =>
            val cleanupQ = page filter { filterEntry(_) } map { l =>
              cleanup(l, effect.isDryRun)
            }

            cleanupQ.accumulate(0) { _ + _ } map { count =>
              if (count > 0) repo.stats.count("Lookup.Repair.Events.Removed", count)
              Some(colIter(gID))
            }
          }
        }
      }

    def apply(gID: GlobalID) =
      Query.some(colIter(gID))
  }
}
