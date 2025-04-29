package fauna.repo

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.repo.query.Query
import fauna.storage.api.set.CollectionAtTSSentinel
import fauna.storage.index._
import fauna.storage.lookup._
import fauna.storage.{ BiTimestamp, Remove, Unresolved }
import scala.concurrent.duration._

package object store {
  /**
   * Called mainly on IndexSet.snapshot()
   */
  def CollectionAtTS(evs: PagedQuery[Iterable[IndexValue]], snapshotTS: Timestamp): PagedQuery[Iterable[IndexValue]] =
    Query.snapshotTime flatMap { snapTs =>
      evs.reduceStreamT(CollectionAtTSSentinel) {
        case (Some(curr), prev) if curr.ts.validTS <= snapshotTS =>
          if (curr.tuple != prev.tuple) {
            (if (prev.isCreate) List(prev) else Nil, curr)
          } else {
            (Nil, if (curr.ts.validTS >= prev.ts.validTS) curr else prev)
          }
        case (Some(_), prev) => (Nil, prev)
        case (None, prev)    => (if (prev.isCreate) List(prev) else Nil, prev)
      }.rejectT { curr =>
        curr.tuple.ttl exists { _ <= snapTs }
      }
    }

  private val LookupEntrySentinel: LookupEntry =
    LiveLookup(ScopeID.MaxValue, ScopeID.MaxValue, DocID.MaxValue, Unresolved, Remove)

  def CollectionAtTS(
    lookups: PagedQuery[Iterable[(GlobalID, LookupEntry)]],
    snapshotTS: Timestamp,
    retentionPeriod: FiniteDuration): PagedQuery[Iterable[LookupEntry]] = {

    val retention = snapshotTS - retentionPeriod

    def isRetained(ts: BiTimestamp) =
      ts != Unresolved && ts.validTS >= retention

    lookups.reduceStreamT(LookupEntrySentinel) {
      case (Some(curr), prev) if curr._2.ts.validTS <= snapshotTS =>
        val currID = (curr._2.globalID, curr._2.scope, curr._2.id)
        val prevID = (prev.globalID, prev.scope, prev.id)

        if (currID != prevID) {
          (if (prev.isCreate || isRetained(prev.ts)) List(prev) else Nil, curr._2)
        } else {
          (Nil, if (curr._2.ts.validTS >= prev.ts.validTS) curr._2 else prev)
        }
      case (Some(_), prev) => (Nil, prev)
      case (None, prev)    =>
        (if (prev.isCreate || isRetained(prev.ts)) List(prev) else Nil, prev)
    }
  }
}
