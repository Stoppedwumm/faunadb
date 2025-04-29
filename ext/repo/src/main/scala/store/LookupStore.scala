package fauna.repo.store

import fauna.atoms._
import fauna.lang._
import fauna.lang.syntax._
import fauna.repo._
import fauna.repo.query.Query
import fauna.storage._
import fauna.storage.api.{ Lookups => LookupsByID }
import fauna.storage.api.scan.LookupScan
import fauna.storage.lookup._
import fauna.storage.ops._
import fauna.trace._
import scala.concurrent.duration._

/** LookupStore provides a map from a GlobalID to a scoped DocID.
  */
object LookupStore {
  type Key = Tables.Lookups.Key

  def add(entry: LookupEntry, occNonExistence: Boolean = false): Query[Unit] = {
    val write = LookupAdd(entry)
    val qWrite = Query.write(write) map { _ =>
      if (isTracingEnabled) {
        traceMsg(s"  INSERT Lookups -> $entry")
      }
    }
    if (occNonExistence) {
      qWrite flatMap { _ =>
        Query.updateState { state =>
          state.recordRead(Tables.Lookups.CFName, write.rowKey, Timestamp.Epoch)
        }
      } join
    } else {
      qWrite
    }
  }

  def remove(entry: LookupEntry): Query[Unit] =
    Query.write(LookupRemove(entry)) map { _ =>
      if (isTracingEnabled) {
        traceMsg(s"  REMOVE Lookups -> $entry")
      }
    }

  def localScan(bounds: ScanBounds): PagedQuery[Iterable[(GlobalID, LookupEntry)]] =
    Query.snapshotTime flatMap { snapTS =>
      val scan = LookupScan(
        snapTS,
        LookupScan.Cursor(
          ScanSlice(Tables.Lookups.CFName, bounds),
          skipStart = false))
      Page.unfold(scan) { op =>
        Query.scan(op) map { res =>
          (res.values, res.next map { slice => LookupScan(snapTS, slice) })
        }
      }
    } mapValuesT { entry => (entry.globalID, entry) }

  def repair(entry: LookupEntry, dryRun: Boolean): Query[Int] = {
    val q = exists(entry) flatMap {
      case true => Query(0)
      case false =>
        if (dryRun) {
          Query(1)
        } else {
          Query.write(LookupRepair(entry)) map { _ =>
            if (isTracingEnabled) {
              traceMsg(s"  REPAIR Lookups -> $entry")
            }
            1
          }
        }
    }

    Query.disableConcurrencyChecks(q)
  }

  def dumpDatabases(snapTS: Timestamp): Query[Seq[DumpEntry]] = {
    Query.repo flatMap { repo =>
      val entriesQ = repo.keyspace.localSegments map { seg =>
        CollectionAtTS(
          localScan(ScanBounds(seg)),
          snapTS,
          0.days
        ) rejectT {
          _.globalID match {
            case GlobalKeyID(_) => true
            case _              => false
          }
        } mapValuesT { entry =>
          DumpEntry(
            entry.globalID,
            entry.scope,
            entry.id.as[DatabaseID],
            Option.when(entry.isDelete)(entry.ts.validTS)
          )
        } flattenT
      } sequence

      entriesQ map { entries =>
        val rootEntries = Seq(
          DumpEntry(ScopeID.RootID, ScopeID.RootID, DatabaseID.RootID, None),
          DumpEntry(
            GlobalDatabaseID.MinValue,
            ScopeID.RootID,
            DatabaseID.RootID,
            None)
        )

        rootEntries ++ entries.flatten
      }
    }
  }

  private[repo] def byID(gID: GlobalID): PagedQuery[Iterable[LookupEntry]] =
    Query.snapshotTime flatMap { snapTS =>
      val readOp = LookupsByID(LookupEntry.MaxValue.copy(globalID = gID), snapTS)
      Page.unfold(readOp) { op =>
        Query.read(op) map { res =>
          (res.values, res.next)
        }
      }
    } spansT { case (a, b) =>
      a.scope == b.scope && a.id == b.id
    }

  def rowKey(id: GlobalID): Array[Byte] = Tables.Lookups.rowKey(id)

  private def exists(entry: LookupEntry): Query[Boolean] =
    Query.snapshotTime flatMap { snapTS =>
      Query.read(LookupsByID(entry, snapTS, 1)) map { res =>
        !res.values.isEmpty && res.values.head == entry
      }
    }
}
