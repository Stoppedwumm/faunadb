package fauna.repair

import fauna.atoms.{ DatabaseID, HostID }
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.repo.service.StorageService
import fauna.storage.api.version.{ DocHistory, StorageVersion }
import fauna.storage.api.Lookups
import fauna.storage.lookup.LookupEntry
import fauna.storage.TxnRead
import fauna.tx.transaction.Partitioner
import io.netty.buffer.ByteBuf
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.DurationInt

/** This class is used to obtain lookup information across hosts for the
  * provided LookupData.  This includes the lookup entry as well as version
  * history associated with the provided LookupData.
  * This class utilizes an Either to return a Right when all nodes agree
  * on the state of a lookup.  When the nodes have different results for
  * a given lookup entry it will return a Left(String).
  */
final class HostLookups(
  storageService: StorageService,
  partitioner: => Partitioner[TxnRead, _]) {
  def getLookup(lookupData: LookupData, readTS: Timestamp)(
    implicit ec: ExecutionContext): Future[Either[String, Option[LookupEntry]]] = {
    val lookupReadOp =
      Lookups(LookupEntry.MaxValue.copy(globalID = lookupData.globalID), readTS)
    val hostIDs = getHostIDs(lookupReadOp.rowKey)
    Future
      .sequence(hostIDs.map { hostID =>
        storageService
          .readFrom(
            lookupData.globalID,
            hostID,
            lookupReadOp,
            Iterable.empty,
            1.minute.bound
          )
          .map { lr =>
            if (lr.next.isDefined) {
              // first want to see if this happens, seems possible that for any given
              // global id all of its mappings fit in one page.
              throw new IllegalStateException(
                s"The lookups result contained another page which is currently unaccounted for.  Pagination across the lookup result must first be implemented to ensure accurate resutls.")
            }
            val possibleLookupEntry = lr.values
              .foldLeft(Option.empty[LookupEntry]) {
                case (None, b) => Some(b)
                case (Some(a), b) =>
                  Some(LookupEntry.latestForGlobalID(a, b))
              }
              .flatMap { le =>
                // if the most recent lookup for the global id doesn't match the
                // lookup entry provided in the
                // diff then we want to filter it out as the host effectively
                // 'doesn't' have an entry for
                // that lookup
                Option.when(
                  le.id == lookupData.docID && le.scope == lookupData.scope && le.globalID == lookupData.globalID
                ) { le }
              }
            hostID -> possibleLookupEntry
          }
      })
      .map { results =>
        val headResult = results.head._2
        if (results.forall(_._2 == headResult)) {
          Right(headResult)
        } else {
          Left(
            s"Hosts contained differing lookup entries for lookup: $lookupData\n${results.mkString("\n")}"
          )
        }
      }
  }

  def getDocHistoryForLookup(lookup: LookupData, readTS: Timestamp)(
    implicit ec: ExecutionContext): Future[Either[String, Option[StorageVersion]]] = {
    assert(
      lookup.docID.collID == DatabaseID.collID,
      s"Received lookup doc id that isn't a databse id, ${lookup.docID}. We use epoch as MVT for the doc history lookup so need to ensure the collection has infinite retention."
    )
    val docHistoryReadOp = DocHistory(
      scopeID = lookup.scope,
      docID = lookup.docID,
      cursor = DocHistory.Cursor(),
      snapshotTS = readTS,
      minValidTS = Timestamp.Epoch,
      maxResults = DocHistory.DefaultMaxResults
    )
    val hostIDs = getHostIDs(docHistoryReadOp.rowKey)
    Future
      .sequence(hostIDs.map { hostID =>
        storageService
          .readFrom(
            lookup.globalID,
            hostID,
            docHistoryReadOp,
            Iterable.empty,
            1.minute.bound
          )
          .map(hostID -> _.versions)
      })
      .map { results =>
        val currentVersions = results.map(_._2.headOption)
        if (currentVersions.forall(_ == currentVersions.head)) {
          Right(currentVersions.head)
        } else {
          Left(
            s"Hosts contained different versions for the lookup: $lookup\n${results.mkString("\n")}")
        }
      }
  }

  private def getHostIDs(rowKey: ByteBuf): Set[HostID] = {
    val hostIDs =
      partitioner.hostsForRead(TxnRead.inDefaultRegion(rowKey))
    assert(hostIDs.nonEmpty, "Expected to find host ids for read, but found none.")
    hostIDs
  }
}
