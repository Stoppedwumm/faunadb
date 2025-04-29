package fauna.model.gc

import fauna.atoms._
import fauna.lang._
import fauna.lang.syntax._
import fauna.model._
import fauna.model.schema._
import fauna.repo.{ MVTProvider => RepoMVTProvider, _ }
import fauna.repo.query._
import fauna.storage.api._
import fauna.storage.api.set._
import fauna.storage.index._
import fauna.storage.ir._

/** Provides MVTs based on the target collection history retention. */
object MVTProvider extends RepoMVTProvider {

  def get(scopeID: ScopeID, collID: CollectionID): Query[Timestamp] =
    Collection.deriveMinValidTime(scopeID, collID)

  def get(cfg: IndexConfig, terms: Vector[Term]): Query[MVTMap] =
    cfg.id match {
      case NativeIndexID(NativeIndexID.DocumentsByCollection) | NativeIndexID(
            NativeIndexID.ChangesByCollection) =>
        // Special case on the documents index and ChangesByCollection index.
        // In both scenarios we want the MVT for just the collection that the
        // index term pertains too. Both indexes use the collection id as their
        // index term.
        collectionBasedIndexMVT(cfg.scopeID, terms)

      case _ =>
        cfg.sources match {
          case IndexSources.All            => allMVTs(cfg.scopeID)
          case IndexSources.Custom         => customMVTs(cfg.scopeID)
          case IndexSources.Limit(collIDs) => collectionMVTs(cfg.scopeID, collIDs)
        }
    }

  /** This method is used to obtain the MVT for internal indexes
    * that exist for a collection and have the collection id as
    * their first term.  Currently that ends up being:
    * 1. DocumentsByCollection
    * 2. ChangesByCollection
    */
  private def collectionBasedIndexMVT(
    scopeID: ScopeID,
    terms: Vector[Term]): Query[MVTMap] = {

    val collIDOpt =
      terms.headOption
        .map { _.unwrap }
        .collect { case DocIDV(CollectionID(id)) => id }

    collIDOpt match {
      case Some(collID) =>
        Collection.deriveMinValidTime(scopeID, collID) map { mvt =>
          Map(collID -> mvt)
        }
      case None =>
        throw new IllegalArgumentException(
          "collection id required when querying the documents index.")
    }
  }

  private def allMVTs(scopeID: ScopeID): Query[MVTMap] =
    getAllMVTs(
      scopeID,
      CollectionID
        .getAllUserDefined(scopeID)
        .appendT(Query.value(Page(NativeCollectionID.All)))
    )

  private def customMVTs(scopeID: ScopeID): Query[MVTMap] =
    getAllMVTs(scopeID, CollectionID.getAllUserDefined(scopeID))

  private def collectionMVTs(
    scopeID: ScopeID,
    collIDs: Set[CollectionID]): Query[MVTMap] =
    getAllMVTs(scopeID, Query.value(Page(collIDs)))

  private def getAllMVTs(
    scopeID: ScopeID,
    collIDs: PagedQuery[Iterable[CollectionID]]): Query[MVTMap] = {

    val mvtsQ =
      collIDs.foldLeftValuesMT(Map.empty[CollectionID, Timestamp]) {
        case (mvts, collID) =>
          get(scopeID, collID) map { mvt =>
            mvts + (collID -> mvt)
          }
      }

    mvtsQ map { MVTMap(_) }
  }

}
