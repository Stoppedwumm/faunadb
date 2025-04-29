package fauna.repo

import fauna.atoms._
import fauna.lang._
import fauna.repo.query._
import fauna.storage.api._
import fauna.storage.api.set._

/** Defines the repo's MVT lookup interface. */
trait MVTProvider {

  /** Lookup the MVT for the given collection. */
  def get(scopeID: ScopeID, collID: CollectionID): Query[Timestamp]

  /** Lookup all index's source collections' MVTs. */
  def get(cfg: IndexConfig, terms: Vector[Term]): Query[MVTMap]
}

object MVTProvider {

  /** An MVT provider that returns epoch for all MVT lookups. */
  object Default extends MVTProvider {
    def get(scopeID: ScopeID, collID: CollectionID) = Query.value(Timestamp.Epoch)
    def get(cfg: IndexConfig, terms: Vector[Term]) = Query.value(MVTMap.Default)
  }
}
