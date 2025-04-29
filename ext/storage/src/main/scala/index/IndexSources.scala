package fauna.storage.index

import fauna.atoms._

/**
  * Index sources constrain the indexer by filtering instances using a
  * set of ClassIDs, only emitting index entries for instances
  * matching the predicate.
  */
sealed trait IndexSources {
  def contains(cls: CollectionID): Boolean
}

object IndexSources {
  def empty: IndexSources = apply(Set.empty[CollectionID])
  def apply(collectionID: CollectionID): IndexSources = apply(Set(collectionID))
  def apply(collectionIDs: Set[CollectionID]): IndexSources = Limit(collectionIDs)

  /**
    * Represents an internal wildcard index. All documents - including
    * internal collections - will be indexed.
    */
  case object All extends IndexSources {
    def contains(cls: CollectionID): Boolean = true
  }

  /**
    * Represents a user-defined wildcard index. Only documents in
    * custom collections will be indexed.
    */
  case object Custom extends IndexSources {
    def contains(cls: CollectionID): Boolean =
      cls match {
        case UserCollectionID(_) => true
        case _                   => false
      }
  }

  /**
    * Limits indexing to documents from a finite set of collections.
    */
  case class Limit(collections: Set[CollectionID]) extends IndexSources {
    def contains(coll: CollectionID): Boolean = collections contains coll
  }
}
