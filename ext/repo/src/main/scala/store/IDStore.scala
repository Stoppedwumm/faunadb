package fauna.repo.store

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.repo.query.Query
import fauna.repo.{ IndexConfig, Store }
import fauna.storage.Tables
import fauna.storage.index.{ IndexTerm, IndexValue }
import fauna.storage.ir.DocIDV

object IDStore {
  def nextSnowflakeID(scope: ScopeID, coll: CollectionID): Query[DocID] =
    Query.nextID flatMap { sub =>
      val id = DocID(SubID(sub), coll)
      Query.valueWithStateUpdate(id) { state =>
        // register OCC check at Epoch for the new document. This will ensure
        // that the create contends with any concurrent write to the same ID.
        // This is not a 100% guarantee since the OCC timestamp for an untouched
        // doc will eventually age out (see _RowTimestamps_ CF TTL in Tables.scala).
        val key = Tables.Versions.rowKeyByteBuf(scope, id)
        state.recordRead(Tables.Versions.CFName, key, Timestamp.Epoch)
      }
    }

  def nextSequentialID(scope: ScopeID, coll: CollectionID, min: Long, max: Long): Query[Option[DocID]] = {
    val idx = IndexConfig.DocumentsByCollection(scope)
    val term = Vector(IndexTerm(DocIDV(coll.toDocID)))

    val startQ = Store
      .sortedIndex(idx, term, IndexValue.MaxValue, IndexValue.MinValue, 1, false)
      .headValueT
      .map {
        case Some(iv) => iv.docID.subID.toLong + 1
        case None => min
      }

    // We rely on the index above as a starting hint, but search for
    // the next ID based on instance state. This is necessary because
    // our native class indexes are not serialized, and so relying
    // solely on index state is subject to write skew. By checking for
    // an existing version itself, the schema creation query will be
    // properly serialized with another concurrent query competing for
    // a sequential ID.
    def next0(sub: Long): Query[Option[DocID]] =
      if (sub < min || sub > max) {
        Query.none
      } else {
        val id = DocID(SubID(sub), coll)
        Store.getLatestNoTTLUnmigrated(scope, id) flatMap {
          case Some(_) => next0(sub + 1)
          case None => Query.value(Some(id))
        }
      }

    startQ flatMap next0
  }
}
