package fauna

import fauna.atoms._
import fauna.lang._
import fauna.lang.syntax._
import fauna.model.schema._
import fauna.repo._
import fauna.storage.api.set._
import fauna.storage.ir._

package object model {

  implicit class AllIDsOps[T <: ID[T]](companion: IDCompanion[T]) {

    /** Return all user defined documents for the coll. ID in the given scope. */
    def getAllUserDefined(
      scope: ScopeID,
      ts: Timestamp = Timestamp.MaxMicros): PagedQuery[Iterable[T]] =
      getAllIDs(scope, ts)(companion.collIDTag)
  }

  private def getAllIDs[T](scope: ScopeID, ts: Timestamp)(
    implicit tag: CollectionIDTag[T]): PagedQuery[Iterable[T]] = {
    val terms = Vector(Scalar(DocIDV(tag.collID.toDocID)))
    val idx = NativeIndex.DocumentsByCollection(scope)
    Store.collection(idx, terms, ts) mapValuesT { _.docID.as[T] }
  }
}
