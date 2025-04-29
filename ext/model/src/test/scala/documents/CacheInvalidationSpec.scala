package fauna.model.test

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.prop.Generators
import fauna.repo._
import fauna.repo.query.Query
import fauna.repo.test.{ CassandraHelper, Spec }
import fauna.storage.doc.Data

class CacheInvalidationSpec extends Spec with Generators {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")
  val scope = ctx ! newScope

  after {
    CassandraHelper.initInvalidationService(ctx, None)
  }

  def check(collID: CollectionID): Unit = {
    var invalidated = false
    var ts = Timestamp.Epoch
    def invalidate(scopeID: ScopeID, docID: DocID, txnTS: Timestamp): Query[Unit] = {
      invalidated = (scope == scopeID) && (docID.collID == collID)
      ts = txnTS
      Query.unit
    }

    CassandraHelper.initInvalidationService(ctx, Some(invalidate))
    val res =
      ctx !! Store.insertUnmigrated(scope, DocID(SubID(1), collID), Data.empty)
    assert(invalidated, "cache was not invalidated")
    assert(ts == res.transactionTS, "cache was not invalidated at proper time")
  }

  "Cache Invalidation" - {
    "invalidates collections" in check(CollectionID.collID)
    "invalidates databases" in check(DatabaseID.collID)
    "invalidates indexes" in check(IndexID.collID)
    "invalidates roles" in check(RoleID.collID)
    "invalidates functions" in check(UserFunctionID.collID)
    "invalidates tokens" in check(TokenID.collID)
    "invalidates keys" in check(KeyID.collID)
    "invalidates access_providers" in check(AccessProviderID.collID)
  }

  "tolerate failures" in {
    var shouldFail = true
    var invalidated = false
    var ts = Timestamp.Epoch
    val docID = DocID(SubID(1), CollectionID.collID)

    def invalidate(scopeID: ScopeID, docID: DocID, txnTS: Timestamp) = {
      if (shouldFail) {
        shouldFail = false
        Query.fail(new IllegalArgumentException())
      } else {
        invalidated = (scope == scopeID) && (docID.collID == CollectionID.collID)
        ts = txnTS
        Query.unit
      }
    }

    CassandraHelper.initInvalidationService(ctx, Some(invalidate))

    // first failed attempt
    ctx ! Store.insertUnmigrated(scope, docID, Data.empty)
    assert(!shouldFail, "invalidation was not called")
    assert(!invalidated, "cache was erroneously invalidated")

    // second attempt should succeed
    val res = ctx !! Store.insertUnmigrated(scope, docID, Data.empty)
    assert(invalidated, "cache was not invalidated")
    assert(ts == res.transactionTS, "cache was not invalidated at proper time")
  }
}
