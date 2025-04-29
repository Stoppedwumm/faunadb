package fauna.repo.test

import fauna.atoms.{ SchemaVersion, ScopeID }
import fauna.lang.clocks.Clock
import fauna.repo.cache.SchemaCache
import fauna.repo.query.Query
import fauna.repo.store.CacheStore
import fauna.storage.Tables
import io.netty.buffer.Unpooled
import scala.concurrent.duration._

class CacheStoreSpec extends Spec {

  var scope: ScopeID = ScopeID.RootID
  @volatile var item: Long = 0L

  before {
    scope = ScopeID(ctx.nextID())
    item = 0L
  }

  val ctx = {
    val ctxBase = CassandraHelper.context("repo")
    val schemaCache = SchemaCache(128, 2, 1, 60.seconds, 60.seconds)
    ctxBase.copy(cacheContext = ctxBase.cacheContext.copy(schema2 = schemaCache))
  }

  case class Key(scope: ScopeID, key: String) extends CacheStore.SchemaKey[Long] {
    def query = Query.some(item)
  }

  def cache = ctx.cacheContext.schema2
  def getItem(key: Key) = ctx ! CacheStore.getItem(key)

  s"CacheStore" - {
    "invalidates cached items based on schema version timestamp" in {
      val key = Key(scope, "foo")
      getItem(key).value shouldBe 0

      // item changed
      item = 1L

      // still cached
      getItem(key).value shouldBe 0

      // schema version cache invalidated, but no version change
      cache.invalidateScopeBefore(scope, Some(SchemaVersion.Max)) shouldBe true
      getItem(key).value shouldBe 0

      // schema version updated, but still cached
      ctx ! CacheStore.updateSchemaVersion(scope)
      getItem(key).value shouldBe 0

      // finally invalidated, update should now be reflected in item
      cache.invalidateScopeBefore(scope, Some(SchemaVersion.Max))
      getItem(key).value shouldBe 1
    }

    "handles timestamp epoch boundary" in {
      val schemaVersionRowKey = Tables.SchemaVersions.rowKey(scope)
      val staleTxnTS = Clock.time - 2.days
      val key = Key(scope, "foo")

      // craft a stale RowTimestamps entry
      ctx.service.storage
        .applyMutationForKey(schemaVersionRowKey) { mut =>
          mut.add(
            Tables.RowTimestamps.CFName,
            Tables.RowTimestamps.encode(staleTxnTS),
            Unpooled.EMPTY_BUFFER,
            staleTxnTS,
            ttl = 0
          )
        }

      // ensure we cache everything correctly
      ctx ! cache.getLastSeenSchema(scope) shouldEqual Some(
        SchemaVersion(staleTxnTS))
      getItem(key).value shouldBe 0

      // clear RowTimestamps entry to simulate cell expiry
      ctx.service.storage
        .applyMutationForKey(schemaVersionRowKey) { mut =>
          mut.delete(Tables.RowTimestamps.CFName, Clock.time)
        }

      cache.invalidateScopeBefore(scope, Some(SchemaVersion.Max))

      noException should be thrownBy {
        ctx ! CacheStore.getItem(key)
      }

      // simulate schema update detected through item cache refresh
      ctx ! CacheStore.updateSchemaVersion(scope)
      cache.invalidateItem(key)
      item = 1

      getItem(key).value shouldBe 1
    }
  }
}
