package fauna.repo.test

import fauna.atoms._
import fauna.prop._
import fauna.repo.store._

class SchemaVersionSnapshotSpec extends Spec {
  val ctx = CassandraHelper.context("repo")

  def scope = Prop.const(ScopeID(ctx.nextID()))

  "works" in {
    val ctx = CassandraHelper.context("repo")
    val scope = ScopeID(ctx.nextID())

    (ctx ! CacheStore.getLastSeenSchemaUncached(scope)) shouldEqual None

    ctx ! CacheStore.updateSchemaVersion(scope)
    val sv1 = ctx ! CacheStore.getLastSeenSchemaUncached(scope)
    (sv1.get > SchemaVersion.Min) should be(true)

    ctx ! CacheStore.updateSchemaVersion(scope)
    val sv2 = ctx ! CacheStore.getLastSeenSchemaUncached(scope)
    (sv2.get > sv1.get) should be(true)
  }
}
