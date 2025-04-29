package fauna.repo.cache

import fauna.atoms.{ SchemaVersion, ScopeID }
import fauna.exec.FaunaExecutionContext
import fauna.lang.syntax._
import fauna.lang.Caffeine
import fauna.repo.query.Query
import fauna.repo.store.CacheStore
import fauna.repo.RepoContext
import fauna.stats.StatsRecorder
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

object SchemaCache {
  val DefaultMaxScopes = 500000
  val DefaultMaxSchemaSizeMB = 16
  val DefaultWeightAccuracy = 8
  val DefaultExpireRate = 5.minutes
  val DefaultRefreshRate = 5.minutes

  def apply(
    maxScopes: Long = DefaultMaxScopes,
    maxSchemaSizeMB: Long = DefaultMaxSchemaSizeMB,
    weightAccuracy: Int = DefaultWeightAccuracy,
    expireRate: FiniteDuration = DefaultExpireRate,
    refreshRate: FiniteDuration = DefaultRefreshRate,
    testing: Boolean = false,
    executor: ExecutionContext = FaunaExecutionContext.Implicits.global
  ): SchemaCache = {
    new SchemaCache(
      maxScopes,
      maxSchemaSizeMB,
      weightAccuracy,
      expireRate,
      refreshRate,
      testing
    )(executor)
  }

  // This wrapper is necesary because Caffeine only supports Object (AnyRef)
  // keys, which ScopeID is not.
  private[SchemaCache] final case class VersionKey(scope: ScopeID)
}

/** A cache for schema items. Most uses of this class should go through CacheStore.
  */
final class SchemaCache private (
  val maxScopes: Long,
  val maxSchemaSizeMB: Long,
  val weightAccuracy: Int,
  val expireRate: FiniteDuration,
  val refreshRate: FiniteDuration,
  val testing: Boolean
)(implicit ec: ExecutionContext) {

  import CacheStore.SchemaKey
  import SchemaCache.VersionKey

  def setRepo(repo: RepoContext) = {
    versionCache.setRepo(repo)
    itemCache.setRepo(repo)
  }

  def reportAndResetStats(stats: StatsRecorder) = {
    versionCache.reportAndResetStats(stats)
    itemCache.reportAndResetStats(stats)
  }

  private[this] val versionCache = {
    val cb = if (testing) {
      Caffeine.newBuilder
        .maxSize(1024)
    } else {
      Caffeine.newBuilder
        .maxSize(maxScopes)
        .expireAfterAccess(expireRate)
        .refreshRate(refreshRate)
    }

    QueryCache[VersionKey, Option[SchemaVersion]]("SchemaVersions", cb) { k =>
      CacheStore.getLastSeenSchemaUncached(k.scope).map(Some(_))
    }
  }

  private[this] val itemCache = {
    val cb = if (testing) {
      Caffeine.newBuilder
        .maxSize(1024)
        .executor(ec.execute)
    } else {
      Caffeine.newBuilder
        .maxWeight(maxSchemaSizeMB * 1024 * 1024)
        .expireAfterAccess(expireRate)
        .executor(ec.execute)
    }

    QueryCache[SchemaKey[_], (Any, Option[SchemaVersion])](
      "SchemaItems",
      cb,
      Some(weightAccuracy)) { k =>
      // Look up the persisted schema valid time as well.
      val schemaQ = CacheStore.getLastSeenSchemaUncached(k.scope)
      schemaQ flatMap { sv => k.query mapT { v => (v, sv) } }
    }
  }

  def getLastSeenSchema(scope: ScopeID): Query[Option[SchemaVersion]] =
    versionCache.get(VersionKey(scope)) map { _.get }

  def invalidateScopeBefore(scope: ScopeID, ver: Option[SchemaVersion]): Boolean =
    versionCache.updateIfPresent(VersionKey(scope)) { sv =>
      val sv0 = sv.getOrElse(SchemaVersion.Min)
      val ver0 = ver.getOrElse(SchemaVersion.Min)
      if (sv0 < ver0) None else Some(sv)
    }

  def getItem[V](key: SchemaKey[V]): Query[Option[(V, Option[SchemaVersion])]] =
    itemCache.get(key).asInstanceOf[Query[Option[(V, Option[SchemaVersion])]]]

  def invalidateItem(key: SchemaKey[_]): Unit =
    itemCache.invalidate(key)

  def invalidate() = {
    versionCache.invalidateAll()
    itemCache.invalidateAll()
  }

  // for testing
  def update[V](key: SchemaKey[V], v: V): Unit = {
    require(testing)
    versionCache.update(VersionKey(key.scope), None)
    itemCache.update(key, (v, None))
  }
}
