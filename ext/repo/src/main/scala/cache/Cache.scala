package fauna.repo.cache

import fauna.exec.FaunaExecutionContext
import fauna.lang.Caffeine
import fauna.logging.ExceptionLogging
import fauna.repo.query.Query
import fauna.repo.RepoContext
import fauna.stats.StatsRecorder
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

trait CacheKey[V] {
  type Value = V
  def query: Query[Option[V]]
  def shouldHaveRegion = true
}

/** A cache for items which are not schema versioned. Usage should go through
  * CacheStore.
  */
object Cache {
  val DefaultMaxSizeMB = 16
  val DefaultWeightAccuracy = 8
  val DefaultExpireRate = 1.hour
  val DefaultRefreshRate = 1.minute

  def apply(
    name: String,
    maxSizeMB: Long = DefaultMaxSizeMB,
    weightAccuracy: Int = DefaultWeightAccuracy,
    expireRate: FiniteDuration = DefaultExpireRate,
    refreshRate: FiniteDuration = DefaultRefreshRate,
    testing: Boolean = false,
    executor: ExecutionContext = FaunaExecutionContext.Implicits.global
  ): Cache = {
    new Cache(
      name,
      maxSizeMB,
      weightAccuracy,
      expireRate,
      refreshRate,
      testing
    )(executor)
  }
}

// Expiration and Refresh are slightly different; this cache will make keys
// _eligible_ for refresh after the specified duration, but will only refresh at the
// next access. If an entry has not been updated - written or refreshed - within the
// expiry duration, it is evicted.
class Cache(
  val name: String,
  val maxSizeMB: Long,
  val weightAccuracy: Int,
  val expireRate: FiniteDuration,
  val refreshRate: FiniteDuration,
  val testing: Boolean
)(implicit ec: ExecutionContext)
    extends ExceptionLogging {

  def setRepo(r: RepoContext) = cache.setRepo(r)

  def reportAndResetStats(stats: StatsRecorder) =
    cache.reportAndResetStats(stats)

  private[this] val cache = {
    val cb = if (testing) {
      Caffeine.newBuilder
        .maxSize(1024)
    } else {
      Caffeine.newBuilder
        .maxWeight(maxSizeMB * 1024 * 1204)
        .expireAfterWrite(expireRate)
        .refreshRate(refreshRate)
        .executor(ec.execute)
    }

    QueryCache[CacheKey[_], Any](name, cb, Some(weightAccuracy)) { k =>
      k.query
    }
  }

  def invalidate(): Unit =
    cache.invalidateAll()

  def invalidate[K <: CacheKey[_]](key: K): Unit =
    cache.invalidate(key)

  def get[K <: CacheKey[_]](key: K): Query[Option[key.Value]] =
    cache.get(key).asInstanceOf[Query[Option[key.Value]]]

  def peek[K <: CacheKey[_]](key: K): Option[key.Value] =
    cache.peek(key).asInstanceOf[Option[key.Value]]

  // for testing. must be two param lists since v is dependent on key's type
  def update[K <: CacheKey[_]](key: K)(v: key.Value): Unit = {
    require(testing)
    cache.update(key, v)
  }
}
