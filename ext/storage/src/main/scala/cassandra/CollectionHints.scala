package fauna.storage.cassandra

import com.carrotsearch.hppc.ShortLongWormMap
import com.github.benmanes.caffeine.cache._
import com.github.benmanes.caffeine.cache.stats.CacheStats
import fauna.atoms._
import fauna.lang.Timestamp
import fauna.stats.StatsRecorder
import scala.concurrent.duration._
import scala.jdk.DurationConverters._

object CollectionHints {

  /** The maximum number of scopes for which to maintain hints. */
  val MaximumSize =
    Option(System.getProperty("fauna.collection-hints.maximum-size"))
      .flatMap { _.toIntOption }
      .getOrElse(10_000)

  /** The amount of time after last access after which a hint is collectible. */
  val Expiration = {
    val duration =
      Option(System.getProperty("fauna.collection-hints.expiration"))
        .map { Duration(_) }
        .getOrElse(1.hour)

    if (duration.isFinite) {
      duration.asInstanceOf[FiniteDuration]
    } else {
      sys.error(s"Finite duration required for expiration, $duration found.")
    }
  }

  /** The amount of time to keep versions below the hinted MVT. */
  val GCGrace = {
    val gcGrace =
      Option(System.getProperty("fauna.collection-hints.gc-grace"))
        .map { Duration(_) }
        .getOrElse(1.hour)

    if (gcGrace.isFinite) {
      gcGrace.asInstanceOf[FiniteDuration]
    } else {
      sys.error(s"Finite duration required for gc grace, $gcGrace found.")
    }
  }
}

/** This class maintains a set of minimum valid timestamps (MVTs) for use during a
  * CollectionStrategy compaction.
  *
  * Hints may come from different sources but, most likely, from read ops. Because a
  * collections' MVTs are computed by the query coordinator, their unsynchronized
  * clocks may cause hints to jitter based on the average clock skew. In order to
  * prevent compaction from removing data that was supposed to be visible to a query
  * running with a stale read clock, this class remembers MVTs fed via `put()` but
  * returns the latest seen MVT minus the `GCGrace` period on `get()` so that
  * collection compaction is only ever discarding versions behind every
  * compute-nodes' read boundary.
  */
final class CollectionHints
    extends PartialFunction[(ScopeID, CollectionID), Timestamp] {

  import CollectionHints._

  private[this] val hints: Cache[ScopeID, ShortLongWormMap] = Caffeine.newBuilder
    .maximumSize(MaximumSize)
    .expireAfterAccess(Expiration.toJava)
    .softValues()
    .recordStats
    .build()

  private[this] var stats: CacheStats = hints.stats()
  private[this] var recorder: StatsRecorder = StatsRecorder.Null

  StatsRecorder.polling(10.seconds) {
    // Don't bother producing stats if they'll end up in the bit
    // bucket.
    if (recorder != StatsRecorder.Null) {
      val prev = stats
      stats = hints.stats()
      val delta = stats.minus(prev)

      recorder.count("CollectionHints.Hits", delta.hitCount.toInt)
      recorder.count("CollectionHints.Misses", delta.missCount.toInt)
      recorder.count("CollectionHints.Evictions", delta.evictionCount.toInt)
      recorder.set("CollectionHints.Objects", hints.estimatedSize.toInt)
    }
  }

  /** Replaces the default null StatsRecorder with `stats`, if it has not already been
    * replaced.
    */
  def setStatsRecorder(stats: StatsRecorder): Unit = synchronized {
    if (recorder == StatsRecorder.Null) {
      recorder = stats
    }
  }

  /** Associates the given MVT with the (scope, id) pair, overriding any
    * previously-associated value.
    */
  def put(scope: ScopeID, id: CollectionID, mvt: Timestamp): Unit = {
    val ids = hints.get(scope, { _ => new ShortLongWormMap })

    // Read hints and ExpiredVersionTask may race here. It's harmless,
    // but allow a single writer anyhow.
    ids.synchronized {
      ids.put(id.value, mvt.micros)
    }
  }

  /** Returns true if an MVT has been associated with the (scope, id) pair with
    * `put()`, and false otherwise.
    */
  def isDefinedAt(key: (ScopeID, CollectionID)): Boolean = {
    val (scope, id) = key
    val ids = hints.getIfPresent(scope)

    if (ids eq null) {
      false
    } else {
      ids.containsKey(id.value)
    }
  }

  /** Returns the MVT associated with the (scope, id) pair, if any, minus the
    * configurefe `GCGrace`. If no associated has been registered with `put()`,
    * returns the Epoch.
    */
  def apply(key: (ScopeID, CollectionID)): Timestamp = {
    val (scope, id) = key
    val ids = hints.getIfPresent(scope)

    if (ids eq null) {
      Timestamp.Epoch
    } else {
      val micros = ids.get(id.value)

      if (micros eq null) {
        Timestamp.Epoch
      } else {
        Timestamp.ofMicros((micros - GCGrace.toMicros) max 0)
      }
    }
  }

  /** Drops all entries in the cache. */
  def clear(): Unit = hints.invalidateAll()

}
