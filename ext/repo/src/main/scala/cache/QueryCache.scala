package fauna.repo.cache

import fauna.exec.ImmediateExecutionContext
import fauna.lang.syntax._
import fauna.lang.Caffeine
import fauna.logging.ExceptionLogging
import fauna.repo.{ RepoContext, SchemaContentionException }
import fauna.repo.query.Query
import fauna.stats.StatsRecorder
import java.util.concurrent.atomic.LongAdder
import scala.concurrent.{ ExecutionContext, TimeoutException }
import scala.util.{ Failure, Success }

/** A cache which loads values from queries. Based on the configuration of the
  * Caffeine provided on construction, this has the ability to reload cached
  * values asynchronously.
  *
  * In order to enable reloading, `refreshRate` must be configured on the
  * builder, and a RepoContext must be supplied via the `setRepo` function. The
  * design of this configuration API is necessary because RepoContext itself has
  * a circular dependency on the schema cache.
  *
  * !!! ATTENTION: !!!
  *
  * Cache reads occur in a separate query context, therefore bypassing the "read
  * your own writes" (RYOW) property for the caller query context.
  */
object QueryCache {

  type Reloader[K, V] =
    (K, QueryLoadingValue[K, V]) => Option[QueryLoadingValue[K, V]]

  def apply[K <: AnyRef, V](
    name: String,
    cacheBuilder: Caffeine.Unrefined,
    weighted: Option[Int] = None)(queryForKey: K => Query[Option[V]])(
    implicit ec: ExecutionContext) = {
    new QueryCache[K, V](name, queryForKey, weighted, cacheBuilder)(ec)
  }

  final case class Stats(
    hits: Long,
    misses: Long,
    loads: Long,
    invalidations: Long,
    timeouts: Long,
    exceptions: Long,
    schemaContentions: Long,
    evictions: Long,
    estimatedSize: Long
  )

  private[QueryCache] final class StatsCollector {
    val hits = new LongAdder
    val misses = new LongAdder
    val loads = new LongAdder
    val invalidations = new LongAdder
    val timeouts = new LongAdder
    val exceptions = new LongAdder
    val schemaContentions = new LongAdder
  }
}

class QueryCache[K <: AnyRef, V] private (
  val name: String,
  queryForKey: K => Query[Option[V]],
  weightAccuracy: Option[Int],
  cacheBuilder: Caffeine.Unrefined)(implicit ec: ExecutionContext)
    extends ExceptionLogging {

  private[this] var _repo = Option.empty[RepoContext]
  private[this] val stats = new QueryCache.StatsCollector()
  @volatile private[this] var lastEvictionCount = 0L

  val isWeighted = weightAccuracy.isDefined
  val caffeine = {
    var cb = cacheBuilder.withTypes[K, QueryLoadingValue[K, V]]
    if (!cb.hasMaxSize) {
      cb = weightAccuracy.fold(cb) { acc => cb.weigher(new MemoryMeterWeigher(acc)) }
    }
    cb.build(
      load = { key => Some(new QueryLoadingValue(key, queryForKey)) },
      reload = { (_, value) =>
        _repo map { repo =>
          val loadF = value.load(repo)
          implicit val iec = ImmediateExecutionContext
          loadF onComplete {
            case Success(_) => ()
            case Failure(e: SchemaContentionException) =>
              getLogger.warn(
                s"[sce] schema contention exception: $e\n${e.getStackTrace().mkString("\n")}")
              stats.schemaContentions.increment()
            case Failure(err) =>
              stats.exceptions.increment()
              logException(err)
          }
          value
        }
      }
    )
  }

  /** This must be called with a RepoContext after creation in order for the
    * cache to be able to reload values. This method is not thread safe.
    */
  def setRepo(repo: RepoContext): Unit = _repo = Some(repo)

  def invalidateAll(): Unit =
    caffeine.invalidateAll()

  def invalidate(key: K): Unit = {
    stats.invalidations.increment()
    caffeine.invalidate(key)
  }

  /** Update the value of a key if it is present in the cache. A value is
    * considered present if it is completely loaded: If the current state of a
    * value is a pending future, this will result in the updater not running.
    *
    * @returns true if the updater is applied, false otherwise.
    *
    * TODO: The behavior of this method seems sufficient for schema
    * version management. Using it for other purposes may require more testing
    * of its concurrent behavior.
    */
  def updateIfPresent(key: K)(f: V => Option[V]): Boolean = {
    import QueryLoadingValue.UpdateRes._
    caffeine.get(key).updateIfPresent(f) match {
      case NotPresent => false
      case Updated    => true
      case Invalidated =>
        stats.invalidations.increment()
        true
    }
  }

  // used for testing
  def update(key: K, v: V): Unit = {
    val entry = new QueryLoadingValue(key, queryForKey)
    entry.set(Some(v))
    caffeine.put(key, entry)
  }

  def get(key: K): Query[Option[V]] = {
    val entry = caffeine.get(key)

    entry.cachedValue match {
      case Left(fut) =>
        // is this a hit or a miss? Perhaps indicate we hit a Future and will stall?
        Query.future(fut)

      case Right(Success(v @ Some(_))) =>
        stats.hits.increment()
        Query.value(v)

      case Right(t @ (Success(None) | Failure(_))) =>
        stats.misses.increment()

        // Prefer the injected repo if present, in order to preserve stats
        // collection behavior for the schema cache. Otherwise we can pull it
        // from the current query context.
        val repoQ = _repo.fold(Query.repo)(Query.value)

        repoQ flatMap { repo =>
          entry.tryLoad(repo, t) match {
            case None =>
              // Lost a race with some another `tryLoad` call.
              entry.cachedValue match {
                case Left(fut)           => Query.future(fut)
                case Right(Success(v))   => Query.value(v)
                case Right(Failure(err)) => Query.fail(err)
              }
            case Some(fut) =>
              // Driving cache load. Respond to its result.
              implicit val iec = ImmediateExecutionContext
              val res = fut andThen {
                case Success(_) =>
                  stats.loads.increment()
                  if (isWeighted) {
                    // put the entry back in the cache to recalculate its weight
                    caffeine.put(key, entry)
                  }
                case Failure(_: TimeoutException) =>
                  stats.timeouts.increment()
                case Failure(_: SchemaContentionException) =>
                  stats.schemaContentions.increment()
                case Failure(_) =>
                  stats.exceptions.increment()
              }
              Query.future(res)
          }
        }
    }
  }

  def peek(key: K): Option[V] =
    caffeine.get(key).cachedValue.toOption.flatMap {
      case Success(v)   => v
      case Failure(err) => throw err
    }

  // Stats functionality

  def hits = stats.hits.sum()
  def misses = stats.misses.sum()
  def loads = stats.loads.sum()
  def exceptions = stats.exceptions.sum()
  def evictions = caffeine.stats().evictionCount - lastEvictionCount
  def estimatedSize = caffeine.estimatedSize

  def getAndResetStats(): QueryCache.Stats = {
    val evictions = {
      val count = caffeine.stats().evictionCount
      val prev = lastEvictionCount
      lastEvictionCount = count
      count - prev
    }

    QueryCache.Stats(
      stats.hits.sumThenReset(),
      stats.misses.sumThenReset(),
      stats.loads.sumThenReset(),
      stats.invalidations.sumThenReset(),
      stats.timeouts.sumThenReset(),
      stats.exceptions.sumThenReset(),
      stats.schemaContentions.sumThenReset(),
      evictions,
      estimatedSize
    )
  }

  def reportAndResetStats(statsrec: StatsRecorder): Unit = {
    val s = getAndResetStats()
    statsrec.count(s"Cache.$name.Hits", s.hits.toInt)
    statsrec.count(s"Cache.$name.Misses", s.misses.toInt)
    statsrec.count(s"Cache.$name.Evictions", s.evictions.toInt)
    statsrec.count(s"Cache.$name.Loads", s.loads.toInt)
    statsrec.count(s"Cache.$name.Invalidations", s.invalidations.toInt)
    statsrec.count(s"Cache.$name.Timeouts", s.timeouts.toInt)
    statsrec.count(s"Cache.$name.Exceptions", s.exceptions.toInt)
    statsrec.count(s"Cache.$name.SchemaContentions", s.schemaContentions.toInt)
    statsrec.set(s"Cache.$name.Objects", s.estimatedSize)
  }

}
