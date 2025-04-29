package fauna.repo.test

import com.github.benmanes.caffeine.cache.Ticker
import fauna.atoms._
import fauna.lang.{ Caffeine, Timestamp }
import fauna.repo.{ RepoContext, Store }
import fauna.repo.cache.QueryCache
import fauna.repo.query.{ QDone, Query }
import fauna.repo.service.rateLimits.{ OpsLimiter, PermissiveOpsLimiter }
import fauna.storage.Tables
import fauna.storage.doc.Data
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ CountDownLatch, TimeUnit, TimeoutException }
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future, Promise }

class QueryCacheSpec extends Spec {
  implicit val ec = ExecutionContext.parasitic

  val ctx =
    CassandraHelper.context("repo", Seq(Tables.Versions.Schema, Tables.RowTimestamps.Schema))

  class TestTicker(d: Duration) extends Ticker {
    val nanos = new AtomicLong()
    override def read = nanos.get
    def tick() = nanos.addAndGet(d.toNanos)
  }

  val refreshDuration = 10.seconds

  def withCache[K <: AnyRef, V](queryForKey: K => Query[Option[V]])(
    test: (QueryCache[K, V], TestTicker) => Unit): Unit = {

    val ticker = new TestTicker(refreshDuration * 2)
    val caffeine = Caffeine.newBuilder
      .maxWeight(1 * 1024 * 1024)
      .refreshRate(refreshDuration)
      .ticker(ticker)
      .executor(ec.execute)

    val cache = QueryCache[K, V]("TestCache", caffeine, Some(1))(queryForKey)

    cache.setRepo(ctx)
    test(cache, ticker)
  }

  "Query Cache" - {
    val scope = ScopeID(ctx.nextID())

    val res = ctx !! Store.insertUnmigrated(scope, CollectionID(1).toDocID, Data.empty)
    val doc = res.value.withTS(res.value.ts.resolve(res.transactionTS))

    "gets value" in {
      def query(key: (ScopeID, DocID)) = {
        val (scope, id) = key
        Store.getUnmigrated(scope, id)
      }

      withCache(query) { (cache, _) =>
        val user = ctx ! cache.get((scope, CollectionID(1).toDocID))
        user shouldEqual Some(doc)

        cache.getAndResetStats().misses shouldEqual 1

        eventually {
          ctx ! cache.get((scope, CollectionID(1).toDocID))
          cache.getAndResetStats().hits shouldEqual 1
        }
      }
    }

    "returns cached value if present" in {
      val p = Promise[Option[String]]()

      def query(@annotation.unused str: String) =
        Query.future(p.future)

      withCache(query) { (cache, _) =>
        cache.peek("foo") shouldEqual None
        p.success(Some("bar"))

        ctx ! cache.get("foo") shouldEqual Some("bar")

        cache.peek("foo") shouldEqual Some("bar")
      }
    }

    "refreshes existing values" in {
      @volatile var curString: Option[String] = Some("foo")

      def query(@annotation.unused s: String) =
        Query.value(curString)

      withCache(query) { (cache, ticker) =>
        (ctx ! cache.get("foo")) shouldEqual Some("foo")

        curString = Some("bar")

        // Kick off the reload
        eventually {
          ticker.tick()
          (ctx ! cache.get("foo")) shouldEqual Some("bar")
        }

        (ctx ! cache.get("foo")) shouldEqual Some("bar")

        // may get some hits in the eventually block
        cache.getAndResetStats().hits should be >= 2L
      }
    }

    "will not cache negative lookups" in {
      def query(@annotation.unused s: String): Query[Option[String]] =
        Query.none

      withCache(query) { (cache, _) =>
        (ctx ! cache.get("foo")) shouldEqual None
        (ctx ! cache.get("foo")) shouldEqual None
        val stats = cache.getAndResetStats()
        stats.misses shouldEqual 2
        stats.hits shouldEqual 0
      }
    }

    "will propagate exceptions for uncached values" in {
      def query(@annotation.unused s: String): Query[Option[String]] =
        Query.deferred(sys.error("fail!"))

      withCache(query) { (cache, _) =>
        an[Exception] should be thrownBy (ctx ! cache.get("foo"))
        an[Exception] should be thrownBy (ctx ! cache.get("foo"))

        val stats = cache.getAndResetStats()
        stats.misses shouldEqual 2
        stats.hits shouldEqual 0
        stats.exceptions shouldEqual 2
      }
    }

    "will propagate timeouts for uncached values" in {
      def query(@annotation.unused s: String): Query[Option[String]] =
        Query.deferred(throw new TimeoutException("timed out"))

      withCache(query) { (cache, _) =>
        a[TimeoutException] should be thrownBy (ctx ! cache.get("foo"))

        // This is inherently racy: get() will start an async reload,
        // but that reload Future may not necessarily execute before
        // this thread observes the cached value again, which will be
        // None. However, the get() will eventually observe a timeout.
        eventually {
          a[TimeoutException] should be thrownBy (ctx ! cache.get("foo"))
        }

        val stats = cache.getAndResetStats()
        stats.misses shouldEqual 2
        stats.hits shouldEqual 0
        stats.timeouts shouldEqual 2
      }

    }

    "will eventually propagate exceptions for failed reloads" in {
      @volatile var fail = false

      def query(@annotation.unused s: String): Query[Option[String]] =
        Query.value(if (fail) sys.error("fail!") else Some("foo"))

      withCache(query) { (cache, ticker) =>
        (ctx ! cache.get("foo")) shouldEqual Some("foo") // cache load

        fail = true
        (ctx ! cache.get("foo")) shouldEqual Some("foo") // cached value

        // See comment above WRT the race between the reload Future
        // and the cachedValue.
        eventually {
          ticker.tick()
          an[Exception] should be thrownBy (ctx ! cache.get("foo")) // failed reload
        }

        val stats = cache.getAndResetStats()
        stats.hits should be >= 1L // may get some hits on the eventually block
        stats.misses shouldEqual 2
      }

    }

    "will not reload negative lookups" in {
      @volatile var r: Option[String] = Some("foo")

      def query(@annotation.unused s: String): Query[Option[String]] =
        Query(r)

      withCache(query) { (cache, ticker) =>
        (ctx ! cache.get("foo")) shouldEqual Some("foo")

        r = None

        eventually {
          ticker.tick()
          (ctx ! cache.get("foo")) shouldEqual None
        }

        cache.getAndResetStats()
        (ctx ! cache.get("foo")) shouldEqual None

        val stats = cache.getAndResetStats()
        stats.misses shouldEqual 1
      }
    }

    "provide snapshotted stats" in {

      def query(s: String) = Query(Option(s))

      withCache(query) { (cache, _) =>
        def checkStats(
          hits: Option[Long] = None,
          misses: Option[Long] = None,
          evictions: Option[Long] = None
        ): Unit = {
          def check(in: Long, out: Option[Long], clue: String): Unit =
            out foreach { o => withClue(clue) { in shouldEqual o } }

          val stats = cache.getAndResetStats()
          check(stats.hits, hits, "hits")
          check(stats.misses, misses, "misses")
          check(stats.evictions, evictions, "evictions")
        }

        checkStats(Some(0), Some(0), Some(0))

        ctx ! cache.get("foo")
        eventually { checkStats(hits = Some(0), misses = Some(1)) }

        ctx ! cache.get("foo")
        eventually { checkStats(hits = Some(1), misses = Some(0)) }

        cache.invalidateAll()

        // This double evicts based on size because the underlying CacheLoader
        // is going to insert the item after it completes (immediately). So it
        // gets inserted twice and evicted twice.
        ctx ! cache.get("f" * 2 * 1024 * 1024)
        ctx ! cache.get("f" * 2 * 1024 * 1024)

        eventually { checkStats(evictions = Some(2)) }
      }
    }

    "will not deadlock if a load operation fails" in {
      def controlledRepo(p: Promise[_]): RepoContext =
        new RepoContext(
          ctx.service,
          ctx.storage,
          ctx.clock,
          ctx.priorityGroup,
          ctx.stats,
          ctx.limiters,
          ctx.retryOnContention,
          ctx.queryTimeout,
          ctx.readTimeout,
          ctx.readTimeout,
          ctx.rangeTimeout,
          ctx.writeTimeout,
          ctx.idSource,
          ctx.cacheContext,
          ctx.isLocalHealthyQ,
          ctx.isClusterHealthyQ,
          ctx.isStorageHealthyQ,
          ctx.backupReadRatio,
          ctx.repairTimeout,
          ctx.schemaRetentionDays,
          ctx.txnSizeLimitBytes,
          ctx.txnMaxOCCReads,
          ctx.enforceIndexEntriesSizeLimit,
          ctx.reindexCancelLimit,
          ctx.reindexCancelBytesLimit,
          ctx.reindexDocsLimit,
          ctx.taskReprioritizerLimit,
          backupReads = ctx.backupReads
        ) {
          override def run[T](
            query: Query[T],
            minSnapTime: Timestamp,
            accountID: AccountID,
            scopeID: ScopeID,
            limiter: OpsLimiter): Future[(Timestamp, T)] =
            query match {
              case QDone(Some("foo")) =>
                p.future.asInstanceOf[Future[T]] map { (Timestamp.Min, _) }
              case _ => super.run(query, minSnapTime, accountID, scopeID, limiter)
            }
        }

      val caffeine = Caffeine.newBuilder
        .maxWeight(1 * 1024 * 1024)
        .refreshRate(refreshDuration)
        .executor(ec.execute)

      val cache = QueryCache[String, String]("TestCache", caffeine, Some(1)) { _ =>
        Query.some("foo")
      }

      cache.setRepo(ctx)

      val latch = new CountDownLatch(2)
      val p = Promise[Unit]()
      val ctrlCtx = controlledRepo(p)
      val clock = ctrlCtx.clock

      // initiate load
      ctrlCtx.run(
        cache.get("foo"),
        clock.time,
        AccountID.Root,
        ScopeID.RootID,
        PermissiveOpsLimiter) andThen { case _ =>
        latch.countDown()
      }
      // block on previous load
      ctrlCtx.run(
        cache.get("foo"),
        clock.time,
        AccountID.Root,
        ScopeID.RootID,
        PermissiveOpsLimiter) andThen { case _ =>
        latch.countDown()
      }

      p.failure(new Exception)
      latch.await(1, TimeUnit.SECONDS) should be(true)
    }
  }
}
