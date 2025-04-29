package benchmarks.repo.query

import fauna.atoms.ScopeID
import fauna.lang._
import fauna.lang.clocks.Clock
import fauna.repo.{ CacheContext, RepoContext }
import fauna.repo.query.{ Query, QueryEvalContext }
import fauna.repo.service.rateLimits._
import fauna.scheduler.PriorityGroup
import fauna.snowflake.IDSource
import fauna.stats.StatsRecorder
import org.openjdk.jmh.annotations._
import scala.annotation.switch
import scala.concurrent.duration._
import scala.concurrent.Await

object QueryBench {

  val repo =
    RepoContext(
      service = null,
      storage = null,
      clock = Clock,
      priorityGroup = PriorityGroup.Default,
      stats = StatsRecorder.Null,
      limiters = PermissiveAccountOpsRateLimitsService,
      retryOnContention = false,
      queryTimeout = Duration.Inf,
      readTimeout = 1.minute,
      newReadTimeout = 10.minute,
      rangeTimeout = 1.minute,
      writeTimeout = 1.minute,
      idSource = new IDSource(() => 42),
      cacheContext = CacheContext(null, null, null),
      isLocalHealthyQ = Query.True,
      isClusterHealthyQ = Query.True,
      isStorageHealthyQ = Query.True,
      backupReadRatio = 0,
      repairTimeout = 1.minute,
      schemaRetentionDays = 1.minute,
      txnSizeLimitBytes = Int.MaxValue,
      txnMaxOCCReads = Int.MaxValue,
      enforceIndexEntriesSizeLimit = false,
      reindexCancelLimit = Int.MaxValue,
      reindexCancelBytesLimit = Long.MaxValue,
      reindexDocsLimit = Int.MaxValue,
      taskReprioritizerLimit = 100,
      backupReads = false
    )

  val query1 = genQuery(1)
  val query2 = genQuery(2)
  val query4 = genQuery(4)
  val query8 = genQuery(8)
  val query16 = genQuery(16)
  val query32 = genQuery(32)
  val query64 = genQuery(64)
  val query128 = genQuery(128)
  val query256 = genQuery(256)
  val query512 = genQuery(512)
  val query1024 = genQuery(1024)
  val query2048 = genQuery(2048)
  val query4096 = genQuery(4096)
  val query8192 = genQuery(8192)
  val query16384 = genQuery(16384)
  val query32768 = genQuery(32768)
  val query65536 = genQuery(65536)
  val query100000 = genQuery(100000) // default max

  private def genQuery(size: Int): Query[Int] =
    Query.accumulate(Iterable.range(0, size) map { Query.value(_) }, 0) {
      case (a, b) => a + b
    }
}

@Fork(1)
@State(Scope.Benchmark)
class QueryBench {
  import QueryBench._

  @Param(
    Array(
      "1",
      "2",
      "4",
      "8",
      "16",
      "32",
      "64",
      "128",
      "256",
      "512",
      "1024",
      "2048",
      "4096",
      "8192",
      "16384",
      "32768",
      "65536",
      "100000"
    ))
  var size = ""

  @Benchmark
  def accumulate() = {
    @switch
    val query = size match {
      case "1"      => query1
      case "2"      => query2
      case "4"      => query4
      case "8"      => query8
      case "16"     => query16
      case "32"     => query32
      case "64"     => query64
      case "128"    => query128
      case "256"    => query256
      case "512"    => query512
      case "1024"   => query1024
      case "2048"   => query2048
      case "4096"   => query4096
      case "8192"   => query8192
      case "16384"  => query16384
      case "32768"  => query32768
      case "65536"  => query65536
      case "100000" => query100000
    }

    run(query)
  }

  @Benchmark
  def unfold() = {
    val page = Page.unfold(1) { n => Query.deferred((Seq(n), Some(n + 1))) }
    val query = page.takeT(size.toInt).flattenT
    run(query)
  }

  @inline private def run[A](query: Query[A]) = {
    val fut = QueryEvalContext.eval(
      query,
      repo,
      Timestamp.Epoch,
      TimeBound.Max,
      Int.MaxValue,
      PermissiveOpsLimiter,
      ScopeID.MaxValue
    )
    Await.result(fut, Duration.Inf)
  }
}
