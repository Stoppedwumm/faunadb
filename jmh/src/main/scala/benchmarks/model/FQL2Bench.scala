package benchmarks.model

import fauna.atoms.{ CollectionID, ScopeID }
import fauna.auth.Auth
import fauna.lang.clocks.Clock
import fauna.lang.Timestamp
import fauna.model.runtime.fql2.{ FQLInterpreter, Result }
import fauna.model.runtime.fql2.FQLInterpreter.TypeMode
import fauna.model.schema.CollectionConfig
import fauna.repo.{ CacheContext, RepoContext }
import fauna.repo.cache._
import fauna.repo.query.Query
import fauna.repo.schema.migration.MigrationList
import fauna.repo.schema.DocIDSource
import fauna.repo.service.rateLimits.PermissiveAccountOpsRateLimitsService
import fauna.repo.values.Value
import fauna.scheduler.PriorityGroup
import fauna.snowflake.IDSource
import fauna.stats.StatsRecorder
import fql.ast.Expr
import fql.parser.Parser
import scala.collection.immutable.SeqMap
import scala.concurrent.duration._

object FQL2Bench {
  implicit val ctl: fauna.lang.ConsoleControl = null
  val auth = Auth.forScope(ScopeID.RootID)
  val intp = new FQLInterpreter(auth)

  val cache =
    CacheContext(
      Cache(name = "Schema", testing = true),
      SchemaCache(testing = true),
      Cache(name = "Keys", testing = true)
    )

  val ctx =
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
      cacheContext = cache,
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

  def dummyCollection(scope: ScopeID, id: CollectionID, name: String) =
    CollectionConfig(
      scope,
      name,
      id,
      Nil,
      Nil,
      Nil,
      Nil,
      1.day,
      Duration.Inf,
      false,
      Timestamp.Epoch,
      Map.empty,
      Map.empty,
      SeqMap.empty,
      Map.empty,
      None,
      DocIDSource.Snowflake,
      Nil,
      Nil,
      MigrationList.empty,
      None
    )

  @inline def evalOk(
    query: String,
    args: Map[String, Value] = Map.empty,
    typeMode: TypeMode = TypeMode.Disabled): Value = {
    val Result.Ok(expr) = (Parser.query(query): Result[Expr])
    val Result.Ok(tup) = ctx ! intp.evalWithTypecheck(expr, args, typeMode)
    tup._1
  }
}
