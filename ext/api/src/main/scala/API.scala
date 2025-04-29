package fauna.api

import fauna.config.CoreConfig
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang._
import fauna.model.stream.StreamContext
import fauna.model.tasks._
import fauna.net.security.JWKProvider
import fauna.repo._
import fauna.repo.cassandra.CassandraService
import fauna.snowflake.IDSource
import fauna.stats._
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.util.Success

final class API(
  val config: CoreConfig,
  val repo: RepoContext,
  val streamCtx: StreamContext,
  val rootKeys: List[String],
  val isReadOnly: Boolean,
  val stats: StatsRecorder,
  val jwkProvider: JWKProvider,
  val messageIDSource: IDSource)
    extends CoreApplication
    with Service {

  private[this] var _replicaName: Option[String] = None

  def replicaName: Option[String] = _replicaName orElse {
    _replicaName = CassandraService.instanceOpt map { _.replicaName }
    _replicaName
  }

  private[this] val svcAndRepo =
    for {
      svc <- CassandraService.started
    } yield {
      (svc, repo.forBackground)
    }

  private[this] val adjustedRetainTime: FiniteDuration =
    repo.schemaRetentionDays + config.cacheSchemaTTL

  private[this] val healthCheckService =
    svcAndRepo map { case (svc, backgroundRepo) =>
      svc.healthChecker.mkUpdater(backgroundRepo)
    }

  private[this] val backgroundServices =
    svcAndRepo map { case (_, backgroundRepo) =>
      val services = Seq.newBuilder[Service]

      if (config.enableSparseDocumentScanner) {
        services += new SparseDocumentScanner(
          backgroundRepo.withSchemaOCCDisabled,
          adjustedRetainTime,
          config.documentScanLoopTime,
          config.taskExecBackoffTime)
      }

      if (config.enableSparseIndexScanner) {
        services += new SparseHistoricalIndexScanner(
          backgroundRepo.withSchemaOCCDisabled,
          adjustedRetainTime,
          config.indexScanLoopTime,
          config.taskExecBackoffTime)

        services += new SparseSortedIndexScanner(
          backgroundRepo.withSchemaOCCDisabled,
          adjustedRetainTime,
          config.indexScanLoopTime,
          config.taskExecBackoffTime)
      }

      if (config.enableFullIndexScanner) {
        services += new FullHistoricalIndexScanner(
          backgroundRepo.withSchemaOCCDisabled,
          adjustedRetainTime,
          config.indexScanLoopTime,
          config.taskExecBackoffTime)

        services += new FullSortedIndexScanner(
          backgroundRepo.withSchemaOCCDisabled,
          adjustedRetainTime,
          config.indexScanLoopTime,
          config.taskExecBackoffTime)
      }

      if (config.enableTaskBouncer) {
        services += new TaskBouncer(backgroundRepo)
      }

      ServiceManager(services.result())
    }

  def isRunning =
    healthCheckService.value match {
      case Some(Success(hcs)) => hcs.isRunning
      case _                  => false
    }

  def start() = {
    val services = Await.result(
      Future.sequence(
        Seq(
          healthCheckService,
          backgroundServices
        )
      ),
      Duration.Inf
    )

    services foreach { _.start() }
  }

  def stop(graceful: Boolean) = {
    healthCheckService foreach { _.stop(graceful) }
    backgroundServices foreach { _.stop(graceful) }
  }
}
