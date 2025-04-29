package fauna.repo.data

import fauna.flags.{ Service => FFClient }
import fauna.net.ArrivalRateService
import fauna.net.security.Encryption
import fauna.repo.Executor
import fauna.repo.cassandra.CassandraService
import fauna.scheduler.PriorityProvider
import fauna.stats.StatsRecorder
import fauna.storage.CassandraConfig
import java.nio.file.Path
import scala.concurrent.Future

/**
  * Stores configuration for future initialisation of [[fauna.repo.cassandra.CassandraService]].
  *
  * @see [[fauna.repo.cassandra.CassandraService.initialize]] to see where this is being created.
  * @see [[fauna.repo.cassandra.CassandraService.start(replicaName: String)]] function
  *      to see where this is being used.
  */

object InitializeConfig {

  @volatile private var config: Option[InitializeConfig] = None

  private def setConfig(config: InitializeConfig): InitializeConfig = {
    this.config = Some(config)
    config
  }

  /**
    * Gets existing or creates and caches a new instance of [[InitializeConfig]].
    *
    * Currently this function is only invoked from CassandraService.cacheConfig which is synchronized so
    * there is no concurrency problem here.
    *
    * Still multiple threads should not be able to concurrently create [[InitializeConfig]]
    * to avoid "init" with multiple configs.
    */
  def getOrCreate(
    config: CassandraConfig,
    encryption: Encryption.Level,
    stats: StatsRecorder,
    prioritizer: PriorityProvider,
    tmpDirectory: Path,
    txnLogBackupPath: Option[Path],
    storagePath: Path,
    backupDir: Path,
    mkExecutor: Option[CassandraService => Future[Executor]],
    flagsClient: FFClient,
    arrivalsService: ArrivalRateService): InitializeConfig =
    synchronized {
      get getOrElse
        setConfig {
          new InitializeConfig(
            config = config,
            encryption = encryption,
            stats = stats,
            prioritizer = prioritizer,
            tmpDirectory = tmpDirectory,
            txnLogBackupPath = txnLogBackupPath,
            storagePath = storagePath,
            backupDir = backupDir,
            mkExecutor = mkExecutor,
            flagsClient = flagsClient,
            arrivalsService = arrivalsService
          )
        }
    }

  def get: Option[InitializeConfig] =
    config

  private[data] def clear(): Unit =
    config = None
}

case class InitializeConfig(
  config: CassandraConfig,
  encryption: Encryption.Level,
  stats: StatsRecorder,
  prioritizer: PriorityProvider,
  tmpDirectory: Path,
  txnLogBackupPath: Option[Path],
  storagePath: Path,
  backupDir: Path,
  mkExecutor: Option[CassandraService => Future[Executor]],
  flagsClient: FFClient,
  arrivalsService: ArrivalRateService)
