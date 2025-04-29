package fauna.repo.test

import fauna.net.security.Encryption
import fauna.repo.data.InitializeConfig
import fauna.scheduler.ConstantPriorityProvider
import fauna.stats.StatsRecorder
import java.nio.file.Paths
import org.scalatest.BeforeAndAfterEach
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

class InitializeConfigSpec extends Spec with BeforeAndAfterEach {

  /**
    * [[InitializeConfig]] sets a thread-safe variable in a singleton class
    * so a clear before each test required.
    */
  private def clear() = {
    val clear = PrivateMethod[Unit](Symbol("clear"))
    InitializeConfig.invokePrivate(clear())
    InitializeConfig.get shouldBe empty
  }

  override def beforeEach(): Unit =
    clear()

  override def afterEach(): Unit =
    clear()

  "get" - {
    "on empty should return None" in {
      InitializeConfig.get shouldBe empty
    }

    "getOrCreate should cache config" in {
      val config =
        InitializeConfig(
          config = CassandraHelper.testConfig(),
          encryption = Encryption.Default,
          stats = StatsRecorder.Null,
          prioritizer = ConstantPriorityProvider,
          tmpDirectory = Paths.get("test"),
          txnLogBackupPath = None,
          storagePath = null,
          backupDir = null,
          mkExecutor = None,
          flagsClient = null,
          arrivalsService = null
        )

      InitializeConfig.getOrCreate(
        config = config.config,
        encryption = config.encryption,
        stats = config.stats,
        prioritizer = config.prioritizer,
        tmpDirectory = config.tmpDirectory,
        txnLogBackupPath = None,
        storagePath = null,
        backupDir = null,
        mkExecutor = None,
        flagsClient = null,
        arrivalsService = null
      ) shouldBe config
    }

    "concurrent modification create should not be allowed" in {
      val initialConfig =
        InitializeConfig(
          config = CassandraHelper.testConfig(),
          encryption = Encryption.Default,
          stats = StatsRecorder.Null,
          prioritizer = ConstantPriorityProvider,
          tmpDirectory = Paths.get("0_test_path"),
          txnLogBackupPath = None,
          storagePath = null,
          backupDir = null,
          mkExecutor = None,
          flagsClient = null,
          arrivalsService = null
        )

      def runGetOrCreate(i: Int) =
        InitializeConfig.getOrCreate(
          config = initialConfig.config,
          encryption = initialConfig.encryption,
          stats = initialConfig.stats,
          prioritizer = initialConfig.prioritizer,
          tmpDirectory = Paths.get(i.toString + "_test_path"),
          txnLogBackupPath = None,
          storagePath = null,
          backupDir = null,
          mkExecutor = None,
          flagsClient = null,
          arrivalsService = null
        ) shouldBe initialConfig

      //create a valid initial config
      runGetOrCreate(0)

      //concurrently trying to modify should return original config
      val f = Future.sequence {
        (1 to 100).map  {
          i => Future { runGetOrCreate(i) }
        }
      }
      Await.result(f, Duration.Inf)
    }
  }

}
