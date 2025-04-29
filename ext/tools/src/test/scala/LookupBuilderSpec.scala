package fauna.tools.test

import fauna.codex.json._
import fauna.config.CoreConfig
import fauna.lang.syntax._
import fauna.prop.api.{ CoreLauncher, FaunaDB }
import fauna.repo.store._
import java.nio.file.Paths
import org.scalatest.BeforeAndAfter
import scala.concurrent.Await
import scala.concurrent.duration._

class LookupBuilderSpec extends ImportExportSpec with BeforeAndAfter {
  before {
    terminate()
  }

  override def afterAll(): Unit = {
    terminate()
  }

  "Lookup Builder" - {
    "works" in {
      launchInstance()

      createDB("parent", FaunaDB.rootKey)
      val key = keyForDB("parent")
      (1 to 10) foreach { i =>
        createDB(s"child_$i", key)
      }

      // Use dump-tree as a proxy for the state of Lookups.
      val before = DumpEntry.fromJSON(JS.parse(dumpTree().readString))

      CoreLauncher.terminate(inst, mode = CoreLauncher.Gracefully)

      val configFile = CoreLauncher.configFile(inst)
      val config = CoreConfig.load(Some(configFile)).config

      // Clear lookups - they'll be rebuilt.
      Paths.get(config.storagePath)
        .resolve("data")
        .resolve("FAUNA")
        .resolve(cfMapping("LookupStore")).entries foreach {
        _.delete()
      }

      val (exitValue, out) = Await.result(
        launchTool(
          main = "fauna.tools.LookupBuilder",
          config = Some(configFile.toString)),
        2.minutes)

      if (exitValue != 0) {
        out forEach { println(_) }
        fail("lookup builder failed.")
      }

      CoreLauncher.relaunch(inst, apiVers)
      eventually(timeout(scaled(60.seconds)), interval(1.second)) {
        api1.get("/ping?scope=write", rootKey) should respond(OK)
      }

      val after = DumpEntry.fromJSON(JS.parse(dumpTree().readString))

      after should contain theSameElementsAs before
    }
  }
}
