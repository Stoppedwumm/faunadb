package fauna.tools.test

import fauna.atoms._
import fauna.codex.json._
import fauna.config.CoreConfig
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.prop.api.{ CoreLauncher, FaunaDB }
import fauna.repo.store._
import fauna.storage.Tables
import fauna.storage.index._
import fauna.util.ZBase32
import java.io.FileOutputStream
import java.nio.file.{ Files, Paths }
import org.scalatest.BeforeAndAfter
import scala.concurrent.Await
import scala.concurrent.duration._

class IndexBuilderSpec extends ImportExportSpec with BeforeAndAfter {
  before {
    terminate()
  }

  override def afterAll(): Unit = {
    terminate()
  }

  "Index Builder" - {
    "works" in {
      launchInstance()

      val nchildren = 10

      val globalIDs = Seq.newBuilder[GlobalDatabaseID]

      createDB("parent", FaunaDB.rootKey)
      val key = keyForDB("parent")
      (1 to nchildren) foreach { i =>
        globalIDs += GlobalDatabaseID(
          ZBase32.decodeLong(createDB(s"child_$i", key)))
      }

      val tree = dumpTree()

      CoreLauncher.terminate(inst, mode = CoreLauncher.Gracefully)

      val configFile = CoreLauncher.configFile(inst)
      val config = CoreConfig.load(Some(configFile)).config

      // Clear sorted index - they'll be rebuilt.
      Paths.get(config.storagePath)
        .resolve("data")
        .resolve("FAUNA")
        .resolve(cfMapping("SortedIndex")).entries foreach {
          _.delete()
        }

      // No need to export anything. This process is only interested
      // in the metdata produced as a side-effect.
      val exportFile = Files.createTempFile("exporter", ".json")
      exportFile.toFile.deleteOnExit()
      val stream = new FileOutputStream(exportFile.toFile)
      JSObject("export" -> Seq.empty[JSValue]).writeTo(stream, pretty = false)
      stream.close()

      val metadata = Files.createTempFile("metadata", ".json")
      metadata.toFile.deleteOnExit()

      launchDataExporter(
        Tables.Versions.CFName,
        tree,
        exportFile,
        metadata,
        Clock.time)

      val entries = DumpEntry.fromJSON(JS.parse(tree.readString))
      val ids = globalIDs.result()

      val children = entries filter { ids contains _.globalID }
      children.size should be (nchildren)

      val parentScope = children.head.parentID

      children foreach { child =>
        child.parentID should be(parentScope)
      }

      val scopes = entries collect { case entry => entry.parentID } toSet

      // Rebuild all the things.
      scopes foreach { scope =>
        NativeIndexID.All foreach { id =>
          val (exitValue, out) = Await.result(
            launchTool(
              main = "fauna.tools.IndexBuilder",
              args = List("--primary"),
              flags = Map(
                "scope-id" -> scope.toLong.toString,
                "index-id" -> id.toLong.toString,
                "metadata" -> metadata.toString),
              config = Some(configFile.toString)),
            2.minutes)

          if (exitValue != 0) {
            out forEach { println(_) }
            fail(s"index builder failed building $id.")
          }
        }
      }

      CoreLauncher.relaunch(inst, apiVers)
      eventually(timeout(scaled(60.seconds)), interval(1.second)) {
        api1.get("/ping?scope=write", rootKey) should respond(OK)
      }

      val res = api1.get("/databases", key).json
      val data = (res / "resource" / "data").as[Seq[JSObject]]

      data.size should be (nchildren)

      val names = (1 to 10) map { i => s"child_$i" }
      data map { ref =>
        (ref / "@ref" / "id").as[String]
      } should contain theSameElementsAs names
    }
  }

}
