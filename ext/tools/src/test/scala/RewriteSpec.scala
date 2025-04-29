package fauna.tools.test

import fauna.atoms.{ GlobalDatabaseID, ScopeID }
import fauna.codex.json.JSObject
import fauna.lang.syntax._
import fauna.model.Database
import fauna.prop.api.FaunaDB
import fauna.repo.store.DumpEntry
import fauna.tools.MappingFile
import java.nio.file.{ Files, Path }
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.jdk.CollectionConverters._

class RewriteSpec extends ImportExportSpec {

  override def launchRewrite(
    cf: String,
    mappingsFile: Path,
    includeKeys: Boolean,
    config: Path) = {

    val mappings = MappingFile.read(mappingsFile.toString)

    var i = 10L
    val newMappings = mappings map { m =>
      i += 1
      m.copy(oldScope = ScopeID(i), oldGlobal = GlobalDatabaseID(i))
    }

    // save new mapping file
    newMappings.save(mappingsFile.toString)

    val args = if (includeKeys) {
      List("-k", mappingsFile.toString)
    } else {
      List(mappingsFile.toString)
    }

    val (exitValue, out) = Await.result(
      launchTool(
        main = "fauna.tools.Rewriter",
        flags = Map("name" -> cf, "threads" -> "1"),
        args = args,
        config = Some(config.toString)
      ),
      10.minutes
    )

    if (exitValue != 0) {
      val msg = out
        .iterator()
        .asScala
        .filter(_.contains("java.lang.IllegalArgumentException")) mkString "\n"
      throw new IllegalStateException(msg)
    }

    0
  }

  "Rewrite" - {
    "fail to map" in {
      launchInstance()

      val tenantDbName = s"user_1234"

      createDB(TenantProduction, FaunaDB.rootKey)

      createDB(tenantDbName, keyForDB(TenantProduction))

      // create user data
      val tenant = new TenantData(tenantDbName, numDocs = 10, failed = false)

      tenant.assertData()

      createDB("test_db", tenant.userDbKey)

      // take a backup of user database
      val exportedData = runOffline { globalDumpTree =>
        exportTenantData(globalDumpTree, Seq(tenant))
      } head

      val ex = the[Exception] thrownBy {
        restore(tenant, exportedData)
      }

      ex.getMessage should include("No mapping found for")
    }
  }
}

class MissingScopeRewriteSpec extends ImportExportSpec {

  "doesn't fail to map deleted database" in {
    launchInstance()

    val tenantDbName = s"user_1234"
    createDB(TenantProduction, FaunaDB.rootKey)
    createDB(tenantDbName, keyForDB(TenantProduction))

    val tenant = new TenantData(tenantDbName, numDocs = 10, failed = false)
    tenant.assertData()

    val childGID = createDB("test_db", tenant.userDbKey)

    val exportedData = runOffline { globalDumpTree =>
      exportTenantData(globalDumpTree, Seq(tenant))
    } head

    // Remove the entries for the child DB, simulating the original bad situation
    // where a (deleted) database in the backup had no dump tree entries.
    val treePath = exportedData.userBackup / "data" / "FAUNA" / "dump-tree.json"
    val dumpTree = DumpEntry.fromStream(Files.newInputStream(treePath))
    val childDBEntry = dumpTree.find {
      _.globalID == Database.decodeGlobalID(childGID).get
    }.get
    val newDumpTree = DumpEntry.toJSON(dumpTree.filterNot {
      _.dbID == childDBEntry.dbID
    })
    newDumpTree.writeTo(Files.newOutputStream(treePath), pretty = false)

    deleteDB("test_db", tenant.userDbKey)

    // The restore should still work because it skips the database with no entries.
    restore(tenant, exportedData, ignoreStats = true)
    tenant.assertData()

    // Check the child wasn't restored.
    api1
      .query(
        Exists(DatabaseRef("test_db")),
        tenant.userDbKey
      )
      .json shouldBe JSObject("resource" -> false)
  }
}
