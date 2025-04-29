package fauna.tools.test

import fauna.atoms.GlobalDatabaseID
import fauna.codex.json._
import fauna.prop.api.FaunaDB
import fauna.repo.store._
import fauna.storage.Tables
import fauna.util.ZBase32
import java.nio.file.Files
import org.scalatest.BeforeAndAfter
import scala.concurrent.duration._

class BackupRestoreSpec extends ImportExportSpec with BeforeAndAfter {

  before {
    terminate()
  }

  override def afterAll(): Unit = {
    terminate()
  }

  "Backup & Restore" - {
    "restore in place multiple users" in {
      launchInstance()

      val tenantsDbName = (1 to 3) map { i =>
        s"user_$i"
      }

      createDB(TenantProduction, FaunaDB.rootKey)

      tenantsDbName foreach {
        createDB(_, keyForDB(TenantProduction))
      }

      // create user data
      val tenants = tenantsDbName map {
        new TenantData(_, numDocs = 10, failed = false)
      }

      tenants foreach {
        _.assertData()
      }

      // take a backup of users database
      val exportedData = runOffline { dumpTreeFile =>
        exportTenantData(dumpTreeFile, tenants)
      }

      val restores = (tenants zip exportedData) map { case (tenant, data) =>
        Restore(tenant, data)
      }

      restoreMultiple(restores)

      // wait for key cache invalidate
      eventually(timeout(1.minutes), interval(1.second)) {
        tenants foreach {
          _.assertData()
        }
      }
    }

    "restore in place multiple times" in {
      launchInstance()

      val tenantDbName = s"user_1234"

      createDB(TenantProduction, FaunaDB.rootKey)

      createDB(tenantDbName, keyForDB(TenantProduction))

      // create user data
      val tenant = new TenantData(tenantDbName, numDocs = 10, failed = false)

      tenant.assertData()

      createDB("test_db", tenant.userDbKey)

      // take a backup of users database
      val exportedData = runOffline { globalDumpTree =>
        exportTenantData(globalDumpTree, Seq(tenant))
      } head

      (1 to 4) foreach { _ =>
        deleteDB("test_db", tenant.userDbKey)

        restore(tenant, exportedData)

        // wait for key cache invalidate
        eventually(timeout(1.minutes), interval(1.second)) {
          api1.query(
            If(
              Exists(DatabaseRef("test_db")),
              JSNull,
              Abort("test_db doesn't exists")),
            tenant.userDbKey
          ) should respond(OK)

          tenant.assertData()
        }
      }

      val treeFile = dumpTree()
      val entries = DumpEntry.fromStream(Files.newInputStream(treeFile))
      val sameGlobalID = entries filter { entry =>
        entry.globalID == tenant.userDbGlobalID
      }

      sameGlobalID.size shouldBe 1
      sameGlobalID.count(_.deletedTS.isEmpty) shouldBe 1

      val tree = DatabaseTree.build(entries)
      tree
        .forGlobalID(tenant.userDbGlobalID)
        .value
        .globalID shouldBe tenant.userDbGlobalID
    }

    "restore nested databases" in {
      launchInstance()

      val tenantDbName = s"user_1234"

      createDB(TenantProduction, FaunaDB.rootKey)

      createDB(tenantDbName, keyForDB(TenantProduction))

      // create user data
      val tenant = new TenantData(tenantDbName, numDocs = 10, failed = false)

      tenant.assertData()

      // creates a nested database with data
      val globalID = createDB("nested_db", tenant.userDbKey)

      val nestedDbKey =
        keyForDB(TenantProduction, tenantDbName, tenant.userDbName, "nested_db")

      api1.query(
        CreateCollection(MkObject("name" -> "cls")),
        nestedDbKey) should respond(Created)

      // creates a document
      val doc = api1.query(
        CreateF(
          MkRef(ClassRef("cls"), "1"),
          MkObject("data" -> MkObject("foo" -> "bar"))
        ),
        nestedDbKey)

      doc should respond(Created)

      // take a backup of users database
      val exportedData = runOffline { globalDumpTree =>
        exportTenantData(globalDumpTree, Seq(tenant))
      } head

      Seq(true, false, true, false) foreach { repair =>
        // update the document
        val updatedDoc = api1.query(
          Update(
            MkRef(ClassRef("cls"), "1"),
            MkObject("data" -> MkObject("foo" -> "baz"))
          ),
          nestedDbKey)

        // document is different now
        doc.resource shouldNot be(updatedDoc.resource)

        restore(tenant, exportedData, repair = repair)

        eventually(timeout(10.second), interval(1.second)) {
          // access nested database via global ID
          enableDatabase(globalID, enable = false)
          enableDatabase(globalID, enable = true)

          tenant.assertData()
        }

        // get the restored document
        val afterRestore = api1.query(Get(MkRef(ClassRef("cls"), "1")), nestedDbKey)

        // assert it has the same data as in the past
        doc.resource should be(afterRestore.resource)
      }
    }

    "export a restored database" in {
      launchInstance()

      val tenantDbName = s"user_1234"

      createDB(TenantProduction, FaunaDB.rootKey)

      createDB(tenantDbName, keyForDB(TenantProduction))

      // create user data
      val tenant = new TenantData(tenantDbName, numDocs = 10, failed = false)

      tenant.assertData()

      (0 to 2) foreach { i =>
        // take a backup of user database
        val exportedData = runOffline { globalDumpTree =>
          exportTenantData(globalDumpTree, Seq(tenant))
        } head

        // In the first execution we only have 1 scope, 1 global and
        // 2 keys (1 parent + 1 child).
        val scopeCells = 1
        val globalCells = 1
        val keyCells = 2
        val cells = scopeCells + globalCells + keyCells

        // 1 scope + 1 global + 2 keys
        val rows = 1 + 1 + 2

        loadStats(
          exportedData.userBackup,
          Tables.Lookups.CFName).value should contain theSameElementsAs Seq(
          "Exporter.Cells.Output" -> JSLong(cells),
          "Exporter.Rows.Output" -> JSLong(rows),
          "Exporter.Rows.Bytes" -> JSLong(rows * 92)
        )

        // versions is a bit unpredictable
        val versionStats = loadStats(exportedData.userBackup, Tables.Versions.CFName)
        // FIXME: This should be 17 with schema enabled
        (versionStats / "Exporter.Cells.Output").as[Long] should be >= 16L
        (versionStats / "Exporter.Rows.Output").as[Long] should be >= 16L
        (versionStats / "Exporter.Rows.Bytes").as[Long] should be >= 1761L

        loadStats(
          exportedData.userBackup,
          Tables.SortedIndex.CFName).value should contain theSameElementsAs Seq(
          "Exporter.Cells.Output" -> JSLong(if (i == 0) 45 else 47),
          "Exporter.Rows.Output" -> JSLong(28),
          "Exporter.Rows.Bytes" -> JSLong(if (i == 0) 3843 else 3975)
        )

        loadStats(
          exportedData.userBackup,
          Tables.HistoricalIndex.CFName).value should contain theSameElementsAs Seq(
          "Exporter.Cells.Output" -> JSLong(if (i == 0) 45 else 47),
          "Exporter.Rows.Output" -> JSLong(28),
          "Exporter.Rows.Bytes" -> JSLong(if (i == 0) 3843 else 3975)
        )

        restore(tenant, exportedData)
        tenant.assertData()
      }
    }

    "restore a deleted database" in {
      launchInstance()

      val tenantDbName = s"user_1234"

      createDB(TenantProduction, FaunaDB.rootKey)

      createDB(tenantDbName, keyForDB(TenantProduction))

      // create user data
      val tenant = new TenantData(tenantDbName, numDocs = 10, failed = false)

      tenant.assertData()

      // take a backup of users database
      val exportedData = runOffline { globalDumpTree =>
        exportTenantData(globalDumpTree, Seq(tenant))
      } head

      // delete the database
      deleteDB(tenant.userDbName, tenant.tenantDbKey)

      tenant.checkDoesntExist()

      restore(tenant, exportedData)

      // wait for key cache invalidate
      eventually(timeout(1.minutes), interval(1.second)) {
        tenant.assertData()
      }

      // restore with repair in the middle

      // delete the database
      deleteDB(tenant.userDbName, tenant.tenantDbKey)

      tenant.checkDoesntExist()

      restore(tenant, exportedData, repair = true)

      // wait for key cache invalidate
      eventually(timeout(1.minutes), interval(1.second)) {
        tenant.assertData()
      }
    }

    "cannot restore to a deleted parent" in {
      launchInstance()

      val tenantDbName = s"user_1234"

      createDB(TenantProduction, FaunaDB.rootKey)

      createDB(tenantDbName, keyForDB(TenantProduction))

      // create user data
      val tenant = new TenantData(tenantDbName, numDocs = 10, failed = false)

      tenant.assertData()

      // take a backup of users database
      val exportedData = runOffline { globalDumpTree =>
        exportTenantData(globalDumpTree, Seq(tenant))
      } head

      deleteDB(tenant.tenantDbName, keyForDB(TenantProduction))

      val ex = the[Exception] thrownBy {
        restore(tenant, exportedData)
      }

      ex.getMessage should include(
        s"Cannot move database '${tenant.userDbName}' into parent '${tenant.userDbName}': Failed to lookup destination's parent database.")
    }

    "cannot copy to a deleted parent" in {
      launchInstance()

      val tenantDbName = s"user_1234"

      createDB(TenantProduction, FaunaDB.rootKey)

      createDB(tenantDbName, keyForDB(TenantProduction))

      // create user data
      val tenant = new TenantData(tenantDbName, numDocs = 10, failed = false)

      tenant.assertData()

      // take a backup of users database
      val exportedData = runOffline { globalDumpTree =>
        exportTenantData(globalDumpTree, Seq(tenant))
      } head

      // creates and delete a database
      val deletedGlobalID = createDB("deleted_database", tenant.tenantDbKey)

      deleteDB("deleted_database", tenant.tenantDbKey)

      val ex = the[Exception] thrownBy {
        restore(tenant, exportedData, destination = Some(deletedGlobalID))
      }

      ex.getMessage should include(
        s"Cannot move database '${tenant.userDbName}' into parent 'deleted_database': deleted_database is not a live database.")
    }

    "multiple operations on same destination" in {
      launchInstance()

      val tenantDbName = s"user_1234"

      createDB(TenantProduction, FaunaDB.rootKey)

      val tenantRoot = createDB(tenantDbName, keyForDB(TenantProduction))

      // create user data
      val tenant = new TenantData(tenantDbName, numDocs = 10, failed = false)

      tenant.assertData()

      // take a backup of users database
      val exportedData = runOffline { globalDumpTree =>
        exportTenantData(globalDumpTree, Seq(tenant))
      } head

      restore(tenant, exportedData)
      tenant.assertData()

      restore(
        tenant,
        exportedData,
        destination = Some(tenantRoot),
        newName = Some("copy_1"))
      tenant.assertDatabaseData(keyForDB(TenantProduction, tenantDbName, "copy_1"))

      restore(
        tenant,
        exportedData,
        destination = Some(tenantRoot),
        newName = Some("copy_2"))
      tenant.assertDatabaseData(keyForDB(TenantProduction, tenantDbName, "copy_2"))

      restore(
        tenant,
        exportedData,
        destination = Some(tenantRoot),
        newName = Some("copy_3"))
      tenant.assertDatabaseData(keyForDB(TenantProduction, tenantDbName, "copy_3"))
    }

    "copy in different place" in {
      launchInstance()

      val tenantDbName = s"user_1234"

      createDB(TenantProduction, FaunaDB.rootKey)

      createDB(tenantDbName, keyForDB(TenantProduction))

      // create user data
      val tenant = new TenantData(tenantDbName, numDocs = 10, failed = false)

      tenant.assertData()

      // take a backup of users database
      val exportedData = runOffline { globalDumpTree =>
        exportTenantData(globalDumpTree, Seq(tenant))
      } head

      (1 to 4) foreach { i =>
        // creates a new database under tenant's root database
        val newParent = s"new_parent_$i"
        val newParentGlobalID = createDB(newParent, tenant.tenantDbKey)

        restore(tenant, exportedData, destination = Some(newParentGlobalID))

        // assert only data within this database
        val key =
          keyForDB(TenantProduction, tenantDbName, newParent, tenant.userDbName)
        tenant.assertDatabaseData(key)
      }

      // make sure the original data is still there
      tenant.assertData()
    }

    "copy on tenant root with different name" in {
      launchInstance()

      val tenantDbName = s"user_1234"

      createDB(TenantProduction, FaunaDB.rootKey)

      val tenantRoot = createDB(tenantDbName, keyForDB(TenantProduction))

      // create user data
      val tenant = new TenantData(tenantDbName, numDocs = 10, failed = false)

      tenant.assertData()

      // take a backup of users database
      val exportedData = runOffline { globalDumpTree =>
        exportTenantData(globalDumpTree, Seq(tenant))
      } head

      (1 to 4) foreach { i =>
        // restore the backup on the tenant root database
        // with a different name
        val newName = s"new_name_$i"
        restore(
          tenant,
          exportedData,
          destination = Some(tenantRoot),
          newName = Some(newName))

        // assert only data within this database
        val key = keyForDB(TenantProduction, tenantDbName, newName)
        tenant.assertDatabaseData(key)
      }

      // make sure the original data is still there
      tenant.assertData()
    }

    "prevent replacing a database with the same name" in {
      launchInstance()

      val tenantDbName = s"user_1234"

      val tenantProdGlobalID = createDB(TenantProduction, FaunaDB.rootKey)

      createDB(tenantDbName, keyForDB(TenantProduction))

      // create user data
      val tenant = new TenantData(tenantDbName, numDocs = 10, failed = false)

      tenant.assertData()

      // take a backup of users database
      val exportedData = runOffline { globalDumpTree =>
        exportTenantData(globalDumpTree, Seq(tenant))
      } head

      // should not override tenant database "user_1234"
      val ex = the[Exception] thrownBy {
        restore(
          tenant,
          exportedData,
          destination = Some(tenantProdGlobalID),
          newName = Some(tenantDbName))
      }

      ex.getMessage should include("Database already exists.")
    }

    "deleted database should remain deleted after rewriter" in {
      launchInstance()

      val tenantDbName = s"user_1234"

      createDB(TenantProduction, FaunaDB.rootKey)
      createDB(tenantDbName, keyForDB(TenantProduction))

      val tenant = new TenantData(tenantDbName, numDocs = 10, failed = false)

      tenant.assertData()

      deleteDB(tenant.userDbName, tenant.tenantDbKey)

      val exportedData = runOffline { globalDumpTree =>
        exportTenantData(globalDumpTree, Seq(tenant))
      } head

      val ex = the[Exception] thrownBy {
        restore(tenant, exportedData)
      }

      ex.getMessage should include(
        s"User backup on folder ${exportedData.userBackup} is empty")
    }

    "size estimate a backup and restored database" in {
      launchInstance()

      val tenantDbName = "user_123456789"

      createDB(TenantProduction, FaunaDB.rootKey)

      val rootTenant =
        GlobalDatabaseID(
          ZBase32.decodeLong(createDB(tenantDbName, keyForDB(TenantProduction))))

      // create user data
      val tenantData = new TenantData(tenantDbName, numDocs = 10, failed = false)

      tenantData.assertData()

      // take a backup of user database
      val (
        exportedData,
        versionsBefore,
        indexesBefore,
        metadataFile
      ) = runOffline { dumpTreeFile =>
        val metadataFile = Files.createTempFile("metadata", ".txt")

        val (versions, indexes) = sizeEstimator(metadataFile)

        // user_123456789
        // 20 + 139 <= 1 key version
        // 20 + 101 <= 1 db version
        versions.get(rootTenant).value shouldBe 280L
        indexes.get(rootTenant).value should be > 0L

        // user_db
        // 10 * (14 + 45) <= 10 docs vers
        // 14 + 82        <= 1 collection version
        // 20 + 118       <= 1 key version
        // 16 + 114       <= 1 index version
        // 20 + 93        <= 1 udf version
        versions.get(tenantData.userDbGlobalID).value shouldBe 1067L
        indexes.get(tenantData.userDbGlobalID).value should be > 0L

        val exportedData = exportTenantData(dumpTreeFile, Seq(tenantData)) head

        (exportedData, versions, indexes, metadataFile)
      }

      /** Validate that the snapshot size estimator returns the same total sizes as the full size estimator
        * when running on the customer backup.
        */
      {
        val backupPath = exportedData.userBackup.resolve("data").resolve("FAUNA")
        val (seVerions, seIndexes) = sizeEstimator(metadataFile, Some(backupPath))
        val (snapshotVersions, snapshotIndexes) =
          snapshotSizeEstimator(Some(backupPath))

        snapshotVersions.values.sum shouldEqual seVerions.values.sum
        snapshotIndexes.values.sum shouldEqual seIndexes.values.sum
      }

      val restore = Restore(tenantData, exportedData)

      restoreMultiple(Seq(restore))

      // wait for key cache invalidate
      eventually(timeout(1.minutes), interval(1.second)) {
        tenantData.assertData()
      }

      runOffline { _ =>
        val metadataFile = Files.createTempFile("metadata2", ".txt")

        val (versionsAfter, indexesAfter) = sizeEstimator(metadataFile)

        // user_123456789
        // 20 + (172 + 139)       <= 2 key versions (new user_db, old user_db)
        // 20 + (113 + 101)       <= 2 db versions  (user_db, !disabled)
        // 20 + (123 + 133 + 101) <= 3 db versions (user_db, disabled, deleted)
        versionsAfter.get(rootTenant).value shouldBe 942L
        indexesAfter
          .get(rootTenant)
          .value should be >= indexesBefore.get(rootTenant).value

        // user_db
        // 10 * (14 + 45) <= 10 docs vers
        // 14 + 82        <= 1 collection version
        // 20 + 118       <= 1 key version
        // 16 + 114       <= 1 index version
        // 20 + 93        <= 1 udf version
        versionsAfter.get(tenantData.userDbGlobalID).value shouldBe versionsBefore
          .get(tenantData.userDbGlobalID)
          .value
        indexesAfter.get(tenantData.userDbGlobalID).value shouldBe indexesBefore
          .get(tenantData.userDbGlobalID)
          .value
      }
    }
  }
}
