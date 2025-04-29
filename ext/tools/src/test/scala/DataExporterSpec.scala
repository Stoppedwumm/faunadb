package fauna.tools.test

import fauna.api.test.HttpClientHelpers
import fauna.atoms._
import fauna.codex.json._
import fauna.codex.json2.JSON
import fauna.flags.test.FlagProps
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.{ Database, SchemaNames }
import fauna.prop.api.{ CoreLauncher, FaunaDB, FeatureFlags }
import fauna.repo.doc.Version
import fauna.repo.store.{ DatabaseTree, DumpEntry }
import fauna.stats.StatsRecorder
import fauna.storage._
import fauna.storage.cassandra.{ CassandraIterator, IteratorStatsCounter }
import fauna.storage.lookup.LiveLookup
import fauna.storage.test.{ Spec => StorageSpec }
import io.netty.buffer.Unpooled
import java.io.{ FileInputStream, FileOutputStream }
import java.nio.file.{ Files, Path }
import org.scalatest.BeforeAndAfter
import scala.io.Source
import scala.util.Random

class DataExporterSpec
    extends ImportExportSpec
    with HttpClientHelpers
    with BeforeAndAfter {

  before {
    terminate()
  }

  override def afterAll(): Unit = {
    terminate()
  }

  "DataExporter application" - {
    "read once write multiple" in {
      launchInstance()

      createDB(TenantProduction, FaunaDB.rootKey)
      createDB("user_1234", keyForDB(TenantProduction))

      val db1 =
        createDB("user_db_1", keyForDB(TenantProduction, "user_1234"))
      val db2 =
        createDB("user_db_2", keyForDB(TenantProduction, "user_1234"))

      val db3 =
        createDB("user_db_3", keyForDB(TenantProduction, "user_1234"))
      deleteDB("user_db_3", keyForDB(TenantProduction, "user_1234"))

      val dumpTreeFile = dumpTree()

      CoreLauncher.terminate(inst, mode = CoreLauncher.Gracefully)

      val Seq(backup1, backup2, backup3) = exportData(
        dumpTreeFile,
        Seq(
          (db1, "backup_db_1", false),
          (db2, "backup_db_2", false),
          (db3, "backup_db_3", false)
        ))

      Files
        .walk(backup1.userBackup.resolve("data").resolve("FAUNA"))
        .filter(_.toString endsWith "-Data.db")
        .count() shouldBe 4
      Files
        .walk(backup2.userBackup.resolve("data").resolve("FAUNA"))
        .filter(_.toString endsWith "-Data.db")
        .count() shouldBe 4
      Files
        .walk(backup3.userBackup.resolve("data").resolve("FAUNA"))
        .count() shouldBe 1 // Empty directory.

      val src = Source.fromFile(backup1.metadata.toFile)
      try {
        src.getLines() foreach { line =>
          val js = JSON.parse[JSValue](line.getBytes)

          if ((js / "type").as[String] == "database") {
            if ((js / "is_deleted").as[Boolean]) {
              (js / "path") shouldBe (JSNull)
            } else {
              val name = (js / "name").as[String]
              val path = (js / "path").as[Seq[String]]

              name match {
                case "root" =>
                  path should equal(Nil)
                case TenantProduction =>
                  path should equal(Seq(TenantProduction))
                case "user_1234" =>
                  path should equal(Seq(TenantProduction, "user_1234"))
                case user =>
                  path should equal(Seq(TenantProduction, "user_1234", user))
              }
            }
          }
        }
      } finally {
        src.close()
      }
    }

    "works for multiple customer" in {
      launchInstance()

      val tenantsDbName = (1 to 3) map { i =>
        s"user_$i"
      }

      createDB(TenantProduction, FaunaDB.rootKey)

      tenantsDbName foreach {
        createDB(_, keyForDB(TenantProduction))
      }

      // take a snapshot before creating user data
      val systemBackup = takeSnapshot("initial_data")

      // create user data
      val tenants = tenantsDbName map {
        new TenantData(_, numDocs = 10, failed = Random.nextBoolean())
      }

      val exportedData = runOfflineWithDumpTree(systemBackup) { dumpTreeFile =>
        exportTenantData(dumpTreeFile, tenants)
      }

      // check user data doesn't exist
      tenants foreach { tenant =>
        tenant.checkDoesntExist()
      }

      // load users backup
      exportedData foreach { data =>
        loadSnapshot(data.userBackup.resolve("data").resolve("FAUNA"))
      }

      // assert users data
      tenants foreach { tenant =>
        if (tenant.failed) {
          tenant.checkDoesntExist()
        } else {
          tenant.assertData()
        }
      }
    }

    "empty snapshots fail" in {
      launchInstance()

      val empty = Files.createTempDirectory("empty")

      val exportFile = Files.createTempFile("exporter", ".json")
      val stream = new FileOutputStream(exportFile.toFile)
      JSObject("export" -> JSArray.empty).writeTo(stream, pretty = false)
      stream.close()

      val metadata = Files.createTempFile("metadata", ".json")

      try {
        runOfflineWithDumpTree(empty) { dumpTree =>
          launchDataExporter(
            Tables.Lookups.CFName,
            dumpTree,
            exportFile,
            metadata,
            Timestamp.MaxMicros) shouldBe 1
        }
      } finally {
        Files.deleteIfExists(metadata)
        Files.deleteIfExists(exportFile)
        Files.deleteIfExists(empty)
      }
    }

    "honors snapshot timestamp" in {
      launchInstance()

      val tenantDbName = "user_1234"

      createDB(TenantProduction, FaunaDB.rootKey)

      createDB(tenantDbName, keyForDB(TenantProduction))

      val systemBackup = takeSnapshot("initial_data")

      val tenant = new TenantData(tenantDbName, numDocs = 50)

      val snapshotTs = tenant.docsTs(25)

      val exportedData = runOfflineWithDumpTree(systemBackup) { dumpTreeFile =>
        exportTenantData(dumpTreeFile, Seq(tenant), snapshotTs).head
      }

      tenant.checkDoesntExist()

      loadSnapshot(exportedData.userBackup.resolve("data").resolve("FAUNA"))
      tenant.assertData(snapshotTs)
    }

    "assert stats" in {
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
        new TenantData(_, numDocs = 10)
      }

      val exportedData = runOffline { dumpTreeFile =>
        exportTenantData(dumpTreeFile, tenants)
      }

      exportedData foreach { data =>
        // 1 user db
        // 2 keys
        // 1 collection
        // 1 index
        // 1 udf
        // 1 SchemaSource doc, not present. FIXME: Re-enable schema
        // 10 docs
        loadStats(
          data.userBackup,
          Tables.Versions.CFName).value should contain theSameElementsAs Seq(
          "Exporter.Cells.Output" -> JSLong(16),
          "Exporter.Rows.Output" -> JSLong(16),
          "Exporter.Rows.Bytes" -> JSLong(1843))

        // 1 db by name
        // 1 key by db
        // 1 doc by coll and by ts | coll=Database
        // 1 doc by coll and by ts | coll=Key
        val rowsTenantDB = 6

        // 1 db by name
        // 1 key by db
        // 1 doc by coll and by ts
        val cellsTenantDB = 4

        // 1 idx by name DONE
        // 1 coll by name
        // 1 udf by name
        // 6 docs by coll and by ts | coll=Collection
        // 1 doc by coll and by ts  | coll=UserFunction
        // 1 doc by coll and by ts  | coll=Collection
        // 1 doc by coll and by ts  | coll=Key
        // 1 doc by coll and by ts  | coll=Index
        val rowsUserDB = 22

        // 1 idx by name
        // 1 coll by name
        // 1 udf by name
        // 14 doc by coll and by ts
        // 10 user idx
        val cellsUserDB = 41

        loadStats(
          data.userBackup,
          Tables.SortedIndex.CFName).value should contain theSameElementsAs Seq(
          "Exporter.Cells.Output" -> JSLong(cellsTenantDB + cellsUserDB),
          "Exporter.Rows.Output" -> JSLong(rowsTenantDB + rowsUserDB),
          "Exporter.Rows.Bytes" -> JSLong(3843)
        )

        loadStats(
          data.userBackup,
          Tables.HistoricalIndex.CFName).value should contain theSameElementsAs Seq(
          "Exporter.Cells.Output" -> JSLong(cellsTenantDB + cellsUserDB),
          "Exporter.Rows.Output" -> JSLong(rowsTenantDB + rowsUserDB),
          "Exporter.Rows.Bytes" -> JSLong(3843)
        )

        // 2 GlobalKeyID
        // 1 user_db = ScopeID
        // 1 user_db = GlobalDatabaseID
        loadStats(
          data.userBackup,
          Tables.Lookups.CFName).value should contain theSameElementsAs Seq(
          "Exporter.Cells.Output" -> JSLong(4),
          "Exporter.Rows.Output" -> JSLong(4),
          "Exporter.Rows.Bytes" -> JSLong(368))
      }
    }

    "different row key with same cell names should not produce zero checksum" in {
      launchInstance()

      // the empty database will produce the following rows
      //
      // Indexes:
      // Row Key: [0x83 0x00 0x00 0x81 0x6D 0x74 0x65 0x73 0x74 0x5F 0x64 0x61 0x74
      // 0x61 0x62 0x61 0x73 0x65] => (ScopeID(0),IndexID(0),Vector("test_database"))
      // Cell Name: [0x00 0x0D 0x81 0xCD 0x4A 0x04 0x96 0x65 0xA4 0x6D 0xE0 0x02 0x00
      // 0x00 0x00 0x00 0x00 0x08 0x00 0x05 0xDE 0x09 0xD2 0xBD 0xCF 0x80 0x00 0x00
      // 0x01 0x01 0x00 0x00 0x08 0x00 0x05 0xDE 0x09 0xD2 0xBD 0xCF 0x80 0x00]
      //
      // Row Key: [0x83 0x00 0x18 0x18 0x82 0xCD 0x4A 0x00 0x00 0x00 0x00 0x00 0x00
      // 0x00 0x00 0x00 0x01 0x07] => (ScopeID(0),IndexID(24),Vector(DocID(0,C1), 7))
      // Cell Name: [0x00 0x0D 0x81 0xCD 0x4A 0x04 0x96 0x65 0xA4 0x6D 0xE0 0x02 0x00
      // 0x00 0x00 0x00 0x00 0x08 0x00 0x05 0xDE 0x09 0xD2 0xBD 0xCF 0x80 0x00 0x00
      // 0x01 0x01 0x00 0x00 0x08 0x00 0x05 0xDE 0x09 0xD2 0xBD 0xCF 0x80 0x00]
      //
      // Lookups:
      // Row Key: [0xC7 0x1B 0x04 0x96 0x67 0x9B 0xF0 0x10 0x02 0x00] =>
      // ScopeID(330565542103482880)
      // Cell Name: [0x00 0x08 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x0B
      // 0x4A 0x04 0x96 0x67 0x9B 0xEF 0x10 0x02 0x00 0x00 0x00 0x00 0x00 0x08 0x00
      // 0x05 0xDE 0x0A 0x4D 0xAB 0x14 0x50 0x00 0x00 0x01 0x01 0x00 0x00 0x08 0x00
      // 0x05 0xDE 0x0A 0x4D 0xAB 0x14 0x50 0x00]
      //
      // Row Key: [0xC8 0x1B 0x04 0x96 0x67 0x9B 0xF0 0x20 0x02 0x00] =>
      // GlobalDatabaseID(330565542104531456)
      // Cell Name: [0x00 0x08 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x0B
      // 0x4A 0x04 0x96 0x67 0x9B 0xEF 0x10 0x02 0x00 0x00 0x00 0x00 0x00 0x08 0x00
      // 0x05 0xDE 0x0A 0x4D 0xAB 0x14 0x50 0x00 0x00 0x01 0x01 0x00 0x00 0x08 0x00
      // 0x05 0xDE 0x0A 0x4D 0xAB 0x14 0x50 0x00]
      val globalID = createDB("test_database", FaunaDB.rootKey)

      runOffline { dumpTree =>
        val dataExported =
          exportData(dumpTree, Seq((globalID, "test", false))) head

        dataExported.checksum(
          Tables.SortedIndex.CFName) should not be Checksum.Zeroes
        dataExported.checksum(
          Tables.HistoricalIndex.CFName) should not be Checksum.Zeroes
        dataExported.checksum(Tables.Lookups.CFName) should not be Checksum.Zeroes
        dataExported.checksum(Tables.Versions.CFName) should not be Checksum.Zeroes
      }
    }

    "doesn't include deleted data in snapshot" in {
      launchInstance()

      val tenantDbName = "user_3555"

      createDB(TenantProduction, FaunaDB.rootKey)

      createDB(tenantDbName, keyForDB(TenantProduction))

      val tenant = new TenantData(tenantDbName, numDocs = 10)

      val existingScopesB = Seq.newBuilder[ScopeID]
      existingScopesB.addOne(
        globalID2ScopeID(tenant.createDBWithDataUnderUserDb("test_db_1")._1)
      )

      val deletedScopes = {
        val res1 = tenant.createDBWithDataUnderUserDb("test_db_2")
        val res2 = tenant.createDBWithDataUnderUserDb("test_db_3")
        val scopes = Seq(res1, res2).map { case (globalID, _) =>
          globalID2ScopeID(globalID)
        }
        deleteDB("test_db_2", tenant.userDbKey)
        deleteDB("test_db_3", tenant.userDbKey)
        scopes
      }

      existingScopesB.addOne(
        globalID2ScopeID(tenant.createDBWithDataUnderUserDb("test_db_4")._1)
      )

      val existingScopes = existingScopesB.result()

      val exportedData = runOffline { dumpTreeFile =>
        exportTenantData(dumpTreeFile, Seq(tenant)).head
      }

      val (versionSizes, indexSizes) = snapshotSizeEstimator(
        Some(exportedData.userBackup.resolve("data").resolve("FAUNA")))

      versionSizes.keySet should not contain oneElementOf(deletedScopes)
      versionSizes.keySet should contain allElementsOf (existingScopes)

      indexSizes.keySet should not contain oneElementOf(deletedScopes)
      indexSizes.keySet should contain allElementsOf (existingScopes)
    }

    "includes collections staged for deletion in snapshot" in {
      launchInstance(
        Some(
          FeatureFlags(
            1,
            Vector(FlagProps("account_id", 0, Map("allow_staged_delete" -> true))))))

      val tenantDbName = "user_1212"

      createDB(TenantProduction, FaunaDB.rootKey)

      val globalID = createDB(tenantDbName, keyForDB(TenantProduction))
      val key = keyForDB(TenantProduction, tenantDbName)

      client.api.upload(
        "/schema/1/update?force=true",
        Seq("main.fsl" -> "collection Foo { }"),
        key) should respond(OK)

      queryOk(s"Set.sequence(0, 10).forEach(i => Foo.create({}))", key)

      client.api.upload(
        "/schema/1/update?force=true&staged=true",
        Seq("main.fsl" -> ""),
        key) should respond(OK)

      val exportedData = runOffline { dumpTreeFile =>
        exportData(dumpTreeFile, Seq((globalID, tenantDbName, false))).head
      }

      val (versionSizes, _) = snapshotSizeEstimator(
        Some(exportedData.userBackup.resolve("data").resolve("FAUNA")))

      // A bit magic but I verified this is the number when all versions are
      // included.
      versionSizes(globalID2ScopeID(globalID)) shouldBe 1038
    }
  }
}

class DataExporterStorageSpec extends ImportExportSpec with BeforeAndAfter {

  before {
    terminate()
  }

  override def afterAll(): Unit = {
    terminate()
  }

  def readDumpTree(dumpTreeFile: Path) = {
    DatabaseTree.build(DumpEntry.fromStream(Files.newInputStream(dumpTreeFile)))
  }

  "DataExporter Storage Spec" - {
    "only export lookup entries that belong to the database" in {
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
        new TenantData(_, numDocs = 10)
      }

      runOffline { dumpTreeFile =>
        val exportedData = exportTenantData(dumpTreeFile, tenants)

        val storageSpec = new StorageSpec(Cassandra.KeyspaceName) {}

        storageSpec.beforeAll()

        val tree = DatabaseTree.build(
          DumpEntry.fromStream(new FileInputStream(dumpTreeFile.toFile)))

        exportedData.zipWithIndex foreach { case (exportedData, index) =>
          val tenantTree = tree.forGlobalID(tenants(index).userDbGlobalID).value

          val storage = Storage(Cassandra.KeyspaceName, Seq(Tables.Lookups.Schema))
          storage.init()

          storageSpec.withStorageEngine(StatsRecorder.Null) { storageEngine =>
            val keyspace = storageEngine.keyspace
            val store = keyspace.getColumnFamilyStore(Tables.Lookups.CFName)

            store.disableAutoCompaction()
            store.clearUnsafe()

            copyBackupToFauna(
              exportedData.userBackup.resolve("data").resolve("FAUNA"),
              Some(storageEngine.config.rootPath))
            store.loadNewSSTables()

            val counter = new IteratorStatsCounter()
            val entries =
              new CassandraIterator(
                counter,
                store,
                ScanBounds(Segment.All),
                Selector.All,
                Clock.time).toSeq map { case (key, cell) =>
                Tables.Lookups.decodeLookup(key, cell)
              }

            withClue(s"entries = $entries stats = ${counter.snapshot()}") {
              entries.size shouldBe 4

              val keys = entries collect {
                case LiveLookup(key @ GlobalKeyID(_), _, _, _, _) => key
              }

              keys.size shouldBe 2
            }

            val expected = tenantTree.toEntries map { entry =>
              (entry.globalID, entry.parentID, entry.dbID.toDocID, entry.deletedTS)
            } toSet

            val actual = entries collect {
              case LiveLookup(
                    globalID @ (ScopeID(_) | GlobalDatabaseID(_)),
                    parentID,
                    id,
                    ts,
                    action) =>
                (globalID, parentID, id, Option.when(action.isDelete) { ts.validTS })
            } toSet

            actual shouldBe expected
          }
        }
      }
    }

    s"only export versions that belong to the database tree" in {
      launchInstance()

      val tenantDbName = "user_123"

      createDB(TenantProduction, FaunaDB.rootKey)

      createDB(tenantDbName, keyForDB(TenantProduction))

      // create user data
      val tenant = new TenantData(tenantDbName, numDocs = 10)

      val (deletedDb, _) = tenant.createDBWithDataUnderUserDb("deleted_db")
      deleteDB("deleted_db", tenant.userDbKey)

      runOffline { dumpTreeFile =>
        val exportedData = exportTenantData(dumpTreeFile, Seq(tenant)) head

        val storageSpec = new StorageSpec(Cassandra.KeyspaceName) {}

        storageSpec.beforeAll()

        val storage =
          Storage(Cassandra.KeyspaceName, Seq(Tables.Versions.Schema))
        storage.init()

        storageSpec.withStorageEngine(StatsRecorder.Null) { storageEngine =>
          val keyspace = storageEngine.keyspace
          val store = keyspace.getColumnFamilyStore(Tables.Versions.CFName)

          store.disableAutoCompaction()
          store.clearUnsafe()

          val userBackup = exportedData.userBackup / "data" / "FAUNA"

          copyBackupToFauna(userBackup, Some(storageEngine.config.rootPath))
          store.loadNewSSTables()

          val counter = new IteratorStatsCounter()
          val versions =
            new CassandraIterator(
              counter,
              store,
              ScanBounds(Segment.All),
              Selector.All,
              Clock.time).toSeq map { case (key, cell) =>
              Version.decodeCell(Unpooled.wrappedBuffer(key), cell)
            } filter { _.id.collID == DatabaseID.collID }

          versions.size shouldBe 1

          SchemaNames.findName(versions.head) shouldBe "user_db"

          val globalTree = readDumpTree(dumpTreeFile)
          val localTree = readDumpTree(userBackup / "dump-tree.json")

          globalTree
            .forGlobalID(Database.decodeGlobalID(deletedDb).get)
            .isEmpty shouldBe true

          localTree
            .forGlobalID(Database.decodeGlobalID(deletedDb).get)
            .isEmpty shouldBe true
        }
      }
    }
  }
}
