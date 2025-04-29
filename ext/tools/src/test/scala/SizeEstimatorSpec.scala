package fauna.tools.test

import fauna.api.test.HttpClientHelpers
import fauna.atoms.GlobalDatabaseID
import fauna.flags.test.FlagProps
import fauna.model.Database
import fauna.prop.api.{ CoreLauncher, FaunaDB, FeatureFlags }
import fauna.util.ZBase32
import java.nio.file.Files
import org.scalatest.BeforeAndAfter

class SizeEstimatorSpec
    extends ImportExportSpec
    with HttpClientHelpers
    with BeforeAndAfter {

  before {
    terminate()
  }

  override def afterAll(): Unit = {
    terminate()
  }

  "Size Estimator" - {
    "works for versions" in {
      launchInstance()

      val globalID = GlobalDatabaseID(
        ZBase32.decodeLong(createDB("size_estimator", FaunaDB.rootKey)))
      val key = keyForDB("size_estimator")

      val res = api1.query(
        CreateCollection(MkObject("name" -> "versions", "history_days" -> 30)),
        key)
      res should respond(Created)

      val versions = 10
      (1 to versions) foreach { _ =>
        val res = api1.query(CreateF(ClassRef("versions"), MkObject()), key)
        res should respond(Created)
      }

      val docKeySize = 22L
      val docCellSize = 31L
      val docsSize = versions * (docKeySize + docCellSize)

      val collKeySize = 14L
      val collCellSize = 87L
      val collSize = collKeySize + collCellSize

      // The backing SchemaSource is added to the total size
      // val schemaKeySize = 12L
      // val schemaCellSize = 90L
      // val schemaSize = schemaKeySize + schemaCellSize
      // val schemaUpdateCellSize = 132

      offlineWithBackup { backupPath =>
        val metadataFile = Files.createTempFile("metadata", ".txt")

        val (versions, indexes) = sizeEstimator(metadataFile)
        // FIXME: Re-enable schema, and add schemaSize once enabled.
        versions.get(globalID).value shouldBe (docsSize + collSize)

        val (snapshotVersions, snapshotIndexes) =
          snapshotSizeEstimator(Some(backupPath))

        snapshotVersions.values.sum shouldEqual versions.values.sum
        snapshotIndexes.values.sum shouldEqual indexes.values.sum
      }

      // delete collection
      api1.query(DeleteF(ClassRef("versions")), key) should respond(OK)

      offlineWithBackup { backupPath =>
        val metadataFile = Files.createTempFile("metadata", ".txt")

        val (versions, indexes) = sizeEstimator(metadataFile)
        // FIXME: Re-enable schema, and assert this equals schemaSize +
        // schemaUpdateCellSize.
        versions.get(globalID) shouldBe None

        val (snapshotVersions, snapshotIndexes) =
          snapshotSizeEstimator(Some(backupPath))

        snapshotVersions.values.sum shouldEqual versions.values.sum
        snapshotIndexes.values.sum shouldEqual indexes.values.sum
      }

      // delete database
      api1.query(
        DeleteF(DatabaseRef("size_estimator")),
        FaunaDB.rootKey) should respond(OK)

      runOffline { _ =>
        val metadataFile = Files.createTempFile("metadata", ".txt")

        val (versions, _) = sizeEstimator(metadataFile)
        versions.get(globalID) shouldBe None
      }
    }

    "works for indexes" in {
      val flags = FeatureFlags(
        version = 1,
        Vector(
          FlagProps(
            "account_id",
            0,
            Map(
              "changes_by_collection_index" -> true
            ))))

      launchInstance(Some(flags))

      val globalID = GlobalDatabaseID(
        ZBase32.decodeLong(createDB("size_estimator", FaunaDB.rootKey)))
      val key = keyForDB("size_estimator")

      api1.query(
        CreateCollection(MkObject("name" -> "indexes")),
        key
      ) should respond(Created)

      api1.query(
        CreateIndex(
          MkObject(
            "name" -> "indexes",
            "source" -> ClassRef("indexes"),
            "terms" -> Seq(MkObject("field" -> Seq("data", "indexes"))))
        ),
        key
      ) should respond(Created)

      api1.query(
        CreateF(
          MkRef(ClassRef("indexes"), "1"),
          MkObject("data" -> MkObject("indexes" -> "indexes0"))
        ),
        key
      ) should respond(Created)

      val updates = 10
      (1 to updates) foreach { i =>
        api1.query(
          Update(
            MkRef(ClassRef("indexes"), "1"),
            MkObject("data" -> MkObject("indexes" -> f"indexes$i%x"))
          ),
          key
        ) should respond(OK)
      }

      offlineWithBackup { backupPath =>
        val metadataFile = Files.createTempFile("metadata", ".txt")

        val (versions, indexes) = sizeEstimator(metadataFile)

        val keySize = 25L
        val createSize = 42L + keySize
        val updateSize = 42L + 42L + keySize
        val indexByNameSize = 20L + 42L // IndexID(1)
        val collectionByNameSize = 20L + 42L // IndexID(2)
        // FIXME: Re-enable schema, and set this from 3 to 4
        val documentsByCollectionSize = 3 * (26L + 42L) // IndexID(24)

        versions.get(globalID).value should be > 0L
        indexes.get(globalID).value shouldBe 2 * (createSize + updates * updateSize +
          indexByNameSize + collectionByNameSize + documentsByCollectionSize)

        val (snapshotVersions, snapshotIndexes) =
          snapshotSizeEstimator(Some(backupPath))
        snapshotVersions.values.sum shouldEqual versions.values.sum
        snapshotIndexes.values.sum shouldEqual indexes.values.sum
      }

      // delete index
      api1.query(DeleteF(IndexRef("indexes")), key) should respond(OK)

      offlineWithBackup { backupPath =>
        val metadataFile = Files.createTempFile("metadata", ".txt")

        val (versions, indexes) = sizeEstimator(metadataFile)

        val indexByNameSize = 20L + 42L + 42L // IndexID(1)
        val collectionByNameSize = 20L + 42L // IndexID(2)
        // FIXME: Re-enable schema, and set this from 3 to 4
        val documentsByCollectionSize = 3 * (26L + 42L) + 42L // IndexID(24)

        versions.get(globalID).value should be > 0L
        indexes
          .get(globalID)
          .value shouldBe 2 * (indexByNameSize + collectionByNameSize + documentsByCollectionSize)

        val (snapshotVersions, snapshotIndexes) =
          snapshotSizeEstimator(Some(backupPath))
        snapshotVersions.values.sum shouldEqual versions.values.sum
        snapshotIndexes.values.sum shouldEqual indexes.values.sum
      }

      // delete database
      api1.query(
        DeleteF(DatabaseRef("size_estimator")),
        FaunaDB.rootKey) should respond(OK)

      runOffline { _ =>
        val metadataFile = Files.createTempFile("metadata", ".txt")

        val (versions, indexes) = sizeEstimator(metadataFile)

        versions.get(globalID) shouldBe None
        indexes.get(globalID) shouldBe None
      }
    }

    "works for collections staged for deletion" in {
      launchInstance(
        Some(
          FeatureFlags(
            1,
            Vector(FlagProps("account_id", 0, Map("allow_staged_delete" -> true))))))

      val db =
        queryOk("Database.create({ name: 'size_estimator' })", FaunaDB.rootKey)
      val globalIDString = (db / "global_id").as[String]
      val globalID = Database.decodeGlobalID(globalIDString).value
      val key = (queryOk(
        "Key.create({ role: 'admin', database: 'size_estimator' })",
        FaunaDB.rootKey) / "secret").as[String]
      val scope = globalID2ScopeID(globalIDString)

      // Create collection.
      client.api.upload(
        "/schema/1/update?force=true",
        Seq("main.fsl" -> "collection Foo { }"),
        key) should respond(OK)

      // Add versions.
      val versions = 10
      (1 to versions) foreach { _ =>
        queryOk("Foo.create({})", key)
      }

      val docKeySize = 22L
      val docCellSize = 37L
      val docsSize = versions * (docKeySize + docCellSize)

      val collKeySize = 14L
      val collCellSize = 119L
      val collSize0 = collKeySize + collCellSize

      val schemaKeySize = 12L
      val schemaCellSize = 80L
      val schemaSize0 = schemaKeySize + schemaCellSize

      val totalSize0 = docsSize + collSize0 + schemaSize0

      offlineWithBackup { backupPath =>
        val metadataFile = Files.createTempFile("metadata", ".txt")

        val (versions, _) = sizeEstimator(metadataFile)
        versions.get(globalID).value shouldBe totalSize0

        val (snapshotVersions, _) = snapshotSizeEstimator(Some(backupPath))
        snapshotVersions.get(scope).value shouldBe totalSize0
      }

      // Stage collection for deletion.
      val stagedDelete = Seq("main.fsl" -> "")
      client.api.upload(
        "/schema/1/update?force=true&staged=true",
        Seq("main.fsl" -> ""),
        key) should respond(OK)

      val deletedCollCellSize = 131L
      val collSize1 = collKeySize + collCellSize + deletedCollCellSize

      val stagedSchemaCellSize = 92L
      val schemaSize1 = schemaKeySize + schemaCellSize + stagedSchemaCellSize

      val totalSize1 = docsSize + collSize1 + schemaSize1

      // The collection still counts.
      offlineWithBackup { backupPath =>
        val metadataFile = Files.createTempFile("metadata", ".txt")

        val (versions, _) = sizeEstimator(metadataFile)
        versions.get(globalID).value shouldBe totalSize1

        val (snapshotVersions, _) = snapshotSizeEstimator(Some(backupPath))
        snapshotVersions.get(scope).value shouldBe totalSize1
      }

      // Commit the deletion.
      client.api.upload(
        s"/schema/1/commit?force=true",
        stagedDelete,
        key) should respond(OK)

      // TODO: The SE and SSE see the schema docs, but probably shouldn't.
      val totalSize2 = schemaKeySize + schemaCellSize + stagedSchemaCellSize

      // The collection does not count.
      offlineWithBackup { backupPath =>
        val metadataFile = Files.createTempFile("metadata", ".txt")

        val (versions, _) = sizeEstimator(metadataFile)
        versions.get(globalID).value shouldBe totalSize2

        val (snapshotVersions, _) = snapshotSizeEstimator(Some(backupPath))
        snapshotVersions.get(scope).value shouldBe totalSize2
      }
    }

    "works for multiple customers" in {
      launchInstance()

      val tenantsDbName = (1 to 3) map { i =>
        s"user_$i"
      }

      createDB("faunadb-cloud-tenants-production", FaunaDB.rootKey)

      val rootTenants = tenantsDbName map { dbName =>
        GlobalDatabaseID(
          ZBase32.decodeLong(
            createDB(dbName, keyForDB("faunadb-cloud-tenants-production"))))
      }

      val tenants = tenantsDbName map {
        new TenantData(_, numDocs = 10, failed = false)
      }

      val docKeySize = 14L
      val docCellSize = 45L
      val docsSize = 10 * (docKeySize + docCellSize)

      val collKeySize = 14L
      val collCellSize = 74L
      val collSize = collKeySize + collCellSize

      val idxKeySize = 16L
      val idxCellSize = 122L
      val idxSize = idxKeySize + idxCellSize

      val keyKeySize = 20L
      val keyCellSize = 118L
      val keySize = keyKeySize + keyCellSize

      val udfKeySize = 20L
      val udfCellSize = 93L
      val udfSize = udfKeySize + udfCellSize

      // The backing SchemaSource is added to the total size
      // FIXME: Re-enable schema
      // val schemaKeySize = 12L
      // val schemaCellSize = 85L
      // val schemaSize = schemaKeySize + schemaCellSize

      val userDBSize = keyKeySize + 101L
      val userKeySize = keyKeySize + 139L

      val versionsSize =
        docsSize + idxSize + collSize + keySize + udfSize

      offlineWithBackup { backupPath =>
        val metadataFile = Files.createTempFile("metadata", ".txt")

        val (versions, indexes) = sizeEstimator(metadataFile)

        tenants.zipWithIndex foreach { case (t, idx) =>
          versions.get(t.userDbGlobalID).value shouldBe versionsSize
          indexes.get(t.userDbGlobalID).value should be > 0L

          val rootTenant = rootTenants(idx)
          versions.get(rootTenant).value shouldBe (userDBSize + userKeySize)
          indexes.get(rootTenant).value should be > 0L
        }

        val (snapshotVersions, snapshotIndexes) =
          snapshotSizeEstimator(Some(backupPath))

        snapshotVersions.values.sum shouldEqual versions.values.sum
        snapshotIndexes.values.sum shouldEqual indexes.values.sum
      }

      CoreLauncher.terminate(inst)
    }

    "render database ref and global id path" in {
      launchInstance()

      val tenantsProd = GlobalDatabaseID(
        ZBase32.decodeLong(createDB(TenantProduction, FaunaDB.rootKey)))
      val tenant = GlobalDatabaseID(
        ZBase32.decodeLong(
          createDB("user_320500903154024993", keyForDB(TenantProduction))))
      val test = GlobalDatabaseID(
        ZBase32.decodeLong(
          createDB("test", keyForDB(TenantProduction, "user_320500903154024993"))))

      val res = api1.query(
        CreateCollection(MkObject("name" -> "versions")),
        keyForDB(TenantProduction, "user_320500903154024993", "test"))
      res should respond(Created)

      runOffline { _ =>
        val metadataFile = Files.createTempFile("metadata", ".txt")

        val (versions, _) = sizeEstimator0(metadataFile)

        versions(tenantsProd).dbPath shouldBe Seq(TenantProduction)
        versions(tenantsProd).globalIDPath shouldBe Seq(
          ZBase32.encodeLong(tenantsProd.toLong))

        versions(tenant).dbPath shouldBe Seq(
          TenantProduction,
          "user_320500903154024993")
        versions(tenant).globalIDPath shouldBe Seq(
          ZBase32.encodeLong(tenantsProd.toLong),
          ZBase32.encodeLong(tenant.toLong)
        )

        versions(test).dbPath shouldBe Seq(
          TenantProduction,
          "user_320500903154024993",
          "test")
        versions(test).globalIDPath shouldBe Seq(
          ZBase32.encodeLong(tenantsProd.toLong),
          ZBase32.encodeLong(tenant.toLong),
          ZBase32.encodeLong(test.toLong)
        )
      }
    }
  }
}
