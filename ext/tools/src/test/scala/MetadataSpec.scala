package fauna.tools.test

import fauna.atoms._
import fauna.lang._
import fauna.model._
import fauna.repo.doc._
import fauna.storage._
import fauna.storage.doc._
import fauna.tools._
import fauna.tools.SchemaMetadata.DB
import scala.jdk.CollectionConverters._

class MetadataSpec extends Spec {
  "SchemaMetadata" - {
    "keys" - {
      "deleted child keys are ignored" in {
        val dbTS = Timestamp.ofMillis(1)

        val liveTS = Timestamp.ofMillis(2)
        val deadTS = Timestamp.ofMillis(3)

        val db = Version.Live(
          ScopeID(1),
          DatabaseID(1).toDocID,
          Resolved(dbTS, dbTS),
          Create,
          SchemaVersion.Min,
          Data(Database.ScopeField -> ScopeID(2)))

        val v1 = Version.Live(
          ScopeID(2),
          KeyID(1).toDocID,
          Resolved(liveTS, liveTS),
          Create,
          SchemaVersion.Min,
          Data.empty)

        val v2 =
          Version.Deleted(
            ScopeID(2),
            KeyID(1).toDocID,
            Resolved(deadTS, deadTS),
            SchemaVersion.Min)

        val b = new SchemaMetadataBuilder

        b.handleVersion(db)

        b.handleVersion(v1)
        b.handleVersion(v2)

        val meta = b.build()

        meta.keys.keySet should not contain (KeyID(1))
      }

      "deleted sibling keys are ignored" in {
        val dbTS = Timestamp.ofMillis(1)

        val liveTS = Timestamp.ofMillis(2)
        val deadTS = Timestamp.ofMillis(3)

        val db = Version.Live(
          ScopeID(1),
          DatabaseID(1).toDocID,
          Resolved(dbTS, dbTS),
          Create,
          SchemaVersion.Min,
          Data(Database.ScopeField -> ScopeID(2)))

        val v1 = Version.Live(
          ScopeID(1),
          KeyID(1).toDocID,
          Resolved(liveTS, liveTS),
          Create,
          SchemaVersion.Min,
          Data(Key.DatabaseField -> Some(DatabaseID(1))))

        val v2 =
          Version.Deleted(
            ScopeID(1),
            KeyID(1).toDocID,
            Resolved(deadTS, deadTS),
            SchemaVersion.Min)

        val b = new SchemaMetadataBuilder

        b.handleVersion(db)

        b.handleVersion(v1)
        b.handleVersion(v2)

        val meta = b.build()

        meta.keys.keySet should not contain (KeyID(1))
      }

      "for deleted databases are ignored" in {
        val keyTS = Timestamp.ofMillis(1)

        // The database has been deleted after the key was created.
        val dbTS = Timestamp.ofMillis(2)

        val key1 = Version.Live(
          ScopeID(1),
          KeyID(1).toDocID,
          Resolved(keyTS, keyTS),
          Create,
          SchemaVersion.Min,
          Data(Key.DatabaseField -> Some(DatabaseID(1))))

        val db1 =
          Version.Deleted(
            ScopeID(1),
            DatabaseID(1).toDocID,
            Resolved(dbTS, dbTS),
            SchemaVersion.Min)

        val key2 = Version.Live(
          ScopeID(2),
          KeyID(2).toDocID,
          Resolved(keyTS, keyTS),
          Create,
          SchemaVersion.Min,
          Data.empty)

        val db2 =
          Version.Deleted(
            ScopeID(2),
            DatabaseID(2).toDocID,
            Resolved(dbTS, dbTS),
            SchemaVersion.Min)

        val b = new SchemaMetadataBuilder

        b.handleVersion(key1)
        b.handleVersion(db1)

        b.handleVersion(key2)
        b.handleVersion(db2)

        val meta = b.build()

        meta.keys.keySet should not contain (KeyID(1))
        meta.keys.keySet should not contain (KeyID(2))
      }

      "respect valid time" in {
        val v1TS = Timestamp.ofMillis(1)
        val v2TS = Timestamp.ofMillis(2)

        val db1 = Version.Live(
          ScopeID(1),
          DatabaseID(2).toDocID,
          Resolved(v1TS, v1TS),
          Create,
          SchemaVersion.Min,
          Data(Database.ScopeField -> ScopeID(2)))

        val db2 = Version.Live(
          ScopeID(1),
          DatabaseID(3).toDocID,
          Resolved(v1TS, v1TS),
          Create,
          SchemaVersion.Min,
          Data(Database.ScopeField -> ScopeID(3)))

        val v1 =
          Version.Live(
            ScopeID(1),
            KeyID(1).toDocID,
            Resolved(v1TS, v1TS),
            Create,
            SchemaVersion.Min,
            Data(Key.DatabaseField -> Some(DatabaseID(1))))

        val v2 =
          Version.Live(
            ScopeID(1),
            KeyID(1).toDocID,
            Resolved(v2TS, v2TS),
            Create,
            SchemaVersion.Min,
            Data(Key.DatabaseField -> Some(DatabaseID(2))))

        val b = new SchemaMetadataBuilder

        b.handleVersion(db1)
        b.handleVersion(db2)
        b.handleVersion(v1)
        b.handleVersion(v2)

        b.build().keys.get(KeyID(1)) should be(ScopeID(2))
      }

    }

    "contains MVT" in {
      val ts = Timestamp.ofMillis(1)

      val coll1 = Version.Live(
        ScopeID(1),
        CollectionID(1024).toDocID,
        Resolved(ts, ts),
        Create,
        SchemaVersion.Min,
        Data(Collection.MinValidTimeFloorField -> ts))

      // Not all collections have been updated to include MVT.
      val coll2 = Version.Live(
        ScopeID(1),
        CollectionID(1025).toDocID,
        Resolved(ts, ts),
        Create,
        SchemaVersion.Min,
        Data.empty)

      val b = new SchemaMetadataBuilder

      b.handleVersion(coll1)
      b.handleVersion(coll2)

      val mvts = b.build().toMVTMap
      val colls = mvts(ScopeID(1))

      colls(CollectionID(1024)) should be(Some(ts))
      colls.contains(CollectionID(1025)) shouldBe (false)
    }

    "disabled databases are marked as deleted" in {
      val ts = Timestamp.ofMillis(1)

      val db1 = Version.Live(
        ScopeID(0),
        DatabaseID(1).toDocID,
        Resolved(ts, ts),
        Create,
        SchemaVersion.Min,
        Data(
          SchemaNames.NameField -> SchemaNames.Name("_"),
          Database.ScopeField -> ScopeID(1),
          Database.DisabledField -> Some(true))
      )

      // this child is not marked as disabled, but it should still get deleted.
      val db2 = Version.Live(
        ScopeID(1),
        DatabaseID(2).toDocID,
        Resolved(ts, ts),
        Create,
        SchemaVersion.Min,
        Data(
          SchemaNames.NameField -> SchemaNames.Name("_"),
          Database.ScopeField -> ScopeID(2))
      )

      val b = new SchemaMetadataBuilder

      b.handleVersion(db1)
      b.handleVersion(db2)

      val meta = b.build()
      meta.databases.size shouldBe 1
    }

    "database paths work" in {
      val ts = Timestamp.ofMillis(1)

      val db1 = Version.Live(
        ScopeID(0),
        DatabaseID(1).toDocID,
        Resolved(ts, ts),
        Create,
        SchemaVersion.Min,
        Data(
          SchemaNames.NameField -> SchemaNames.Name("foo"),
          Database.ScopeField -> ScopeID(1))
      )

      val db2 = Version.Live(
        ScopeID(1),
        DatabaseID(2).toDocID,
        Resolved(ts, ts),
        Create,
        SchemaVersion.Min,
        Data(
          SchemaNames.NameField -> SchemaNames.Name("bar"),
          Database.ScopeField -> ScopeID(2))
      )

      val b = new SchemaMetadataBuilder

      b.handleVersion(db1)
      b.handleVersion(db2)

      val meta = b.build()
      meta.databases.size shouldBe 3
      meta.databases.get((ScopeID(0), DatabaseID(0))).path shouldBe Some(Seq.empty)
      meta.databases.get((ScopeID(0), DatabaseID(1))).path shouldBe Some(Seq("foo"))
      meta.databases.get((ScopeID(1), DatabaseID(2))).path shouldBe Some(
        Seq("foo", "bar"))
    }

    "deleted databases don't read the scope ID" in {
      val ts = Timestamp.ofMillis(1)

      val db1 = Version.Live(
        ScopeID(0),
        DatabaseID(1).toDocID,
        Resolved(ts, ts),
        Create,
        SchemaVersion.Min,
        Data(
          SchemaNames.NameField -> SchemaNames.Name("foo"),
          Database.ScopeField -> ScopeID(555))
      )

      // This has a different doc ID, but a broken diff, containing the same scope
      // ID. Because it is deleted, this should be ignored.
      val db2 = Version.Deleted(
        ScopeID(0),
        DatabaseID(2).toDocID,
        Resolved(ts, ts),
        SchemaVersion.Min,
        Some(
          Diff(
            SchemaNames.NameField -> SchemaNames.Name("foo"),
            Database.ScopeField -> ScopeID(555)))
      )

      val b = new SchemaMetadataBuilder

      b.handleVersion(db1)
      b.handleVersion(db2)

      val meta = b.build()
      meta.databases.size shouldBe 2
      meta.databases.get((ScopeID(0), DatabaseID(0))).scopeID shouldBe ScopeID(0)
      meta.databases.get((ScopeID(0), DatabaseID(1))).scopeID shouldBe ScopeID(555)
    }

    "if parent is not deleted, children should remain deleted" in {
      val parentGlobalID = GlobalDatabaseID(10)
      val db0 = Version.Live(
        ScopeID(1),
        DatabaseID(1).toDocID,
        Resolved(
          Timestamp.parse("1970-01-01T00:00:00.000Z"),
          Timestamp.parse("1970-01-01T00:00:00.000Z")),
        Create,
        SchemaVersion.Min,
        Data(
          SchemaNames.NameField -> SchemaNames.Name("parent"),
          Database.ScopeField -> ScopeID(10),
          Database.GlobalIDField -> parentGlobalID
        )
      )

      val db1 = Version.Live(
        ScopeID(10),
        DatabaseID(10).toDocID,
        Resolved(
          Timestamp.parse("1970-01-01T00:00:00.001Z"),
          Timestamp.parse("1970-01-01T00:00:00.001Z")),
        Create,
        SchemaVersion.Min,
        Data(
          SchemaNames.NameField -> SchemaNames.Name("child"),
          Database.ScopeField -> ScopeID(20),
          Database.GlobalIDField -> GlobalDatabaseID(20)
        )
      )

      val db1_deleted = Version.Deleted(
        ScopeID(10),
        DatabaseID(10).toDocID,
        Resolved(
          Timestamp.parse("1970-01-01T00:00:00.002Z"),
          Timestamp.parse("1970-01-01T00:00:00.002Z")),
        SchemaVersion.Min,
        None
      )

      val b = new SchemaMetadataBuilder

      b.handleVersion(db0)
      b.handleVersion(db1)
      b.handleVersion(db1_deleted)

      val databases = b.build().databases.asScala.values

      databases.size shouldBe 2

      databases should contain allElementsOf Seq(
        SchemaMetadata.RootDB,
        DB(
          Timestamp.parse("1970-01-01T00:00:00.000Z"),
          ScopeID(1),
          DatabaseID(1),
          ScopeID(10),
          GlobalDatabaseID(10),
          Some("parent"),
          Some(List("parent")),
          isDeleted = false,
          None,
          Seq(parentGlobalID)
        )
      )
    }

    "one live + one deleted for same scope" in {
      val db0 = Version.Live(
        ScopeID(1),
        DatabaseID(1).toDocID,
        Resolved(
          Timestamp.parse("1970-01-01T00:00:00.000Z"),
          Timestamp.parse("1970-01-01T00:00:00.000Z")),
        Create,
        SchemaVersion.Min,
        Data(
          SchemaNames.NameField -> SchemaNames.Name("loser"),
          Database.ScopeField -> ScopeID(10),
          Database.GlobalIDField -> GlobalDatabaseID(10)
        )
      )

      val db0_deleted = Version.Deleted(
        ScopeID(1),
        DatabaseID(1).toDocID,
        Resolved(
          Timestamp.parse("1970-01-01T00:00:00.001Z"),
          Timestamp.parse("1970-01-01T00:00:00.001Z")),
        SchemaVersion.Min,
        None
      )

      val winnerGlobalID = GlobalDatabaseID(10)
      val db1 = Version.Live(
        ScopeID(2),
        DatabaseID(2).toDocID,
        Resolved(
          Timestamp.parse("1970-01-01T00:00:00.001Z"),
          Timestamp.parse("1970-01-01T00:00:00.001Z")),
        Create,
        SchemaVersion.Min,
        Data(
          SchemaNames.NameField -> SchemaNames.Name("winner"),
          Database.ScopeField -> ScopeID(10),
          Database.GlobalIDField -> winnerGlobalID
        )
      )

      val b = new SchemaMetadataBuilder

      b.handleVersion(db0)
      b.handleVersion(db0_deleted)
      b.handleVersion(db1)

      val databases = b.build().databases.asScala.values

      databases.size shouldBe 2

      databases should contain allElementsOf Seq(
        SchemaMetadata.RootDB,
        DB(
          Timestamp.parse("1970-01-01T00:00:00.001Z"),
          ScopeID(2),
          DatabaseID(2),
          ScopeID(10),
          GlobalDatabaseID(10),
          Some("winner"),
          Some(List("winner")),
          isDeleted = false,
          None,
          Seq(winnerGlobalID)
        )
      )
    }

    "two deleted dbs" in {
      val db0_live = Version.Live(
        ScopeID(1),
        DatabaseID(1).toDocID,
        Resolved(
          Timestamp.parse("1970-01-01T00:00:00.000Z"),
          Timestamp.parse("1970-01-01T00:00:00.000Z")),
        Create,
        SchemaVersion.Min,
        Data(
          SchemaNames.NameField -> SchemaNames.Name("loser"),
          Database.ScopeField -> ScopeID(10),
          Database.GlobalIDField -> GlobalDatabaseID(10)
        )
      )

      val db0_deleted = Version.Deleted(
        ScopeID(1),
        DatabaseID(1).toDocID,
        Resolved(
          Timestamp.parse("1970-01-01T00:00:00.001Z"),
          Timestamp.parse("1970-01-01T00:00:00.001Z")),
        SchemaVersion.Min,
        None
      )

      val db1_live = Version.Live(
        ScopeID(2),
        DatabaseID(2).toDocID,
        Resolved(
          Timestamp.parse("1970-01-01T00:00:00.000Z"),
          Timestamp.parse("1970-01-01T00:00:00.000Z")),
        Create,
        SchemaVersion.Min,
        Data(
          SchemaNames.NameField -> SchemaNames.Name("winner"),
          Database.ScopeField -> ScopeID(10),
          Database.GlobalIDField -> GlobalDatabaseID(10)
        )
      )

      val db1_deleted = Version.Deleted(
        ScopeID(2),
        DatabaseID(2).toDocID,
        Resolved(
          Timestamp.parse("1970-01-01T00:00:00.001Z"),
          Timestamp.parse("1970-01-01T00:00:00.001Z")),
        SchemaVersion.Min,
        None
      )

      val b = new SchemaMetadataBuilder

      b.handleVersion(db0_live)
      b.handleVersion(db0_deleted)
      b.handleVersion(db1_live)
      b.handleVersion(db1_deleted)

      val databases = b.build().databases.asScala.values

      databases.size shouldBe 1

      databases.head shouldBe SchemaMetadata.RootDB
    }

    "deleted restored database versions should not delete current live database" in {
      val tenantRootGlobalID = GlobalDatabaseID(301567882240720960L)
      val tenantsRoot = Version.Live(
        ScopeID.RootID,
        DatabaseID(301567882226040896L).toDocID,
        Resolved(
          Timestamp.parse("2022-05-04T19:37:57.620Z"),
          Timestamp.parse("2022-05-04T19:37:57.620Z")),
        Create,
        SchemaVersion.Min,
        Data(
          SchemaNames.NameField -> SchemaNames.Name(
            "faunadb-cloud-tenants-production"),
          Database.ScopeField -> ScopeID(301567882240720960L),
          Database.GlobalIDField -> tenantRootGlobalID
        )
      )

      val userGlobalID = GlobalDatabaseID(302430815014356036L)
      val user_123456789 = Version.Live(
        ScopeID(301567882240720960L),
        DatabaseID(302430815014355012L).toDocID,
        Resolved(
          Timestamp.parse("2024-06-25T20:27:47.935Z"),
          Timestamp.parse("2024-06-25T20:27:47.935Z")),
        Create,
        SchemaVersion.Min,
        Data(
          SchemaNames.NameField -> SchemaNames.Name("user_123456789"),
          Database.ScopeField -> ScopeID(302430815014356036L),
          Database.GlobalIDField -> userGlobalID
        )
      )

      val restore_deleted = Version.Deleted(
        ScopeID.RootID,
        DatabaseID(385007355501215831L).toDocID,
        Resolved(
          Timestamp.parse("2023-12-24T15:09:06.730Z"),
          Timestamp.parse("2023-12-24T15:09:06.730Z")),
        SchemaVersion.Min,
        None
      )

      val restore_live = Version.Live(
        ScopeID.RootID,
        DatabaseID(385007355501215831L).toDocID,
        Resolved(
          Timestamp.parse("2023-12-24T15:07:56.980Z"),
          Timestamp.parse("2023-12-24T15:07:56.980Z")),
        Create,
        SchemaVersion.Min,
        Data(
          SchemaNames.NameField -> SchemaNames.Name(
            "restore-yq5m4ixzwybbg-yo38gqk1nynre-1703430450"),
          Database.ScopeField -> ScopeID(385007355535818839L),
          Database.GlobalIDField -> GlobalDatabaseID(385007355535819863L)
        )
      )

      val customer_db_old_deleted = Version.Deleted(
        ScopeID(385007355535818839L),
        DatabaseID(10).toDocID,
        Resolved(
          Timestamp.parse("2023-12-24T15:09:05.990Z"),
          Timestamp.parse("2023-12-24T15:09:05.990Z")),
        SchemaVersion.Min,
        None
      )

      val customer_db_old_live = Version.Live(
        ScopeID(385007355535818839L),
        DatabaseID(10).toDocID,
        Resolved(
          Timestamp.parse("2023-12-24T15:09:04.010Z"),
          Timestamp.parse("2023-12-24T15:09:04.010Z")),
        Create,
        SchemaVersion.Min,
        Data(
          SchemaNames.NameField -> SchemaNames.Name("customer_db"),
          Database.ScopeField -> ScopeID(385007357325739095L),
          Database.GlobalIDField -> GlobalDatabaseID(385007357325738071L)
        )
      )

      val customerDbGlobalID = GlobalDatabaseID(385007357325738071L)
      val customer_db = Version.Live(
        ScopeID(302430815014356036L),
        DatabaseID(385007428200038487L).toDocID,
        Resolved(
          Timestamp.parse("2023-12-24T15:09:05.990Z"),
          Timestamp.parse("2023-12-24T15:09:05.990Z")),
        Create,
        SchemaVersion.Min,
        Data(
          SchemaNames.NameField -> SchemaNames.Name("customer_db"),
          Database.ScopeField -> ScopeID(385007357325739095L),
          Database.GlobalIDField -> GlobalDatabaseID(385007357325738071L)
        )
      )

      val versions = Seq(
        tenantsRoot,
        user_123456789,
        restore_deleted,
        restore_live,
        customer_db_old_live,
        customer_db_old_deleted,
        customer_db
      )

      val b = new SchemaMetadataBuilder

      scala.util.Random.shuffle(versions).foreach(b.handleVersion(_))

      val meta = b.build()

      val databases = meta.databases.asScala.values

      meta.databases.size() shouldBe 4

      databases should contain allElementsOf Seq(
        SchemaMetadata.RootDB,
        DB(
          Timestamp.parse("2022-05-04T19:37:57.620Z"),
          ScopeID(0),
          DatabaseID(301567882226040896L),
          ScopeID(301567882240720960L),
          GlobalDatabaseID(301567882240720960L),
          Some("faunadb-cloud-tenants-production"),
          Some(List("faunadb-cloud-tenants-production")),
          isDeleted = false,
          None,
          Seq(tenantRootGlobalID)
        ),
        DB(
          Timestamp.parse("2024-06-25T20:27:47.935Z"),
          ScopeID(301567882240720960L),
          DatabaseID(302430815014355012L),
          ScopeID(302430815014356036L),
          GlobalDatabaseID(302430815014356036L),
          Some("user_123456789"),
          Some(List("faunadb-cloud-tenants-production", "user_123456789")),
          isDeleted = false,
          None,
          Seq(tenantRootGlobalID, userGlobalID)
        ),
        DB(
          Timestamp.parse("2023-12-24T15:09:05.990Z"),
          ScopeID(302430815014356036L),
          DatabaseID(385007428200038487L),
          ScopeID(385007357325739095L),
          GlobalDatabaseID(385007357325738071L),
          Some("customer_db"),
          Some(
            List(
              "faunadb-cloud-tenants-production",
              "user_123456789",
              "customer_db")),
          isDeleted = false,
          None,
          Seq(tenantRootGlobalID, userGlobalID, customerDbGlobalID)
        )
      )
    }

    "preserve latest name" in {
      val parentScope = ScopeID(1000)
      val childScope = ScopeID(2000)

      val ts0 = Timestamp.ofMillis(1)
      val ts1 = Timestamp.ofMillis(2)
      val ts2 = Timestamp.ofMillis(3)

      val parentDB = Version.Live(
        ScopeID.RootID,
        DatabaseID(1).toDocID,
        Resolved(ts0, ts0),
        Create,
        SchemaVersion.Min,
        Data(
          SchemaNames.NameField -> SchemaNames.Name("foo"),
          Database.ScopeField -> parentScope)
      )

      val parentDBRenamed0 = Version.Live(
        ScopeID.RootID,
        DatabaseID(1).toDocID,
        Resolved(ts1, ts1),
        Create,
        SchemaVersion.Min,
        Data(
          SchemaNames.NameField -> SchemaNames.Name("foo0"),
          Database.ScopeField -> parentScope)
      )

      val parentDBRenamed1 = Version.Live(
        ScopeID.RootID,
        DatabaseID(1).toDocID,
        Resolved(ts2, ts2),
        Create,
        SchemaVersion.Min,
        Data(
          SchemaNames.NameField -> SchemaNames.Name("foo1"),
          Database.ScopeField -> parentScope)
      )

      val childDB = Version.Live(
        parentScope,
        DatabaseID(1).toDocID,
        Resolved(ts1, ts1),
        Create,
        SchemaVersion.Min,
        Data(
          SchemaNames.NameField -> SchemaNames.Name("bar"),
          Database.ScopeField -> childScope)
      )

      val b = new SchemaMetadataBuilder

      // order here should not matter
      b.handleVersion(parentDBRenamed0)
      b.handleVersion(parentDBRenamed1)
      b.handleVersion(parentDB)
      b.handleVersion(childDB)

      val meta = b.build()
      meta.databases.size shouldBe 3
      meta.databases
        .get((ScopeID.RootID, DatabaseID(0)))
        .scopeID shouldBe ScopeID.RootID

      meta.databases
        .get((ScopeID.RootID, DatabaseID(1)))
        .scopeID shouldBe parentScope
      meta.databases.get((ScopeID.RootID, DatabaseID(1))).path shouldBe Some(
        List("foo1"))

      meta.databases.get((parentScope, DatabaseID(1))).scopeID shouldBe childScope
      meta.databases.get((parentScope, DatabaseID(1))).path shouldBe Some(
        List("foo1", "bar"))
    }
  }
}
