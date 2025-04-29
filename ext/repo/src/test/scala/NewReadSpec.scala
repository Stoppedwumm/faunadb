package fauna.repo.test

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.repo.{ IndexConfig => _, Store }
import fauna.repo.query.Query
import fauna.repo.schema.CollectionSchema
import fauna.repo.store.HealthCheckStore
import fauna.stats.{ QueryMetrics, StatsRequestBuffer }
import fauna.storage.{ Value => _, _ }
import fauna.storage.api.set.Element
import fauna.storage.doc.{ Data, Field }
import fauna.storage.index._
import fauna.storage.ir.{ MapV, TimeV }
import fauna.storage.ops.VersionAdd

class NewReadSpec extends Spec {
  val ctx = CassandraHelper.context("repo")

  def newScopeID = ScopeID(ctx.nextID())

  val faunaCollection = CollectionID(1024)
  val collectionIdx = IndexID(1024)

  def ID(subID: Long) = DocID(SubID(subID), faunaCollection)
  def TS(ts: Long) = Timestamp.ofMicros(ts)

  def toElement(iv: IndexValue): Element = iv match {
    case IndexValue(tuple, ts, Add) =>
      Element.Live(tuple.scopeID, tuple.docID, tuple.values, ts.validTSOpt)
    case IndexValue(tuple, ts, Remove) =>
      Element.Deleted(tuple.scopeID, tuple.docID, tuple.values, ts.validTSOpt)
  }

  "New reads" - {
    "work" in {
      val scopeID = newScopeID

      val nameIdx = IndexID(1025)
      val nameCfg = IndexConfig(
        scopeID,
        nameIdx,
        faunaCollection,
        Vector(DefaultExtractor(List("data", "name"))),
        Vector(DefaultExtractor(List("data", "surname"))))
      val schema =
        CollectionSchema
          .empty(scopeID, faunaCollection)
          .copy(indexes = List(nameCfg))

      def mkData(name: String, surname: String) =
        MapV("data" -> MapV("name" -> name, "surname" -> surname)).toData

      val sam1 = mkData("sam", "I am")
      val sam1V2 = sam1.merge(MapV("surname" -> "I ain't").toData)
      val sam2 = mkData("sam", "fisher")
      val sam3 = mkData("sam", "adams")
      val john1 = mkData("john", "doe")
      val john2 = mkData("john", "hancock")
      val john3 = mkData("john", "adams")
      val steve = MapV(
        "ttl" -> TimeV(TS(2)),
        "data" -> MapV("name" -> "steve", "surname" -> "stevens")).toData

      def insertCreate(id: Int, ts: Int, data: Data) =
        ctx ! Store.insertCreate(schema, ID(id), TS(ts), data)

      // Sam.
      insertCreate(1, 1, sam1)
      insertCreate(2, 1, sam2)
      insertCreate(3, 1, sam3)
      insertCreate(2, 2, sam1V2)
      ctx ! Store.insertDelete(schema, ID(2), TS(3))

      // John.
      insertCreate(4, 1, john1)
      insertCreate(5, 2, john2)
      insertCreate(6, 3, john3)
      ctx ! Store.insertDelete(schema, ID(5), TS(4))

      // Steve.
      insertCreate(7, 1, steve)

      {
        // Doc history works.
        // Use ID(2) because it has multiple events.
        val history = ctx ! (Store.versions(schema, ID(2)) flattenT)

        // Doc history works with pagination.
        val pages =
          ctx ! Store.versions(schema, ID(2), pageSize = 1).flattenT
        pages shouldBe (history)
      }

      {
        // Set sorted values works with pagination.
        val entries =
          ctx ! Store.sortedIndex(nameCfg, Vector(IndexTerm("sam"))).flattenT
        val pages = ctx ! Store
          .sortedIndex(nameCfg, Vector(IndexTerm("sam")), pageSize = 1)
          .flattenT
        pages shouldBe (entries)
      }

      {
        // Set snapshot works.
        val entries = ctx ! (Query.snapshotTime flatMap { snap =>
          Store.collection(nameCfg, Vector("sam"), snap)
        } flattenT)
        // Set snapshot works with pagination.
        val pages = ctx ! Store
          .collection(nameCfg, Vector("sam"), Timestamp.MaxMicros, pageSize = 1)
          .flattenT
        pages shouldBe (entries)
      }

      { // Sparse set snapshot works.
        // Applies TTLs (because set snapshot does).
        {
          val ts = Vector(IndexTuple(scopeID, ID(-1), Vector()))
          (ctx ! Store
            .sparseCollection(nameCfg, Vector("steve"), TS(2), ts, false)
            .flattenT) shouldBe empty
        }
        // Returns an empty page on empty slices
        (ctx ! Store
          .sparseCollection(nameCfg, Vector("steve"), TS(2), Vector.empty, false)
          .flattenT) shouldBe empty
      }

      {
        // Set history works.
        val entries =
          ctx ! Store.historicalIndex(nameCfg, Vector(IndexTerm("sam"))).flattenT
        // Set history works with pagination.
        val pages =
          ctx ! Store.historicalIndex(nameCfg, Vector(IndexTerm("sam"))).flattenT
        pages shouldBe (entries)
      }

      {
        // GetLatestNoTTL gets the latest version, whether live or deleted.
        val v = ctx ! Store.getLatestNoTTLUnmigrated(scopeID, ID(5))
        val cursor =
          ctx ! Store.getLatestNoTTLUnmigrated(
            scopeID,
            ID(5),
            VersionID(TS(4), Delete))
        v exists { _.isDeleted } shouldBe true
        cursor exists { _.isDeleted } shouldBe true
      }
    }

    "cache results" in {
      val scopeID = newScopeID
      val schema = CollectionSchema.empty(scopeID, faunaCollection)
      val samV1 = MapV("data" -> MapV("name" -> "sam")).toData
      ctx ! Store.insertCreate(schema, ID(1), TS(1), samV1)

      val stats = new StatsRequestBuffer(Set(QueryMetrics.BytesRead))
      val statsCtx = ctx.withStats(stats)

      // Read it once.
      var bytesRead1 = 0L
      statsCtx ! Store.getUnmigrated(scopeID, ID(1))
      stats.output({ case (_, v) => bytesRead1 = v.asInstanceOf[Long] })

      // Read it twice.
      statsCtx ! (Store.getUnmigrated(scopeID, ID(1)) flatMap { _ =>
        Store.getUnmigrated(scopeID, ID(1))
      })
      // Caching means total bytes read is 2x one read instead of 3x.
      stats.output({ case (_, v) => v.asInstanceOf[Long] shouldBe 2 * bytesRead1 })

      // Caching should take into account pending writes as well.
      val readQ = Store.getUnmigrated(scopeID, ID(2))
      val createQ =
        Store.insertCreate(schema, ID(2), TS(1), Data.empty)
      val q = readQ flatMap {
        case Some(_) => fail("document already exists?")
        case None    => createQ
      } flatMap { _ => readQ }
      (ctx ! q).isEmpty shouldBe false
    }

    "apply pending writes" in {
      val scopeID = newScopeID
      val schema = CollectionSchema.empty(scopeID, faunaCollection)
      val samV1 = MapV("data" -> MapV("name" -> "sam")).toData
      val samV2 = MapV("data" -> MapV("name" -> "samwise")).toData
      ctx ! Store.insertCreate(schema, ID(1), TS(1), samV1)

      val getQ = Store.getUnmigrated(scopeID, ID(1))
      val nameField = Field[String]("data", "name")

      {
        // An irrelevant write doesn't change the result.
        val writeQ =
          Query.write(
            VersionAdd(
              scopeID,
              ID(2),
              Unresolved,
              Create,
              SchemaVersion.Min,
              samV2,
              None))
        val get = ctx ! (writeQ flatMap { _ => getQ })
        get.value.data(nameField) shouldBe "sam"
      }

      {
        // A relevant pending write may not change the result.
        val writeQ = Query.write(
          VersionAdd(
            scopeID,
            ID(1),
            AtValid(TS(0)),
            Create,
            SchemaVersion.Min,
            samV2,
            None))
        val get = ctx ! (writeQ flatMap { _ => getQ })
        get.value.data(nameField) shouldBe "sam"
      }

      {
        // A relevant pending write can change the result.
        val writeQ =
          Query.write(
            VersionAdd(
              scopeID,
              ID(1),
              Unresolved,
              Create,
              SchemaVersion.Min,
              samV2,
              None))
        val get = ctx ! (writeQ flatMap { _ => getQ })
        get.value.data(nameField) shouldBe "samwise"
      }
    }

    "work for health checks" in {
      val id0 = HostID.randomID
      val id1 = HostID.randomID
      ctx ! HealthCheckStore.insert(id0, TS(1))
      ctx ! HealthCheckStore.insert(id1, TS(1))
      ctx ! HealthCheckStore.insert(id0, TS(2))
      ctx ! HealthCheckStore.insert(id1, TS(3))

      val neuu = ctx ! HealthCheckStore.getAll
      neuu shouldBe Map(id0 -> TS(2), id1 -> TS(3))
    }
  }
}
