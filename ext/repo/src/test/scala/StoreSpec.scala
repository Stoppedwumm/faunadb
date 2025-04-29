package fauna.repo.test

import fauna.atoms._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.repo.{ IndexConfig => _, _ }
import fauna.repo.query.Query
import fauna.repo.schema.CollectionSchema
import fauna.storage._
import fauna.storage.doc._
import fauna.storage.index.IndexTuple
import fauna.storage.ir._

class StoreSpec extends Spec {

  val ctx = CassandraHelper.context("repo")

  def newScope = ScopeID(ctx.nextID())

  val faunaCollection = CollectionID(1024)
  val collectionIdx = IndexID(1024)
  val nameIdx = IndexID(1025)
  def mkIndex(scope: ScopeID) = {
    val classCfg = IndexConfig(
      scope,
      collectionIdx,
      faunaCollection,
      Vector(DefaultExtractor(List("class"))))
    val nameCfg = IndexConfig(
      scope,
      nameIdx,
      faunaCollection,
      Vector(DefaultExtractor(List("data", "name"))))
    val indexer = classCfg.indexer + nameCfg.indexer
    (classCfg, nameCfg, indexer)
  }

  def ID(subID: Long) = DocID(SubID(subID), faunaCollection)
  def TS(ts: Long) = Timestamp.ofMicros(ts)

  "Store" - {
    "Basics" - {
      "works" in {
        val scope = newScope
        val (_, nameCfg, _) = mkIndex(scope)
        val schema = CollectionSchema
          .empty(scope, faunaCollection)
          .copy(indexes = List(nameCfg))

        val dataV1 = MapV("data" -> MapV("name" -> "sam")).toData
        val userV1 = ctx ! Store.insertCreate(schema, ID(1), TS(1), dataV1)

        userV1.data should equal(dataV1)
        (ctx ! Store.getUnmigrated(
          scope,
          userV1.docID,
          TS(2))).get should equalVersion(userV1)

        val dataV2 = dataV1 merge MapV("data" -> MapV("foo" -> "bar")).toData
        val userV2 =
          ctx ! Store.insertCreate(schema, ID(1), TS(3), dataV2)

        userV2.data should equal(dataV2)
        (ctx ! Store.getUnmigrated(
          scope,
          userV1.docID,
          userV2.ts.validTS)).get should equalVersion(userV2)
        (ctx ! Store.getUnmigrated(
          scope,
          userV1.docID,
          userV1.ts.validTS)).get should equalVersion(userV1)

        (ctx ! Store.versions(schema, userV1.docID).flattenT)
          .zip(Seq(userV2, userV1)) foreach { case (a, b) =>
          a should equalVersion(b)
        }

        (ctx ! Store
          .collection(nameCfg, Vector("sam"), userV2.ts.validTS)
          .flattenT) map { _.tuple } should equal(List(IndexTuple(scope, ID(1))))
      }

      "reads its own writes" in {
        val scope = newScope
        val schema = CollectionSchema.empty(scope, faunaCollection)

        for (i <- 101 to 1000) {
          val user = ctx ! Store.insertCreate(schema, ID(i), TS(1), Data.empty)

          user.docID should equal(ID(i))
          user.parentScopeID should equal(scope)
          (ctx ! Store.getUnmigrated(scope, ID(i), TS(1))).get should equalVersion(
            user)
        }
      }

      "reads pending writes" in {
        val scope = newScope
        val schema = CollectionSchema.empty(scope, faunaCollection)

        val (user, rv) = ctx ! (for {
          user <- Store.insertCreate(schema, ID(1), TS(1), Data.empty)
          rv   <- Store.getUnmigrated(scope, user.docID, TS(1))
        } yield (user, rv))

        rv should equal(Some(user))
        (ctx ! Store.getUnmigrated(
          scope,
          user.docID,
          TS(2))).get should equalVersion(user)
      }

      "create is hidden by delete at the same valid time" in {
        val scope = newScope
        val schema = CollectionSchema.empty(scope, faunaCollection)

        ctx ! Store.insertCreate(schema, ID(1), TS(1), Data.empty)
        val createV2 = ctx ! Store.insertCreate(schema, ID(1), TS(2), Data.empty)
        val delete = ctx ! Store.insertDelete(schema, ID(1), TS(1))
        val versions = ctx ! Store
          .versions(
            schema,
            ID(1),
            VersionID.MaxValue,
            VersionID.MinValue,
            DefaultPageSize,
            false)
          .flattenT

        versions.zip(Seq(createV2, delete)) foreach { case (a, b) =>
          a should equalVersion(b)
        }
      }

      "paged queries and foldLeftMT behave correctly" in {
        val scope = newScope
        val schema = CollectionSchema.empty(scope, faunaCollection)

        for (i <- 1 to 99)
          ctx ! Store.insertCreate(schema, ID(1), TS(i), Data.empty)

        @volatile var iters = 0L

        val pq = Store.versions(
          schema,
          ID(1),
          VersionID.MaxValue,
          VersionID.MinValue,
          2,
          false)

        val count = ctx ! (pq foldLeftMT 0L) { (count, page) =>
          Query.repo map { _ =>
            synchronized { iters = iters + 1 }
            page.size + count
          }
        }

        count should equal(99)
        iters should equal(50)
      }

      "ttl on read" in {
        val scope = newScope
        val (_, cfg, _) = mkIndex(scope)
        val schema = CollectionSchema
          .empty(scope, faunaCollection)
          .copy(indexes = List(cfg))

        val data =
          MapV("data" -> MapV("name" -> "sam"), "ttl" -> TimeV(TS(15))).toData
        val user = ctx ! Store.insertCreate(schema, ID(1), TS(10), data)

        user.data should equal(data)

        ctx ! Store.getUnmigrated(scope, user.docID, TS(1)) shouldBe None
        ctx ! Store.getUnmigrated(scope, user.docID, TS(10)) shouldBe None
        ctx ! Store.getUnmigrated(scope, user.docID, TS(15)) shouldBe None
        ctx ! Store.getUnmigrated(scope, user.docID, TS(20)) shouldBe None
        ctx ! Store.getUnmigrated(scope, user.docID) shouldBe None
      }
    }
  }
}
