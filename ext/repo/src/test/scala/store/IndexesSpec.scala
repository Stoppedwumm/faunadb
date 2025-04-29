package fauna.repo.test

import fauna.atoms._
import fauna.lang.clocks.{ Clock, TestClock }
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.repo.{ IndexConfig => _, _ }
import fauna.repo.doc._
import fauna.repo.query.Query
import fauna.repo.schema.CollectionSchema
import fauna.repo.store._
import fauna.storage._
import fauna.storage.doc._
import fauna.storage.index._
import fauna.storage.ir._
import scala.concurrent.duration._

class IndexesSpec extends Spec {

  val ctx = CassandraHelper.context("repo")

  def newScope = ScopeID(ctx.nextID())

  val faunaClass = CollectionID(1024)
  val nameField = Field[String]("data", "name")

  def ID(subID: Long) = DocID(SubID(subID), faunaClass)
  def TS(ts: Long) = Timestamp.ofMicros(ts)

  "Indexes" - {
    "discovers invalid index entries" in {
      val scope = newScope
      val indexID = IndexID(1)

      val nameField = Field[String]("name")

      val config = IndexConfig(
        scope,
        indexID,
        faunaClass,
        Vector(DefaultExtractor(nameField.path)))
      val schema =
        CollectionSchema.empty(scope, faunaClass).copy(indexes = List(config))

      val version =
        ctx ! Store.insertCreate(schema, ID(1), TS(1), MapV("name" -> "fred").toData)

      val collQ =
        Store.sortedIndex(config, Vector(IndexTerm("fred"))).flattenT flatMap {
          indexEntries =>
            SortedIndex.invalidIndexRowsForDocID(scope, version.id) { (_, _) =>
              Query.value(Timestamp.Epoch)
            } map {
              (indexEntries, _)
            }
        }

      val (coll1, check1) = ctx ! collQ
      coll1.size should equal(1)
      check1.isEmpty should be(true)

      // Delete only the document, leaving a broken index in place
      val emptySchema = CollectionSchema.empty(scope, faunaClass)
      ctx ! Store.removeVersion(emptySchema, version.id, version.versionID)

      val (_, check2) = ctx ! collQ
      check2.size should equal(1)
      check2.head._2.value.docID should equal(version.docID)
    }

    "discovers invalid index entries for RowMultiSliceRead" in {
      val scope = newScope
      val indexID = IndexID(1)

      val nameField = Field[String]("name")

      val config = IndexConfig(
        scope,
        indexID,
        faunaClass,
        Vector(DefaultExtractor(nameField.path)))
      val schema =
        CollectionSchema.empty(scope, faunaClass).copy(indexes = List(config))

      val version =
        ctx ! Store.insertCreate(schema, ID(1), TS(1), MapV("name" -> "fred").toData)

      val collQ =
        Store
          .sparseCollection(
            config,
            Vector("fred"),
            Timestamp.MaxMicros,
            Vector(IndexTuple(scope, ID(1))),
            true)
          .flattenT flatMap { indexEntries =>
          SortedIndex.invalidIndexRowsForDocID(scope, version.id) { (_, _) =>
            Query.value(Timestamp.Epoch)
          } map {
            (indexEntries, _)
          }
        }

      val (coll1, check1) = ctx ! collQ
      coll1.size should equal(1)
      check1.isEmpty should be(true)

      // Delete only the document, leaving a broken index in place
      val emptySchema = CollectionSchema.empty(scope, faunaClass)
      ctx ! Store.removeVersion(emptySchema, version.id, version.versionID)

      val (_, check2) = ctx ! collQ
      check2.size should equal(1)
      check2.head._2.value.docID should equal(version.docID)
    }

    "covered values sort" in {
      val scope = newScope
      val indexID = IndexID(1)

      val nameField = Field[String]("name")
      val ageField = Field[Long]("age")

      val config = IndexConfig(
        scope,
        indexID,
        faunaClass,
        Vector(DefaultExtractor(ageField.path)),
        Vector(DefaultExtractor(nameField.path), DefaultExtractor(List("ref"))))
      val schema =
        CollectionSchema.empty(scope, faunaClass).copy(indexes = List(config))

      ctx ! Store.insertCreate(
        schema,
        ID(1),
        TS(1),
        MapV("name" -> "fred", "age" -> 10).toData)
      ctx ! Store.insertCreate(
        schema,
        ID(2),
        TS(1),
        MapV("name" -> "waldo", "age" -> 10).toData)
      ctx ! Store.insertCreate(schema, ID(3), TS(1), MapV("age" -> 10).toData)

      val people =
        ctx ! Store.collection(config, Vector(10), TS(2)).flattenT
      people map { _.tuple.coveredValues.head } map {
        _.value.toString
      } should equal(Seq("\"fred\"", "\"waldo\"", "null"))
    }

    "subsequent writes to instances with conflicts diff with the correct winner" in {
      val scope = newScope
      val userID = ID(1)
      val indexID = IndexID(1)

      val fooField = Field[String]("data", "foo")
      val config = IndexConfig(
        scope,
        indexID,
        faunaClass,
        Vector(DefaultExtractor(fooField.path)))
      val barIdx = "bar"
      val bazIdx = "baz"
      val schema =
        CollectionSchema.empty(scope, faunaClass).copy(indexes = List(config))

      def collection(term: String, ts: Timestamp) =
        (ctx ! Store.collection(config, Vector(term), ts)).value.toSeq map {
          _.tuple
        }

      ctx ! (Seq("bar", "baz") map { s =>
        Store.insertCreate(
          schema,
          userID,
          TS(1),
          MapV("data" -> MapV("foo" -> s)).toData)
      }).join

      ctx ! Store.insertDelete(schema, userID, TS(2))

      val user = ctx ! Store.getUnmigrated(scope, userID, TS(1))

      user.get.data(fooField) should equal("baz")

      collection(barIdx, TS(1)) should equal(Nil)
      collection(bazIdx, TS(1)) should equal(Seq(IndexTuple(scope, userID)))

      collection(barIdx, TS(2)) should equal(Nil)
      collection(bazIdx, TS(2)) should equal(Nil)
    }

    "given txns i, j where snapTS(j) == txnTS(i), index entries are correctly emitted" in {
      val scope = newScope
      val field = Field[String]("name")
      val config =
        IndexConfig(
          scope,
          IndexID(1),
          faunaClass,
          Vector(DefaultExtractor(field.path)))
      val schema = CollectionSchema
        .empty(scope, faunaClass)
        .copy(indexes = List(config))

      def mkVers(name: String) = Store.insert(schema, ID(1), Data(field -> name))

      val v1 = ctx !! mkVers("bob")

      val v2 = {
        val ctx0 = ctx.copy(clock = new TestClock(v1.transactionTS))
        ctx0 !! mkVers("fred")
      }

      val p1 = ctx ! Store
        .historicalIndex(config, Vector(IndexTerm(StringV("bob"))))
        .flattenT
      val p2 = ctx ! Store
        .historicalIndex(config, Vector(IndexTerm(StringV("fred"))))
        .flattenT

      val tup = IndexTuple(scope, ID(1), Vector())

      p1 should equal(
        Vector(
          IndexValue(tup, AtValid(v2.transactionTS), Remove),
          IndexValue(tup, AtValid(v1.transactionTS), Add)))

      p2 should equal(Vector(IndexValue(tup, AtValid(v2.transactionTS), Add)))
    }

    "don't emit index entries if document is ttl'ed" in {
      val scope = newScope
      val field = Field[String]("data", "name")
      val config =
        IndexConfig(
          scope,
          IndexID(1),
          faunaClass,
          Vector(DefaultExtractor(field.path)))
      val schema = CollectionSchema
        .empty(scope, faunaClass)
        .copy(indexes = List(config))

      val ttlTS = Clock.time - 5.seconds

      def mkVers(name: String) =
        Store.insert(
          schema,
          ID(1),
          Data(field -> name, Version.TTLField -> Some(ttlTS)))

      ctx ! mkVers("bob")

      val values = ctx ! Store
        .collection(config, Vector("bob"), Clock.time)
        .flattenT

      values shouldBe Seq.empty
    }

    "emit two events when adding a TTL" in {
      val scope = newScope
      val config = IndexConfig(scope, IndexID(1), faunaClass, Vector.empty)
      val schema = CollectionSchema
        .empty(scope, faunaClass)
        .copy(indexes = List(config))

      val ttl = Some(Clock.time + 1.hour)

      ctx ! Store.insert(schema, ID(1), Data.empty)
      ctx ! Store.insert(schema, ID(1), Data(Version.TTLField -> ttl))

      val values = ctx ! Store.historicalIndex(config, Vector.empty).flattenT

      values should have size 3

      values(0).tuple.ttl should equal(ttl)
      values(0).action should equal(Add)

      values(1).tuple.ttl should equal(None)
      values(1).action should equal(Remove)

      values(2).tuple.ttl should equal(None)
      values(2).action should equal(Add)
    }

    "can change TTL within query" in {
      val scope = newScope
      val field = Field[String]("data", "name")
      val config =
        IndexConfig(
          scope,
          IndexID(1),
          faunaClass,
          Vector(DefaultExtractor(field.path))
        )

      val schema =
        CollectionSchema
          .empty(scope, faunaClass)
          .copy(indexes = List(config))

      // Create without TTL, then add TTL
      ctx ! Query.snapshotTime.flatMap { snapTS =>
        Store.insert(schema, ID(1), Data(field -> "bob")) flatMap { _ =>
          val update = Data(field -> "bob", Version.TTLField -> Some(snapTS))
          Store.insert(schema, ID(1), update)
        }
      }

      val values =
        ctx ! Store
          .collection(config, Vector("bob"), Clock.time)
          .flattenT

      values shouldBe Seq.empty

      // Create with TTL, then remove TTL
      ctx ! Query.snapshotTime.flatMap { snapTS =>
        val create = Data(field -> "bob", Version.TTLField -> Some(snapTS))
        Store.insert(schema, ID(1), create) flatMap { _ =>
          val update = Data(field -> "bob", Version.TTLField -> None)
          Store.insert(schema, ID(1), update)
        }
      }

      val values0 =
        ctx ! Store
          .collection(config, Vector("bob"), Clock.time)
          .flattenT

      values0 should not be empty
    }

    "uniqueIDForKey is accurate at snapshots" in {
      val scope = newScope
      val field = Field[String]("name")
      val config =
        IndexConfig(
          scope,
          IndexID(1),
          faunaClass,
          Vector(DefaultExtractor(field.path)))
      val schema =
        CollectionSchema.empty(scope, faunaClass).copy(indexes = List(config))
      val total = DefaultPageSize * 4

      for {
        i <- 1 to total
        id = ID(i)
        ts = TS(i)
      } {
        ctx ! Store.insertCreate(schema, id, ts, Data(field -> "alice"))
      }

      for {
        i <- 1 to total
        id = ID(i)
        ts = TS(i)
      } {
        val rv =
          ctx ! Store.uniqueIDForKey(config, Vector(IndexTerm(StringV("alice"))), ts)
        rv should equal(Some(id))
      }
    }

    "out of order event writes remain consistent" in {
      val scope = newScope
      val config =
        IndexConfig(
          scope,
          IndexID(1),
          faunaClass,
          Vector(DefaultExtractor(List("class"))))
      val schema =
        CollectionSchema.empty(scope, faunaClass).copy(indexes = List(config))

      ctx ! Store.insertDelete(schema, ID(1), TS(6))
      ctx ! Store.insertCreate(schema, ID(1), TS(5), Data.empty)
      ctx ! Store.insertDelete(schema, ID(1), TS(4))
      ctx ! Store.insertDelete(schema, ID(1), TS(3))
      ctx ! Store.insertCreate(schema, ID(1), TS(2), Data.empty)
      ctx ! Store.insertCreate(schema, ID(1), TS(1), Data.empty)

      val eventsQ = Store.historicalIndex(
        config,
        Vector(IndexTerm(DocIDV(faunaClass.toDocID)))) mapValuesT {
        _.event
      } flattenT

      val expected =
        Seq((TS(6), Remove), (TS(5), Add), (TS(3), Remove), (TS(1), Add))

      (ctx ! eventsQ) map { e => (e.ts.validTS, e.action) } should equal(expected)
    }

    "build" in {
      val scope = newScope
      val data = Data(nameField -> "sam")
      val config = IndexConfig(
        scope,
        IndexID(1),
        faunaClass,
        Vector(DefaultExtractor(nameField.path)))
      val indexer = config.indexer

      val emptySchema = CollectionSchema.empty(scope, faunaClass)
      ctx ! Store.insertCreate(emptySchema, ID(1), TS(1), data)
      ctx ! Store.insertDelete(emptySchema, ID(1), TS(2))

      val terms = Vector(IndexTerm(StringV("sam")))

      val idxQ = Store.sortedIndex(config, terms).flattenT
      val instanceQ = Store.versions(emptySchema, ID(1)).flattenT

      (ctx ! idxQ).isEmpty should be(true)
      (ctx ! instanceQ).size should equal(2)

      val expected = Seq.newBuilder[IndexValue]

      val resultQ = Store.versions(emptySchema, ID(1)).flattenT map {
        Version.resolveConflicts(_) map { c =>
          expected ++= (ctx ! indexer.rows(c.canonical)) map { _.value }
          val result = IndexStore.build(c.canonical, indexer)
          val count = ctx ! result
          count should equal(1)
          count
        }
      }

      (ctx ! resultQ).sum should equal(2)
      (ctx ! idxQ) should equal(expected.result() map { v =>
        v.copy(ts = AtValid(v.ts.validTS))
      })
    }

    "concurrent access to value modifiers works" in {
      val scope = newScope
      val field = Field[String]("data", "name")
      val config =
        IndexConfig(
          scope,
          IndexID(1),
          faunaClass,
          Vector(DefaultExtractor(field.path)))
      val schema = CollectionSchema
        .empty(scope, faunaClass)
        .copy(indexes = List(config))

      val ttlTS = Clock.time + 5.seconds

      def mkVers(name: String) =
        Store.insert(
          schema,
          ID(1),
          Data(field -> name, Version.TTLField -> Some(ttlTS)))

      ctx ! mkVers("bob")

      val q = IndexStore
        .sortedIndex(
          config,
          Vector(IndexTerm(StringV("bob"))),
          IndexValue.MaxValue,
          IndexValue.MinValue,
          4)
        .flattenT
      val values = ctx ! Seq(q, q).sequence

      val ttls = values.flatten map { _.tuple.ttl } flatten

      ttls shouldBe Seq(ttlTS, ttlTS)
    }
  }
}
