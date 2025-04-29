package fauna.model.test

import fauna.ast.PageL
import fauna.atoms._
import fauna.auth.Auth
import fauna.codex.json._
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.model.{ Collection, Index, Task }
import fauna.model.runtime.fql2._
import fauna.model.runtime.fql2.FQLInterpreter.TypeMode
import fauna.model.runtime.Effect
import fauna.model.schema.index.{ CollectionIndex, UserDefinedIndex }
import fauna.model.tasks.{ IndexSwap, TaskExecutor, TaskRouter }
import fauna.repo.{ IndexConfig, Store }
import fauna.repo.doc.{ FieldIndexer, Version }
import fauna.repo.query.ReadCache
import fauna.repo.values.Value
import fauna.storage.api.set._
import fauna.storage.doc.{ Data, FieldPair }
import fauna.storage.index.IndexTerm
import fauna.storage.ir._
import fauna.storage.Tables
import java.time.temporal.ChronoUnit
import java.time.Instant
import org.scalactic.source.Position
import org.scalatest.tags.Slow
import org.scalatest.Inspectors._
import scala.collection.immutable.ArraySeq
import scala.concurrent.duration.DurationInt
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global

@Slow
class FQL2IndexSpec extends FQL2WithV4Spec {
  "FQL2Indexes" - {
    "indexes are created via create" in {
      val auth = newDB

      val collID = evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byName: {
            |      terms: [{ field: "name" }]
            |    },
            |    byAge: {
            |      terms: [{ field: "age" }]
            |    }
            |  }
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      (ctx ! Collection.getUncached(
        auth.scopeID,
        collID)).value.active.get.config.collIndexes
        .asInstanceOf[List[UserDefinedIndex]]
        .map(_.name) shouldBe List("byName", "byAge")
      (ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID)).size shouldBe 2

      val person = evalOk(
        auth,
        """|Person.create({
           |  name: "John",
           |  age: 50
           |})""".stripMargin
      ).to[Value.Doc]

      val collIndexes =
        (ctx ! Collection.getUncached(
          auth.scopeID,
          collID)).value.active.get.config.collIndexes
          .asInstanceOf[List[UserDefinedIndex]]
      val indexes = ctx ! Index.getUserDefinedBySourceUncached(auth.scopeID, collID)
      val byNameId = collIndexes.find(_.name == "byName").value.indexID
      val byName = indexes find { _.id == byNameId }
      val byAgeId = collIndexes.find(_.name == "byAge").value.indexID
      val byAge = indexes find { _.id == byAgeId }

      // byName
      validateIndex(
        byName.value,
        person.id,
        Vector("John"),
        Vector.empty
      )

      // byAge
      validateIndex(
        byAge.value,
        person.id,
        Vector(50),
        Vector.empty
      )
    }

    "partitions are set correctly" in {
      val auth = newDB

      val collID = evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byName: {
            |      terms: [{ field: "name" }]
            |    },
            |    byAge: {
            |      terms: []
            |    }
            |  }
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      val collIndexes =
        (ctx ! Collection.getUncached(
          auth.scopeID,
          collID)).value.active.get.config.collIndexes
          .asInstanceOf[List[UserDefinedIndex]]
      val indexes = ctx ! Index.getUserDefinedBySourceUncached(auth.scopeID, collID)
      val byNameId = collIndexes.find(_.name == "byName").value.indexID
      val byName = indexes find { _.id == byNameId }
      val byAgeId = collIndexes.find(_.name == "byAge").value.indexID
      val byAge = indexes find { _.id == byAgeId }

      byName.get.partitions shouldBe 1
      byAge.get.partitions shouldBe 8
    }

    "read partitions once" in {
      val auth = newDB

      evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byName: { terms: [{ field: "name" }] },
            |    ages: { values: [{ field: "age" }] }
            |  }
            |})
            |""".stripMargin
      )

      evalOk(
        auth,
        s"""|Set.sequence(0, 64).forEach(n =>
            |  Person.create({
            |    name: "Bob",
            |    age: n
            |  })
            |)
            |""".stripMargin
      )

      def assertSinglePageRead(query: String, parts: Int)(implicit pos: Position) = {
        val state = eval(auth, query).state.get
        val reads = state.readsWrites.reads
        val pages =
          reads count { read =>
            read.cf == Tables.SortedIndex.CFName
          }
        withClue("number of pages read") {
          math.ceil(pages.toDouble / parts).toInt shouldBe 1
        }
      }

      assertSinglePageRead("Person.byName('Bob').map(.name).paginate()", parts = 1)
      // FIXME: It seems as if the per partition page size is damaging performance on
      // small pages. Requesting 16 elements result in 8 pages of 3 elements each
      // which requires additional round-trips to storage when merging them.
      pendingUntilFixed {
        assertSinglePageRead("Person.ages().map(.age).paginate()", parts = 8)
      }
    }

    "indexes can cover values of ts and ttl" in {
      val auth = newDB
      val collID = evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byName: {
            |      terms: [{ field: "name" }],
            |      values: [{ field: "ts" }, { field: "ttl" }]
            |    },
            |  }
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      val ttl = Instant.now().plus(3, ChronoUnit.DAYS).toString
      val doc = evalOk(
        auth,
        s"""|Person.create({
            |  name: "John",
            |  ttl: Time.fromString('$ttl'),
            |  age: 50
            |})""".stripMargin
      ).to[Value.Doc]

      val personDoc = ctx ! Store.getUnmigrated(auth.scopeID, doc.id)
      val idx =
        (ctx ! Index.getUserDefinedBySourceUncached(auth.scopeID, collID)).head
      val res = ctx ! (ctx ! Store.collection(
        idx,
        terms = Vector("John"),
        Clock.time)).flatten
      res.size shouldBe 1

      res.head.tuple.values shouldEqual Vector(
        IndexTerm(TimeV(personDoc.value.ts.transactionTS)),
        IndexTerm(TimeV(personDoc.value.ttl.get))
      )
    }

    "indexes can cover id" in {
      val auth = newDB

      val coll1 = evalOk(
        auth,
        s"""|Collection.create({
            |  name: 'Person',
            |  indexes: {
            |    byID2: { values: [{ field: 'id' }] },
            |    byIDDesc: { values: [{ field: 'id', order: 'desc' }] },
            |    byIDAnd: { values: [{ field: 'id'}, { field: 'name' }] }
            |  }
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      val coll = (ctx ! Store.getUnmigrated(auth.scopeID, coll1.toDocID)).get
      val backing =
        coll.data.fields
          .get(List("backingIndexes"))
          .get
          .asInstanceOf[ArrayV]
          .elems
          .map(_.asInstanceOf[MapV])

      // backing index for byID2 has no 'id' field
      forAtLeast(1, backing) { m =>
        m.get(List("values")).get shouldEqual ArrayV.empty
      }
      // backing index for byIDDesc has 'id' field
      forAtLeast(1, backing) { m =>
        m.get(List("values")).get shouldEqual ArrayV(
          MapV("field" -> "id", "order" -> "desc"))
      }
      // backing index for byIDAnd has 'id' field
      forAtLeast(1, backing) { m =>
        m.get(List("values")).get shouldEqual ArrayV(
          MapV("field" -> "id", "order" -> "asc"),
          MapV("field" -> "name", "order" -> "asc"))
      }

      evalOk(auth, "Person.create({ id: '123', name: 'John' })")
      evalOk(auth, "Person.create({ id: '456', name: 'Alice' })")

      evalOk(auth, "Person.byID2().toArray().map(.id)") shouldEqual
        Value.Array(ArraySeq(Value.ID(123), Value.ID(456)))

      evalOk(auth, "Person.byID2({ from: '124' }).toArray().map(.id)") shouldEqual
        Value.Array(ArraySeq(Value.ID(456)))

      evalOk(auth, "Person.byID2({ from: 124 }).toArray().map(.id)") shouldEqual
        Value.Array(ArraySeq(Value.ID(456)))

      evalOk(
        auth,
        "Person.byID2({ from: ID('124') }).toArray().map(.id)") shouldEqual
        Value.Array(ArraySeq(Value.ID(456)))

      // handles non-id values
      evalOk(
        auth,
        "Person.byIDDesc({ from: 'a' }).toArray().map(.id)") shouldEqual Value.Array(
        ArraySeq(Value.ID(456), Value.ID(123)))
      evalOk(
        auth,
        "Person.byIDDesc({ from: null }).toArray().map(.id)") shouldEqual Value.Array.empty

      evalOk(
        auth,
        "Person.byIDAnd({ from: ['a'] }).toArray().map(.id)") shouldEqual Value.Array.empty
      evalOk(
        auth,
        "Person.byIDAnd({ from: [null] }).toArray().map(.id)") shouldEqual Value
        .Array(ArraySeq(Value.ID(123), Value.ID(456)))

      evalOk(
        auth,
        "Person.byID2({ from: 'a' }).toArray().map(.id)") shouldEqual Value.Array.empty
      evalOk(
        auth,
        "Person.byID2({ from: null }).toArray().map(.id)") shouldEqual Value.Array(
        ArraySeq(Value.ID(123), Value.ID(456)))
    }

    "indexes covering id ascending can reuse existing index" in {
      val auth = newDB

      val coll1 = evalOk(
        auth,
        s"""|Collection.create({
            |  name: 'Person',
            |  indexes: {
            |    byID1: { values: [{ field: 'id' }, { field: 'id' }] }
            |  }
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      evalOk(auth, "Person.create({ id: '123', name: 'John' })")
      evalOk(auth, "Person.create({ id: '456', name: 'Alice' })")

      val backing1 =
        (ctx ! Store.getUnmigrated(auth.scopeID, coll1.toDocID)).get.data.fields
          .get(List("backingIndexes"))
          .get
          .asInstanceOf[ArrayV]
          .elems
          .map(_.asInstanceOf[MapV])

      // backing index for byID1 has one 'id' field, created using new rules
      backing1.size shouldEqual 1
      backing1.head.get(List("values")).get shouldEqual ArrayV(
        MapV("field" -> "id", "order" -> "asc"))

      evalOk(
        auth,
        s"""|Collection.byName('Person')!.update({
            |  indexes: {
            |    byID1: { values: [{ field: 'id' }, { field: 'id' }] },
            |    byID2: { values: [{ field: 'id' }] }
            |  }
            |})""".stripMargin
      )

      val backing2 =
        (ctx ! Store.getUnmigrated(auth.scopeID, coll1.toDocID)).get.data.fields
          .get(List("backingIndexes"))
          .get
          .asInstanceOf[ArrayV]
          .elems
          .map(_.asInstanceOf[MapV])

      // backing index for byID2 should reuse byID1 based on compat rules
      backing2.size shouldEqual 2
      backing2.map(_.get(List("values")).get) should contain theSameElementsAs Seq(
        ArrayV(),
        ArrayV(MapV("field" -> "id", "order" -> "asc")))

      evalOk(
        auth,
        "Person.byID1({ from: ID('124') }).toArray().map(.id)") shouldEqual
        Value.Array(ArraySeq(Value.ID(456)))

      evalOk(
        auth,
        "Person.byID2({ from: ID('124') }).toArray().map(.id)") shouldEqual
        Value.Array(ArraySeq(Value.ID(456)))
    }

    "ttl can be used in index terms" in {
      val auth = newDB
      evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byTtl: {
            |      terms: [{ field: "ttl" }],
            |    },
            |  }
            |})""".stripMargin
      )

      val ttl = Instant.now().plus(3, ChronoUnit.DAYS).toString
      val doc = evalOk(
        auth,
        s"""|Person.create({
            |  name: "John",
            |  ttl: Time.fromString('$ttl'),
            |  age: 50
            |})""".stripMargin
      ).to[Value.Doc]

      val byTtlRes = evalOk(
        auth,
        s"Person.byTtl(Time.fromString('$ttl')).paginate().data".stripMargin
      ).to[Value.Array]

      byTtlRes.elems.length shouldBe 1
      byTtlRes.elems.head shouldEqual doc
    }
    "documents with that don't have covered values are still indexed if they have indexed terms present" in {
      val auth = newDB

      evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byA: {
            |      terms: [{ field: "a" }],
            |      values: [{ field: "b" }]
            |    }
            |  }
            |})""".stripMargin
      )
      val doc = evalOk(auth, "Person.create({ a: 'indexMe!' })")
      evalOk(auth, "Person.create({ b: 'non indexed doc' })")
      val res = evalOk(auth, "Person.byA('indexMe!').toArray()")
      res should matchPattern {
        case Value.Array(elems) if elems == ArraySeq(doc) =>
      }
    }
    "documents with null value come after documents in unbounded to queries and don't show up in bounded to queries" in {
      val auth = newDB

      evalOk(
        auth,
        s"""|
            |Collection.create({
            |  name: "Products",
            |  indexes: {
            |    byPrice: {
            |      terms: [{ field: "category" }],
            |      values: [{ field: "price" }, { field: "count" } ]
            |    }
            |  }
            |}
            |)""".stripMargin
      )
      val docNull = evalOk(auth, "Products.create({ category: 'sports' })")
      val docValPrice =
        evalOk(auth, "Products.create({ category: 'sports', price: 1000 })")
      val docValCount =
        evalOk(auth, "Products.create({ category: 'sports', count: 200})")
      val docValAll = evalOk(
        auth,
        "Products.create({ category: 'sports', price: 300, count: 300})")

      evalOk(
        auth,
        "Products.byPrice('sports', { from: 100 }).toArray()") should matchPattern {
        case Value.Array(elems)
            if elems == ArraySeq(docValAll, docValPrice, docValCount, docNull) =>
      }
      evalOk(
        auth,
        "Products.byPrice('sports', { from: (100, 100) }).toArray()") should matchPattern {
        case Value.Array(elems)
            if elems == ArraySeq(docValAll, docValPrice, docValCount, docNull) =>
      }
      evalOk(
        auth,
        "Products.byPrice('sports', { from: null }).toArray()") should matchPattern {
        case Value.Array(elems) if elems == ArraySeq(docValCount, docNull) =>
      }
      evalOk(
        auth,
        "Products.byPrice('sports', { from: (null, null) }).toArray()") should matchPattern {
        case Value.Array(elems) if elems == ArraySeq(docNull) =>
      }
      evalOk(
        auth,
        "Products.byPrice('sports', { to: 1000 }).toArray()") should matchPattern {
        case Value.Array(elems) if elems == ArraySeq(docValAll, docValPrice) =>
      }
      evalOk(
        auth,
        "Products.byPrice('sports', { to: (1000, 1000) }).toArray()") should matchPattern {
        case Value.Array(elems) if elems == ArraySeq(docValAll) =>
      }
      evalOk(
        auth,
        "Products.byPrice('sports', { to: null }).toArray()") should matchPattern {
        case Value.Array(elems)
            if elems == ArraySeq(docValAll, docValPrice, docValCount, docNull) =>
      }
      evalOk(
        auth,
        "Products.byPrice('sports', { to: (null, null) }).toArray()") should matchPattern {
        case Value.Array(elems)
            if elems == ArraySeq(docValAll, docValPrice, docValCount, docNull) =>
      }
    }
    "documents with null values in a desc index show up first in unbounded from queries and don't show up in bounded from queries" in {
      val auth = newDB

      evalOk(
        auth,
        s"""|
            |Collection.create({
            |  name: "Products",
            |  indexes: {
            |    byPrice: {
            |      terms: [{ field: "category" }],
            |      values: [{ field: "price", order: "desc" }, { field: "count", order: "desc" }]
            |    }
            |  }
            |}
            |)""".stripMargin
      )
      val docNull = evalOk(auth, "Products.create({ category: 'sports' })")
      val docValPrice =
        evalOk(auth, "Products.create({ category: 'sports', price: 1000 })")
      val docValCount =
        evalOk(auth, "Products.create({ category: 'sports', count: 200})")
      val docValAll = evalOk(
        auth,
        "Products.create({ category: 'sports', price: 300, count: 300})")

      evalOk(
        auth,
        "Products.byPrice('sports', { to: 100 }).toArray()") should matchPattern {
        case Value.Array(elems)
            if elems == ArraySeq(docNull, docValCount, docValPrice, docValAll) =>
      }
      evalOk(
        auth,
        "Products.byPrice('sports', { to: (100, 100) }).toArray()") should matchPattern {
        case Value.Array(elems)
            if elems == ArraySeq(docNull, docValCount, docValPrice, docValAll) =>
      }
      evalOk(
        auth,
        "Products.byPrice('sports', { to: null }).toArray()") should matchPattern {
        case Value.Array(elems) if elems == ArraySeq(docNull, docValCount) =>
      }
      evalOk(
        auth,
        "Products.byPrice('sports', { to: (null, null) }).toArray()") should matchPattern {
        case Value.Array(elems) if elems == ArraySeq(docNull) =>
      }
      evalOk(
        auth,
        "Products.byPrice('sports', { from: 1000 }).toArray()") should matchPattern {
        case Value.Array(elems) if elems == ArraySeq(docValPrice, docValAll) =>
      }
      evalOk(
        auth,
        "Products.byPrice('sports', { from: (1000, 1000) }).toArray()") should matchPattern {
        case Value.Array(elems) if elems == ArraySeq(docValAll) =>
      }
      evalOk(
        auth,
        "Products.byPrice('sports', { from: null }).toArray()") should matchPattern {
        case Value.Array(elems)
            if elems == ArraySeq(docNull, docValCount, docValPrice, docValAll) =>
      }
      evalOk(
        auth,
        "Products.byPrice('sports', { from: (null, null) }).toArray()") should matchPattern {
        case Value.Array(elems)
            if elems == ArraySeq(docNull, docValCount, docValPrice, docValAll) =>
      }
    }

    // This is a regression test where indexing refs to a deleted collection
    // would cause exceptions getting a read broker for the collection.
    "handles indexing refs to deleted collection" in {
      val auth = newDB

      evalOk(auth, "Collection.create({ name: 'B' })")
      evalOk(auth, "B.create({ id: 0, foo: 1 })")

      evalOk(auth, "Collection.create({ name: 'A' })")
      evalOk(auth, "A.create({ id: 0, b: B.byId(0) })")

      evalOk(auth, "B.definition.delete()")
      evalOk(
        auth,
        """|A.definition.update({
           |  indexes: {
           |    byBFoo: { terms: [{ field: ".b.foo" }] }
           |  }
           |})""".stripMargin
      )

      // Of course, nothing should have been indexed.
      evalOk(auth, "A.byBFoo(1).isEmpty()") shouldBe Value.True
    }

    "non-MVA" - {
      val auth = newDB

      evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    noMva: {
            |      terms: [{ field: "a" }],
            |      values: [{ field: "b" }]
            |    }
            |  }
            |})""".stripMargin
      )

      "indexes support indexing objects" in {
        val d1 = evalOk(auth, "Person.create({ a: 'obj', b: { x: 1, y: 1 } })")
        val d2 = evalOk(auth, "Person.create({ a: 'obj', b: { y: 2, x: 2 } })")
        val d3 = evalOk(auth, "Person.create({ a: 'obj', b: { x: 3, y: 3 } })")
        val d4 = evalOk(auth, "Person.create({ a: 'obj', b: { y: 4, x: 4 } })")

        evalOk(auth, "Person.noMva('obj').toArray()") shouldEqual
          Value.Array(d1, d2, d3, d4)

        evalOk(
          auth,
          """|Person.noMva('obj', {
             |  from: { x: 2, y: 2 },
             |  to: { y: 3, x: 3 }
             |}).toArray()""".stripMargin
        ) shouldEqual Value.Array(d2, d3)

        evalOk(
          auth,
          """|Person.noMva('obj', {
             |  from: { x: 2 },
             |  to: { x: 4 }
             |}).toArray()""".stripMargin
        ) shouldEqual Value.Array(d2, d3)

        val d5 = evalOk(auth, "Person.create({ a: { x: 1, y: 1 }, b: 1 })")
        val d6 = evalOk(auth, "Person.create({ a: { y: 1, x: 1 }, b: 2 })")

        evalOk(auth, "Person.noMva({ y: 1, x: 1 }).toArray()") shouldEqual
          Value.Array(d5, d6)
        evalOk(auth, "Person.noMva({ x: 1, y: 1 }).toArray()") shouldEqual
          Value.Array(d5, d6)
      }

      "indexes support indexing arrays" in {
        val d1 = evalOk(auth, "Person.create({ a: 'arr', b: [1, 2] })")
        val d2 = evalOk(auth, "Person.create({ a: 'arr', b: [3, 4] })")
        val d3 = evalOk(auth, "Person.create({ a: 'arr', b: [5, 6] })")
        val d4 = evalOk(auth, "Person.create({ a: 'arr', b: [7, 8] })")

        evalOk(auth, "Person.noMva('arr').toArray()") shouldEqual
          Value.Array(d1, d2, d3, d4)

        evalOk(
          auth,
          """|Person.noMva('arr', {
             |  from: [3, 4],
             |  to: [5, 6]
             |}).toArray()""".stripMargin
        ) shouldEqual Value.Array(d2, d3)

        evalOk(
          auth,
          """|Person.noMva('arr', {
             |  from: [3],
             |  to: [5]
             |}).toArray()""".stripMargin
        ) shouldEqual Value.Array(d2, d3)

        val d5 = evalOk(auth, "Person.create({ a: [1, 2], b: 1 })")
        val d6 = evalOk(auth, "Person.create({ a: [1, 2], b: 2 })")

        evalOk(auth, "Person.noMva([1, 2]).toArray()") shouldEqual
          Value.Array(d5, d6)
        evalOk(auth, "Person.noMva([1, 2]).toArray()") shouldEqual
          Value.Array(d5, d6)
      }
    }

    "indexes can MVA arrays" in {
      val auth = newDB

      evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    mva: {
            |      terms: [{ field: "a", mva: true }]
            |    }
            |  }
            |})""".stripMargin
      )

      val d1 = evalOk(auth, "Person.create({ a: [1, 2] })")
      evalOk(auth, "Person.mva(1).toArray()") shouldEqual Value.Array(d1)
      evalOk(auth, "Person.mva(2).toArray()") shouldEqual Value.Array(d1)
    }

    "MVA values are discarded from partals" in {
      val auth = newDB

      evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Foo",
            |  indexes: {
            |    mva: {
            |      values: [
            |        { field: "a", mva: false },
            |        { field: "b", mva: true },
            |        { field: "c", mva: false }
            |      ]
            |    }
            |  }
            |})""".stripMargin
      )

      evalOk(auth, "Foo.create({ a: [1, 2], b: [3, 4], c: [5, 6] })")

      val part = evalOk(auth, "Foo.mva().map(.data).first()")
        .asInstanceOf[Value.Struct.Partial]

      project(part, "a").value shouldBe Value.Array(Value.Int(1), Value.Int(2))
      project(part, "b") shouldBe empty
      project(part, "c").value shouldBe Value.Array(Value.Int(5), Value.Int(6))
    }

    "MVA terms are discarded from partials" in {
      val auth = newDB
      evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Product",
            |  indexes: {
            |    byName: {
            |      terms: [{ field: "nameMva", mva: true }, { field: "name" }],
            |    }
            |  }
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      val part = evalOk(
        auth,
        s"""
           |Product.create({  name: "basketball", nameMva: "soccer" })
           |Product.byName("soccer", "basketball").map(.data).first()
           |""".stripMargin
      ).asInstanceOf[Value.Struct.Partial]

      project(part, "nameMva") shouldBe empty
      project(part, "name").value shouldBe Value.Str("basketball")
    }

    "collection indexes are hidden from FQL 4" in {
      val auth = newDB

      evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byName: {
            |      terms: [{ field: "name" }]
            |    },
            |    byAge: {
            |      terms: [{ field: "age" }]
            |    }
            |  }
            |})""".stripMargin
      )

      evalV4Ok(auth, Paginate(JSObject("indexes" -> JSNull))) shouldEqual PageL(
        Nil,
        Nil,
        None,
        None)

      evalV4Ok(
        auth,
        Paginate(
          JSObject("documents" -> JSObject("indexes" -> JSNull)))) shouldEqual PageL(
        Nil,
        Nil,
        None,
        None)
    }

    "indexes are created when added to collections via update" in {
      val auth = newDB

      val personCID = mkColl(auth, "Person")
      val authorCID = mkColl(auth, "Author")

      evalOk(
        auth,
        s"""|Person.definition.update({
            |  indexes: {
            |    byName: {
            |      terms: [{ field: "name" }],
            |      values: [{ field: "occupation" }, {field: "years", order: "desc"}]
            |    }
            |  }
            |})
            |Author.definition.update({
            |   indexes: {
            |     byName: {
            |       terms: [{ field: "lastName" }]
            |     }
            |   }
            |})
            |""".stripMargin
      )

      (ctx ! Collection.getUncached(
        auth.scopeID,
        authorCID)).value.active.get.config.collIndexes
        .asInstanceOf[List[UserDefinedIndex]]
        .map(_.name) shouldBe List("byName")
      (ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        authorCID)).size shouldBe 1

      (ctx ! Collection.getUncached(
        auth.scopeID,
        personCID)).value.active.get.config.collIndexes
        .asInstanceOf[List[UserDefinedIndex]]
        .map(_.name) shouldBe List("byName")
      (ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        personCID)).size shouldBe 1

      val person = evalOk(
        auth,
        """|Person.create({
           |  name: "Jewel",
           |  occupation: "Doctor",
           |  years: 17
           |})""".stripMargin
      ).to[Value.Doc]

      val author = evalOk(
        auth,
        """|Author.create({
           |  lastName: "Cardamom",
           |  rating: 4,
           |  genre: "fantasy"
           |})""".stripMargin
      ).to[Value.Doc]

      validateIndex(
        (ctx ! Index.getUserDefinedBySourceUncached(auth.scopeID, personCID)).head,
        person.id,
        Vector("Jewel"),
        Vector(IndexTerm("Doctor"), IndexTerm(17, reverse = true))
      )

      validateIndex(
        (ctx ! Index.getUserDefinedBySourceUncached(auth.scopeID, authorCID)).head,
        author.id,
        Vector("Cardamom"),
        Vector.empty
      )
    }

    "indexes are deleted when removed from collections via update" in {
      val auth = newDB

      val collID = evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byName: {
            |      terms: [{ field: "name" }]
            |    },
            |    byAge: {
            |      terms: [{ field: "age" }]
            |    }
            |  }
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      (ctx ! Collection.getUncached(
        auth.scopeID,
        collID)).value.active.get.config.collIndexes
        .asInstanceOf[List[UserDefinedIndex]]
        .map(_.name) shouldBe List("byName", "byAge")
      (ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID)).size shouldBe 2

      evalOk(
        auth,
        """|Person.definition.update({
           |  indexes: {
           |    byName: null
           |  }
           |})""".stripMargin
      )

      (ctx ! Collection.getUncached(
        auth.scopeID,
        collID)).value.active.get.config.collIndexes
        .asInstanceOf[List[UserDefinedIndex]]
        .map(_.name) shouldBe List("byAge")
      (ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID)).size shouldBe 1

      evalOk(
        auth,
        """|Person.definition.update({
           |  indexes: {
           |    byAge: null
           |  }
           |})""".stripMargin
      )

      (ctx ! Collection.getUncached(
        auth.scopeID,
        collID)).value.active.get.config.collIndexes shouldBe List.empty
      (ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID)).size shouldBe 0
    }

    "indexes are recreated when updated from collections via update" in {
      val auth = newDB

      val collID = evalOk(
        auth,
        s"""|Collection.create({
              |  name: "Person",
              |  indexes: {
              |    byName: {
              |      terms: [{ field: "name" }]
              |    }
              |  }
              |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      val person = evalOk(
        auth,
        """|Person.create({
             |  name: "John",
             |  age: 50
             |})""".stripMargin
      ).to[Value.Doc].id

      (ctx ! Collection.getUncached(
        auth.scopeID,
        collID)).value.active.get.config.collIndexes
        .asInstanceOf[List[UserDefinedIndex]]
        .map(_.name) shouldBe List("byName")
      (ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID)).size shouldBe 1

      validateIndex(
        (ctx ! Index.getUserDefinedBySourceUncached(auth.scopeID, collID)).head,
        person,
        Vector("John"),
        Vector.empty
      )

      evalOk(
        auth,
        """|Person.definition.update({
           |  indexes: {
           |    byName: {
           |      terms: [{ field: "name" }],
           |      values: [{ field: "age" }, { field: "name" }]
           |    }
           |  }
           |})""".stripMargin
      )

      (ctx ! Collection.getUncached(
        auth.scopeID,
        collID)).value.active.get.config.collIndexes
        .asInstanceOf[List[UserDefinedIndex]]
        .map(_.name) shouldBe List("byName")
      (ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID)).size shouldBe 1

      validateIndex(
        (ctx ! Index.getUserDefinedBySourceUncached(auth.scopeID, collID)).head,
        person,
        Vector("John"),
        Vector(IndexTerm(50), IndexTerm("John"))
      )
    }

    "indexes are deleted when collection is deleted" in {
      val auth = newDB

      val collID = evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byName: {
            |      terms: [{ field: "name" }]
            |    },
            |    byAge: {
            |      terms: [{ field: "age" }]
            |    }
            |  }
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      (ctx ! Collection.getUncached(
        auth.scopeID,
        collID)).value.active.get.config.collIndexes
        .asInstanceOf[List[UserDefinedIndex]]
        .map(_.name) shouldBe List("byName", "byAge")
      (ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID)).size shouldBe 2

      evalOk(
        auth,
        """Person.definition.delete()"""
      )

      (ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID)).size shouldBe 0
    }

    "backing indexes are reused on create" in {
      val auth = newDB

      val collID = evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byNameSortedByName1: {
            |      terms: [{ field: "name" }],
            |      values: [{ field: "name" }]
            |    },
            |    byNameSortedByName2: {
            |      terms: [{ field: "name" }],
            |      values: [{ field: "name" }]
            |    }
            |  }
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      val indexes =
        (ctx ! Collection.getUncached(
          auth.scopeID,
          collID)).value.active.get.collIndexes
          .asInstanceOf[List[UserDefinedIndex]]

      indexes.map(_.name) shouldBe List("byNameSortedByName1", "byNameSortedByName2")
      indexes.map(_.indexID).toSet.size shouldBe 1

      (ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID)).size shouldBe 1
    }

    "backing indexes are reused on replace" in {
      val auth = newDB

      val collID = evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byNameSortedByName1: {
            |      terms: [{ field: "name" }],
            |      values: [{ field: "name" }]
            |    },
            |    byNameSortedByName2: {
            |      terms: [{ field: "name" }],
            |      values: [{ field: "name" }]
            |    }
            |  }
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      val indexes0 =
        (ctx ! Collection.getUncached(
          auth.scopeID,
          collID)).value.active.get.collIndexes
          .asInstanceOf[List[UserDefinedIndex]]
      indexes0.map(_.name) shouldBe List(
        "byNameSortedByName1",
        "byNameSortedByName2")
      indexes0.map(_.indexID).toSet.size shouldBe 1
      (ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID)).size shouldBe 1

      evalOk(
        auth,
        """|Person.definition.replace({
           |  name: "Person",
           |  indexes: {
           |    byNameSortedByName1: {
           |      terms: [{ field: "name" }],
           |      values: [{ field: "name" }]
           |    },
           |    byNameSortedByName2: {
           |      terms: [{ field: "name" }],
           |      values: [{ field: "name" }]
           |    }
           |  }
           |})""".stripMargin
      )

      val indexes1 =
        (ctx ! Collection.getUncached(
          auth.scopeID,
          collID)).value.active.get.collIndexes
          .asInstanceOf[List[UserDefinedIndex]]
      indexes1.map(_.name) shouldBe List(
        "byNameSortedByName1",
        "byNameSortedByName2")
      indexes1.map(_.indexID).toSet.size shouldBe 1
      (ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID)).size shouldBe 1

      indexes0 shouldBe indexes1
    }

    "backing indexes are re-used when mva is set" in {
      val auth = newDB
      val collID = evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Product",
            |  indexes: {
            |    byName: {
            |      terms: [{ field: "name"}],
            |      values: [{ field: "name"} ],
            |    }
            |  }
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]
      val indexes0 =
        (ctx ! Index.getUserDefinedBySourceUncached(auth.scopeID, collID))

      evalOk(
        auth,
        s"""|Product.definition.update({
            |  indexes: {
            |    byName: {
            |      terms: [{ field: ".name", mva: false }],
            |      values: [{ field: ".name", mva: false }]
            |    }
            |  }
            |})""".stripMargin
      )

      val indexes1 =
        (ctx ! Index.getUserDefinedBySourceUncached(auth.scopeID, collID))

      indexes0.size shouldEqual 1
      indexes1.size shouldEqual 1
      indexes0.head.id shouldEqual indexes1.head.id
    }

    "backing indexes are reused on delete" in {
      val auth = newDB

      val collID = evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byNameSortedByName1: {
            |      terms: [{ field: "name" }],
            |      values: [{ field: "name" }]
            |    },
            |    byNameSortedByName2: {
            |      terms: [{ field: "name" }],
            |      values: [{ field: "name" }]
            |    }
            |  }
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      val configs0 =
        (ctx ! Collection.getUncached(
          auth.scopeID,
          collID)).value.active.get.collIndexes
          .asInstanceOf[List[UserDefinedIndex]]
      configs0.map(_.name) shouldBe List(
        "byNameSortedByName1",
        "byNameSortedByName2")
      configs0.map(_.indexID).toSet.size shouldBe 1
      (ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID)).size shouldBe 1

      evalOk(
        auth,
        """|Person.definition.update({
           |  indexes: {
           |    byNameSortedByName2: null
           |  }
           |})""".stripMargin
      )

      val configs1 =
        (ctx ! Collection.getUncached(
          auth.scopeID,
          collID)).value.active.get.collIndexes
          .asInstanceOf[List[UserDefinedIndex]]
      configs1.map(_.name) shouldBe List("byNameSortedByName1")
      configs1.map(_.indexID).toSet.size shouldBe 1
      (ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID)).size shouldBe 1

      evalOk(
        auth,
        """|Person.definition.update({
           |  indexes: {
           |    byNameSortedByName1: null
           |  }
           |})""".stripMargin
      )

      val configs2 =
        (ctx ! Collection.getUncached(
          auth.scopeID,
          collID)).value.active.get.collIndexes
          .asInstanceOf[List[UserDefinedIndex]]
      configs2.size shouldBe 0
      (ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID)).size shouldBe 0
    }

    "terms and values show up in partials" in {
      val auth = newDB
      evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Product",
            |  indexes: {
            |    byName: {
            |      terms: [{ field: "name" }],
            |      values: [{ field: "price" }]
            |    }
            |  }
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      val part = evalOk(
        auth,
        s"""
           |Product.create({  name: "basketball", price: 100 })
           |Product.byName("basketball").map(.data).first()
           |""".stripMargin
      ).asInstanceOf[Value.Struct.Partial]
      project(part, "price").value shouldBe Value.Int(100)
      project(part, "name").value shouldBe Value.Str("basketball")
    }

    "backing indexes are reused when adding more indexes" in {
      val auth = newDB

      val collID = evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byNameSortedByName1: {
            |      terms: [{ field: "name" }],
            |      values: [{ field: "name" }]
            |    }
            |  }
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      val configs0 =
        (ctx ! Collection.getUncached(
          auth.scopeID,
          collID)).value.active.get.collIndexes
          .asInstanceOf[List[UserDefinedIndex]]
      configs0.map(_.name) shouldBe List("byNameSortedByName1")
      configs0.map(_.indexID).toSet.size shouldBe 1
      (ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID)).size shouldBe 1

      evalOk(
        auth,
        """|Person.definition.update({
           |  indexes: {
           |    byNameSortedByName2: {
           |      terms: [{ field: "name" }],
           |      values: [{ field: "name" }]
           |    }
           |  }
           |})""".stripMargin
      )

      val configs1 =
        (ctx ! Collection.getUncached(
          auth.scopeID,
          collID)).value.active.get.collIndexes
          .asInstanceOf[List[UserDefinedIndex]]

      configs1.map(_.name) shouldBe List(
        "byNameSortedByName1",
        "byNameSortedByName2")
      configs1.map(_.indexID).toSet.size shouldBe 1
      (ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID)).size shouldBe 1

      configs0.map(_.indexID).toSet shouldBe configs1.map(_.indexID).toSet
    }

    "backing indexes are not reused when updating an index" in {
      val auth = newDB

      val collID = evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byName: {
            |      terms: [{ field: "name" }],
            |      values: [{ field: "name" }]
            |    },
            |    byName2: {
            |      terms: [{ field: "name" }],
            |      values: [{ field: "name" }]
            |    },
            |    byAge: {
            |      terms: [{ field: "age" }],
            |      values: [{ field: "age" }]
                }
            |  }
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      val configs0 =
        (ctx ! Collection.getUncached(
          auth.scopeID,
          collID)).value.active.get.collIndexes
          .asInstanceOf[List[UserDefinedIndex]]
      configs0.map(_.name) shouldBe List("byName", "byName2", "byAge")
      configs0.map(_.indexID).toSet.size shouldBe 2
      (ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID)).size shouldBe 2

      evalOk(
        auth,
        """|Person.definition.update({
           |  indexes: {
           |    byName: {
           |      terms: [{ field: "name" }],
           |      values: [{ field: "name" }, { field: "age" }]
           |    }
           |  }
           |})""".stripMargin
      )

      val configs1 =
        (ctx ! Collection.getUncached(
          auth.scopeID,
          collID)).value.active.get.collIndexes
          .asInstanceOf[List[UserDefinedIndex]]

      configs1.map(_.name) shouldBe List("byName", "byName2", "byAge")
      configs1.map(_.indexID).toSet.size shouldBe 3
      (ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID)).size shouldBe 3

      configs0.find(_.name == "byName").get.indexID should not equal configs1
        .find(_.name == "byName")
        .get
        .indexID
    }

    "backing index can not be provided on create or update" in {
      val auth = newDB
      val err = evalErr(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  backingIndexes: {
            |    byNameSortedByName: { indexID: "iid" }
            |  },
            |  indexes: {
            |    byNameSortedByName: {
            |      terms: [{ field: "name" }],
            |      values: [{ field: "name" }]
            |    }
            |  }
            |})""".stripMargin,
        typecheck = false
      )
      err shouldBe a[QueryRuntimeFailure]
      err.code shouldBe "constraint_failure"
      evalOk(auth, "Collection.create({ name: 'Person' })")
      val errUpdate = evalErr(
        auth,
        s"""|
            |Person.definition.update({
            |  backingIndexes: {
            |    byNameSortedByName: { indexID: "iid" }
            |  },
            |  indexes: {
            |    byNameSortedByName: {
            |      terms: [{ field: "name" }],
            |      values: [{ field: "name" }]
            |    }
            |  }
            |})""".stripMargin,
        typecheck = false
      )
      errUpdate shouldBe a[QueryRuntimeFailure]
      errUpdate.code shouldBe "constraint_failure"
    }

    "backing indexes are reused when renaming an index" in {
      val auth = newDB

      val collID = evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byNameSortedByName: {
            |      terms: [{ field: "name" }],
            |      values: [{ field: "name" }]
            |    }
            |  }
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      val configs0 =
        (ctx ! Collection.getUncached(
          auth.scopeID,
          collID)).value.active.get.collIndexes
          .asInstanceOf[List[UserDefinedIndex]]
      configs0.map(_.name) shouldBe List("byNameSortedByName")
      configs0.map(_.indexID).toSet.size shouldBe 1
      (ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID)).size shouldBe 1

      evalOk(
        auth,
        """|Person.definition.update({
           |  indexes: {
           |    byNameSortedByName: null,
           |    byNameSortedByName2: {
           |      terms: [{ field: "name" }],
           |      values: [{ field: "name" }]
           |    }
           |  }
           |})""".stripMargin
      )

      val configs1 =
        (ctx ! Collection.getUncached(
          auth.scopeID,
          collID)).value.active.get.collIndexes
          .asInstanceOf[List[UserDefinedIndex]]

      configs1.map(_.name) shouldBe List("byNameSortedByName2")
      configs1.map(_.indexID).toSet.size shouldBe 1
      (ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID)).size shouldBe 1

      configs0.map(_.indexID).toSet shouldBe configs1.map(_.indexID).toSet
    }

    "status" - {
      "is complete after sync build" in {
        val auth = newDB

        mkColl(auth, "Person")

        (1 to Index.BuildSyncSize) foreach { i =>
          evalOk(
            auth,
            s"""Person.create({foo: $i})"""
          )
        }

        evalOk(
          auth,
          s"""|Person.definition.update({
              |  indexes: {
              |    byName: {
              |      terms: [{ field: "name" }]
              |    }
              |  }
              |})
              |""".stripMargin
        )

        evalOk(
          auth,
          "Person.definition.indexes.byName?.status"
        ).as[String] shouldBe CollectionIndex.Status.Complete.asStr
      }

      "is complete after async build" in {
        val auth = newDB

        mkColl(auth, "Person")

        (1 to Index.BuildSyncSize) foreach { i =>
          evalOk(
            auth,
            s"""|[
                |  Person.create({foo: ${i * 2 + 0}}),
                |  Person.create({foo: ${i * 2 + 1}})
                |]""".stripMargin
          )
        }

        evalOk(
          auth,
          s"""|Person.definition.update({
              |  indexes: {
              |    byName: {
              |      terms: [{ field: "name" }]
              |    }
              |  }
              |})
              |""".stripMargin
        )

        evalOk(
          auth,
          "Person.definition.indexes.byName?.status"
        ).as[String] shouldBe CollectionIndex.Status.Building.asStr

        // drain runQ
        val executor = TaskExecutor(ctx)
        (ctx ! executor.runQueue(ctx.service.localID.get).nonEmptyT) shouldBe true
        while (ctx ! executor.runQueue(ctx.service.localID.get).nonEmptyT)
          executor.step()

        evalOk(
          auth,
          "Person.definition.indexes.byName?.status"
        ).as[String] shouldBe CollectionIndex.Status.Complete.asStr
      }
      "correctly reuses an index after an async build" in {
        val auth = newDB

        mkColl(auth, "Person")

        (1 to Index.BuildSyncSize) foreach { i =>
          evalOk(
            auth,
            s"""|[
                |  Person.create({foo: ${i * 2 + 0}}),
                |  Person.create({foo: ${i * 2 + 1}})
                |]""".stripMargin
          )
        }

        evalOk(
          auth,
          s"""|Person.definition.update({
              |  indexes: {
              |    byName: {
              |      terms: [{ field: "name" }]
              |    }
              |  }
              |})
              |""".stripMargin
        )

        evalOk(
          auth,
          "Person.definition.indexes.byName?.status"
        ).as[String] shouldBe CollectionIndex.Status.Building.asStr

        // drain runQ
        val executor = TaskExecutor(ctx)
        while (ctx ! executor.runQueue(ctx.service.localID.get).nonEmptyT)
          executor.step()

        evalOk(
          auth,
          "Person.definition.indexes.byName?.status"
        ).as[String] shouldBe CollectionIndex.Status.Complete.asStr

        evalOk(
          auth,
          s"""|Person.definition.update({
              |  indexes: {
              |    byNameTwo: {
              |      terms: [{ field: "name" }]
              |    }
              |  }
              |})
              |""".stripMargin
        )

        evalOk(
          auth,
          "Person.definition.indexes.byNameTwo?.status"
        ).as[String] shouldBe CollectionIndex.Status.Complete.asStr

        evalOk(
          auth,
          "Person.definition.indexes.byNameTwo?.queryable"
        ).as[Boolean] shouldBe true
      }

      "is failed if task is canceled" in {
        val auth = newDB

        mkColl(auth, "Person")

        (1 to Index.BuildSyncSize) foreach { i =>
          evalOk(
            auth,
            s"""|[
                |  Person.create({foo: ${i * 2 + 0}}),
                |  Person.create({foo: ${i * 2 + 1}})
                |]""".stripMargin
          )
        }

        evalOk(
          auth,
          s"""|Person.definition.update({
              |  indexes: {
              |    byName: {
              |      terms: [{ field: "name" }]
              |    }
              |  }
              |})
              |""".stripMargin
        )

        evalOk(
          auth,
          "Person.definition.indexes.byName?.status"
        ).as[String] shouldBe CollectionIndex.Status.Building.asStr

        val tasks = ctx ! Task.getAllRunnable().flattenT
        tasks.size shouldBe 1

        ctx ! TaskRouter.cancel(tasks.head, None)

        evalOk(
          auth,
          "Person.definition.indexes.byName?.status"
        ).as[String] shouldBe CollectionIndex.Status.Failed.asStr
      }

      "cannot be updated" in {
        val auth = newDB

        mkColl(auth, "Person")

        def assertErr(err: QueryFailure): Unit = {
          err shouldBe a[QueryRuntimeFailure]
          err.code shouldBe "constraint_failure"
          err.failureMessage shouldBe "Failed to update Collection `Person`."
          err.asInstanceOf[QueryRuntimeFailure].message shouldBe
            s"""|Failed to update Collection `Person`.
                |constraint failures:
                |  indexes.byName.status: Failed to update field because it is readonly""".stripMargin
        }

        // cannot add index object with status
        assertErr(
          evalErr(
            auth,
            s"""|Person.definition.update({
                |  indexes: {
                |    byName: {
                |      status: 'Building',
                |      terms: [{ field: "name" }]
                |    }
                |  }
                |})
                |""".stripMargin
          )
        )

        // add index object
        evalOk(
          auth,
          """|Person.definition.update({
             |  indexes: {
             |    byName: {
             |      terms: [{ field: "name" }]
             |    }
             |  }
             |})
             |""".stripMargin
        )

        // cannot update status on existing index object
        assertErr(
          evalErr(
            auth,
            """|Person.definition.update({
               |  indexes: {
               |    byName: {
               |      status: 'Building'
               |    }
               |  }
               |})""".stripMargin
          )
        )
      }
    }

    "internal and read-only fields (backing index and status)" - {
      "are maintained for unchanged index definitions in replace call" in {
        val auth = newDB

        val collName = "Albums"
        evalOk(
          auth,
          s"""|Collection.create({
             |  name: "$collName",
             |  indexes: {
             |    byArtist: {
             |      terms: [{ field: "Artist" }]
             |    }
             |  }
             |})""".stripMargin
        )
        val collID = (ctx ! Collection.idByNameActive(auth.scopeID, collName)).value
        val coll =
          (ctx ! Collection.getUncached(auth.scopeID, collID)).get.active.get

        evalOk(
          auth,
          s"""|$collName.definition.replace({
              |  name: "$collName",
              |  history_days: 76,
              |  indexes: {
              |    byArtist: {
              |      terms: [{ field: "Artist" }]
              |    }
              |  }
              |})""".stripMargin
        )
        val replacedColl =
          (ctx ! Collection.getUncached(auth.scopeID, collID)).get.active.get

        replacedColl.config.historyDuration shouldEqual 76.days
        replacedColl.config.collIndexes
          .asInstanceOf[List[UserDefinedIndex]]
          .find(_.name == "byArtist") shouldEqual coll.config.collIndexes
          .asInstanceOf[List[UserDefinedIndex]]
          .find(_.name == "byArtist")
      }
      "are maintained for index definitions that only change the queryable field" in {
        val auth = newDB

        val collName = "Albums"
        evalOk(
          auth,
          s"""|Collection.create({
              |  name: "$collName",
              |  indexes: {
              |    byArtist: {
              |      terms: [{ field: "Artist" }]
              |    }
              |  }
              |})""".stripMargin
        )
        val collID = (ctx ! Collection.idByNameActive(auth.scopeID, collName)).value
        val coll =
          (ctx ! Collection.getUncached(auth.scopeID, collID)).get.active.get

        evalOk(
          auth,
          s"""|$collName.definition.replace({
              |  name: "$collName",
              |  history_days: 76,
              |  indexes: {
              |    byArtist: {
              |      terms: [{ field: "Artist" }],
              |      queryable: false
              |    }
              |  }
              |})""".stripMargin
        )
        val replacedColl =
          (ctx ! Collection.getUncached(auth.scopeID, collID)).get.active.get

        replacedColl.config.historyDuration shouldEqual 76.days
        replacedColl.collIndexes
          .asInstanceOf[List[UserDefinedIndex]]
          .find(_.name == "byArtist")
          .get
          .queryable shouldEqual false
        replacedColl.collIndexes
          .asInstanceOf[List[UserDefinedIndex]]
          .find(_.name == "byArtist")
          .get
          .copy(
            queryable = coll.collIndexes
              .asInstanceOf[List[UserDefinedIndex]]
              .find(_.name == "byArtist")
              .get
              .queryable
          ) shouldEqual coll.collIndexes
          .asInstanceOf[List[UserDefinedIndex]]
          .find(_.name == "byArtist")
          .get
      }
    }

    "queryable" - {
      "is true after sync build" in {
        val auth = newDB

        mkColl(auth, "Person")

        evalOk(
          auth,
          s"""|Person.definition.update({
              |  indexes: {
              |    byName: {
              |      terms: [{ field: "name" }]
              |    }
              |  }
              |})
              |""".stripMargin
        )

        evalOk(
          auth,
          "Person.definition.indexes.byName?.queryable"
        ).as[Boolean] shouldBe true
      }

      "is true after async build" in {
        val auth = newDB

        mkColl(auth, "Person")

        (1 to Index.BuildSyncSize) foreach { i =>
          evalOk(
            auth,
            s"""|[
                |  Person.create({foo: ${i * 2 + 0}}),
                |  Person.create({foo: ${i * 2 + 1}})
                |]""".stripMargin
          )
        }

        evalOk(
          auth,
          s"""|Person.definition.update({
              |  indexes: {
              |    byName: {
              |      terms: [{ field: "name" }]
              |    }
              |  }
              |})
              |""".stripMargin
        )

        evalOk(
          auth,
          "Person.definition.indexes.byName?.queryable"
        ).as[Boolean] shouldBe false

        // drain runQ
        val executor = TaskExecutor(ctx)
        while (ctx ! executor.runQueue(ctx.service.localID.get).nonEmptyT)
          executor.step()

        evalOk(
          auth,
          "Person.definition.indexes.byName?.queryable"
        ).as[Boolean] shouldBe true
      }

      "is false if task is canceled" in {
        val auth = newDB

        mkColl(auth, "Person")

        (1 to Index.BuildSyncSize) foreach { i =>
          evalOk(
            auth,
            s"""|[
                |  Person.create({foo: ${i * 2 + 0}}),
                |  Person.create({foo: ${i * 2 + 1}})
                |]""".stripMargin
          )
        }

        evalOk(
          auth,
          s"""|Person.definition.update({
              |  indexes: {
              |    byName: {
              |      terms: [{ field: "name" }]
              |    }
              |  }
              |})
              |""".stripMargin
        )

        evalOk(
          auth,
          "Person.definition.indexes.byName?.queryable"
        ).as[Boolean] shouldBe false

        val tasks = ctx ! Task.getAllRunnable().flattenT
        tasks.size shouldBe 1

        ctx ! TaskRouter.cancel(tasks.head, None)

        evalOk(
          auth,
          "Person.definition.indexes.byName?.queryable"
        ).as[Boolean] shouldBe false
      }

      "can be updated" in {
        val auth = newDB

        val collID = mkColl(auth, "Person")

        val id = evalOk(
          auth,
          """Person.create({name: "foo"}).id"""
        ).as[Long]

        evalOk(
          auth,
          """|Person.definition.update({
             |  indexes: {
             |    byName: {
             |      terms: [{ field: "name" }]
             |    }
             |  }
             |})
             |""".stripMargin
        )

        (ctx ! Collection.getUncached(
          auth.scopeID,
          collID)).value.active.get.collIndexes
          .asInstanceOf[List[UserDefinedIndex]]
          .map(_.name) shouldBe List("byName")
        (ctx ! Index.getUserDefinedBySourceUncached(
          auth.scopeID,
          collID)).size shouldBe 1
        (ctx ! Index.getUserDefinedBySourceUncached(
          auth.scopeID,
          collID
        )).head.isActive shouldBe true

        evalOk(
          auth,
          """Person.byName("foo").first()!.id"""
        ).as[Long] shouldBe id

        evalOk(
          auth,
          """|Person.definition.update({
             |  indexes: {
             |    byName: { queryable: false }
             |  }
             |})""".stripMargin
        )

        eventually { // wait for cache invalidation.
          val err = evalErr(auth, """Person.byName("foo")""")
          err shouldBe a[QueryRuntimeFailure]
          err.code shouldBe "invalid_index_invocation"
          err.asInstanceOf[QueryRuntimeFailure].message shouldBe
            "The index `Person.byName` is not queryable."
        }

        evalOk(
          auth,
          """|Person.definition.update({
             |  indexes: {
             |    byName: { queryable: true }
             |  }
             |})""".stripMargin
        )

        (ctx ! Index.getUserDefinedBySourceUncached(
          auth.scopeID,
          collID)).head.isActive shouldBe true

        evalOk(
          auth,
          """Person.byName("foo").first()!.id"""
        ).as[Long] shouldBe id
      }
    }

    "swap" - {
      "updates collections" in {
        val auth = newDB

        val collID = evalOk(
          auth,
          s"""|Collection.create({
              |  name: "Person",
              |  indexes: {
              |    byName: {
              |      terms: [{ field: "name" }]
              |    }
              |  }
              |})""".stripMargin
        ).to[Value.Doc].id.as[CollectionID]

        val id = evalOk(
          auth,
          """Person.create({name: "alice"}).id"""
        ).as[Long]

        evalOk(
          auth,
          """Person.byName("alice").first()!.id"""
        ).as[Long] shouldBe id

        val before = ctx ! Collection.get(auth.scopeID, collID).map { _.get }
        before.collIndexes.size shouldBe (1)

        val indexes =
          ctx ! Index.getUserDefinedBySourceUncached(auth.scopeID, collID)
        indexes.size shouldBe (1)

        before.collIndexes map { _.indexID } should equal(indexes map { _.id })

        ctx ! IndexSwap.Root.create(auth.scopeID, indexes.head.id)

        val executor = TaskExecutor(ctx)

        while (ctx ! executor.runQueue(ctx.service.localID.get).nonEmptyT) {
          executor.step()
        }

        val after = ctx ! Collection.get(auth.scopeID, collID).map { _.get }
        after.collIndexes.size shouldBe (1)

        after.collIndexes map { _.indexID } shouldNot equal(indexes map { _.id })

        evalOk(
          auth,
          """Person.byName("alice").first()!.id"""
        ).as[Long] shouldBe id
      }
    }

    "unique constraints" - {
      for (build <- Seq("sync", "async")) {
        s"apply even when $build built after-the-fact" in {
          val auth = newDB

          mkColl(auth, "Person")

          // The async test runs ~3x faster if we do this rather than run a
          // query per document.
          val count = if (build == "async") {
            Index.BuildSyncSize + 1
          } else {
            4
          }
          val query =
            (1 to count).map(_ => "Person.create({name: 'Bob'})").mkString(";")
          evalOk(auth, query)

          evalOk(
            auth,
            s"""|Person.definition.update({
                |  constraints: [
                |    { unique: [ { field: "name" } ] }
                |  ]
                |})
                |""".stripMargin
          )

          if (build == "async") {
            // If async, run the build task.
            val executor = TaskExecutor(ctx)
            while (ctx ! executor.runQueue(ctx.service.localID.get).nonEmptyT)
              executor.step()
          }

          // Constraint applies despite duplicates already existing.
          evalErr(
            auth,
            "Person.create({ name: 'Bob' })").code shouldBe "constraint_failure"
        }
      }
    }

    "computed fields" - {

      // Common setup for several tests.
      // Returns the collection id and indexer.
      def setup(auth: Auth, ret: String) = {
        evalOk(
          auth,
          s"""|Collection.create({
              |  name: "Foo",
              |  computed_fields: {
              |    a: {
              |      body: "doc => if (doc.x == 0) $ret else 0"
              |    }
              |  },
              |  indexes: {
              |    byA: {
              |      terms: [{ field: ".a" }],
              |    }
              |  }
              |})""".stripMargin
        )

        val col = ctx ! (Collection.idByNameActive(auth.scopeID, "Foo") flatMap {
          case None =>
            fail("no collection?")
          case Some(id) =>
            Collection.get(auth.scopeID, id)
        } map {
          case None =>
            fail("collection ID but no collection?")
          case Some(col) =>
            col
        })
        val cfg =
          ctx ! (Index.getConfig(auth.scopeID, col.collIndexes.head.indexID) map {
            case None =>
              fail("no index?")
            case Some(cfg) =>
              cfg
          })
        (col.id, FieldIndexer(cfg))
      }

      Seq(
        "produces a non-persistable value" -> "(_ => 1)",
        "explodes" -> "1/0",
        "isn't pure" -> "Foo.byId(doc.id)!.y"
      ) foreach { case (suffix, ret) =>
        s"emit no index rows if the computation $suffix" in {
          val auth = newDB(typeChecked = false)
          val (colID, indexer) = setup(auth, ret)

          def doc(x: Int) =
            Version.Live(
              auth.scopeID,
              DocID(SubID(1), colID),
              SchemaVersion.Min,
              Data(FieldPair(List("data", "x"), x)))

          // Indexer rows are returned...
          (ctx ! indexer.rows(doc(1))).isEmpty shouldBe false

          // ...but not for bad computations.
          (ctx ! indexer.rows(doc(0))).isEmpty shouldBe true
        }
      }
    }

    def validateIndex(
      index: IndexConfig,
      docID: DocID,
      terms: Vector[Term],
      values: Vector[IndexTerm]): Unit = {
      val returnedValues = ctx ! Store.collection(index, terms, Clock.time).flattenT
      returnedValues.size shouldEqual 1
      returnedValues.head.docID shouldEqual docID
      returnedValues.head.tuple.values shouldEqual values
    }

    def project(partial: Value.Struct.Partial, field: String) =
      partial.fragment
        .project(field :: Nil)
        .map { _.asInstanceOf[ReadCache.Fragment.Value].unwrap }
  }

  // v10 version of "detects contention" from documents/IndexSpec.
  //
  // This ensures that all v10 indexes are serialized.
  "v10 indexes are serialized" in {
    val auth = newDB

    evalOk(
      auth,
      """|Collection.create({
         |  name: "Visitor",
         |  indexes: {
         |    byName: {
         |      terms: [{ field: "a" }],
         |      values: [{ field: "b" }]
         |    }
         |  }
         |})""".stripMargin
    )

    evalOk(auth, "Visitor.create({ a: 0, b: 0 })")

    val futs = 1 to 20 map { _ =>
      val query = """|let i = Visitor.byName(0).last()!.b
                     |Visitor.create({ a: 0, b: i + 1 })
                     |""".stripMargin

      val expr = parseOk(query)

      val intp = new FQLInterpreter(auth, Effect.Limit(Effect.Write, "model tests"))
      val q = intp
        .evalWithTypecheck(expr, Map.empty, TypeMode.InferType)
        .flatMap { res =>
          intp.runPostEvalHooks().flatMap { postEvalRes =>
            intp.infoWarns.map { infoWarns =>
              postEvalRes match {
                case Result.Ok(_)        => (res, infoWarns)
                case err @ Result.Err(_) => (err, infoWarns)
              }
            }
          }
        }

      ctx.withRetryOnContention(maxAttempts = 20).runNow(q)
    }

    Await.result(futs.join, 30.seconds)

    evalOk(auth, "Visitor.byName(0).last()!.b") shouldBe Value.Int(20)
  }

  "index lookup handles NaN correctly" in {
    val auth = newDB

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  index byX {
           |    terms [.x]
           |    values [.y]
           |  }
           |}""".stripMargin
    )

    evalOk(auth, "Foo.create({ id: 0, x: 1, y: Math.NaN })")
    evalOk(auth, "Foo.byX(1).count()") shouldBe Value.Int(1)
    evalOk(auth, "Foo.byId(0)!.delete()")

    // Previously, we didn't consider the add and remove tuples
    // equivalent because NaN != NaN, so the snapshot would
    // include the deleted document.
    evalOk(auth, "Foo.byX(1).count()") shouldBe Value.Int(0)
  }
}
