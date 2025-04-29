package fauna.api.test

import fauna.codex.json.{ JSArray, JSNull, JSObject }
import fauna.exec.FaunaExecutionContext
import fauna.lang.syntax._
import fauna.model.runtime.fql2.ValueSet
import fauna.net.http.HttpResponse
import fauna.prop.Prop
import fauna.stats.QueryMetrics
import org.scalactic.source.Position
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

class FQL2MetricsSpec extends FQL2APISpec {
  import FaunaExecutionContext.Implicits.global

  private def assertPure(
    query: Future[HttpResponse])(implicit pos: Position): Unit = {
    query should respond(OK)
    (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
    (query.json / "stats" / "read_ops").as[Long] shouldEqual 0
    (query.json / "stats" / "write_ops").as[Long] shouldEqual 0
    (query.json / "stats" / "storage_bytes_read").as[Long] shouldEqual 0
    (query.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
  }

  private def assertWriteOps(query: Future[HttpResponse])(
    implicit pos: Position): Unit = {
    val bytes = (query.json / "stats" / "storage_bytes_write").as[Long]
    bytes shouldNot equal(0)

    val ops = Math.ceil(bytes / QueryMetrics.BytesPerWriteOp.toDouble).toInt
    (query.json / "stats" / "write_ops").as[Long] shouldEqual ops
  }

  private def assertReadOps(partitions: Long, query: Future[HttpResponse])(
    implicit pos: Position): Unit = {
    val bytes = (query.json / "stats" / "storage_bytes_read").as[Long]
    bytes shouldNot be(0)

    val byteOps = Math.ceil(bytes / QueryMetrics.BytesPerReadOp.toDouble).toInt
    val readOps = (query.json / "stats" / "read_ops").as[Long]
    readOps should be(byteOps + partitions - 1)
  }

  "literals" - {
    once("number") {
      for {
        db  <- aDatabase
        num <- jsLong
      } {
        assertPure(queryRaw(num, db))
      }
    }

    once("object") {
      for {
        db  <- aDatabase
        obj <- jsObject
      } {
        assertPure(queryRaw(obj, db))
      }
    }
  }

  "expressions" - {
    once("do") {
      for {
        db <- aDatabase
      } {
        assertPure(
          queryRaw(
            """|null
               |null
               |null
               |""".stripMargin,
            db))
      }
    }

    once("at") {
      for {
        db <- aDatabase
      } {
        val query = queryRaw("at (Time.now()) {}", db)
        assertPure(query)
      }
    }

    once("let") {
      for {
        db <- aDatabase
      } {
        assertPure(
          queryRaw(
            s"""|let x = 1
                |null
                |""".stripMargin,
            db))
      }
    }

    once("if/else") {
      for {
        db   <- aDatabase
        bool <- Prop.boolean
      } {
        assertPure(
          queryRaw(
            s"""|if ($bool) {
                |  null
                |} else {
                |  null
                |}
                |""".stripMargin,
            db
          ))
      }
    }
  }

  "projections" - {
    once("object key") {
      for {
        db <- aDatabase
      } {
        assertPure(queryRaw("{ foo: 42 }.foo", db))
      }
    }

    once("array index") {
      for {
        db <- aDatabase
      } {
        assertPure(queryRaw("[0, 1, 2][0]", db))
      }
    }
  }

  "operators" - {
    once("booleans") {
      for {
        db <- aDatabase
      } {
        assertPure(queryRaw("true && true && false", db))
        assertPure(queryRaw("false || false || true", db))
        assertPure(queryRaw("!true", db))
        assertPure(queryRaw("!false", db))
      }
    }

    once("numerics") {
      for {
        db <- aDatabase
      } {
        assertPure(queryRaw("2 + 2", db))
        assertPure(queryRaw("2 - 2", db))
        assertPure(queryRaw("2 * 2", db))
        assertPure(queryRaw("2 / 2", db))
      }
    }

    once("universal") {
      for {
        db <- aDatabase
      } {
        assertPure(queryRaw("1 == 2", db))
        assertPure(queryRaw("1 != 2", db))
        assertPure(queryRaw("1 > 2", db))
        assertPure(queryRaw("1 >= 2", db))
        assertPure(queryRaw("1 < 2", db))
        assertPure(queryRaw("1 <= 2", db))
      }
    }
  }

  "write ops" - {

    ignore("TODO: create database") {
      // TODO: implement me
    }

    once("create collection") {
      for {
        db   <- aDatabase
        name <- aUniqueIdentifier
        query = queryRaw(s"Collection.create({ name: '$name' })", db)
      } {
        (query.json / "data" / "name").as[String] shouldBe name
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "read_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldNot equal(0)
        assertWriteOps(query)
      }
    }

    once("create collection with indexes") {
      for {
        db   <- aDatabase
        coll <- anIdentifier
        idx  <- anIdentifier
        query = queryRaw(
          s"""|Collection.create({
              |  name: "$coll",
              |  indexes: {
              |    $idx: {
              |      values: [{field: "foo"}]
              |    }
              |  }
              |})
              |""".stripMargin,
          db
        )
      } {
        (query.json / "data" / "indexes" / idx / "values" / 0 / "field")
          .as[String] shouldBe "foo"
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "read_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldNot equal(0)
        assertWriteOps(query)
      }
    }

    once("create a new index") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        idx  <- aUniqueIdentifier
        query = queryRaw(
          s"""|$coll.definition.update({
              |  indexes: {
              |    $idx: {
              |      values: [{field: "foo"}]
              |    }
              |  }
              |})
              |""".stripMargin,
          db
        )
      } {
        query should respond(OK)
        (query.json / "data" / "indexes" / idx / "values" / 0 / "field")
          .as[String] shouldBe "foo"
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "read_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldNot equal(0)
        // coll. update + index create + fsl update
        (query.json / "stats" / "write_ops").as[Long] shouldEqual 3
        (query.json / "stats" / "storage_bytes_write").as[Long] shouldNot equal(0)
      }
    }

    once("create index charges compute ops for synchronous build") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        numDocs = 100
        _ <- aDocument(db, coll) * numDocs
      } {
        val query = queryRaw(
          s"""|$coll.definition.update({
              |  name: "$coll",
              |  indexes: {
              |    v10: {
              |      terms: [{ field: "x" }, { field: "y" }],
              |      values: [{ field: "z" }]
              |    }
              |  }
              |})
              |""".stripMargin,
          db
        )

        // 1 op for creation plus 1 op per term or value for
        // every QueryMetrics.BaselineCompute docs.
        val expCompute = 1 + (2 + 1) * numDocs / QueryMetrics.BaselineCompute

        query should respond(OK)
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual expCompute
        (query.json / "stats" / "read_ops").as[Long] shouldBe 1
        // coll. update + index create + fsl update
        (query.json / "stats" / "write_ops").as[Long] shouldEqual 3
        (query.json / "stats" / "storage_bytes_write").as[Long] shouldNot equal(0)
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldNot equal(0)
      }
    }

    once("create function") {
      for {
        db <- aDatabase
        fn <- aUniqueIdentifier
        query = queryRaw(
          s"""|Function.create({
              |  name: "$fn",
              |  body: "_ => true"
              |})
              |""".stripMargin,
          db
        )
      } {
        query should respond(OK)
        (query.json / "data" / "name").as[String] shouldBe fn
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "read_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldNot equal(0)
        assertWriteOps(query)
      }
    }

    once("create key") {
      for {
        db <- aDatabase
        query = queryRaw("Key.create({ role: 'admin' })", db)
      } {
        query should respond(OK)
        (query.json / "data" / "role").as[String] shouldBe "admin"
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "read_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldEqual 0
        assertWriteOps(query)
      }
    }

    once("create role") {
      for {
        db   <- aDatabase
        role <- aUniqueIdentifier
        query = queryRaw(
          s"""|Role.create({
              |  name: "$role",
              |  privileges: [{
              |    resource: "Collection",
              |    actions: { create: true }
              |  }]
              |})
              |""".stripMargin,
          db
        )
      } {
        query should respond(OK)
        (query.json / "data" / "name").as[String] shouldBe role
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "read_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldNot equal(0)
        assertWriteOps(query)
      }
    }

    once("create document") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        query = queryRaw(s"$coll.create({ a: 'b' })", db)
      } {
        query should respond(OK)
        (query.json / "data" / "a").as[String] shouldBe "b"
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "read_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldEqual 0
        assertWriteOps(query)
      }
    }

    once("create document with indexes") {
      for {
        db   <- aDatabase
        coll <- aCollection(db, anIndex() * 10)
        query = queryRaw(s"$coll.create({ a: 'b' })", db)
      } {
        query should respond(OK)
        (query.json / "data" / "a").as[String] shouldBe "b"
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "read_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldEqual 0
        assertWriteOps(query)
      }
    }

    ignore("TODO: login") {
      // TODO: implement me
    }

    once("update document") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc  <- aDocument(db, coll)
        query = queryRaw(s"$coll.byId('$doc')!.update({ a: 'c' })", db)
      } {
        query should respond(OK)
        (query.json / "data" / "a").as[String] shouldBe "c"
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "read_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldNot equal(0)
        assertWriteOps(query)
      }
    }

    once("update document multiple times") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc  <- aDocument(db, coll)
        query = queryRaw(
          s"""|let doc = $coll.byId('$doc')!
              |doc.update({ i: 1 })
              |Set.sequence(1, 100).forEach(_ => {
              |  doc.update({ i: doc.i + 1 })
              |})
              |doc.i
              |""".stripMargin,
          db
        )
      } {
        query should respond(OK)
        (query.json / "data").as[Int] shouldBe 100
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 7
        (query.json / "stats" / "read_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldNot equal(0)
        assertReadOps(partitions = 1, query)
        assertWriteOps(query)
      }
    }

    once("update document with indexes") {
      for {
        db   <- aDatabase
        coll <- aCollection(db, anIndex() * 10)
        doc  <- aDocument(db, coll)
        query = queryRaw(s"$coll.byId('$doc')!.update({ a: 'b' })", db)
      } {
        query should respond(OK)
        (query.json / "data" / "a").as[String] shouldBe "b"
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "read_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldNot equal(0)
        assertWriteOps(query)
      }
    }

    once("delete document") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc  <- aDocument(db, coll)
        query = queryRaw(s"$coll.byId('$doc')!.delete()", db)
      } {
        query should respond(OK)
        (query.json / "data") shouldBe JSNull
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "read_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldNot equal(0)
        assertWriteOps(query)
      }
    }

    once("replace document") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc  <- aDocument(db, coll)
        query = queryRaw(s"$coll.byId('$doc')!.replace({ a: 'c' })", db)
      } {
        query should respond(OK)
        (query.json / "data" / "a").as[String] shouldBe "c"
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "read_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldNot equal(0)
        assertWriteOps(query)
      }
    }

    ignore("TODO: insert event") {
      // TODO: implement me
    }

    ignore("TODO: remove event") {
      // TODO: implement me
    }

    once("invalidated read") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        // prefer a small object for predictable X-Byte-Write-Ops
        docID <- aDocument(db, coll, jsObject(maxSize = 5, maxDepth = 1))
        query = queryRaw(
          s"""|let doc = $coll.byId("$docID")!
              |doc.replace({a: "b"})
              |$coll.byId("$docID")! // replace invalidates this read
              |$coll.create({})
              |""".stripMargin,
          db
        )
      } {
        query should respond(OK)
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "read_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldNot equal(0)
        (query.json / "stats" / "storage_bytes_write").as[Long] shouldNot equal(0)
        // replace + create
        (query.json / "stats" / "write_ops").as[Long] shouldEqual 2
      }
    }

    once("contended write") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc  <- aDocument(db, coll)
        query = s"$coll.byId('$doc')!.update({a: 'c'})"
      } {
        val baseline = queryRaw(query, db)
        baseline.statusCode shouldBe OK

        val reads = (baseline.json / "stats" / "read_ops").as[Long]
        val writes = (baseline.json / "stats" / "write_ops").as[Long]

        val futs =
          for (_ <- 1 to 10) yield {
            Future {
              val res = queryRaw(query, db)
              res.statusCode should (be(OK) or be(Conflict))
              if (res.statusCode == OK) {
                val tries = (res.json / "stats" / "contention_retries").as[Long] + 1
                (res.json / "data" / "a").as[String] shouldBe "c"
                (res.json / "stats" / "compute_ops").as[Long] shouldBe 1
                (res.json / "stats" / "read_ops").as[Long] shouldBe reads * tries
                (res.json / "stats" / "write_ops").as[Long] shouldBe writes * tries
              }
            }
          }

        Await.result(futs.join, 1.minute)
      }
    }
  }

  "read ops" - {
    once("get document") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc  <- aDocument(db, coll)
        query = queryRaw(s"$coll.byId('$doc')!", db)
      } {
        query should respond(OK)
        (query.json / "data" / "id").as[String] shouldBe doc
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
        assertReadOps(partitions = 1, query) // default # or partitions in v10
      }
    }

    once("get document not found") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        query = queryRaw(s"$coll.byId('0')?.foo ?? 'not found'", db)
      } {
        query should respond(OK)
        (query.json / "data").as[String] shouldBe "not found"
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "read_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldEqual 14
        (query.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
      }
    }

    once("exists incurs one read op") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        query = queryRaw(s"""$coll.byId("1234").exists()""", db)
      } {
        query should respond(OK)
        (query.json / "data").as[Boolean] shouldBe false
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "read_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
      }
    }

    once("get same document many times") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc  <- aDocument(db, coll)
        query = queryRaw(
          s"""|${coll}.byId("$doc")!
              |""".stripMargin * 100,
          db
        )
      } {
        query should respond(OK)
        (query.json / "data" / "id").as[String] shouldBe doc
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 2
        (query.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
        assertReadOps(partitions = 1, query) // default # or partitions in v10
      }
    }

    once("get docs in parallel") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        docs <- aDocument(db, coll) * 100
      } {
        val ops =
          docs.foldLeft(0) { (acc, doc) =>
            val q = queryRaw(s"$coll.byId('$doc')!", db)
            acc + (q.json / "stats" / "read_ops").as[Int]
          }

        val query =
          queryRaw(
            docs.view
              .map { doc => s"$coll.byId('$doc')!" }
              .mkString("[", ",", "]"),
            db
          )

        query should respond(OK)
        (query.json / "data").as[JSArray].length shouldBe 100
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 2
        (query.json / "stats" / "read_ops").as[Long] shouldEqual ops
        (query.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldNot equal(0)
        (query.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
      }
    }

    once("get first element of a set") {
      for {
        db <- aDatabase
        coll <- aCollection(
          db,
          anIndex(
            name = Prop.const("byChar"),
            terms = Prop.const(Seq("char"))
          ))
        _ <- aDocument(db, coll, data = Prop.const("{ char: 'a' }"))
        query = queryRaw(s"$coll.byChar('a').first()", db)
      } {
        query should respond(OK)
        (query.json / "data" / "char").as[String] shouldEqual "a"
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
        (query.json / "stats" / "read_ops").as[Long] shouldBe 2
      }
    }

    once("get first element of an empty set") {
      for {
        db <- aDatabase
        coll <- aCollection(
          db,
          anIndex(
            name = Prop.const("byChar"),
            terms = Prop.const(Seq("char"))
          ))
        _ <- aDocument(db, coll, data = Prop.const("{ char: 'a' }"))
        query = queryRaw(s"$coll.byChar('b').first()", db)
      } {
        query should respond(OK)
        (query.json / "data") shouldBe a[JSNull]
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "read_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
      }
    }

    // TODO: implement get database
    once("get schema document") {
      for {
        db  <- aDatabase
        idx <- aUniqueIdentifier
        coll <- aCollection(
          db,
          anIndex(name = Prop.const(idx), terms = Prop.const(Seq("foo"))))
      } {
        val collF = queryRaw(s"Collection.byName('$coll')", db)
        collF should respond(OK)
        (collF.json / "data" / "name").as[String] shouldBe coll
        (collF.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (collF.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (collF.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
        assertReadOps(partitions = 1, collF) // default # or partitions in v10

        val idxF = queryRaw(s"$coll.definition.indexes.$idx!", db)
        idxF should respond(OK)
        (idxF.json / "data" / "terms" / 0 / "field").as[String] shouldBe "foo"
        (idxF.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (idxF.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (idxF.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
        assertReadOps(partitions = 1, idxF) // default # or partitions in v10
      }
    }

    once("paginate") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc  <- aDocument(db, coll)
        // Project the doc's id so that data projection doesn't cost more read ops.
        query = queryRaw(s"$coll.all().map(.id).paginate()", db)
      } {
        query should respond(OK)
        (query.json / "data" / "data" / 0).as[String] shouldBe doc
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
        assertReadOps(partitions = 8, query) // documents index has 8 partitions
      }
    }

    once("paginate/union") {
      for {
        db   <- aDatabase
        idx  <- anIdentifier
        coll <- aCollection(db, anIndex(name = Prop.const(idx)))
        doc  <- aDocument(db, coll)
      } {
        // Project the doc's id so that data projection doesn't cost more read ops.
        val query1 =
          queryRaw(s"$coll.all().map(.id).concat([].toSet()).paginate()", db)
        query1 should respond(OK)
        (query1.json / "data" / "data" / 0).as[String] shouldBe doc
        (query1.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query1.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (query1.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
        assertReadOps(partitions = 8, query1) // documents index has 8 partitions

        // Project the doc's id so that data projection doesn't cost more read ops.
        val query2 =
          queryRaw(
            s"""|let a = $coll.all().map(.id)
                |let b = $coll.$idx().map(.id)
                |a.concat(b).paginate()
                |""".stripMargin,
            db
          )
        query2 should respond(OK)
        (query2.json / "data" / "data").as[Seq[String]] shouldBe Seq(doc, doc)
        (query2.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query2.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (query2.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
        // 8 partitions form docs index + 8 from user defined index
        assertReadOps(partitions = 8 + 8, query2)
      }
    }

    once("paginate large set") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        nDocs = ValueSet.MaximumElements + 16 // at least 2 pages
        _ <- aDocument(db, coll) * nDocs
      } {
        val query = queryRaw(s"$coll.all().count()", db)
        (query.json / "data").as[Int] shouldBe nDocs
        (query.json / "stats" / "read_ops").as[Long] shouldBe 180
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 321
        (query.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
        (query.json / "stats" / "write_ops").as[Long] shouldEqual 0
      }
    }

    ignore("TODO: paginate/difference") {
      // TODO: implement me
    }

    ignore("TODO: paginate/intersection") {
      // TODO: implement me
    }

    once("paginate/map/get") {
      for {
        db  <- aDatabase
        idx <- anIdentifier
        coll <- aCollection(
          db,
          anIndex(
            name = Prop.const(idx)
          ))
        docs <- aDocument(db, coll, jsObject("foo" -> jsString)) * 10
        query = queryRaw(s"$coll.$idx().map(.foo).paginate()", db)
      } {
        val getOps =
          docs.foldLeft(8L) { (acc, doc) => // + 8 for paginate
            val q = queryRaw(s"$coll.byId('$doc')!", db)
            acc + (q.json / "stats" / "read_ops").as[Long]
          }
        query should respond(OK)
        (query.json / "data" / "data").as[Seq[String]].length shouldBe 10
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "read_ops").as[Long] shouldBe getOps
        (query.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldNot equal(0)
      }
    }

    once("paginate/map/get same ref") {
      for {
        db  <- aDatabase
        idx <- anIdentifier
        coll <- aCollection(
          db,
          anIndex(
            name = Prop.const(idx),
            values = Prop.const(Seq("doc"))
          ))
        doc <- aDocument(db, coll)
        _   <- aDocument(db, coll, Prop.const(s"{ doc: $coll.byId('$doc')! }")) * 10
        query = queryRaw(s"$coll.$idx().map(.doc).paginate()", db)
      } {
        val getOps =
          (queryRaw(s"$coll.byId('${doc}')!", db).json / "stats" / "read_ops")
            .as[Long] + 8 // + 8 for paginate

        query should respond(OK)
        (query.json / "data" / "data").as[JSArray].length shouldBe 11
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "read_ops").as[Long] shouldEqual getOps
        (query.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldNot equal(0)
        (query.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
      }
    }

    "order with covered values on set should only read documents for page size" - {
      orderCoveredValueTestsWithFormat("simple")
      orderCoveredValueTestsWithFormat("tagged")
      orderCoveredValueTestsWithFormat("decorated")
    }
    def orderCoveredValueTestsWithFormat(format: String) = {
      once(s"$format") {
        for {
          db <- aDatabase
          _ = queryOk(
            """
              |Collection.create({
              |  name: 'things',
              |  indexes: {
              |    by_color__value_asc: {
              |      terms: [{ field: '.color' }],
              |      values: [{ field: '.value' }]
              |    }
              |  }
              |})
              |""".stripMargin,
            db
          )
          _ = queryOk(
            """
              |
              |Set.sequence(0, 1000).forEach(i => things.create({ value: i, color: 'yellow' }))
              |Set.sequence(0, 1000).forEach(i => things.create({ value: i, color: 'purple' }))
              |""".stripMargin,
            db
          )
        } {
          val respF = queryRaw(
            """
              |let b = things.by_color__value_asc("yellow")
              |let a = things.by_color__value_asc("purple")
              |a.concat(b).order(.value).pageSize(10)
              |""".stripMargin,
            db,
            FQL2Params(format = Some(format))
          )

          (respF.json / "stats" / "read_ops").as[Long] shouldBe 32
        }
      }
    }

    once("role predicate on index read uses covered values") {
      for {
        db <- aDatabase
        roleSecret = queryOk(
          """
            |Collection.create({
            |  name: "Books",
            |  indexes: {
            |    byAuthor: { terms: [ { field: "author" } ], values: [ { field: "title" } ] }
            |  }
            |})
            |
            |Role.create({
            |  name: "TestRole",
            |  privileges: {
            |    resource: "Books",
            |    actions: {
            |      read: 'doc => doc.value == "not found"'
            |    }
            |  }
            |})
            |
            |Key.create({ role: "TestRole" }).secret
            |""".stripMargin,
          db
        ).as[String]
        _ = queryOk(
          """
            |
            |Set.sequence(0, 1000).forEach(i => Books.create({ author: "test_author", value: i, title: 'Dawnshard' }))
            |""".stripMargin,
          db
        )

      } {
        val respF = queryRaw(
          """Books.byAuthor("test_author").pageSize(1) { id }""",
          roleSecret
        )
        val rolePredicateReadsNonCoveredValue =
          (respF.json / "stats" / "read_ops").as[Long]

        val rs2 = queryOk(
          """
            |Role.byName("TestRole")!.update({
            |    privileges: {
            |      resource: "Books",
            |      actions: {
            |        read: 'doc => doc.title == "not found"'
            |      }
            |    }
            |})
            |
            |Key.create({ role: "TestRole" }).secret
            |""".stripMargin,
          db
        ).as[String]

        val resp2F = queryRaw(
          """Books.byAuthor("test_author").pageSize(1) { id }""",
          rs2
        )
        val rolePredicateReadsOnCoveredValue =
          (resp2F.json / "stats" / "read_ops").as[Long]

        rolePredicateReadsOnCoveredValue should be < (rolePredicateReadsNonCoveredValue / 2)
      }
    }
    // TODO: implement mean
    // TODO: implement all
    // TODO: implement any
    once("reduce with set") {
      for {
        db <- aDatabase
        coll <- aCollection(
          db,
          anIndex(
            name = Prop.const("allNumbers"),
            values = Prop.const(Seq("number"))
          ))
      } {
        // make docs
        queryOk(
          s"Set.sequence(0, 10_000).forEach(x => $coll.create({ number: 1 }))",
          db)

        // Returning null skips the projection of all elements in the produced array.
        val toArrayF = queryRaw(s"$coll.allNumbers().toArray(); null", db)
        toArrayF should respond(OK)
        (toArrayF.json / "data") shouldBe a[JSNull]
        (toArrayF.json / "stats" / "compute_ops").as[Long] shouldEqual 201
        (toArrayF.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (toArrayF.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
        assertReadOps(partitions = 8, toArrayF) // # of partitions in v10

        val foldF =
          queryRaw(
            s"$coll.allNumbers().fold(0, (acc, d) => acc + d.number)",
            db
          )
        foldF should respond(OK)
        (foldF.json / "data").as[Long] shouldBe 10000
        (foldF.json / "stats" / "compute_ops").as[Long] shouldEqual 401
        (foldF.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (foldF.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
        assertReadOps(partitions = 8, foldF) // # of partitions in v10

        // NOTE: There are 200 extra compute ops here from the `.map` call.
        val reduceF =
          queryRaw(
            s"$coll.allNumbers().map(d => d.number).reduce((acc, v) => acc + v)",
            db
          )
        reduceF should respond(OK)
        (reduceF.json / "data").as[Long] shouldBe 10000
        (reduceF.json / "stats" / "compute_ops").as[Long] shouldEqual 601
        (reduceF.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (reduceF.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
        assertReadOps(partitions = 8, reduceF) // # of partitions in v10

        val countF = queryRaw(s"$coll.allNumbers().count()", db)
        countF should respond(OK)
        (countF.json / "data").as[Long] shouldBe 10000
        (countF.json / "stats" / "compute_ops").as[Long] shouldEqual 201
        (countF.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (countF.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
        assertReadOps(partitions = 8, countF) // # of partitions in v10

        // NOTE: sum costs double the amount of FQL 4's sum because it requires a
        // mapping function to turn the index into a set of numbers.
        val sumF = queryRaw(s"Math.sum($coll.allNumbers().map(.number))", db)
        sumF should respond(OK)
        (sumF.json / "data").as[Long] shouldBe 10000
        (sumF.json / "stats" / "compute_ops").as[Long] shouldEqual 201
        (sumF.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (sumF.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
        assertReadOps(partitions = 8, sumF) // # of partitions in v10

        // Ditto on mean's cost vs. FQL4 mean.
        val meanF = queryRaw(s"Math.mean($coll.allNumbers().map(.number))", db)
        meanF should respond(OK)
        (meanF.json / "data").as[Long] shouldBe 1
        (meanF.json / "stats" / "compute_ops").as[Long] shouldEqual 201
        (meanF.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (meanF.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
        assertReadOps(partitions = 8, sumF) // # of partitions in v10
      }
    }

    ignore("TODO: identify") {
      // TODO: implement me
    }

    once("exists") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc  <- aDocument(db, coll, jsObject("foo" -> jsString))
        query = queryRaw(s"$coll.byId('$doc')!", db)
      } {
        query should respond(OK)
        (query.json / "data" / "id").as[String] shouldBe doc
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
        assertReadOps(partitions = 1, query)
      }
    }

    once("exists set") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc  <- aDocument(db, coll)
        query = queryRaw(s"$coll.all().isEmpty()", db)
      } {
        query should respond(OK)
        (query.json / "data").as[Boolean] shouldBe false
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldNot equal(0)
        (query.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
        assertReadOps(partitions = 8, query) // default # of partitions in v10
      }
    }

    once("if/exists/get") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc  <- aDocument(db, coll, Prop.const("{ foo: 42 }"))
        query = queryRaw(
          s"""|let val = $coll.byId('$doc')?.foo
              |if (val != null) {
              |  val
              |} else {
              |  "not found"
              |}
              |""".stripMargin,
          db
        )
      } {
        val docQ = queryRaw(s"$coll.byId('$doc')!", db)
        val getOps = (docQ.json / "stats" / "read_ops").as[Long]

        query should respond(OK)
        (query.json / "data").as[Long] shouldBe 42
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "read_ops").as[Long] shouldBe getOps
        (query.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldNot equal(0)
        (query.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
      }
    }

    once("index value term access shouldn't require an extra read op") {
      for {
        db  <- aDatabase
        idx <- anIdentifier
        coll <- aCollection(
          db,
          anIndex(
            name = Prop.const(idx),
            terms = Prop.const(Seq("foo"))
          ))
      } {
        queryOk(
          s"$coll.create({ foo: 'bar' })",
          db
        )
        val termAccess = queryRaw(s"$coll.$idx('bar').map(.foo).first()", db)
        (termAccess.json / "data").as[String] shouldBe "bar"
        (termAccess.json / "stats" / "read_ops").as[Long] shouldEqual 1
      }
    }

    /** Update off of an index entry requires an extra read op because we must read the document in order to update it
      */
    once("update off of index entry should require an extra read op") {
      for {
        db  <- aDatabase
        idx <- anIdentifier
        coll <- aCollection(
          db,
          anIndex(
            name = Prop.const(idx),
            terms = Prop.const(Seq("foo")),
            values = Prop.const(Seq("bar"))
          ))
      } {
        queryOk(
          s"$coll.create({ foo: 'bar', bar: 'foo' })",
          db
        )
        val indexUpdate =
          queryRaw(
            s"$coll.$idx('bar').first()!.update({ bar: 'baz' }) { foo, bar }",
            db)

        indexUpdate should respond(OK)
        (indexUpdate.json / "data") should containJSON(
          JSObject(
            "foo" -> "bar",
            "bar" -> "baz"
          )
        )
        (indexUpdate.json / "stats" / "read_ops").as[Long] shouldEqual 2
        (indexUpdate.json / "stats" / "write_ops").as[Long] shouldEqual 1
      }
    }

    once("if/exists/get set") {
      for {
        db  <- aDatabase
        idx <- anIdentifier
        coll <- aCollection(
          db,
          anIndex(
            name = Prop.const(idx),
            values = Prop.const(Seq("foo"))
          ))
        doc <- aDocument(db, coll, jsObject("foo" -> jsString))
        query = queryRaw(
          s"""|let set = $coll.$idx()
              |if (set.isEmpty()) {
              |  "empty"
              |} else {
              |  set.first().id
              |}
              |""".stripMargin,
          db,
          FQL2Params(typecheck = Some(false))
        )
      } {
        val docQ = queryRaw(s"$coll.byId('$doc')!", db)
        (docQ.json / "stats" / "read_ops").as[Long] shouldEqual 1

        query should respond(OK)
        (query.json / "data").as[String] shouldBe doc
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "read_ops").as[Long] shouldEqual 8
        (query.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldNot equal(0)
        (query.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
      }
    }

    once("if/exists/get set with bang") {
      for {
        db  <- aDatabase
        idx <- anIdentifier
        coll <- aCollection(
          db,
          anIndex(
            name = Prop.const(idx),
            values = Prop.const(Seq("foo"))
          ))
        doc <- aDocument(db, coll, jsObject("foo" -> jsString))
        query = queryRaw(
          s"""|let set = $coll.$idx()
              |if (set.isEmpty()) {
              |  "empty"
              |} else {
              |  set.first()!.id
              |}
              |""".stripMargin,
          db,
          FQL2Params(typecheck = Some(false))
        )
      } {
        val docQ = queryRaw(s"$coll.byId('$doc')!", db)
        (docQ.json / "stats" / "read_ops").as[Long] shouldEqual 1

        query should respond(OK)
        (query.json / "data").as[String] shouldBe doc
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "read_ops").as[Long] shouldEqual 8
        (query.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldNot equal(0)
        (query.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
      }
    }

    ignore("TODO: key_from_secret") {
      // TODO: implement me
    }

    once("abort") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc  <- aDocument(db, coll)
      } {
        val query = queryRaw(s"abort($coll.byId('$doc'))", db)

        query should respond(BadRequest)
        (query.json / "error" / "code").as[String] shouldEqual "abort"
        (query.json / "stats" / "compute_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "read_ops").as[Long] shouldEqual 1
        (query.json / "stats" / "write_ops").as[Long] shouldEqual 0
        (query.json / "stats" / "storage_bytes_read").as[Long] shouldNot equal(0)
        (query.json / "stats" / "storage_bytes_write").as[Long] shouldEqual 0
      }
    }
  }

  "miscellaneous" - {
    once("covered computed index values don't get read twice") {
      for {
        db <- aDatabase
      } {
        queryOk(
          s"""|Collection.create({
              |  name: "User",
              |  computed_fields: {
              |    firstAndLast: { body: 'u => "#{u.first} #{u.last}"' }
              |  },
              |  indexes: {
              |    byLast: {
              |      terms: [{ field: ".last" }],
              |      values: [{ field: ".firstAndLast" }]
              |    }
              |  }
              |})""".stripMargin,
          db
        )

        queryOk("User.create({ first: 'Bob', last: 'Jones' })", db)

        val query = queryRaw(s"User.byLast('Jones').first()!.firstAndLast", db)
        query should respond(OK)
        (query.json / "stats" / "read_ops").as[Long] shouldEqual 1
      }
    }

    once("take and drop work correctly") {
      for {
        db <- aDatabase
      } {
        queryOk("Collection.create({ name: 'User' })", db)

        queryOk("Set.sequence(0, 200).forEach(i => User.create({ id: ID(i) }))", db)

        val q1 = queryRaw("User.all().map(.id).take(110).toArray()", db)
        q1 should respond(OK)
        (q1.json / "stats" / "read_ops").as[Long] shouldBe 10
        val q2 = queryRaw("User.all().map(.id).drop(100).take(10).toArray()", db)
        q2 should respond(OK)
        (q2.json / "stats" / "read_ops").as[Long] shouldBe 10
        val q3 = queryRaw("User.all().map(.id).take(110).drop(100).toArray()", db)
        q3 should respond(OK)
        (q3.json / "stats" / "read_ops").as[Long] shouldBe 10
      }
    }
  }
}
