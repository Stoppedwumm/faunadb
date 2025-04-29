package fauna.api.test.queries

import fauna.api.test.QueryAPI21Spec
import fauna.codex.json._
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.syntax._
import fauna.net.http.HttpResponse
import fauna.prop.Prop
import fauna.stats.QueryMetrics
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

class MetricsSpec extends QueryAPI21Spec {

  def assertPure(query: Future[HttpResponse]): Unit = {
    query should respond (OK)
    query.header("X-Compute-Ops") shouldBe "1"
    query.header("X-Byte-Read-Ops") shouldBe "0"
    query.header("X-Byte-Write-Ops") shouldBe "0"
    query.header("X-Storage-Bytes-Read") shouldBe "0"
    query.header("X-Storage-Bytes-Write") shouldBe "0"
  }

  def assertWriteOps(query: Future[HttpResponse]) = {
    val bytes = query.header("X-Storage-Bytes-Write").toLong
    bytes shouldNot be(0)

    val byteOps = Math.ceil(bytes / QueryMetrics.BytesPerWriteOp.toDouble).toInt
    query.header("X-Byte-Write-Ops").toInt should be(byteOps)
  }

  def assertReadOps(partitions: Long, query: Future[HttpResponse]) = {
    val bytes = query.header("X-Storage-Bytes-Read").toLong
    bytes shouldNot be(0)

    val byteOps = Math.ceil(bytes / QueryMetrics.BytesPerReadOp.toDouble).toInt
    query.header("X-Byte-Read-Ops").toInt should be(byteOps + partitions - 1)
  }

  once("literals") {
    for {
      db <- aDatabase
    } {
      val query = runRawQuery(
        JSArray(
          MkRef(ClassRef("users"), 123),
          TS("1970-01-01T00:00:00Z"),
          Date("1970-01-01"),
          Bytes(Array[Byte](0x01, 0x02, 0x03))),
        db.key)

      assertPure(query)
    }
  }

  "forms" - {
    once("do") {
      for {
        db <- aDatabase
      } {
        val query = runRawQuery(Do(JSNull), db.key)
        assertPure(query)
      }
    }

    once("at") {
      for {
        db <- aDatabase
      } {
        val query = runRawQuery(At(Time("now"), JSNull), db.key)
        assertPure(query)
      }
    }

    once("let") {
      for {
        db <- aDatabase
      } {
        val query = runRawQuery(Let("x" -> "y")(Var("x")), db.key)
        assertPure(query)
      }
    }

    once("if/else") {
      for {
        db <- aDatabase
      } {
        val thenQ = runRawQuery(If(true, JSNull, JSNull), db.key)
        assertPure(thenQ)

        val elseQ = runRawQuery(If(false, JSNull, JSNull), db.key)
        assertPure(elseQ)
      }
    }

    once("and") {
      for {
        db <- aDatabase
      } {
        val query = runRawQuery(And(true, true, false), db.key)
        assertPure(query)
      }
    }

    once("or") {
      for {
        db <- aDatabase
      } {
        val query = runRawQuery(Or(false, false, true), db.key)
        assertPure(query)
      }
    }

    once("object") {
      for {
        db <- aDatabase
      } {
        val query = runRawQuery(MkObject(), db.key)
        assertPure(query)
      }
    }

    once("select") {
      for {
        db <- aDatabase
      } {
        val query = runRawQuery(Select(0, JSArray(1, 2, 3)), db.key)
        assertPure(query)
      }
    }
  }

  "write ops" - {
    once("create database") {
      for {
        db <- aDatabase
        dbName <- aUniqueDBName
      } {
        val query =
          runRawQuery(CreateDatabase(MkObject("name" -> dbName)), db.adminKey)

        query should respond (Created)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Byte-Read-Ops") shouldBe "0"
        query.header("X-Storage-Bytes-Read") shouldNot be("0")

        assertWriteOps(query)
      }
    }

    once("create class") {
      for {
        db <- aDatabase
        clsName <- aName
      } {
        val query =
          runRawQuery(CreateClass(MkObject("name" -> clsName)), db.adminKey)

        query should respond (Created)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Byte-Read-Ops") shouldBe "0"
        query.header("X-Storage-Bytes-Read") shouldNot be("0")

        // FIXME: this should exclude the SchemaSource write, but we
        // currently cannot dynamically control which writes accrue ops costs.
        // FIXME: Re-enable schema. 1 write op is correct, but only because
        // schema is disabled in api tests.
        query.header("X-Byte-Write-Ops") shouldBe "1"
        query.header("X-Storage-Bytes-Write") shouldNot be("0")
      }
    }

    once("create index") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idxName <- aName
      } {
        val query = runRawQuery(
          CreateIndex(MkObject("name" -> idxName, "source" -> cls.refObj)),
          db.adminKey)

        query should respond (Created)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Byte-Read-Ops") shouldBe "0"
        query.header("X-Storage-Bytes-Read") shouldNot be("0")

        assertWriteOps(query)
      }
    }

    once("create index with documents and synchronous build") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idxName <- aName
        numDocs = 100
        _ <- aDocument(coll) * numDocs
      } {
        val query = runRawQuery(
          CreateIndex(MkObject(
              "name" -> idxName,
              "source" -> coll.refObj,
              "terms" -> JSArray(
                MkObject("field" -> JSArray("data", "x")),
                MkObject("field" -> JSArray("data", "y"))
              ),
              "values" -> JSArray(MkObject("field" -> JSArray("data", "z")))
            )),
          db.adminKey)

        // 1 op for creation plus 1 op per term or value for
        // every QueryMetrics.BaselineCompute docs.
        val expCompute = (1 + (2 + 1) * numDocs / QueryMetrics.BaselineCompute).toInt

        query should respond (Created)
        query.header("X-Compute-Ops") shouldBe s"$expCompute"
        query.header("X-Byte-Read-Ops") shouldBe "0"
        query.header("X-Storage-Bytes-Read") shouldNot be("0")

        assertWriteOps(query)
      }
    }

    once("create function") {
      for {
        db <- aDatabase
        fnName <- aName
      } {
        val query = runRawQuery(
          CreateFunction(
            MkObject("name" -> fnName, "body" -> QueryF(Lambda("x" -> Var("x"))))),
          db.adminKey)

        query should respond (Created)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Byte-Read-Ops") shouldBe "0"
        query.header("X-Storage-Bytes-Read") shouldNot be("0")

        assertWriteOps(query)
      }
    }

    once("create key") {
      for {
        rootDB <- aDatabase
        db <- aDatabase(apiVers, rootDB)
      } {
        val query = runRawQuery(
          CreateKey(MkObject("role" -> "server", "database" -> db.refObj)),
          rootDB.adminKey)

        query should respond (Created)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Byte-Read-Ops") shouldBe "0"
        query.header("X-Storage-Bytes-Read") shouldNot be("0")

        assertWriteOps(query)
      }
    }

    once("create role") {
      for {
        db <- aDatabase
        roleName <- aName
      } {
        val query = runRawQuery(
          CreateRole(MkObject("name" -> roleName, "privileges" -> JSArray())),
          db.adminKey)

        query should respond (Created)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Byte-Read-Ops") shouldBe "0"
        query.header("X-Storage-Bytes-Read") shouldNot be("0")

        assertWriteOps(query)
      }
    }

    once("create instance") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        val query = runRawQuery(
          CreateF(cls.refObj, MkObject("data" -> MkObject("a" -> "b"))),
          db.adminKey)

        query should respond (Created)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Byte-Read-Ops") shouldBe "0"
        query.header("X-Storage-Bytes-Read") shouldBe "0"

        assertWriteOps(query)
      }
    }

    once("create instance with indexes") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        _ <- anIndex(cls).times(10)
      } {
        val query = runRawQuery(
          CreateF(cls.refObj, MkObject("data" -> MkObject("a" -> "b"))),
          db.adminKey)

        query should respond (Created)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Byte-Read-Ops") shouldBe "0"
        query.header("X-Storage-Bytes-Read") shouldBe "0"

        assertWriteOps(query)
      }
    }

    once("login") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
        pass <- Prop.string(1, 40)
        _ <- mkCredentials(db, inst, pass)
      } {
        val query =
          runRawQuery(Login(inst.refObj, MkObject("password" -> pass)), db.clientKey)

        query should respond (Created)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Byte-Read-Ops") shouldBe "1"
        query.header("X-Storage-Bytes-Read") shouldNot be("0")

        assertWriteOps(query)
      }
    }

    once("update") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        val query = runRawQuery(
          Update(inst.refObj, MkObject("data" -> MkObject("a" -> "c"))),
          db.adminKey)

        query should respond (OK)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Byte-Read-Ops") shouldBe "0"
        query.header("X-Storage-Bytes-Read") shouldNot be("0")

        assertWriteOps(query)
      }
    }

    once("update instance with indexes") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        _ <- anIndex(cls).times(10)
        inst <- aDocument(cls)
      } {
        val query = runRawQuery(
          Update(inst.refObj, MkObject("data" -> MkObject("a" -> "c"))),
          db.adminKey)

        query should respond (OK)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Byte-Read-Ops") shouldBe "0"
        query.header("X-Storage-Bytes-Read") shouldNot be("0")

        assertWriteOps(query)
      }
    }

    once("delete") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        val query = runRawQuery(DeleteF(inst.refObj), db.adminKey)

        query should respond (OK)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Byte-Read-Ops") shouldBe "0"
        query.header("X-Storage-Bytes-Read") shouldNot be("0")

        assertWriteOps(query)
      }
    }

    once("insert") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        val query = runRawQuery(
          InsertVers(
            inst.refObj,
            inst.ts + 1,
            "create",
            MkObject("data" -> MkObject("a" -> "c"))),
          db.adminKey)

        query should respond (OK)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Byte-Read-Ops") shouldBe "0"
        query.header("X-Storage-Bytes-Read") shouldNot be("0")

        assertWriteOps(query)
      }
    }

    once("remove") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        val query =
          runRawQuery(RemoveVers(inst.refObj, inst.ts, "create"), db.adminKey)

        query should respond (OK)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Byte-Read-Ops") shouldBe "0"
        query.header("X-Storage-Bytes-Read") shouldNot be("0")

        assertWriteOps(query)
      }
    }

    once("replace") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        val query = runRawQuery(
          Replace(inst.refObj, MkObject("data" -> MkObject("a" -> "c"))),
          db.adminKey)

        query should respond (OK)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Byte-Read-Ops") shouldBe "0"
        query.header("X-Storage-Bytes-Read") shouldNot be("0")

        assertWriteOps(query)
      }
    }

    once("invalidated read") {
      for {
        db <- aDatabase
        col <- aCollection(db)
        // prefer a small object for predictable X-Byte-Write-Ops
        doc <- aDocument(col, dataProp = jsObject(maxSize = 5, maxDepth = 1))
      } {
        val query = runRawQuery(
          Let(
            "_" -> Replace(doc.refObj, MkObject("data" -> MkObject("a" -> "c"))),
            "_" -> Exists(doc.refObj) // replace invalidates this read
          ) {
            CreateF(col.refObj)
          },
          db.adminKey
        )

        query should respond (Created)
        query.header("X-Byte-Read-Ops") shouldBe "0"
        query.header("X-Byte-Write-Ops").toInt shouldBe 2 // update + create
        query.header("X-Storage-Bytes-Read") shouldNot be("0")
        query.header("X-Storage-Bytes-Write") shouldNot be("0")
      }
    }

    once("contended write") {
      for {
        db <- aDatabase
        col <- aCollection(db)
        doc <- aDocument(col)
      } {
        val q = Update(doc.refObj, MkObject("data" -> MkObject("a" -> "c")))
        val baseline = runRawQuery(q, db.adminKey)
        baseline.statusCode shouldBe OK

        val reads = baseline.header("X-Byte-Read-Ops").toInt
        val writes = baseline.header("X-Byte-Write-Ops").toInt

        val futs = (1 to 20) map { _ =>
          Future {
            val res = runRawQuery(q, db.adminKey)
            res.statusCode should (be(OK) or be(Conflict))
            if (res.statusCode == OK) {
              val tries = res.header("X-Txn-Retries").toInt + 1
              res.header("X-Byte-Read-Ops").toInt shouldBe reads * tries
              res.header("X-Byte-Write-Ops").toInt shouldBe writes * tries
            }
          }
        }

        Await.result(futs.join, 1.minute)
      }
    }
  }

  "read ops" - {
    once("get ref") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        val query = runRawQuery(Get(inst.refObj), db.adminKey)

        query should respond (OK)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(1, query)
      }
    }

    once("get ref not found") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        val query = runRawQuery(Get(RefV(1, cls.refObj)), db.adminKey)

        query should respond (NotFound)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Byte-Read-Ops") shouldBe "0"
        query.header("X-Byte-Write-Ops") shouldBe "0"
        query.header("X-Storage-Bytes-Read") shouldBe "0"
        query.header("X-Storage-Bytes-Write") shouldBe "0"
      }
    }

    once("get same ref many times") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        val gets = Seq.fill(100)(Get(inst.refObj))

        val queryArray = runRawQuery(JSArray(gets: _*), db.adminKey)

        queryArray should respond (OK)
        queryArray.header("X-Compute-Ops") shouldBe "2"
        queryArray.header("X-Byte-Write-Ops") shouldBe "0"
        queryArray.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(1, queryArray)

        val queryDo = runRawQuery(Do(gets: _*), db.adminKey)

        queryDo should respond (OK)
        queryDo.header("X-Compute-Ops") shouldBe "2"
        queryDo.header("X-Byte-Write-Ops") shouldBe "0"
        queryDo.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(1, queryDo)
      }
    }

    once("get ref parallel") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        docs <- aDocument(cls).times(100)
      } {
        val ops = docs.foldLeft(0) { (acc, doc) =>
          val q = runRawQuery(Get(doc.refObj), db.adminKey)
          acc + q.header("X-Byte-Read-Ops").toInt
        }

        val gets = docs.map(ref => Get(ref.refObj))

        val queryArray = runRawQuery(JSArray(gets: _*), db.adminKey)

        queryArray should respond (OK)
        queryArray.header("X-Compute-Ops") shouldBe "2"
        queryArray.header("X-Byte-Read-Ops").toInt shouldBe ops
        queryArray.header("X-Byte-Write-Ops") shouldBe "0"
        queryArray.header("X-Storage-Bytes-Read") shouldNot be ("0")
        queryArray.header("X-Storage-Bytes-Write") shouldBe "0"

        val queryDo = runRawQuery(Do(gets: _*), db.adminKey)

        queryDo should respond (OK)
        queryDo.header("X-Compute-Ops") shouldBe "2"
        queryDo.header("X-Byte-Read-Ops").toInt shouldBe ops
        queryDo.header("X-Byte-Write-Ops") shouldBe "0"
        queryDo.header("X-Storage-Bytes-Read") shouldNot be ("0")
        queryDo.header("X-Storage-Bytes-Write") shouldBe "0"
      }
    }

    once("get set") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        partitions <- Prop.int(1 to 8)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "field"))), partitionsProp = Prop.const(Some(partitions)))
        _ <-
          aDocument(cls, dataProp = Prop.const(JSObject("field" -> "a"))).times(10)
      } {
        val query = runRawQuery(Get(Match(idx.refObj, "a")), db.adminKey)

        query should respond (OK)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Byte-Write-Ops") shouldBe "0"
        query.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(idx.partitions, query)
      }
    }

    once("get set not found") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "field"))))
        _ <-
          aDocument(cls, dataProp = Prop.const(JSObject("field" -> "a"))).times(10)
      } {
        val query = runRawQuery(Get(Match(idx.refObj, "b")), db.adminKey)

        query should respond (NotFound)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Byte-Read-Ops") shouldBe "0"
        query.header("X-Byte-Write-Ops") shouldBe "0"
        query.header("X-Storage-Bytes-Read") shouldBe "0"
        query.header("X-Storage-Bytes-Write") shouldBe "0"
      }
    }

    once("get schema ref") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
      } {
        val queryDB = runRawQuery(Get(db.refObj), rootKey)
        queryDB should respond (OK)
        queryDB.header("X-Compute-Ops") shouldBe "1"
        queryDB.header("X-Byte-Write-Ops") shouldBe "0"
        queryDB.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(1, queryDB)

        val queryCls = runRawQuery(Get(cls.refObj), db.adminKey)
        queryCls should respond (OK)
        queryCls.header("X-Compute-Ops") shouldBe "1"
        queryCls.header("X-Byte-Write-Ops") shouldBe "0"
        queryCls.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(1, queryCls)

        val queryIdx = runRawQuery(Get(idx.refObj), db.adminKey)
        queryIdx should respond (OK)
        queryIdx.header("X-Compute-Ops") shouldBe "1"
        queryIdx.header("X-Byte-Write-Ops") shouldBe "0"
        queryIdx.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(idx.partitions, queryIdx)
      }
    }

    once("paginate") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
        _ <- aDocument(cls)
      } {
        val query = runRawQuery(Paginate(Match(idx.refObj)), db.adminKey)

        query should respond (OK)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(idx.partitions, query)
      }
    }

    once("paginate/union") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
        _ <- aDocument(cls)
      } {
        val m = Match(idx.refObj)

        val query1 = runRawQuery(Paginate(Union(m)), db.adminKey)

        query1 should respond (OK)
        query1.header("X-Compute-Ops") shouldBe "1"
        query1.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(idx.partitions, query1)

        val query2 = runRawQuery(Paginate(Union(m, m)), db.adminKey)

        query2 should respond (OK)
        query2.header("X-Compute-Ops") shouldBe "1"
        query2.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(idx.partitions * 2, query2)
      }
    }

    once("paginate/difference") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
        _ <- aDocument(cls)
      } {
        val m = Match(idx.refObj)

        val query1 = runRawQuery(Paginate(Difference(m)), db.adminKey)

        query1 should respond (OK)
        query1.header("X-Compute-Ops") shouldBe "1"
        query1.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(idx.partitions, query1)

        val query2 = runRawQuery(Paginate(Difference(m, m)), db.adminKey)

        query2 should respond (OK)
        query2.header("X-Compute-Ops") shouldBe "1"
        query2.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(idx.partitions * 2, query2)
      }
    }

    once("paginate/intersection") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
        _ <- aDocument(cls)
      } {
        val m = Match(idx.refObj)

        val query1 = runRawQuery(Paginate(Intersection(m)), db.adminKey)

        query1 should respond (OK)
        query1.header("X-Compute-Ops") shouldBe "1"
        query1.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(idx.partitions, query1)

        val query2 = runRawQuery(Paginate(Intersection(m, m)), db.adminKey)

        query2 should respond (OK)
        query2.header("X-Compute-Ops") shouldBe "1"
        query2.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(idx.partitions * 2, query2)
      }
    }

    once("paginate/map/get") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "field"))))
        docs <-
          aDocument(cls, dataProp = Prop.const(JSObject("field" -> "a"))).times(10)
      } {
        val getOps = docs.foldLeft(0) { (acc, doc) =>
          val q = runRawQuery(Get(doc.refObj), db.adminKey)
          acc + q.header("X-Byte-Read-Ops").toInt
        }

        val query = runRawQuery(
          MapF(Lambda("ref" -> Get(Var("ref"))), Paginate(Match(idx.refObj, "a"))),
          db.adminKey
        )

        query should respond (OK)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Byte-Read-Ops").toInt shouldBe (getOps + 1) // +1 for paginate
        query.header("X-Storage-Bytes-Read") shouldNot be("0")
        query.header("X-Storage-Bytes-Write") shouldBe "0"
      }
    }

    once("paginate/map/get same ref") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(
          cls,
          Prop.const(Seq(JSArray("data", "field"))),
          Prop.const(Seq((JSArray("data", "dummy"), false))))
        dummy <- aDocument(cls)
        _ <- aDocument(
          cls,
          dataProp = Prop.const(JSObject("field" -> "a", "dummy" -> dummy.refObj)))
          .times(10)
      } {
        val dummyQ = runRawQuery(Get(dummy.refObj), db.adminKey)
        val getOps = dummyQ.header("X-Byte-Read-Ops").toInt

        val query = runRawQuery(
          MapF(
            Lambda("dummy" -> Get(Var("dummy"))),
            Paginate(Match(idx.refObj, "a"))),
          db.adminKey
        )

        query should respond (OK)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Byte-Read-Ops").toInt shouldBe (getOps + 1) // +1 for paginate
        query.header("X-Storage-Bytes-Read") shouldNot be("0")
        query.header("X-Storage-Bytes-Write") shouldBe "0"
      }
    }

    once("reduce functions with set") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        numberIdx <- anIndex(
          cls,
          Prop.const(Seq.empty),
          Prop.const(Seq((JSArray("data", "number"), false))))
        boolIdx <- anIndex(
          cls,
          Prop.const(Seq.empty),
          Prop.const(Seq((JSArray("data", "bool"), false))))
      } {
        for (_ <- 1 to 50) {
          runQuery(
            Foreach(
              Lambda(
                "i" -> CreateF(
                  cls.refObj,
                  MkObject("data" -> MkObject("number" -> 1, "bool" -> true)))),
              1 to 2000),
            db
          )
        }

        val reduceF = runRawQuery(
          Reduce(
            Lambda(JSArray("acc", "v") -> AddF(Var("acc"), Var("v"))),
            0,
            Match(numberIdx.refObj)
          ),
          db.adminKey
        )

        reduceF should respond (OK)
        reduceF.header("X-Compute-Ops") shouldBe "4001"
        reduceF.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(numberIdx.partitions, reduceF)

        val meanF = runRawQuery(
          Mean(
            Match(numberIdx.refObj)
          ),
          db.adminKey
        )

        meanF should respond (OK)
        meanF.header("X-Compute-Ops") shouldBe "2001"
        meanF.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(numberIdx.partitions, meanF)

        val countF = runRawQuery(
          Count(
            Match(numberIdx.refObj)
          ),
          db.adminKey
        )

        countF should respond (OK)
        countF.header("X-Compute-Ops") shouldBe "2001"
        countF.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(numberIdx.partitions, countF)

        val sumF = runRawQuery(
          Sum(
            Match(numberIdx.refObj)
          ),
          db.adminKey
        )

        sumF should respond (OK)
        sumF.header("X-Compute-Ops") shouldBe "2001"
        sumF.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(numberIdx.partitions, sumF)

        val allF = runRawQuery(
          All(
            Match(boolIdx.refObj)
          ),
          db.adminKey
        )

        allF should respond (OK)
        allF.header("X-Compute-Ops") shouldBe "2001"
        allF.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(boolIdx.partitions, allF)

        val anyF = runRawQuery(
          Any(
            Match(boolIdx.refObj)
          ),
          db.adminKey
        )

        anyF should respond (OK)
        anyF.header("X-Compute-Ops") shouldBe "2001"
        anyF.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(boolIdx.partitions, anyF)
      }
    }

    once("reduce functions with pages") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        numberIdx <- anIndex(
          cls,
          Prop.const(Seq.empty),
          Prop.const(Seq((JSArray("data", "number"), false))))
        boolIdx <- anIndex(
          cls,
          Prop.const(Seq.empty),
          Prop.const(Seq((JSArray("data", "bool"), false))))
      } {
        for (_ <- 1 to 50) {
          runQuery(
            Foreach(
              Lambda(
                "i" -> CreateF(
                  cls.refObj,
                  MkObject("data" -> MkObject("number" -> 1, "bool" -> true)))),
              1 to 2000),
            db
          )
        }

        val reduceF = runRawQuery(
          Reduce(
            Lambda(JSArray("acc", "v") -> AddF(Var("acc"), Var("v"))),
            0,
            Paginate(Match(numberIdx.refObj), size = 100000)
          ),
          db.adminKey
        )

        reduceF should respond (OK)
        reduceF.header("X-Compute-Ops") shouldBe ("4001")
        reduceF.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(numberIdx.partitions, reduceF)

        val meanF = runRawQuery(
          Mean(
            Paginate(Match(numberIdx.refObj), size = 100000)
          ),
          db.adminKey
        )

        meanF should respond (OK)
        meanF.header("X-Compute-Ops") shouldBe "2001"
        meanF.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(numberIdx.partitions, meanF)

        val countF = runRawQuery(
          Count(
            Paginate(Match(numberIdx.refObj), size = 100000)
          ),
          db.adminKey
        )

        countF should respond (OK)
        countF.header("X-Compute-Ops") shouldBe "2001"
        countF.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(numberIdx.partitions, countF)

        val sumF = runRawQuery(
          Sum(
            Paginate(Match(numberIdx.refObj), size = 100000)
          ),
          db.adminKey
        )

        sumF should respond (OK)
        sumF.header("X-Compute-Ops") shouldBe "2001"
        sumF.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(numberIdx.partitions, sumF)

        val allF = runRawQuery(
          All(
            Paginate(Match(boolIdx.refObj), size = 100000)
          ),
          db.adminKey
        )

        allF should respond (OK)
        allF.header("X-Compute-Ops") shouldBe "2001"
        allF.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(boolIdx.partitions, allF)

        val anyF = runRawQuery(
          Any(
            Paginate(Match(boolIdx.refObj), size = 100000)
          ),
          db.adminKey
        )

        anyF should respond (OK)
        anyF.header("X-Compute-Ops") shouldBe "2001"
        anyF.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(boolIdx.partitions, anyF)
      }
    }

    once("identify") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
        pass <- Prop.string(1, 40)
        _ <- mkCredentials(db, inst, pass)
      } {
        val query = runRawQuery(Identify(inst.refObj, pass), db.clientKey)

        query should respond (OK)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(1, query)
      }
    }

    once("exists") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        val query = runRawQuery(Exists(inst.refObj), db.adminKey)

        query should respond (OK)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(1, query)
      }
    }

    once("exists (miss)") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        val query = runRawQuery(Exists(RefV(1, cls.refObj)), db.adminKey)

        query should respond (OK)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(1, query)
      }
    }

    once("exists set") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "field"))))
        _ <- aDocument(cls, dataProp = Prop.const(JSObject("field" -> "a")))
      } {
        val query = runRawQuery(Exists(Match(idx.refObj, "a")), db.adminKey)

        query should respond (OK)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(idx.partitions, query)
      }
    }

    once("if/exists/get") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        doc <- aDocument(cls)
      } {
        val docQ = runRawQuery(Get(doc.refObj), db.adminKey)
        val getOps = docQ.header("X-Byte-Read-Ops")

        val query =
          runRawQuery(If(Exists(doc.refObj), Get(doc.refObj), false), db.adminKey)

        query should respond (OK)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Byte-Read-Ops") shouldBe getOps
        query.header("X-Storage-Bytes-Read") shouldNot be("0")
        query.header("X-Storage-Bytes-Write") shouldBe "0"
      }
    }

    once("if/exists/get set") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "field"))))
        doc <- aDocument(cls, dataProp = Prop.const(JSObject("field" -> "a")))
      } {
        val docQ = runRawQuery(Get(doc.refObj), db.adminKey)
        val getOps = docQ.header("X-Byte-Read-Ops")

        val set = Match(idx.refObj, "a")
        val query = runRawQuery(If(Exists(set), Get(set), false), db.adminKey)

        query should respond (OK)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Byte-Read-Ops") shouldBe getOps
        query.header("X-Storage-Bytes-Read") shouldNot be("0")
        query.header("X-Storage-Bytes-Write") shouldBe "0"
      }
    }

    once("key_from_secret") {
      for {
        parent <- aDatabase
        db <- aDatabase(apiVers, parent)
        key <- aKey(parent, Some(db))
      } {
        val query = runRawQuery(KeyFromSecret(key.secret), parent.adminKey)

        query should respond (OK)
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(1, query)
      }
    }

    once("abort") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        doc <- aDocument(coll)
      } {
        val query =
          runRawQuery(If(Exists(doc.refObj), Abort("abort"), JSNull), db.adminKey)

        query should respond (BadRequest)
        (query.json / "errors" / 0 / "description") shouldBe JSString("abort")
        query.header("X-Compute-Ops") shouldBe "1"
        query.header("X-Storage-Bytes-Write") shouldBe "0"

        assertReadOps(1, query)
      }
    }
  }
}
