package fauna.api.test

import fauna.codex.json._
import fauna.lang.Timestamp
import fauna.net.http._
import fauna.prop.Prop
import language.implicitConversions
import org.scalatest.concurrent.Eventually
import scala.concurrent.duration._

class ExportDataEndpointSpec extends FQL2APISpec with Eventually with TaskHelpers {

  implicit def jsToBody(js: JSValue) = Body(js.toString, ContentType.JSON)

  val exportHeaders = Seq(HTTPHeaders.ExportKey -> "secret")

  def putReq(id: String, body: HttpBody, key: String) =
    api.put(s"/export/1/$id", body, key, headers = exportHeaders)

  def getReq(id: String, key: String) =
    api.get(s"/export/1/$id", key, headers = exportHeaders)

  def deleteReq(id: String, key: String) =
    api.delete(s"/export/1/$id", key, headers = exportHeaders)

  val anExportID = Prop.int(1 to Int.MaxValue).map(_.toString)

  "PUT export/1/{export_id}" / {
    once("requires export key") {
      for {
        db <- aDatabase
        _  <- aCollection(db)
        id <- anExportID
      } {
        val body = JSObject.empty

        api.put(s"/export/1/$id", body, db.key) should respond(NotFound)

        api.put(
          s"/export/1/$id",
          body,
          db.key,
          headers = Seq(HTTPHeaders.ExportKey -> "foo")) should respond(NotFound)

        api.put(
          s"/export/1/$id",
          body,
          db.key,
          headers = exportHeaders) should respond(Created)
      }
    }

    once("must be admin or server") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
      } {
        Seq(db.adminKey, db.key) foreach { key =>
          val id = anExportID.sample
          val body = JSObject.empty
          val res = putReq(id, body, key)
          res should respond(Created)

          (res.json / "account_id").asOpt[String] should not be None
          (res.json / "export_id").asOpt[String] shouldBe Some(id)
          (res.json / "task_id").asOpt[String] should not be None

          val accountID = (res.json / "account_id").as[String]
          val taskID = (res.json / "task_id").as[String]
          (res.json / "path").asOpt[String] shouldBe Some(
            s"$accountID/export_$id/$taskID")

          (res.json / "collections").asOpt[Seq[String]] shouldBe Some(
            Seq("Credential", coll))
          (res.json / "snapshot_ts").asOpt[String] should not be None
          (res.json / "parts").asOpt[Int] shouldBe Some(1)
          (res.json / "state").asOpt[String] shouldBe Some("pending")
          (res.json / "error").asOpt[String] shouldBe None
        }
      }
    }

    once("don't allow client keys") {
      for {
        db <- aDatabase
        _  <- aCollection(db)
        id <- anExportID
      } {
        val body = JSObject.empty

        putReq(id, body, db.clientKey) should respond(Forbidden)
      }
    }

    once("validates body") {
      for {
        db <- aDatabase
        id <- anExportID
      } {
        val res = putReq(id, NoBody, db.adminKey)
        res should respond(BadRequest)

        res.json shouldBe JSObject("error" -> "Missing request body")
      }
    }

    once("validates collections field") {
      for {
        db <- aDatabase
        id <- anExportID
      } {
        val body = JSObject("collections" -> JSArray())

        val res = putReq(id, body, db.key)
        res should respond(BadRequest)

        res.json shouldBe JSObject("error" -> "'collections' cannot be empty")
      }
    }

    once("validates collections existence") {
      for {
        db <- aDatabase
        id <- anExportID
      } {
        val body = JSObject(
          "collections" -> JSArray("foo")
        )

        val res = putReq(id, body, db.key)
        res should respond(BadRequest)

        res.json shouldBe JSObject("error" -> "Collections not found: foo")
      }
    }

    once("requires collection name, not alias") {
      for {
        db <- aDatabase
        id <- anExportID
      } {
        queryOk("Collection.create({ name: 'AColl', alias: 'AnAlias' })", db)

        val body = JSObject(
          "collections" -> JSArray("AnAlias")
        )

        val res = putReq(id, body, db.key)
        res should respond(BadRequest)

        res.json shouldBe JSObject("error" -> "Collections not found: AnAlias")
      }
    }

    once("validates doc format") {
      for {
        db <- aDatabase
        _  <- aCollection(db)
      } {
        def exportData(format: String) = {
          val body = JSObject(
            "doc_format" -> format
          )

          putReq(anExportID.sample, body, db.key)
        }

        exportData("simple") should respond(Created)
        exportData("tagged") should respond(Created)

        // disallow decorated, as well as generic invalid
        Seq("decorated", "foobar") foreach { bad =>
          val err = exportData(bad)
          err should respond(BadRequest)
          err.json shouldBe JSObject("error" -> s"Invalid doc_format '$bad'")
        }
      }
    }

    once("validates datafile format") {
      for {
        db <- aDatabase
        _  <- aCollection(db)
      } {
        def exportData(format: String) = {
          val body = JSObject(
            "datafile_format" -> format
          )

          putReq(anExportID.sample, body, db.key)
        }

        exportData("json") should respond(Created)
        exportData("jsonl") should respond(Created)

        // disallow generic invalid
        val err = exportData("foobar")
        err should respond(BadRequest)
        err.json shouldBe JSObject("error" -> s"Invalid datafile_format 'foobar'")
      }
    }

    once("idempotency") {
      for {
        db    <- aDatabase
        coll1 <- aCollection(db)
        coll2 <- aCollection(db)
        id    <- anExportID
      } {
        def exportData(colls: Option[Seq[String]], format: String) = {
          val body = JSObject(
            "collections" -> colls,
            "doc_format" -> format
          )

          putReq(id, body, db.key)
        }

        val ret0 = exportData(Some(Seq(coll1, coll2)), "tagged")
        ret0 should respond(Created)

        val ret1 = exportData(Some(Seq(coll1, coll2)), "tagged")
        ret1 should respond(OK)
        ret0.json shouldBe ret1.json

        val ret2 = exportData(Some(Seq(coll1)), "tagged")
        ret2 should respond(BadRequest)

        val ret3 = exportData(Some(Seq(coll1, coll2)), "simple")
        ret3 should respond(BadRequest)

        val ret4 = exportData(None, "tagged")
        ret4 should respond(OK)
        ret0.json shouldBe ret4.json

        // poll task until it completes
        eventually(timeout(20.minutes), interval(2.seconds)) {
          val get = getReq(id, db.key)
          (get.json / "state").as[String] should not equal ("pending")
        }

        // allow recreation
        val ret5 = exportData(Some(Seq(coll1)), "simple")
        ret5 should respond(Created)
      }
    }
  }

  "GET export/1/{export_id}" / {
    once("must be admin or server") {
      for {
        db <- aDatabase
        _  <- aCollection(db)
      } {
        Seq(db.adminKey, db.key) foreach { key =>
          val id = anExportID.sample
          val body = JSObject.empty
          val createF = putReq(id, body, db.key)
          createF should respond(Created)
          (createF.json / "export_id").as[String] shouldEqual id

          val getF = getReq(id, key)
          getF should respond(OK)

          createF.json shouldBe getF.json
        }
      }
    }

    once("don't allow client keys") {
      for {
        db <- aDatabase
        _  <- aCollection(db)
        id <- anExportID
      } {
        val body = JSObject.empty
        val createF = putReq(id, body, db.key)
        createF should respond(Created)
        (createF.json / "export_id").as[String] shouldEqual id

        getReq(id, db.clientKey) should respond(Forbidden)
      }
    }

    once("fail if export_id don't exist") {
      for {
        db <- aDatabase
        id <- anExportID
      } {
        getReq(id, db.key) should respond(NotFound)
      }
    }

    once("fail if export_id belongs to a different database") {
      for {
        db  <- aDatabase
        db2 <- aDatabase
        _   <- aCollection(db)
        id  <- anExportID
      } {
        val body = JSObject.empty
        val createF = putReq(id, body, db.key)
        createF should respond(Created)
        (createF.json / "export_id").as[String] shouldEqual id

        getReq(id, db2.key) should respond(NotFound)
      }
    }

    once("returns the latest task") {
      for {
        db <- aDatabase
        _  <- aCollection(db)
      } {
        val id = anExportID.sample
        val body = JSObject.empty

        val firstF = putReq(id, body, db.key)
        firstF should respond(Created)
        (firstF.json / "export_id").as[String] shouldEqual id

        // poll task until it completes
        eventually(timeout(20.minutes), interval(2.seconds)) {
          val get = getReq(id, db.key)
          (get.json / "state").as[String] should not equal "pending"
        }

        putReq(id, body, db.key) should respond(Created)

        // poll task until it completes
        eventually(timeout(20.minutes), interval(2.seconds)) {
          val get = getReq(id, db.key)
          (get.json / "state").as[String] should not equal "pending"
        }

        val latestF = getReq(id, db.key)

        (latestF.json / "task_id")
          .as[String] should not equal (firstF.json / "task_id").as[String]

        val ts0 = Timestamp.parse((firstF.json / "snapshot_ts").as[String])
        val ts1 = Timestamp.parse((latestF.json / "snapshot_ts").as[String])

        ts1 should be > ts0
      }
    }
  }

  "DELETE export/1/{export_id}" / {
    // TODO: Recombine the following two tests once the cache is improved
    //            so doing two exports in the same scope isn't flaky.
    once("can delete with server key") {
      for {
        db <- aDatabase
        _  <- aCollection(db)
      } {
        val id = anExportID.sample
        val body = JSObject.empty
        val createF = putReq(id, body, db.key)
        createF should respond(Created)
        (createF.json / "export_id").as[String] shouldEqual id

        deleteReq(id, db.key) should respond(NoContent)
      }
    }

    once("can delete with admin key") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
      } {
        val id = anExportID.sample
        val body = JSObject(
          "collections" -> JSArray(coll)
        )
        val createF = putReq(id, body, db.key)
        createF should respond(Created)
        (createF.json / "export_id").as[String] shouldEqual id

        deleteReq(id, db.adminKey) should respond(NoContent)
      }
    }

    once("don't allow client keys") {
      for {
        db <- aDatabase
        _  <- aCollection(db)
        id <- anExportID
      } {
        val body = JSObject.empty
        val createF = putReq(id, body, db.key)
        createF should respond(Created)
        (createF.json / "export_id").as[String] shouldEqual id

        deleteReq(id, db.clientKey) should respond(Forbidden)
      }
    }

    once("fail if export_id don't exist") {
      for {
        db <- aDatabase
        id <- anExportID
      } {
        deleteReq(id, db.key) should respond(NotFound)
      }
    }

    once("fail if export_id belongs to a different database") {
      for {
        db  <- aDatabase
        db2 <- aDatabase
        _   <- aCollection(db)
        id  <- anExportID
      } {
        val body = JSObject.empty
        val createF = putReq(id, body, db.key)
        createF should respond(Created)
        (createF.json / "export_id").as[String] shouldEqual id

        deleteReq(id, db2.key) should respond(NotFound)
      }
    }
  }
}
