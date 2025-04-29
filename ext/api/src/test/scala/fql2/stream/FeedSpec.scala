package fauna.api.test

import fauna.codex.json._
import fauna.lang.Timestamp
import fauna.model.runtime.stream.Feed
import fauna.prop.Prop
import fauna.repo.values.Value

class FeedSpec extends FeedSpecHelpers {

  ".all().eventSource()" - {
    once("poll from an empty stream") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(s"$coll.all().eventSource()", db).as[String]
      } {
        val page = poll(stream, db)
        (page / "has_next").as[Boolean] shouldBe false
        (page / "events").as[Seq[JSValue]] should have size 0
      }
    }

    once("start from a txn time") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(s"$coll.all().eventSource()", db).as[String]
        doc1TS = queryOk(s"$coll.create({}).ts", db).as[String]
        doc1TxnTS = Timestamp.parse(doc1TS).micros
        doc2ID <- aDocument(db, coll)
      } {
        val page = poll(stream, db, JSObject("start_ts" -> doc1TxnTS))
        (page / "has_next").as[Boolean] shouldBe false

        val events = (page / "events").as[Seq[JSValue]]
        events should have size 1
        (assertDataEvent(events(0), "add") / "@doc" / "id")
          .as[String] shouldBe doc2ID
      }
    }

    once("poll pages of events") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(s"$coll.all().eventSource()", db).as[String]
        nDocs    <- Prop.int(1 to 64)
        docs     <- aDocument(db, coll) * nDocs
        pageSize <- Prop.int(1 to 5)
      } {
        var page: JSObject = null
        var params = JSObject("page_size" -> pageSize)
        val eventIDs = Seq.newBuilder[String]

        do {
          page = poll(stream, db, params)
          params :+= "cursor" -> page / "cursor"

          (page / "events").as[Seq[JSValue]] foreach { event =>
            val data = assertDataEvent(event, "add")
            eventIDs += (data / "@doc" / "id").as[String]
          }
        } while ((page / "has_next").as[Boolean])

        eventIDs.result() should contain theSameElementsInOrderAs docs
      }
    }

    once("stop at first error, then resume the feed") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(
          s"""|$coll
              |  .all()
              |  .map(doc =>
              |    if (doc.n == 1) abort('oops')
              |    else doc.n
              |  )
              |  .eventSource()
              |""".stripMargin,
          db
        ).as[String]
      } {
        for (n <- 0 to 2) {
          queryOk(s"$coll.create({ n: $n })", db)
        }

        val page0 = poll(stream, db)
        val events0 = (page0 / "events").as[Seq[JSValue]]
        events0 should have size 2
        (assertDataEvent(events0(0), "add") / "@int").as[String] shouldBe "0"
        assertErrorEvent(events0(1), "abort")

        val page1 = poll(stream, db, JSObject("cursor" -> page0 / "cursor"))
        val events1 = (page1 / "events").as[Seq[JSValue]]
        events1 should have size 1
        (assertDataEvent(events1(0), "add") / "@int").as[String] shouldBe "2"
      }
    }
  }

  "errors" - {
    once("requires stream token") {
      for {
        db <- aDatabase
      } {
        val res =
          client.api.post(
            "/feed/1",
            JSObject.empty,
            db.adminKey
          )
        res should respond(BadRequest)
        (res.json / "error" / "code").as[String] shouldBe "invalid_request"
        (res.json / "error" / "message").as[String] shouldBe
          "Invalid request body: Value for key token not found."
      }
    }

    once("rejects invalid event source") {
      for {
        db <- aDatabase
      } {
        val res =
          client.api.post(
            "/feed/1",
            JSObject("token" -> "INVALID!"),
            db.adminKey
          )
        res should respond(BadRequest)
        (res.json / "error" / "code").as[String] shouldBe "invalid_request"
        (res.json / "error" / "message").as[String] shouldBe
          "Invalid request body: invalid event source provided."
      }
    }

    once("rejects large page size") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(s"$coll.all().eventSource()", db).as[String]
      } {
        val res =
          client.api.post(
            "/feed/1",
            JSObject(
              "token" -> stream,
              "page_size" -> (Feed.MaxPageSize + 1)
            ),
            db.adminKey
          )
        res should respond(BadRequest)
        (res.json / "error" / "code").as[String] shouldBe "invalid_request"
        (res.json / "error" / "message").as[String] shouldBe
          s"Invalid request body: page size can not be greater than ${Feed.MaxPageSize}."
      }
    }

    once("rejects invalid cursor") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(s"$coll.all().eventSource()", db).as[String]
      } {
        val res =
          client.api.post(
            "/feed/1",
            JSObject(
              "token" -> stream,
              "cursor" -> "INVALID!"
            ),
            db.adminKey
          )
        res should respond(BadRequest)
        (res.json / "error" / "code").as[String] shouldBe "invalid_request"
        (res.json / "error" / "message").as[String] shouldBe
          "Invalid request body: invalid event cursor provided."
      }
    }

    once("rejects invalid start time") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(s"$coll.all().eventSource()", db).as[String]
      } {
        val res =
          client.api.post(
            "/feed/1",
            JSObject(
              "token" -> stream,
              "start_ts" -> "INVALID!"
            ),
            db.adminKey
          )
        res should respond(BadRequest)
        (res.json / "error" / "code").as[String] shouldBe "invalid_request"
        (res.json / "error" / "message").as[String] shouldBe
          "Invalid request body: Unexpected string value 'INVALID!'."
      }
    }

    once("rejects both cursor and start ts at the same time") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(s"$coll.all().eventSource()", db).as[String]
        cursor = Value.EventSource.Cursor.MinValue.toBase64
      } {
        val res =
          client.api.post(
            "/feed/1",
            JSObject(
              "token" -> stream,
              "start_ts" -> Timestamp.Epoch.micros,
              "cursor" -> cursor
            ),
            db.adminKey
          )
        res should respond(BadRequest)
        (res.json / "error" / "code").as[String] shouldBe "invalid_request"
        (res.json / "error" / "message").as[String] shouldBe
          "Invalid request body: can not provide both `start_ts` and `cursor`."
      }
    }

    once("rejects invalid page size") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(s"$coll.all().eventSource()", db).as[String]
        cursor = Value.EventSource.Cursor.MinValue.toBase64
      } {
        val res =
          client.api.post(
            "/feed/1",
            JSObject(
              "token" -> stream,
              "page_size" -> -1
            ),
            db.adminKey
          )
        res should respond(BadRequest)
        (res.json / "error" / "code").as[String] shouldBe "invalid_request"
        (res.json / "error" / "message").as[String] shouldBe
          "Invalid request body: page size must be greater than zero."
      }
    }

    once("rejects writes") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(
          s"""|$coll
              |  .all()
              |  .map(doc => doc.update({ foo: 'bar' }))
              |  .eventSource()
              |""".stripMargin,
          db
        ).as[String]
        _ <- aDocument(db, coll)
      } {
        val page = poll(stream, db)
        (page / "has_next").as[Boolean] shouldBe false

        val events = (page / "events").as[Seq[JSValue]]
        events should have size 1
        assertErrorEvent(events(0), "invalid_effect")
      }
    }
  }
}
