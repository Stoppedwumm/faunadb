package fauna.api.test

import fauna.codex.json._
import fauna.codex.json2._
import fauna.exec._
import fauna.lang.syntax._
import fauna.net.http._
import fauna.prop._
import fauna.prop.api._
import scala.concurrent.{ Await, ExecutionContext, Future, Promise }
import scala.concurrent.duration._
import scala.util.Failure

class StreamSpec extends API3Spec with StreamAPISupport {

  import FaunaExecutionContext.Implicits.global

  "stream" / {
    "document" - {
      def streamDocumentUpdates(fn: JSObject => JSObject): Prop[Unit] = {
        for {
          db <- aDatabase
          coll <- aCollection(db)
          doc <- aDocument(coll, dataProp = jsObject(minSize = 1))
          nUpdates <- Prop.int(1 to 64)
          objects <- (jsObject(minSize = 1) * nUpdates) mapT { obj =>
            JSObject("data" -> obj)
          }
          events = subscribe(fn(doc), db.key, expect = nUpdates) {
            objects foreach { data =>
              api.query(Replace(doc.refObj, Quote(data)), db.key) should respond(OK)
            }
          }
        } {
          events.zip(objects) foreach {
            case (event, data) =>
              (event / "type").as[String] shouldBe "version"
              (event / "event" / "action").as[String] shouldBe "update"
              (event / "event" / "document") should matchJSON(doc.merge(data))
          }
        }
      }

      prop("stream document update events from ref") {
        streamDocumentUpdates { doc =>
          doc.refObj
        }
      }

      once("stream document update events from version") {
        streamDocumentUpdates { doc =>
          Get(doc.refObj)
        }
      }

      prop("stream large documents") {
        for {
          db <- aDatabase
          coll <- aCollection(db)
          doc <- aDocument(coll)
          data <- jsObject(
            "data" -> jsObject(
              "field" -> jsString(
                minSize = 4 * 1024,
                maxSize = 8 * 1024
              )
            ))
          events = subscribe(doc.refObj, db.key, expect = 1) {
            api.query(Replace(doc.refObj, Quote(data)), db.key) should respond(OK)
          }
        } {
          (events.head / "event" / "document" / "data") shouldBe (data / "data")
        }
      }

      prop("stream document delete/re-create events") {
        for {
          db <- aDatabase
          col <- aCollection(db)
          doc <- aDocument(col, dataProp = jsObject(minSize = 1))
          data = MkObject("data" -> Quote(doc / "data"))
          events = subscribe(doc.refObj, db.key, expect = 2) {
            api.query(DeleteF(doc.refObj), db.key) should respond(OK)
            api.query(CreateF(doc.refObj, data), db.key) should respond(Created)
          }
        } {
          // Deleted version
          (events.head / "type").as[String] shouldBe "version"
          (events.head / "event" / "action").as[String] shouldBe "delete"
          (events.head / "event" / "document") should matchJSON(doc)

          // Re-created version
          (events(1) / "type").as[String] shouldBe "version"
          (events(1) / "event" / "action").as[String] shouldBe "create"
          (events(1) / "event" / "document") should matchJSON(doc)
        }
      }

      prop("stream prev and diff fields") {
        for {
          db <- aDatabase
          coll <- aCollection(db)
          doc <- aDocument(coll, dataProp = jsObject(minSize = 1))
          data <- jsObject(minSize = 1)
          next = JSObject("data" -> data)
          diff = JSObject("data" -> (doc / "data").as[JSObject].diffTo(data))
          fields = Seq("prev", "diff", "document")
          events = subscribe(doc.refObj, db.key, expect = 1, fields) {
            api.query(Replace(doc.refObj, Quote(next)), db.key) should respond(OK)
          }
        } {
          val txnTS = (events.head / "txn").as[Long]
          (events.head / "event" / "document") should matchJSON(doc.merge(next))
          (events.head / "event" / "document").ts shouldBe txnTS
          (events.head / "event" / "diff") should matchJSON(doc.merge(diff))
          (events.head / "event" / "diff").ts shouldBe txnTS
          (events.head / "event" / "prev") should matchJSON(doc)
          (events.head / "event" / "prev").ts shouldBe doc.ts
        }
      }

      prop("stream history rewrite events") {
        for {
          db <- aDatabase
          coll <- aCollection(db)
          doc <- aDocument(coll, dataProp = jsObject(minSize = 1))
          // NB: Modify history after the collection's MVT.
          ts = doc.ts - 10 * 1000
          events = subscribe(doc.refObj, db.key, expect = 4) {
            api.query(InsertVers(doc.refObj, ts, "create"), db.key) should respond(OK)
            api.query(RemoveVers(doc.refObj, ts, "create"), db.key) should respond(OK)
          }
        } {
          // insert revised the current version
          (events.head / "type").as[String] shouldBe "history_rewrite"
          (events.head / "event" / "action").as[String] shouldBe "update"
          (events.head / "event" / "document").ts shouldBe doc.ts
          (events.head / "event" / "document") should matchJSON(doc)

          // inserted version
          (events(1) / "type").as[String] shouldBe "history_rewrite"
          (events(1) / "event" / "action").as[String] shouldBe "create"
          (events(1) / "event" / "document").ts shouldBe ts
          (events(1) / "event" / "document" / "ref") shouldBe doc.refObj

          // removed version
          (events(2) / "type").as[String] shouldBe "history_rewrite"
          (events(2) / "event" / "action").as[String] shouldBe "create"
          (events(2) / "event" / "document").ts shouldBe ts
          (events(2) / "event" / "document" / "ref") shouldBe doc.refObj

          // remove revised the current version
          (events(3) / "type").as[String] shouldBe "history_rewrite"
          (events(3) / "event" / "action").as[String] shouldBe "create"
          (events(3) / "event" / "document").ts shouldBe doc.ts
          (events(3) / "event" / "document") should matchJSON(doc)
        }
      }

      once("filter doc events based on ABAC") {
        for {
          db   <- aDatabase
          coll <- aCollection(db)
          doc  <- aDocument(coll, dataProp = Prop.const(JSObject.empty))
          role <- aRole(
            db,
            Privilege(
              coll.refObj,
              read = RoleAction(
                Lambda(
                  "ref" -> Equals(
                    "foo",
                    Select(
                      JSArray("data", "field"),
                      Get(Var("ref")),
                      JSNull
                    ))))
            ))
          key <- aKey(role)
          events = subscribe(doc.refObj, key.secret, expect = 1) {
            def update(field: String): Unit = {
              val data = MkObject("data" -> MkObject("field" -> field))
              api.query(Update(doc.refObj, data), db.key) should respond(OK)
            }
            update("bar") // can't see
            update("bar") // can't see
            update("foo") // CAN SEE
            update("bar") // can't see
          }
        } {
          events should have size 1
        }
      }

      once("supports multiple streams to the same document") {
        for {
          db <- aDatabase
          coll <- aCollection(db)
          doc <- aDocument(coll)
        } {
          def subscribe0() = {
            val res = api.post("/stream", doc.refObj, db.key).res
            res.body.events.takeF(2) // start event + 1 update
          }
          val streams = Seq(subscribe0(), subscribe0()).sequence
          eventually {
            api.query(Update(doc.refObj), db.key) should respond(OK)
            assert(streams.isCompleted, "both streams completed")
          }
          val events = Await.result(streams, Duration.Zero)
          all(events) should have size 2
        }
      }

      once("can not subscribe to schema changes") {
        for {
          db <- aDatabase
          coll <- aCollection(db)
          res = api.post("/stream", coll.refObj, db.key)
        } {
          res should respond(BadRequest)
          res.errors should have size 1
          res.errors.head should containJSON(
            JSObject(
              "code" -> "invalid argument",
              "description" -> "Expected a Document Ref or Version, or a Set Ref, got Collection Ref."
            ))
        }
      }
    }

    "set" - {
      prop("stream index matches for added documents") {
        for {
          db <- aDatabase
          coll <- aCollection(db)
          partitionsP = Prop.const(Some(8))
          valueP = Prop.const(Seq((JSArray("data", "foo"), false)))
          index <- anIndex(coll, valueProp = valueP, partitionsProp = partitionsP)
          nDoc <- Prop.int(1 to 64)
          objs <- (jsObject(minSize = 1) * nDoc) mapT { obj =>
            JSObject(
              "data" -> JSObject(
                "foo" -> "bar",
                "rest" -> obj
              )
            )
          }
          setMatch = Match(index.refObj, "bar")
          fields = Seq("action", "document", "index")
          events = subscribe(setMatch, db.key, expect = nDoc, fields = fields) {
            objs foreach { data =>
              val query = CreateF(coll.refObj, Quote(data))
              api.query(query, db.key) should respond(Created)
            }
          }
        } {
          events foreach { e =>
            (e / "type").as[String] shouldBe "set"
            (e / "event" / "action").as[String] shouldBe "add"
            (e / "event" / "document").collRefObj shouldBe coll.refObj
            (e / "event" / "index" / "ref") shouldBe index.refObj
            (e / "event" / "index" / "terms").as[Seq[String]] shouldBe Seq("bar")
            (e / "event" / "index" / "values").as[Seq[String]] shouldBe Seq("bar")
          }
        }
      }

      prop("stream index matches for removed documents") {
        for {
          db <- aDatabase
          coll <- aCollection(db)
          partitionsP = Prop.const(Some(8))
          valueP = Prop.const(Seq((JSArray("data", "foo"), false)))
          index <- anIndex(coll, valueProp = valueP, partitionsProp = partitionsP)
          nDoc <- Prop.int(1 to 64)
          dataP = Prop.const(JSObject("foo" -> "bar"))
          docs <- someDocuments(nDoc, coll, dataProp = dataP)
          setMatch = Match(index.refObj, "bar")
          fields = Seq("action", "document", "index")
          replacement = JSObject("data" -> JSObject("foo" -> "baz"))
          events = subscribe(setMatch, db.key, expect = nDoc, fields = fields) {
            docs foreach { doc =>
              val query = Replace(doc.refObj, Quote(replacement))
              api.query(query, db.key) should respond(OK)
            }
          }
        } {
          events.zip(docs) foreach {
            case (e, doc) =>
              (e / "type").as[String] shouldBe "set"
              (e / "event" / "action").as[String] shouldBe "remove"
              (e / "event" / "document" / "ref") shouldBe doc.refObj
              (e / "event" / "index" / "ref") shouldBe index.refObj
              (e / "event" / "index" / "terms").as[Seq[String]] shouldBe Seq("bar")
              (e / "event" / "index" / "values").as[Seq[String]] shouldBe Seq("bar")
          }
        }
      }

      prop("pair-wise stream of removes and adds") {
        for {
          db <- aDatabase
          coll <- aCollection(db)
          partitionsP = Prop.const(Some(8))
          valueP = Prop.const(Seq((JSArray("data", "foo"), false)))
          index <- anIndex(coll, valueProp = valueP, partitionsProp = partitionsP)
          nDoc <- Prop.int(1 to 64)
          dataP = Prop.const(JSObject("foo" -> "bar"))
          docs <- someDocuments(nDoc, coll, dataProp = dataP)
          barMatch = Match(index.refObj, "bar")
          bazMatch = Match(index.refObj, "baz")
          fields = Seq("action", "document", "index")
          replacement = JSObject("data" -> JSObject("foo" -> "baz"))
          removesF = Future {
            subscribe(barMatch, db.key, expect = nDoc, fields = fields) {
              docs foreach { doc =>
                val query = Replace(doc.refObj, Quote(replacement))
                api.query(query, db.key) should respond(OK)
              }
            }
          }
          addsF = Future {
            subscribe(bazMatch, db.key, expect = nDoc, fields = fields) {
              // produce nothing, just listen
            }
          }
        } {
          val (removes, adds) = Await.result(removesF.zip(addsF), 1.minute)

          removes.zip(docs) foreach {
            case (e, doc) =>
              (e / "type").as[String] shouldBe "set"
              (e / "event" / "action").as[String] shouldBe "remove"
              (e / "event" / "document" / "ref") shouldBe doc.refObj
              (e / "event" / "index" / "ref") shouldBe index.refObj
              (e / "event" / "index" / "terms").as[Seq[String]] shouldBe Seq("bar")
              (e / "event" / "index" / "values").as[Seq[String]] shouldBe Seq("bar")
          }

          adds.zip(docs) foreach {
            case (e, doc) =>
              (e / "type").as[String] shouldBe "set"
              (e / "event" / "action").as[String] shouldBe "add"
              (e / "event" / "document" / "ref") shouldBe doc.refObj
              (e / "event" / "index" / "ref") shouldBe index.refObj
              (e / "event" / "index" / "terms").as[Seq[String]] shouldBe Seq("baz")
              (e / "event" / "index" / "values").as[Seq[String]] shouldBe Seq("baz")
          }
        }
      }

      once("can stream events from term-less index") {
        for {
          db <- aDatabase
          coll <- aCollection(db)
          termP = Prop.const(Seq.empty) // no terms
          valueP = Prop.const(Seq((JSArray("data", "foo"), false)))
          index <- anIndex(coll, termProp = termP, valueProp = valueP)
          nDoc <- Prop.int(1 to 64)
          objs <- (jsObject(minSize = 1) * nDoc) mapT { obj =>
            JSObject(
              "data" -> JSObject(
                "foo" -> "bar",
                "rest" -> obj
              )
            )
          }
          setMatch = Match(index.refObj)
          fields = Seq("action", "document", "index")
          events = subscribe(setMatch, db.key, expect = nDoc, fields) {
            objs foreach { data =>
              val query = CreateF(coll.refObj, Quote(data))
              api.query(query, db.key) should respond(Created)
            }
          }
        } {
          events foreach { e =>
            (e / "type").as[String] shouldBe "set"
            (e / "event" / "action").as[String] shouldBe "add"
            (e / "event" / "document").collRefObj shouldBe coll.refObj
            (e / "event" / "index" / "ref") shouldBe index.refObj
            (e / "event" / "index" / "terms").as[Seq[String]] shouldBe empty
            (e / "event" / "index" / "values").as[Seq[String]] shouldBe Seq("bar")
          }
        }
      }

      once("filter set events based on ABAC unrestricted_read action") {
        for {
          db    <- aDatabase
          coll  <- aCollection(db)
          index <- anIndex(coll, partitionsProp = Prop.const(Some(8)))
          role <- aRole(
            db,
            Privilege(
              index.refObj,
              unrestrictedRead = RoleAction(
                Lambda(
                  "terms" -> Equals(
                    Var("terms"),
                    JSArray("foo")
                  )))
            ))
          key <- aKey(role)
          bars = subscribe(Match(index.refObj, "bar"), key.secret, expect = 0) {
            val data = MkObject("data" -> MkObject("foo" -> "bar"))
            api.query(CreateF(coll.refObj, data), db.key) should respond(Created)
          }
          foos = subscribe(Match(index.refObj, "foo"), key.secret, expect = 1) {
            val data = MkObject("data" -> MkObject("foo" -> "foo"))
            api.query(CreateF(coll.refObj, data), db.key) should respond(Created)
          }
        } {
          bars should have size 0
          foos should have size 1
        }
      }

      once("filter set events based on ABAC read action") {
        for {
          db    <- aDatabase
          coll  <- aCollection(db)
          index <- anIndex(coll, partitionsProp = Prop.const(Some(8)))
          role <- aRole(
            db,
            Privilege(
              index.refObj,
              read = RoleAction(
                Lambda(
                  "terms" -> Equals(
                    Var("terms"),
                    JSArray("foo")
                  )))
            ),
            Privilege(
              coll.refObj,
              read = RoleAction(
                Lambda(
                  "ref" -> Equals(
                    0,
                    Select(
                      JSArray("data", "idx"),
                      Get(Var("ref")),
                      JSNull
                    ))))
            )
          )
          key <- aKey(role)
          events = subscribe(Match(index.refObj, "foo"), key.secret, expect = 1) {
            def create(idx: Int) = {
              val data = MkObject("data" -> MkObject("foo" -> "foo", "idx" -> idx))
              api.query(CreateF(coll.refObj, data), db.key) should respond(Created)
            }
            create(1) // can't see
            create(0) // CAN SEE
            create(1) // can't see
            create(1) // can't see
          }
        } {
          events should have size 1
        }
      }

      once("support streaming out of the 'documents' set") {
        for {
          db <- aDatabase
          coll <- aCollection(db)
          nDocs <- Prop.int(1 to 64)
          objs <- (jsObject(minSize = 1) * nDocs) mapT { obj =>
            JSObject("data" -> obj)
          }
          events = subscribe(Documents(coll.refObj), db.key, expect = nDocs) {
            objs foreach { data =>
              val query = CreateF(coll.refObj, Quote(data))
              api.query(query, db.key) should respond(Created)
            }
          }
        } {
          events foreach { e =>
            (e / "type").as[String] shouldBe "set"
            (e / "event" / "action").as[String] shouldBe "add"
            (e / "event" / "document").collRefObj shouldBe coll.refObj
          }
        }
      }

      once(
        "filter events from 'documents' set based on ABAC unrestricted_read action") {
        for {
          db   <- aDatabase
          coll <- aCollection(db)
          role <- aRole(
            db,
            Privilege(
              coll.refObj,
              unrestrictedRead = RoleAction.Granted
            ))
          key <- aKey(role)
          events = subscribe(Documents(coll.refObj), key.secret, expect = 1) {
            api.query(CreateF(coll.refObj), db.key) should respond(Created)
          }
        } {
          events should have size 1
        }
      }

      once("filter events from 'documents' set based on ABAC read action") {
        for {
          db   <- aDatabase
          coll <- aCollection(db)
          role <- aRole(
            db,
            Privilege(
              coll.refObj,
              read = RoleAction(
                Lambda(
                  "ref" -> Equals(
                    "foo",
                    Select(
                      JSArray("data", "field"),
                      Get(Var("ref")),
                      JSNull
                    ))))
            ))
          key <- aKey(role)
          events = subscribe(Documents(coll.refObj), key.secret, expect = 1) {
            def create(field: String) = {
              val data = MkObject("data" -> MkObject("field" -> field))
              api.query(CreateF(coll.refObj, data), db.key) should respond(Created)
            }
            create("bar") // can't see
            create("foo") // CAN SEE
            create("bar") // can't see
            create("bar") // can't see
          }
        } {
          events should have size 1
        }
      }
    }

    once("requires authentication") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        doc <- aDocument(coll)
        res = api.post("/stream", doc.refObj)
      } {
        res should respond(Unauthorized)
      }
    }

    once("reports stream id") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        doc <- aDocument(coll)
        res = api.post("/stream", doc.refObj, db.key)
      } {
        res.res.getHeader(HTTPHeaders.FaunaStreamID) should not be empty
      }
    }

    once("closes stream when authorization is lost") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        doc <- aDocument(coll, dataProp = Prop.const(JSObject.empty))
        role <- aRole(db, Privilege.open(coll.refObj))
        key <- aKey(role)
        events = subscribe(doc.refObj, key.secret, expect = 1) {
          api.query(DeleteF(key.refObj), db.adminKey) should respond(OK)
          api.query(Update(doc.refObj), db.key) should respond(OK)
        }
      } {
        (events.head / "type").as[String] shouldBe "error"
        (events.head / "event") should containJSON(
          JSObject(
            "code" -> "permission denied",
            "description" -> "Authorization lost during stream evaluation."
          ))
      }
    }

    once("only allows for POST method") {
      for {
        db <- aDatabase
      } {
        all(
          Seq(
            api.get("/stream", db.key),
            api.head("/stream", db.key),
            api.delete("/stream", db.key),
            api.put("/stream", NoBody, db.key),
            api.patch("/stream", NoBody, db.key))
        ) should respond(MethodNotAllowed)
      }
    }

    once("does not allow for writes") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        obj <- jsObject
        create = CreateF(coll.refObj, MkObject("data" -> Quote(obj)))
        res = api.post("/stream", create, db.key)
      } {
        res should respond(BadRequest)
        res.errors should have size 1
        res.errors.head should containJSON(
          JSObject(
            "code" -> "invalid expression",
            "description" -> "Call performs a write, which is not allowed in stream requests."
          ))
      }
    }

    once("fail to stream non-streamable type") {
      for {
        db <- aDatabase
        res = api.post("/stream", MkObject(), db.key)
      } {
        res should respond(BadRequest)
        res.errors should have size 1
        res.errors.head should containJSON(
          JSObject(
            "code" -> "invalid argument",
            "description" -> "Expected a Document Ref or Version, or a Set Ref, got Object."
          ))
      }
    }

    once("fail on unresolved ref") {
      for {
        db <- aDatabase
        unresolved = MkRef(ClassRef("foo"), "1234")
        res = api.post("/stream", unresolved, db.key)
      } {
        res should respond(BadRequest)
        res.errors should have size 1
        res.errors.head should containJSON(
          JSObject(
            "code" -> "invalid ref",
            "description" -> "Ref refers to undefined collection 'foo'"
          ))
      }
    }
  }
}

trait StreamAPISupport { self: FQL1APISpec =>

  protected def subscribe(
    body: HttpBody,
    auth: AuthType,
    expect: Int,
    fields: Seq[String] = Seq.empty)(produceEvents: => Unit)(implicit
    ec: ExecutionContext): Seq[JSValue] = {

    // expected events + start event message
    val events = gatherEvents(body, auth, expect + 1, fields)(produceEvents)
    val startTS = (events.head / "txn").as[Long]

    // validate start timestamp consistency
    withClue(s"Events: ${events.mkString("\n- ", "\n- ", "\n")}") {
      (events.head / "type").as[String] shouldBe "start"
      all(events map { e => (e / "txn").as[Long] }) should be >= startTS
    }

    // discard start event
    events.drop(1)
  }

  protected def gatherEvents(
    body: HttpBody,
    auth: AuthType,
    expect: Int,
    fields: Seq[String])(produceEvents: => Unit)(implicit
    ec: ExecutionContext): Seq[JSValue] = {

    val query = if (fields.nonEmpty) s"fields=${fields.mkString(",")}" else ""
    val request = api.post("/stream", body, auth, query)
    val resP = Promise[Seq[JSValue]]()

    @volatile var started = false

    val values = request.res.body.lines map {
      JSON.parse[JSValue]
    } map { event =>
      if (!started) { // wait for stream to start before producing events
        Future(produceEvents) andThen { case Failure(err) =>
          resP.tryFailure(err)
        }
        started = true
      }
      event
    }

    resP.completeWith(values.takeF(expect))
    Await.result(resP.future, 1.minute)
  }
}
