package fauna.api.test

import fauna.codex.cbor.CBOR
import fauna.codex.json._
import fauna.lang.clocks.Clock
import fauna.lang.Timestamp
import fauna.model.runtime.fql2.{ FQLInterpreter, ProjectedSet, SequenceSet }
import fauna.prop.Prop
import fauna.repo.schema.Path
import fauna.repo.values.{ Value, ValueReification }
import fauna.util.Base64
import fql.ast.{ Expr, Literal, Span }
import java.time.Instant
import org.scalatest.concurrent.Eventually
import org.scalatest.time.Seconds
import scala.collection.immutable.ArraySeq
import scala.concurrent.duration._

class FQL2StreamSpec extends FQL2StreamSpecHelpers with Eventually {
  implicit val codec = Value.EventSource.IR.spanlessCodec

  "Set.single(..).eventSource()" - {
    once("stream update events") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc  <- aDocument(db, coll)
        stream = s"Set.single($coll.byId($doc)).eventSource()"
        eventsF = subscribeQuery(stream, db, expect = 2)
      } yield {
        queryOk(s"$coll.byId($doc)!.update({ foo: 'bar' })", db)

        val events = await(eventsF)
        assertStatusEvent(events(0))
        assertDataEvent(events(1), "update")

        (events(1) / "data" / "@doc" / "foo").as[String] shouldBe "bar"
      }
    }

    once("stream remove events") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc  <- aDocument(db, coll)
        stream = s"Set.single($coll.byId($doc)).eventSource()"
        eventsF = subscribeQuery(stream, db, expect = 2)
      } yield {
        queryOk(s"$coll.byId($doc)!.delete()", db)

        val events = await(eventsF)
        assertStatusEvent(events(0))
        assertDataEvent(events(1), "remove")

        (events(1) / "data" / "@doc" / "id").as[String] shouldBe doc
      }
    }

    once("replay past events") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc  <- aDocument(db, coll)
        stream = queryOk(s"Set.single($coll.byId($doc)!).eventSource()", db)
        _ = queryOk(s"$coll.byId('$doc')!.update({})", db) // update from hist.
        eventsF = subscribeStream(stream.as[String], db, expect = 3)
        _ = queryOk(s"$coll.byId('$doc')!.update({})", db) // update on live stream
      } yield {
        val events = await(eventsF)
        assertStatusEvent(events(0))
        assertDataEvent(events(1), "update")
        assertDataEvent(events(2), "update")
      }
    }
  }

  ".all().eventSource()" - {
    once("stream create events") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        eventsF = subscribeQuery(s"$coll.all().eventSource()", db, expect = 2)
      } yield {
        queryOk(s"$coll.create({ foo: 'bar' })", db)

        val events = await(eventsF)
        assertStatusEvent(events(0))
        assertDataEvent(events(1), "add")

        (events(1) / "data" / "@doc" / "foo").as[String] shouldBe "bar"
      }
    }

    once("stream update events") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc  <- aDocument(db, coll)
        eventsF = subscribeQuery(s"$coll.all().eventSource()", db, expect = 2)
      } yield {
        queryOk(s"$coll.byId($doc)!.update({ foo: 'bar' })", db)

        val events = await(eventsF)
        assertStatusEvent(events(0))
        assertDataEvent(events(1), "update")

        (events(1) / "data" / "@doc" / "foo").as[String] shouldBe "bar"
      }
    }

    once("stream remove events") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc  <- aDocument(db, coll)
        eventsF = subscribeQuery(s"$coll.all().eventSource()", db, expect = 2)
      } yield {
        queryOk(s"$coll.byId($doc)!.delete()", db)

        val events = await(eventsF)
        assertStatusEvent(events(0))
        assertDataEvent(events(1), "remove")

        (events(1) / "data" / "@doc" / "id").as[String] shouldBe doc
      }
    }

    once("replay past events") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(s"$coll.all().eventSource()", db).as[String]

        doc <- aDocument(db, coll) // create from history
        update = queryOk(s"$coll.byId('$doc')!.update({})", db) // update from hist.
        delete = queryOk(s"$coll.byId('$doc')!.delete()", db) // delete from history

        eventsF = subscribeStream(stream, db, expect = 5)
        _ <- aDocument(db, coll) // create on live stream
      } yield {
        val events = await(eventsF)
        assertStatusEvent(events(0))
        assertDataEvent(events(1), "add")
        assertDataEvent(events(2), "update")
        assertDataEvent(events(3), "remove")
        assertDataEvent(events(4), "add")
      }
    }
  }

  "<user-index>().eventSource()" - {
    once("stream from user defined indexes") {
      for {
        db <- aDatabase
        coll <- aCollection(
          db,
          anIndex(
            name = Prop.const("byN"),
            terms = Prop.const(Seq(".n")),
            values = Prop.const(Seq(".m"))
          ))
        eventsF = subscribeQuery(s"$coll.byN(42).eventSource()", db, expect = 6)
        doc <- aDocument(db, coll, Prop.const("{ n: 42, m: 0 }")) // add (1)
      } yield {
        queryOk(s"$coll.byId($doc)!.update({ n: 42, m: 1 })", db) // update (2)
        queryOk(s"$coll.byId($doc)!.update({ n: 44, m: 1 })", db) // remove (3)
        queryOk(s"$coll.byId($doc)!.update({ n: 44, m: 2 })", db) // discard (-)
        queryOk(s"$coll.byId($doc)!.update({ n: 42, m: 2 })", db) // add (4)
        queryOk(s"$coll.byId($doc)!.update({ n: 42, x: 0 })", db) // discard (-)
        queryOk(s"$coll.byId($doc)!.delete()", db) // remove (5)

        val events = await(eventsF)
        assertStatusEvent(events(0))
        assertDataEvent(events(1), "add")
        assertDataEvent(events(2), "update")
        assertDataEvent(events(3), "remove")
        assertDataEvent(events(4), "add")
        assertDataEvent(events(5), "remove")
      }
    }

    once("replay past events") {
      for {
        db <- aDatabase
        coll <- aCollection(
          db,
          anIndex(
            name = Prop.const("byN"),
            terms = Prop.const(Seq(".n")),
            values = Prop.const(Seq(".m"))
          ))
        stream = queryOk(s"$coll.byN(42).eventSource()", db).as[String]
        doc <- aDocument(db, coll, Prop.const("{ n: 42, m: 0 }")) // add (1)
        _ = queryOk(s"$coll.byId($doc)!.update({ n: 42, m: 1 })", db) // update (2)
        _ = queryOk(s"$coll.byId($doc)!.update({ n: 44, m: 1 })", db) // remove (3)
        _ = queryOk(s"$coll.byId($doc)!.update({ n: 44, m: 2 })", db) // discard (-)
        _ = queryOk(s"$coll.byId($doc)!.update({ n: 42, m: 2 })", db) // add (4)
        _ = queryOk(s"$coll.byId($doc)!.update({ n: 42, x: 0 })", db) // discard (-)
        _ = queryOk(s"$coll.byId($doc)!.delete()", db) // remove (5)
      } yield {
        val events = await(subscribeStream(stream, db, expect = 6))
        assertStatusEvent(events(0))
        assertDataEvent(events(1), "add")
        assertDataEvent(events(2), "update")
        assertDataEvent(events(3), "remove")
        assertDataEvent(events(4), "add")
        assertDataEvent(events(5), "remove")
      }
    }
  }

  ".where(..).eventSource()" - {
    once("filter live events") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        eventsF = subscribeQuery(
          s"""|$coll
              |  .where(.n >= 1)
              |  .where(.n <= 3)
              |  .eventSource()
              |""".stripMargin,
          db,
          expect = 5
        )
      } yield {
        queryOk(s"$coll.create({ n: 0 })", db) // omitted

        for (_ <- 1 to 5) {
          queryOk(
            s"""|let doc = $coll.all().first()!
                |doc.update({ n: doc.n + 1 })
                |""".stripMargin,
            db
          )
        }

        queryOk(s"$coll.all().first()!.delete()", db) // omitted

        val events = await(eventsF)
        assertStatusEvent(events(0))
        assertDataEvent(events(1), "add")
        assertDataEvent(events(2), "update")
        assertDataEvent(events(3), "update")
        assertDataEvent(events(4), "remove")
      }
    }

    once("filter replayed events") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(
          s"""|$coll
              |  .where(.n >= 1)
              |  .where(.n <= 3)
              |  .eventSource()
              |""".stripMargin,
          db
        ).as[String]
      } yield {
        queryOk(s"$coll.create({ n: 0 })", db) // omitted

        for (_ <- 1 to 5) {
          queryOk(
            s"""|let doc = $coll.all().first()!
                |doc.update({ n: doc.n + 1 })
                |""".stripMargin,
            db
          )
        }

        queryOk(s"$coll.all().first()!.delete()", db) // omitted

        val events = await(subscribeStream(stream, db, expect = 5))
        assertStatusEvent(events(0))
        assertDataEvent(events(1), "add")
        assertDataEvent(events(2), "update")
        assertDataEvent(events(3), "update")
        assertDataEvent(events(4), "remove")
      }
    }
  }

  ".eventsOn(..)" - {
    once("tracks watched fields") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        eventsF = subscribeQuery(
          s"""|$coll
              |  .all()
              |  .eventsOn(.foo)
              |""".stripMargin,
          db,
          expect = 4
        )
      } yield {
        queryOk(s"$coll.create({ foo: 1 })", db)
        queryOk(s"$coll.all().first()!.update({ foo: 1 })", db)
        queryOk(s"$coll.all().first()!.update({ foo: 2 })", db)
        queryOk(s"$coll.all().first()!.delete()", db)

        val events = await(eventsF)
        assertStatusEvent(events(0))
        (assertDataEvent(events(1), "add") / "@doc" / "foo" / "@int")
          .as[String] shouldBe "1"
        (assertDataEvent(events(2), "update") / "@doc" / "foo" / "@int")
          .as[String] shouldBe "2"
        assertDataEvent(events(3), "remove")
      }
    }
  }

  "event feeds + stream" - {
    once("start stream after event feeds") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        _    <- aDocument(db, coll)
        source = queryOk(s"$coll.all().eventSource()", db).as[String]
      } yield {
        val page = {
          val res = client.api.post(
            "/feed/1",
            JSObject(
              "token" -> source,
              "start_ts" -> (Clock.time - 5.minutes).micros
            ),
            db.key
          )
          res should respond(OK)
          res.json
        }

        val feedEvents = (page / "events").as[Seq[JSValue]]
        feedEvents should have size 1
        assertDataEvent(feedEvents(0), "add")

        val cursor = (page / "cursor").as[String]
        val eventsF = subscribeStream(source, db, expect = 2, cursor = Some(cursor))
        queryOk(s"$coll.create({})", db)

        val streamEvents = await(eventsF)
        assertStatusEvent(streamEvents(0))
        assertDataEvent(streamEvents(1), "add")
      }
    }
  }

  "ABAC" - {
    once("fails to create a collection stream based on ABAC") {
      for {
        db     <- aDatabase
        coll   <- aCollection(db)
        role   <- aRole(db) // no privileges
        secret <- aKeySecret(db, role)
      } yield {
        val err = queryErr(s"$coll.all().eventSource()", secret)
        (err / "error" / "code").as[String] shouldBe "permission_denied"
        (err / "error" / "message").as[String] should include(
          "Insufficient privileges to read from collection")
      }
    }

    once("fails to create an index stream based on ABAC") {
      for {
        db <- aDatabase
        coll <- aCollection(
          db,
          anIndex(
            name = Prop.const("byFoo"),
            terms = Prop.const(Seq(".foo")),
            values = Prop.const(Seq(".bar"))
          ))
        role   <- aRole(db) // no privileges
        secret <- aKeySecret(db, role)
      } yield {
        val err = queryErr(s"$coll.byFoo(42).eventSource()", secret)
        (err / "error" / "code").as[String] shouldBe "permission_denied"
        (err / "error" / "message").as[String] should include(
          "Insufficient privileges to read from collection")
      }
    }

    once("fails to create a doc stream based on ABAC") {
      for {
        db     <- aDatabase
        coll   <- aCollection(db)
        doc    <- aDocument(db, coll)
        role   <- aRole(db) // no privileges
        secret <- aKeySecret(db, role)
      } yield {
        val err = queryErr(s"Set.single($coll.byId($doc)).eventSource()", secret)
        (err / "error" / "code").as[String] shouldBe "permission_denied"
        (err / "error" / "message").as[String] shouldBe
          "Insufficient privileges to create a stream on the source document."
      }
    }

    once("filter live stream based on ABAC") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        role <- aRole(
          db,
          privileges = Seq(
            Privilege(coll, read = RoleAction("doc => doc.foo <= 42"))
          ))
        secret <- aKeySecret(db, role)
        eventsF = subscribeQuery(s"$coll.all().eventSource()", secret, expect = 3)
        _ = queryOk(s"$coll.create({ foo: 42 })", db) // can see
        _ = queryOk(s"$coll.create({ foo: 55 })", db) // can NOT see
        _ = queryOk(s"$coll.create({ foo: 32 })", db) // can see
      } yield {
        val events = await(eventsF)
        assertStatusEvent(events(0))

        (assertDataEvent(events(1), "add") / "@doc" / "foo" / "@int")
          .as[String] shouldBe "42"

        (assertDataEvent(events(2), "add") / "@doc" / "foo" / "@int")
          .as[String] shouldBe "32"
      }
    }

    once(
      "removes caused by deletes with a read predicate permitting read of removed data are visible") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        _ = queryOk(
          s"""
             |$coll.create({ name: "hi" })
             |""".stripMargin,
          db
        )
        stream = s"$coll.all().eventSource()"
        roleName <- aUniqueIdentifier
        _ = queryOk(
          s"""
             |Role.create({
             |  name: "$roleName",
             |  privileges: [
             |    {
             |      resource: "$coll",
             |      actions: {
             |        read: 'doc => doc?.name == "hi"',
             |      }
             |    }
             |  ]
             |})
             |""".stripMargin,
          db
        )
        secret <- aKeySecret(db, roleName)
        eventsF = subscribeQuery(stream, secret, expect = 2)
      } yield {
        queryOk(s"$coll.all().forEach(.delete())", db)

        val events = await(eventsF)
        assertStatusEvent(events(0))
        val data = assertDataEvent(events(1), "remove")
        (data / "@doc" / "name").as[String] shouldEqual "hi"
      }
    }

    once("filter replayed stream based on ABAC") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        role <- aRole(
          db,
          privileges = Seq(
            Privilege(coll, read = RoleAction("doc => doc.foo <= 42"))
          ))
        secret <- aKeySecret(db, role)
        stream = queryOk(s"$coll.all().eventSource()", db).as[String]
        _ = queryOk(s"$coll.create({ foo: 42 })", db) // can see
        _ = queryOk(s"$coll.create({ foo: 55 })", db) // can NOT see
        _ = queryOk(s"$coll.create({ foo: 32 })", db) // can see
        eventsF = subscribeStream(stream, secret, expect = 3)
      } yield {
        val events = await(eventsF)
        assertStatusEvent(events(0))

        (assertDataEvent(events(1), "add") / "@doc" / "foo" / "@int")
          .as[String] shouldBe "42"

        (assertDataEvent(events(2), "add") / "@doc" / "foo" / "@int")
          .as[String] shouldBe "32"
      }
    }

    once(
      "an update granting permission to a document via a read role predicate is visible") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = s"$coll.all().where(.level == 1).eventSource()"
        roleName <- aUniqueIdentifier
        _ = queryOk(
          s"""
             |Role.create({
             |  name: "$roleName",
             |  privileges: [
             |    {
             |      resource: "$coll",
             |      actions: {
             |        read: 'doc => doc?.name == "hi"',
             |      }
             |    }
             |  ]
             |})
             |""".stripMargin,
          db
        )
        secret <- aKeySecret(db, roleName)
        eventsF = subscribeQuery(stream, secret, expect = 2)
      } yield {
        queryOk(
          s"""$coll.create({ name: "bye", level: 1 })""",
          db
        ) // filtered out due to read role predicate

        /** This is seen as an add because from the consumers perspective it is just entering the set, as they didn't
          * have permissions to see it prior so it effectively wasn't there for them.
          */
        queryOk(s"""$coll.all().forEach(.update({ name: "hi" }))""", db)

        val events = await(eventsF)
        assertStatusEvent(events(0))
        val data = assertDataEvent(events(1), "add")
        (data / "@doc" / "name").as[String] shouldEqual "hi"
      }
    }

    once("updates that result in permission loss show up as removes") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        _ = queryOk(
          s"""
             |$coll.create({ name: "hi" })
             |""".stripMargin,
          db
        )
        stream = s"$coll.all().eventSource()"
        roleName <- aUniqueIdentifier
        _ = queryOk(
          s"""
             |Role.create({
             |  name: "$roleName",
             |  privileges: [
             |    {
             |      resource: "$coll",
             |      actions: {
             |        read: 'doc => doc?.name == "hi"',
             |      }
             |    }
             |  ]
             |})
             |""".stripMargin,
          db
        )
        secret <- aKeySecret(db, roleName)
        eventsF = subscribeQuery(stream, secret, expect = 2)
      } yield {
        queryOk(
          s"""$coll.all().forEach(.update({ name: "bye" }))""",
          db
        ) // remove because we are losing permission to read see the document

        val events = await(eventsF)
        assertStatusEvent(events(0))
        val data = assertDataEvent(events(1), "remove")
        (data / "@doc" / "name").as[String] shouldEqual "hi"
      }
    }

    prop("close stream on permission loss") {
      for {
        db     <- aDatabase
        coll   <- aCollection(db)
        secret <- aKeySecret(db, "admin")
        stream = queryOk(s"$coll.all().eventSource()", db).as[String]
        eventsF = subscribeStream(stream, secret, expect = 2)
        _ = queryOk("Key.all().forEach(.delete())", db)
        _ = queryOk(s"$coll.create({})", db) // forces auth revalidation
      } yield {

        /** We issue enough creates here to exceed the batch size to ensure that we will send the permission loss event.
          * Based on how our batching observable works, while the last batch future isn't complete it will keep returning
          * ContinueF while it fills its batch size. Once full or the lastBatchF is complete it will return the result
          * of the lastBatchF.
          * In practice what this means is that for a customer that encounters permission loss in this way,
          * if the stream is not active, it will take until the next status event for them to get the permission loss
          * error event and closed stream ~1 minute.
          */
        eventually(timeout(org.scalatest.time.Span(10, Seconds))) {
          queryOk(s"$coll.create({})", db)
          eventsF.isCompleted shouldBe true
        }
        val events = await(eventsF)
        assertStatusEvent(events(0))
        assertErrorEvent(events(1), "permission_loss")
      }
    }
  }

  "correctly merges multiple same transaction document events" - {
    once("handles double remove events replay collection.all()") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc  <- aDocument(db, coll, Prop.const("{ n: 42, m: 0 }"))
        stream = queryOk(s"$coll.all().eventSource()", db).as[String]
        _ = queryOk(
          s"""
             |let doc = $coll.byId($doc)!
             |doc.update({ m: 43 })
             |doc.delete()
             |""".stripMargin,
          db
        )
        eventsF = subscribeStream(stream, db, expect = 2)
      } yield {
        val events = await(eventsF)
        assertStatusEvent(events(0))
        val data = assertDataEvent(events(1), "remove")
        (data / "@doc" / "m" / "@int").as[String] shouldBe "0"
      }
    }

    once("handles double remove events replay user defined index") {
      for {
        db <- aDatabase
        coll <- aCollection(
          db,
          anIndex(
            name = Prop.const("byN"),
            terms = Prop.const(Seq(".n")),
            values = Prop.const(Seq(".m"))
          ))
        doc <- aDocument(db, coll, Prop.const("{ n: 42, m: 0 }"))
        stream = queryOk(s"$coll.byN(42).eventSource() { m }", db).as[String]
        _ = queryOk(
          s"""
             |let doc = $coll.byId($doc)!
             |doc.update({ m: 43 })
             |doc.delete()
             |""".stripMargin,
          db
        )
        eventsF = subscribeStream(stream, db, expect = 2)
      } yield {
        val events = await(eventsF)
        assertStatusEvent(events(0))
        val data = assertDataEvent(events(1), "remove")
        (data / "m" / "@int").as[String] shouldBe "0"
      }
    }

    once("handles double remove events live stream collection.all()") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc  <- aDocument(db, coll, Prop.const("{ n: 42, m: 0 }"))
        eventsF = subscribeQuery(s"$coll.all().eventSource()", db, expect = 2)
      } yield {
        queryOk(
          s"""
             |let doc = $coll.byId($doc)!
             |doc.update({ m: 43 })
             |doc.delete()
             |""".stripMargin,
          db
        )

        val events = await(eventsF)
        assertStatusEvent(events(0))
        val data = assertDataEvent(events(1), "remove")
        (data / "@doc" / "m" / "@int").as[String] shouldBe "0"
      }
    }

    once("handles double remove events live stream user index") {
      for {
        db <- aDatabase
        coll <- aCollection(
          db,
          anIndex(
            name = Prop.const("byN"),
            terms = Prop.const(Seq(".n")),
            values = Prop.const(Seq(".m"))
          ))
        doc <- aDocument(db, coll, Prop.const("{ n: 42, m: 0 }"))
        eventsF = subscribeQuery(
          s"$coll.byN(42).eventSource() { m }",
          db,
          expect = 2)
      } yield {
        queryOk(
          s"""
             |let doc = $coll.byId($doc)!
             |doc.update({ m: 43 })
             |doc.delete()
             |""".stripMargin,
          db
        )

        val events = await(eventsF)
        assertStatusEvent(events(0))
        val data = assertDataEvent(events(1), "remove")
        (data / "m" / "@int").as[String] shouldBe "0"
      }
    }

    prop("handles multiple updates events live .all()") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc1 <- aDocument(db, coll, Prop.const("{ n: 42, m: 0 }"))
        doc2 <- aDocument(db, coll, Prop.const("{ n: 42, m: 1 }"))
        doc3 <- aDocument(db, coll, Prop.const("{ n: 42, m: 2 }"))
        stream = queryOk(s"$coll.all().eventSource()", db).as[String]

        eventsF = subscribeStream(stream, db, 4)

        _ = queryOk(
          s"""
             |let d1 = $coll.byId($doc1)!
             |let d2 = $coll.byId($doc2)!
             |let d3 = $coll.byId($doc3)!
             |d1.update({ m: 43 })
             |d2.update({ m: 44 })
             |d3.update({ m: 45 })
             |""".stripMargin,
          db
        )
      } yield {
        val events = await(eventsF)
        assertStatusEvent(events(0))
        val results = Seq(
          (assertDataEvent(events(1), "update") / "@doc" / "m" / "@int").as[String],
          (assertDataEvent(events(2), "update") / "@doc" / "m" / "@int").as[String],
          (assertDataEvent(events(3), "update") / "@doc" / "m" / "@int").as[String]
        )

        results shouldEqual Seq("43", "44", "45")
      }
    }

    once("handles multiple update events live user index") {
      for {
        db <- aDatabase
        coll <- aCollection(
          db,
          anIndex(
            name = Prop.const("byN"),
            terms = Prop.const(Seq(".n")),
            values = Prop.const(Seq(".m"))
          ))
        doc1 <- aDocument(db, coll, Prop.const("{ n: 42, m: 0 }"))
        doc2 <- aDocument(db, coll, Prop.const("{ n: 42, m: 1 }"))
        doc3 <- aDocument(db, coll, Prop.const("{ n: 42, m: 2 }"))
        eventsF = subscribeQuery(s"$coll.byN(42).eventSource()", db, 4)
        _ = queryOk(
          s"""
             |let d1 = $coll.byId($doc1)!
             |let d2 = $coll.byId($doc2)!
             |let d3 = $coll.byId($doc3)!
             |d1.update({ m: 43 })
             |d2.update({ m: 44 })
             |d3.update({ m: 45 })
             |""".stripMargin,
          db
        )
      } yield {
        val events = await(eventsF)
        assertStatusEvent(events(0))
        val results = Seq(
          (assertDataEvent(events(1), "update") / "@doc" / "m" / "@int").as[String],
          (assertDataEvent(events(2), "update") / "@doc" / "m" / "@int").as[String],
          (assertDataEvent(events(3), "update") / "@doc" / "m" / "@int").as[String]
        )

        results shouldEqual Seq("43", "44", "45")
      }
    }

    once("handles multiple update events replay") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc1 <- aDocument(db, coll, Prop.const("{ n: 42, m: 0 }"))
        doc2 <- aDocument(db, coll, Prop.const("{ n: 42, m: 1 }"))
        doc3 <- aDocument(db, coll, Prop.const("{ n: 42, m: 2 }"))
        stream = queryOk(s"$coll.all().eventSource()", db).as[String]
        _ = queryOk(
          s"""
             |let d1 = $coll.byId($doc1)!
             |let d2 = $coll.byId($doc2)!
             |let d3 = $coll.byId($doc3)!
             |d1.update({ m: 43 })
             |d2.update({ m: 44 })
             |d3.update({ m: 45 })
             |""".stripMargin,
          db
        )
        eventsF = subscribeStream(stream, db, expect = 4)
      } yield {
        val events = await(eventsF)
        assertStatusEvent(events(0))

        val results = Seq(
          (assertDataEvent(events(1), "update") / "@doc" / "m" / "@int").as[String],
          (assertDataEvent(events(2), "update") / "@doc" / "m" / "@int").as[String],
          (assertDataEvent(events(3), "update") / "@doc" / "m" / "@int").as[String]
        )

        results shouldEqual Seq("43", "44", "45")
      }
    }
  }

  "providing a start time" - {
    once("replays events, but not before the provided start time") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(s"$coll.all().eventSource()", db).as[String]

        doc <- aDocument(db, coll) // create from before start time
        _ = queryOk(
          s"$coll.byId('$doc')!.update({})",
          db
        ) // update from before start time.
        startTs = (queryRaw(s"$coll.byId('$doc')!.update({})", db).json / "txn_ts")
          .as[Long] // start time to be used
        _ = queryOk(s"$coll.byId('$doc')!.update({})", db) // update after start
        _ = queryOk(s"$coll.byId('$doc')!.delete()", db) // delete after start
        eventsF = subscribeStream(stream, db, expect = 4, Some(startTs))
        _ <- aDocument(db, coll) // create on live stream
      } yield {
        val events = await(eventsF)
        assertStatusEvent(events(0))
        assertDataEvent(events(1), "update")
        assertDataEvent(events(2), "remove")
        assertDataEvent(events(3), "add")
      }
    }

    once(
      "providing a start time prior to the stream token time fails with an error") {
      for {
        db <- aDatabase
        _ = queryOk(
          """Collection.create({ name: "TestColl", history_days: 0 })""",
          db)
      } yield {
        val res = queryRaw("TestColl.all().eventSource()", db).json
        val stream = (res / "data").as[String]
        val streamTS = (res / "txn_ts").as[Long]
        val streamRes =
          client.api.post(
            "/stream/1",
            JSObject("token" -> stream, "start_ts" -> (streamTS - 1)),
            db.key)
        streamRes should respond(400)
        streamRes.json shouldBe JSObject(
          "error" -> JSObject(
            "code" -> "invalid_request",
            "message" -> "Invalid stream start time: Stream start time can not be before the stream create time."
          )
        )
      }
    }

    once("providing a start time prior to collection mvt fails with an error") {
      for {
        db <- aDatabase
        _ = queryOk(
          """Collection.create({ name: "TestColl", history_days: 0 })""",
          db)
        stream = queryOk("TestColl.all().eventSource()", db).as[String]
        now = Timestamp(Instant.now())
        cursor = Value.EventSource.Cursor(now - 20.minutes)
        streamIR = Value.EventSource.fromBase64(stream).get.copy(cursor = cursor)
        streamWithUpdatedTime = Base64.encodeStandardAscii(
          CBOR.encode(streamIR).nioBuffer())
        eventsF = subscribeStream(
          streamWithUpdatedTime.toString,
          db,
          expect = 2,
          Some((now - 16.minutes).micros))
      } yield {
        val events = await(eventsF)
        assertStatusEvent(events.head)
        assertErrorEvent(events(1), "invalid_stream_start_time")
      }
    }

    once(
      "returns 400 when a body with a start_time is provided that is not a long") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        res = queryOk(s"$coll.all().eventSource()", db).as[String]
      } yield {
        val streamRes =
          client.api.post(
            "/stream/1",
            JSObject("token" -> res, "start_ts" -> "1"),
            db.key)
        streamRes should respond(400)
        streamRes.json shouldBe JSObject(
          "error" -> JSObject(
            "code" -> "invalid_request",
            "message" -> "Invalid request body: Unexpected string value '1'."
          )
        )
      }
    }
  }

  "providing a cursor" - {
    once("replays events") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(s"$coll.all().eventSource()", db).as[String]
        eventsF = subscribeStream(stream, db, expect = 2)
        _ = queryOk(
          s"""|Set.sequence(0, 2).forEach(n =>
              |  $coll.create({ n: n })
              |)
              |""".stripMargin,
          db
        )
      } yield {
        val events = await(eventsF)
        assertStatusEvent(events(0))
        (assertDataEvent(events(1), "add") / "@doc" / "n" / "@int")
          .as[String] shouldBe "0"

        val cursor = (events(1) / "cursor").as[String]
        val eventsF0 = subscribeStream(stream, db, expect = 2, cursor = Some(cursor))
        val events0 = await(eventsF0)

        assertStatusEvent(events0(0))
        (assertDataEvent(events0(1), "add") / "@doc" / "n" / "@int")
          .as[String] shouldBe "1"
      }
    }

    once("rejects both cursor and start time") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(s"$coll.all().eventSource()", db).as[String]
        startTS = Clock.time
      } yield {
        val res =
          client.api.post(
            "/stream/1",
            streamRequest(
              stream,
              startTime = Some(startTS.micros),
              cursor =
                Some(Value.EventSource.Cursor(startTS, ord = 1).toBase64.toString)
            ),
            db.key)
        res should respond(400)
        res.json shouldBe JSObject(
          "error" -> JSObject(
            "code" -> "invalid_request",
            "message" -> "Invalid request body: can not provide both `start_ts` and `cursor`"
          )
        )
      }
    }
  }

  "projected set streams" - {
    once("projection is applied to stream events") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        eventsF = subscribeQuery(
          s"""|$coll.all().eventSource() {
              |  first
              |}
              |""".stripMargin,
          db,
          expect = 3
        )
      } yield {
        queryOk(s"""$coll.create({ first: 0 , last: "foo"})""", db)

        queryOk(
          s"""|let doc = $coll.all().first()!
              |doc.update({ first: 1 })
              |""".stripMargin,
          db
        )

        val events = await(eventsF)
        assertStatusEvent(events(0))
        assertDataEvent(events(1), "add") shouldEqual JSObject(
          "first" -> JSObject("@int" -> "0"))
        assertDataEvent(events(2), "update") shouldEqual JSObject(
          "first" -> JSObject("@int" -> "1"))
      }
    }

    once("map is applied to stream events") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        eventsF = subscribeQuery(
          s"""|$coll.all().map(.first).eventSource()
              |""".stripMargin,
          db,
          expect = 3
        )
      } yield {
        queryOk(s"""$coll.create({ first: 0 , last: "foo"})""", db)

        queryOk(
          s"""|let doc = $coll.all().first()!
              |doc.update({ first: 1 })
              |""".stripMargin,
          db
        )

        val events = await(eventsF)
        assertStatusEvent(events(0))
        assertDataEvent(events(1), "add") shouldEqual JSObject("@int" -> "0")
        assertDataEvent(events(2), "update") shouldEqual JSObject("@int" -> "1")
      }
    }

    once("can project off of a filtered stream") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        eventsF = subscribeQuery(
          s"""|($coll
              |  .where(.n >= 1)
              |  .where(.n <= 3) { first }).eventSource()
              |""".stripMargin,
          db,
          expect = 5
        )
      } yield {
        queryOk(s"$coll.create({ n: 0 , first: 0})", db) // omitted

        for (_ <- 1 to 5) {
          queryOk(
            s"""|let doc = $coll.all().first()!
                |doc.update({ n: doc.n + 1 })
                |""".stripMargin,
            db
          )
        }

        queryOk(s"$coll.all().first()!.delete()", db) // omitted

        val events = await(eventsF)
        assertStatusEvent(events(0))
        assertDataEvent(events(1), "add") shouldEqual JSObject(
          "first" -> JSObject("@int" -> "0"))
        assertDataEvent(events(2), "update") shouldEqual JSObject(
          "first" -> JSObject("@int" -> "0"))
        assertDataEvent(events(3), "update") shouldEqual JSObject(
          "first" -> JSObject("@int" -> "0"))
        assertDataEvent(events(4), "remove")
      }
    }

    once("can read through to foreign keys in stream projection") {
      for {
        db    <- aDatabase
        coll  <- aCollection(db)
        coll2 <- aCollection(db)
        eventsF = subscribeQuery(
          s"""$coll.all().map(c => c.fk.n).eventSource()""".stripMargin,
          db,
          expect = 4
        )
      } yield {
        queryOk(
          s"""
             |let fk = $coll2.create({ n: 0 })
             |$coll.create({ name: "foo", fk: fk})""".stripMargin,
          db)

        queryOk(
          s"""|let doc = $coll.all().first()!
              |doc.update({ name: "boo" })
              |""".stripMargin,
          db
        )

        queryOk(
          s"""|let doc = $coll2.all().first()!
              |doc.update({ n: "hiya" })
              |""".stripMargin,
          db
        )

        queryOk(
          s"""|let doc = $coll.all().first()!
              |doc.update({ name: "who" })
              |""".stripMargin,
          db
        )

        val events = await(eventsF)
        assertStatusEvent(events(0))
        assertDataEvent(events(1), "add") shouldEqual JSObject("@int" -> "0")
        assertDataEvent(events(2), "update") shouldEqual JSObject("@int" -> "0")
        assertDataEvent(events(3), "update").as[String] shouldEqual "hiya"
      }
    }

    once("can read sets in stream projection") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(
          s"$coll.all().map(c => $coll.all().map(.name)).eventSource()",
          db).as[String]
        _ = queryOk(s"""$coll.create({ name: "foo" })""".stripMargin, db)
        _ = queryOk(
          s"""|let doc = $coll.all().first()!
              |doc.update({ name: "boo" })
              |""".stripMargin,
          db
        )
        eventsF = subscribeStream(
          stream,
          db,
          expect = 5
        )
      } yield {
        queryOk(s"""$coll.create({ name: "zoo" })""".stripMargin, db)
        queryOk(s"""$coll.create({ name: "too" })""".stripMargin, db)

        val events = await(eventsF)
        assertStatusEvent(events(0))
        assertDataEvent(events(1), "add") shouldEqual JSObject(
          "@set" -> JSObject("data" -> JSArray("foo")))
        assertDataEvent(events(2), "update") shouldEqual JSObject(
          "@set" -> JSObject("data" -> JSArray("boo")))
        assertDataEvent(events(3), "add") shouldEqual JSObject(
          "@set" -> JSObject("data" -> JSArray("boo", "zoo")))
        assertDataEvent(events(4), "add") shouldEqual JSObject(
          "@set" -> JSObject("data" -> JSArray("boo", "zoo", "too")))
      }
    }

    once("can filter off of a projected stream") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        eventsF = subscribeQuery(
          s"""|$coll.all().map(.n)
              |  .where(n => n >= 1)
              |  .where(n => n <= 3)
              |  .eventSource()
              |""".stripMargin,
          db,
          expect = 5
        )
      } yield {
        queryOk(s"$coll.create({ n: 0 , first: 0})", db) // omitted

        for (_ <- 1 to 5) {
          queryOk(
            s"""|let doc = $coll.all().first()!
                |doc.update({ n: doc.n + 1 })
                |""".stripMargin,
            db
          )
        }

        queryOk(s"$coll.all().first()!.delete()", db) // omitted

        val events = await(eventsF)
        assertStatusEvent(events(0))
        assertDataEvent(events(1), "add") shouldEqual JSObject("@int" -> "1")
        assertDataEvent(events(2), "update") shouldEqual JSObject("@int" -> "2")
        assertDataEvent(events(3), "update") shouldEqual JSObject("@int" -> "3")
        assertDataEvent(events(4), "remove")
      }
    }
  }

  "metrics" - {
    "live" - {
      runTestsWith(isReplay = false)
    }

    "replay" - {
      runTestsWith(isReplay = true)

      once("includes read metrics on start event collection stream") {
        for {
          db   <- aDatabase
          coll <- aCollection(db)
        } yield {
          val events = await(
            getEvents(
              eventQuery = s"$coll.create({})",
              streamQuery = s"$coll.all().eventSource()",
              db,
              isReplay = true,
              expect = 2
            ))

          assertStatusEvent(events.head)
          // the ChangesByCollection index has 8 partitions,
          // we add (partitions - 1) to our read ops calculation
          (events.head / "stats" / "read_ops").as[Long] shouldEqual 8
          (events.head / "stats" / "compute_ops").as[Long] shouldEqual 1
        }
      }

      once("includes read metrics on start event document stream") {
        for {
          db   <- aDatabase
          coll <- aCollection(db)
          _    <- aDocument(db, coll)
        } yield {
          val events = await(
            getEvents(
              eventQuery = s"$coll.all().first()!.update({ vv: 4 })",
              streamQuery = s"Set.single($coll.all().first()!).eventSource()",
              db,
              isReplay = true,
              expect = 2
            ))

          assertStatusEvent(events.head)
          (events.head / "stats" / "read_ops").as[Long] shouldEqual 1
          (events.head / "stats" / "compute_ops").as[Long] shouldEqual 1
        }
      }

      once("includes read metrics on start event user index (non-partitioned)") {
        for {
          db <- aDatabase
          coll <- aCollection(
            db,
            anIndex(
              Prop.const("byN"),
              Prop.const(Seq(".n"))
            ))
        } yield {
          val events = await(
            getEvents(
              eventQuery = s"$coll.create({ n: 1 })",
              streamQuery = s"$coll.byN(1).eventSource()",
              db,
              isReplay = true,
              expect = 2
            ))

          assertStatusEvent(events.head)
          (events.head / "stats" / "read_ops").as[Long] shouldEqual 1
          (events.head / "stats" / "compute_ops").as[Long] shouldEqual 1
        }
      }
    }

    def runTestsWith(isReplay: Boolean) = {
      once("gathers read and compute metrics for events collection stream") {
        for {
          db   <- aDatabase
          coll <- aCollection(db)
        } yield {
          val eventsF = getEvents(
            eventQuery = s"""
               |$coll.create({})
               |$coll.create({})
               |""".stripMargin,
            streamQuery = s"$coll.all().eventSource()",
            db,
            isReplay,
            expect = 3
          )

          val events = await(eventsF)
          assertStatusEvent(events.head)
          (events(1) / "stats" / "read_ops").as[Long] shouldEqual 1
          (events(1) / "stats" / "compute_ops").as[Long] shouldEqual 1
          (events(2) / "stats" / "read_ops").as[Long] shouldEqual 1
          (events(2) / "stats" / "compute_ops").as[Long] shouldEqual 1
        }
      }

      once("gathers read and compute metrics for live events document stream") {
        for {
          db   <- aDatabase
          coll <- aCollection(db)
          doc  <- aDocument(db, coll)
        } yield {
          val eventsF = getEvents(
            eventQuery = s"$coll.all().first()!.update({})",
            streamQuery = s"Set.single($coll.byId($doc)).eventSource()",
            db,
            isReplay,
            expect = 2
          )

          val events = await(eventsF)
          assertStatusEvent(events.head)
          (events(1) / "stats" / "read_ops").as[Long] shouldEqual 0
          (events(1) / "stats" / "compute_ops").as[Long] shouldEqual 1
        }
      }

      once("gathers read op metrics for foreign key collection stream reads") {
        for {
          db    <- aDatabase
          coll  <- aCollection(db)
          coll2 <- aCollection(db)
        } yield {
          val eventQuery = s"""
             |let d2 = $coll2.create({})
             |let d3 = $coll2.create({})
             |$coll.create({ adoc: d2 })
             |$coll.create({ adoc: d3 })
             |""".stripMargin

          val eventsF = getEvents(
            eventQuery,
            streamQuery = s"$coll.all().map(.adoc).eventSource()",
            db,
            isReplay,
            expect = 3
          )

          val events = await(eventsF)
          assertStatusEvent(events.head)
          (events(1) / "stats" / "read_ops").as[Long] shouldEqual 2
          (events(2) / "stats" / "read_ops").as[Long] shouldEqual 2
        }
      }

      once("gathers read op metrics for foreign key document stream reads") {
        for {
          db    <- aDatabase
          coll  <- aCollection(db)
          coll2 <- aCollection(db)
          doc   <- aDocument(db, coll)
        } yield {
          val streamQuery =
            s"Set.single($coll.byId($doc)!).map(.adoc).eventSource()"
          val eventsQuery =
            s"""
               |let d2 = $coll2.create({})
               |$coll.all().first()!.update({ adoc: d2 })
               |""".stripMargin

          val events =
            await(getEvents(eventsQuery, streamQuery, db, isReplay, expect = 2))
          assertStatusEvent(events.head)
          (events(1) / "stats" / "read_ops").as[Long] shouldEqual 1
        }
      }

      once("correctly tracks compute ops collection streams") {
        for {
          db   <- aDatabase
          coll <- aCollection(db)
        } yield {
          val streamQuery =
            s"""
               |$coll.all().map(d => {
               |  Set.sequence(1, 50).map( i => i ).paginate(50)
               |}).eventSource()""".stripMargin

          val events = await(
            getEvents(
              eventQuery = s"$coll.create({})",
              streamQuery,
              db,
              isReplay,
              expect = 2
            ))
          assertStatusEvent(events.head)
          (events(1) / "stats" / "compute_ops").as[Long] shouldEqual 2
        }
      }

      once("correctly tracks compute ops document streams") {
        for {
          db   <- aDatabase
          coll <- aCollection(db)
          doc  <- aDocument(db, coll)
        } yield {

          /** on an update, event transformations are applied for the
            * updated and previous version, which increases compute
            * ops
            */
          val streamQuery =
            s"""
               |Set.single($coll.byId($doc)).map(d => {
               |  Set.sequence(1, 25).map( i => i ).paginate(50)
               |}).eventSource()""".stripMargin

          val events = await(
            getEvents(
              eventQuery = s"$coll.all().first()!.update({})",
              streamQuery,
              db,
              isReplay,
              expect = 2
            ))
          assertStatusEvent(events.head)
          (events(1) / "stats" / "compute_ops").as[Long] shouldEqual 2
        }
      }

      once("records ops when error occurs during event processing") {
        for {
          db    <- aDatabase
          coll  <- aCollection(db)
          coll2 <- aCollection(db)
        } yield {
          val streamQuery =
            s"""$coll.all().map(d => {
               |  d.doc
               |}).map(.first.second).eventSource()""".stripMargin

          val eventsQuery =
            s"""
               |let aDoc = $coll2.create({})
               |$coll.create({
               | doc: aDoc
               |})
               |""".stripMargin

          val events = await(
            getEvents(
              eventsQuery,
              streamQuery,
              db,
              isReplay,
              expect = 2
            ))
          assertStatusEvent(events.head)
          (events(1) / "type").as[String] shouldBe "error"
          (events(1) / "stats" / "read_ops").as[Long] shouldBe 2
          (events(1) / "stats" / "compute_ops").as[Long] shouldBe 1
        }
      }
    }
  }

  "errors" - {
    once("returns correct 401 unauthorized error with no auth provided") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(s"$coll.all().eventSource()", db).as[String]
      } yield {
        val res = client.api.post("/stream/1", streamRequest(stream))
        res should respond(401)
        res.json shouldBe JSObject(
          "error" -> JSObject(
            "code" -> "unauthorized",
            "message" -> "Invalid token, unable to authenticate request"
          )
        )
      }
    }

    once("returns correct 401 unauthorized error with invalid auth provided") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(s"$coll.all().eventSource()", db).as[String]
      } yield {
        val res =
          client.api.post("/stream/1", streamRequest(stream), s"${db.key}invalidkey")
        res should respond(401)
        res.json shouldBe JSObject(
          "error" -> JSObject(
            "code" -> "unauthorized",
            "message" -> "Invalid token, unable to authenticate request"
          )
        )
      }
    }

    once("returns 400 when a body that doesn't map to a stream is provided") {
      for {
        db <- aDatabase
        res = queryOk(s"[1, 2].toSet().paginate(1)", db)

      } yield {
        val streamRes =
          client.api.post(
            "/stream/1",
            streamRequest((res / "after").as[String]),
            db.key)
        streamRes should respond(400)
        streamRes.json shouldBe JSObject(
          "error" -> JSObject(
            "code" -> "invalid_request",
            "message" -> "Invalid request body: invalid event source provided"
          )
        )
      }
    }

    once("returns 400 when a body without a stream field is provided") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        res = queryOk(s"$coll.all().eventSource()", db).as[String]
      } yield {
        val streamRes =
          client.api.post("/stream/1", JSObject("streamz" -> res), db.key)
        streamRes should respond(400)
        streamRes.json shouldBe JSObject(
          "error" -> JSObject(
            "code" -> "invalid_request",
            "message" -> "Invalid request body: Value for key token not found."
          )
        )
      }
    }

    once(
      "returns 400 when an encoded value that parses into a stream IR but doesn't have a stream expression fails") {
      for {
        db <- aDatabase
        expr = Expr.Lit(Literal.True, Span.Null)
        cursor = Value.EventSource.Cursor(Clock.time)
        streamIR = Value.EventSource.IR(expr, Vector.empty, cursor)
        stream = Base64.encodeStandardAscii(CBOR.encode(streamIR).nioBuffer())
      } yield {
        val streamRes =
          client.api.post("/stream/1", streamRequest(stream.toString), db.key)

        streamRes should respond(400)
        streamRes.json shouldBe JSObject(
          "error" -> JSObject(
            "code" -> "invalid_request",
            "message" -> "Invalid request body: invalid event source provided, Expected EventSource<Any>, received Boolean"
          )
        )
      }
    }

    once("eventSource(): returns 400 for not supported streams") {
      for {
        db <- aDatabase
        (expr, _) = ValueReification.reify(
          Value.EventSource(SequenceSet(0, 10), None))
        cursor = Value.EventSource.Cursor(Clock.time)
        streamIR = Value.EventSource.IR(expr, Vector.empty, cursor)
        stream = Base64.encodeStandardAscii(CBOR.encode(streamIR).nioBuffer())
      } yield {
        val streamRes =
          client.api.post("/stream/1", streamRequest(stream.toString), db.key)

        streamRes should respond(400)
        streamRes.json shouldBe JSObject(
          "error" -> JSObject(
            "code" -> "invalid_request",
            "message" -> "Error evaluating provided stream, can't call `.eventSource()` because streaming is not supported on sets created from `Set.sequence()`."
          )
        )
      }
    }

    once("eventSource(): returns 400 for streams wrapped in at(...) expression") {
      for {
        db <- aDatabase
        (expr, closure) = ValueReification.reify(
          Value.EventSource(
            ProjectedSet(
              SequenceSet(0, 10),
              Value.Lambda(
                ArraySeq(Some("x")),
                None,
                Expr.Id("x", Span.Null),
                Map.empty
              ),
              Some(Timestamp.Min),
              FQLInterpreter.StackTrace.empty
            ),
            None
          )
        )
        cursor = Value.EventSource.Cursor(Clock.time)
        streamIR = Value.EventSource.IR(expr, closure, cursor)
        stream = Base64.encodeStandardAscii(CBOR.encode(streamIR).nioBuffer())
      } yield {
        val streamRes =
          client.api.post("/stream/1", streamRequest(stream.toString), db.key)

        streamRes should respond(400)
        streamRes.json shouldBe JSObject(
          "error" -> JSObject(
            "code" -> "invalid_request",
            "message" -> "Error evaluating provided stream, can't call `.eventSource()` because streaming is not supported for sets created within an `at()` expression."
          )
        )
      }
    }

    once("eventsOn(): returns 400 for not supported streams") {
      for {
        db <- aDatabase
        (expr, _) = ValueReification.reify(
          Value.EventSource(SequenceSet(0, 10), None, Seq(Path(Right("foo")))))
        cursor = Value.EventSource.Cursor(Clock.time)
        streamIR = Value.EventSource.IR(expr, Vector.empty, cursor)
        stream = Base64.encodeStandardAscii(CBOR.encode(streamIR).nioBuffer())
      } yield {
        val streamRes =
          client.api.post("/stream/1", streamRequest(stream.toString), db.key)

        streamRes should respond(400)
        streamRes.json shouldBe JSObject(
          "error" -> JSObject(
            "code" -> "invalid_request",
            "message" -> "Error evaluating provided stream, can't call `.eventsOn()` because streaming is not supported on sets created from `Set.sequence()`."
          )
        )
      }
    }

    once(
      "returns 400 when a valid auth but different auth from stream create fails") {
      for {
        db   <- aDatabase
        db2  <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(s"$coll.all().eventSource()", db).as[String]
      } yield {
        val res = client.api.post("/stream/1", streamRequest(stream), db2.key)
        res should respond(400)
        val resJson = res.json
        (resJson / "error" / "code").as[String] shouldBe "invalid_request"
        (resJson / "error" / "message")
          .as[String]
          .startsWith("Error evaluating provided stream") shouldBe true
      }
    }

    once("aborting a stream shares the same error structure as a query abort") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(
          s"""$coll.all().map(doc => abort("test abort")).eventSource()""",
          db).as[String]
        eventsF = subscribeStream(stream, db, expect = 2)
        _ = queryOk(s"$coll.create({})", db)
      } yield {
        val abortQueryError = queryRaw("""abort("test abort")""", db).json
        val events = await(eventsF)
        assertStatusEvent(events.head)
        assertErrorEvent(events(1), "abort")
        (abortQueryError / "error") shouldEqual (events(1) / "error")
      }
    }

    once(
      "starting a stream that would replay over 128 events fails with an error event") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(s"""$coll.all().eventSource()""", db).as[String]
        _ <- aDocument(db, coll) * 129
        eventsF = subscribeStream(stream, db, expect = 130)
      } yield {
        val events = await(eventsF)
        assertStatusEvent(events.head)
        events.tail.take(128) foreach { assertDataEvent(_, "add") }
        assertErrorEvent(events.last, "stream_replay_volume_exceeded")
      }
    }

    once("attempting to write in a stream projection fails with an error event") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(
          s"""$coll.all().map(d => {
             |  $coll.create({})
             |  d
             |}).eventSource()""".stripMargin,
          db).as[String]
        eventsF = subscribeStream(stream, db, expect = 2)
        _ = queryOk(s"$coll.create({})", db)
      } yield {
        val events = await(eventsF)
        assertStatusEvent(events.head)
        assertErrorEvent(events(1), "invalid_effect")
      }
    }

    once("error during stream execution sends error event") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = queryOk(
          s"""$coll.all().map(d => {
             |  d.first.second
             |}).eventSource()""".stripMargin,
          db).as[String]
        eventsF = subscribeStream(stream, db, expect = 3)
        _ = queryOk(
          s"""
            |$coll.create({
            | first: {
            |   second: "hi"
            | }
            |})
            |""".stripMargin,
          db
        )
        _ = queryOk(s"$coll.create({})", db)
      } yield {
        val events = await(eventsF)
        assertStatusEvent(events.head)
        assertDataEvent(events(1), "add")
        assertErrorEvent(events(2), "invalid_null_access")
      }
    }
  }

  "encoding/decoding" - {
    once("can decode stream tokens with spans") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc  <- aDocument(db, coll)
        stream = s"Set.single($coll.byId($doc)).eventSource()"
        streamToken = queryOk(stream, db).as[String]
      } yield {
        // decode using spanless codec - as that is now the encoding path
        val bytes = Base64.decodeStandard(streamToken)
        val ir =
          CBOR.parse[Value.EventSource.IR](bytes)(Value.EventSource.IR.spanlessCodec)

        // encode with codec that contains spans
        val buf = CBOR.encode(ir)(Value.EventSource.IR.codec)
        val streamTokenWithSpans = Base64.encodeStandardAscii(buf.nioBuffer).toString

        // test subscribing with prior tokens that contain spans
        val eventsF = subscribeStream(streamTokenWithSpans, db, expect = 2)
        queryOk(s"$coll.byId($doc)!.update({ foo: 'bar' })", db)

        val events = await(eventsF)
        assertStatusEvent(events(0))
        assertDataEvent(events(1), "update")

        (events(1) / "data" / "@doc" / "foo").as[String] shouldBe "bar"
      }
    }
  }
}
