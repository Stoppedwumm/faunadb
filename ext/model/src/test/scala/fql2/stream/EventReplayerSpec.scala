package fauna.model.test

import fauna.auth.Auth
import fauna.exec.ImmediateExecutionContext
import fauna.lang.clocks.Clock
import fauna.lang.Timestamp
import fauna.model.runtime.fql2.{ IndexSet, SingletonSet }
import fauna.model.runtime.fql2.serialization.MaterializedValue
import fauna.model.runtime.stream.{
  Event,
  EventFilter,
  EventReplayer,
  EventTransformer,
  EventType
}
import fauna.repo.schema.Path
import fauna.repo.values.Value
import fql.ast.{ Expr, Span }
import java.time.Instant
import scala.collection.immutable.ArraySeq
import scala.concurrent.duration._

class EventReplayerSpec extends FQL2Spec with StreamContextHelpers {

  implicit val ec = ImmediateExecutionContext

  var auth: Auth = _

  before {
    auth = newDB
    evalOk(auth, "Collection.create({ name: 'Foo' })")
  }

  "EventReplayer" - {
    "replays" - {
      "sets" - {
        "ts based" in {
          val stream = evalStream(auth, "Foo.all().toStream()")
          val create = evalRes(auth, "Foo.create({})")
          val update = evalRes(auth, "Foo.all().first()!.update({ foo: 'bar' })")
          val remove = evalRes(auth, "Foo.all().first()!.delete()")

          replayedEvents(stream, create.ts) should have size 2 // status + create
          replayedEvents(stream, update.ts) should have size 3 // ... + update

          val events = replayedEvents(stream, remove.ts)
          events should have size 4 // ... + remove

          inside(events.head) { case event: Event.Status =>
            event.eventType shouldBe EventType.Status
            event.ts shouldBe stream.cursor.value.ts
          }

          inside(events(1)) { case event: Event.Data =>
            event.eventType shouldBe EventType.Add
            event.cursor.ts shouldBe create.ts
          }

          inside(events(2)) { case event: Event.Data =>
            event.eventType shouldBe EventType.Update
            event.cursor.ts shouldBe update.ts
          }

          inside(events(3)) { case event: Event.Data =>
            event.eventType shouldBe EventType.Remove
            event.cursor.ts shouldBe remove.ts
          }
        }

        "merges multiple updates for mva values" in {
          val coll = "TestColl"
          evalOk(
            auth,
            s"""|
                |Collection.create({
                |  name: "$coll",
                |  indexes: {
                |    byN: {
                |      terms: [ { field: ".n" } ],
                |      values: [ {field: ".m", mva: true } ]
                |    }
                |  }
                |})
                |""".stripMargin
          )
          val d1 = evalOk(auth, "TestColl.create({ n: 42, m: 0 }).id").as[Long]
          val d2 = evalOk(auth, "TestColl.create({ n: 42, m: 1 }).id").as[Long]
          val d3 = evalOk(auth, "TestColl.create({ n: 42, m: 2 }).id").as[Long]
          val stream = evalStream(auth, "TestColl.byN(42).toStream()")

          val res = evalRes(
            auth,
            s"""
               |let d1 = TestColl.byId($d1)!
               |let d2 = TestColl.byId($d2)!
               |let d3 = TestColl.byId($d3)!
               |d1.update({ m: 43 })
               |d2.update({ m: 44 })
               |d3.update({ m: 45 })
               |""".stripMargin
          ).ts

          val events = replayedEvents(stream, res)
          events.size shouldEqual 4
          inside(events(1)) { case event: Event.Data =>
            event.eventType shouldBe EventType.Update
            event.data.docs.values.head
              .asInstanceOf[Value.Struct.Full]
              .fields
              .get("m") shouldEqual Some(Value.Int(43))
          }
          inside(events(2)) { case event: Event.Data =>
            event.eventType shouldBe EventType.Update
            event.data.docs.values.head
              .asInstanceOf[Value.Struct.Full]
              .fields
              .get("m") shouldEqual Some(Value.Int(44))
          }
          inside(events(3)) { case event: Event.Data =>
            event.eventType shouldBe EventType.Update
            event.data.docs.values.head
              .asInstanceOf[Value.Struct.Full]
              .fields
              .get("m") shouldEqual Some(Value.Int(45))
          }
        }
        "handles same ts across page boundary" in {
          evalOk(
            auth,
            """
              |Collection.create({
              |  name: "TestColl",
              |  indexes: {
              |    byN: {
              |      terms: [
              |        { field: ".n" }
              |      ],
              |      values: [
              |        { field: ".m" }
              |      ]
              |    }
              |  }
              |})
              |""".stripMargin
          )
          val d1 = evalOk(auth, "TestColl.create({ n: 42, m: 0 }).id").as[Long]
          val d2 = evalOk(auth, "TestColl.create({ n: 42, m: 1 }).id").as[Long]
          val d3 = evalOk(auth, "TestColl.create({ n: 42, m: 2 }).id").as[Long]
          val d4 = evalOk(auth, "TestColl.create({ n: 42, m: 12 }).id").as[Long]
          val stream = evalStream(auth, "TestColl.byN(42).toStream()")
          val res = evalRes(
            auth,
            s"""
               |let d1 = TestColl.byId($d1)!
               |let d2 = TestColl.byId($d2)!
               |let d3 = TestColl.byId($d3)!
               |TestColl.byId($d4)!.delete()
               |d1.update({ m: 43 })
               |d2.update({ m: 44 })
               |d3.update({ m: 45 })
               |""".stripMargin
          ).ts

          var nextCursor = Value.EventSource.Cursor.MinValue

          val events = replayedEvents(stream, res, pageSize = 2)

          inside(events(1)) { case event: Event.Data =>
            event.eventType shouldBe EventType.Update
            event.data.docs.values.head
              .asInstanceOf[Value.Struct.Full]
              .fields
              .get("m") shouldEqual Some(Value.Int(43))
          }
          nextCursor = events(1) match {
            case Event.Data(_, _, cursor) => cursor
            case ev => fail(s"Expected data event, received $ev")
          }

          nextCursor = {
            val events = replayedEvents(
              stream,
              res,
              streamCursor = Some(nextCursor),
              pageSize = 2)

            inside(events(1)) { case event: Event.Data =>
              event.eventType shouldBe EventType.Update
              event.data.docs.values.head
                .asInstanceOf[Value.Struct.Full]
                .fields
                .get("m") shouldEqual Some(Value.Int(44))
            }

            events(1) match {
              case Event.Data(_, _, cursor) => cursor
              case ev => fail(s"Expected data event, received $ev")
            }
          }

          nextCursor = {
            val events = replayedEvents(
              stream,
              res,
              streamCursor = Some(nextCursor),
              pageSize = 2)
            inside(events(1)) { case event: Event.Data =>
              event.eventType shouldBe EventType.Update
              event.data.docs.values.head
                .asInstanceOf[Value.Struct.Full]
                .fields
                .get("m") shouldEqual Some(Value.Int(45))
            }
            events(1) match {
              case Event.Data(_, _, cursor) => cursor
              case ev => fail(s"Expected data event, received $ev")
            }
          }

          nextCursor = {
            val events = replayedEvents(
              stream,
              res,
              streamCursor = Some(nextCursor),
              pageSize = 2)
            events.size shouldBe 2
            inside(events.head) { case _: Event.Status => }
            inside(events(1)) { case event: Event.Data =>
              event.eventType shouldBe EventType.Remove
              event.data.docs.values.head
                .asInstanceOf[Value.Struct.Full]
                .fields
                .get("m") shouldEqual Some(Value.Int(12))
            }

            events(1) match {
              case Event.Data(_, _, cursor) => cursor
              case ev => fail(s"Expected data event, received $ev")
            }
          }

          {
            val events = replayedEvents(
              stream,
              res,
              streamCursor = Some(nextCursor),
              pageSize = 2)
            events.size shouldBe 1
            inside(events.head) { case _: Event.Status => }
          }
        }

        "cursor based" in {
          val stream = evalStream(auth, "Foo.all().toStream()")
          val docs =
            evalRes(
              auth,
              """|Set.sequence(0, 3).forEach(n =>
                 |  Foo.create({ n: n })
                 |)
                 |""".stripMargin
            )

          val events =
            replayedCursor(stream, Value.EventSource.Cursor(docs.ts, ord = 1))
          events should have size 2 // start + 2

          inside(events.head) { case event: Event.Status =>
            event.eventType shouldBe EventType.Status
            event.ts shouldBe docs.ts
          }

          inside(events(1)) { case event: Event.Data =>
            event.eventType shouldBe EventType.Add
            event.cursor shouldBe Value.EventSource.Cursor(docs.ts, ord = 2)
          }
        }
      }

      "docs" in {
        val id = evalOk(auth, "Foo.create({}).id").as[Long]
        val stream = evalStream(auth, s"Set.single(Foo.byId($id)).toStream()")
        val update = evalRes(auth, "Foo.all().first()!.update({ foo: 'bar' })")
        val remove = evalRes(auth, "Foo.all().first()!.delete()")

        replayedEvents(stream, update.ts) should have size 2 // start + update

        val events = replayedEvents(stream, remove.ts)
        events should have size 3 // ... + remove

        inside(events.head) { case event: Event.Status =>
          event.eventType shouldBe EventType.Status
          event.ts shouldBe stream.cursor.value.ts
        }

        inside(events(1)) { case event: Event.Data =>
          event.eventType shouldBe EventType.Update
          event.cursor.ts shouldBe update.ts
        }

        inside(events(2)) { case event: Event.Data =>
          event.eventType shouldBe EventType.Remove
          event.cursor.ts shouldBe remove.ts
        }
      }
    }

    "do not replay events prior to subscription" - {
      "sets" in {
        evalRes(auth, "Foo.create({})") // before subscription
        val stream =
          evalStream(
            auth,
            """|Foo.create({}) // at subscription
               |Foo.all().toStream()
               |""".stripMargin
          )
        val create = evalRes(auth, "Foo.create({})") // after subscription
        val events = replayedEvents(stream, create.ts)
        events should have size 2 // start + create after subscription

        inside(events.head) { case event: Event.Status =>
          event.eventType shouldBe EventType.Status
          event.ts shouldBe stream.cursor.value.ts
        }

        inside(events(1)) { case event: Event.Data =>
          event.eventType shouldBe EventType.Add
          event.cursor.ts shouldBe create.ts
        }
      }

      "docs" in {
        val id = evalOk(auth, "Foo.create({}).id").as[Long]
        evalRes(auth, s"Foo.byId($id)!.update({ bar: 42 })") // before subscription

        val stream =
          evalStream(
            auth,
            s"""|let doc = Foo.byId($id)!
                |doc.update({ baz: 42 }) // at subscription
                |Set.single(doc).toStream()
                |""".stripMargin
          )

        val update = evalRes(auth, s"Foo.byId($id)!.update({bar: 44})") // after sub
        val events = replayedEvents(stream, update.ts)
        events should have size 2 // start + update after subscription

        inside(events.head) { case event: Event.Status =>
          event.eventType shouldBe EventType.Status
          event.ts shouldBe stream.cursor.value.ts
        }

        inside(events(1)) { case event: Event.Data =>
          event.eventType shouldBe EventType.Update
          event.cursor.ts shouldBe update.ts
        }
      }
    }

    "respects collection mvt when replaying events" - {
      "sets" in {
        evalOk(auth, "Collection.create({ name: 'TestColl', history_days: 0 })")
        evalOk(auth, "Collection.create({ name: 'TestColl2', history_days: 30 })")

        val now = Clock.time
        val rejected = evalStream(auth, "TestColl.all().toStream()")
        val events = replayedEvents(rejected, now, Some(now - 16.minutes))
        events should have size 2 // start and before mvt error

        inside(events(1)) { case event: Event.Error =>
          event.code shouldBe Event.Error.Code.InvalidStreamStartTime
        }

        val allowed = evalStream(auth, "TestColl2.all().toStream()")
        val events0 = replayedEvents(allowed, now, Some(now - 20.days))
        events0 should have size 1 // start
      }

      "docs" in {
        evalOk(auth, "Collection.create({ name: 'TestColl', history_days: 0 })")
        evalOk(auth, "Collection.create({ name: 'TestColl2', history_days: 30 })")

        val now = Clock.time
        val rejected =
          evalStream(
            auth,
            s"""|let doc = TestColl.create({})
                |Set.single(doc).toStream()
                |""".stripMargin
          )

        val events = replayedEvents(rejected, now, Some(now - 16.minutes))
        events should have size 2 // start and before mvt error

        inside(events(1)) { case event: Event.Error =>
          event.code shouldBe Event.Error.Code.InvalidStreamStartTime
        }

        val allowed =
          evalStream(
            auth,
            s"""|let doc = TestColl2.create({})
                |Set.single(doc).toStream()
                |""".stripMargin
          )
        val events0 = replayedEvents(allowed, now, Some(now - 20.days))
        events0 should have size 1 // start
      }
    }

    "filters replayed events" - {
      "sets" in {
        val filter = new EventFilter(Seq(Path(Right("foo"))))
        val stream = evalStream(auth, "Foo.all().toStream()")
        val create = evalRes(auth, "Foo.create({ foo: 'bar' })")
        val update = evalRes(auth, "Foo.all().first()!.update({ foo: 'bar' })")
        val remove = evalRes(auth, "Foo.all().first()!.delete()")

        replayedEvents(
          stream,
          create.ts,
          filter = filter
        ) should have size 2 // start + create

        replayedEvents(
          stream,
          update.ts,
          filter = filter
        ) should have size 2 // start + create (skip update)

        replayedEvents(
          stream,
          remove.ts,
          filter = filter
        ) should have size 3 // start + create (skip update) + remove
      }

      "docs" in {
        val filter = new EventFilter(Seq(Path(Right("foo"))))
        val stream = evalStream(auth, "Set.single(Foo.create({})).toStream()")
        val update1 = evalRes(auth, "Foo.all().first()!.update({ foo: 'bar' })")
        val update2 = evalRes(auth, "Foo.all().first()!.update({ bar: 'baz' })")
        val remove = evalRes(auth, "Foo.all().first()!.delete()")

        replayedEvents(
          stream,
          update1.ts,
          filter = filter
        ) should have size 2 // start + update

        replayedEvents(
          stream,
          update2.ts,
          filter = filter
        ) should have size 2 // start + create (skip update)

        replayedEvents(
          stream,
          remove.ts,
          filter = filter
        ) should have size 3 // start + create (skip update) + remove
      }
    }

    "providing a start time prior to collection mvt returns an error" in {
      evalOk(auth, """Collection.create({ name: "TestColl", history_days: 0 })""")
      val stream = evalStream(auth, "TestColl.all().toStream()")
      val now = Timestamp(Instant.now())
      val events = replayedEvents(
        stream,
        now,
        subTS = Some(now - 16.minutes)
      )
      events should have size 2
      inside(events.head) { case _: Event.Status => }
      inside(events(1)) { case event: Event.Error =>
        event.code shouldEqual "invalid_stream_start_time"
      }
    }

    "snapshots reads at the event transaction time" in {
      mkColl(auth, "TestColl")
      mkColl(auth, "StreamColl")
      val doc =
        evalOk(auth, """TestColl.create({ name: "pre-update" })""")
          .to[Value.Doc]

      // create a transform that will read the above created doc
      // at event translation time
      val readTransform =
        new EventTransformer.Project(
          Value.Lambda(
            params = ArraySeq(Some("arg")),
            vari = None,
            expr = Expr.Id("doc", Span.Null),
            closure = Map("doc" -> doc)
          )
        )

      val stream = evalStream(auth, "StreamColl.all().toStream()")

      // query to create first event, event from this should read pre-update
      evalOk(auth, "StreamColl.create({})")

      // update to post-update prior to event replay
      evalOk(auth, """TestColl.all().first()!.update({ name: "post-update" })""")

      // query to create second event, this event should read post-update
      val create = evalRes(auth, "StreamColl.create({})")

      val events =
        replayedEvents(
          stream,
          create.ts,
          transformer = new EventTransformer(Seq(readTransform))
        )

      events should have size 3

      inside(events.head) { case event: Event.Status =>
        event.eventType shouldBe EventType.Status
      }

      inside(events(1)) { case event: Event.Data =>
        event.data.docs(MaterializedValue.DocKey(doc)) match {
          case Value.Struct.Full(fields, _, _, _) =>
            fields("name") shouldEqual Value.Str("pre-update")
          case v =>
            fail(s"expected materialized doc value to Value.Struct.Full, got $v")
        }
      }

      inside(events(2)) { case event: Event.Data =>
        event.data.docs(MaterializedValue.DocKey(doc)) match {
          case Value.Struct.Full(fields, _, _, _) =>
            fields("name") shouldEqual Value.Str("post-update")
          case v =>
            fail(s"expected materialized doc value to Value.Struct.Full, got $v")
        }
      }
    }
  }

  private def evalStream(auth: Auth, query: String): Value.EventSource = {
    val res = evalRes(auth, query)
    val stream = res.value.to[Value.EventSource]
    // NB. Streams get their start time after materialized. Mock the materialization
    // process by overriding its start time here for testing.
    stream.copy(cursor = Some(Value.EventSource.Cursor(res.ts)))
  }

  private def replayedEvents(
    stream: Value.EventSource,
    startTS: Timestamp,
    subTS: Option[Timestamp] = None,
    filter: EventFilter = EventFilter.empty,
    transformer: EventTransformer = EventTransformer.empty,
    pageSize: Int = streamCtx.eventReplayLimit,
    streamCursor: Option[Value.EventSource.Cursor] = None
  ): Seq[Event] = {
    val cursor = streamCursor.orElse(subTS map {
      Value.EventSource.Cursor(_)
    } orElse stream.cursor)
    replayedCursor(stream, cursor.get, Some(startTS), filter, transformer, pageSize)
  }

  private def replayedCursor(
    stream: Value.EventSource,
    cursor: Value.EventSource.Cursor,
    startTS: Option[Timestamp] = None,
    filter: EventFilter = EventFilter.empty,
    transformer: EventTransformer = EventTransformer.empty,
    pageSize: Int = streamCtx.eventReplayLimit
  ): Seq[Event] = {
    val replayer =
      new EventReplayer(
        auth,
        filter,
        transformer
      )
    val replayQ =
      stream.set match {
        case SingletonSet(doc: Value.Doc) =>
          replayer.replay(
            auth.scopeID,
            doc.id,
            cursor,
            startTS,
            pageSize
          )
        case set: IndexSet =>
          replayer.replay(
            set,
            cursor,
            startTS,
            pageSize
          )
        case other => fail(s"unsupported set $other")
      }

    val eventsQ = replayQ.takeT(pageSize).flattenT
    (ctx ! eventsQ).flatMap { _.event }.toSeq
  }
}
