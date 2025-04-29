package fauna.model.test

import fauna.auth.Auth
import fauna.lang.{ TimeBound, Timestamp }
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.model.runtime.stream.{ Event, Feed }
import fauna.repo.values.Value
import java.time.Instant
import org.scalatest.concurrent.Eventually
import scala.concurrent.duration._

class FeedSpec extends FQL2Spec with Eventually {

  var auth: Auth = _

  before {
    auth = newDB
    mkColl(auth, "Foo")
  }

  "ChangeFeed" - {
    "polling an empty page moves the cursor ts" in {
      val stream = evalStream("Foo.all().toStream()")
      val cursor0 = {
        val page = poll(stream)
        page.events shouldBe empty
        page.hasNext shouldBe false
        page.cursor
      }

      val nextCursor = {
        val page = poll(stream.copy(cursor = Some(cursor0)))
        page.events shouldBe empty
        page.hasNext shouldBe false
        page.cursor
      }

      nextCursor.ts.millis should be > cursor0.ts.millis
      evalOk(auth, "Foo.create({ bar: 42 })")

      val page = poll(stream.copy(cursor = Some(nextCursor)))
      page.events should have size 1
      page.events(0)._1 shouldBe a[Event.Data]
      page.hasNext shouldBe false
    }

    "polling an empty page moves the cursor ts - document stream" in {
      val stream =
        evalStream("""Set.single(Foo.create({ name: "test" })).toStream()""")
      val cursor0 = {
        val page = poll(stream)
        page.events shouldBe empty
        page.hasNext shouldBe false
        page.cursor
      }

      /** In CI it seems to be running fast enough that the next cursor ts and cursor0 ts are the same.
        * Adding this here to be sure we are advancing ts first.
        */
      eventually {
        val ts = evalRes(auth, "1 + 1").ts
        ts should be > cursor0.ts
      }

      val nextCursor = {
        val page = poll(stream.copy(cursor = Some(cursor0)))
        page.events shouldBe empty
        page.hasNext shouldBe false
        page.cursor
      }

      nextCursor.ts.millis should be > cursor0.ts.millis
      evalOk(auth, "Foo.all().first()!.update({ bar: 42 })")

      val page = poll(stream.copy(cursor = Some(nextCursor)))
      page.events should have size 1
      page.events(0)._1 shouldBe a[Event.Data]
      page.hasNext shouldBe false
    }

    "polling a window larger than the collection retention fails with an error" in {
      val now = Timestamp(Instant.now())
      val stream = evalStream("Foo.all().toStream()").copy(cursor =
        Some(Value.EventSource.Cursor(now - 20.minutes)))
      val page = poll(stream)
      page.events should have size 1
      val ev = page.events(0)._1
      ev match {
        case err: Event.Error =>
          err.code shouldBe Event.Error.Code.InvalidStreamStartTime
        case v =>
          fail(s"Expected error event, received $v")
      }
    }

    "error events don't rollup the page cursor" in {
      val cursor = Value.EventSource.Cursor(Clock.time - 90.days)
      val source =
        evalStream("Foo.all().eventSource()")
          .copy(cursor = Some(cursor))

      val page = poll(source)
      page.events should have size 1
      page.events(0)._1 shouldBe a[Event.Error]
      page.cursor shouldBe cursor
    }

    "poll a single page" in {
      val stream = evalStream("Foo.all().toStream()")
      evalOk(auth, "Foo.create({ bar: 42 })")

      val page = poll(stream)
      page.events should have size 1
      page.events(0)._1 shouldBe a[Event.Data]
      page.hasNext shouldBe false
    }

    "poll multiple pages" - {
      "from separate txns" in {
        val stream = evalStream("Foo.all().toStream()")
        for (i <- 1 until 10) {
          evalOk(auth, s"Foo.create({ bar: $i })")
        }
        check(stream)
      }

      "from the same txn" in {
        val stream = evalStream("Foo.all().toStream()")
        evalOk(
          auth,
          s"""|Set.sequence(1, 10).forEach(i =>
              |  Foo.create({ bar: i })
              |)
              |""".stripMargin
        )
        check(stream)
      }

      def check(stream: Value.EventSource) = {
        val page = poll(stream, pageSize = 5)
        page.hasNext shouldBe true

        val events = page.events map { _._1 }
        events should have size 5
        all(events) shouldBe a[Event.Data]

        val page0 = poll(stream.copy(cursor = Some(page.cursor)), pageSize = 5)
        page0.hasNext shouldBe false

        val events0 = page0.events map { _._1 }
        events0 should have size 4
        all(events0) shouldBe a[Event.Data]
      }
    }

    "pagination is time bound" in {
      val stream = evalStream("Foo.all().toStream()")
      evalOk(
        auth,
        """|Set
           |  .sequence(0, 1000)
           |  .forEach(n => Foo.create({ n: n }))
           |""".stripMargin
      )

      val page = poll(stream, deadline = 1.millis.bound)
      page.events.size should be < 1000
      page.hasNext shouldBe true
    }
  }

  private def poll(
    stream: Value.EventSource,
    pageSize: Int = Feed.DefaultPageSize,
    deadline: TimeBound = Feed.DefaultTimeout.bound
  ): Feed.Page =
    ctx ! new Feed(auth, stream).poll(pageSize, deadline)

  private def evalStream(query: String): Value.EventSource = {
    val res = evalRes(auth, query)
    val stream = res.value.to[Value.EventSource]
    // NB. Streams get their start time after materialized. Mock the materialization
    // process by overriding its start time here for testing.
    stream.copy(cursor = Some(Value.EventSource.Cursor(res.ts)))
  }
}
