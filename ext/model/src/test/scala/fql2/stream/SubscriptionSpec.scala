package fauna.model.test

import fauna.auth.Auth
import fauna.exec.{ ImmediateExecutionContext, Publisher }
import fauna.exec.Observer
import fauna.lang.clocks.TestClock
import fauna.lang.Timestamp
import fauna.model.runtime.stream.{ Event, Subscription }
import fauna.repo.service.stream.TxnResult
import fauna.repo.values.Value
import java.time.Instant
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

class SubscriptionSpec extends FQL2Spec with StreamContextHelpers {

  implicit val ec = ImmediateExecutionContext

  var auth: Auth = _

  before {
    auth = newDB
    mkColl(auth, "Foo")
  }

  "Subscription" - {
    "idle events" - {
      "sends periodic idle events with increasing txnTS collection stream" in {
        val stream = evalStream(auth, "Foo.all().toStream()")
        val subscription = new Subscription(
          auth,
          streamCtx,
          stream,
          statusEventInterval = Duration.Zero
        )
        val eventsF = subscription.subscribe().takeF(5)
        val events = await(eventsF).map(_._1.asInstanceOf[Event.Status])

        events(1).ts should be > events.head.ts
        events(2).ts should be > events(1).ts
        events(3).ts should be > events(2).ts
        events(4).ts should be > events(3).ts
      }

      "sends periodic idle events with increasing txnTS document stream" in {
        evalOk(auth, "Foo.create({})")
        val stream =
          evalStream(auth, "Set.single(Foo.all().first()!).toStream()")
        val subscription = new Subscription(
          auth,
          streamCtx,
          stream,
          statusEventInterval = Duration.Zero
        )
        val eventsF = subscription.subscribe().takeF(5)
        val events = await(eventsF).map(_._1.asInstanceOf[Event.Status])

        events(1).ts should be > events.head.ts
        events(2).ts should be > events(1).ts
        events(3).ts should be > events(2).ts
        events(4).ts should be > events(3).ts
      }

      "setting a statusEventInterval > MVTOffset fails" in {
        evalOk(auth, "Foo.create({})")
        val stream =
          evalStream(auth, "Set.single(Foo.all().first()!).toStream()")
        intercept[IllegalStateException] {
          new Subscription(
            auth,
            streamCtx,
            stream,
            statusEventInterval = 16.minutes
          )
        }
      }

      "idle events include ops from filtered events" in {
        val stream =
          evalStream(auth, """Foo.all().where(.name == "nofilter").toStream()""")
        val subscription = new Subscription(
          auth,
          streamCtx,
          stream,
          statusEventInterval = 5.seconds
        )
        evalOk(
          auth,
          """
          |Foo.create({})
          |Foo.create({})
          |Foo.create({})
          """.stripMargin)

        val events = await(subscription.subscribe().takeF(2))
        val metrics = events(1)._2
        metrics.readOps shouldEqual 3
        metrics.computeOps shouldEqual 3
      }

      "idle events include accrued compute ops for keeping the stream open" in {
        val stream =
          evalStream(auth, """Foo.all().toStream()""")
        val clock = new TestClock(Timestamp(Instant.now))
        val pub = mockService()
        val subscription = new Subscription(
          auth,
          streamCtx,
          stream,
          statusEventInterval = 1.seconds,
          streamClock = clock
        )

        var events: Seq[(Event, Event.Metrics)] = Seq.empty

        val sub = subscription
          .subscribe()
          .map { res =>
            if (events.isEmpty) {
              clock.advance(1.minute)
            } else {
              clock.advance(1.second)
            }
            res
          }

        val observer =
          new Observer[(Event, Event.Metrics)] {

            override implicit val ec: ExecutionContext = ImmediateExecutionContext

            def onComplete() = ()
            def onNext(value: (Event, Event.Metrics)): Future[Observer.Ack] = {
              events = events :+ value
              Observer.ContinueF
            }
            def onError(cause: Throwable): Unit = throw cause
          }

        val cancelable = sub.subscribe(observer)

        pub.publish(
          TxnResult(
            Timestamp(Instant.now()),
            Timestamp(Instant.now()),
            Vector.empty
          ))

        eventually {
          pub.publish(
            TxnResult(
              Timestamp(Instant.now()),
              Timestamp(Instant.now()),
              Vector.empty
            ))
          events.length should be > 1
          events.last._2.computeOps shouldEqual 1
        }

        cancelable.cancel()
      }
    }

    "error handler" - {
      "stream overflow" in {
        val publisher = mockService()
        val stream = evalStream(auth, "Foo.all().toStream()")
        val subscription = new Subscription(auth, streamCtx, stream)

        publisher.fail(Publisher.QueueFull(-1))
        val (error, _) = await(subscription.subscribe().firstF).value

        error shouldBe Event.Error(
          Event.Error.Code.StreamOverflow,
          "Too many events to process."
        )
      }

      "internal error" in {
        val publisher = mockService()
        val stream = evalStream(auth, "Foo.all().toStream()")
        val subscription = new Subscription(auth, streamCtx, stream)

        publisher.fail(new RuntimeException("Oops"))
        val (error, _) = await(subscription.subscribe().firstF).value

        error shouldBe Event.Error(
          Event.Error.Code.InternalError,
          "Oops. Please create a ticket at https://support.fauna.com"
        )
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

}
