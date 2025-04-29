package fauna.repo.service.stream.test

import fauna.atoms._
import fauna.exec._
import fauna.lang._
import fauna.lang.syntax._
import fauna.lang.clocks._
import fauna.repo.service.stream._
import fauna.repo.test._
import scala.concurrent.Promise
import scala.concurrent.duration._

class SourceSpec extends Spec {
  import FaunaExecutionContext.Implicits.global

  private var _minAppliedTS: Timestamp = _

  private def TS(n: Int) =
    Timestamp.ofSeconds(n)

  private def publish(
    source: Source[Int, Int],
    minAppliedTS: Timestamp,
    transactionTS: Timestamp,
    valuesByKey: Map[Int, Vector[Int]]) = {

    assert(_minAppliedTS <= minAppliedTS)
    _minAppliedTS = minAppliedTS
    source.publish(transactionTS, Clock.time, valuesByKey)
  }

  before {
    _minAppliedTS = Timestamp.Epoch
  }

  "Source" - {

    "publish values" in withSource() { source =>
      source.register(0)
      source.register(1)

      val eventsF = source.output.sequenceF
      publish(source, TS(0), TS(3), Map(0 -> Vector(43)))
      publish(source, TS(0), TS(1), Map(1 -> Vector(41)))
      publish(source, TS(1), TS(2), Map(1 -> Vector(42)))
      source.close()

      val events = await(eventsF)
      events(0).values should contain only 43
      events(0).lastSentTS(0) shouldBe Timestamp.Epoch
      events(1).values should contain only 41
      events(1).lastSentTS(1) shouldBe Timestamp.Epoch
      events(2).values should contain only 42
      events(2).lastSentTS(1) shouldBe TS(1)
    }

    "publish carries idle stream messages" in withSource(idlePeriod = 300.millis) {
      source =>
        source.register(0) // last sent = epoch
        source.register(1) // last sent = epoch

        val eventsF = source.output.sequenceF
        publish(source, TS(1), TS(3), Map(1 -> Vector(42))) // 0 is idle
        publish(source, TS(1), TS(2), Map(2 -> Vector(42))) // no idles
        publish(source, TS(4), TS(5), Map(2 -> Vector(42))) // idles: 0,1
        source.close()

        val events = await(eventsF)
        events(0).lastSentTS(0) shouldBe Timestamp.Epoch // idle since register
        events(1).lastSentTS(0) shouldBe TS(1) // last sent was LAT
        events(1).lastSentTS(1) shouldBe TS(3) // last sent was TXN TS
    }

    "can register stream keys" in withSource() { source =>
      source.register(0).value shouldBe Source.Index(0)
      source.register(0).value shouldBe Source.Index(1) // no re-use
      source.register(1).value shouldBe Source.Index(2)
    }

    "closed source fails to register new keys" in withSource() { source =>
      source.close()
      source.register(0) shouldBe empty
    }

    "discard values on non-subscribed source" in withSource() { source =>
      val values = Map(0 -> Vector(42), 1 -> Vector(44))
      publish(source, TS(0), TS(1), values)
      source.close()
      await(source.output.sequenceF) shouldBe empty
    }

    "can filter values based on registered keys" in withSource() { source =>
      val values = Map(0 -> Vector(42), 1 -> Vector(44))
      source.register(1)
      publish(source, TS(0), TS(1), values)
      await(source.output.firstF).value.values should contain only 44
    }

    "automatically closes after ttl" in withSource(ttl = 10.millis) { source =>
      await(source.output.firstF) shouldBe empty // closed with no values
    }

    "sync with sink hosts using index mask" in withSource() { source =>
      source.register(0)
      source.register(1)
      source.synchronize(Vector(1))
      source.keys should contain only 1
    }

    "sync keeps source's ttl" in {
      val start = Clock.time
      var stop = Timestamp.Max

      withSource(ttl = 50.millis) { source =>
        val obsF = source.output.sequenceF
        obsF ensure { stop = Clock.time }

        for (_ <- 1 to 10) {
          source.synchronize(Vector.empty)
          Thread.sleep(10)
        }

        await(obsF)
        stop.difference(start) should be >= 100.millis
      }
    }

    "closes on max buffer size" in withSource(maxBuffer = 1) { source =>
      val latch = Promise[Unit]()
      val sub = (source.output mapAsync { _ => latch.future }).sequenceF
      source.register(0)
      publish(source, TS(0), TS(1), Map(0 -> Vector(42))) // dispatches
      publish(source, TS(1), TS(2), Map(0 -> Vector(42))) // buffer
      publish(source, TS(2), TS(3), Map(0 -> Vector(42))) // overflows
      latch.setDone()
      a[Publisher.QueueFull] should be thrownBy { await(sub) }
    }
  }

  "Manager" - {

    "forward results on new source" in {
      var forwardCalled = 0
      withManager((_, _, _) => forwardCalled += 1) { manager =>
        val generation = Sink.Generation.next()
        val hostID = HostID.randomID
        manager.register(hostID, generation, 0)
        manager.register(hostID, generation, 0) // reuse preview source
      }
      forwardCalled shouldBe 1
    }

    "forward results on recreated source" in {
      var forwardCalled = 0

      @annotation.nowarn("cat=unused-params")
      def forward(h: HostID, g: Sink.Generation, source: Source[_, Int]): Unit = {
        forwardCalled += 1
        source.close()
      }

      withManager(forward) { manager =>
        val hostID = HostID.randomID
        val generation = Sink.Generation.next()
        manager.register(hostID, generation, 0)
        manager.register(hostID, generation, 1)
      }

      forwardCalled shouldBe 2
    }
  }

  private def withSource[A](
    maxBuffer: Int = Int.MaxValue,
    ttl: Duration = Duration.Inf,
    idlePeriod: Duration = Duration.Inf,
    maxIdleOverhead: Int = Int.MaxValue
  )(fn: Source[Int, Int] => A): A = {

    val source =
      new Source[Int, Int](
        () => _minAppliedTS,
        maxBuffer,
        ttl,
        idlePeriod,
        maxIdleOverhead
      )

    try {
      fn(source)
    } finally {
      source.close()
    }
  }

  private def withManager[A](
    forward: Source.ForwardFn[Int, Int],
    maxBuffer: Int = Int.MaxValue,
    ttl: Duration = Duration.Inf,
    idlePeriod: Duration = Duration.Inf,
    maxIdleOverhead: Int = Int.MaxValue
  )(fn: Source.Manager[Int, Int] => A): A = {

    val manager =
      new Source.Manager[Int, Int](
        () => _minAppliedTS,
        forward,
        maxBuffer,
        ttl,
        idlePeriod,
        maxIdleOverhead
      )

    try {
      fn(manager)
    } finally {
      manager.close()
    }
  }
}
