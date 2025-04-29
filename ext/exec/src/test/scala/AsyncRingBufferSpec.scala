package fauna.exec.test

import fauna.exec.{
  AsyncRingBuffer,
  Cancelable,
  FaunaExecutionContext,
  ImmediateExecutionContext,
  Observer,
  Publisher,
  Timer
}
import java.util.concurrent.CountDownLatch
import scala.concurrent.{ Await, Future, Promise }
import scala.concurrent.duration._

// TODO: test .close(err)
class AsyncRingBufferSpec extends Spec {

  implicit val ec = ImmediateExecutionContext

  var ring: AsyncRingBuffer[Int] = _

  before { ring = new AsyncRingBuffer(16) }
  after { ring.close() }

  "AsyncRingBuffer" - {
    "can publish values" in {
      val values = ring.sequenceF
      ring.publish(0)
      ring.publish(1)
      ring.publish(2)
      ring.publish(3)
      ring.close()

      await(values) should contain.inOrderOnly(0, 1, 2, 3)
    }

    "supports multiple watchers" in {
      val values0 = ring.sequenceF
      ring.publish(0)
      ring.publish(1)

      val values1 = ring.sequenceF
      ring.publish(2)
      ring.publish(3)
      ring.close()

      await(values0) should contain.inOrderOnly(0, 1, 2, 3)
      await(values1) should contain.inOrderOnly(2, 3)
    }

    "close laggers" in {
      val handler = Promise[Observer.Ack]()
      val obs =
        new Observer.ObsPromise[Int, Unit] {
          def onNext(value: Int) = handler.future
          def onComplete() = fail()
        }

      ring.subscribe(obs)
      // 1st value is pushed and it's waiting completion behind the handler future.
      // 2nd to 17th values are in the buffer. 18th value out run the consumer.
      for (i <- 1 to 18) ring.publish(i)

      handler.success(Observer.Continue)
      a[Publisher.QueueFull] shouldBe thrownBy {
        await(obs.future)
      }
    }

    "cancel subscriber" in {
      val handler = Promise[Observer.Ack]()
      val obs =
        new Observer.ObsPromise[Int, Unit] {
          def onNext(value: Int) = handler.future
          def onComplete() = fail()
        }

      val sub = ring.subscribe(obs)
      ring.publish(0)
      sub.cancel()

      handler.success(Observer.Continue)
      a[Cancelable.Canceled] shouldBe thrownBy {
        await(obs.future)
      }
    }

    "closes with an error" in {
      val values0 = ring.sequenceF
      ring.close(new IllegalStateException)
      an[IllegalStateException] shouldBe thrownBy { await(values0) }

      val values1 = ring.sequenceF
      an[IllegalStateException] shouldBe thrownBy { await(values1) }
    }

    "maintains ordering" in {
      @volatile var id = 0
      Timer.Global.scheduleRepeatedly(1.millis, !ring.isClosed) {
        ring.publish(id)
        id += 1
      }

      val watchers = Seq.newBuilder[Future[Seq[Int]]]
      val latch = new CountDownLatch(64)

      Timer.Global.scheduleRepeatedly(10.millis, latch.getCount > 0) {
        watchers += ring.takeF(32)(FaunaExecutionContext.Implicits.global)
        latch.countDown()
      }

      latch.await()

      for {
        values <- await(Future.sequence(watchers.result()))
        i      <- 0 to 30
      } {
        withClue(s"values: $values ($i)") {
          (values(i) + 1) shouldBe values(i + 1)
        }
      }
    }
  }

  private def await[A](f: Future[A]): A =
    Await.result(f, 30.seconds)
}
