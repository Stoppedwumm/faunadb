package fauna.repo.service.stream.test

import fauna.atoms._
import fauna.exec._
import fauna.lang._
import fauna.repo.service.stream._
import fauna.repo.test._
import scala.concurrent.{ Future, TimeoutException }
import scala.util.Random

class SinkSpec extends Spec {
  import FaunaExecutionContext.Implicits.global

  private var sink: Sink[Int, Int] = _
  private var hostID: HostID = _
  private var gen: Sink.Generation = _
  private var idx: Source.Index = _
  private var subscribeCalled: Int = _
  private var streamKey: Int = _

  before {
    streamKey = 0
    sink = new Sink[Int, Int](streamKey, maxBufferSize = 4)
    hostID = HostID.randomID
    gen = Sink.Generation.next()
    idx = Source.Index(Random.nextInt(100))
    subscribeCalled = 0
  }

  after {
    sink.close()
  }

  "Sink" - {

    "receives values" in {
      val first = sink.watch().firstF
      sink.receive(Timestamp.Min, Timestamp.ofSeconds(1), 42)
      await(first).value shouldBe 42
    }

    "failing to subscribe fail registered publishers" in {
      val first = sink.watch().firstF
      sink.close(new IllegalStateException())
      an[IllegalStateException] should be thrownBy { await(first) }
    }

    "failed subscription continues to fail publishers" in {
      sink.close(new IllegalStateException())
      val first = sink.watch().firstF
      an[IllegalStateException] should be thrownBy { await(first) }
    }

    "canceled observalbes decrement sink's ref count" in {
      sink.refCnt shouldBe 1

      sink
        .watch()
        .subscribe(new Observer.Default[Int] {})
        .cancel()

      eventually {
        sink.refCnt shouldBe 0
      }
    }

    "fail on missing messages" in {
      val values = sink.watch().sequenceF
      sink.receive(Timestamp.ofSeconds(1), Timestamp.ofSeconds(2), 42)
      sink.receive(Timestamp.ofSeconds(3), Timestamp.ofSeconds(4), 42)
      the[IllegalStateException] thrownBy await(values) should
        have message "Closed due to missing or out-of-order event."
    }

    "fail on out-of-order messages" in {
      val values = sink.watch().sequenceF
      sink.receive(Timestamp.ofSeconds(2), Timestamp.ofSeconds(3), 42)
      sink.receive(Timestamp.ofSeconds(1), Timestamp.ofSeconds(2), 42)
      the[IllegalStateException] thrownBy await(values) should
        have message "Closed due to missing or out-of-order event."
    }
  }

  "Manager" - {

    @annotation.nowarn("cat=unused-params")
    def subscribe(key: Int, hostID: HostID, generation: Sink.Generation) = {
      subscribeCalled += 1
      Future.successful((Timestamp.Epoch, idx))
    }

    "subscribe on new sink" in {
      withManager(subscribe) { manager =>
        manager.register(0).sequenceF
        subscribeCalled shouldBe 1
      }
    }

    "closes sink on failed subscription" in {
      @annotation.nowarn("cat=unused-params")
      def failedSub(key: Int, hostID: HostID, generation: Sink.Generation) = {
        Future.failed(new IllegalStateException())
      }
      withManager(failedSub) { manager =>
        val obs = manager.register(0).sequenceF
        an[IllegalStateException] should be thrownBy { await(obs) }
      }
    }

    "re-subscribe after closing sink" in {
      withManager(subscribe) { manager =>
        eventually {
          manager
            .register(0)
            .subscribe(new Observer.Default[Int]() {
              override def onError(cause: Throwable) = ()
            })
            .cancel()
          subscribeCalled should be > 1
        }
      }
    }

    "dispatch received value" in {
      withManager(subscribe) { manager =>
        val values = manager.register(0).takeF(1)
        manager.receive(
          hostID,
          gen,
          Timestamp.Min,
          Timestamp.ofSeconds(1),
          streamKey,
          42)
        await(values) should contain only 42
      }
    }

    "do not block when receiving on a closed sink" in {
      withManager(subscribe) { manager =>
        manager.register(0).takeF(1) // closes after 1st event
        manager.receive(
          hostID,
          gen,
          Timestamp.ofSeconds(1),
          Timestamp.ofSeconds(2),
          streamKey,
          42
        )
        noException shouldBe thrownBy {
          manager.receive(
            hostID,
            gen,
            Timestamp.ofSeconds(2),
            Timestamp.ofSeconds(3),
            streamKey,
            42
          ) // sink already closed
        }
      }
    }

    "synchronizes with sources" in {
      var sentMask = Vector.empty[Long]
      var i = 0

      @annotation.nowarn("cat=unused-params")
      def subscribe(key: Int, hostID: HostID, generation: Sink.Generation) = {
        val res = Future.successful((Timestamp.Epoch, Source.Index(i)))
        i += 1
        res
      }

      @annotation.nowarn("cat=unused-params")
      def sync(
        sourceHostID: HostID,
        gen: Sink.Generation,
        mask: Vector[Long]): Future[Unit] = {

        sentMask = mask
        Future.unit
      }

      withManager(subscribe, sync) { manager =>
        manager
          .register(0)
          .subscribe(new Observer.Default[Int]() {
            override def onError(cause: Throwable) = ()
          })
          .cancel()

        // Needs eventually since ENG-5756 made this racy.
        eventually {
          manager.receive(
            hostID,
            gen,
            Timestamp.ofSeconds(1),
            Timestamp.ofSeconds(2),
            streamKey,
            42
          )
          manager.synchronize()
          sentMask shouldBe Vector(1) // 0001
        }
      }
    }

    "closes sink on synchronization failures" in {
      @annotation.nowarn("cat=unused-params")
      def subscribe(key: Int, hostID: HostID, generation: Sink.Generation) =
        Future.successful((Timestamp.Epoch, Source.Index(0)))

      @annotation.nowarn("cat=unused-params")
      def sync(
        sourceHostID: HostID,
        gen: Sink.Generation,
        mask: Vector[Long]): Future[Unit] =
        Future.failed(new TimeoutException())

      withManager(subscribe, sync) { manager =>
        val sub = manager.register(0).sequenceF
        manager.synchronize()
        an[Sink.ClosedBySource] should be thrownBy { await(sub) }
      }
    }
  }

  private def withManager[A](
    subscribe: Sink.SubscribeFn[Int],
    sync: Sink.SyncFn = (_, _, _) => Future.never,
    maxBuffer: Int = 16)(fn: Sink.Manager[Int, Int] => A): A = {

    @annotation.nowarn("cat=unused-params")
    def locate(key: Int): HostID = hostID
    val manager = new Sink.Manager[Int, Int](locate, subscribe, sync, maxBuffer, gen)

    try {
      fn(manager)
    } finally {
      manager.close()
    }
  }
}
