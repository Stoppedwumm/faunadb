package fauna.exec.test

import fauna.exec._
import fauna.lang.syntax._
import scala.concurrent.{ Await, Future, Promise }
import scala.concurrent.duration._

class ObservableSpec extends Spec {

  implicit val ec = ImmediateExecutionContext

  def await[A](f: Future[A]): A = Await.result(f, Duration.Zero)

  "Observable" - {

    "can observe an iterable" in {
      val obs = Observable.from(Seq(1, 2, 3))
      var seen = Seq.empty[Int]

      obs.subscribe(new Observer.Default[Int] {
        override def onNext(value: Int) = {
          seen :+= value
          Observer.ContinueF
        }
      })

      seen should contain.only(1, 2, 3)
    }

    "can stop an iterable subscription" in {
      val obs = Observable.from(Seq(1, 2, 3))
      var seen = Seq.empty[Int]

      obs.subscribe(new Observer.Default[Int] {
        override def onNext(value: Int) = {
          seen :+= value
          Observer.StopF
        }
      })

      seen should contain.only(1)
    }

    "notifies listeners on completion" in {
      val obs = Observable.from(Seq(1, 2, 3))
      val completed = Promise[Unit]()

      obs.subscribe(new Observer.Default[Int] {
        override def onComplete() =
          completed.setDone()
      })

      completed.isCompleted shouldBe true
    }

    "notifies listeners on error" in {
      val obsP =
        new Observer.ObsPromise[Int, Unit] {
          def onNext(value: Int) = Future.failed(new IllegalArgumentException)
          def onComplete() = promise.trySuccess(())
        }

      Observable.from(Seq(1, 2, 3)).subscribe(obsP)
      an[IllegalArgumentException] shouldBe thrownBy { await(obsP.future) }
    }

    "catches errors on value processing" in {
      val obsP =
        new Observer.ObsPromise[Int, Throwable] {
          def onNext(value: Int) = throw new IllegalArgumentException
          def onComplete() = promise.trySuccess(new IllegalStateException)
        }

      Observable.from(Seq(1, 2, 3)).subscribe(obsP)
      an[IllegalArgumentException] shouldBe thrownBy { await(obsP.future) }
    }

    "can cancel an iterable subscription" in {
      val obs = Observable.from(Seq(1, 2, 3))
      val next = Promise[Observer.Ack]()
      var seen = Seq.empty[Int]

      val obsP =
        new Observer.ObsPromise[Int, Unit] {
          def onNext(value: Int) = { seen :+= value; next.future }
          def onComplete() = promise.trySuccess(())
        }

      val sub = obs.subscribe(obsP)
      sub.cancel()

      next.success(Observer.Continue)
      an[Cancelable.Canceled] should be thrownBy { await(obsP.future) }
      seen should contain.only(1)
    }

    "can get first element" in {
      await(Observable.empty.firstF) shouldBe None
      await(Observable.single(1).firstF) shouldBe Some(1)
    }

    "can consume the whole observable" in {
      await(Observable.empty.sequenceF) shouldBe empty
      await(Observable.from(Seq(1, 2, 3)).sequenceF) should
        contain.inOrderOnly(1, 2, 3)
    }

    "can take n values from the observable" in {
      await(Observable.empty.takeF(3)) shouldBe empty
      await(Observable.from(Seq(1, 2, 3)).takeF(3)) should
        contain.inOrderOnly(1, 2, 3)
      await(Observable.from(Seq(1, 2, 3, 4)).takeF(3)) should
        contain.inOrderOnly(1, 2, 3)

      val obs =
        Observable.from(Seq(1, 3)) flatMap { n =>
          Observable.from(Seq(n, n + 1))
        }
      await(obs.takeF(2)) should contain.inOrderOnly(1, 2)
    }

    "can fold values" in {
      await(Observable.empty[Int].foldLeftF(0) { _ + _ }) shouldBe 0
      await(Observable.from(Seq(1, 2, 3)).foldLeftF(0) { _ + _ }) shouldBe 6
    }

    "can run function for each value" in {
      val values = Seq.newBuilder[Int]
      await(Observable.from(Seq(1, 2, 3)) foreachF { values += _ })
      values.result() should contain.inOrderOnly(1, 2, 3)
    }

    "can run function for each value asynchronously" in {
      val values = Seq.newBuilder[Int]
      await(Observable.from(Seq(1, 2, 3)) foreachAsyncF { n =>
        values += n
        Future.unit
      })
      values.result() should contain.inOrderOnly(1, 2, 3)
    }

    "can run function after observable is completed" in {
      val done0 = Promise[Unit]()
      await(Observable.single(1) ensure { done0.setDone() } firstF) shouldBe Some(1)
      done0.isCompleted shouldBe true

      val done1 = Promise[Unit]()
      val obs = Observable.single(1)
      val err = obs.map[Int] { _ => throw new IllegalArgumentException() }
      val end = err ensure { done1.setDone() }
      an[IllegalArgumentException] should be thrownBy { await(end.firstF) }
      done1.isCompleted shouldBe true
    }

    "can map values" in {
      val obs = Observable.from(Seq(1, 2, 3)) map { _ * 2 }
      await(obs.sequenceF) should contain.inOrderOnly(2, 4, 6)
    }

    "can map values asynchronously" in {
      val obs = Observable.from(Seq(1, 2, 3)) mapAsync { n => Future(n * 2) }
      await(obs.sequenceF) should contain.inOrderOnly(2, 4, 6)
    }

    "can flatMap values" in {
      val obs = Observable.from(Seq(1, 2)) flatMap { n =>
        Observable.from(Seq(n, n + 1))
      }
      await(obs.sequenceF) should contain theSameElementsInOrderAs Seq(1, 2, 2, 3)
    }

    "can stop a flatMap" in {
      val obs =
        Observable
          .from(Seq(1, 2))
          .flatMap { n => Observable.single(n * 2) }
          .transform[Int] { (observer, n) =>
            observer.onNext(n).unit before Observer.StopF
          }

      await(obs.sequenceF) should contain only 2
    }

    "can flatMap values asynchronously" in {
      val obs = Observable.from(Seq(1, 2)) flatMapAsync { n =>
        Future(Observable.from(Seq(n, n + 1)))
      }
      await(obs.sequenceF) should contain theSameElementsInOrderAs Seq(1, 2, 2, 3)
    }

    "can stop a flatMapAsync" in {
      val obs =
        Observable
          .from(Seq(1, 2))
          .flatMapAsync { n => Future.successful(Observable.single(n * 2)) }
          .transform[Int] { (observer, n) =>
            observer.onNext(n).unit before Observer.StopF
          }

      await(obs.sequenceF) should contain only 2
    }

    "can select values" in {
      val obs = Observable.from(Seq(1, 2, 3, 4)) select { _ % 2 == 0 }
      await(obs.sequenceF) should contain.inOrderOnly(2, 4)
    }

    "can select values asynchronously" in {
      val obs = Observable.from(Seq(1, 2, 3, 4)) selectAsync { n =>
        Future.successful(n % 2 == 0)
      }
      await(obs.sequenceF) should contain.inOrderOnly(2, 4)
    }

    "can recover asynchronously" in {
      val obs =
        Observable.failed[Int](new IllegalArgumentException()) recoverAsync {
          case _: IllegalArgumentException =>
            Future.successful(Observable.single(42))
        }
      await(obs.firstF).value shouldBe 42
    }

    "recover propagates initial error if no match" in {
      val obs =
        Observable.failed[Int](new IllegalArgumentException()) recoverAsync {
          case _: IllegalStateException => // wrong catch
            Future.successful(Observable.single(42))
        }
      an[IllegalArgumentException] should be thrownBy { await(obs.firstF) }
    }

    "can fail to recover" in {
      val obs =
        Observable.failed[Int](new IllegalArgumentException()) recoverAsync {
          case _: IllegalArgumentException =>
            Future.failed(new IllegalStateException()) // failed to recover
        }
      an[IllegalStateException] should be thrownBy { await(obs.firstF) }
    }

    "can concate observables" in {
      val s1 = Observable.from(Seq(1, 2, 3))
      val s2 = Observable.from(Seq(4, 5, 6))
      await(s1.concat(s2).sequenceF) should contain.inOrderOnly(1, 2, 3, 4, 5, 6)
    }

    "can batch values" in {
      val latch = Promise[Unit]()
      val seqs =
        Observable
          .from(Seq(1, 2, 3, 4, 5, 6))
          .batched(2)
          .mapAsync { vs =>
            latch.future map { _ => vs }
          }
          .sequenceF

      latch.setDone()
      await(seqs) should contain.inOrderOnly(Seq(1), Seq(2, 3), Seq(4, 5), Seq(6))
    }
  }

  "Publisher" - {

    "can publish values" in {
      val obs =
        Observable.create { pub: Publisher[Int] =>
          pub.publish(1)
          pub.publish(2)
          pub.publish(3)
          pub.close()
          Cancelable.empty
        }
      await(obs.sequenceF) should contain.inOrderOnly(1, 2, 3)
    }

    "can close a publisher with an error" in {
      val obs =
        Observable.create { pub: Publisher[Int] =>
          pub.fail(new IllegalArgumentException())
          Cancelable.empty
        }
      an[IllegalArgumentException] should be thrownBy { await(obs.sequenceF) }
    }

    "gathering accumulate values until observed" in {
      val (pub, obs) = Observable.gathering[Int](OverflowStrategy.unbounded)
      pub.publish(1)
      pub.publish(2)
      pub.publish(3)
      pub.close()
      await(obs.sequenceF) should contain.inOrderOnly(1, 2, 3)
    }

    "close if buffer is full" in {
      val (pub, obs) = Observable.gathering[Int](OverflowStrategy.bounded(1))
      pub.publish(1)
      pub.publish(2)
      pub.publish(3)
      an[Publisher.QueueFull] should be thrownBy { await(obs.sequenceF) }
    }

    "can handle overflowed values" in {
      val values = Seq.newBuilder[Int]
      val (pub, obs) =
        Observable.gathering[Int](OverflowStrategy.callbackOnOverflow(1) {
          values += _
        })
      pub.publish(1)
      pub.publish(2)
      pub.publish(3)
      an[Publisher.QueueFull] should be thrownBy { await(obs.sequenceF) }
      values.result() should contain.inOrderOnly(2, 3)
    }
  }
}
