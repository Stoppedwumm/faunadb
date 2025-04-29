package fauna.repo.service.stream.test

import fauna.atoms._
import fauna.exec._
import fauna.lang._
import fauna.lang.clocks._
import fauna.repo.service.stream._
import fauna.repo.test._
import fauna.storage._
import fauna.storage.ir._
import fauna.storage.ops._
import scala.concurrent.duration._
import scala.concurrent.Await

class StreamMergerSpec extends Spec {
  import StreamMerger._

  implicit val ec = ImmediateExecutionContext

  def TS(ts: Long) = Timestamp.ofMicros(ts)

  def awaitObs[A](obs: Observable[A]): Seq[A] =
    Await.result(obs.sequenceF, 5.seconds)

  private val scope = ScopeID(1)
  private val index = IndexID(1025)
  private val streamKey = StreamKey.SetKey(scope, index, Vector.empty)

  private val termsA0 = Vector(LongV(0))
  private val termsA1 = Vector(LongV(1))
  private val termsB2 = Vector(LongV(2))

  private val keyA0 = StreamKey.SetKey(scope, index, termsA0)
  private val keyA1 = StreamKey.SetKey(scope, index, termsA1)
  private val keyB2 = StreamKey.SetKey(scope, index, termsB2)

  private val doc = DocID(SubID(1026), CollectionID(1025))
  private val write =
    SetAdd(scope, index, Vector.empty, Vector.empty, doc, Unresolved, Add)

  private val writeA0 = write.copy(terms = termsA0)
  private val writeA1 = write.copy(terms = termsA1)
  private val writeB2 = write.copy(terms = termsB2)

  var mergers: StreamMerger = _

  before {
    mergers = new StreamMerger(
      maxBufferSize = 4,
      maxMergeQueueSize = 8,
      idlePeriod = 50.nanos
    )
  }

  after {
    mergers.close()
  }

  def input(
    key: StreamKey.SetKey,
    txnTS: Timestamp,
    writes: Vector[Write] = Vector.empty) =
    Input(key, TxnResult(txnTS, Clock.time, writes))

  "StreamMerger" - {

    "start the merged stream" in {
      val (pubA, eventsA) = Observable.gathering[StreamMerger.Input]()
      val (pubB, eventsB) = Observable.gathering[StreamMerger.Input]()

      val obs =
        mergers
          .register(streamKey, Set(eventsA, eventsB))
          .firstF

      pubA.publish(input(keyA1, TS(2)))
      pubB.publish(input(keyB2, TS(3)))

      val merged = await(obs)
      merged.value.txnTS shouldBe TS(3) // highiest txn ts
    }

    "publishes merged events" in {
      val (pubA0, eventsA0) = Observable.gathering[StreamMerger.Input]()
      val (pubA1, eventsA1) = Observable.gathering[StreamMerger.Input]()
      val (pubB2, eventsB2) = Observable.gathering[StreamMerger.Input]()

      val obs =
        mergers
          .register(streamKey, Set(eventsA0, eventsA1, eventsB2))
          .takeF(2)

      pubA0.publish(input(keyA0, TS(1), Vector.empty)) // idle notification
      pubA1.publish(input(keyA1, TS(1), Vector.empty)) // idle notification
      pubB2.publish(input(keyB2, TS(1), Vector.empty)) // idle notification

      pubA0.publish(input(keyA0, TS(2), Vector(writeA0))) // txn to merge
      pubA1.publish(input(keyA1, TS(2), Vector(writeA1))) // txn to merge
      pubB2.publish(input(keyB2, TS(2), Vector(writeB2))) // txn to merge

      pubA0.publish(input(keyA0, TS(3), Vector.empty)) // idle notification
      pubA1.publish(input(keyA1, TS(3), Vector.empty)) // idle notification
      pubB2.publish(input(keyB2, TS(3), Vector.empty)) // idle notification

      val merged = await(obs)
      merged should have size 2 // start event + 1 merged txns
      merged(0).txnTS shouldBe TS(1) // start event
      merged(0).writes shouldBe empty // always empty
      merged(1).txnTS shouldBe TS(2) // merged between all keys in 2 hosts
      merged(1).writes should contain.only(writeA0, writeA1, writeB2)
    }

    "discards txn before start time" in {
      val (pubA0, eventsA0) = Observable.gathering[StreamMerger.Input]()
      val (pubB2, eventsB2) = Observable.gathering[StreamMerger.Input]()

      val obs =
        mergers
          .register(
            streamKey,
            Set(
              eventsA0,
              eventsB2
            ))
          .takeF(2)

      pubA0.publish(input(keyA0, TS(1), Vector.empty)) // idle notification
      pubA0.publish(input(keyA0, TS(2), Vector(writeA0))) // txn to discard
      pubB2.publish(input(keyB2, TS(2), Vector.empty)) // idle notification

      pubA0.publish(input(keyA0, TS(3), Vector(writeA1))) // txn to merge
      pubB2.publish(input(keyB2, TS(3), Vector(writeB2))) // txn to merge

      pubA0.publish(input(keyA0, TS(4), Vector.empty)) // idle notification
      pubB2.publish(input(keyB2, TS(4), Vector.empty)) // idle notification

      val merged = await(obs)
      merged should have size 2 // start event + 1 merged txns
      merged(0).txnTS shouldBe TS(2) // start event
      merged(0).writes shouldBe empty // always empty
      merged(1).txnTS shouldBe TS(3) // merged between all keys in 2 hosts
      merged(1).writes should contain.only(writeA1, writeB2)
    }

    "produces idle events" in {
      val (pubA0, eventsA0) = Observable.gathering[StreamMerger.Input]()
      val (pubB2, eventsB2) = Observable.gathering[StreamMerger.Input]()

      val obs =
        mergers
          .register(
            streamKey,
            Set(
              eventsA0,
              eventsB2
            ))
          .takeF(2)

      // Start the merged stream
      pubA0.publish(input(keyA0, TS(1), Vector.empty)) // idle notification
      pubB2.publish(input(keyB2, TS(2), Vector.empty)) // idle notification

      // Everybody is idle
      pubA0.publish(input(keyA0, TS(3), Vector.empty)) // idle notification
      pubB2.publish(input(keyB2, TS(3), Vector.empty)) // idle notification

      val merged = await(obs)
      merged should have size 2 // start event + 1 idle event
      merged(0).txnTS shouldBe TS(2) // start event
      merged(0).writes shouldBe empty // always empty
      merged(1).txnTS shouldBe TS(3) // idle event
      merged(1).writes shouldBe empty // always empty
    }

    "closes merged stream under pressure on start" in {
      val (pubA0, eventsA0) = Observable.gathering[StreamMerger.Input]()
      val (_, eventsB2) = Observable.gathering[StreamMerger.Input]()

      val obs =
        mergers
          .register(
            streamKey,
            Set(
              eventsA0,
              eventsB2
            ))
          .takeF(1)

      for (i <- 1 to 10) {
        pubA0.publish(input(keyA0, TS(i), Vector(writeA1))) // txn to merge
      }

      a[StreamMerger.SyncTimeout.type] shouldBe thrownBy { await(obs) }
    }

    "closes merged stream under pressure on merge" in {
      val (pubA0, eventsA0) = Observable.gathering[StreamMerger.Input]()
      val (pubB2, eventsB2) = Observable.gathering[StreamMerger.Input]()

      val obs =
        mergers
          .register(
            streamKey,
            Set(
              eventsA0,
              eventsB2
            ))
          .takeF(3)

      // Start the stream
      pubA0.publish(input(keyA0, TS(1), Vector.empty)) // idle notification
      pubB2.publish(input(keyB2, TS(2), Vector.empty)) // idle notification

      for (i <- 2 to 10) {
        pubA0.publish(input(keyA0, TS(i), Vector(writeA1))) // txn to merge
      }

      a[StreamMerger.SyncTimeout.type] shouldBe thrownBy { await(obs) }
    }
  }
}
