package fauna.tx.test

import fauna.lang.syntax._
import fauna.tx.log._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

class LogStoreSpec extends Spec {
  val entries = (1 to 10) map { i => LogEntry(TX(i), s"entry $i") }

  "BinaryLogStore" - {
    testStore(BinaryLogStore.open(aTestDir(), None, fileSize = 1))
  }

  def testStore(store: LogStore[TX, String]) = {
    "initial state" in {
      store.prevIdx should equal (TX.MinValue)
      store.lastIdx should equal (TX.MinValue)
      store.flushedLastIdx should equal (TX.MinValue)
      store.uncommittedLastIdx should equal (TX.MinValue)
      store.entries(TX.MinValue) releaseAfter { es =>
        es.toSeq should equal (Nil)
      }
    }

    "add entries" in {
      1 to 10 foreach { i => store.add(Seq(s"entry $i")) should equal (TX(i)) }

      store.prevIdx should equal (TX.MinValue)
      store.lastIdx should equal (TX.MinValue)
      store.flushedLastIdx should equal (TX.MinValue)
      store.uncommittedLastIdx should equal (TX(10))

      store.entries(TX.MinValue) releaseAfter { es =>
        es.size should equal (0)
      }

      entries.tails.zipWithIndex foreach {
        case (es, i) =>
          store.uncommittedEntries(TX(i)) releaseAfter { es2 =>
            es2.toSeq should equal (es)
          }
      }
    }

    "flush" in {
      store.flush()

      store.prevIdx should equal (TX.MinValue)
      store.lastIdx should equal (TX.MinValue)
      store.flushedLastIdx should equal (TX(10))
      store.uncommittedLastIdx should equal (TX(10))
    }

    "commit entries" in {
      store.updateCommittedIdx(TX(8))

      store.prevIdx should equal (TX.MinValue)
      store.lastIdx should equal (TX(8))
      store.flushedLastIdx should equal (TX(10))
      store.uncommittedLastIdx should equal (TX(10))

      (entries take 8).tails.zipWithIndex foreach {
        case (es, i) =>
          store.entries(TX(i)) releaseAfter { es2 =>
            es2.toSeq should equal (es)
          }
      }
    }

    "truncate" in {
      store.flush()
      store.truncate(TX(2))

      store.prevIdx should equal (TX(2))
      store.lastIdx should equal (TX(8))
      store.flushedLastIdx should equal (TX(10))
      store.uncommittedLastIdx should equal (TX(10))

      store.entries(TX.MinValue) releaseAfter { es => es.toSeq should equal (entries.slice(2, 8)) }
      store.uncommittedEntries(TX.MinValue) releaseAfter { es => es.toSeq should equal (entries drop 2) }
    }

    "discard" in {
      store.discard(TX(8))

      store.prevIdx should equal (TX(2))
      store.lastIdx should equal (TX(8))
      store.flushedLastIdx should equal (TX(8))
      store.uncommittedLastIdx should equal (TX(8))

      store.entries(TX.MinValue) releaseAfter { es => es.toSeq should equal (entries.slice(2, 8)) }
    }

    "bad args" in {
      an[IllegalArgumentException] should be thrownBy store.updateCommittedIdx(TX(9))

      an[IllegalArgumentException] should be thrownBy store.truncate(TX(11))
      an[IllegalArgumentException] should be thrownBy store.truncate(TX(9))

      an[IllegalArgumentException] should be thrownBy store.discard(TX(1))
      an[IllegalArgumentException] should be thrownBy store.discard(TX(7))

      store.prevIdx should equal (TX(2))
      store.lastIdx should equal (TX(8))
      store.flushedLastIdx should equal (TX(8))
      store.uncommittedLastIdx should equal (TX(8))
    }

    "reinit" in {
      9 to 10 foreach { i => store.add(Seq(s"entry $i")) should equal (TX(i)) }

      store.reinit(TX(4))

      store.prevIdx should equal (TX(2))
      store.lastIdx should equal (TX(8))
      store.flushedLastIdx should equal (TX(10))
      store.uncommittedLastIdx should equal (TX(10))

      store.reinit(TX(8))

      store.prevIdx should equal (TX(2))
      store.lastIdx should equal (TX(8))
      store.flushedLastIdx should equal (TX(10))
      store.uncommittedLastIdx should equal (TX(10))

      store.reinit(TX(9))

      store.prevIdx should equal (TX(2))
      store.lastIdx should equal (TX(9))
      store.flushedLastIdx should equal (TX(10))
      store.uncommittedLastIdx should equal (TX(10))

      store.reinit(TX(10))

      store.prevIdx should equal (TX(2))
      store.lastIdx should equal (TX(10))
      store.flushedLastIdx should equal (TX(10))
      store.uncommittedLastIdx should equal (TX(10))

      store.reinit(TX(11))

      store.prevIdx should equal (TX(11))
      store.lastIdx should equal (TX(11))
      store.flushedLastIdx should equal (TX(11))
      store.uncommittedLastIdx should equal (TX(11))
    }

    "subscribe" - {
      "idles" in {
        val done = store.subscribe(TX(500), 1.second) {
          case Log.Idle(tx) =>
            tx should equal (TX(500))
            FutureFalse
          case v => sys.error(v.toString)
        }

        Await.result(done, 10.seconds)
      }

      "can subscribe to updates of log" in {
        store.reinit(TX(100))

        val received1 = ListBuffer.empty[TX]
        val received2 = ListBuffer.empty[TX]

        val done1 = store.subscribe(TX(100), 500.millis) {
          case Log.Entries(_, es) =>
            es foreach { e => received1 += e.idx }
            Future.successful(received1.size < 400)
          case _ => FutureFalse
        }

        // This one is a little slower
        val done2 = store.subscribe(TX(100), 1.second) {
          case Log.Entries(_, es) =>
            Thread.sleep(100)
            es foreach { e => received2 += e.idx }
            Future.successful(received2.size < 400)
          case _ => FutureFalse
        }

        received1.result() should equal (Nil)
        received2.result() should equal (Nil)

        Future {
          101 to 500 foreach { _ =>
            val idx = store.add(Seq("entry"))
            store.flush()
            store.updateCommittedIdx(idx)
          }
        }

        Await.result(done1, 30.seconds)
        Await.result(done2, 30.seconds)

        store.lastIdx should equal (TX(500))

        received1.result() should equal (101 to 500 map { TX(_) })
        received2.result() should equal (101 to 500 map { TX(_) })
      }

      "sends reinit if afterIdx is < prevIdx" in {
        val received = ListBuffer.empty[TX]

        val done = store.subscribe(TX.MinValue, 1.second) {
          case Log.Reinit(tx) =>
            tx should equal (TX(100))
            FutureTrue
          case Log.Entries(_, es) =>
            es foreach { e => received += e.idx }
            Future.successful(received.size < 400)
          case v => fail(v.toString)
        }

        Await.result(done, 30.seconds)

        received.result() should equal (101 to 500 map { TX(_) })
      }

      "sends reinit if log is reinitialized" in {
        val done = store.subscribe(TX(500), 1.second) {
          case Log.Reinit(tx) =>
            tx should equal (TX(1000))
            FutureFalse
          case v => fail(v.toString)
        }

        store.reinit(TX(1000))

        Await.result(done, 30.seconds)
      }

      "should see close" in {
        val done = store.subscribe(TX(1000), 1.second) {
          case Log.Closed => FutureFalse
          case v => fail(v.toString)
        }

        store.close()

        Await.result(done, 30.seconds)
      }
    }
  }
}
