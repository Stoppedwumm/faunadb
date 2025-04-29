package fauna.tx.test

import fauna.lang.Timestamp
import fauna.lang.clocks.Clock
import fauna.tx.transaction.{ ApplySequencer, TxnPipeline }
import fauna.stats.StatsRecorder
import scala.concurrent.Promise
import scala.concurrent.duration._

class ApplySequencerSpec extends Spec {

  val stats = new TxnPipeline.Stats(StatsRecorder.Null)

  "ApplySequencer" - {
    "timestamp echoes back `max` when key is not present" in {
      val s = new ApplySequencer[AnyRef]

      s.maxAppliedTimestamp should equal (Timestamp.Epoch)
      s.appliedTimestamp should equal (Timestamp.Epoch)
      s.appliedTimestamp(key) should equal (Timestamp.Epoch)

      val max = Clock.time
      s.updateMaxAppliedTimestamp(max)

      s.maxAppliedTimestamp should equal (max)
      s.appliedTimestamp should equal (max)
      s.appliedTimestamp(key) should equal (max)
    }

    "timestamp advances when applies complete in order" in {
      val s = new ApplySequencer[AnyRef]
      val key = new Object

      val ts1 = Clock.time
      val ts2 = ts1 + 1.second
      val ts3 = ts1 + 2.seconds
      val max = ts1 + 3.seconds

      s.updateMaxAppliedTimestamp(max)
      val r1 = s.join(key)
      val p1 = Promise[Unit]()
      s.sequence(ts1, List(key), stats)(p1.future)
      val r2 = s.join(key)
      val p2 = Promise[Unit]()
      s.sequence(ts2, List(key), stats)(p2.future)
      val r3 = s.join(key)
      val p3 = Promise[Unit]()
      s.sequence(ts3, List(key), stats)(p3.future)
      val r4 = s.join(key)

      def readsCompleted = List(r1, r2, r3, r4) map { _.isCompleted }

      s.maxAppliedTimestamp should equal (max)
      s.appliedTimestamp should equal (ts1.prevNano)
      s.appliedTimestamp(key) should equal (ts1.prevNano)
      readsCompleted should equal (List(true, false, false, false))
      s.keysSize should equal (1)

      p1.success(())

      s.maxAppliedTimestamp should equal (max)
      s.appliedTimestamp should equal (ts2.prevNano)
      s.appliedTimestamp(key) should equal (ts2.prevNano)
      readsCompleted should equal (List(true, true, false, false))
      s.keysSize should equal (1)

      p2.success(())

      s.maxAppliedTimestamp should equal (max)
      s.appliedTimestamp should equal (ts3.prevNano)
      s.appliedTimestamp(key) should equal (ts3.prevNano)
      readsCompleted should equal (List(true, true, true, false))
      s.keysSize should equal (1)

      p3.success(())

      s.maxAppliedTimestamp should equal (max)
      s.appliedTimestamp should equal (max)
      s.appliedTimestamp(key) should equal (max)
      readsCompleted should equal (List(true, true, true, true))
      s.keysSize should equal (0)
    }

    "timestamp advances when applies complete out of order" in {
      val s = new ApplySequencer[AnyRef]
      val key = new Object

      val ts1 = Clock.time
      val ts2 = ts1 + 1.second
      val ts3 = ts1 + 2.seconds
      val max = ts1 + 3.seconds

      s.updateMaxAppliedTimestamp(max)
      val r1 = s.join(key)
      val p1 = Promise[Unit]()
      s.sequence(ts1, List(key), stats)(p1.future)
      val r2 = s.join(key)
      val p2 = Promise[Unit]()
      s.sequence(ts2, List(key), stats)(p2.future)
      val r3 = s.join(key)
      val p3 = Promise[Unit]()
      s.sequence(ts3, List(key), stats)(p3.future)
      val r4 = s.join(key)

      def readsCompleted = List(r1, r2, r3, r4) map { _.isCompleted }

      s.maxAppliedTimestamp should equal (max)
      s.appliedTimestamp should equal (ts1.prevNano)
      s.appliedTimestamp(key) should equal (ts1.prevNano)
      readsCompleted should equal (List(true, false, false, false))
      s.keysSize should equal (1)

      p3.success(())

      s.maxAppliedTimestamp should equal (max)
      s.appliedTimestamp should equal (ts1.prevNano)
      s.appliedTimestamp(key) should equal (ts1.prevNano)
      readsCompleted should equal (List(true, false, false, false))
      s.keysSize should equal (1)

      p1.success(())

      s.maxAppliedTimestamp should equal (max)
      s.appliedTimestamp should equal (ts2.prevNano)
      s.appliedTimestamp(key) should equal (ts2.prevNano)
      readsCompleted should equal (List(true, true, false, false))
      s.keysSize should equal (1)

      p2.success(())

      s.maxAppliedTimestamp should equal (max)
      s.appliedTimestamp should equal (max)
      s.appliedTimestamp(key) should equal (max)
      readsCompleted should equal (List(true, true, true, true))
      s.keysSize should equal (0)
    }

    "appliedTimestamp returns minimum across keys" in {
      val s = new ApplySequencer[AnyRef]
      val key1 = new Object
      val key2 = new Object

      val ts1 = Clock.time
      val ts2 = ts1 + 1.second
      val ts3 = ts1 + 2.seconds
      val max = ts1 + 3.seconds

      s.updateMaxAppliedTimestamp(max)
      val r1 = s.join(key1)
      val p1 = Promise[Unit]()
      s.sequence(ts1, List(key1), stats)(p1.future)
      val r2 = s.join(key2)
      val p2 = Promise[Unit]()
      s.sequence(ts2, List(key2), stats)(p2.future)
      val r3 = s.join(key1)
      val p3 = Promise[Unit]()
      s.sequence(ts3, List(key1), stats)(p3.future)
      val r4 = s.join(key1)
      val r5 = s.join(key2)

      def readsCompleted = List(r1, r2, r3, r4, r5) map { _.isCompleted }

      s.maxAppliedTimestamp should equal (max)
      s.appliedTimestamp should equal (ts1.prevNano)
      s.appliedTimestamp(key1) should equal (ts1.prevNano)
      s.appliedTimestamp(key2) should equal (ts2.prevNano)
      readsCompleted should equal (List(true, true, false, false, false))
      s.keysSize should equal (2)

      p1.success(())

      s.maxAppliedTimestamp should equal (max)
      s.appliedTimestamp should equal (ts2.prevNano)
      s.appliedTimestamp(key1) should equal (ts3.prevNano)
      s.appliedTimestamp(key2) should equal (ts2.prevNano)
      readsCompleted should equal (List(true, true, true, false, false))
      s.keysSize should equal (2)

      p2.success(())

      s.maxAppliedTimestamp should equal (max)
      s.appliedTimestamp should equal (ts3.prevNano)
      s.appliedTimestamp(key1) should equal (ts3.prevNano)
      s.appliedTimestamp(key2) should equal (max)
      readsCompleted should equal (List(true, true, true, false, true))
      s.keysSize should equal (1)

      p3.success(())

      s.maxAppliedTimestamp should equal (max)
      s.appliedTimestamp should equal (max)
      s.appliedTimestamp(key1) should equal (max)
      s.appliedTimestamp(key2) should equal (max)
      readsCompleted should equal (List(true, true, true, true, true))
      s.keysSize should equal (0)
    }
    "write chain correctly returns chain length" in {
      val ts1 = Clock.time
      val ts2 = ts1 + 1.second
      val ts3 = ts1 + 2.seconds
      val ts4 = ts1 + 3.seconds

      val wc1 = new ApplySequencer.WriteChain(ts1, Promise[Unit](), null)
      val wc2 = new ApplySequencer.WriteChain(ts2, Promise[Unit](), wc1)
      val wc3 = new ApplySequencer.WriteChain(ts3, Promise[Unit](), wc2)
      val wc4 = new ApplySequencer.WriteChain(ts4, Promise[Unit](), wc3)

      wc1.length shouldEqual 1
      wc2.length shouldEqual 2
      wc3.length shouldEqual 3
      wc4.length shouldEqual 4

      val res1 = wc4.resolved(ts1)
      val res2 = res1.resolved(ts2)
      val res3 = res2.resolved(ts3)
      val res4 = res3.resolved(ts4)
      res1.length shouldEqual 3
      res2.length shouldEqual 2
      res3.length shouldEqual 1
      res4 shouldBe null
    }
  }
}
