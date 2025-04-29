package fauna.lang.test

import fauna.lang._
import fauna.lang.clocks._
import java.util.{ List => JList }
import scala.concurrent.duration._

class ReservoirSampleSpec extends Spec {
  implicit val ord = Ordering.Double.TotalOrdering

  class TestClock extends DeadlineClock {
    var nanos = 0L
  }

  "ReservoirSample" - {
    "empty" in {
      val res = new ReservoirSample(100, 0.015)
      val snap = res.snapshot()

      snap.min should equal (0)
      snap.max should equal (0)
      snap.mean should equal (0.0)
      snap.median should equal (0.0)
      snap.stddev should equal (0.0)
      snap.size should equal (0)
    }

    "partial" in {
      val res = new ReservoirSample(100, 0.99)

      for (i <- 0 until 10) {
        res.sample(i)
      }

      res.size should be (10)

      val snap = res.snapshot()

      snap.size should be (10)

      snap.values foreach { v =>
        v shouldBe >= (0L)
        v shouldBe < (10L)
      }
    }

    "full" in {
      val res = new ReservoirSample(100, 0.99)

      for (i <- 0 until 1000) {
        res.sample(i)
      }

      res.size should be (100)

      val snap = res.snapshot()

      snap.size should be (100)

      snap.values foreach { v =>
        v shouldBe >= (0L)
        v shouldBe < (1000L)
      }
    }

    "biased" in {
      val res = new ReservoirSample(1000, 0.01)

      for (i <- 0 until 100) {
        res.sample(i)
      }

      res.size should be (100)

      val snap = res.snapshot()

      snap.size should be (100)

      snap.values foreach { v =>
        v shouldBe >= (0L)
        v shouldBe < (100L)
      }
    }

    "idle" in {
      val clock = new TestClock
      val res = new ReservoirSample(10, 0.015, clock)

      for (i <- 0 until 1000) {
        res.sample(1000 + i)
        clock.nanos += 100.millis.toNanos
      }

      res.snapshot().size should be (10)
      res.snapshot().values foreach { v =>
        v shouldBe >= (1000L)
        v shouldBe < (2000L)
      }

      clock.nanos += 20.hours.toNanos

      res.snapshot().size should equal (0)
    }

    "rises" in {
      val clock = new TestClock
      val res = new ReservoirSample(1000, 0.015, clock)

      val ratePerMinute = 10
      val interval = 1.minute.toMillis / ratePerMinute

      for (_ <- 0 until 120 * ratePerMinute) {
        res.sample(177)
        clock.nanos += interval.millis.toNanos
      }

      for (_ <- 0 until 10 * ratePerMinute) {
        res.sample(9999)
        clock.nanos += interval.millis.toNanos
      }

      res.snapshot().median should equal (9999)
    }

    "falls" in {
      val clock = new TestClock
      val res = new ReservoirSample(1000, 0.015, clock)

      val ratePerMinute = 10
      val interval = 1.minute.toMillis / ratePerMinute

      for (_ <- 0 until 120 * ratePerMinute) {
        res.sample(9998)
        clock.nanos += interval.millis.toNanos
      }

      for (_ <- 0 until 10 * ratePerMinute) {
        res.sample(178)
        clock.nanos += interval.millis.toNanos
      }

      res.snapshot().valueAt(0.95) should equal (178)
    }

    "quantile weights" in {
      val clock = new TestClock
      val res = new ReservoirSample(1000, 0.015, clock)

      for (_ <- 0 until 40) {
        res.sample(177)
      }

      clock.nanos += 120.seconds.toNanos

      for (_ <- 0 until 10) {
        res.sample(9999)
      }

      res.size should be (50)

      val snap = res.snapshot()

      // the first 40 items (177) have weights 1
      // the next 10 items (9999) have weights ~6
      // so, 40/60 distribution, not 40/10
      snap.median should equal (9999)
      snap.valueAt(0.75) should equal (9999)
    }
  }

  "ReservoirSample.Snapshot" - {
    import ReservoirSample._

    val snapshot = Snapshot(JList.of(
      Sample(5, 1.0),
      Sample(1, 2.0),
      Sample(2, 3.0),
      Sample(3, 2.0),
      Sample(4, 2.0)
    ))

    "min" in {
      snapshot.min should equal (1.0)
    }

    "max" in {
      snapshot.max should equal (5.0)
    }

    "mean" in {
      snapshot.mean should equal (2.7)
    }

    "median" in {
      snapshot.median should equal (3.0)
    }

    "valueAt" in {
      snapshot.valueAt(0.0) should equal (1.0)
      snapshot.valueAt(0.75) should equal (4.0)
      snapshot.valueAt(0.95) should equal (5.0)
      snapshot.valueAt(0.98) should equal (5.0)
      snapshot.valueAt(0.99) should equal (5.0)
      snapshot.valueAt(0.999) should equal (5.0)
      snapshot.valueAt(1.0) should equal (5.0)

      an[IllegalArgumentException] should be thrownBy {
        snapshot.valueAt(-0.5)
      }

      an[IllegalArgumentException] should be thrownBy {
        snapshot.valueAt(1.5)
      }

      an[IllegalArgumentException] should be thrownBy {
        snapshot.valueAt(Double.NaN)
      }
    }

    "stddev" in {
      snapshot.stddev shouldEqual 1.2688 +- 0.0001
    }

    "variance" in {
      snapshot.variance should equal (1.61)
    }
  }
}
