package fauna.lang.test

import fauna.lang.{ Timestamp, Timing }
import fauna.lang.clocks._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Future, Await }
import scala.util.Random

class ClockSpec extends Spec {
  "Clock" - {
    def sleep(duration: FiniteDuration) = {
      val timing = Timing.start

      Clock.sleepUninterruptibly(duration)

      val elapsed = timing.elapsed

      // Subtract a msec. to account for the vagueries of time.
      elapsed should be > duration - 1.milli

      // Check for some large (but not wildly so) amount of time after
      // the target time to ensure sleep returned close enough after
      // the target duration.
      elapsed should be < duration + 100.millis
    }

    def interruptAfter(duration: FiniteDuration) = {
      val target = Thread.currentThread
      val thread = new Thread {
        try {
          NANOSECONDS.sleep(duration.toNanos)
        } catch {
          case _: InterruptedException =>
            fail("interrupted the interrupter!")
        }

        target.interrupt()
      }

      thread.start()
    }

    "sleeps" in {
      sleep(10.millis)
    }

    "interrupts" in {
      interruptAfter(10.millis)
      sleep(50.millis)

      try {
        // sleep() returns/throws immediately if an interrupt is
        // registered.
        Thread.sleep(10)
        fail("no interrupt!")
      } catch {
        case _: InterruptedException => ()
      }
    }
  }

  "MonotonicClock" - {
    "advances based on config" in {
      // by 1 milli
      val clock1 = new MonotonicClock(new MillisClock { val millis = 0L }, 1.millis)
      for (i <- 1 to 100) clock1.millis should equal (i)

      // by 2 millis
      val clock2 = new MonotonicClock(new MillisClock { val millis = 0L }, 2.millis)
      for (i <- 2 to 100 by 2) clock2.millis should equal (i)

      // by 1 micro
      val clock3 = new MonotonicClock(new MillisClock { val millis = 0L }, 1.micros)
      for (i <- 1 to 100) clock3.micros should equal (i)
    }

    "does not regress" in {
      val clock = new MonotonicClock(new MicrosClock { def micros = Random.nextInt().toLong }, 1.micros)

      var prev = clock.micros

      for (_ <- 1 to 100) {
        val tick = clock.micros
        prev should be < tick
        prev = tick
      }
    }

    "synchronizes across threads" in {
      val clock = new MonotonicClock(new MillisClock { val millis = 0L }, 1.millis)
      val futs = (1 to 20) map { _ =>
        Future {
          (1 to 100) map { _ => clock.micros }
        }
      }

      val stamps = futs map { Await.result(_, 1.second) } flatten

      stamps.distinct.size should equal (2000)
    }
  }

  "DelayTrackingClock" - {
    "returns time marked by delay" in {
      val src = new TestClock(Timestamp.Epoch + 100.micros)
      val clock = new WeightedDelayTrackingClock(src)

      clock.micros should equal (100)

      src.advance(10.micros)
      clock.micros should equal (110)

      clock.mark(Timestamp.Epoch)

      src.advance(10.micros)
      src.micros should equal (120)
      clock.micros should equal (110)
    }
  }
}
