package fauna.net.test

import fauna.lang.Timestamp
import fauna.lang.clocks._
import fauna.net._
import scala.concurrent.duration._

class RateLimiterSpec extends Spec {

  var clock: TestClock = _
  var limiter: BurstLimiter = _

  def checkDelay(result: RateLimiter.Result, expected: FiniteDuration) =
    result match {
      case RateLimiter.Acquired(_) => fail("unexpected permit acquisition")
      case RateLimiter.DelayUntil(`expected`) => ()
      case RateLimiter.DelayUntil(delay) =>
        fail(s"expected delay $expected, but received $delay")
    }

  before {
    clock = new TestClock()
    limiter = BurstLimiter
      .newBuilder()
      .withPermitsPerSecond(5.0)
      .withMaxBurstSeconds(1.0)
      .withClock(clock)
      .build()
  }

  "BurstLimiter" - {

    "works" in {
      // Reserve the initial tranche of permits.
      limiter.acquire() should equal(0.millis)
      limiter.acquire() should equal(0.millis)
      limiter.acquire() should equal(0.millis)
      limiter.acquire() should equal(0.millis)
      limiter.acquire() should equal(0.millis)

      // One more to push the next permit acquisition.
      limiter.acquire() should equal(0.millis)

      // Wait.
      limiter.acquire() should equal(200.millis)
    }

    "inactivity" in {
      // Reserve the initial tranche of permits.
      limiter.acquire() should equal(0.millis)
      limiter.acquire() should equal(0.millis)
      limiter.acquire() should equal(0.millis)
      limiter.acquire() should equal(0.millis)
      limiter.acquire() should equal(0.millis)

      // Push the next acquisition.
      limiter.acquire() should equal(0.millis)

      // Ready for the next request.
      clock.advance(200.millis)

      // Granted immediately.
      limiter.acquire() should equal(0.millis)

      // Back on track.
      limiter.acquire() should equal(200.millis)
    }

    "no burst" in {
      limiter = BurstLimiter
        .newBuilder()
        .withPermitsPerSecond(5.0)
        .withMaxBurstSeconds(0.0)
        .withClock(clock)
        .build()

      // Max wait.
      clock.advance(1.second)
      clock.advance(1.second) // nop

      // Fulfill the burst from the 5 base permits.
      limiter.tryAcquire(1) should equal(RateLimiter.Acquired(false))
      limiter.tryAcquire(3) should equal(RateLimiter.Acquired(false))
      limiter.tryAcquire(1) should equal(RateLimiter.Acquired(false))

      // Hit the limit.
      limiter.tryAcquire(1) should equal(RateLimiter.Acquired(false))

      // No burst, must wait.
      limiter.tryAcquire(5) should equal(RateLimiter.DelayUntil(200.millis))
    }

    "intervals refill correctly" in {
      // 1 permit per 2ms.
      limiter = BurstLimiter
        .newBuilder()
        .withPermitsPerSecond(500.0)
        .withMaxBurstSeconds(0.0)
        .withClock(clock)
        .build()

      // Start with full permits.
      clock.advance(1.second)

      // Drain them.
      limiter.tryAcquire(500) should equal(RateLimiter.Acquired(false))

      // Get just one more permit.
      clock.advance(2.millis)
      // Try to use more than the available permit, which pushes nextFreeMicros
      // in the future.
      limiter.tryAcquire(5) should equal(RateLimiter.Acquired(false))
      // Without moving the clock, getting more permits should fail.
      limiter.tryAcquire() should equal(RateLimiter.DelayUntil(8.millis))
    }

    "burst" in {
      // Max wait.
      clock.advance(1.second)
      clock.advance(1.second) // nop

      // Fulfill the burst from 1 sec. of burst permits.
      limiter.tryAcquire(1) should equal(RateLimiter.Acquired(false))
      limiter.tryAcquire(3) should equal(RateLimiter.Acquired(false))
      limiter.tryAcquire(1) should equal(RateLimiter.Acquired(false))

      // Plus the 5 base permits.
      limiter.tryAcquire(5) should equal(RateLimiter.Acquired(true))

      // Back on track.
      limiter.tryAcquire(1) should equal(RateLimiter.Acquired(false))
      limiter.tryAcquire() should equal(RateLimiter.DelayUntil(200.millis))
    }

    "sustained burst" in {
      limiter = BurstLimiter
        .newBuilder()
        .withPermitsPerSecond(10.0)
        .withMaxBurstSeconds(10.0)
        .withClock(clock)
        .build()

      clock.advance(10.seconds)

      // Reserve from the base rate.
      limiter.acquire(10) should equal(0.millis)

      // Drain the accumulated burst permits at a rate just higher
      // than the base rate.
      (0 to 90) foreach { _ =>
        clock.advance(1.second)
        limiter.acquire(11) should equal(0.millis)
      }

      // Back on track.
      limiter.acquire() should equal(100.millis)
    }

    "burst overflow" in {
      limiter = BurstLimiter
        .newBuilder()
        .withPermitsPerSecond(50_000.0)
        .withMaxBurstSeconds(10.0)
        .withClock(clock)
        .build()

      clock.set(Timestamp.ofMicros(1691701259180000L))
      limiter.acquire() should equal(0.millis)
      clock.set(Timestamp.ofMicros(1691755235652000L))

      limiter.acquire() should equal(0.millis)
    }

    "try acquire" in {
      // Can't get more than base + burst.
      assert(!limiter.tryAcquire(11).isSuccess)

      // Empty the base, and borrow from the future.
      assert(limiter.tryAcquire(6).isSuccess)

      // No more available immediately.
      checkDelay(limiter.tryAcquire(1), 200.millis)

      clock.advance(200.millis) // +1 permit

      // Reserve 1, but the next will have to wait.
      assert(limiter.tryAcquire(1).isSuccess)

      // Not enough available immediately.
      checkDelay(limiter.tryAcquire(2), 200.millis)

      // Willing to wait for the other 4.
      assert(limiter.tryAcquire(1, 200.millis * 4).isSuccess)
    }

    "sync" in {
      val remote = BurstLimiter
        .newBuilder()
        .withPermitsPerSecond(5.0)
        .withMaxBurstSeconds(1.0)
        .withClock(clock)
        .build()

      // Local arrival.
      assert(limiter.tryAcquire(6).isSuccess)

      // Sync the remote.
      remote.sync(6)

      // Both local and remote delay for the same amount of time.
      checkDelay(limiter.tryAcquire(1), 200.millis)
      checkDelay(remote.tryAcquire(1), 200.millis)

      clock.advance(1.second) // +5 permits everywhere.

      // Both local and remote succeed independently.
      assert(limiter.tryAcquire(5).isSuccess)
      assert(remote.tryAcquire(5).isSuccess)

      // X-Sync
      remote.sync(5)
      limiter.sync(5)

      // The delay on both increases to accomodate.
      checkDelay(limiter.tryAcquire(1), 200.millis * 6)
      checkDelay(remote.tryAcquire(1), 200.millis * 6)
    }

  }
}
