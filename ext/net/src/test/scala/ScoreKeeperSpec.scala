package fauna.net.test

import fauna.atoms.HostID
import fauna.lang.clocks.DeadlineClock
import fauna.net.ScoreKeeper
import scala.util.Random

class ScoreKeeperSpec extends Spec {
  class TestClock extends DeadlineClock {
    var nanos = 0L
  }

  // provided to ensure deterministic score computation
  class TestRandom extends Random {
    override def nextDouble(): Double = 0.0D
  }

  val window = 100
  val clock = new TestClock
  val random = new TestRandom

  "ScoreKeeper" - {
    "computes scores" in {
      val sk = new ScoreKeeper("test", window, clock = clock, random = random)

      val hostA = HostID.randomID
      val hostB = HostID.randomID
      val hostC = HostID.randomID

      sk.score(hostA) should equal (0.0)
      sk.score(hostB) should equal (0.0)
      sk.score(hostC) should equal (0.0)

      (0 until window) foreach { _ =>
        sk.receiveTiming(hostA, 100)
        sk.receiveTiming(hostB, 200)
        sk.receiveTiming(hostC, 400)
      }

      sk.score(hostA) should equal (0.0)
      sk.score(hostB) should equal (0.0)
      sk.score(hostC) should equal (0.0)

      sk.computeScores()

      sk.score(hostA) should equal (0.25)
      sk.score(hostB) should equal (0.5)
      sk.score(hostC) should equal (1.0)

      sk.reset()
      sk.computeScores()

      sk.score(hostA) should equal (0.0)
      sk.score(hostB) should equal (0.0)
      sk.score(hostC) should equal (0.0)
    }

    "requires adequate samples" in {
      val threshold = (window * ScoreKeeper.FullnessThreshold).toInt
      val sk = new ScoreKeeper("test", window, clock = clock, random = random)

      val host = HostID.randomID

      (0 until threshold - 1) foreach { _ =>
        sk.receiveTiming(host, 100)
      }

      sk.computeScores()
      sk.score(host) should equal (0.0)

      sk.receiveTiming(host, 100)

      sk.computeScores()
      sk.score(host) should equal (1.0)
    }

    "resets a single host" in {
      val sk = new ScoreKeeper("test", window, clock = clock, random = random)

      val hostA = HostID.randomID
      val hostB = HostID.randomID
      val hostC = HostID.randomID

      sk.score(hostA) should equal (0.0)
      sk.score(hostB) should equal (0.0)
      sk.score(hostC) should equal (0.0)

      (0 until window) foreach { _ =>
        sk.receiveTiming(hostA, 100)
        sk.receiveTiming(hostB, 200)
        sk.receiveTiming(hostC, 400)
      }

      sk.computeScores()

      sk.score(hostA) should equal (0.25)
      sk.score(hostB) should equal (0.5)
      sk.score(hostC) should equal (1.0)

      sk.reset(hostC)
      sk.computeScores()

      sk.score(hostA) should equal (0.5)
      sk.score(hostB) should equal (1.0)
      sk.score(hostC) should equal (0.0)
    }

    "high score adjustment does not affect other scores" in {
      val sk = new ScoreKeeper("test", window, clock = clock) // NB. default Random

      val hostA = HostID.randomID
      val hostB = HostID.randomID
      val hostC = HostID.randomID

      sk.score(hostA) should equal (0.0)
      sk.score(hostB) should equal (0.0)
      sk.score(hostC) should equal (0.0)

      (0 until window) foreach { _ =>
        sk.receiveTiming(hostA, 100)
        sk.receiveTiming(hostB, 200)
        sk.receiveTiming(hostC, 400)
      }

      sk.score(hostA) should equal (0.0)
      sk.score(hostB) should equal (0.0)
      sk.score(hostC) should equal (0.0)

      sk.computeScores()

      sk.score(hostA) should equal (0.25)
      sk.score(hostB) should equal (0.5)
      sk.score(hostC) should be <= (1.0)

      sk.reset()
      sk.computeScores()

      sk.score(hostA) should equal (0.0)
      sk.score(hostB) should equal (0.0)
      sk.score(hostC) should equal (0.0)
    }

  }
}
