package fauna.cluster.test

import fauna.atoms.HostID
import fauna.cluster._
import fauna.lang.clocks._
import fauna.stats.StatsRecorder
import scala.concurrent.duration._
import scala.util.Random

class FailureDetectorSpec extends Spec {
  val clock = new TestClock
  val stats = StatsRecorder.Null

  val node1 = Node.create()
  val node2 = Node.create()

  await(node1.membership.init())
  node1.awaitActive()

  await(node2.membership.join(node1.address))
  node2.awaitActive()

  val fd1 = FailureDetector(node1.membership, stats, clock)
  val fd2 = FailureDetector(node2.membership, stats, clock)

  def fuzz(increment: Duration) = {
    // follow a folded-normal distribution to simulate tail latency on
    // the local machine - i.e. "local pauses"
    val factor = Random.nextGaussian().abs

    val min = increment.toMillis * factor
    val max = increment.toMillis / factor
    val delta = (min + Random.nextDouble() * (max - min + 1)).toLong

    clock.advance(delta.millis)
  }

  override def afterAll() = {
    node1.stop()
    node2.stop()
  }

  "FailureDetector" - {
    "considers itself live" in {
      fd1.isAlive(node1.id) should be (true)
      fd2.isAlive(node2.id) should be (true)
    }

    "considers unknown members unreachable" in {
      val nobody = HostID.randomID
      fd1.isAlive(nobody) should be (false)
      fd2.isAlive(nobody) should be (false)
    }

    "considers peers unreachable before they have data" in {
      fd1.isAlive(node2.id) should be (false)
      fd2.isAlive(node1.id) should be (false)
    }

    "regular heartbeats keep a peer alive" in {
      fd1.remove(node2.id)

      for (_ <- 0 to FailureDetector.SampleSize) {
        fuzz(FailureDetector.CheckInterval)

        fd1.mark(node2.id)
        fd1.interpret(node2.id)

        fd1.isAlive(node2.id) should be (true)
      }
    }

    "long pauses mark a peer unreachable" in {
      fd1.remove(node2.id)

      // liveness
      for (_ <- 0 to FailureDetector.SampleSize) {
        fuzz(FailureDetector.CheckInterval)

        fd1.mark(node2.id)
        fd1.interpret(node2.id)
      }


      // unreachability
      eventually(timeout(5.seconds)) {
        fuzz(FailureDetector.CheckInterval)
        fd1.interpret(node2.id)
        fd1.isAlive(node2.id) should be (false)
      }

      // react within 1 check interval
      clock.advance(FailureDetector.CheckInterval)

      fd1.mark(node2.id)

      fd1.interpret(node2.id)
      fd1.isAlive(node2.id) should be (true)
    }
  }
}
