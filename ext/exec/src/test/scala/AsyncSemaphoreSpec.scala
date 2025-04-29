package fauna.exec.test

import fauna.exec.{ AsyncLatch, AsyncSemaphore }
import fauna.exec.FaunaExecutionContext.Implicits._
import fauna.lang.syntax._
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

class AsyncSemaphoreSpec extends Spec {
  "works" in {
    val MaxPermits = 3
    val Concurrency = 16
    val Iterations = 10000

    val s = AsyncSemaphore(MaxPermits)

    val start = new AsyncLatch[Unit]
    val conc = new AtomicInteger(0)

    def worker(i: Int): Future[Unit] =
      if (i <= 0) {
        Future.unit
      } else {
        val p = ThreadLocalRandom.current().nextInt(MaxPermits + 2) // intentionally more
        s.acquire(p) flatMap { d =>
          conc.addAndGet(d) should be <= MaxPermits
          conc.addAndGet(-d)
          s.release(d)
          worker(i - d)
        }
      }

    val workers = 1 to Concurrency map { _ =>
      start.await flatMap { _ =>
        worker(Iterations)
      }
    }

    start.signal(())
    Await.result(workers.join, 30.seconds)
    conc.get() should equal(0)
  }
}
