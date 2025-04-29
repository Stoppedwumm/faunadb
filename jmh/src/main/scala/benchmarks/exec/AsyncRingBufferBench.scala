package benchmarks.exec

import fauna.exec.{ AsyncRingBuffer, FaunaExecutionContext }
import java.util.concurrent.atomic.AtomicInteger
import org.openjdk.jmh.annotations._
import scala.concurrent.duration._

@Fork(1)
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@Warmup(iterations = 5, time = 5, timeUnit = SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = SECONDS)
class AsyncRingBufferBench {
  import FaunaExecutionContext.Implicits.global

  @Param(Array("1", "10", "100", "1000", "10000", "100000"))
  var subscribersArg: String = ""
  var subscribers: Int = 0

  @Param(Array("1", "10", "100", "1000"))
  var valuesArg: String = ""
  var values: Int = 0

  var ring: AsyncRingBuffer[Int] = _
  val waiters = new AtomicInteger()

  @Setup
  def setup() = {
    subscribers = subscribersArg.toInt
    values = valuesArg.toInt

    ring = new AsyncRingBuffer(1024)

    for (_ <- 1 to subscribers) {
      ring foreachF { _ =>
        waiters.decrementAndGet()
      }
    }
  }

  @Benchmark
  @OutputTimeUnit(SECONDS)
  def observeAnEvent() = {
    waiters.set(subscribers * values)
    for (i <- 1 to values) {
      ring.publish(i)
    }
    while (waiters.get > 0) {
      Thread.onSpinWait()
    }
  }
}
