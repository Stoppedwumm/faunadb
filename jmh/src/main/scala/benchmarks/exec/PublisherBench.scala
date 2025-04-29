package benchmarks.exec

import fauna.exec.{ FaunaExecutionContext, Observable, OverflowStrategy, Publisher }
import java.util.concurrent.atomic.AtomicInteger
import org.openjdk.jmh.annotations._
import scala.concurrent.duration._

@Fork(1)
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@Warmup(iterations = 5, time = 5, timeUnit = SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = SECONDS)
class PublisherBench {
  import FaunaExecutionContext.Implicits.global

  @Param(Array("1", "10", "100", "1000", "10000", "100000"))
  var subscribersArg: String = ""
  var subscribers: Int = 0

  @Param(Array("1", "10", "100", "1000"))
  var valuesArg: String = ""
  var values: Int = 0

  var publishers: Seq[Publisher[Int]] = _
  val waiters = new AtomicInteger()

  @Setup
  def setup() = {
    subscribers = subscribersArg.toInt
    values = valuesArg.toInt

    val builder = Seq.newBuilder[Publisher[Int]]
    builder.sizeHint(subscribers)

    for (_ <- 1 to subscribers) {
      val (pub, obs) = Observable.gathering[Int](OverflowStrategy.bounded(1024))
      obs foreachF { _ => waiters.decrementAndGet() }
      builder += pub
    }

    publishers = builder.result()
  }

  @Benchmark
  @OutputTimeUnit(SECONDS)
  def observeAnEvent() = {
    waiters.set(subscribers * values)
    for {
      pub <- publishers
      i   <- 1 to values
    } {
      pub.publish(i)
    }
    while (waiters.get > 0) {
      Thread.onSpinWait()
    }
  }
}
