package benchmarks.exec

import fauna.exec.ImmediateExecutionContext
import org.openjdk.jmh.annotations._
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

@Fork(1)
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@Warmup(iterations = 5, time = 5, timeUnit = SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = SECONDS)
class IECBench {
  import IECBench._

  @Param(Array("1", "2", "4", "8", "16", "32", "64", "128", "256", "512", "1024"))
  var depth: String = ""

  @Benchmark
  @OutputTimeUnit(MILLISECONDS)
  def executeFutures() = {
    val future = buildFuture(depth.toInt)
    Await.result(future, 2.minutes)
  }
}

object IECBench {

  implicit val ec = ImmediateExecutionContext

  def buildFuture(depth: Int): Future[Int] =
    // Initiates the first IEC trampoline.
    Future.delegate {
      // Schedule depth runnables into the IEC queue.
      val futs = List.tabulate(depth) { i => Future.unit map { _ => i } }
      Future.foldLeft(futs)(0) { _ + _ } // Join them all.
    }
}
