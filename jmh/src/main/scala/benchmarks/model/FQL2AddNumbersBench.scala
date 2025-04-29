package benchmarks.model

import fauna.repo.values.Value
import org.openjdk.jmh.annotations._
import scala.concurrent.duration._
import FQL2Bench._

@Fork(1)
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(MICROSECONDS)
class FQL2AddNumbersBench {

  val q = (1 to 200).mkString("+")
  val res = (1 to 200).sum

  @Benchmark
  def addNumbers() = {
    val v = evalOk(q)
    v match {
      case Value.Int(`res`) => {}
      case _                => throw new IllegalStateException("it wrong")
    }
  }
}
