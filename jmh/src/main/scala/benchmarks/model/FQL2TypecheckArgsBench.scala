package benchmarks.model

import fauna.model.runtime.fql2.FQLInterpreter.TypeMode
import fauna.repo.values.Value
import org.openjdk.jmh.annotations._
import scala.concurrent.duration._
import FQL2Bench._

@Fork(1)
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(MICROSECONDS)
class FQL2TypecheckArgsBench {

  val innerObject = Value.Struct((1 to 200).map { v =>
    v.toString -> Value.Int(v)
  }: _*)
  val myObject = Value.Struct((1 to 2000).map { v => v.toString -> innerObject }: _*)
  val q = "myObject['3']['4']"
  val res = 4

  @Benchmark
  def typecheckArgs() = {
    val v = evalOk(q, Map("myObject" -> myObject), TypeMode.InferType)
    v match {
      case Value.Int(`res`) => {}
      case _                => throw new IllegalStateException("it wrong")
    }
  }
}
