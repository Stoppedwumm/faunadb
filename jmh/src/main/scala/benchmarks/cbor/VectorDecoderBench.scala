package benchmarks.cbor

import fauna.codex.cbor.CBOR
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@Fork(value = 1)
class VectorDecoderBench {

  @Param(Array("32", "100", "1000", "10000"))
  var vectorSize: String = ""

  @Benchmark
  def decodeVector() = {
    val vec = vectorSize match {
      case "32"    => VectorDecoderBench.thirtyTwoVectorInt
      case "100"   => VectorDecoderBench.hundredVectorInt
      case "1000"  => VectorDecoderBench.thousandVectorInt
      case "10000" => VectorDecoderBench.tenThousandVectorInt
    }
    CBOR.encode(vec)
  }

}

object VectorDecoderBench {
  val thirtyTwoVectorInt = Vector.fill(32)(42)
  val hundredVectorInt = Vector.fill(100)(42)
  val thousandVectorInt = Vector.fill(1000)(42)
  val tenThousandVectorInt = Vector.fill(10000)(42)
}
