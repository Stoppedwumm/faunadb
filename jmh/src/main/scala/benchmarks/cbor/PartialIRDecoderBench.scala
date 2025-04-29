package benchmarks.cbor

import fauna.codex.cbor.{ CBOR, CBORParser }
import fauna.storage.ir._
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@Fork(value = 1)
class PartialIRDecoderBench {

  @Param(Array("small", "medium", "large"))
  var size: String = ""

  @Param(Array("start", "end"))
  var pos: String = ""

  @Benchmark
  def partialDecode() = {
    val (t, bytes) = size match {
      case "small"  => PartialIRDecoderBench.small
      case "medium" => PartialIRDecoderBench.medium
      case _        => PartialIRDecoderBench.large
    }
    bytes.resetReaderIndex()
    val key = pos match {
      case "start" => "0"
      case _       => t
    }
    CBORParser(bytes).read(new PartialIRCodec(List(key)))
  }
}

object PartialIRDecoderBench {
  def makeIR(n: Int): IRValue =
    MapV((0 to n) map { k =>
      (k.toString, ArrayV(Vector.fill(32)(LongV(0))))
    }: _*)

  val small  = ("8", CBOR.encode(makeIR(8)))       // 289 bytes.
  val medium = ("64", CBOR.encode(makeIR(64)))     // 2361 bytes.
  val large  = ("4096", CBOR.encode(makeIR(4096))) // 158640 bytes.
}
