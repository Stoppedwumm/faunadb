package benchmarks.lang

import fauna.lang.SpinedSeq
import org.openjdk.jmh.annotations._
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

@Fork(1)
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@Warmup(iterations = 10, time = 10, timeUnit = SECONDS)
@Measurement(iterations = 10, time = 10, timeUnit = SECONDS)
class SpinedSeqBench {

  @Param(Array("2", "16", "64", "128", "1024", "4096", "32768", "131072", "1048576"))
  var elems: String = ""

  @Benchmark
  @OutputTimeUnit(MILLISECONDS)
  def listBuffer() = {
    val buf = ListBuffer.empty[Int]

    (0 until elems.toInt) foreach { i =>
      buf.append(i)
    }
  }

  @Benchmark
  @OutputTimeUnit(MILLISECONDS)
  def spinedSeq() = {
    val buf = SpinedSeq.empty[Int]

    (0 until elems.toInt) foreach { i =>
      buf.append(i)
    }
  }
}
