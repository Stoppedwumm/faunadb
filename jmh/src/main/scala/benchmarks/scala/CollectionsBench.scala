package benchmarks.scala

import org.openjdk.jmh.annotations._
import scala.collection.immutable.ArraySeq
import scala.collection.mutable.Builder
import scala.concurrent.duration._

@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
@Warmup(iterations = 5, time = 5, timeUnit = SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = SECONDS)
class CollectionsBench {
  import CollectionsBench._

  @Param(
    Array(
      "1",
      "2",
      "4",
      "8",
      "16",
      "32",
      "64",
      "128",
      "256",
      "512",
      "1024",
      "2048",
      "4096",
      "8192"))
  var size: String = ""

  @Benchmark def listSize() = list(size).size
  @Benchmark def vectorSize() = vec(size).size
  @Benchmark def arrSeqSize() = arrSeq(size).size
  @Benchmark def listSplitAt() = list(size).splitAt(size.toInt / 2)
  @Benchmark def vectorSplitAt() = vec(size).splitAt(size.toInt / 2)
  @Benchmark def arrSeqSplitAt() = arrSeq(size).splitAt(size.toInt / 2)
  @Benchmark def listBuilder() = builder(List.newBuilder[Int], size)
  @Benchmark def vectorBuilder() = builder(Vector.newBuilder[Int], size)
  @Benchmark def arrSeqBuilder() = builder(ArraySeq.newBuilder[Int], size)
  @Benchmark def listBuilderHint() = builder(List.newBuilder[Int], size, true)
  @Benchmark def vectorBuilderHint() = builder(Vector.newBuilder[Int], size, true)
  @Benchmark def arrSeqBuilderHint() = builder(ArraySeq.newBuilder[Int], size, true)
}

object CollectionsBench {

  private[this] val cache = new ThreadLocal[AnyRef]

  def list(size: String) = cached(List.range(0, size.toInt))
  def vec(size: String) = cached(Vector.range(0, size.toInt))
  def arrSeq(size: String) = cached(ArraySeq.range(0, size.toInt))

  private[this] def cached[CC[_] <: AnyRef](mkColl: => CC[Int]): CC[Int] =
    cache.get match {
      case null =>
        val coll = mkColl
        cache.set(coll)
        coll
      case coll =>
        coll.asInstanceOf[CC[Int]]
    }

  def builder[CC[_]](
    b: Builder[Int, CC[Int]],
    size: String,
    hint: Boolean = false) = {
    if (hint) b.sizeHint(size.toInt)
    for (i <- 0 until size.toInt) b += i
    b.result()
  }
}
