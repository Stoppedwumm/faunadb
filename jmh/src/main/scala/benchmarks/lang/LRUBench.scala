package benchmarks.lang

import fauna.lang.LRU
import org.openjdk.jmh.annotations._
import scala.concurrent.duration._

object LRUBench {
  class Value(val isPinned: Boolean) extends LRU.Pinnable
  val unpinned = new Value(isPinned = false)
  val pinned = new Value(isPinned = true)
}

@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(Array(Mode.Throughput))
@Warmup(iterations = 10, time = 10, timeUnit = SECONDS)
@Measurement(iterations = 10, time = 10, timeUnit = SECONDS)
abstract class LRUBench {
  import LRUBench._

  @Param(Array("16", "64", "128", "1024", "4096", "32768", "131072", "1048576"))
  var maxCapacity: String = ""
  var cache: LRU[Int, Value] = _
  var _next = 0

  def next() = {
    _next += 1
    _next
  }
}

class UnpinneableLRUBench extends LRUBench {
  import LRUBench._
  @Setup def setup() = cache = LRU.unpinneable(maxCapacity.toInt)
  @Benchmark def putUnpinned() = cache.put(next(), unpinned)
}

class PinnableLRUBench extends LRUBench {
  import LRUBench._
  @Setup def setup() = cache = LRU.pinneable(maxCapacity.toInt)
  @Benchmark def putUnpinned() = cache.put(next(), unpinned)
  @Benchmark def putPinned() = cache.put(next(), pinned)
}
