package fauna.codex.test

import fauna.codex.cbor.{ CBOR, CBOROrdering }
import fauna.prop.{ Prop, PropConfig }
import io.netty.buffer.ByteBuf
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class OrderingBenchmarkSpec extends AnyFreeSpec with Matchers {
  implicit val propConfig = PropConfig(minSuccessful = 1, seed=157612892246843684L)
  // This is a base count, we'll actually end up with 4 times this number as we'll
  // add uppercase variants of all strings, random prefixes of all strings, and
  // random prefixes of uppercase variants.
  // The run time is proportional to the square of the number of strings.
  val stringCount = 500
  val maxStringLength = 1000
  val repetitions = 5

  "CBOR string comparison should be fast" in {
    runBenchmark("benchmark", genData(stringCount), repetitions) should be < 500L
  }

  private def genData(count: Int) = {
    var strings = Prop.string(1, maxStringLength).times(count).sample
    strings = strings ++ (strings map { _.toUpperCase }) // add uppercase variants
    strings = strings ++ (strings map { s => s.substring(0, Prop.int(s.length).sample) }) // add random prefixes
    strings map { s => CBOR.encode(s) }
  }

  private def runBenchmark(round: String, data: Seq[ByteBuf], repetitions: Int) = {
    val comparisons = data.length * data.length * repetitions
    val t1 = System.nanoTime()
    (1 to repetitions) foreach { _ =>
      for(s1 <- data; s2 <- data) {
        CBOROrdering.compare(s1, s2)
      }
    }
    val duration = System.nanoTime() - t1
    val nsPerComparison = duration/comparisons
    info(s"$round: ${duration}ns for $comparisons comparisons = ${nsPerComparison}ns/comparison\n")
    nsPerComparison
  }
}
