package benchmarks.tx

import fauna.exec.FaunaExecutionContext
import fauna.lang.clocks.Clock
import fauna.net.statsd.DogStatsDClient
import fauna.stats.BufferedRecorder
import fauna.tx.transaction.{ ApplySequencer, TxnPipeline }
import org.openjdk.jmh.annotations._
import scala.annotation.switch
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.Random

class ApplySequencerBench {
  import ApplySequencerBench._

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @Warmup(iterations = 5, time = 5, timeUnit = SECONDS)
  @Measurement(iterations = 10, time = 5, timeUnit = SECONDS)
  @Fork(value = 1)
  def sequence(state: BenchmarkState) = {
    implicit val ec: scala.concurrent.ExecutionContext =
      FaunaExecutionContext.Implicits.global
    @switch
    val contendedKeys = state.numContendedKeys match {
      case "1"  => state.contendedKeys1
      case "2"  => state.contendedKeys2
      case "4"  => state.contendedKeys4
      case "8"  => state.contendedKeys8
      case "16" => state.contendedKeys16
    }

    state.ts = state.ts + 1.second

    val sequenceKeys = if (state.rand.nextDouble() <= state.contentionSamplingRate) {
      contendedKeys
    } else {
      List(state.ts.toString())
    }

    // patch call
    state.sequencer.sequence(state.ts, sequenceKeys, state.stats) {
      //
      // master call
      // state.sequencer.sequence(state.ts, sequenceKeys) {
      // doing this to create long write chains to be able to
      // determine if computing them impacts performance
      Future.unit map (_ => 1 + 1)
    }
  }
}

object ApplySequencerBench {
  @State(Scope.Benchmark)
  class BenchmarkState {
    @Param(Array("1", "2", "4", "8", "16"))
    var numContendedKeys: String = ""

    val contendedKeys1 = List("key1")
    val contendedKeys2 = List("key1", "key2")
    val contendedKeys4 = List("key1", "key2", "key3", "key4")
    val contendedKeys8 = (1 to 8).toList.map(i => s"key$i")
    val contendedKeys16 = (1 to 16).toList.map(i => s"key$i")

    val contentionSamplingRate = 0.8

    val rand = new Random(3)

    var ts = Clock.time

    var sequencer = new ApplySequencer[AnyRef]
    val dogstatsdClient = DogStatsDClient("localhost", 8999)
    dogstatsdClient.start()
    var recorder = new BufferedRecorder(
      10.seconds,
      100,
      dogstatsdClient
    )

    var stats = new TxnPipeline.Stats(recorder)
  }
}
