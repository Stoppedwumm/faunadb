package benchmarks.repo.service.stream

import fauna.exec.{ FaunaExecutionContext, Observer }
import fauna.lang.Timestamp
import fauna.lang.syntax._
import fauna.repo.service.stream.Source
import java.util.concurrent.atomic.AtomicLong
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole
import scala.concurrent.duration._
import scala.concurrent.{ Await, Promise }

@Fork(1)
@State(Scope.Benchmark)
class SourceBench {

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
      "8192",
      "16384",
      "32768",
      "65536",
      "131072",
      "262144",
      "524288",
      "1048576"))
  var keys = ""

  val txnClock = new AtomicLong(0)

  val source =
    new Source[Int, Int](
      () => Timestamp.ofMicros(txnClock.get()),
      maxBuffer = Int.MaxValue,
      ttl = Duration.Inf,
      idlePeriod = 300.millis,
      maxIdleOverhead = 256
    )

  @Setup
  def setup(): Unit = {
    for (i <- 0 until keys.toInt)
      source.register(i)

    new Thread(() => {
      val done = Promise[Unit]()
      source.output.subscribe(new Observer[Source.Output[Int, Int]] {
        implicit val ec = FaunaExecutionContext.Implicits.global
        def onNext(value: Source.Output[Int, Int]) = Observer.ContinueF
        def onError(cause: Throwable) = { done.setDone(); throw cause }
        def onComplete() = done.setDone()
      })
      Await.result(done.future, Duration.Inf)
    }).start()
  }

  @TearDown
  def teardown(): Unit =
    source.close()

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  def idleStreamsThroughput(hole: Blackhole) = {
    // simulate an txn epoch at every 10 micros; push no values so that the only
    // work done by the thread is to drain and publish idle keys
    val ts = Timestamp.ofMicros(txnClock.addAndGet(10))
    hole.consume(source.publish(ts, ts, Map.empty))
  }
}
