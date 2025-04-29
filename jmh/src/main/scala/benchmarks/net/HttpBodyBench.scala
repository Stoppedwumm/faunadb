package benchmarks.net.http

import fauna.exec.{ FaunaExecutionContext, Observable, OverflowStrategy }
import fauna.net.http.{ Chunked, HttpServer }
import io.netty.buffer.{ ByteBuf, Unpooled }
import org.openjdk.jmh.annotations._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Random

object ChunkedBench {

  val buffers =
    (0 to 13) map { n =>
      val kbytes = math.pow(2, n).toInt
      val buffers = genBuffs(bytes = kbytes * 1024)
      kbytes.toString -> buffers
    } toMap

  private def genBuffs(bytes: Int): Vector[ByteBuf] = {
    def buff(n: Int) = Unpooled.wrappedBuffer(Random.nextBytes(n)).asReadOnly()
    val buffers = Vector.fill(bytes / HttpServer.MaxChunkSize) {
      buff(HttpServer.MaxChunkSize)
    }

    val remaining = bytes % HttpServer.MaxChunkSize
    if (remaining > 0) {
      buffers :+ buff(remaining)
    } else {
      buffers
    }
  }
}

@Fork(1)
@State(Scope.Benchmark)
class ChunkedBench {
  import ChunkedBench._
  import FaunaExecutionContext.Implicits.global

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
  var size = ""

  @Benchmark
  def data() = {
    val (pub, obs) = Observable.gathering[ByteBuf](OverflowStrategy.unbounded)
    val chunked = Chunked(obs, contentType = "text/plain")
    val dataF = chunked.data

    buffers(size) foreach { buf =>
      buf.retain()
      pub.publish(buf)
    }
    pub.close()

    val data = Await.result(dataF, Duration.Inf)
    data.release()
    data
  }
}
