package fauna.net.test

import fauna.exec._
import fauna.lang.syntax._
import fauna.net.Network
import fauna.net.http._
import fauna.trace.{ SpanID, TraceFlags, TraceID }
import io.netty.buffer.{ ByteBuf, Unpooled }
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.util.AsciiString
import java.io.IOException
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, Promise }
import scala.util.Random

class HttpRequestHandlerSpec extends Spec {
  "HttpRequestHandler" - {
    "should handle a non-chunked request" in {
      val randomData = Random.alphanumeric.take(32).mkString.toUTF8Bytes
      val port = findFreePort()
      val client = HttpClient("localhost", port)
      val server = HttpServer(Network.address(port)) { (_, req) =>
        HttpServer.discard(req) map { _ =>
          HttpResponse(HttpResponseStatus.OK, Body(randomData, "text/plain"))
        }
      }

      server.start()

      val result = Await.result(client.get("/test"), Duration.Inf)
      result.code should equal(200)
      Await.result(result.body.data, Duration.Inf).equals(Unpooled.wrappedBuffer(randomData)) should equal(true)
      result.body.maybeRelease()

      server.stop()
    }

    "should handle a chunked request (1 chunk)" in {
      val randomData = Random.alphanumeric.take(4096).mkString.toUTF8Bytes
      val (pub, obs) = Observable.gathering[ByteBuf](OverflowStrategy.unbounded)
      val chunk = Chunked(obs, "text/plain")
      val port = findFreePort()
      val client = HttpClient("localhost", port)
      val server = HttpServer(Network.address(port)) { (_, msg) =>
        msg.body.data.map { chunks =>
          HttpResponse(HttpResponseStatus.OK, Body(chunks, "text/plain"))
        }
      }

      pub.publish(Unpooled.wrappedBuffer(randomData))
      pub.close()
      server.start()

      val result = Await.result(client.post("/test", chunk), Duration.Inf)
      result.code should equal(200)
      Await.result(result.body.data, Duration.Inf).equals(Unpooled.wrappedBuffer(randomData)) should equal(true)
      result.body.maybeRelease()

      server.stop()
    }

    "should handle a chunked request (4 chunks)" in {
      val randomData = Random.alphanumeric.take(8192).mkString.toUTF8Bytes
      val groups = randomData.grouped(2048).map(Unpooled.wrappedBuffer(_)).toSeq
      val (pub, obs) = Observable.gathering[ByteBuf](OverflowStrategy.unbounded)
      val chunks = Chunked(obs, "text/plain")
      val port = findFreePort()
      val client = HttpClient("localhost", port)
      val server = HttpServer(Network.address(port)) { (_, msg) =>
        msg.body.data.map { chunks =>
          HttpResponse(HttpResponseStatus.OK, Body(chunks, "text/plain"))
        }
      }

      groups foreach { pub.publish(_) }
      pub.close()
      server.start()

      val result = Await.result(client.post("/test", chunks), Duration.Inf)
      result.code should equal(200)
      Await.result(result.body.data, Duration.Inf).equals(Unpooled.wrappedBuffer(randomData)) should equal(true)
      result.body.maybeRelease()

      server.stop()
    }

    "should handle an unchunked large request as a chunked request" in {
      val randomData = (Random.alphanumeric.take(16 * 1024).mkString * 125).toUTF8Bytes // Should be 2MB
      val body = Body(Unpooled.wrappedBuffer(randomData), "text/plain", None)
      val port = findFreePort()
      val client = HttpClient("localhost", port)
      val server = HttpServer(Network.address(port)) { (_, msg) =>
        msg.body.events.foldLeftF(0) { _ + _.readableBytes } map { numBytes =>
          HttpResponse(HttpResponseStatus.OK, s"$numBytes bytes")
        }
      }

      server.start()

      val result = Await.result(client.post("/test", body), Duration.Inf)
      result.code should equal (200)
      Await.result(result.body.data, Duration.Inf).equals(Unpooled.wrappedBuffer((s"${randomData.length} bytes").toUTF8Bytes)) should equal (true)
      result.body.maybeRelease()

      server.stop()
    }

    "should reject a large chunked request" in {
      val requestSize = HttpServer.MaxRequestSize / 1024
      val randomData = (Random.alphanumeric.take(requestSize).mkString * (1024 + 256)).toUTF8Bytes
      val groups = randomData.grouped(HttpServer.MaxChunkSize).map(Unpooled.wrappedBuffer(_)).toSeq
      val (pub, obs) = Observable.gathering[ByteBuf](OverflowStrategy.unbounded)
      val chunks = Chunked(obs, "text/plain")
      val port = findFreePort()
      val client = HttpClient("localhost", port)
      val server = HttpServer(Network.address(port)) { (_, msg) =>
        msg.body.data.map { chunks =>
          HttpResponse(HttpResponseStatus.OK, Body(chunks, "text/plain"))
        }
      }

      groups foreach { pub.publish(_) }
      pub.close()
      server.start()

      try {
        val result = Await.result(client.post("/test", chunks), Duration.Inf)
        result.code should equal(413)
      } catch {
        case _: IOException =>
          // We can expect an exception here as proper behavior of the test because the client
          // does not check for the 413 response while it is still sending chunks and will
          // throw a connection reset in the process
      }

      server.stop()
    }

    "should handle a chunked response (2 chunks)" in {
      val randomData = Random.alphanumeric.take(4096).mkString.toUTF8Bytes
      val groups = randomData.grouped(2048).map(Unpooled.wrappedBuffer(_)).toSeq
      val (pub, obs) = Observable.gathering[ByteBuf](OverflowStrategy.unbounded)
      val chunks = Chunked(obs, "text/plain")
      val port = findFreePort()
      val client = HttpClient("localhost", port)
      val server = HttpServer(Network.address(port)) { (_, req) =>
        HttpServer.discard(req) map { _ =>
          HttpResponse(HttpResponseStatus.OK, chunks)
        }
      }

      groups foreach { pub.publish(_) }
      pub.close()
      server.start()

      val result = Await.result(client.get("/test"), Duration.Inf)
      result.code should equal(200)
      Await.result(result.body.data, Duration.Inf).equals(Unpooled.wrappedBuffer(randomData)) should equal(true)
      result.body.maybeRelease()

      server.stop()
    }

    "handles keepalive" in {
      import java.io._
      import java.net._

      val port = findFreePort()
      val server = HttpServer(Network.address(port)) { (_, msg) =>
        HttpServer.discard(msg) map { _ =>
          HttpResponse(HttpResponseStatus.OK,
                       Body(msg.path.get + "\n", "text/plain"))
        }
      }

      server.start()

      val sock = new Socket("localhost", port)
      val cin = new BufferedReader(new InputStreamReader(sock.getInputStream))
      val cout = new PrintWriter(sock.getOutputStream, false)

      (new Thread(new Runnable { def run = { Thread.sleep(10000); sock.close() } })).start()

      for (i <- 0 to 5) {
        cout.println(s"GET /r$i HTTP/1.1")
        cout.println("Connection: keep-alive")
        cout.println("")
        cout.flush()

        cin.readLine should equal ("HTTP/1.1 200 OK")
        cin.readLine should equal ("connection: keep-alive")
        cin.readLine should startWith ("traceparent: ")
        cin.readLine should equal ("content-length: 4")
        cin.readLine should equal ("content-type: text/plain")
        cin.readLine should equal ("")
        cin.readLine should equal (s"/r$i")
      }

      cout.println("GET /rX HTTP/1.1")
      cout.println("Connection: close")
      cout.println("")
      cout.flush()

      cin.readLine should equal ("HTTP/1.1 200 OK")
      cin.readLine should equal ("connection: close")
      cin.readLine should startWith ("traceparent: ")
      cin.readLine should equal ("content-length: 4")
      cin.readLine should equal ("content-type: text/plain")
      cin.readLine should equal ("")
      cin.readLine should equal ("/rX")

      sock.close()
      server.stop()
    }

    "cancels outstanding work when a client closes the connection" in pendingUntilFixed {
      import java.io._
      import java.net._

      val respP = Promise[HttpResponse]()

      val port = findFreePort()
      val server = HttpServer(Network.address(port)) { (_, req) =>
        HttpServer.discard(req) before {
          respP.future
        }
      }

      server.start()

      val sock = new Socket("localhost", port)
      val cout = new PrintWriter(sock.getOutputStream, false)

      (new Thread(new Runnable { def run = { Thread.sleep(10000); sock.close() } })).start()

      cout.println("GET /rX HTTP/1.1")
      cout.println("Connection: close")
      cout.println("")
      cout.flush()

      sock.close()

      // give it a little time to respond to the closed socket
      //eventually { respP.isInterrupted should be ('defined) }

      //respP.isInterrupted.get shouldBe a[ChannelClosedException]

      server.stop()
      throw new Exception("Need a way to cancel scala Futures")
    }
  }

  "traceparent header" - {
    implicit val rng: Random = ThreadLocalRandom.current()

    "no traceparent header" in {
      val randomData = Random.alphanumeric.take(32).mkString.toUTF8Bytes
      val port = findFreePort()
      val client = HttpClient("localhost", port)
      val server = HttpServer(Network.address(port)) { (_, req) =>
        req.traceContext.traceID should not be new TraceID(0x00, 0x00)
        req.traceContext.flags should be(TraceFlags.Default)

        HttpServer.discard(req) map { _ =>
          HttpResponse(HttpResponseStatus.OK, Body(randomData, "text/plain"))
        }
      }

      server.start()

      val result = Await.result(client.get("/test"), Duration.Inf)
      result.code should equal(200)
      Await
        .result(result.body.data, Duration.Inf)
        .equals(Unpooled.wrappedBuffer(randomData)) should equal(true)
      result.body.maybeRelease()

      server.stop()
    }

    "traceparent header set & flags are cleared" in {
      val randomData = Random.alphanumeric.take(32).mkString.toUTF8Bytes
      val port = findFreePort()
      val traceID = TraceID.randomID
      val client = HttpClient("localhost", port)
      val server = HttpServer(Network.address(port)) { (_, req) =>
        req.traceContext.traceID should be(traceID)
        req.traceContext.flags should be(TraceFlags.Default)

        HttpServer.discard(req) map { _ =>
          HttpResponse(HttpResponseStatus.OK, Body(randomData, "text/plain"))
        }
      }

      server.start()

      val result = Await.result(
        client.get(
          "/test",
          headers = Seq(
            new AsciiString("traceparent") ->
              s"00-${traceID.toHexString}-${SpanID.randomID.toHexString}-23")),
        Duration.Inf)
      result.code should equal(200)
      Await
        .result(result.body.data, Duration.Inf)
        .equals(Unpooled.wrappedBuffer(randomData)) should equal(true)
      result.body.maybeRelease()

      server.stop()
    }
  }
}
