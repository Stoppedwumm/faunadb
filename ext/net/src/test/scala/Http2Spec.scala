package fauna.net.test

import fauna.exec.{ Observable, OverflowStrategy }
import fauna.lang.syntax._
import fauna.net.Network
import fauna.net.http._
import fauna.net.netty.{ ChannelClosedException, SimpleNettyClient }
import fauna.net.security._
import io.netty.buffer.ByteBuf
import io.netty.channel._
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.http.{ HttpRequest => _, HttpResponse => _, _ }
import io.netty.handler.codec.http2._
import io.netty.handler.ssl._
import io.netty.util.{ AsciiString, CharsetUtil }
import java.net.URI
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{ HttpClient => JHttpClient, HttpRequest => JHttpRequest }
import java.util.stream.Collectors
import javax.net.ssl.SSLException
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future, Promise }
import scala.util.Random

abstract class TestHttp2Client(
  host: AsciiString,
  port: Int,
  clientSSL: Option[ClientSSL] = None
) extends SimpleNettyClient(Duration.Zero)
    with AbstractHttpClient {
  val ssl = SSLConfig(None, clientSSL)

  protected def doInit(
    ch: SocketChannel,
    req: HttpRequest,
    resP: Promise[HttpResponse]): Unit

  def sendRequest(
    req: HttpRequest,
    setContentLength: Boolean): Future[HttpResponse] = {
    val responseP = Promise[HttpResponse]()

    // Schema MUST be set.
    req.raw.headers.add(
      HttpConversionUtil.ExtensionHeaderNames.SCHEME.text,
      if (ssl.sslEnabled) HttpScheme.HTTPS.name else HttpScheme.HTTP.name)

    connect(host.toString, port, doInit(_, req, responseP)).unit before responseP.future
  }

  protected def httpToHttp2Handler = {
    val connection = new DefaultHttp2Connection(false)
    new HttpToHttp2ConnectionHandlerBuilder()
      .frameListener(
        new DelegatingDecompressorFrameListener(
          connection,
          new InboundHttp2ToHttpAdapterBuilder(connection)
            .maxContentLength(8 * 1024 * 1024)
            .propagateSettings(true)
            .build()))
      .connection(connection)
      .build()
  }

  def isRunning = isInitialized
  def start() = init()
  def stop(graceful: Boolean) = clear()
}

case class UpgradingHttp2Client(host: AsciiString, port: Int)
    extends TestHttp2Client(host, port) {

  private val initReq = {
    val message = "foo".toUTF8Buf
    val req =
      new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/", message)
    req.headers().add(HttpHeaderNames.CONTENT_LENGTH, message.readableBytes())
    req
  }

  protected def doInit(
    ch: SocketChannel,
    req: HttpRequest,
    resP: Promise[HttpResponse]) = {
    val sourceCodec = new HttpClientCodec()
    val upgradeCodec = new Http2ClientUpgradeCodec(httpToHttp2Handler)
    val upgradeHandler =
      new HttpClientUpgradeHandler(sourceCodec, upgradeCodec, 65536)

    ch.pipeline.addLast("codec", sourceCodec)
    ch.pipeline.addLast("upgrade", upgradeHandler)

    // Kick off the upgrade then send the real request.
    ch.pipeline.addLast(
      "upgrade request",
      new ChannelInboundHandlerAdapter {
        override def channelActive(ctx: ChannelHandlerContext): Unit = {
          ctx.writeAndFlush(initReq)
          ctx.pipeline.remove(this)

          ctx.pipeline.addLast("handler", new HttpResponseHandler(resP, false))

          // After the SETTINGS header arrives, our upgrade is complete
          ctx.pipeline.addLast(
            "sender",
            new SimpleChannelInboundHandler[Http2Settings] {
              override def channelRead0(
                ctx: ChannelHandlerContext,
                msg: Http2Settings): Unit =
                req writeTo ctx.channel
            }
          )
        }
      }
    )
  }
}

case class SSLHttp2Client(host: AsciiString, port: Int, clientSSL: Option[ClientSSL])
    extends TestHttp2Client(host, port, clientSSL) {
  protected def doInit(
    ch: SocketChannel,
    req: HttpRequest,
    resP: Promise[HttpResponse]) = {

    ssl
      .withProtoConfig(HttpServer.Http2ProtocolConfig)
      .getClientHandler(ch.alloc, Some((host.toString, port))) foreach {
      ch.pipeline.addLast("ssl", _)
    }

    // Negotiate the protocol before setting up the http->http2 pipeline and sending the request.
    ch.pipeline.addLast(
      "alpn",
      new ApplicationProtocolNegotiationHandler("") {
        override protected def configurePipeline(
          ctx: ChannelHandlerContext,
          prot: String): Unit =
          if (ApplicationProtocolNames.HTTP_2.equals(prot)) {
            ctx.pipeline.addBefore("handler", "http2 handler", httpToHttp2Handler)
            req writeTo ctx.channel

          } else {
            ctx.close()
            throw new IllegalStateException(s"unknown protocol: $prot")
          }
      }
    )

    // We setup our handler before negotiation to catch and handle errors.
    ch.pipeline.addLast("handler", new HttpResponseHandler(resP, false))
  }
}

class Http2Spec extends Spec {
  val localhost = new AsciiString("localhost")

  "ClearText" - {
    "server can handle an HTTP/2 upgrade" in {
      val randomData = Random.alphanumeric.take(32).mkString
      val port = findFreePort()
      val client = UpgradingHttp2Client(localhost, port)
      val server = HttpServer(Network.address(port)) { (_, req) =>
        HttpServer.discard(req) map { _ =>
          HttpResponse(
            HttpResponseStatus.OK,
            Body(randomData.toUTF8Bytes, "text/plain")
          )
        }
      }

      server.start()

      val result = Await.result(client.get("/test"), 5.seconds)
      Await.result(result.body.data, 5.seconds).toString(CharsetUtil.UTF_8) should equal(randomData)
      server.stop()
    }

    "server can handle an HTTP/2 prior knowledge connection" in {
      val randomData = Random.alphanumeric.take(32).mkString
      val port = findFreePort()
      val client = Http2Client(localhost.toString(), port)
      val server = HttpServer(Network.address(port)) { (_, req) =>
        HttpServer.discard(req) map { _ =>
          HttpResponse(
            HttpResponseStatus.OK,
            Body(randomData.toUTF8Bytes, "text/plain")
          )
        }
      }

      server.start()

      val result = Await.result(client.get("/test"), 5.seconds)
      Await.result(result.body.data, 5.seconds).toString(CharsetUtil.UTF_8) should equal(randomData)
      server.stop()
    }
  }

  "SSL" - {
    "server can handle ALPN" in {
      val key = testPEMFile("private_key1.pem")
      val sKey = KeySource(key)
      val sTrust = TrustSource(key)

      val port = findFreePort()
      val server =
        HttpServer(Network.address(port), ssl = SSL(sKey, sTrust, false)) {
          (_, req) =>
            HttpServer.discard(req) map { _ =>
              HttpResponse(HttpResponseStatus.OK, Body("ok", "text/plain"))
            }
        }
      server.start()

      val good = SSLHttp2Client(localhost, port, SSL(sKey, sTrust, false).client)
      val res = Await.result(good.get(""), 5.seconds)
      res.code should equal(200)
      res.body.maybeRelease()

      // Client key and trust not configured. Client rejects server.
      val bad1 = SSLHttp2Client(localhost, port, DefaultSSL.client)
      val ex1 = intercept[Exception] {  Await.result(bad1.get(""), 5.seconds) }
      assert(
        ex1.isInstanceOf[SSLException] || ex1.isInstanceOf[ChannelClosedException])

      // Client trust not configured. Client rejects server.
      val bad2 = SSLHttp2Client(localhost, port, SSL(sKey, false).client)
      val ex2 = intercept[Exception] {  Await.result(bad2.get(""), 5.seconds) }
      assert(
        ex2.isInstanceOf[SSLException] || ex2.isInstanceOf[ChannelClosedException])

      // Client key not configured. Server rejects client.
      val bad3 = SSLHttp2Client(localhost, port, SSL(sTrust).client)
      val ex3 = intercept[Exception] { Await.result(bad3.get(""), 5.seconds) }
      assert(
        ex3.isInstanceOf[SSLException] || ex3.isInstanceOf[ChannelClosedException])

      server.stop()
    }

    "server can handle SSL with http2 prior knowledge" in {
      val key = testPEMFile("private_key1.pem")
      val sKey = KeySource(key)
      val sTrust = TrustSource(key)
      val port = findFreePort()
      val ssl = SSL(sKey, sTrust, priv = false)
      val server =
        HttpServer(Network.address(port), ssl = ssl) { (_, req) =>
          HttpServer.discard(req) map { _ =>
            HttpResponse(HttpResponseStatus.OK, Body("ok", "text/plain"))
          }
        }

      try {
        server.start()
        val cli = Http2Client(
          localhost.toString(),
          port,
          Some(ssl),
          enableEndpointIdentification = false)
        val res = Await.result(cli.get("/test"), 5.seconds)
        res.code should equal(200)
        res.body.maybeRelease()
      } finally {
        server.stop()
      }
    }
  }

  "Mixed Protocols" - {
    // This scenario uses the native JVM http client as the means to simulate a bug
    // found in the JVM driver where a keep alive connection with mixed HTTP1 and
    // HTTP2 protocol would leave a lingering idle timeout attached to the root of
    // the HTTP2 channel and prematurely close the connection.
    "server keeps connection alive after upgrade" in {
      val port = findFreePort()
      val (pub, obs) = Observable.gathering(OverflowStrategy.unbounded[ByteBuf])

      val server =
        HttpServer(Network.address(port), keepAliveTimeout = 100.millis) {
          (_, req) =>
            HttpServer.discard(req) map { _ =>
              val body =
                if (req.path.contains("/stream")) {
                  Chunked(obs, "text/plain")
                } else {
                  Body("ok", "text/plain")
                }
              HttpResponse(HttpResponseStatus.OK, body)
            }
        }

      def mkReq(version: JHttpClient.Version, path: String) = {
        val uri = URI.create(s"http://$localhost:$port$path")
        JHttpRequest.newBuilder().version(version).uri(uri).GET().build()
      }

      try {
        server.start()
        val client = JHttpClient.newBuilder().build()
        val http1Req = mkReq(JHttpClient.Version.HTTP_1_1, path = "/")
        val http1Res = client.send(http1Req, BodyHandlers.discarding())
        http1Res.statusCode shouldBe 200

        val http2Req = mkReq(JHttpClient.Version.HTTP_2, path = "/stream")
        val http2Res = client.send(http2Req, BodyHandlers.ofLines())
        http2Res.statusCode shouldBe 200

        Thread.sleep(200) // sleep longer than the idle time
        assert(pub.publish("foo\n".toUTF8Buf), ", connection was closed!")
        assert(pub.publish("bar\n".toUTF8Buf), ", connection was closed!")
        pub.close()

        val lines = http2Res.body().collect(Collectors.toList())
        lines should contain.inOrderOnly("foo", "bar")
      } finally {
        server.stop()
      }
    }
  }
}
