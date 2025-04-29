package fauna.net.test

import fauna.lang.syntax._
import fauna.net.http._
import fauna.net.http.HttpResponse
import fauna.net.netty.SimpleNettyClient
import fauna.net.security._
import fauna.net.Network
import fauna.stats.{ QueryMetrics, StatsRequestBuffer }
import io.netty.handler.codec.http.HttpResponseStatus
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.test.appender.ListAppender
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.Level
import org.scalatest.concurrent.Eventually
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class TCPPingClient extends SimpleNettyClient() {
  def connectAndClose(port: Int): Future[Unit] =
    connect("localhost", port, { _ => () }) flatMap { ch =>
      ch.close().toFuture.unit
    }
}

class HttpServerSpec extends Spec with Eventually {
  "SSL" - {
    "server can handle a closed connection in the middle of a handshake" in {
      val layout = PatternLayout.newBuilder().withPattern("%d %C %L %m").build()
      val listAppender = new ListAppender("List", null, layout, false, false)
      listAppender.start()

      val loggerCtx = LoggerContext.getContext(false)
      val alpnLogger = loggerCtx.getLogger(
        "io.netty.handler.ssl.ApplicationProtocolNegotiationHandler")
      alpnLogger.addAppender(listAppender)

      alpnLogger.setLevel(Level.WARN)

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

        val cli = new TCPPingClient
        Await.result(cli.connectAndClose(port), 5.seconds)

        // Ick. We have to wait for the server to see the close and try to log
        // the exception. I'm not sure of a better way than this.
        Thread.sleep(1000)

        listAppender.getMessages.size should equal(0)
      } finally {
        server.stop()
      }
    }
  }

  "Stats" - {
    "server sets connection stats" in {
      val stats = new StatsRequestBuffer()
      val port = findFreePort()
      val server =
        HttpServer(Network.address(port), stats = stats) { (_, req) =>
          HttpServer.discard(req) map { _ =>
            HttpResponse(HttpResponseStatus.OK, Body("ok", "text/plain"))
          }
        }
      val client = Http2Client("localhost", port)

      try {
        server.start()

        Await.result(client.get("/"), 5.seconds)

        stats.countOpt("HTTP.Connections.Opened") should equal(Some(1))
        stats.countOpt(QueryMetrics.BytesIn) should equal(Some(80))
        stats.countOpt(QueryMetrics.BytesOut) should equal(Some(
          66 + "traceparent: 00-c84c0dccfcf88d1fdc056d90c271eccf-5eea1b9da1fe45a2-00\n".length))

        client.stop()

        // might take a bit for the connection to register as closed
        eventually {
          stats.getD("HTTP.Connections.Duration") shouldNot equal(0.0)
          stats.countOpt("HTTP.Connections.Closed") should equal(Some(1))
        }
      } finally {
        server.stop()
      }
    }
  }
}
