package fauna.api

import fauna.api.util.RequestLogging
import fauna.atoms.Build
import fauna.codex.json.JSObject
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.net.http._
import fauna.net.security.SSLConfig
import fauna.stats._
import io.netty.channel.unix.Errors$NativeIoException
import io.netty.handler.codec.{ DecoderException, TooLongFrameException }
import io.netty.handler.timeout.{
  ReadTimeoutException => NettyReadTimeout,
  WriteTimeoutException => NettyWriteTimeout
}
import java.net.{ SocketAddress, URISyntaxException }
import java.util.concurrent.CancellationException
import javax.net.ssl.SSLException
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

abstract class Application extends ExceptionLogging with RequestLogging {

  def networkHostID: String
  def stats: StatsRecorder
  def handleRequest(
    info: HttpRequestChannelInfo,
    httpReq: HttpRequest): Future[HttpResponse]

  def server(
    addr: SocketAddress,
    readTimeout: FiniteDuration,
    keepAliveTimeout: FiniteDuration,
    ssl: SSLConfig,
    maxInitialLineLength: Int = HttpServer.MaxInitialLineLength,
    maxHeaderSize: Int = HttpServer.MaxHeaderSize,
    maxChunkSize: Int = HttpServer.MaxChunkSize,
    maxConcurrentStreams: Int = Int.MaxValue) =
    HttpServer(
      addr,
      handleChannelException,
      stats = stats,
      ssl = ssl,
      readTimeout = readTimeout,
      keepAliveTimeout = keepAliveTimeout,
      maxInitialLineLength = maxInitialLineLength,
      maxHeaderSize = maxHeaderSize,
      maxChunkSize = maxChunkSize,
      maxConcurrentStreams = maxConcurrentStreams)(handler)

  private def handler(
    info: HttpRequestChannelInfo,
    req: HttpRequest): Future[HttpResponse] = {
    handleRequest(info, req) map { res =>
      if (req.tracingEnabled) {
        res.setHeader(HTTPHeaders.FaunaDBHost, networkHostID)
      }

      res.setHeader(HTTPHeaders.FaunaDBBuild, Build.identifier)

      res
    }
  }

  /** Handle an uncaught exception in the netty handler. */
  private def handleChannelException(
    info: HttpRequestChannelInfo,
    e: Throwable): Future[HttpResponse] = {

    def badRequest = HttpResponse(400, JSObject("error" -> "bad request"))
    def requestTimeout(msg: String) =
      HttpResponse(408, JSObject("error" -> s"request timeout: $msg"))
    def internalError =
      HttpResponse(500, JSObject("error" -> "internal server error"))

    val res = e match {
      case _: URISyntaxException       => badRequest
      case _: IllegalArgumentException => badRequest
      case _: SSLException             => badRequest
      case _: Errors$NativeIoException => badRequest
      case _: TooLongFrameException    => badRequest
      case _: DecoderException         => badRequest
      case _: NettyWriteTimeout        => requestTimeout("Client timed out.")
      case _: NettyReadTimeout         => requestTimeout("Client timed out.")
      case _: CancellationException =>
        requestTimeout("Request was cancelled.")
      case _ =>
        getLogger.error(s"Unhandled channel exception: $e", e)
        logException(e)
        internalError
    }

    logRequest(None, APIEndpoint.Response(res), info)
    Future.successful(res)
  }
}
