package fauna.net.http

import fauna.exec._
import fauna.exec.FaunaExecutionContext
import fauna.lang.syntax._
import fauna.net.security.SSL
import fauna.trace.{
  Attributes,
  GlobalTracer,
  InternalError,
  Server,
  Span,
  Status,
  TraceContext,
  TraceFlags
}
import io.netty.buffer.ByteBuf
import io.netty.channel.{ ChannelHandlerContext, SimpleChannelInboundHandler }
import io.netty.handler.codec.http.{
  HttpRequest => NettyRequest,
  HttpHeaders => _,
  HttpResponse => _,
  _
}
import java.net.InetSocketAddress
import scala.concurrent.Future
import scala.util.{ Failure, Success }

object HttpRequestChannelInfo {
  def fromContext(ctx: ChannelHandlerContext): HttpRequestChannelInfo =
    HttpRequestChannelInfo(ctx.channel.remoteAddress.asInstanceOf[InetSocketAddress])
}

case class HttpRequestChannelInfo(remoteAddr: InetSocketAddress)

object HttpRequestHandler {
  object DefaultExceptionHandlerF extends HttpExceptionHandlerF {
    private lazy val log = getLogger

    def apply(info: HttpRequestChannelInfo, e: Throwable): Future[HttpResponse] =
      e match {
        case RequestTooLarge =>
          Future.successful(
            HttpResponse(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, NoBody))
        case _ =>
          log.error("Unhandled Exception", e)
          Future.successful(
            HttpResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, NoBody))
      }
  }

  def apply(handlerF: HttpHandlerF): HttpRequestHandler =
    HttpRequestHandler(handlerF, DefaultExceptionHandlerF)
}

case class HttpRequestHandler(
  handlerF: HttpHandlerF,
  exceptionF: HttpExceptionHandlerF)
    extends SimpleChannelInboundHandler[HttpObject](false) {

  implicit private[this] val ec = ImmediateExecutionContext
  private[this] var request: Option[Publisher[ByteBuf]] = None

  override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable): Unit = {
    exceptionF(HttpRequestChannelInfo.fromContext(ctx), e) flatMap { response =>
      val tracer = GlobalTracer.instance
      tracer.activeSpan foreach {
        _.setStatus(Status.forHTTP(response.code, response.status.reasonPhrase))
      }

      if (ctx.channel.isWritable) {
        response.writeTo(ctx.channel)
      } else {
        Future.unit
      }
    } ensure {
      ctx.channel.closeQuietly()
    }
  }

  /**
    * All requests are composed of multiple parts, regardless of
    * chunked encoding: 1 HttpRequest packet, followed by 0 or more
    * HttpContent packets, terminated by 1 LastHttpContent packet.
    */
  override def channelRead0(ctx: ChannelHandlerContext, req: HttpObject): Unit =
    (req: @unchecked) match {
      case message: NettyRequest =>
        // This will parse the traceparent header. x-fauna-tags is parsed
        // later. It will use the requested traceID, which is replace below
        // when we start the span.
        //
        // If x-trace-secret is provided and matches traceparent will be
        // accepted as-is. If it is not, we clear the flags to take control
        // of whether we trace or not.
        val traceParent = if (message.headers.contains(HTTPHeaders.TraceParent)) {
          TraceContext.fromHTTPHeader(
            message.headers.get(HTTPHeaders.TraceParent)) map { ctx =>
            val traceSecret = Option(message.headers.get(HTTPHeaders.TraceSecret))
            if (
              GlobalTracer.instance.headerSecret.isDefined && traceSecret == GlobalTracer.instance.headerSecret
            ) {
              ctx
            } else {
              ctx.copy(flags = TraceFlags.Default)
            }
          } getOrElse {
            TraceContext.random()
          }
        } else {
          TraceContext.random()
        }

        val tracer = GlobalTracer.instance
        val b = tracer.buildSpan(message.method.asciiName)
          .withKind(Server)
          .ignoreParent()

        val auth = AuthType.fromHeaders(message.headers())
        auth match {
          case BasicAuth(secret, _)  => b.withSecret(secret)
          case BearerAuth(secret, _) => b.withSecret(secret)
          case _                     => ()
        }

        b.withParent(traceParent)

        // This will use the parent given on b.withParent(), and generate a new
        // traceID for this request.
        val (span, traceContext) = b.startWithContext()
        val scope = tracer.activate(span)

        try {
          val isHTTP2 = message.headers.contains("x-http2-stream-id")
          val contentType =
            message.headers.get(HTTPHeaders.ContentType, ContentType.JSON)

          setTraceMetadata(ctx, message, isHTTP2, span)

          // Handles decoder failures (e.g. bad requests)
          if (!req.decoderResult.isSuccess) {
            exceptionCaught(ctx, req.decoderResult.cause)
          } else {

            // write out 100 continue if expected
            // FIXME: Send this conditionally depending on auth, route, etc.
            if (HttpUtil.is100ContinueExpected(message)) {
              HttpResponse(HttpResponseStatus.CONTINUE, NoBody).writeTo(ctx.channel)
            }

            val body = message match {
              case m: FullHttpMessage =>
                Body(m.content, contentType, Some(HttpServer.MaxRequestSize))
              case _ =>
                val contentLength = HttpUtil.getContentLength(message, 0)
                val isChunked = HttpUtil.isTransferEncodingChunked(message)
                if (isChunked || contentLength > 0) {
                  val (publisher, buffers) =
                    Observable.gathering[ByteBuf](
                      OverflowStrategy.callbackOnOverflow { _.release() })(
                      FaunaExecutionContext.Implicits.global)

                  request = Some(publisher)
                  Chunked(
                    buffers,
                    contentType,
                    Some(contentLength),
                    Some(HttpServer.MaxRequestSize))
                } else {
                  NoBody
                }
            }

            val responseF =
              Future.delegate(
                handlerF(
                  HttpRequestChannelInfo.fromContext(ctx),
                  new HttpRequest(message, body, auth, traceContext)))(
                FaunaExecutionContext.Implicits.global)

            responseF onComplete {
              case Success(response) =>
                val isKeepAlive = HttpUtil.isKeepAlive(message)
                val connVal = if (isKeepAlive) {
                  HttpHeaderValues.KEEP_ALIVE
                } else {
                  HttpHeaderValues.CLOSE
                }

                span.setStatus(Status.forHTTP(response.code, response.status.reasonPhrase))
                span.addAttribute(Attributes.HTTP.StatusCode, response.code)

                response.setHeader(HTTPHeaders.Connection, connVal)

                response.setHeader(HTTPHeaders.TraceParent, traceContext.toHTTPHeader)

                response.writeTo(ctx.channel, message.method == HttpMethod.HEAD).unit ensure {
                  body.maybeRelease()
                } andThen {
                  case Success(_) => if (!isKeepAlive) ctx.channel.close()
                  case Failure(t) => exceptionCaught(ctx, t)
                }

              case Failure(e) =>
                span.setStatus(InternalError(e.getMessage))
                body.maybeRelease()
                exceptionCaught(ctx, e)
            }
          }
        } finally {
          scope foreach { _.close() }
        }
      case chunk: LastHttpContent =>
        request match {
          case None =>
            if (chunk.content.readableBytes > 0)
              throw MessageOutOfOrderException
          case Some(publisher) =>
            publisher.publish(chunk.content)
            publisher.close()
            request = None
        }
      case chunk: HttpContent =>
        request match {
          case None            => throw MessageOutOfOrderException
          case Some(publisher) => publisher.publish(chunk.content)
        }
    }

  private def setTraceMetadata(ctx: ChannelHandlerContext, message: NettyRequest, isHTTP2: Boolean, span: Span) =
    if (span.isSampled) {
      span.addAttribute(Attributes.Component, HttpScheme.HTTP.name)

      if (Option(ctx.pipeline.get(SSL.HandlerName)).isDefined) {
        span.addAttribute(Attributes.HTTP.Scheme, HttpScheme.HTTPS.name)
      } else {
        span.addAttribute(Attributes.HTTP.Scheme, HttpScheme.HTTP.name)
      }
      span.addAttribute(Attributes.HTTP.Method, message.method.asciiName)

      val local = ctx.channel.localAddress.asInstanceOf[InetSocketAddress]
      span.addAttribute(Attributes.Net.HostName, local.getHostString)
      span.addAttribute(Attributes.Net.HostPort, local.getPort)
      span.addAttribute(Attributes.HTTP.Target, message.uri)

      Option(message.headers.get(HttpHeaderNames.HOST)) foreach { host =>
        span.addAttribute(Attributes.HTTP.Host, host)
      }

      Option(message.headers.get(HttpHeaderNames.USER_AGENT)) foreach { agent =>
        span.addAttribute(Attributes.HTTP.UserAgent, agent)
      }

      // network peer and client will be different if the client
      // is speaking through a proxy
      val remote = ctx.channel.remoteAddress.asInstanceOf[InetSocketAddress]
      val peer = remote.getAddress.getHostAddress
      span.addAttribute(Attributes.Net.PeerIP, peer)
      span.addAttribute(Attributes.HTTP.ClientIP,
        message.headers.get(HTTPHeaders.ForwardedFor, peer))

      val version = if (isHTTP2) {
        Attributes.HTTP.V2
      } else {
        message.protocolVersion match {
          case HttpVersion.HTTP_1_0 => Attributes.HTTP.V10
          case HttpVersion.HTTP_1_1 => Attributes.HTTP.V11
          case _                    => Attributes.HTTP.VUnknown
        }
      }

      span.addAttribute(Attributes.HTTP.Flavor, version)
    }

}
