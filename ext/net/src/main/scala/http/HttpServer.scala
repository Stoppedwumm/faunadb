package fauna.net.http

import fauna.exec.ImmediateExecutionContext
import fauna.lang.syntax._
import fauna.net.netty.{ SimpleNettyServer, UseEpoll, UseIOUring }
import fauna.net.security.{ NoSSL, SSL, SSLConfig }
import fauna.stats.StatsRecorder
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel._
import io.netty.channel.epoll.EpollChannelOption
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.http.{
  HttpMessage => NettyMessage,
  HttpServerCodec,
  HttpServerUpgradeHandler
}
import io.netty.handler.codec.http.HttpServerUpgradeHandler.{
  UpgradeCodec,
  UpgradeCodecFactory
}
import io.netty.handler.codec.http2._
import io.netty.handler.codec.http2.Http2CodecUtil.{
  HTTP_UPGRADE_PROTOCOL_NAME => UpgradeName
}
import io.netty.handler.logging._
import io.netty.handler.ssl._
import io.netty.handler.ssl.ApplicationProtocolConfig._
import io.netty.incubator.channel.uring._
import io.netty.util.{ AsciiString, ReferenceCountUtil }
import java.lang.{ Boolean => JBoolean }
import java.net.SocketAddress
import java.nio.channels.ClosedChannelException
import scala.concurrent.duration._
import scala.concurrent.Future
import HttpRequestHandler._

object HttpServer {
  val MaxInitialLineLength = 16 * 1024
  val MaxHeaderSize = 8 * 1024
  val MaxChunkSize = 16 * 1024
  val MaxRequestSize = 8 * 1024 * 1024
  val DefaultClientReadTimeout = 60.seconds
  val DefaultKeepAliveTimeout = 5.seconds

  // Dead connections will remain open for
  // KeepAliveTime + (KeepAliveInterval * KeepAliveProbes)
  val KeepAliveTime = 120 // seconds
  val KeepAliveInterval = 30 // seconds
  val KeepAliveProbes = 6 // count

  // see: https://blog.cloudflare.com/when-tcp-sockets-refuse-to-die/
  val TCPUserTimeout =
    (KeepAliveTime + (KeepAliveInterval * KeepAliveProbes)) * 1000 // msecs.

  // Setting this key will enable Netty's logging handler at the INFO level.
  // This will only enable it for new channels.
  val LoggingSyspropKey = "fauna.httpserver.info.logging"

  val Http2ProtocolConfig =
    new ApplicationProtocolConfig(
      Protocol.ALPN,
      // NO_ADVERTISE is currently the only mode supported by both OpenSsl and JDK
      // providers.
      SelectorFailureBehavior.NO_ADVERTISE,
      // ACCEPT is currently the only mode supported by both OpenSsl and JDK
      // providers.
      SelectedListenerFailureBehavior.ACCEPT,
      ApplicationProtocolNames.HTTP_2,
      ApplicationProtocolNames.HTTP_1_1
    )

  def apply(
    addr: SocketAddress,
    exceptionF: HttpExceptionHandlerF = DefaultExceptionHandlerF,
    readTimeout: Duration = DefaultClientReadTimeout,
    keepAliveTimeout: Duration = DefaultKeepAliveTimeout,
    ssl: SSLConfig = NoSSL,
    maxInitialLineLength: Int = HttpServer.MaxInitialLineLength,
    maxHeaderSize: Int = HttpServer.MaxHeaderSize,
    maxChunkSize: Int = HttpServer.MaxChunkSize,
    maxConcurrentStreams: Int = Int.MaxValue,
    stats: StatsRecorder = StatsRecorder.Null
  )(handlerF: HttpHandlerF): HttpServer =
    new HttpServer(
      addr,
      readTimeout,
      keepAliveTimeout,
      ssl,
      stats,
      () => HttpRequestHandler(handlerF, exceptionF),
      maxInitialLineLength,
      maxHeaderSize,
      maxChunkSize,
      maxConcurrentStreams)

  /** Gathers all request's data before releasing its body to avoid memory leaks. */
  def discard(request: HttpRequest): Future[Unit] = {
    implicit val ec = ImmediateExecutionContext
    request.body.data.unit ensure {
      request.body.maybeRelease()
    }
  }
}

class HttpServer(
  addr: SocketAddress,
  readTimeout: Duration,
  keepAliveTimeout: Duration,
  sslConfig: SSLConfig,
  stats: StatsRecorder,
  newHandler: () => ChannelInboundHandler,
  maxInitialLineLength: Int,
  maxHeaderSize: Int,
  maxChunkSize: Int,
  maxConcurrentStreams: Int
) extends SimpleNettyServer(addr) {
  import HttpServer._

  val ssl = sslConfig.withProtoConfig(HttpServer.Http2ProtocolConfig)
  def sslEnabled = ssl.sslEnabled

  override protected def initBoot(b: ServerBootstrap): Unit = {
    b.childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)

    if (UseIOUring) {
      b.childOption[java.lang.Integer](
        IOUringChannelOption.TCP_KEEPIDLE,
        HttpServer.KeepAliveTime)
      b.childOption[java.lang.Integer](
        IOUringChannelOption.TCP_KEEPINTVL,
        HttpServer.KeepAliveInterval)
      b.childOption[java.lang.Integer](
        IOUringChannelOption.TCP_KEEPCNT,
        HttpServer.KeepAliveProbes)
      b.childOption[java.lang.Integer](
        IOUringChannelOption.TCP_USER_TIMEOUT,
        HttpServer.TCPUserTimeout)
    } else if (UseEpoll) {
      b.childOption[java.lang.Integer](
        EpollChannelOption.TCP_KEEPIDLE,
        HttpServer.KeepAliveTime)
      b.childOption[java.lang.Integer](
        EpollChannelOption.TCP_KEEPINTVL,
        HttpServer.KeepAliveInterval)
      b.childOption[java.lang.Integer](
        EpollChannelOption.TCP_KEEPCNT,
        HttpServer.KeepAliveProbes)
      b.childOption[java.lang.Integer](
        EpollChannelOption.TCP_USER_TIMEOUT,
        HttpServer.TCPUserTimeout)
    }

    super.initBoot(b)
  }

  protected def initChannel(ch: SocketChannel): Unit =
    if (sslEnabled) {
      configureSSL(ch)
    } else {
      configureClear(ch)
    }

  private[this] val exceptionHandler = new ExceptionHandler
  private[this] val statsHandler = new HttpServerStatsHandler(stats)

  private def timeoutHandler =
    new HttpServerTimeoutHandler(readTimeout, keepAliveTimeout, stats)

  private def multiplexHandler =
    new Http2MultiplexHandler(new ChannelInitializer[Channel]() {
      override def initChannel(ch: Channel): Unit = {
        // NB. Remove lingering timeout handler from parent channel to prevent it
        // from prematurely closing a HTTP2 connection.
        if (ch.parent ne null) {
          try {
            ch.parent.pipeline.remove("timeout handler")
          } catch {
            case _: NoSuchElementException => () // ignore
          }
        }

        val http2Codec = new Http2StreamFrameToHttpObjectCodec(true)
        ch.pipeline.addLast("http2 codec", http2Codec)
        ch.pipeline.addLast("timeout handler", timeoutHandler)
        ch.pipeline.addLast("handler", newHandler())
      }
    })

  private def frameCodec: Http2FrameCodec = {
    val builder = Http2FrameCodecBuilder.forServer()
    val settings = builder.initialSettings()
    settings.maxConcurrentStreams(maxConcurrentStreams)
    builder.initialSettings(settings).build()
  }

  private def initHandler(multiplex: Boolean, pipeline: ChannelPipeline): Unit = {
    pipeline.addLast("exception", exceptionHandler)
    pipeline.addLast("channel stats", statsHandler)

    if (multiplex) {
      pipeline.addLast(frameCodec)
      pipeline.addLast("multiplexer", multiplexHandler)
    } else {
      pipeline.addLast("timeout handler", timeoutHandler)
      pipeline.addLast("handler", newHandler())
    }
  }

  private def sourceCodec =
    new HttpServerCodec(
      maxInitialLineLength,
      maxHeaderSize,
      maxChunkSize)

  private def multiplexer =
    new ChannelInitializer[Channel]() {
      override def initChannel(ch: Channel): Unit =
        initHandler(true, ch.pipeline)
    }

  private def configureSSL(ch: SocketChannel): Unit = {
    ssl.getServerHandler(ch.alloc) foreach {
      ch.pipeline.addLast(SSL.HandlerName, _)
    }

    if (JBoolean.getBoolean(LoggingSyspropKey)) {
      ch.pipeline.addLast("logging", new LoggingHandler(LogLevel.INFO))
    }

    ch.pipeline.addLast(
      "alpn negotiator",
      new ApplicationProtocolNegotiationHandler(ApplicationProtocolNames.HTTP_1_1) {
        // By default Netty will log a warning. AWS's NLBs ping the port by
        // opening and then immediately closing a connection causing a handshake
        // failure which floods logs with warnings.
        override protected def handshakeFailure(
          ctx: ChannelHandlerContext,
          cause: Throwable): Unit =
          cause match {
            case _: ClosedChannelException => ctx.close()
            case _                         => super.handshakeFailure(ctx, cause)
          }

        override protected def configurePipeline(
          ctx: ChannelHandlerContext,
          prot: String): Unit =
          prot match {
            case ApplicationProtocolNames.HTTP_1_1 =>
              ctx.pipeline.addLast("http codec", sourceCodec)
              initHandler(false, ctx.pipeline)

            case ApplicationProtocolNames.HTTP_2 =>
              ctx.pipeline.addLast(multiplexer)

            case _ =>
              throw new IllegalStateException(s"unknown protocol: $prot")
          }
      }
    )
  }

  private def upgradeCodecFactory = new UpgradeCodecFactory() {
    override def newUpgradeCodec(prot: CharSequence): UpgradeCodec =
      if (AsciiString.contentEquals(UpgradeName, prot)) {
        new Http2ServerUpgradeCodec(frameCodec, multiplexHandler)
      } else {
        null
      }
  }

  private def configureClear(ch: SocketChannel): Unit = {
    if (JBoolean.getBoolean(LoggingSyspropKey)) {
      ch.pipeline.addLast("logging", new LoggingHandler(LogLevel.INFO))
    }

    val srcCodec = sourceCodec
    val upgradeHandler =
      new HttpServerUpgradeHandler(srcCodec, upgradeCodecFactory, MaxRequestSize)
    val clearHttp2UpgradeHandler =
      new CleartextHttp2ServerUpgradeHandler(srcCodec, upgradeHandler, multiplexer)
    ch.pipeline.addLast("http2 clear upgrade", clearHttp2UpgradeHandler)

    ch.pipeline.addLast(
      "clear http1.1",
      new SimpleChannelInboundHandler[NettyMessage]() {
        override protected def channelRead0(
          ctx: ChannelHandlerContext,
          msg: NettyMessage): Unit = {
          initHandler(false, ctx.pipeline)
          ctx.fireChannelRead(ReferenceCountUtil.retain(msg))
          ctx.pipeline.remove(this)
        }
      }
    )
  }
}
