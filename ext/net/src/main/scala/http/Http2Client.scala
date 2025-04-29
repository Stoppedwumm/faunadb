package fauna.net.http

import fauna.exec._
import fauna.lang.syntax._
import fauna.net.netty._
import fauna.net.netty.{ DefaultEventLoopGroup => DefaultELGroup }
import fauna.net.security._
import io.netty.channel._
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.http2._
import io.netty.util.AsciiString
import io.netty.util.concurrent.{ Future => NettyFuture }
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{ Future, Promise }
import scala.util.{ Success, Try }

object Http2Client {
  import HttpClient._

  def apply(
    host: String,
    port: Int,
    ssl: Option[SSLConfig] = None,
    headers: Seq[(AsciiString, AnyRef)] = Nil,
    responseTimeout: Duration = DefaultResponseTimeout,
    connectionTimeout: Duration = Duration.Zero,
    enableEndpointIdentification: Boolean = true,
    eventLoopGroupOverride: EventLoopGroup = DefaultELGroup): Http2Client = {

    val (hostArg, portArg, sslArg) = deriveDefaults(host, port, ssl)
    new Http2Client(
      new AsciiString(hostArg),
      portArg,
      sslArg.withProtoConfig(HttpServer.Http2ProtocolConfig),
      headers,
      responseTimeout,
      connectionTimeout,
      enableEndpointIdentification,
      eventLoopGroupOverride
    )
  }
}

final class Http2Client private (
  val host: AsciiString,
  val port: Int,
  val ssl: SSLConfig,
  headers: Seq[(AsciiString, AnyRef)],
  responseTimeout: Duration,
  connectionTimeout: Duration,
  enableEndpointIdentification: Boolean,
  eventLoopGroupOverride: EventLoopGroup)
    extends SimpleNettyClient(connectionTimeout)
    with AbstractHttpClient {
  import FaunaExecutionContext.Implicits.global

  private[this] val connection = new AtomicReference[Future[Channel]]()

  override def keepAlive: Boolean = true

  override protected def eventLoopGroup: EventLoopGroup = eventLoopGroupOverride

  override protected def initChannel(ch: SocketChannel): Unit = {
    ssl.getClientHandler(ch.alloc(), Some((host.toString, port))) foreach {
      handler =>
        if (enableEndpointIdentification) {
          val engine = handler.engine
          val params = engine.getSSLParameters

          params.setEndpointIdentificationAlgorithm("HTTPS")
          engine.setSSLParameters(params)
        }

        ch.pipeline.addLast(SSL.HandlerName, handler)
    }

    val codec = Http2FrameCodecBuilder.forClient().build()
    val multiplexer = new Http2MultiplexHandler(
      new SimpleChannelInboundHandler[AnyRef]() {
        def channelRead0(ctx: ChannelHandlerContext, msg: AnyRef): Unit =
          throw new IllegalStateException("Client inbound not supported.")
      }
    )

    ch.pipeline.addLast(codec)
    ch.pipeline.addLast(multiplexer)
  }

  private def initStreamChannel(ch: Channel, res: Promise[HttpResponse]): Unit = {
    val http2ToHttp1Codec = new Http2StreamFrameToHttpObjectCodec(false)
    val responseHandler = new HttpResponseHandler(res, pooledBuffers = false)

    ch.pipeline.addLast(http2ToHttp1Codec)
    if (responseTimeout.isFinite && responseTimeout != Duration.Zero) {
      val timeoutHandler = new HttpClientTimeoutHandler(responseTimeout)
      ch.pipeline.addLast(timeoutHandler)
    }
    ch.pipeline.addLast(responseHandler)
  }

  private def newStream(): Future[(Channel, Future[HttpResponse])] = {
    @tailrec def getOrCreateConnection(): Future[Channel] = {
      val connF = connection.get()
      if (connF eq null) {
        val p = Promise[Channel]()
        if (connection.compareAndSet(null, p.future)) {
          p.completeWith(connect(host.toString, port, _ => ()))
          p.future
        } else {
          getOrCreateConnection()
        }
      } else {
        connF.value match {
          case None                                 => connF
          case Some(Success(conn)) if conn.isActive => connF
          case _ =>
            connection.compareAndSet(connF, null)
            getOrCreateConnection() // retry/reconnect
        }
      }
    }

    def newStream0(ch: Channel, res: Promise[HttpResponse]): Future[Channel] = {
      val bootstrap = new Http2StreamChannelBootstrap(ch)
      val stream = Promise[Channel]()

      bootstrap.handler(new ChannelInitializer[Channel] {
        def initChannel(ch: Channel): Unit =
          initStreamChannel(ch, res)
      })

      val open = bootstrap.open()
      open.addListener((future: NettyFuture[Channel]) => {
        stream.tryComplete(Try(future.sync().getNow))
      })

      stream.future
    }

    val responses = Promise[HttpResponse]()
    getOrCreateConnection() flatMap {
      newStream0(_, responses) map {
        (_, responses.future)
      }
    }
  }

  def sendRequest(req: HttpRequest, setContentLength: Boolean): Future[HttpResponse] = {
    headers foreach {
      case (name, value) =>
        if (!req.containsHeader(name)) {
          req.raw.headers.set(name, value)
        }
    }
    newStream() flatMap {
      case (ch, resF) =>
        req.writeTo(ch, setContentLength = setContentLength) before {
          resF map { res =>
            res.body match {
              case NoBody | _: Body => res
              case c: Chunked =>
                // close channel once no more events are needed
                val obs = c.events ensure { ch.closeQuietly() }
                new HttpResponse(res.raw, c.copy(events = obs))
            }
          }
        }
    }
  }

  def start(): Unit = init()

  def isRunning: Boolean = isInitialized

  def stop(graceful: Boolean): Unit = {
    val connF = connection.get()
    if (connF ne null) {
      connF map { _.closeQuietly() }
    }
    clear()
  }
}
