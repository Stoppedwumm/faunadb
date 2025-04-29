package fauna.trace.datadog

import fauna.codex.json2.JSONWriter
import fauna.lang.NamedPoolThreadFactory
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.trace.{ Exporter, Span, Tracer }
import io.netty.bootstrap.Bootstrap
import io.netty.channel._
import io.netty.channel.epoll._
import io.netty.channel.nio._
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio._
import io.netty.handler.codec.http._
import io.netty.handler.timeout._
import io.netty.util.AsciiString
import java.io.IOException
import java.net.{ InetSocketAddress, SocketAddress, URI }
import java.util.concurrent.{
  ConcurrentLinkedQueue,
  RejectedExecutionException
}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ Await, ExecutionContext, Future, Promise }
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

final class Client(
  host: String,
  port: Int,
  version: String,
  endpoint: String,
  connectTimeout: FiniteDuration,
  responseTimeout: FiniteDuration,
  maxConnections: Int,
  stats: Tracer.Stats)
    extends Exporter
    with ExceptionLogging {

  /**
    * Maximum acceptable content length in bytes.
    *
    * As of datadog-agent 7.31.x:
    * https://github.com/DataDog/datadog-agent/blob/7.31.x/pkg/trace/config/config.go#L163
    */
  val MaxContentLengthBytes = 50 * 1024 * 1024

  /**
    * The number of traces sent per request to the agent.
    */
  val DataDogTraceCount = new AsciiString("X-Datadog-Trace-Count")

  private[this] val uri = new URI(s"http://$host:$port/$version/$endpoint")
  private[this] val log = getLogger()

  private[this] val boot = {
    val cls = if (Epoll.isAvailable) {
      classOf[EpollSocketChannel]
    } else {
      classOf[NioSocketChannel]
    }

    val group = {
      val threadFactory = new NamedPoolThreadFactory("DataDog", makeDaemons = true)

      if (Epoll.isAvailable) {
        new EpollEventLoopGroup(0, threadFactory)
      } else {
        new NioEventLoopGroup(0, threadFactory)
      }
    }

    new Bootstrap()
      .group(group)
      .channel(cls)
      .option[Integer](
        ChannelOption.CONNECT_TIMEOUT_MILLIS,
        connectTimeout.toMillis.toInt)
  }

  private[this] val init = new ChannelInitializer[SocketChannel] {
    override def initChannel(ch: SocketChannel): Unit = {
      ch.pipeline.addLast("codec", new HttpClientCodec)
      ch.pipeline.addLast("inflator", new HttpContentDecompressor)
      ch.pipeline.addLast("timeout", new TimeoutHandler(responseTimeout))
      ch.pipeline.addLast("aggregator", new HttpObjectAggregator(MaxContentLengthBytes))
    }
  }

  private[this] val activeConnections = new AtomicInteger(0)

  private[this] val pool = new ConcurrentLinkedQueue[Channel]()

  def exportOne(data: Span.Data): Unit =
    exportAll(Vector(data))

  override def exportAll(data: Vector[Span.Data]): Unit = {
    implicit val ec = ExecutionContext.parasitic // XXX: can't access IEC

    val fut = getChannel() flatMap {
      case (ch, res) =>
        val buf = ch.alloc.ioBuffer
        val writer = JSONWriter(buf)

        writer.writeArrayStart() // list of traces

        data foreach { span =>
          writer.writeArrayStart() // list of spans

          Format.writeSpan(writer, span)

          writer.writeArrayEnd() // list of spans
        }

        writer.writeArrayEnd() // list of traces

        val req = new DefaultFullHttpRequest(
          HttpVersion.HTTP_1_1,
          HttpMethod.PUT,
          uri.getRawPath,
          buf)

        req.headers.set(DataDogTraceCount, data.size)

        req.headers.set(HttpHeaderNames.HOST, host)
        req.headers.set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
        req.headers.set(HttpHeaderNames.CONTENT_LENGTH, buf.readableBytes)

        // optimism below...
        req.headers.set(HttpHeaderNames.CONNECTION,
          HttpHeaderValues.KEEP_ALIVE)
        req.headers.set(HttpHeaderNames.ACCEPT_ENCODING,
          HttpHeaderValues.GZIP)

        if (ch.isActive) {
          ch.writeAndFlush(req).toFuture flatMap { _ => res }
        } else {
          Future.failed(new RejectedExecutionException("DataDog: channel closed"))
        }
    }

    try {
      val resp = Await.result(fut, responseTimeout)

      if (resp.status.codeClass != HttpStatusClass.SUCCESS) {
        throw new RejectedExecutionException(s"DataDog: ${resp.status.code} ${resp.status.reasonPhrase}")
      } else {
        stats.incrSpansExported()
      }
    } catch {
      case NonFatal(_) =>
        stats.incrSpansDropped(data.size)
    }
  }

  @annotation.tailrec
  private def getChannel()(implicit ec: ExecutionContext): Future[(Channel, Future[HttpResponse])] =
    Option(pool.poll()) match {
      case Some(ch) if ch.isActive =>
        val p = Promise[HttpResponse]()
        val handler = new ResponseHandler(p)

        try {
          ch.pipeline.replace(classOf[ResponseHandler], "response", handler)
        } catch {
          case _: NoSuchElementException =>
            // no response handler found, but the channel is still
            // active - try to reuse it
            ch.pipeline.addLast("response", handler)
        }

        Future.successful((ch, p.future))
      case Some(_) => getChannel()
      case None =>
        val active = activeConnections.get

        if (active < maxConnections) {
          if (activeConnections.compareAndSet(active, active + 1)) {
            val p = Promise[HttpResponse]()
            connect() map { ch =>
              stats.recorder.incr("Tracing.DataDog.Pool.Size")
              ch.pipeline.addLast("response", new ResponseHandler(p))

              // called regardless of when/how channel is closed
              ch.closeFuture.addListener { _: ChannelFuture =>
                stats.recorder.decr("Tracing.DataDog.Pool.Size")
                activeConnections.decrementAndGet
              }

              (ch, p.future)
            } transform {
              // connect() failure
              case f @ Failure(_) =>
                activeConnections.decrementAndGet
                f
              case s => s
            }
          } else {
            getChannel()
          }
        } else {
          stats.recorder.incr("Tracing.DataDog.Pool.Exhausted")
          Future.failed(new RejectedExecutionException("DataDog: connection pool exhausted"))
        }
    }

  private def connect(): Future[Channel] =
    connect(new InetSocketAddress(host, port))

  private def connect(addr: SocketAddress): Future[Channel] =
    Try(boot.clone()) match {
      case Success(boot) =>
        boot.handler(init)
        boot.connect(addr).toFuture
      case Failure(t) =>
        Future.failed(t)
    }

  private final class TimeoutHandler(timeout: FiniteDuration)
      extends ChannelDuplexHandler {

    // NB. Do not pipeline requests
    @volatile private[this] var waiting: Boolean = false

    private[this] val trigger =
      new IdleStateHandler(timeout.length, 0, 0, timeout.unit)

    override def handlerAdded(ctx: ChannelHandlerContext): Unit =
      ctx.pipeline.addBefore(ctx.name, "idle", trigger)

    override def handlerRemoved(ctx: ChannelHandlerContext): Unit =
      ctx.pipeline.remove(trigger)

    override def write(ctx: ChannelHandlerContext, e: AnyRef, p: ChannelPromise): Unit =
      if (ctx.channel.isActive) {
        ctx.write(e, p)

        e match {
          case _: FullHttpMessage | _: LastHttpContent =>
            waiting = true
          case _ => ()
        }
      }

    override def channelRead(ctx: ChannelHandlerContext, e: AnyRef): Unit = {
      e match {
        case _: FullHttpMessage | _: LastHttpContent =>
          waiting = false
        case _ => ()
      }

      ctx.fireChannelRead(e)
    }

    override def userEventTriggered(ctx: ChannelHandlerContext, e: AnyRef): Unit =
      e match {
        case e: IdleStateEvent if e.state == IdleState.READER_IDLE =>
          if (waiting) {
            throw ReadTimeoutException.INSTANCE
          }
        case _ => ()
      }
  }

  private final class ResponseHandler(promise: Promise[HttpResponse])
      extends SimpleChannelInboundHandler[HttpObject] {

    override def channelRead0(ctx: ChannelHandlerContext, msg: HttpObject): Unit = {
      msg match {
        case m: HttpResponse =>
          promise.trySuccess(m)
        case m =>
          promise.tryFailure(new IllegalStateException(s"Unknown HTTP object: $m"))
      }

      pool.offer(ctx.channel)
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable): Unit = {
      e match {
        case _: ReadTimeoutException | _: IOException => ()
        case _                       => logException(e)
      }
      log.debug("Closing datadog connection due to an error", e)
      ctx.close()
    }
  }
}
