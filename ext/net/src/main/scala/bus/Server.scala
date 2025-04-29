package fauna.net.bus.transport

import com.github.benmanes.caffeine.cache._
import fauna.atoms._
import fauna.codex.cbor._
import fauna.exec.{ FaunaExecutionContext, Timer }
import fauna.lang.syntax._
import fauna.lang.Local
import fauna.logging.ExceptionLogging
import fauna.net._
import fauna.net.bus._
import fauna.net.netty._
import fauna.net.security._
import fauna.net.util.{ MultiPacketStream, SinglePacketStream }
import fauna.trace._
import io.netty.buffer._
import io.netty.channel._
import io.netty.channel.socket.SocketChannel
import io.netty.channel.unix._
import io.netty.handler.codec.DecoderException
import io.netty.handler.ssl.NotSslRecordException
import java.nio.channels.ClosedChannelException
import java.util.concurrent.{ ConcurrentHashMap, TimeoutException }
import scala.concurrent.duration._
import scala.concurrent.Future

final class Server(
  cluster: String,
  hostInfo: HostInfo,
  aliases: Vector[Alias],
  val listenAddress: HostAddress,
  ssl: SSLConfig,
  hostIDMap: ConcurrentHashMap[HostID, List[HostAddress]],
  handshakeTimeout: FiniteDuration,
  idleTimeout: FiniteDuration,
  keepaliveInterval: FiniteDuration,
  stats: MessageBus.Stats,
  dispatcher: => MessageBus.Dispatcher)
    extends SimpleNettyServer(listenAddress.sockaddr)
    with ExceptionLogging {

  private[this] val log = getLogger
  private[this] val ec = FaunaExecutionContext.Implicits.global

  val hostID = hostInfo.id
  val hostAddress = hostInfo.address

  private val openChannels = new ConcurrentHashMap[Channel, Channel]

  protected def initChannel(ch: SocketChannel): Unit = {
    log.debug(s"Received connection from ${ch.remoteAddress}.")

    ssl.getServerHandler(ch.alloc) foreach { ch.pipeline.addLast("ssl", _) }
    FrameCodec.addLast(ch.pipeline)

    ch.pipeline.addLast(
      "handshake timeout",
      CloseOnIdleHandler.All(handshakeTimeout, None))
    ch.pipeline.addLast("version handler", new VersionHandler)
  }

  override def stop(graceful: Boolean): Unit = {
    super.stop(graceful)
    openChannels.values forEach {
      _.close.await
    }
  }

  private def catchException(ctx: ChannelHandlerContext, e: Throwable): Unit = {
    e match {
      case _: Errors$NativeIoException =>
        () // Silence 'expected' exceptions do to peer resets, &c.

      case e: DecoderException if e.getCause.isInstanceOf[NotSslRecordException] =>
        () // Silence SSL/TLS decoding errors.

      case e =>
        val prefix =
          s"Server connection ${hostInfo.address.sockaddr} => ${ctx.channel.remoteAddress}"
        logException(new Exception(s"${prefix}: Bus Exception", e))
    }

    ctx.channel.closeQuietly()
  }

  // handlers

  private final class VersionHandler extends SimpleChannelInboundHandler[ByteBuf] {
    override def channelRead0(ctx: ChannelHandlerContext, buf: ByteBuf): Unit = {
      val remote = buf.readableBytes match {
        case 8 => Range(buf.readInt, buf.readInt).inclusive
        case _ =>
          throw UnexpectedTransportException(
            s"Received invalid version message from client ${ctx.channel.remoteAddress}.")
      }

      val local = Range(MinVersion, MaxVersion).inclusive
      val compat = remote intersect local

      if (compat.nonEmpty) {
        log.debug(
          s"Sending transport version ${compat.last} to ${ctx.channel.remoteAddress}.")

        ctx.pipeline.replace(
          "version handler",
          "info handler",
          new InfoHandler(compat.last))

        // Respond with negotiated version

        ctx.writeAndFlush(ctx.channel.alloc.buffer(4).writeInt(compat.last))

      } else {
        log.warn(
          s"Received invalid connection version range $remote from ${ctx.channel.remoteAddress}.")

        ctx.writeAndFlush(
          ctx.channel.alloc.buffer(4).writeInt(Transport.VersionError))
        ctx.channel.close()
      }
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable) =
      catchException(ctx, e)
  }

  private final class InfoHandler(vers: Int)
      extends SimpleChannelInboundHandler[ByteBuf] {
    override def channelRead0(ctx: ChannelHandlerContext, buf: ByteBuf) = {
      val (clientCluster, clientInfo, streamSigID) = (vers: @unchecked) match {
        case 2 =>
          val info = CBOR.decode[ClientSessionInfoV2](buf)
          (info.cluster, HostInfo(info.hostID, info.hostAddress), None)
        case 3 =>
          val info = CBOR.decode[ClientSessionInfoV3](buf)
          (info.cluster, HostInfo(info.hostID, info.hostAddress), info.tcpStreamID)
      }

      // Send server session info
      val info = ServerSessionInfo(cluster, hostID, aliases)
      ctx.writeAndFlush(CBOR.encode(ctx.alloc.buffer, info))

      // Make sure to error on invalid cluster after writing out the
      // server info, so the client can figure out why it can't connect.
      if (cluster != clientCluster) {
        throw InvalidClusterIDException(cluster, clientCluster, clientInfo.address)
      }

      ctx.pipeline.remove("info handler")

      streamSigID match {
        case None =>
          openChannels.put(ctx.channel, ctx.channel)
          ctx.pipeline.remove("handshake timeout")
          ctx.pipeline.addLast(
            "idle timeout",
            CloseOnIdleHandler.Read(idleTimeout, None))
          ctx.pipeline.addLast(
            "keepalive ping",
            new KeepalivePingSend(keepaliveInterval))
          ctx.pipeline.addLast("dispatch handler", new DispatchHandler(clientInfo))
        case Some(sigID) =>
          stats.incrInboundTCPStreams()
          ctx.pipeline.remove("frame decoder")
          ctx.pipeline.remove("frame encoder")
          ctx.pipeline.addLast(
            "stream init handler",
            new TCPStreamInitHandler(clientInfo, sigID))
      }

      hostIDMap.put(clientInfo.id, List(clientInfo.address))

      val typeStr = streamSigID.fold("messages")(sig => s"tcp stream $sig")
      log.debug(
        s"Established transport server connection ($typeStr) with ${ctx.channel.remoteAddress} (${clientInfo.address.sockaddr}).")
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable) =
      catchException(ctx, e)
  }

  private class TCPStreamInitHandler(clientInfo: HostInfo, signalID: SignalID)
      extends SimpleChannelInboundHandler[ByteBuf] {
    override def channelRead0(ctx: ChannelHandlerContext, buf: ByteBuf) = {
      buf.readBytes(Transport.TCPStreamStart.readableBytes) releaseAfter { start =>
        if (start != Transport.TCPStreamStart) {
          throw UnexpectedTransportException(
            s"Received unexpected stream init message ${start.toUTF8String} from ${ctx.channel.remoteAddress}.")
        }
      }

      stats.incrInboundTCPStreams()
      stats.incrOpenTCPStreams()

      ctx.pipeline.remove("handshake timeout")
      ctx.channel.closeFuture.addListener { _: ChannelFuture =>
        stats.decrOpenTCPStreams()
      }

      dispatcher.recvTCPStream(clientInfo, signalID, ctx.channel)

      // re-fire read if there are still bytes left
      if (buf.isReadable) {
        ctx.fireChannelRead(buf.retain())
      }

      ctx.pipeline.remove("stream init handler")
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable) =
      catchException(ctx, e)
  }

  private final class DispatchHandler(remoteInfo: HostInfo)
      extends SimpleChannelInboundHandler[ByteBuf](false) {

    import scala.jdk.FutureConverters._

    protected val streams: Cache[(SignalID, Int), MultiPacketStream] =
      Caffeine
        .newBuilder()
        .expireAfterAccess(idleTimeout.length, idleTimeout.unit)
        .executor(ec)
        .scheduler { (ec, run, delay, unit) =>
          val resF =
            Timer.Global.delay(Duration(delay, unit)) {
              ec.execute(run)
              Future.unit
            }
          resF.asJava.toCompletableFuture()
        }
        .removalListener {
          (_: (SignalID, Int), stream: MultiPacketStream, cause: RemovalCause) =>
            // We only release on eviction. Explicit removal or replacement
            // requires us to release the buffer manually
            if (cause.wasEvicted) {
              stream.fail(
                new TimeoutException(s"Byte stream timed out: $remoteInfo"))
            }
        }
        .build()

    override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable) =
      catchException(ctx, e)

    override def channelInactive(ctx: ChannelHandlerContext) = {
      streams.asMap forEach { case (_, src) =>
        src.fail(new ClosedChannelException())
      }
      super.channelInactive(ctx)
    }

    override def channelUnregistered(ctx: ChannelHandlerContext) =
      openChannels.remove(ctx.channel, ctx.channel)

    override def channelRead0(ctx: ChannelHandlerContext, packet: ByteBuf) = {
      val tracer = GlobalTracer.instance

      CBOR.decode[Message](packet) match {
        case MessageHeader(signalID, _, 1, trace, deadline) =>
          val scope = trace flatMap { ctx =>
            val span = buildSpan(signalID, ctx)
            GlobalTracer.instance.activate(span)
          }

          stats.recordReceivedSize(signalID, packet.readableBytes)

          try {
            val context = Local.save()
            scope foreach { _.retain() }

            ec.execute { () =>
              Local.let(context) {
                try {
                  dispatcher.recvMessage(
                    remoteInfo,
                    signalID,
                    new SinglePacketStream(packet),
                    deadline)
                } finally {
                  scope foreach { _.close() }
                }
              }
            }
          } finally {
            scope foreach { _.close() }
          }

        case MessageHeader(signalID, streamID, msgCount, trace, deadline) =>
          val scope = trace flatMap { ctx =>
            val span = buildSpan(signalID, ctx)
            tracer.activate(span)
          }

          stats.recordReceivedSize(signalID, packet.readableBytes)

          try {
            val key = (signalID, streamID)
            if (streams.getIfPresent(key) eq null) {
              val stream = new MultiPacketStream(msgCount, ctx.alloc, scope)
              streams.put(key, stream)

              val context = Local.save()
              scope foreach { _.retain() }

              ec.execute { () =>
                Local.let(context) {
                  try {
                    dispatcher.recvMessage(remoteInfo, signalID, stream, deadline)
                  } finally {
                    scope foreach { _.close() }
                  }
                }
              }

              stream.addChunk(1, packet, ec)
            } else {
              // don't cross the streams
              stats.incrMessageIgnored(signalID)
              log.trace(s"A stream at $streamID for $signalID is already open!?")
              tracer.activeSpan foreach {
                _.setStatus(AlreadyExists(s"$signalID at $streamID"))
              }
            }
          } finally {
            scope foreach { _.close() }
          }
        case MessageFrame(signalID, streamID, msgNum) =>
          val key = (signalID, streamID)

          stats.recordReceivedSize(signalID, packet.readableBytes)

          Option(streams.getIfPresent(key)) match {
            case Some(stream) =>
              if (stream.addChunk(msgNum, packet, ec)) {
                // src is done. remove from cache.
                streams.invalidate(key)
              }

            case None =>
              stats.incrMessageIgnored(signalID)
              log.trace(s"No open stream for $streamID on $signalID.")
          }
      }
    }

    private def buildSpan(signalID: SignalID, ctx: TraceContext): Span = {
      val span = GlobalTracer.instance
        .buildSpan("server")
        .withKind(Consumer)
        .withParent(ctx)
        .start()

      if (span.isSampled) {
        span.addAttribute(Attributes.Net.Transport, Attributes.Net.TCP)
        span.addAttribute(
          Attributes.Net.PeerIP,
          remoteInfo.address.address.getHostAddress)
        span.addAttribute(Attributes.Net.PeerPort, remoteInfo.address.port)
        span.addAttribute(Attributes.Net.PeerName, remoteInfo.address.hostName)
        span.addAttribute(
          Attributes.Net.HostIP,
          hostInfo.address.address.getHostAddress)
        span.addAttribute(Attributes.Net.HostPort, hostInfo.address.port)
        span.addAttribute(Attributes.Net.HostName, hostInfo.address.hostName)
        span.addAttribute(Attributes.Net.SignalID, signalID.toInt)
      }

      span
    }
  }
}
