package fauna.net.bus.transport

import fauna.atoms._
import fauna.codex.cbor._
import fauna.exec._
import fauna.lang.syntax._
import fauna.lang.TimeBound
import fauna.logging.ExceptionLogging
import fauna.net._
import fauna.net.bus._
import fauna.net.netty._
import fauna.net.security._
import fauna.trace.{ Attributes, GlobalTracer, Producer }
import io.netty.bootstrap.Bootstrap
import io.netty.buffer._
import io.netty.channel._
import io.netty.util.ReferenceCountUtil
import java.io.IOException
import java.net.{ ConnectException, InetSocketAddress, SocketAddress }
import java.util.concurrent.{
  ArrayBlockingQueue,
  ConcurrentHashMap,
  TimeoutException
}
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicInteger, AtomicReference }
import scala.annotation.tailrec
import scala.concurrent.{ Await, ExecutionContext, Future, Promise }
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{ Failure, Success }

final class Client(
  val cluster: String,
  val hostInfo: HostInfo,
  val hostIDMap: ConcurrentHashMap[HostID, List[HostAddress]],
  val dedicatedStreamSignals: AtomicReference[Set[SignalID]],
  val localZone: Option[String],
  val ssl: SSLConfig,
  val connectionsPerHost: Int,
  val connectTimeout: FiniteDuration,
  val handshakeTimeout: FiniteDuration,
  val idleTimeout: FiniteDuration,
  val keepaliveTimeout: FiniteDuration,
  val maxOutboundMessages: Int,
  val maxMessageSize: Int,
  val stats: MessageBus.Stats)
    extends SimpleNettyClient(connectTimeout = connectTimeout)
    with ExceptionLogging {

  private[this] val log = getLogger

  val hostID = hostInfo.id
  val hostAddress = hostInfo.address

  // Convenience logging methods that add a host:port => host:port prefix.
  private def connPrefix(remote: SocketAddress): String =
    s"Client connection ${hostAddress.sockaddr} => ${remote}"

  private def logInfoWithPrefix(remote: HostAddress, msg: => String): Unit =
    log.info(s"${connPrefix(remote.sockaddr)}: $msg")

  private def logWarnWithPrefix(remote: HostAddress, msg: => String): Unit =
    log.warn(s"${connPrefix(remote.sockaddr)}: $msg")

  private def logExceptionWithPrefix(remote: SocketAddress, e: Throwable): Unit =
    logException(new Exception(s"${connPrefix(remote)}: Bus Exception", e))

  private[this] val msgConnections =
    new ConcurrentHashMap[(HostAddress, Int), Promise[Option[MsgConnection]]]

  override protected def initBoot(b: Bootstrap) = {
    super.initBoot(b)
    b.option(ChannelOption.SO_RCVBUF, Integer.valueOf(FrameCodec.MaxMessageBytes))
    b.option(ChannelOption.SO_SNDBUF, Integer.valueOf(FrameCodec.MaxMessageBytes))
  }

  // send

  def sendBytes(signalID: SignalID, to: HostDest, msg: ByteBuf, deadline: TimeBound, reason: String)(
    implicit ec: ExecutionContext): Future[Boolean] =
    openConn(signalID, to.toEither, reason)(connectMsgConn) flatMap {
      case Some(conn) if deadline.hasTimeLeft =>
        conn.send(signalID, deadline, msg)

      case _ =>
        msg.release()
        stats.incrMessageDropped(signalID)
        FutureFalse
    }

  def openTCPStream(signalID: SignalID, to: HostDest, reason: String)(configure: Channel => Unit)(
    implicit ec: ExecutionContext): Future[Channel] =
    openConn(signalID, to.toEither, reason)(connectTCPStream) map {
      case None => throw TCPStreamOpenFailure(to.toEither)
      case Some(ch) =>
        ch.pipeline.remove("frame decoder")
        ch.pipeline.remove("frame encoder")
        ch.pipeline.remove("idle timeout")

        // Signal to server-side the stream will actually be used after hostID
        // negotiation.
        // write the start message before handing the channel to the callback,
        // which may introduce new ChannelHandlers.
        ch.write(Transport.TCPStreamStart)
        configure(ch)
        ch.flush()
        ch
    }

  // connection establishment

  private def openConn[T](signalID: SignalID, remote: Either[HostAddress, HostID], reason: String)(
    getConn: (SignalID, HostAddress, String) => Future[Option[(T, HostID)]])
    : Future[Option[T]] = {

    implicit val ec = ImmediateExecutionContext

    remote match {
      case Left(addr) => getConn(signalID, addr, reason) mapT { _._1 }
      case Right(id) =>
        Option(hostIDMap.get(id)) match {
          case None =>
            stats.incrHostIDLookupFailures()
            log.debug(s"$id does not map to a known host IP address.")
            FutureNone

          case Some(list) =>
            def openConn0(addrs: List[HostAddress]): Future[Option[T]] = {
              addrs match {
                case Nil =>
                  log.debug(
                    s"$id could not be contacted at any of its IP addresses $list.")
                  FutureNone
                case addr :: rest =>
                  getConn(signalID, addr, reason) flatMap {
                    case Some((conn, remoteID)) =>
                      if (remoteID != id) {
                        log.warn(
                          s"Host for $id $addr responded with different HostID $remoteID.")
                        openConn0(rest)
                      } else {
                        hostIDMap.put(remoteID, List(addr))
                        Future.successful(Some(conn))
                      }
                    case None => openConn0(rest)
                  }
              }
            }
            openConn0(list)
        }
    }
  }

  @tailrec
  private def connectMsgConn(
    signalID: SignalID,
    remote: HostAddress,
    reason: String): Future[Option[(MsgConnection, HostID)]] = {
    implicit val ec = ImmediateExecutionContext
    val sid =
      if (dedicatedStreamSignals.get.contains(signalID)) Some(signalID) else None
    val connDesc = sid.fold("main connection") { _.description }
    val key = (remote, sid.fold(-1) { _.toInt })

    msgConnections.get(key) match {
      case null =>
        val p = Promise[Option[MsgConnection]]()

        if (msgConnections.putIfAbsent(key, p) eq null) {
          // won the race. open the connection.
          connect(remote, ssl, None, reason) onComplete {
            case Success(Some((ch, remoteID))) =>
              stats.incrClientConnections()
              logInfoWithPrefix(remote, s"opened $connDesc ($reason)")

              val keepalive = new KeepalivePingReceive(keepaliveTimeout, sid, stats)
              val queue = new WriteQueue(ch)
              val conn = MsgConnection(remoteID, ch, queue)

              ch.pipeline.addLast("keepalive ping receive", keepalive)
              ch.pipeline.addLast("write queue", queue)

              conn.channel.closeFuture.addListener { fut: ChannelFuture =>
                msgConnections.remove(key, p)
                stats.decrClientConnections()
                val reason = if (fut.isSuccess) {
                  " successfully"
                } else if (fut.isCancelled) {
                  // Does this happen?
                  ": Netty reports close operation cancelled"
                } else if (fut.cause() != null) {
                  s": ${fut.cause.getMessage()}"
                } else {
                  // This shouldn't happen.
                  s": connection not closed successfuly but no more information is available"
                }
                logInfoWithPrefix(remote, s"closed $connDesc$reason")
              }
              p.success(Some(conn))

            case Success(None) =>
              msgConnections.remove(key, p)
              p.success(None)

            case Failure(ex) =>
              msgConnections.remove(key, p)
              p.failure(ex)
          }
        }

        connectMsgConn(signalID, remote, reason)
      case p => p.future mapT { c => (c, c.remoteID) }
    }
  }

  private def connectTCPStream(
    sig: SignalID,
    remote: HostAddress,
    reason: String): Future[Option[(Channel, HostID)]] = {
    implicit val ec = ImmediateExecutionContext
    connect(remote, ssl, Some(sig), reason) mapT { t =>
      val (ch, _) = t
      stats.incrOutboundTCPStreams()
      stats.incrOpenTCPStreams()
      ch.closeFuture.addListener { _: ChannelFuture => stats.decrOpenTCPStreams() }
      t
    }
  }

  private def connect(
    remote: HostAddress,
    ssl: SSLConfig,
    streamSigID: Option[SignalID],
    reason: String): Future[Option[(Channel, HostID)]] = {

    implicit val ec = ImmediateExecutionContext

    def connect0(remote: HostAddress, ssl: SSLConfig) = {
      val p = Promise[(Channel, HostID, Seq[Alias])]()

      logInfoWithPrefix(remote, s"Connecting ($reason)")

      connect(
        remote.sockaddr,
        { ch =>
          ssl.getClientHandler(ch.alloc, remote) foreach {
            ch.pipeline.addLast("ssl", _)
          }

          FrameCodec.addLast(ch.pipeline)
          ch.pipeline.addLast(
            "handshake timeout",
            CloseOnIdleHandler.All(handshakeTimeout, streamSigID))
          ch.pipeline
            .addLast("version handler", new VersionHandler(remote, streamSigID, p))
        }
      ) onComplete {
        case Success(ch) =>
          ch.writeAndFlush(
            ch.alloc
              .buffer(8)
              .writeInt(MinVersion)
              .writeInt(MaxVersion))
        case Failure(e) =>
          p.failure(e)
      }

      p.future
    }

    val fut = connect0(remote, ssl) flatMap { case (ch, hostID, aliases) =>
      val preferred = aliases find { a =>
        // should be the same zone or allow all zones.
        (a.zone.isEmpty || a.zone == localZone) &&
        // if we don't have ssl enabled, we can't connect to an ssl alias.
        ((a.variant == ClearVariant) || ssl.sslEnabled)
      } filter {
        // if the preferred alias is the one we're connected to, ignore it.
        _.hostAddress != remote
      }

      preferred match {
        case None => Future.successful((ch, hostID))
        case Some(a) =>
          logInfoWithPrefix(
            remote,
            s"Reconnecting to $remote with preferred alias $a")

          val ssl2 = a.variant match {
            case SSLVariant   => ssl
            case ClearVariant => NoSSL
            case str =>
              throw new IllegalArgumentException(s"Unknown SSL variant $str")
          }

          connect0(a.hostAddress, ssl2) map { case (ch2, _, _) =>
            ch.closeQuietly()
            (ch2, hostID)
          } recover { case _: TimeoutException =>
            logInfoWithPrefix(
              remote,
              s"Failed to connect to remote $remote with alias $a: falling back to original connection")
            (ch, hostID)
          }
      }
    }

    fut transform {
      case Success(t) =>
        Success(Some(t))
      case Failure(_: ConnectTimeoutException) | Failure(_: TimeoutException) =>
        stats.incrClientConnErrors()
        logWarnWithPrefix(
          remote,
          s"Connection attempt timed out after $connectTimeout")
        Success(None)
      case Failure(err: ConnectException) =>
        stats.incrClientConnErrors()
        logWarnWithPrefix(remote, s"Connection attempt failed: ${err.getMessage}")
        Success(None)
      case Failure(err) =>
        stats.incrClientConnErrors()
        logExceptionWithPrefix(remote.sockaddr, err)
        logWarnWithPrefix(remote, s"Connection attempt failed: ${err.getMessage}")
        Success(None)
    }
  }

  // handlers

  private final class VersionHandler(
    remote: HostAddress,
    signalID: Option[SignalID],
    readyPromise: Promise[(Channel, HostID, Seq[Alias])])
      extends SimpleChannelInboundHandler[ByteBuf] {

    override def channelRead0(ctx: ChannelHandlerContext, buf: ByteBuf) = {

      val vers = buf.readableBytes match {
        case 4 =>
          buf.readInt
        case _ =>
          throw UnexpectedTransportException(
            s"Received invalid version message from server $remote.")
      }

      if (MinVersion to MaxVersion contains vers) {
        logInfoWithPrefix(remote, s"Negotiated transport version $vers")

        ctx.pipeline.remove("version handler")
        ctx.pipeline.addLast(
          "info handler",
          new InfoHandler(remote, signalID, readyPromise))

        (vers: @unchecked) match {
          case 2 =>
            // server doesn't support opening direct TCP streams
            if (signalID.isDefined) {
              throw UnsupportedTCPStream(remote)
            }

            val info = ClientSessionInfoV2(cluster, hostID, hostAddress)
            ctx.writeAndFlush(CBOR.encode(ctx.alloc.buffer, info))

          case 3 =>
            val info = ClientSessionInfoV3(cluster, hostID, hostAddress, signalID)
            ctx.writeAndFlush(CBOR.encode(ctx.alloc.buffer, info))
        }
      } else {
        throw UnsupportedTransportVersionRange(remote, MinVersion, MaxVersion)
      }
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable) = {
      logExceptionWithPrefix(ctx.channel.remoteAddress, e)
      readyPromise.tryFailure(e)
      ctx.channel.closeQuietly()
    }

    override def channelInactive(ctx: ChannelHandlerContext) = {
      readyPromise.tryFailure(new TimeoutException(
        s"${connPrefix(remote.sockaddr)}: Connection establishment timed out after $connectTimeout"))
      super.channelInactive(ctx)
    }
  }

  private final class InfoHandler(
    remote: HostAddress,
    signalID: Option[SignalID],
    readyPromise: Promise[(Channel, HostID, Seq[Alias])])
      extends SimpleChannelInboundHandler[ByteBuf] {

    override def channelRead0(ctx: ChannelHandlerContext, buf: ByteBuf) = {
      val serverInfo = CBOR.decode[ServerSessionInfo](buf)

      ctx.pipeline.remove("handshake timeout")
      ctx.pipeline.remove("info handler")
      ctx.pipeline.addLast(
        "idle timeout",
        CloseOnIdleHandler.Write(idleTimeout, signalID))

      if (cluster != serverInfo.cluster) {
        throw InvalidClusterIDException(cluster, serverInfo.cluster, remote)
      } else {
        logInfoWithPrefix(remote, s"Established transport connection")
        readyPromise.trySuccess((ctx.channel, serverInfo.hostID, serverInfo.aliases))
      }
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable) = {
      logExceptionWithPrefix(ctx.channel.remoteAddress, e)
      readyPromise.tryFailure(e)
      ctx.channel.closeQuietly()
    }

    override def channelInactive(ctx: ChannelHandlerContext) = {
      readyPromise.tryFailure(
        new TimeoutException(s"Connection establishment to $remote timed out"))
      super.channelInactive(ctx)
    }
  }

  private case class MsgConnection(
    remoteID: HostID,
    channel: Channel,
    queue: WriteQueue) {

    implicit val ec = ImmediateExecutionContext
    private[this] val idGen = new AtomicInteger()

    def send(
      signalID: SignalID,
      deadline: TimeBound,
      msg: ByteBuf): Future[Boolean] = {
      val tracer = GlobalTracer.instance
      val trace = tracer.activeSpan flatMap { parent =>
        Option.when(parent.isSampled) {
          val span = tracer
            .buildSpan("client")
            .withKind(Producer)
            .withParent(parent)
            .start()

          if (span.isSampled) {
            span.addAttribute(Attributes.Net.Transport, Attributes.Net.TCP)
            span.addAttribute(
              Attributes.Net.HostIP,
              hostInfo.address.address.getHostAddress)
            span.addAttribute(Attributes.Net.HostPort, hostInfo.address.port)
            span.addAttribute(Attributes.Net.HostName, hostInfo.address.hostName)
            span.addAttribute(Attributes.Net.SignalID, signalID.toInt)

            val peer = channel.remoteAddress.asInstanceOf[InetSocketAddress]
            span.addAttribute(Attributes.Net.PeerIP, peer.getAddress.getHostAddress)
            span.addAttribute(Attributes.Net.PeerPort, peer.getPort)
            span.addAttribute(Attributes.Net.PeerName, peer.getHostString)
          }

          span
        }
      }

      if (msg.readableBytes <= maxMessageSize) {
        val packet = channel.alloc.compositeBuffer(2)
        val header = MessageHeader(signalID, 0, 1, trace map { _.context }, deadline)
        val encHeader = CBOR.encode(channel.alloc.buffer, header)
        packet.addComponents(true, encHeader, msg)

        val scope = trace flatMap { tracer.activate(_) }

        try {
          queue.enqueue(packet, signalID, deadline)
        } finally {
          scope foreach { _.close() }
        }

      } else {
        val msgCount = (msg.readableBytes.toDouble / maxMessageSize).ceil.toInt
        val streamID = idGen.incrementAndGet()
        val scope = trace flatMap { tracer.activate(_) }

        def write(msgNum: Int): Future[Boolean] = {
          val msgHead = if (msgNum == 1) {
            MessageHeader(
              signalID,
              streamID,
              msgCount,
              trace map { _.context },
              deadline)
          } else {
            MessageFrame(signalID, streamID, msgNum)
          }
          val header = CBOR.encode(channel.alloc.buffer, msgHead)
          val packet = channel.alloc.compositeBuffer(2)

          val size = msg.readableBytes min maxMessageSize

          packet.addComponents(
            true,
            header,
            msg.retainedSlice(msg.readerIndex, size))
          msg.skipBytes(size)

          queue.enqueue(packet, signalID, deadline) flatMap {
            case false                      => FutureFalse
            case true if msgNum == msgCount => FutureTrue
            case true                       => write(msgNum + 1)
          }
        }

        try {
          write(1) ensure {
            ReferenceCountUtil.release(msg)
          }
        } finally {
          scope foreach { _.close() }
        }
      }
    }
  }

  private final class WriteQueue(ch: Channel) extends ChannelInboundHandlerAdapter {

    private[this] val processing = new AtomicBoolean(false)
    private[this] val queue = new ArrayBlockingQueue[Msg](maxOutboundMessages)

    private case class Msg(buf: ByteBuf, to: SignalID, deadline: TimeBound) {
      val size = buf.readableBytes

      private[this] val scope = GlobalTracer.instance.retainActive() // in-queue time
      private[this] val p = Promise[Boolean]()
      private[this] val cp = ch.newPromise

      cp.addListener { cf: ChannelFuture =>
        scope foreach { _.release() }
        stats.decrPendingOutbound(size)
        if (cf.isSuccess) {
          stats.incrTX(to)
          p.success(true)
        } else if (cf.isCancelled) {
          stats.incrMessageExpired(to)
          p.success(false)
        } else {
          stats.incrMessageDropped(to)
          p.success(false)
        }
      }

      def future = p.future

      def discard() = {
        buf.release()
        cp.cancel(false)
      }

      def expire() = if (deadline.hasTimeLeft) false else discard()

      def write() = if (!expire()) ch.write(buf, cp)
    }

    final def enqueue(
      buf: ByteBuf,
      to: SignalID,
      deadline: TimeBound): Future[Boolean] =
      if (deadline.isOverdue) {
        buf.release()
        stats.incrMessageExpired(to)
        FutureFalse
      } else {
        val msg = Msg(buf, to, deadline)
        var offered = queue.offer(msg)

        // if the queue is full, pre-process in an attempt to free up a
        // slot.
        if (!offered) {
          process()

          if (deadline.hasTimeLeft) {
            offered = queue.offer(msg)
          }
        }

        process()

        stats.recordSentSize(to, msg.size)

        if (offered) {
          stats.incrPendingOutbound(msg.size)
          msg.future
        } else {
          msg.buf.release()
          stats.incrMessageDropped(to)
          FutureFalse
        }
      }

    private[this] val processRunner = new Runnable {
      def run() = {

        // write it!
        if (ch.isWritable) {
          while (!queue.isEmpty && ch.isWritable) {
            val m = queue.poll
            m.write()
          }
          ch.flush()

          // channel closed
        } else if (!ch.isActive) {
          var m = queue.poll
          while (m != null) {
            m.discard()
            m = queue.poll
          }

          // queue full
        } else if (queue.remainingCapacity == 0) {
          val iter = queue.iterator

          while (iter.hasNext) {
            val m = iter.next
            if (m.expire()) iter.remove()
          }
        }

        processing.set(false)
        if (!queue.isEmpty && (ch.isWritable || !ch.isActive)) process()
      }
    }

    private def process(): Unit =
      if (processing.compareAndSet(false, true)) {
        ch.eventLoop.execute(processRunner)
      }

    override def channelWritabilityChanged(ctx: ChannelHandlerContext) = {
      if (ch.isWritable) process()
      super.channelWritabilityChanged(ctx)
    }

    override def channelInactive(ctx: ChannelHandlerContext) = {
      process()
      super.channelInactive(ctx)
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable) = {
      e match {
        case e: IOException if e.getMessage.contains("Connection reset by peer") =>
          ()
        case e => logExceptionWithPrefix(ch.remoteAddress, e)
      }

      ctx.channel.closeQuietly()
    }
  }

  def stop(): Unit = {
    implicit val ec = ImmediateExecutionContext
    val futs = msgConnections.values().asScala map {
      _.future flatMap {
        case Some(c) => c.channel.close.toFuture.unit
        case None    => Future.unit
      }
    }
    Await.result(futs.join, Duration.Inf)
  }
}
