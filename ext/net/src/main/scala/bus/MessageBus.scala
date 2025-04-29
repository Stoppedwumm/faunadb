package fauna.net.bus

import fauna.atoms._
import fauna.codex.cbor.CBOR
import fauna.exec._
import fauna.lang._
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.net._
import fauna.net.bus.transport._
import fauna.net.security.{ NoSSL, SSLConfig }
import fauna.net.util.ByteBufStream
import fauna.stats.StatsRecorder
import fauna.trace._
import io.netty.buffer.{ ByteBuf, ByteBufAllocator }
import io.netty.channel.Channel
import io.netty.util.ReferenceCountUtil
import java.net.BindException
import java.util.UUID
import java.util.concurrent.{ ConcurrentHashMap, TimeoutException }
import java.util.concurrent.atomic.{ AtomicReference, LongAdder }
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success }

import MessageBus._

/**
  * MessageBus provides an abstraction for the sending of 1-way
  * messages to registered handlers within a cluster. It provides no
  * guarantees for message delivery (order, reliability, etc.), but
  * abstracts out connection handling, multiplexing, version
  * negotiation, pipelining, TLS, etc.
  *
  * # Virtual IDs
  *
  * Each bus has a physical address (host:port), and a virtual ID
  * (UUID). Messages may be routed to either, although no native
  * mechanism is provided for discovering the HostID -> HostAddress
  * mapping.
  *
  * If virtual IDs are used, the internal map should be maintained via
  * the methods `registerHostID` and `unregisterHostID`. Nodes are
  * aware of their own and connected peers' HostIDs, however:
  * Misrouted messages will be dropped. Therefore this mapping may be
  * maintained in an eventually consistent manner.
  */
object MessageBus {
  val DefaultConnectTimeout = 5.seconds
  val DefaultHandshakeTimeout = 5.seconds
  val DefaultIdleTimeout = 1.minute
  val DefaultKeepaliveInterval = 1.second
  val DefaultKeepaliveTimeout = 10.seconds
  val DefaultMaxOutboundMessages = 16384
  val DefaultMaxMessageSize = Int.MaxValue

  val DefaultHandlerExpiry = 10.seconds

  trait Dispatcher {
    def recvMessage(from: HostInfo, signalID: SignalID, bytes: ByteBufStream, deadline: TimeBound): Unit
    def recvTCPStream(from: HostInfo, signalID: SignalID, ch: Channel): Unit

    def sendBytes(signalID: SignalID, to: HostDest, message: ByteBuf, deadline: TimeBound, reason: String)(implicit ec: ExecutionContext): Future[Boolean]
    def openTCPStreamWithConfig(dest: HandlerID, reason: String)(configure: Channel => Unit)(implicit ec: ExecutionContext): Future[Channel]
  }

  class Stats(stats: StatsRecorder, statsSigs: Set[SignalID]) {
    private val handlerCount = new LongAdder
    private val clientConnCount = new LongAdder
    private val outboundMessageCount = new LongAdder
    private val outboundMessageBytesCount = new LongAdder
    private val openTCPStreams = new LongAdder

    StatsRecorder.polling(10.seconds) {
      stats.set("Bus.Handlers", handlerCount.sum)
      stats.set("Bus.Connections", clientConnCount.sum)
      stats.set("Bus.Messages.Pending", outboundMessageCount.sum)
      stats.set("Bus.Messages.Pending.Bytes", outboundMessageBytesCount.sum)
      stats.set("Bus.TCPStreams.Open", openTCPStreams.sum)
    }

    class MemoizedStat(mkStat: String => String) {
      val ref = new AtomicReference[Map[Int, String]](Map.empty)
      val global = mkStat("")
      val temp = mkStat(".SigTemp")

      def apply(sig: SignalID): String =
        if (sig.isTemp) {
          temp
        } else {
          apply0(sig.toInt)
        }

      @tailrec final def apply0(sigID: Int): String = {
        val map = ref.get
        map.get(sigID) match {
          case Some(str) => str
          case None =>
            ref.compareAndSet(map, map + (sigID -> mkStat(s".Sig$sigID")))
            apply0(sigID)
        }
      }
    }

    private def isSpecial(sig: SignalID) = statsSigs.isEmpty || statsSigs(sig)

    private[this] def statsIncr(m: MemoizedStat, sig: SignalID) = {
      if (isSpecial(sig)) stats.incr(m(sig))
      stats.incr(m.global)
    }

    val MessagesOut = new MemoizedStat(c => s"Bus.Messages.Out$c")
    def incrTX(sig: SignalID) = statsIncr(MessagesOut, sig)

    val MessagesIn = new MemoizedStat(c => s"Bus.Messages.In$c")
    def incrRX(sig: SignalID) = statsIncr(MessagesIn, sig)

    val MessagesHandled = new MemoizedStat(c => s"Bus.Messages.Handled$c")
    def incrMessageHandled(sig: SignalID) = statsIncr(MessagesHandled, sig)

    // The number of inbound messages that were not successfully received.
    val MessagesErrors = new MemoizedStat(c => s"Bus.Messages.Errors$c")
    def incrMessageErrors(sig: SignalID) = statsIncr(MessagesErrors, sig)

    // The number of inbound messages whose deadlines expired before being passed to a handler,
    // plus the number of outbound messages whose deadlines expired before the message was sent.
    // TODO: Should this be two different metrics? It mixes inbound and outbound counts.
    val MessagesExpired = new MemoizedStat(c => s"Bus.Messages.Expired$c")
    def incrMessageExpired(sig: SignalID) = statsIncr(MessagesExpired, sig)

    // The number of outbound messages that were not sent by the bus, except for expired messages.
    val MessagesDropped = new MemoizedStat(c => s"Bus.Messages.Dropped$c")
    def incrMessageDropped(sig: SignalID) = statsIncr(MessagesDropped, sig)

    // The number of client->server connections closed due to keepalive timeouts
    def incrKeepaliveTimeouts() = stats.incr("Bus.Connections.KeepaliveTimeouts")

    // The number of inbound messages ignored for one of the following reasons:
    // 1. no handler was defined for the message
    // 2. the message header indicates the message is for a different stream than the current one
    // 3. a message frame indicates the message is for a nonexistent stream
    // TODO: Are cases 2 and 3 better described as invalid messages?
    //            Could a message with many frames be counted many times in case 3?
    val MessagesIgnored = new MemoizedStat(c => s"Bus.Messages.Ignored$c")
    def incrMessageIgnored(sig: SignalID) = statsIncr(MessagesIgnored, sig)

    // The number of inbound messages received erroneously by a TCP stream handler.
    val MessagesInvalid = new MemoizedStat(c => s"Bus.Messages.Invalid$c")
    def incrMessageInvalid(sig: SignalID) = statsIncr(MessagesInvalid, sig)

    val ReceivedSize = new MemoizedStat(c => s"Bus.Bytes.In$c")
    val ReceivedThroughput = new MemoizedStat(c => s"Bus.Bytes.In.Throughput$c")
    def recordReceivedSize(sig: SignalID, size: Int) = {
      stats.timing(ReceivedSize(sig), size)
      stats.count(ReceivedThroughput(sig), size)
      stats.count(ReceivedThroughput.global, size)
    }

    val SentSize = new MemoizedStat(c => s"Bus.Bytes.Out$c")
    val SentThroughput = new MemoizedStat(c => s"Bus.Bytes.Out.Throughput$c")
    def recordSentSize(sig: SignalID, size: Int) = {
      stats.timing(SentSize(sig), size)
      stats.count(SentThroughput(sig), size)
      stats.count(SentThroughput.global, size)
    }

    def incrHandlerExpired() = stats.incr("Bus.Handlers.Expired")
    def incrHandlers() = { stats.incr("Bus.Handlers.Bind"); handlerCount.increment }
    def decrHandlers() = { stats.incr("Bus.Handlers.Unbind"); handlerCount.decrement }
    def incrHostIDLookupFailures() = { stats.incr("Bus.HostID.Lookup.Failures") }
    def incrClientConnErrors() = stats.incr("Bus.Connections.Errors")
    def incrClientConnections() = { stats.incr("Bus.Connections.Open"); clientConnCount.increment }
    def decrClientConnections() = { stats.incr("Bus.Connections.Close"); clientConnCount.decrement }

    def incrInboundTCPStreams() = stats.incr("Bus.TCPStreams.Inbound.Established")
    def incrOutboundTCPStreams() = stats.incr("Bus.TCPStreams.Outbound.Established")
    def incrTCPStreamInvalid() = stats.incr("Bus.TCPStreams.Invalid")

    def incrOpenTCPStreams() = openTCPStreams.increment
    def decrOpenTCPStreams() = openTCPStreams.decrement

    def incrPendingOutbound(size: Int) = {
      outboundMessageCount.increment
      outboundMessageBytesCount.add(size)
    }
    def decrPendingOutbound(size: Int) = {
      outboundMessageCount.decrement
      outboundMessageBytesCount.add(size * -1)
    }
  }

  type ServerParams = (String, String, Int, SSLConfig, Option[String])

  def apply(
    cluster: String,
    hostID: HostID,
    address: String,
    internalAddress: Option[String] = None,
    zone: Option[String] = None,
    ssl: SSLConfig = NoSSL,
    port: Int = 7000,
    sslPort: Int = 7001,
    connectionsPerHost: Int = 1,
    connectTimeout: FiniteDuration = DefaultConnectTimeout,
    handshakeTimeout: FiniteDuration = DefaultHandshakeTimeout,
    idleTimeout: FiniteDuration = DefaultIdleTimeout,
    keepaliveInterval: FiniteDuration = DefaultKeepaliveInterval,
    keepaliveTimeout: FiniteDuration = DefaultKeepaliveTimeout,
    maxOutboundMessages: Int = DefaultMaxOutboundMessages,
    maxMessageSize: Int = DefaultMaxMessageSize,
    timer: Timer = Timer.Global,
    stats: StatsRecorder = StatsRecorder.Null,
    statsSigs: Set[SignalID] = Set.empty): MessageBus = {

    val listen = (internalAddress getOrElse address)
    val hostAddress = HostAddress(address, if (ssl == NoSSL) port else sslPort)

    val sslParams = if (ssl != NoSSL) {
      List((address, listen, sslPort, ssl, None))
    } else {
      Nil
    }

    val clearParams = if (ssl == NoSSL) {
      List((address, listen, port, NoSSL, None))
    } else if (zone.isDefined) {
      List((listen, listen, port, NoSSL, zone))
    } else {
      Nil
    }

    val serverParams = clearParams ++ sslParams
    val clearZone = serverParams.head._5

    new MessageBus(
      cluster,
      HostInfo(hostID, hostAddress),
      clearZone,
      ssl,
      serverParams,
      connectionsPerHost,
      connectTimeout,
      handshakeTimeout,
      idleTimeout,
      keepaliveInterval,
      keepaliveTimeout,
      maxOutboundMessages,
      maxMessageSize,
      timer,
      stats,
      statsSigs
    )
  }
}

class MessageBus(
  val clusterID: String,
  val hostInfo: HostInfo,
  val zone: Option[String],
  ssl: SSLConfig,
  serverParams: List[ServerParams],
  connectionsPerHost: Int,
  connectTimeout: FiniteDuration,
  handshakeTimeout: FiniteDuration,
  idleTimeout: FiniteDuration,
  keepaliveInterval: FiniteDuration,
  keepaliveTimeout: FiniteDuration,
  maxOutboundMessages: Int,
  maxMessageSize: Int,
  timer: Timer,
  statsRecorder: StatsRecorder,
  statsSigs: Set[SignalID])
    extends Service
    with ExceptionLogging { bus =>

  val hostID = hostInfo.id
  val hostAddress = hostInfo.address

  private val log = getLogger

  // FIXME: When we get priority wired up, add priority name somehow.
  private val stats = new Stats(statsRecorder, statsSigs)

  private val handlers = new ConcurrentHashMap[SignalID, Handler]

  private val hostIDMap = new ConcurrentHashMap[HostID, List[HostAddress]]
  hostIDMap.put(hostID, List(hostAddress))

  private val dedicatedStreamSignals = new AtomicReference[Set[SignalID]](Set.empty)

  private val client = new Client(
    clusterID,
    hostInfo,
    hostIDMap,
    dedicatedStreamSignals,
    zone,
    ssl,
    connectionsPerHost,
    connectTimeout,
    handshakeTimeout,
    idleTimeout,
    keepaliveTimeout,
    maxOutboundMessages,
    maxMessageSize,
    stats
  )

  private val aliases = serverParams map { case (address, _, port, ssl, zone) =>
    val variant = if (ssl == NoSSL) ClearVariant else SSLVariant
    Alias(HostAddress(address, port), variant, zone)
  } toVector

  private val servers = serverParams collect {
    case (address, listen, port, ssl, _) =>
      new Server(
        clusterID,
        HostInfo(hostID, HostAddress(address, port)),
        aliases,
        HostAddress(listen, port),
        ssl,
        hostIDMap,
        handshakeTimeout,
        idleTimeout,
        keepaliveInterval,
        stats,
        dispatcher
      )
  }

 /** 
   * This intermediate object allows us to insert a shim which can intercept
   * messages and channels bidirectionally.
   */
  private var dispatcher = new MessageBus.Dispatcher {
    def recvMessage(from: HostInfo, to: SignalID, bufF: ByteBufStream, deadline: TimeBound): Unit = 
      bus.recv(from, to, bufF, deadline)

    def recvTCPStream(from: HostInfo, to: SignalID, ch: Channel) =
      bus.recvTCPStream(from, to, ch)

    def sendBytes(signalID: SignalID, to: HostDest, message: ByteBuf, deadline: TimeBound, reason: String)(implicit ec: ExecutionContext): Future[Boolean] =
      client.sendBytes(signalID, to, message, deadline, reason)

    def openTCPStreamWithConfig(dest: HandlerID, reason: String)(configure: Channel => Unit)(implicit ec: ExecutionContext): Future[Channel] =
      client.openTCPStream(dest.signalID, dest.host, reason)(configure)
  }

  def shimDispatcher(f: MessageBus.Dispatcher => MessageBus.Dispatcher): Unit = {
    log.warn("Shimming MessageBus transport dispatcher. (This should only be used for testing purposes.)")
    dispatcher = f(dispatcher)
  }

  // implement Service

  def isRunning = servers exists { _.isRunning }

  def start() = servers foreach { server =>
    try {
      server.start()
    } catch {
      case e: BindException =>
        log.error(s"Unable to bind to ${server.listenAddress}. Verify network_listen_address is set to a valid local network address.")
        throw e
    }
  }

  def stop(graceful: Boolean) = {
    servers foreach {
      _.stop(graceful)
    }
    client.stop()
  }

  /**
    * Mark a SignalID to receive a dedicated TCP connection. This should be done
    * on both the send and receive sides to ensure setup remains correct, even
    * though currently this only affects connecting, which happens on the
    * client.
    */
  def useDedicatedStream(signalID: SignalID): Unit = {
    require(!signalID.isTemp, "Cannot created dedicated TCP connection for temporary signal")
    dedicatedStreamSignals.updateAndGet { _ + signalID }
  }

  /**
    * Associate a virtual host ID with an known address.
    */
  def registerHostID(id: HostID, addr: HostAddress): Unit =
    if (id != hostID) {
      hostIDMap.compute(id, { (_, addrs) =>
        if (addrs eq null) {
          addr :: Nil
        } else if (!addrs.contains(addr)) {
          addr :: addrs
        } else {
          addrs
        }
      })
    }

  /**
    * Forget a virtual host ID.
    */
  def unregisterHostID(id: HostID): Unit =
    if (id != hostID) {
      hostIDMap.remove(id)
    }

  /**
    * Sets the registered host IDs. Hosts with IDs not in the passed
    * set are deregistered.
    */
  def setRegisteredHostIDs(ids: Set[HostID]): Unit = {
    val ids0 = ids + hostID
    hostIDMap forEach { (id, addr) => if (!(ids0 contains id)) hostIDMap.remove(id, addr) }
  }

  /**
    * Get the current mapping of a virtual host ID to an address
    */
  def getHostAddresses(id: HostID): List[HostAddress] =
    if (id == hostID) {
      List(hostAddress)
    } else {
      Option(hostIDMap.get(id)).getOrElse(Nil)
    }

  /**
    * Search for the address for a given virtual host id. This may be
    * slow.
    */
  def getFirstHostID(addr: HostAddress): Option[HostID] =
    if (addr == hostAddress) {
      Some(hostID)
    } else {
      val h = hostIDMap.searchEntries(Long.MaxValue, { e =>
        if (e.getValue.contains(addr)) {
          e.getKey.uuid
        } else {
          null
        }
      })
      Option(h) map { HostID(_) }
    }

  private def getHandler(signalID: SignalID) = Option(handlers.get(signalID))

  private final class BoundHandlerCtx(h: Handler, val id: HandlerID) extends HandlerCtx {
    def isClosed = !isBound(id.signalID, h)
    def close() = unbind(id.signalID, h)
  }

  /**
    * Register a handler to receive messages on the local node,
    * destined for @signalID.
    */
  def bind(signalID: SignalID, handler: Handler, idle: Duration = Duration.Inf): HandlerCtx = {
    if (handlers.putIfAbsent(signalID, handler) ne null) {
      throw new IllegalStateException(s"$signalID is already registered")
    }

    log.trace(s"Bound to $signalID with idle: $idle: $handler")
    stats.incrHandlers()
    scheduleExpire(signalID, handler, idle)

    new BoundHandlerCtx(handler, HandlerID(hostInfo, signalID))
  }

  def unbind(signalID: SignalID): Unit = {
    val prev = handlers.remove(signalID)
    if (prev ne null) {
      stats.decrHandlers()
      prev.setDone()
    }
  }

  def unbind(signalID: SignalID, handler: Handler): Unit = {
    if (handlers.remove(signalID, handler)) {
      stats.decrHandlers()
      handler.setDone()
    }
  }

  def isBound(signalID: SignalID): Boolean =
    handlers.containsKey(signalID)

  def isBound(signalID: SignalID, handler: Handler): Boolean =
    handlers.get(signalID) eq handler

  /**
    * Create a sink which sends typed messages to a target handler.
    * See info on `gathering` in send()
    */
  def sink[P <: Protocol[_]](protocol: P, dest: HandlerID, gathering: Boolean = false): SinkCtx[P] =
    new SinkCtx(this, protocol, dest, gathering)

  /**
   * Open a dedicated TCP stream with the target handler configured for use with ChannelSyntax methods.
   */
  def openTCPStream(dest: HandlerID, reason: String)(implicit ec: ExecutionContext): Future[Channel] =
    openTCPStreamWithConfig(dest, reason) { _.enableChannelSyntax() }

  /**
   * Open a dedicated TCP stream with the target handler with manual configuration.
   */
  def openTCPStreamWithConfig(dest: HandlerID, reason: String)(configure: Channel => Unit)(implicit ec: ExecutionContext): Future[Channel] =
    dispatcher.openTCPStreamWithConfig(dest, reason)(configure)

  /**
    * Bind an `M` protocol handler to `signalID`. If `autoRelease` is
    * false, the underlying message buffer is released when `f`'s
    * return future completes. If `autorelease` is true, the buffer is
    * decoded with proper retain/release semantics and `f` is
    * responsible for releasing component buffers of the message.
    */
  def handler[M](
    protocol: Protocol[M],
    signalID: SignalID,
    expiry: Duration = Duration.Inf,
    bufRetention: CBOR.BufRetention = CBOR.BufRetention.Borrow)(
    f: (HostInfo, M, TimeBound) => Future[Unit],
    expired: => Future[Unit] = Future.unit): HandlerCtx = {
    val h = Handler[M](protocol.name, f, expired, bufRetention)(protocol.codec)
    bind(signalID, h, expiry)
  }

  /**
    * Like `handler` but `f` is passed a future which will complete
    * with a fully received message, rather than being invoked after
    * full message receipt.
    */
  def asyncHandler[M](
    protocol: Protocol[M],
    signalID: SignalID,
    expiry: Duration = Duration.Inf,
    bufRetention: CBOR.BufRetention = CBOR.BufRetention.Borrow)(
    f: (HostInfo, Future[M], TimeBound) => Future[Unit],
    expired: => Future[Unit] = Future.unit): HandlerCtx = {
    val h = Handler.async[M](protocol.name, f, expired, bufRetention)(protocol.codec)
    bind(signalID, h, expiry)
  }

  /**
    * Bind a handler to `signalID` which expects raw TCP stream connections.
    */
  def tcpStreamHandler(signalID: SignalID)(f: (HostInfo, Channel) => Future[Unit]): HandlerCtx = 
    bind(signalID, Handler.stream(f, Future.unit, enableChannelSyntax = true))

  /**
    * See `handler`. A temporary signalID is assigned and returned.
    */
  @annotation.tailrec
  final def tempHandler[M](
    protocol: Protocol[M],
    expiry: Duration = DefaultHandlerExpiry,
    bufRetention: CBOR.BufRetention = CBOR.BufRetention.Borrow)(
    f: (HostInfo, M, TimeBound) => Future[Unit],
    expired: => Future[Unit] = Future.unit): HandlerCtx =
    try {
      handler(protocol, SignalID.temp, expiry, bufRetention)(f, expired)
    } catch {
      case _: IllegalStateException =>
        tempHandler(protocol, expiry, bufRetention)(f, expired)
    }

  /**
   *  See `tcpStreamHandler`. A temporary signalID is assigned and returned.
   */
  @annotation.tailrec
  final def tempTCPStreamHandler(f: (HostInfo, Channel) => Future[Unit]): HandlerCtx = 
    try {
      tcpStreamHandler(SignalID.temp)(f)
    } catch {
      case _: IllegalStateException => tempTCPStreamHandler(f)
    }

  def source[M](
    protocol: Protocol[M],
    signalID: SignalID,
    expiry: Duration = Duration.Inf,
    maxPending: Int = Source.DefaultMaxPending): Source[M] = {

    val l = new QueueSource(this, protocol, HandlerID(hostInfo, signalID), maxPending)
    bind(signalID, l, expiry)
    l
  }

  @annotation.tailrec
  final def tempSource[M](protocol: Protocol[M], expiry: Duration = Duration.Inf): Source[M] =
    try {
      source(protocol, SignalID.temp, expiry)
    } catch {
      case _: IllegalStateException => tempSource(protocol, expiry)
    }

  // expiry

  private def expireHandler(signalID: SignalID, handler: Handler): Unit =
    if (handlers.remove(signalID, handler)) {
      stats.incrHandlerExpired()
      handler.recvExpire()
    }

  private def scheduleExpire(signalID: SignalID, handler: Handler, idle: Duration): Unit =
    idle match {
      case idle: FiniteDuration =>
        handler.synchronized {
          if (!handler.isDone) {
            val timeout = timer.scheduleTimeout(idle) {
              if (handler.isIdle) {
                expireHandler(signalID, handler)
              } else {
                handler.isIdle = true
                scheduleExpire(signalID, handler, idle)
              }
            }

            // Overwrite the previous timeout, because that one has expired.
            handler.timeout = timeout
          }
        }

      case _ => ()
    }

  protected def recv(
    from: HostInfo,
    to: SignalID,
    bytestream: ByteBufStream,
    deadline: TimeBound): Unit = {

    def releaseStream() =
      bytestream.close()

    implicit val _ec = ImmediateExecutionContext
    val tracer = GlobalTracer.instance

    stats.incrRX(to)

    if (deadline.isOverdue) {
      val msg = s"Received expired message from $from, to $to."
      log.trace(msg)
      stats.incrMessageExpired(to)
      tracer.activeSpan foreach { _.setStatus(DeadlineExceeded(msg)) }
      releaseStream()
    } else {
      getHandler(to) match {
        case Some(h) if !h.isTCPStreamHandler =>
          h.isIdle = false
          val f = GuardFuture {
            bytestream onChunk { () => h.isIdle = false }
            h.recvBytes(from, bytestream.bytes, deadline)
          }

          f onComplete {
            case Success(_) => stats.incrMessageHandled(to)
            case Failure(e) =>

              e match {
                case _: TimeoutException =>
                  () // Silence timeouts from evicted streams. See `Server.streams`.
                case _ => logException(e)
              }

              stats.incrMessageErrors(to)
              tracer.activeSpan foreach {
                _.setStatus(InternalError(e.getMessage))
              }
          }

        case Some(_) =>
          val msg = s"Ignored byte message for tcp handler $to."
          log.warn(msg)
          stats.incrMessageInvalid(to)
          tracer.activeSpan foreach {
            _.setStatus(NotFound(msg))
          }
          releaseStream()

        case None =>
          val msg = s"Ignored message for unbound $to."
          log.trace(msg)
          stats.incrMessageIgnored(to)
          tracer.activeSpan foreach {
            _.setStatus(NotFound(msg))
          }
          releaseStream()
      }
    }
  }

  protected def recvTCPStream(from: HostInfo, to: SignalID, ch: Channel): Unit = 
    getHandler(to) match {
      case Some(h) if h.isTCPStreamHandler =>
        h.isIdle = false
        try {
          implicit val ec = ImmediateExecutionContext
          h.recvTCPStream(from, ch) onComplete { rv =>
            if (rv.isFailure) {
              logException(rv)
              ch.closeQuietly()
            }
          }
        } catch {
          case NonFatal(e) =>
            logException(e)
            ch.closeQuietly()
        }

      case Some(_) =>
        log.warn(s"Ignored tcp stream connection for message handler $to.")
        stats.incrTCPStreamInvalid()
        ch.closeQuietly()

      case None =>
        log.trace(s"Ignored tcp stream connection for unbound $to.")
        ch.closeQuietly()
    }

  /**
    * Sends a message to a handler, once the handler is able to
    * receive more messages. The returned future will be complete when
    * the message is successfully sent.
    *
    * If `gathering` is set to true, constituent ByteBufs of the
    * message T are gathered into a composite ByteBuf for send rather
    * than copied. This requires the ByteBufs to be properly
    * refcounted so is off by default.
    * FIXME: make gathering mode the only option
    *
    * Currently, a node/handler is considered "busy" if the underlying
    * connection is not writable due to there being too many
    * outstanding messages.
    */
  def send[T](signalID: SignalID, to: HostDest, message: T, reason: String, deadline: TimeBound = TimeBound.Max, gathering: Boolean = false)(implicit ec: ExecutionContext, enc: CBOR.Encoder[T]): Future[Boolean] =
    if (deadline.isOverdue) {
      FutureFalse
    } else {
      val alloc = ByteBufAllocator.DEFAULT
      val buf = if (gathering) {
        CBOR.gatheringEncode(alloc.compositeBuffer, message)
      } else {
        val b = CBOR.encode(alloc.buffer, message)
        ReferenceCountUtil.release(message)
        b
      }

      sendBytes(signalID, to, buf, reason, deadline)
    }

  /**
    * Same semantics as send, but bytes do not go through an encoding
    * step in order to minimize data copies.
    */
  def sendBytes(signalID: SignalID, to: HostDest, message: ByteBuf, reason: String, deadline: TimeBound = TimeBound.Max)(implicit ec: ExecutionContext): Future[Boolean] =
    dispatcher.sendBytes(signalID, to, message, deadline, reason)
}

object MessageBusTestContext {
  case class Builder(
    newBusF: (HostID, String, Int) => MessageBus,
    dispatcherShimF: MessageBus.Dispatcher => MessageBus.Dispatcher = null,
    useUniqueIPs: Boolean = false,
    fixedPort: Int = 0
  ) {
    def withUniqueIPs = copy(useUniqueIPs = true)

    def withFixedPort(port: Int) = copy(fixedPort = port)

    def withDispatcherShim(f: MessageBus.Dispatcher => MessageBus.Dispatcher) = copy(dispatcherShimF = f)

    def build() = 
      new MessageBusTestContext(useUniqueIPs, fixedPort, newBusF, dispatcherShimF)

    def withContext[T](f: MessageBusTestContext => T): T = {
      val ctx = build()
      try {
        f(ctx)
      } finally {
        ctx.stopAll()
      }
    } 
  }

  def default = 
    Builder(newBusF = (id, addr, port) => MessageBus("test", id, addr, port = port))
}

class MessageBusTestContext(
  useUniqueIPs: Boolean,
  fixedPort: Int,
  newBusF: (HostID, String, Int) => MessageBus,
  dispatcherShimF: MessageBus.Dispatcher => MessageBus.Dispatcher = null,
) {

  require(useUniqueIPs || fixedPort == 0, "Cannot use a fixed port and reuse 127.0.0.1")

  private var nextID = 0
  private var buses = List.empty[MessageBus]

  def allBuses() = buses

  def stopAll() = buses foreach { _.stop() }

  def nextHostIDAndAddress() = synchronized {
    nextID += 1
    val hostID = HostID(new UUID(0, nextID))
    val address = if (useUniqueIPs) s"127.0.0.$nextID" else "127.0.0.1"
    val port = if (fixedPort > 0) fixedPort else Network.findFreePort()
    (hostID, address, port)
  }

  def createBus(): MessageBus = {
    val bus = newBusF.tupled(nextHostIDAndAddress())
    if (dispatcherShimF ne null) {
      bus.shimDispatcher(dispatcherShimF)
    }
    bus.start()
    synchronized {
      buses foreach { bus0 =>
        bus.registerHostID(bus0.hostID, bus0.hostAddress)
        bus0.registerHostID(bus.hostID, bus.hostAddress)
      }
      buses = bus :: buses
    }
    bus
  }
}
