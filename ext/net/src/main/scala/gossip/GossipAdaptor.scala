package fauna.net.gossip

import fauna.codex.cbor.{ CBOR, CBORInvalidLengthException }
import fauna.lang.{ NamedPoolThreadFactory, Service }
import fauna.lang.clocks.Clock
import fauna.net.{ HostDest, HostInfo }
import fauna.net.bus.{ Handler, HandlerCtx, MessageBus, Protocol, SignalID }
import fauna.stats.StatsRecorder
import fauna.lang.syntax._
import io.netty.buffer.{ ByteBuf, Unpooled }
import java.util.concurrent.{ ConcurrentHashMap, Executors }
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.{ DurationInt, FiniteDuration }
import scala.util.Random

object GossipAdaptor {

  val GossipRate = 100.millis
  val Retrans = 2

  // Used only by the single thread running `transmit()`.
  private val Deficit = new ThreadLocal[Int] {
    override protected def initialValue() = 0
  }

  object Segment {

    implicit lazy val codec: CBOR.Codec[Segment] =
      new CBOR.PartialCodec[Segment]("Array") {
        val ts = implicitly[CBOR.Codec[Lamport.Timestamp]]
        val sig = implicitly[CBOR.Codec[SignalID]]
        val bs = implicitly[CBOR.Codec[ByteBuf]]
        val default = CBOR.TupleCodec[Segment]

        override def readArrayStart(length: Long, stream: CBOR.In) =
          length match {
            // make a defensive copy to avoid messing with anyone elses reference counting
            case 3 =>
              Segment(
                ts.decode(stream),
                sig.decode(stream),
                bs.decode(stream).copy())
            case _ => throw CBORInvalidLengthException(length, 3)
          }

        def encode(stream: CBOR.Out, segment: Segment) =
          default.encode(stream, segment)
      }

    implicit lazy val protocol: Protocol[Segment] =
      Protocol[Segment]("gossip.segment")
  }

  /**
    * Single gossip events are encapsulated in segments
    *
    * @param ts The associated lamport timestamp
    * @param forwardTo The signal of the handler for which the gossip is bound
    * @param buf The encoded gossip.
    */
  case class Segment(ts: Lamport.Timestamp, forwardTo: SignalID, buf: ByteBuf) {
    def retainedDuplicate: Segment = copy(buf = buf.retainedDuplicate)
    def retain: Segment = { buf.retain(); this }
    def release(): Unit = buf.release()
  }

  object Request {
    implicit lazy val codec: CBOR.Codec[Request] = CBOR.TupleCodec[Request]

    implicit lazy val protocol: Protocol[Request] =
      Protocol[Request]("gossip.request")
  }

  /**
    * Gossip is aggregated and sent periodically.
    *
    * @param ping The time in micros when the request was sent.
    * @param segments The aggregated gossip.
    */
  case class Request(ping: Long, segments: Vector[Segment])

  object Response {
    implicit lazy val codec = CBOR.TupleCodec[Response]
    implicit lazy val protocol = Protocol.Reply[Request, Response]("gossip.reply")
  }

  /**
    * Response to gossip.
    *
    * @param ping The time in micros recorded by the gossip producer
    * @param pong The time in micros when it was received.
    */
  case class Response(ping: Long, pong: Long)

  private class DupElem(var ts: Long, var events: Set[ByteBuf]) {

    def set(ts: Long, events: Set[ByteBuf]): Unit = {
      this.ts = ts
      this.events = events
    }

    def set(events: Set[ByteBuf]): Unit = {
      this.events = events
    }

    def release(): Unit = {
      events foreach { _.release() }
    }
  }

  object DupTracker {

    def apply(window: Int, ltime: Lamport, stats: StatsRecorder): DupTracker = {
      new DupTracker(
        Array.tabulate[DupElem](window) { i =>
          new DupElem(i.toLong, Set.empty)
        },
        ltime,
        stats)
    }
  }

  /**
    * Class for tracking seen gossip.
    *
    * Gossip is indexed by lamport time and checked by content.
    * Older events are dropped.
    */
  final class DupTracker private (
    times: Array[DupElem],
    ltime: Lamport,
    stats: StatsRecorder) {

    val size = times.length.toLong

    /**
      * Check if we have seen an event.
      *
      * Somewhat unintuitively we will claim to have seen everything once it
      * is too old to care about.
      */
    def seen(lts: Lamport.Timestamp, buf: ByteBuf): Boolean = {
      val curr = ltime().ts
      val ts = lts.ts

      // check if buffer rolled over. If we did the event is too old, drop it.
      if (curr >= size && ts <= curr - size) {
        stats.incr("Gossip.Event.Dropped")
        true
      } else {
        val idx = (ts % size).toInt
        val elem = times(idx)
        elem.synchronized {
          val matchingTS = ts == elem.ts
          val matchingBuf = elem.events contains buf

          if (!matchingTS) {
            elem.release()
            elem.set(ts, Set(buf.retain))
          } else if (!matchingBuf) {
            elem.set(elem.events + buf.retain)
          }
          matchingTS && matchingBuf
        }
      }
    }

    def close(): Unit = {
      synchronized {
        times foreach { _.release() }
      }
    }

  }
}

/**
  * Gossip over the message bus.
  *
  * This adaptor seeks to do 2 things.
  *
  * 1. Allow the scalable dissemination of data over the cluster.
  * 2. Gather clock information, which will eventually be used for cool things™
  * 3. Eventually participate in failure detection.
  *
  * The implementation takes the style of gossip from SWIM (gossip is propagated
  * "infection style" via retransmission) with many implementation cues taken
  * from memberlist / serf (bounded retransmission queue / lamport clocks).
  */
final class GossipAdaptor(
  pingID: SignalID,
  gossipID: SignalID,
  bus: MessageBus,
  clock: Clock,
  limit: Int,
  hosts: => Seq[HostDest],
  stats: StatsRecorder
)(implicit ec: ExecutionContext)
    extends Service {
  import GossipAdaptor._

  // FIXME: calculate a capacity that isn't made up.
  private[this] val pq = new FinitePriorityDeque[Segment](4096, Retrans)

  private[this] val executor = {
    val factory = new NamedPoolThreadFactory("gossip-scheduler", makeDaemons = true)
    Executors.newSingleThreadScheduledExecutor(factory)
  }

  private[this] val rand = new Random
  private[this] val shutdownSection = new Object

  // TODO: persist? I don't think it matters a great deal.
  // The first few messages would get dropped, as soon as it witnesses one high
  // ts from the whole cluster it will catch up.
  private[this] val ltime = Lamport()
  private[this] val dupTracker = DupTracker(4096, ltime, stats)

  private[this] val handlers = new ConcurrentHashMap[SignalID, Handler]

  // forward all messages to their respective local handlers.
  private[this] val forwarder = bus.handler(Request.protocol, gossipID) {
    (host, req, bound) =>
      req.segments foreach {
        case seg @ Segment(ts, id, buf) =>
          ltime.witness(ts)

          // unseen buffers will be retained.
          val seen = dupTracker.seen(ts, buf)
          // This is racy at high enough throughput, but the gossip system
          // would be so overwhelmed at this point the exceptions would be the
          // least of its problems.
          val refCnt = buf.refCnt()
          require(seen && refCnt == 1 || refCnt == 2,
            s"seen=$seen, refCnt=$refCnt")
          val done =
            if (!seen) {
              val local = handlers.get(id)
              // Queued messages are retained.
              push(seg.retain)
              // See comment about racing above.
              val refCnt = buf.refCnt()
              require(refCnt == 3, s"refCnt=$refCnt")
              if (local != null) {
                // Because this is someone elses future, this is where we draw the line
                // on assertions.
                local.recvBytes(
                  host,
                  Future.successful(buf.retainedDuplicate),
                  bound)
              } else {
                Future.unit
              }
            } else {
              Future.unit
            }
          done.ensure {
            // because we don't block either message drain
            // or dup tracking we can't assert the unseen path.
            // but if nothing else is happening it should be 3.
            val refCnt = buf.refCnt()
            require(seen && refCnt == 1 || refCnt >= 1,
              s"seen=$seen, refCnt=$refCnt")
            buf.release
          }
      }

      // we don't need to sync inner access to the buffer because the codec takes
      // a copy
      Future.unit
  }

  // Collate ping responses.
  private[this] val ping = bus.handler(Response.protocol, pingID) { (_, rep, _) =>
    val now = clock.micros
    val sent = rep.ping
    val recv = rep.pong

    val rtt = (now - sent) / 2
    val skew = (recv - rtt) - sent
    // TODO: do something cool with this once we have a specific use case.
    stats.set("Gossip.ClockSkew", skew)
    Future.unit
  }

  private type E = FinitePriorityDeque.Elem[Segment]

  def start(): Unit = {
    executor.scheduleAtFixedRate(
      () => transmit(),
      GossipRate.length,
      GossipRate.length,
      GossipRate.unit)
  }

  def stop(graceful: Boolean) = {
    shutdownSection.synchronized {
      if (isRunning) {
        if (graceful) {
          forwarder.close()
          ping.close()
          dupTracker.close()
          while (pq.nonEmpty) {
            pq.pop() foreach { _.value.release() }
          }
          executor.shutdown
        } else {
          executor.shutdownNow
        }
      }
    }
  }

  def isRunning = !executor.isShutdown

  /**
    * Send a gossip event to the given signal.
    *
    * Most uses will probably want scheduledSend.
    */
  def send[T](signalID: SignalID, message: T)(implicit
    encoder: CBOR.Encoder[T]): Unit = {
    val comp = Unpooled.compositeBuffer
    CBOR.gatheringEncode(comp, message)
    stats.timing("Gossip.Message.Bytes", comp.readableBytes)

    val lts = ltime.increment()
    val seg = Segment(lts, signalID, comp)
    // This shouldn't ever be false, but I'm not sure it's worth an exception.
    val seen = dupTracker.seen(seg.ts, seg.buf)
    // Either we've seen it and retained it in the dup tracker, or we just have
    // ownership for the transmit queue.
    val refCnt = seg.buf.refCnt()
    require(
      (seen && refCnt == 1) ||
        (!seen && refCnt == 2),
      s"seen=$seen, refCnt=$refCnt")
    push(seg)
  }

  private def push(seg: Segment): Unit =
    pq.push(seg)

  /**
    * Send buffered gossip.
    */
  private def transmit(): Unit = {
    val roundLimit = Deficit.get + limit
    var sent = 0
    var canSend = true
    val builder = Vector.newBuilder[Segment]
    builder sizeHint pq.size

    if (pq.isEmpty) {
      Deficit.set(0)
    }

    var e = Option.empty[E]
    var toRequeue = Vector.empty[E]
    var toReplace = Option.empty[E]

    do {
      e = pq.pop()
      e foreach { elem =>
        val seg = elem.value
        val buf = seg.buf
        val userSize = buf.readableBytes
        canSend = sent + userSize <= roundLimit
        if (canSend) {
          // If we are sending we must retain because sending _will_ release.
          builder += seg.retainedDuplicate
          sent += userSize
          toRequeue = toRequeue :+ elem
        } else {
          Deficit.set(roundLimit - sent)
          toReplace = e
        }
      }
    } while (canSend && e.nonEmpty)

    // If we are done with a buffer we must release it.
    // (can't requeue or can't replace)
    toRequeue foreach { elem =>
      if (!pq.requeue(elem)) {
        elem.value.release()
      }
    }

    toReplace foreach { elem =>
      if (!pq.replace(elem)) {
        elem.value.release()
      }
    }

    val req = Request(clock.micros, builder.result())
    selectAndSend(req)
  }

  private def selectAndSend(req: Request): Unit =
    rand.chooseOption(hosts) match {
      // sending must always release
      case Some(to) => bus.send(gossipID, to, req, "gossip", gathering = true)
      case None     => req.segments.foreach { _.release() }
    }

  /**
    * Bind a gossip handler.
    *
    * This also binds to the message bus. The context works as expected unbinding
    * from both.
    */
  def handler[M](signalID: SignalID)(f: (HostInfo, M) => Future[Unit])(implicit
    ev: CBOR.Codec[M]): HandlerCtx = {
    val handler = Handler[M]("gossip", (hostinfo, msg, _) => f(hostinfo, msg))
    val ctx = bus.bind(signalID, handler)
    handlers.put(signalID, handler)
    new HandlerCtx { self =>
      val id = ctx.id

      def close() = {
        handlers.remove(signalID)
        ctx.close()
      }

      def isClosed = ctx.isClosed
    }
  }

  /**
    * Repeatedly invoke a closure to generate gossip over the given signal id.
    */
  def scheduledSend[T](
    signalID: SignalID,
    f: => Option[T],
    interval: FiniteDuration,
    stop: Future[Unit])(implicit ev: CBOR.Encoder[T]) = {
    val task = executor.scheduleAtFixedRate(
      () => {
        f foreach { msg =>
          send(signalID, msg)
        }
      },
      0,
      interval.length,
      interval.unit)

    stop.onComplete { _ =>
      task.cancel(false)
    }
  }
}
