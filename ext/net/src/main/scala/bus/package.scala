package fauna.net

import fauna.codex.cbor.CBOR
import fauna.exec.Timer
import fauna.lang.syntax._
import fauna.lang.TimeBound
import fauna.trace.GlobalTracer
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration._
import scala.util.Random

package bus {
  object SignalID {
    private val IDBlockSize = 10000
    private val nextBlock = new AtomicInteger(Random.nextInt())

    // A thread-local block of ephemeral IDs
    private final class IDBlock {
      private[this] var _next: Int = 0
      private[this] var limit: Int = 0

      def next = {
        if (_next == limit) {
          _next = nextBlock.getAndAdd(IDBlockSize)
          limit = _next + IDBlockSize
        }
        val n = _next
        _next += 1
        n
      }
    }

    private val idBlock = new ThreadLocal[IDBlock] {
      override def initialValue() = new IDBlock
    }

    def temp: SignalID = SignalID(idBlock.get().next | Int.MinValue)

    implicit val CBORCodec = CBOR.TupleCodec[SignalID]
  }

  case class SignalID(toInt: Int) extends AnyVal {
    def isTemp = toInt < 0
    def at(host: HostDest) = HandlerID(host, this)
    def at(host: HostInfo) = HandlerID(host, this)
    def description: String = s"dedicated connection ${toInt}"
  }

  object HandlerID {
    implicit val CBORCodec = CBOR.TupleCodec[HandlerID]

    def apply(info: HostInfo, signalID: SignalID): HandlerID =
      HandlerID(info.id, signalID)
  }

  final case class HandlerID(host: HostDest, signalID: SignalID)

  trait Protocol[M] {
    type Msg = M

    val name: String

    // FIXME: hide CBOR via decode/encode methods?
    val codec: CBOR.Codec[Msg]
  }

  final case class MsgProtocol[M](name: String)(implicit val codec: CBOR.Codec[M])
      extends Protocol[M]

  object Protocol {
    final case class Reply[M, R](name: String)(implicit val codec: CBOR.Codec[R])
        extends Protocol[R]

    def apply[M: CBOR.Codec](name: String): Protocol[M] = MsgProtocol[M](name)
  }

  final class SinkCtx[+P <: Protocol[_]](
    private[SinkCtx] val bus: MessageBus,
    val protocol: P,
    val dest: HandlerID,
    val gathering: Boolean) {

    def send(
      msg: protocol.Msg,
      deadline: TimeBound = Timer.LongTimeoutThreshold.bound)(
      implicit ec: ExecutionContext): Future[Boolean] =
      GlobalTracer.instance.withSpan(protocol.name) {
        bus.send(dest.signalID, dest.host, msg, protocol.name, deadline, gathering)(
          ec,
          protocol.codec)
      }

    def request[V <: protocol.Msg, R](
      ctor: SignalID => V,
      deadline: TimeBound = Timer.LongTimeoutThreshold.bound)(
      implicit ec: ExecutionContext,
      reply: Protocol.Reply[V, R]) = {
      val ret = Promise[Option[R]]()
      val timeout = Timer.Global.scheduleTimeout(deadline.timeLeft) {
        ret.trySuccess(None)
      }

      val src = bus.tempHandler(reply, Duration.Inf) { case (_, msg, _) =>
        ret.trySuccess(Some(msg))
        timeout.cancel()
        Future.unit
      }

      send(ctor(src.id.signalID), deadline)

      ret.future andThen { case _ => src.close() }
    }
  }

  trait HandlerCtx {
    val id: HandlerID
    def close(): Unit
    def isClosed: Boolean
  }

  package object transport {
    // Minimum and maximum transport versions supported by this build, inclusive
    val MinVersion = 2
    val MaxVersion = 3

    val SSLVariant = "ssl"
    val ClearVariant = "clear"
  }
}
