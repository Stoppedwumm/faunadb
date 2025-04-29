package fauna.net

import fauna.atoms._
import fauna.codex.cbor._
import fauna.codex.json2._
import fauna.lang.clocks.Clock
import fauna.net.bus.SignalID
import fauna.net.gossip.GossipAdaptor
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration._
import scala.util.Success

object ArrivalRateInfo {
  implicit object Encoder extends JSON.Encoder[ArrivalRateInfo] {
    def encode(out: JSON.Out, arrivals: ArrivalRateInfo): JSON.Out =
      arrivals.toJSON(out)
  }

  implicit def codec: CBOR.Codec[ArrivalRateInfo] =
    new CBOR.Codec[ArrivalRateInfo] {
      def encode(stream: CBOR.Out, info: ArrivalRateInfo): CBOR.Out = {
        CBOR.IntCodec.encode(stream, info.arrivals.size)
        info.arrivals foreach {
          case (account, stats) =>
            CBOR.LongCodec.encode(stream, account.toLong)
            CBOR.LongCodec.encode(stream, stats.readCount)
            CBOR.LongCodec.encode(stream, stats.writeCount)
            CBOR.LongCodec.encode(stream, stats.computeCount)
            CBOR.LongCodec.encode(stream, stats.streamCount)
        }

        stream
      }

      def decode(stream: CBOR.In): ArrivalRateInfo = {
        val size = CBOR.IntCodec.decode(stream)
        val builder = Map.newBuilder[AccountID, LimiterStats]

        for (_ <- 0 until size) {
          val account = CBOR.LongCodec.decode(stream)
          val readCount = CBOR.LongCodec.decode(stream)
          val writeCount = CBOR.LongCodec.decode(stream)
          val computeCount = CBOR.LongCodec.decode(stream)
          val streamCount = CBOR.LongCodec.decode(stream)

          val stats = LimiterStats(readCount, writeCount, computeCount, streamCount)
          builder += AccountID(account) -> stats
        }

        ArrivalRateInfo(builder.result())
      }
    }

  val AccountField = JSON.Escaped("account_id")
  val ArrivalsField = JSON.Escaped("arrivals")
  val OpsField = JSON.Escaped("ops")
  val TSField = JSON.Escaped("ts")
}

final case class ArrivalRateInfo(arrivals: Map[AccountID, LimiterStats]) {
  import ArrivalRateInfo._

  def toJSON(out: JSON.Out): JSON.Out = {
    out.writeObjectStart()

    out.writeObjectField(TSField, out.writeString(Clock.time.toString))

    out.writeObjectField(
      ArrivalsField, {
        out.writeArrayStart()
        arrivals foreach { case (id, stats) =>
          out.writeObjectStart()
          out.writeObjectField(AccountField, out.writeNumber(id.toLong))
          out.writeObjectField(OpsField, stats.toJSON(out))
          out.writeObjectEnd()
        }
        out.writeArrayEnd()
      }
    )

    out.writeObjectEnd()
  }
}

trait ArrivalRateService {
  def poll(): Map[AccountID, LimiterStats]
  def register(arrivals: Map[AccountID, LimiterStats]): Unit
}

/** This class spreads the arrival rate at each limiter among its
  * peers. These rates are used to adjust the number of permits
  * available to the local limiters by the number of permits issued
  * elsewhere in the cluster, simulating a coordinated, global token
  * bucket.
  *
  * See also:
  *   - B. Raghavan et al. "Cloud Control with Distributed Rate Limiting". SIGCOMM 2007
  *   - Yahoo! Cloud Bouncer (ca. 2014)
  */
final class ArrivalRate(
  signal: SignalID,
  interval: FiniteDuration,
  service: ArrivalRateService,
  gossip: GossipAdaptor) {

  private[this] val stop = Promise[Unit]()

  private[this] val handler = gossip.handler[ArrivalRateInfo](signal) {
    case (_, ArrivalRateInfo(arrivals)) =>
      service.register(arrivals)
      Future.unit
  }

  def start(): Unit =
    gossip.scheduledSend(
      signal,
      {
        val arrivals = service.poll()
        Option.when(arrivals.nonEmpty) {
          ArrivalRateInfo(arrivals)
        }
      },
      interval,
      stop.future)

  def stop(): Unit = {
    handler.close()
    stop.tryComplete(Success(()))
  }
}
