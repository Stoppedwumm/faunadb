package fauna

import com.typesafe.config.{ Config, ConfigFactory }
import fauna.codex.cbor.CBOR
import fauna.prop.PropConfig
import io.netty.buffer._
import java.io._
import org.HdrHistogram.Histogram
import scala.concurrent.duration._

package object qa {

  implicit val FiniteDurationCodec =
    CBOR.AliasCodec[FiniteDuration, Long](FiniteDuration(_, NANOSECONDS), _.toNanos)

  implicit val DurationCodec =
    CBOR.AliasCodec[Duration, Long](Duration.fromNanos(_), _.toNanos)

  implicit val TimeUnitCodec = new CBOR.Codec[TimeUnit] {

    def encode(stream: CBOR.Out, tu: TimeUnit): CBOR.Out =
      CBOR.IntCodec.encode(stream, tu match {
        case DAYS         => 0
        case HOURS        => 1
        case MINUTES      => 2
        case SECONDS      => 3
        case MILLISECONDS => 4
        case MICROSECONDS => 5
        case NANOSECONDS  => 6
      })

    def decode(stream: CBOR.In): TimeUnit =
      CBOR.IntCodec.decode(stream) match {
        case 0 => DAYS
        case 1 => HOURS
        case 2 => MINUTES
        case 3 => SECONDS
        case 4 => MILLISECONDS
        case 5 => MICROSECONDS
        case 6 => NANOSECONDS
      }
  }

  implicit val ConfigCodec =
    CBOR.AliasCodec[Config, String](ConfigFactory.parseString(_), _.root.render)

  implicit val PropConfigCodec = CBOR.RecordCodec[PropConfig]

  implicit val HistogramCodec = new CBOR.Codec[Histogram] {

    def encode(stream: CBOR.Out, histo: Histogram): CBOR.Out = {
      val buf = Unpooled.buffer
      val oos = new ObjectOutputStream(new ByteBufOutputStream(buf))
      oos.writeObject(histo)
      oos.close()
      CBOR.ByteBufCodec.encode(stream, buf)
    }

    def decode(stream: CBOR.In): Histogram = {
      val buf = CBOR.ByteBufCodec.decode(stream)
      val ois = new ObjectInputStream(new ByteBufInputStream(buf))
      val histo = ois.readObject.asInstanceOf[Histogram]
      ois.close()
      histo
    }
  }

  implicit val PropConfCodec = CBOR.RecordCodec[PropConfig]
}

package qa {
  sealed trait Rate

  object Rate {
    case object Max extends Rate
    case class Limited(value: Int, unit: TimeUnit) extends Rate

    object Limited {

      def apply(value: Int, unit: String): Limited = unit match {
        case "day"         => Limited(value, DAYS)
        case "hour"        => Limited(value, HOURS)
        case "minute"      => Limited(value, MINUTES)
        case "second"      => Limited(value, SECONDS)
        case "millisecond" => Limited(value, MILLISECONDS)
      }
    }

    implicit val Codec =
      CBOR.SumCodec[Rate](
        CBOR.SingletonCodec(Max),
        CBOR.RecordCodec[Limited]
      )
  }
}
