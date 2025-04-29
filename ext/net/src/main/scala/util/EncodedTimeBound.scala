package fauna.net.util

import fauna.codex.cbor.CBOR
import fauna.lang.TimeBound
import fauna.lang.syntax._
import scala.concurrent.duration._
import scala.language.implicitConversions

/**
  * Enables CBOR serialization of deadlines. Usually it's enough to
  * just specify EncodedTimeBound in the message field and let implicit
  * conversions do the rest.
  *
  * In order to be portable across machines, despite underlying use of
  * nanoTime(), deadlines are encoded as the number of nanos left.
  */
final case class EncodedTimeBound(deadline: TimeBound) extends AnyVal

object EncodedTimeBound {
  implicit def fromTimeBound(d: TimeBound) = EncodedTimeBound(d)
  implicit def toTimeBound(e: EncodedTimeBound) = e.deadline

  implicit val CBORCodec: CBOR.Codec[EncodedTimeBound] = {
    def from(l: Long) = if (l == -1) TimeBound.Max else l.millis.bound
    def to(d: EncodedTimeBound) = if (d.deadline == TimeBound.Max) -1 else d.timeLeft.toMillis max 0
    CBOR.AliasCodec(from, to)
  }
}
