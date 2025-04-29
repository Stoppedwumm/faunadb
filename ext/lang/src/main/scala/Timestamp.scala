package fauna.lang

import java.time.{ Instant, LocalDate, ZoneOffset }
import java.time.format.DateTimeFormatter.{ ISO_INSTANT, ISO_LOCAL_DATE_TIME }
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoUnit
import scala.concurrent.duration._

object Timestamp {
  private val MicrosPerSecond = 1e6.toLong
  private val MicrosPerNano = 1000
  private val NanosPerSecond = 1e9.toLong

  val Epoch = Timestamp(Instant.EPOCH)
  val Min = Timestamp(Instant.MIN)
  val Max = Timestamp(Instant.MAX)

  // FIXME: Remove once we no longer serialize timestamps as microseconds.
  val MaxMicros = Timestamp.ofMicros(Long.MaxValue)

  def apply(second: Long, nanos: Long): Timestamp =
    Timestamp(Instant.ofEpochSecond(second, nanos))

  def apply(date: LocalDate): Timestamp =
    Timestamp(date.atStartOfDay(ZoneOffset.UTC).toInstant)

  def fromEpoch(offset: Long, unit: MidstUnit) =
    Timestamp(Instant.EPOCH.plus(offset, unit.toChronoUnit))

  def ofSeconds(seconds: Long) =
    Timestamp(Instant.ofEpochSecond(seconds))

  def ofMillis(millis: Long) =
    Timestamp(Instant.ofEpochMilli(millis))

  def ofMicros(micros: Long) =
    Timestamp(micros / MicrosPerSecond, (micros % MicrosPerSecond) * MicrosPerNano)

  def ofNanos(nanos: Long) =
    Timestamp(nanos / NanosPerSecond, nanos % NanosPerSecond)

  private val TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
    .append(ISO_LOCAL_DATE_TIME)
    .optionalStart()
    .appendOffset("+HH:MM:ss", "Z")
    .optionalEnd()
    .optionalStart()
    .appendOffset("+HHMM", "+0000")
    .optionalEnd()
    .toFormatter()

  def parse(str: String) =
    Timestamp(Instant.from(TIMESTAMP_FORMATTER.parse(str)))

  /** Parse a String assuming it has been formatted according to
    * Timestamp.toString.
    */
  def parseInstant(str: String) =
    Timestamp(Instant.from(ISO_INSTANT.parse(str)))
}

final case class Timestamp(toInstant: Instant) extends Ordered[Timestamp] {
  import Timestamp._

  override def toString = toInstant.toString

  def compare(other: Timestamp) = toInstant compareTo other.toInstant

  def seconds = toInstant.getEpochSecond

  def nanoOffset = toInstant.getNano

  def millis = toInstant.toEpochMilli

  def micros =
    (toInstant.getEpochSecond * MicrosPerSecond) + (toInstant.getNano / MicrosPerNano)

  def prevNano = Timestamp(toInstant.minusNanos(1))

  def prevMicro = Timestamp(toInstant.minusNanos(1000))

  def nextMicro = Timestamp(toInstant.plusNanos(1000))

  def -(other: Duration) = if (other.isFinite) {
    Timestamp(toInstant.minus(other.length, asChronoUnit(other.unit)))
  } else {
    Timestamp.Min
  }

  def +(other: Duration) = if (other.isFinite) {
    Timestamp(toInstant.plus(other.length, asChronoUnit(other.unit)))
  } else {
    Timestamp.Max
  }

  def plus(offset: Long, unit: MidstUnit): Timestamp =
    copy(toInstant = toInstant.plus(offset, unit.toChronoUnit))

  def plus(offset: Double, unit: MidstUnit): Timestamp = {
    if (offset < 0) {
      minus(-offset, unit)
    } else {
      val (seconds, nanos) = toSecondsNanos(offset, unit)
      Timestamp(toInstant.plusSeconds(seconds).plusNanos(nanos))
    }
  }

  def minus(offset: Long, unit: MidstUnit): Timestamp =
    copy(toInstant = toInstant.minus(offset, unit.toChronoUnit))

  def minus(offset: Double, unit: MidstUnit): Timestamp = {
    if (offset < 0) {
      plus(-offset, unit)
    } else {
      val (seconds, nanos) = toSecondsNanos(offset, unit)
      Timestamp(toInstant.minusSeconds(seconds).minusNanos(nanos))
    }
  }

  def min(other: Timestamp): Timestamp = if (this < other) this else other

  def max(other: Timestamp): Timestamp = if (this > other) this else other

  def difference(other: Timestamp): FiniteDuration = {
    val i = seconds - other.seconds
    val o = nanoOffset - other.nanoOffset
    ((i * NanosPerSecond) + o).nanos
  }

  private def asChronoUnit(t: TimeUnit) = t match {
    case DAYS         => ChronoUnit.DAYS
    case HOURS        => ChronoUnit.HOURS
    case MINUTES      => ChronoUnit.MINUTES
    case SECONDS      => ChronoUnit.SECONDS
    case MILLISECONDS => ChronoUnit.MILLIS
    case MICROSECONDS => ChronoUnit.MICROS
    case NANOSECONDS  => ChronoUnit.NANOS
  }

  private def toSecondsNanos(offset: Double, unit: MidstUnit) = {
    val duration = unit.toChronoUnit.getDuration
    val rounded = offset.floor
    val fraction = offset - rounded
    val fractionNanos = duration.toNanos * fraction

    val toNanos = BigDecimal(rounded) * duration.toNanos
    val seconds = toNanos / NanosPerSecond
    val nanos = toNanos % NanosPerSecond

    if (seconds >= Long.MaxValue) {
      (Long.MaxValue, 0L)
    } else {
      (seconds.toLong, (nanos + fractionNanos).toLong)
    }
  }
}
