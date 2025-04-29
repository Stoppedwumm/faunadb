package fauna.lang

import java.time.temporal.ChronoUnit

/**
  * A midst unit is a time interval - similar to j.u.c.TimeUnit and
  * j.t.t.ChronoUnit - constrained to the needs of temporal math in
  * FQL.
  */
sealed abstract class MidstUnit {

  /**
    * The string provided as input to a query; may be returned in
    * error conditions.
    */
  def original: String

  def toChronoUnit: ChronoUnit

  override def toString = original
}

case class Day(original: String) extends MidstUnit {
  val toChronoUnit = ChronoUnit.DAYS
}

case class HalfDay(original: String) extends MidstUnit {
  val toChronoUnit = ChronoUnit.HALF_DAYS
}

case class Hour(original: String) extends MidstUnit {
  val toChronoUnit = ChronoUnit.HOURS
}

case class Minute(original: String) extends MidstUnit {
  val toChronoUnit = ChronoUnit.MINUTES
}

case class Second(original: String) extends MidstUnit {
  val toChronoUnit = ChronoUnit.SECONDS
}

case class Millisecond(original: String) extends MidstUnit {
  val toChronoUnit = ChronoUnit.MILLIS
}

case class Microsecond(original: String) extends MidstUnit {
  val toChronoUnit = ChronoUnit.MICROS
}

case class Nanosecond(original: String) extends MidstUnit {
  val toChronoUnit = ChronoUnit.NANOS
}

object MidstUnit {
  def unapply(unit: String): Option[MidstUnit] =
    unit.trim match {
      case "day"          => Some(Day(unit))
      case "days"         => Some(Day(unit))
      case "half day"     => Some(HalfDay(unit))
      case "half days"    => Some(HalfDay(unit))
      case "hour"         => Some(Hour(unit))
      case "hours"        => Some(Hour(unit))
      case "minute"       => Some(Minute(unit))
      case "minutes"      => Some(Minute(unit))
      case "second"       => Some(Second(unit))
      case "seconds"      => Some(Second(unit))
      case "millisecond"  => Some(Millisecond(unit))
      case "milliseconds" => Some(Millisecond(unit))
      case "microsecond"  => Some(Microsecond(unit))
      case "microseconds" => Some(Microsecond(unit))
      case "nanosecond"   => Some(Nanosecond(unit))
      case "nanoseconds"  => Some(Nanosecond(unit))
      case _              => None
    }
}
