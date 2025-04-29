package fauna.ast

import fauna.lang.{ MidstUnit, Timestamp }
import fauna.model.runtime.Effect
import fauna.repo.query.Query
import java.time.{
  DateTimeException,
  Instant,
  LocalDate,
  OffsetDateTime,
  ZoneOffset
}
import scala.annotation.unused

object TimeFunction extends QFunction {
  val effect = Effect.Pure

  def apply(str: String, ec: EvalContext, pos: Position): Query[R[Literal]] = {
    val time = str match {
      case "now" =>
        Right(TimeL(ec.snapshotTime))
      case _ =>
        try {
          Right(TimeL(Timestamp.parse(str.trim)))
        } catch {
          case _: DateTimeException => Left(List(InvalidTimeArgument(str, pos)))
        }
    }
    Query(time)
  }

}

/** Returns the current snapshot time. It has the same effect as Time("now").
  */
object NowFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    @unused ignore: Unit,
    ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] =
    Query.value(Right(TimeL(ec.snapshotTime)))
}

object EpochFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    num: Long,
    unit: MidstUnit,
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] =
    Query.value(try {
      Right(TimeL(Timestamp.fromEpoch(num, unit)))
    } catch {
      // AE occurs when the offset exceeds representable time by overflowing long
      // DTE occurs when base + offset exceeds Instant.MAX
      case _: ArithmeticException | _: DateTimeException =>
        Left(
          List(
            BoundsError(
              "dates and times",
              s"between ${Instant.MIN} and ${Instant.MAX}",
              pos)))
    })
}

object DateFunction extends QFunction {
  val effect = Effect.Pure

  def apply(str: String, @unused ec: EvalContext, pos: Position): Query[R[Literal]] =
    Query(try {
      Right(DateL(LocalDate.parse(str.trim)))
    } catch {
      case _: DateTimeException =>
        Left(List(InvalidDateArgument(str, pos)))
    })
}

sealed class ConversionFunction(convert: Timestamp => Long) extends QFunction {
  val effect = Effect.Pure

  def apply(
    ts: Timestamp,
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] =
    Query(Right(LongL(convert(ts))))
}

object ToMicrosFunction extends ConversionFunction(_.micros)
object ToMillisFunction extends ConversionFunction(_.millis)
object ToSecondsFunction extends ConversionFunction(_.seconds)

sealed class TimeComponentFunction(component: OffsetDateTime => Long)
    extends QFunction {
  val effect = Effect.Pure

  def apply(
    ts: Timestamp,
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] =
    Query(Right(LongL(component(ts.toInstant.atOffset(ZoneOffset.UTC)))))
}

object SecondFunction extends TimeComponentFunction(_.getSecond)
object MinuteFunction extends TimeComponentFunction(_.getMinute)
object HourFunction extends TimeComponentFunction(_.getHour)
object DayOfMonthFunction extends TimeComponentFunction(_.getDayOfMonth)
object DayOfWeekFunction extends TimeComponentFunction(_.getDayOfWeek.getValue)
object DayOfYearFunction extends TimeComponentFunction(_.getDayOfYear)
object MonthFunction extends TimeComponentFunction(_.getMonthValue)
object YearFunction extends TimeComponentFunction(_.getYear)

sealed abstract class TimeMathFunction extends QFunction {

  def compute(
    base: Either[LocalDate, Timestamp],
    offset: Long,
    unit: MidstUnit): Literal

  val effect = Effect.Pure

  def apply(
    base: Either[LocalDate, Timestamp],
    offset: Long,
    unit: MidstUnit,
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    val result =
      try {
        base match {
          case Right(_) =>
            Right(compute(base, offset, unit))
          case Left(_) if unit.toChronoUnit.isDateBased =>
            Right(compute(base, offset, unit))
          case Left(_) =>
            Left(List(InvalidTimeUnit(unit.toString, pos at "unit")))
        }
      } catch {
        // AE occurs when the offset exceeds representable time by overflowing long
        // DTE occurs when base + offset exceeds Instant.MAX
        case _: ArithmeticException | _: DateTimeException =>
          Left(
            List(
              BoundsError(
                "dates and times",
                s"between ${Instant.MIN} and ${Instant.MAX}",
                pos at "offset")))
      }

    Query.value(result)
  }
}

/** Returns a new Time or Date with the offset in terms of the midst
  * unit added. Dates values may only be offset in terms of Day.
  *
  * The supported units behave as follows:
  *
  * Nanosecond - adds the specified number of nanoseconds.
  * Microsecond - equivalent to Nanosecond with the offset multiplied by 1,000.
  * Millisecond - equivalent to Nanosecond with the amount multiplied by 1,000,000.
  * Second - adds the specified number of seconds.
  * Minute - equivalent to Second with the amount multiplied by 60.
  * Hour - equivalent to Second with the amount multiplied by 3,600.
  * HalfDay - equivalent to Second with the amount multiplied by 43,200 (12 hours).
  * Day - equivalent to Second with the amount multiplied by 86,400 (24 hours).
  */
object TimeAddFunction extends TimeMathFunction {

  def compute(
    base: Either[LocalDate, Timestamp],
    offset: Long,
    unit: MidstUnit): Literal =
    base match {
      case Right(ts) =>
        TimeL(ts.plus(offset, unit))
      case Left(date) =>
        DateL(date.plus(offset, unit.toChronoUnit))
    }
}

/** Returns a new Time or Date with the offset in terms of the midst
  * unit subtracted.
  *
  * The behavior of each unit is the same as TimeAddFunction, with the
  * offset negated.
  */
object TimeSubtractFunction extends TimeMathFunction {

  def compute(
    base: Either[LocalDate, Timestamp],
    offset: Long,
    unit: MidstUnit): Literal =
    base match {
      case Right(ts) =>
        TimeL(ts.minus(offset, unit))
      case Left(date) =>
        DateL(date.minus(offset, unit.toChronoUnit))
    }
}

/** Returns the number of intervals in terms of a midst unit between a
  * starting Time or Date (inclusive) and an ending Time or Date
  * (exclusive).
  */
object TimeDiffFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    start: Either[LocalDate, Timestamp],
    finish: Either[LocalDate, Timestamp],
    unit: MidstUnit,
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    val result = (start, finish) match {
      case (Right(tsA), Right(tsB)) =>
        Right(LongL(unit.toChronoUnit.between(tsA.toInstant, tsB.toInstant)))
      case (Left(ldA), Left(ldB)) if unit.toChronoUnit.isDateBased =>
        Right(LongL(unit.toChronoUnit.between(ldA, ldB)))
      case (Left(_), Left(_)) =>
        Left(List(InvalidTimeUnit(unit.toString, pos at "unit")))
      case (Right(_), Left(_)) | (Left(_), Right(_)) =>
        Left(List(IncompatibleTimeArguments(pos at "time_diff")))
    }

    Query.value(result)
  }

}
