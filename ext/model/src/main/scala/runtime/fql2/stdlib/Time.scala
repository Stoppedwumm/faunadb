package fauna.model.runtime.fql2.stdlib

import fauna.lang.{ MidstUnit, Timestamp }
import fauna.lang.syntax._
import fauna.model.runtime.fql2._
import fauna.model.runtime.Effect
import fauna.repo.query.Query
import fauna.repo.values.Value
import java.time.{ DateTimeException, ZoneOffset }
import scala.jdk.DurationConverters._
import scala.util.control.NonFatal

object TimePrototype extends Prototype(TypeTag.Time, isPersistable = true) {
  private val MaxTime = Timestamp.parse("+999999999-12-31T23:59:59.999999999Z")
  private val MinTime = Timestamp.parse("-999999999-01-01T00:00:00.000000000Z")

  private def toODT(ts: Timestamp) =
    ts.toInstant.atOffset(ZoneOffset.UTC)

  defField("dayOfMonth" -> tt.Int) { case (_, Value.Time(ts)) =>
    Query.value(Value.Int(toODT(ts).getDayOfMonth))
  }

  defField("dayOfWeek" -> tt.Int) { case (_, Value.Time(ts)) =>
    Query.value(Value.Int(toODT(ts).getDayOfWeek.getValue))
  }

  defField("dayOfYear" -> tt.Int) { case (_, Value.Time(ts)) =>
    Query.value(Value.Int(toODT(ts).getDayOfYear))
  }

  defField("hour" -> tt.Int) { case (_, Value.Time(ts)) =>
    Query.value(Value.Int(toODT(ts).getHour))
  }

  defField("month" -> tt.Int) { case (_, Value.Time(ts)) =>
    Query.value(Value.Int(toODT(ts).getMonth.getValue))
  }

  defField("minute" -> tt.Int) { case (_, Value.Time(ts)) =>
    Query.value(Value.Int(toODT(ts).getMinute))
  }

  defField("second" -> tt.Int) { case (_, Value.Time(ts)) =>
    Query.value(Value.Int(toODT(ts).getSecond))
  }

  defMethod("toMicros" -> tt.Number)() { case (_, Value.Time(ts)) =>
    Value.Number(ts.micros).toQuery
  }

  defMethod("toMillis" -> tt.Number)() { case (_, Value.Time(ts)) =>
    Value.Number(ts.millis).toQuery
  }

  defMethod("toSeconds" -> tt.Number)() { case (_, Value.Time(ts)) =>
    Value.Number(ts.seconds).toQuery
  }

  defField("year" -> tt.Int) { case (_, Value.Time(ts)) =>
    Query.value(Value.Int(toODT(ts).getYear))
  }

  private def checkRange(
    time: Value.Time,
    ctx: FQLInterpCtx): Query[Result[Value.Time]] = {
    if (time.value < MinTime)
      QueryRuntimeFailure.MinimumTimeOverflow(ctx.stackTrace).toQuery
    else if (time.value > MaxTime)
      QueryRuntimeFailure.MaximumTimeOverflow(ctx.stackTrace).toQuery
    else
      time.toQuery
  }

  defMethod("add" -> tt.Time)("amount" -> tt.Number, "unit" -> tt.Str) {
    (ctx, self, amount, unit) =>
      try {
        ((amount, unit.value) match {
          case (Value.Int(value), MidstUnit(unit)) =>
            Value.Time(self.value.plus(value, unit)).toQuery

          case (Value.Long(value), MidstUnit(unit)) =>
            Value.Time(self.value.plus(value, unit)).toQuery

          case (Value.Double(value), MidstUnit(unit)) =>
            Value.Time(self.value.plus(value, unit)).toQuery

          case (_, unit) =>
            QueryRuntimeFailure.InvalidTimeUnit(unit, ctx.stackTrace).toQuery
        }) flatMapT {
          checkRange(_, ctx)
        }
      } catch {
        case NonFatal(_) =>
          QueryRuntimeFailure.MaximumTimeOverflow(ctx.stackTrace).toQuery
      }
  }

  defMethod("subtract" -> tt.Time)("amount" -> tt.Number, "unit" -> tt.Str) {
    (ctx, self, amount, unit) =>
      try {
        ((amount, unit.value) match {
          case (Value.Int(value), MidstUnit(unit)) =>
            Value.Time(self.value.minus(value, unit)).toQuery

          case (Value.Long(value), MidstUnit(unit)) =>
            Value.Time(self.value.minus(value, unit)).toQuery

          case (Value.Double(value), MidstUnit(unit)) =>
            Value.Time(self.value.minus(value, unit)).toQuery

          case (_, unit) =>
            QueryRuntimeFailure.InvalidTimeUnit(unit, ctx.stackTrace).toQuery
        }) flatMapT {
          checkRange(_, ctx)
        }
      } catch {
        case NonFatal(_) =>
          QueryRuntimeFailure.MinimumTimeOverflow(ctx.stackTrace).toQuery
      }
  }

  // Return the number of time units between start and this time, including start
  // but excluding this.
  defMethod("difference" -> tt.Number)("start" -> tt.Time, "unit" -> tt.Str) {
    (ctx, self, other, unit) =>
      val (Value.Time(end), Value.Time(start)) = (self, other)
      unit.value match {
        case MidstUnit(unit) =>
          val diff = unit.toChronoUnit.between(start.toInstant, end.toInstant)
          Value.Number(diff).toQuery
        case _ =>
          QueryRuntimeFailure.InvalidTimeUnit(unit.value, ctx.stackTrace).toQuery
      }
  }
}

object TimeCompanion extends CompanionObject("Time") {

  def contains(v: Value): Boolean = v match {
    case _: Value.Time => true
    case _             => false
  }

  private def fromStringImpl(str: Value.Str, stackTrace: FQLInterpreter.StackTrace) =
    try {
      Value.Time(Timestamp.parse(str.value)).toQuery
    } catch {
      case _: DateTimeException =>
        QueryRuntimeFailure.InvalidTime(str.value, stackTrace).toQuery
    }

  defApply(tt.Time)("time" -> tt.Str) { (ctx, str) =>
    fromStringImpl(str, ctx.stackTrace)
  }

  defStaticFunction("fromString" -> tt.Time)("time" -> tt.Str) { (ctx, str) =>
    fromStringImpl(str, ctx.stackTrace)
  }

  defStaticFunction("now" -> tt.Time)() { (ctx) =>
    Effect.Action.Function("now").check(ctx, Effect.Observation).flatMapT { _ =>
      Query.snapshotTime flatMap { snapTs =>
        Value.Time(snapTs).toQuery
      }
    }
  }

  defStaticFunction("epoch" -> tt.Time)("offset" -> tt.Number, "unit" -> tt.Str) {
    (ctx, amount, unit) =>
      (amount, unit.value) match {
        case (Value.Int(value), MidstUnit(unit)) =>
          Value.Time(Timestamp.Epoch.plus(value, unit)).toQuery

        case (Value.Long(value), MidstUnit(unit)) =>
          Value.Time(Timestamp.Epoch.plus(value, unit)).toQuery

        case (Value.Double(value), MidstUnit(unit)) =>
          val offset = unit.toChronoUnit.getDuration().toScala * value
          Value.Time(Timestamp.Epoch + offset).toQuery

        case (_, unit) =>
          QueryRuntimeFailure.InvalidTimeUnit(unit, ctx.stackTrace).toQuery
      }
  }
}
