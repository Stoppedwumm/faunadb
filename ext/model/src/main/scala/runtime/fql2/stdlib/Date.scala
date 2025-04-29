package fauna.model.runtime.fql2.stdlib

import fauna.lang.MidstUnit
import fauna.model.runtime.fql2._
import fauna.repo.query.Query
import fauna.repo.values.Value
import java.time.{ DateTimeException, LocalDate, ZoneOffset }
import java.time.temporal.ChronoUnit

object DatePrototype extends Prototype(TypeTag.Date, isPersistable = true) {
  defField("dayOfMonth" -> tt.Int) { case (_, Value.Date(v)) =>
    Query.value(Value.Int(v.getDayOfMonth))
  }

  defField("dayOfWeek" -> tt.Int) { case (_, Value.Date(v)) =>
    Query.value(Value.Int(v.getDayOfWeek.getValue()))
  }

  defField("dayOfYear" -> tt.Int) { case (_, Value.Date(v)) =>
    Query.value(Value.Int(v.getDayOfYear))
  }

  defField("month" -> tt.Int) { case (_, Value.Date(v)) =>
    Query.value(Value.Int(v.getMonth().getValue()))
  }

  defField("year" -> tt.Int) { case (_, Value.Date(v)) =>
    Query.value(Value.Int(v.getYear()))
  }

  defMethod("add" -> tt.Date)("amount" -> tt.Number, "unit" -> tt.Str) {
    (ctx, self, amount, unit) =>
      (amount, unit.value) match {
        case (Value.Int(value), MidstUnit(unit)) if unit.toChronoUnit.isDateBased =>
          Value.Date(self.value.plus(value, unit.toChronoUnit)).toQuery

        case (Value.Long(value), MidstUnit(unit)) if unit.toChronoUnit.isDateBased =>
          Value.Date(self.value.plus(value, unit.toChronoUnit)).toQuery

        case (Value.Double(value), MidstUnit(unit))
            if unit.toChronoUnit.isDateBased =>
          Value.Date(self.value.plus(math.round(value), unit.toChronoUnit)).toQuery

        case (_, unit) =>
          QueryRuntimeFailure.InvalidDateUnit(unit, ctx.stackTrace).toQuery
      }
  }

  defMethod("subtract" -> tt.Date)("amount" -> tt.Number, "unit" -> tt.Str) {
    (ctx, self, amount, unit) =>
      (amount, unit.value) match {
        case (Value.Int(value), MidstUnit(unit)) if unit.toChronoUnit.isDateBased =>
          Value.Date(self.value.minus(value, unit.toChronoUnit)).toQuery

        case (Value.Long(value), MidstUnit(unit)) if unit.toChronoUnit.isDateBased =>
          Value.Date(self.value.minus(value, unit.toChronoUnit)).toQuery

        case (Value.Double(value), MidstUnit(unit))
            if unit.toChronoUnit.isDateBased =>
          Value.Date(self.value.minus(math.round(value), unit.toChronoUnit)).toQuery

        case (_, unit) =>
          QueryRuntimeFailure.InvalidDateUnit(unit, ctx.stackTrace).toQuery
      }
  }

  // Return the number of days between start and this date, including start
  // but excluding this.
  defMethod("difference" -> tt.Long)("start" -> tt.Date) {
    case (_, Value.Date(end), Value.Date(start)) =>
      Value.Long(ChronoUnit.DAYS.between(start, end)).toQuery
  }
}

object DateCompanion extends CompanionObject("Date") {

  def contains(v: Value): Boolean = v match {
    case _: Value.Date => true
    case _             => false
  }

  private def fromStringImpl(str: Value.Str, stackTrace: FQLInterpreter.StackTrace) =
    try {
      Value.Date(LocalDate.parse(str.value)).toQuery
    } catch {
      case _: DateTimeException =>
        QueryRuntimeFailure.InvalidDate(str.value, stackTrace).toQuery
    }

  defApply(tt.Date)("date" -> tt.Str) { (ctx, str) =>
    fromStringImpl(str, ctx.stackTrace)
  }

  defStaticFunction("fromString" -> tt.Date)("date" -> tt.Str) { (ctx, str) =>
    fromStringImpl(str, ctx.stackTrace)
  }

  defStaticFunction("today" -> tt.Date)() { (_) =>
    Query.snapshotTime flatMap { snapTs =>
      Value.Date(LocalDate.ofInstant(snapTs.toInstant, ZoneOffset.UTC)).toQuery
    }
  }
}
