package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.prop.Prop

class TimeFunctionsSpec extends QueryAPI21Spec {
  "time" - {
    prop("accepts iso date times") {
      for {
        db <- aDatabase
        time <- Prop.isoDate
      } {
        qequals(ToString(Time(time)), time, db)
      }
    }

    once("does not accept iso date times without time zone") {
      for {
        db <- aDatabase
      } {
        qassertErr(
          Time("2019-05-27T05:15:48"),
          "invalid argument",
          "Cannot cast '2019-05-27T05:15:48' to a time.",
          JSArray(),
          db)
      }
    }

    once(
      "accepts iso date times with time zone designator without colon separator") {
      for {
        db <- aDatabase
      } {
        qequals(
          ToString(Time("2019-05-27T05:15:48+0000")),
          "2019-05-27T05:15:48Z",
          db)
        qequals(
          ToString(Time("2019-05-27T05:15:48+0100")),
          "2019-05-27T04:15:48Z",
          db)
        qequals(
          ToString(Time("2019-05-27T05:15:48+0200")),
          "2019-05-27T03:15:48Z",
          db)
        qequals(
          ToString(Time("2019-05-27T05:15:48+0300")),
          "2019-05-27T02:15:48Z",
          db)
      }
    }

    once("accepts offset date times") {
      for {
        db <- aDatabase
      } {
        qequals(Time("1970-01-01T00:00:00Z"), Epoch(0, "second"), db)
        qequals(Time("1970-01-01T00:00:00+00:00"), Epoch(0, "second"), db)
        qequals(Time("2038-01-19T03:14:08+00:00"), Epoch(2147483648000000L, "microsecond"), db)
        qequals(Time("1970-01-01T00:00:01+00:00"), Epoch(1, "second"),  db)
        qequals(Time("1970-01-01T00:00:00.001+00:00"), Epoch(1, "millisecond"), db)
        qequals(Time("1970-01-01T00:00:00.000001+00:00"), Epoch(1, "microsecond"), db)
        qequals(Time("1970-01-01T00:00:00.000000001+00:00"), Epoch(1, "nanosecond"), db)
        qassertErr(Time(Time("1970-01-01T00:00:00+00:00")), "invalid argument", JSArray("time"), db)
      }
    }

    once("now is stable") {
      for {
        db <- aDatabase
      } {
        qassert(Equals(Time("now"), Time("now")), db)
        qassert(Equals(Now(), Now()), db)
        qassert(Equals(Time("now"), Now()), db)
      }
    }
  }

  "now" - {
    once("works") {
      for {
        db <- aDatabase
      } {
        val t0 = runQuery(Now(), db)
        val t1 = runQuery(Now(), db)

        qequals(LessThanOrEquals(t0, t1, Now()), JSTrue, db)
      }
    }
  }

  "epoch" - {
    once("works") {
      for {
        db <- aDatabase
      } {
        qassertErr(Epoch(Long.MaxValue, "day"), "invalid argument", JSArray.empty, db)
        qassertErr(Epoch(0, "picosecond"), "invalid argument", JSArray("unit"), db)
      }
    }

    once("days") {
      for {
        db <- aDatabase
      } {
        qequals(Epoch(-1, "day"), Time("1969-12-31T00:00:00+00:00"), db)
        qequals(Epoch(0, "day"), Time("1970-01-01T00:00:00+00:00"), db)
        qequals(Epoch(1, "day"), Time("1970-01-02T00:00:00+00:00"), db)

        qequals(Epoch(0, "days"), Time("1970-01-01T00:00:00+00:00"), db)
      }
    }

    once("half days") {
      for {
        db <- aDatabase
      } {
        qequals(Epoch(-1, "half day"), Time("1969-12-31T12:00:00+00:00"), db)
        qequals(Epoch(0, "half day"), Time("1970-01-01T00:00:00+00:00"), db)
        qequals(Epoch(1, "half day"), Time("1970-01-01T12:00:00+00:00"), db)

        qequals(Epoch(0, "half days"), Time("1970-01-01T00:00:00+00:00"), db)
      }
    }

    once("hours") {
      for {
        db <- aDatabase
      } {
        qequals(Epoch(-1, "hour"), Time("1969-12-31T23:00:00+00:00"), db)
        qequals(Epoch(0, "hour"), Time("1970-01-01T00:00:00+00:00"), db)
        qequals(Epoch(1, "hour"), Time("1970-01-01T01:00:00+00:00"), db)

        qequals(Epoch(0, "hours"), Time("1970-01-01T00:00:00+00:00"), db)
      }
    }

    once("minutes") {
      for {
        db <- aDatabase
      } {
        qequals(Epoch(-1, "minute"), Time("1969-12-31T23:59:00+00:00"), db)
        qequals(Epoch(0, "minute"), Time("1970-01-01T00:00:00+00:00"), db)
        qequals(Epoch(1, "minute"), Time("1970-01-01T00:01:00+00:00"), db)

        qequals(Epoch(0, "minutes"), Time("1970-01-01T00:00:00+00:00"), db)
      }
    }

    once("seconds") {
      for {
        db <- aDatabase
      } {
        qequals(Epoch(-1, "second"), Time("1969-12-31T23:59:59+00:00"), db)
        qequals(Epoch(0, "second"), Time("1970-01-01T00:00:00+00:00"), db)
        qequals(Epoch(1, "second"), Time("1970-01-01T00:00:01+00:00"), db)

        qequals(Epoch(0, "seconds"), Time("1970-01-01T00:00:00+00:00"), db)
      }
    }

    once("milliseconds") {
      for {
        db <- aDatabase
      } {
        qequals(Epoch(-1, "millisecond"), Time("1969-12-31T23:59:59.999+00:00"), db)
        qequals(Epoch(0, "millisecond"), Time("1970-01-01T00:00:00+00:00"), db)
        qequals(Epoch(1, "millisecond"), Time("1970-01-01T00:00:00.001+00:00"), db)

        qequals(Epoch(0, "milliseconds"), Time("1970-01-01T00:00:00+00:00"), db)
      }
    }

    once("microseconds") {
      for {
        db <- aDatabase
      } {
        qequals(Epoch(-1, "microsecond"), Time("1969-12-31T23:59:59.999999+00:00"), db)
        qequals(Epoch(0, "microsecond"), Time("1970-01-01T00:00:00+00:00"), db)
        qequals(Epoch(1, "microsecond"), Time("1970-01-01T00:00:00.000001+00:00"), db)

        qequals(Epoch(0, "microseconds"), Time("1970-01-01T00:00:00+00:00"), db)
      }
    }

    once("nanoseconds") {
      for {
        db <- aDatabase
      } {
        qequals(Epoch(-1, "nanosecond"), Time("1969-12-31T23:59:59.999999999+00:00"), db)
        qequals(Epoch(0, "nanosecond"), Time("1970-01-01T00:00:00+00:00"), db)
        qequals(Epoch(1, "nanosecond"), Time("1970-01-01T00:00:00.000000001+00:00"), db)

        qequals(Epoch(0, "nanoseconds"), Time("1970-01-01T00:00:00+00:00"), db)
      }
    }
  }

  "date" - {
    once("accepts date strings") {
      for {
        db <- aDatabase
      } {
        qequals(Date("1970-01-01"), DateF("1970-01-01"), db)
        qassertErr(DateF("19700101"), "invalid argument", JSArray(), db)
        qassertErr(DateF(Date("1970-01-01")), "invalid argument", JSArray("date"), db)
      }
    }
  }

  "to_micros" - {
    once("converts time") {
      for {
        db <- aDatabase
      } {
        qequals(ToMicros(Epoch(0, "second")), 0, db)
        qequals(ToMicros(Epoch(2147483648000000L, "microsecond")), 2147483648000000L, db)
        qequals(ToMicros(0), 0, db)
        qequals(ToMicros(2147483648000000L), 2147483648000000L, db)
        qassertErr(ToMicros("one"), "invalid argument", JSArray("to_micros"), db)
      }
    }
  }

  "to_millis" - {
    once("converts time") {
      for {
        db <- aDatabase
      } {
        qequals(ToMillis(Epoch(0, "second")), 0, db)
        qequals(ToMillis(Epoch(2147483648000L, "millisecond")), 2147483648000L, db)
        qequals(ToMillis(0), 0, db)
        qequals(ToMillis(2147483648000000L), 2147483648000L, db)
        qassertErr(ToMillis("one"), "invalid argument", JSArray("to_millis"), db)
      }
    }
  }

  "to_seconds" - {
    once("converts time") {
      for {
        db <- aDatabase
      } {
        qequals(ToSeconds(Epoch(0, "second")), 0, db)
        qequals(ToSeconds(Epoch(2147483648L, "second")), 2147483648L, db)
        qequals(ToSeconds(0), 0, db)
        qequals(ToSeconds(2147483648000000L), 2147483648L, db)
        qassertErr(ToSeconds("one"), "invalid argument", JSArray("to_seconds"), db)
      }
    }
  }

  "second" - {
    once("converts time") {
      for {
        db <- aDatabase
      } {
        qequals(Second(Epoch(0, "second")), 0, db)
        qequals(Second(Epoch(2147483648L, "second")), 8, db)
        qequals(Second(0), 0, db)
        qequals(Second(2147483648000000L), 8, db)
        qassertErr(Second("one"), "invalid argument", JSArray("second"), db)
      }
    }
  }

  "minute" - {
    once("converts time") {
      for {
        db <- aDatabase
      } {
        qequals(Minute(Epoch(0, "second")), 0, db)
        qequals(Minute(Epoch(2147483648L, "second")), 14, db)
        qequals(Minute(0), 0, db)
        qequals(Minute(2147483648000000L), 14, db)
        qassertErr(Minute("one"), "invalid argument", JSArray("minute"), db)
      }
    }
  }

  "hour" - {
    once("converts time") {
      for {
        db <- aDatabase
      } {
        qequals(Hour(Epoch(0, "second")), 0, db)
        qequals(Hour(Epoch(2147483648L, "second")), 3, db)
        qequals(Hour(0), 0, db)
        qequals(Hour(2147483648000000L), 3, db)
        qassertErr(Hour("one"), "invalid argument", JSArray("hour"), db)
      }
    }
  }

  "day_of_month" - {
    once("converts time") {
      for {
        db <- aDatabase
      } {
        qequals(DayOfMonth(Epoch(0, "second")), 1, db)
        qequals(DayOfMonth(Epoch(2147483648L, "second")), 19, db)
        qequals(DayOfMonth(0), 1, db)
        qequals(DayOfMonth(2147483648000000L), 19, db)
        qassertErr(DayOfMonth("one"), "invalid argument", JSArray("day_of_month"), db)
      }
    }
  }

  "day_of_week" - {
    once("converts time") {
      for {
        db <- aDatabase
      } {
        qequals(DayOfWeek(Epoch(0, "second")), 4, db)
        qequals(DayOfWeek(Epoch(2147483648L, "second")), 2, db)
        qequals(DayOfWeek(0), 4, db)
        qequals(DayOfWeek(2147483648000000L), 2, db)
        qassertErr(DayOfWeek("one"), "invalid argument", JSArray("day_of_week"), db)
      }
    }
  }

  "day_of_year" - {
    once("converts time") {
      for {
        db <- aDatabase
      } {
        qequals(DayOfYear(Epoch(0, "second")), 1, db)
        qequals(DayOfYear(Epoch(2147483648L, "second")), 19, db)
        qequals(DayOfYear(0), 1, db)
        qequals(DayOfYear(2147483648000000L), 19, db)
        qassertErr(DayOfYear("one"), "invalid argument", JSArray("day_of_year"), db)
      }
    }
  }

  "month" - {
    once("converts time") {
      for {
        db <- aDatabase
      } {
        qequals(Month(Epoch(0, "second")), 1, db)
        qequals(Month(Epoch(2147483648L, "second")), 1, db)
        qequals(Month(0), 1, db)
        qequals(Month(2147483648000000L), 1, db)
        qassertErr(Month("one"), "invalid argument", JSArray("month"), db)
      }
    }
  }

  "year" - {
    once("converts time") {
      for {
        db <- aDatabase
      } {
        qequals(Year(Epoch(0, "second")), 1970, db)
        qequals(Year(Epoch(2147483648L, "second")), 2038, db)
        qequals(Year(0), 1970, db)
        qequals(Year(2147483648000000L), 2038, db)
        qassertErr(Year("one"), "invalid argument", JSArray("year"), db)
      }
    }
  }

  "time_add" - {
    once("adds days") {
      for {
        db <- aDatabase
      } {
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), -1, "day"),
          Time("1969-12-31T00:00:00Z"), db)
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), 0, "day"),
          Time("1970-01-01T00:00:00Z"), db)
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), 1, "day"),
          Time("1970-01-02T00:00:00Z"), db)

        qassertErr(TimeAdd(Time("1970-01-01T00:00:00Z"), Long.MinValue, "day"), "invalid argument", JSArray("offset"), db)
        qassertErr(TimeAdd(Time("1970-01-01T00:00:00Z"), Long.MaxValue / 86400, "day"), "invalid argument", JSArray("offset"), db)
      }
    }

    once("adds half days") {
      for {
        db <- aDatabase
      } {
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), -1, "half day"),
          Time("1969-12-31T12:00:00Z"), db)
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), 0, "half day"),
          Time("1970-01-01T00:00:00Z"), db)
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), 1, "half day"),
          Time("1970-01-01T12:00:00Z"), db)

        qassertErr(TimeAdd(Time("1970-01-01T00:00:00Z"), Long.MinValue, "half day"), "invalid argument", JSArray("offset"), db)
        qassertErr(TimeAdd(Time("1970-01-01T00:00:00Z"), Long.MaxValue / 43200, "half day"), "invalid argument", JSArray("offset"), db)
      }
    }

    once("adds hours") {
      for {
        db <- aDatabase
      } {
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), -1, "hour"),
          Time("1969-12-31T23:00:00Z"), db)
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), 0, "hour"),
          Time("1970-01-01T00:00:00Z"), db)
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), 1, "hour"),
          Time("1970-01-01T01:00:00Z"), db)

        qassertErr(TimeAdd(Time("1970-01-01T00:00:00Z"), Long.MinValue, "hour"), "invalid argument", JSArray("offset"), db)
        qassertErr(TimeAdd(Time("1970-01-01T00:00:00Z"), Long.MaxValue / 3600, "hour"), "invalid argument", JSArray("offset"), db)
      }
    }

    once("adds minutes") {
      for {
        db <- aDatabase
      } {
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), -1, "minute"),
          Time("1969-12-31T23:59:00Z"), db)
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), 0, "minute"),
          Time("1970-01-01T00:00:00Z"), db)
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), 1, "minute"),
          Time("1970-01-01T00:01:00Z"), db)

        qassertErr(TimeAdd(Time("1970-01-01T00:00:00Z"), Long.MinValue, "minute"), "invalid argument", JSArray("offset"), db)
        qassertErr(TimeAdd(Time("1970-01-01T00:00:00Z"), Long.MaxValue / 60, "minute"), "invalid argument", JSArray("offset"), db)
      }
    }

    once("adds seconds") {
      for {
        db <- aDatabase
      } {
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), -1, "second"),
          Time("1969-12-31T23:59:59Z"), db)
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), 0, "second"),
          Time("1970-01-01T00:00:00Z"), db)
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), 1, "second"),
          Time("1970-01-01T00:00:01Z"), db)

        qassertErr(TimeAdd(Time("1970-01-01T00:00:00Z"), Long.MinValue, "second"), "invalid argument", JSArray("offset"), db)
        qassertErr(TimeAdd(Time("1970-01-01T00:00:00Z"), Long.MaxValue, "second"), "invalid argument", JSArray("offset"), db)
      }
    }

    once("adds milliseconds") {
      for {
        db <- aDatabase
      } {
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), -1, "millisecond"),
          Time("1969-12-31T23:59:59.999Z"), db)
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), 0, "millisecond"),
          Time("1970-01-01T00:00:00Z"), db)
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), 1, "millisecond"),
          Time("1970-01-01T00:00:00.001Z"), db)
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), Long.MinValue, "millisecond"),
          Time("-292275055-05-16T16:47:04.192Z"), db)
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), Long.MaxValue, "millisecond"),
          Time("+292278994-08-17T07:12:55.807Z"), db)
      }
    }

    once("adds microseconds") {
      for {
        db <- aDatabase
      } {
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), -1, "microsecond"),
          Time("1969-12-31T23:59:59.999999Z"), db)
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), 0, "microsecond"),
          Time("1970-01-01T00:00:00Z"), db)
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), 1, "microsecond"),
          Time("1970-01-01T00:00:00.000001Z"), db)
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), Long.MinValue, "microsecond"),
          Time("-290308-12-21T19:59:05.224192Z"), db)
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), Long.MaxValue, "microsecond"),
          Time("+294247-01-10T04:00:54.775807Z"), db)
      }
    }

    once("adds nanoseconds") {
      for {
        db <- aDatabase
      } {
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), -1, "nanosecond"),
          Time("1969-12-31T23:59:59.999999999Z"), db)
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), 0, "nanosecond"),
          Time("1970-01-01T00:00:00Z"), db)
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), 1, "nanosecond"),
          Time("1970-01-01T00:00:00.000000001Z"), db)
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), Long.MinValue, "nanosecond"),
          Time("1677-09-21T00:12:43.145224192Z"), db)
        qequals(TimeAdd(Time("1970-01-01T00:00:00Z"), Long.MaxValue, "nanosecond"),
          Time("2262-04-11T23:47:16.854775807Z"), db)
      }
    }
  }

  "time_subtract" - {
    once("subtracts days") {
      for {
        db <- aDatabase
      } {
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), 1, "day"),
          Time("1969-12-31T00:00:00Z"), db)
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), 0, "day"),
          Time("1970-01-01T00:00:00Z"), db)
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), -1, "day"),
          Time("1970-01-02T00:00:00Z"), db)

        qassertErr(TimeSubtract(Time("1970-01-01T00:00:00Z"), Long.MinValue, "day"), "invalid argument", JSArray("offset"), db)
        qassertErr(TimeSubtract(Time("1970-01-01T00:00:00Z"), Long.MaxValue / 86400, "day"), "invalid argument", JSArray("offset"), db)
      }
    }

    once("subtracts half days") {
      for {
        db <- aDatabase
      } {
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), 1, "half day"),
          Time("1969-12-31T12:00:00Z"), db)
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), 0, "half day"),
          Time("1970-01-01T00:00:00Z"), db)
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), -1, "half day"),
          Time("1970-01-01T12:00:00Z"), db)

        qassertErr(TimeSubtract(Time("1970-01-01T00:00:00Z"), Long.MinValue, "half day"), "invalid argument", JSArray("offset"), db)
        qassertErr(TimeSubtract(Time("1970-01-01T00:00:00Z"), Long.MaxValue / 43200, "half day"), "invalid argument", JSArray("offset"), db)
      }
    }

    once("subtracts hours") {
      for {
        db <- aDatabase
      } {
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), 1, "hour"),
          Time("1969-12-31T23:00:00Z"), db)
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), 0, "hour"),
          Time("1970-01-01T00:00:00Z"), db)
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), -1, "hour"),
          Time("1970-01-01T01:00:00Z"), db)

        qassertErr(TimeSubtract(Time("1970-01-01T00:00:00Z"), Long.MinValue, "hour"), "invalid argument", JSArray("offset"), db)
        qassertErr(TimeSubtract(Time("1970-01-01T00:00:00Z"), Long.MaxValue / 3600, "hour"), "invalid argument", JSArray("offset"), db)
      }
    }

    once("subtracts minutes") {
      for {
        db <- aDatabase
      } {
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), 1, "minute"),
          Time("1969-12-31T23:59:00Z"), db)
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), 0, "minute"),
          Time("1970-01-01T00:00:00Z"), db)
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), -1, "minute"),
          Time("1970-01-01T00:01:00Z"), db)

        qassertErr(TimeSubtract(Time("1970-01-01T00:00:00Z"), Long.MinValue, "minute"), "invalid argument", JSArray("offset"), db)
        qassertErr(TimeSubtract(Time("1970-01-01T00:00:00Z"), Long.MaxValue / 60, "minute"), "invalid argument", JSArray("offset"), db)
      }
    }

    once("subtracts seconds") {
      for {
        db <- aDatabase
      } {
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), 1, "second"),
          Time("1969-12-31T23:59:59Z"), db)
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), 0, "second"),
          Time("1970-01-01T00:00:00Z"), db)
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), -1, "second"),
          Time("1970-01-01T00:00:01Z"), db)

        qassertErr(TimeSubtract(Time("1970-01-01T00:00:00Z"), Long.MinValue, "second"), "invalid argument", JSArray("offset"), db)
        qassertErr(TimeSubtract(Time("1970-01-01T00:00:00Z"), Long.MaxValue, "second"), "invalid argument", JSArray("offset"), db)
      }
    }

    once("subtracts milliseconds") {
      for {
        db <- aDatabase
      } {
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), 1, "millisecond"),
          Time("1969-12-31T23:59:59.999Z"), db)
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), 0, "millisecond"),
          Time("1970-01-01T00:00:00Z"), db)
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), -1, "millisecond"),
          Time("1970-01-01T00:00:00.001Z"), db)
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), Long.MaxValue, "millisecond"),
          Time("-292275055-05-16T16:47:04.193Z"), db)
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), Long.MinValue, "millisecond"),
          Time("+292278994-08-17T07:12:55.808Z"), db)
      }
    }

    once("subtracts microseconds") {
      for {
        db <- aDatabase
      } {
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), 1, "microsecond"),
          Time("1969-12-31T23:59:59.999999Z"), db)
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), 0, "microsecond"),
          Time("1970-01-01T00:00:00Z"), db)
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), -1, "microsecond"),
          Time("1970-01-01T00:00:00.000001Z"), db)
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), Long.MaxValue, "microsecond"),
          Time("-290308-12-21T19:59:05.224193Z"), db)
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), Long.MinValue, "microsecond"),
          Time("+294247-01-10T04:00:54.775808Z"), db)
      }
    }

    once("subtracts nanoseconds") {
      for {
        db <- aDatabase
      } {
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), 1, "nanosecond"),
          Time("1969-12-31T23:59:59.999999999Z"), db)
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), 0, "nanosecond"),
          Time("1970-01-01T00:00:00Z"), db)
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), -1, "nanosecond"),
          Time("1970-01-01T00:00:00.000000001Z"), db)
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), Long.MaxValue, "nanosecond"),
          Time("1677-09-21T00:12:43.145224193Z"), db)
        qequals(TimeSubtract(Time("1970-01-01T00:00:00Z"), Long.MinValue, "nanosecond"),
          Time("2262-04-11T23:47:16.854775808Z"), db)
      }
    }
  }

  "time_diff" - {
    once("works") {
      for {
        db <- aDatabase
      } {
        qassertErr(TimeDiff(Date("1970-01-01"), Time("1970-01-01T00:00:00Z"), "hours"), "invalid argument", "Incompatible time arguments.", JSArray("time_diff"), db)
        qassertErr(TimeDiff(Time("1970-01-01T00:00:00Z"), Date("1970-01-01"), "hours"), "invalid argument", "Incompatible time arguments.", JSArray("time_diff"), db)
        qassertErr(TimeDiff(Date("1970-01-01"), Date("1970-01-01"), "jiffies"), "invalid argument", "Time Unit expected, String provided.", JSArray("unit"), db)
      }
    }

    once("days") {
      for {
        db <- aDatabase
      } {
        qequals(TimeDiff(Date("1970-01-01"), Date("1970-01-02"), "days"), 1, db)
        qequals(TimeDiff(Date("1970-01-01"), Date("1970-01-01"), "days"), 0, db)
        qequals(TimeDiff(Date("1970-01-02"), Date("1970-01-01"), "days"), -1, db)

        qequals(TimeDiff(Time("1970-01-01T00:00:00Z"), Time("1970-01-02T00:00:00Z"), "days"), 1, db)
        qequals(TimeDiff(Time("1970-01-01T00:00:00Z"), Time("1970-01-01T00:00:00Z"), "days"), 0, db)
        qequals(TimeDiff(Time("1970-01-02T00:00:00Z"), Time("1970-01-01T00:00:00Z"), "days"), -1, db)
      }
    }

    once("half days") {
      for {
        db <- aDatabase
      } {
        qequals(TimeDiff(Time("1970-01-01T00:00:00Z"), Time("1970-01-01T12:00:00Z"), "half days"), 1, db)
        qequals(TimeDiff(Time("1970-01-01T00:00:00Z"), Time("1970-01-01T00:00:00Z"), "half days"), 0, db)
        qequals(TimeDiff(Time("1970-01-01T12:00:00Z"), Time("1970-01-01T00:00:00Z"), "half days"), -1, db)

        qassertErr(TimeDiff(Date("1970-01-01"), Date("1970-01-01"), "half days"), "invalid argument", "Invalid time unit 'half days'.", JSArray("unit"), db)
      }
    }

    once("hours") {
      for {
        db <- aDatabase
      } {
        qequals(TimeDiff(Time("1970-01-01T00:00:00Z"), Time("1970-01-01T01:00:00Z"), "hours"), 1, db)
        qequals(TimeDiff(Time("1970-01-01T00:00:00Z"), Time("1970-01-01T00:00:00Z"), "hours"), 0, db)
        qequals(TimeDiff(Time("1970-01-01T01:00:00Z"), Time("1970-01-01T00:00:00Z"), "hours"), -1, db)

        qassertErr(TimeDiff(Date("1970-01-01"), Date("1970-01-01"), "hours"), "invalid argument", "Invalid time unit 'hours'.", JSArray("unit"), db)
      }
    }

    once("minutes") {
      for {
        db <- aDatabase
      } {
        qequals(TimeDiff(Time("1970-01-01T00:00:00Z"), Time("1970-01-01T00:01:00Z"), "minutes"), 1, db)
        qequals(TimeDiff(Time("1970-01-01T00:00:00Z"), Time("1970-01-01T00:00:00Z"), "minutes"), 0, db)
        qequals(TimeDiff(Time("1970-01-01T00:01:00Z"), Time("1970-01-01T00:00:00Z"), "minutes"), -1, db)

        qassertErr(TimeDiff(Date("1970-01-01"), Date("1970-01-01"), "minutes"), "invalid argument", "Invalid time unit 'minutes'.", JSArray("unit"), db)
      }
    }

    once("seconds") {
      for {
        db <- aDatabase
      } {
        qequals(TimeDiff(Time("1970-01-01T00:00:00Z"), Time("1970-01-01T00:00:01Z"), "seconds"), 1, db)
        qequals(TimeDiff(Time("1970-01-01T00:00:00Z"), Time("1970-01-01T00:00:00Z"), "seconds"), 0, db)
        qequals(TimeDiff(Time("1970-01-01T00:00:01Z"), Time("1970-01-01T00:00:00Z"), "seconds"), -1, db)

        qassertErr(TimeDiff(Date("1970-01-01"), Date("1970-01-01"), "seconds"), "invalid argument", "Invalid time unit 'seconds'.", JSArray("unit"), db)
      }
    }

    once("milliseconds") {
      for {
        db <- aDatabase
      } {
        qequals(TimeDiff(Time("1970-01-01T00:00:00Z"), Time("1970-01-01T00:00:00.001Z"), "milliseconds"), 1, db)
        qequals(TimeDiff(Time("1970-01-01T00:00:00Z"), Time("1970-01-01T00:00:00Z"), "milliseconds"), 0, db)
        qequals(TimeDiff(Time("1970-01-01T00:00:00.001Z"), Time("1970-01-01T00:00:00Z"), "milliseconds"), -1, db)

        qassertErr(TimeDiff(Date("1970-01-01"), Date("1970-01-01"), "milliseconds"), "invalid argument", "Invalid time unit 'milliseconds'.", JSArray("unit"), db)
      }
    }

    once("microseconds") {
      for {
        db <- aDatabase
      } {
        qequals(TimeDiff(Time("1970-01-01T00:00:00Z"), Time("1970-01-01T00:00:00.000001Z"), "microseconds"), 1, db)
        qequals(TimeDiff(Time("1970-01-01T00:00:00Z"), Time("1970-01-01T00:00:00Z"), "microseconds"), 0, db)
        qequals(TimeDiff(Time("1970-01-01T00:00:00.000001Z"), Time("1970-01-01T00:00:00Z"), "microseconds"), -1, db)

        qassertErr(TimeDiff(Date("1970-01-01"), Date("1970-01-01"), "microseconds"), "invalid argument", "Invalid time unit 'microseconds'.", JSArray("unit"), db)
      }
    }

    once("nanoseconds") {
      for {
        db <- aDatabase
      } {
        qequals(TimeDiff(Time("1970-01-01T00:00:00Z"), Time("1970-01-01T00:00:00.000000001Z"), "nanoseconds"), 1, db)
        qequals(TimeDiff(Time("1970-01-01T00:00:00Z"), Time("1970-01-01T00:00:00Z"), "nanoseconds"), 0, db)
        qequals(TimeDiff(Time("1970-01-01T00:00:00.000000001Z"), Time("1970-01-01T00:00:00Z"), "nanoseconds"), -1, db)

        qassertErr(TimeDiff(Date("1970-01-01"), Date("1970-01-01"), "nanoseconds"), "invalid argument", "Invalid time unit 'nanoseconds'.", JSArray("unit"), db)
      }
    }
  }
}
