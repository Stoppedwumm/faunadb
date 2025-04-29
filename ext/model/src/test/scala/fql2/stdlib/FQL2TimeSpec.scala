package fauna.model.test

import fauna.lang.Timestamp
import fauna.model.runtime.fql2.{ stdlib, FQLInterpreter, QueryRuntimeFailure }
import fauna.model.runtime.Effect
import fauna.repo.values.Value
import fql.ast.{ Span, Src }
import org.scalactic.source.Position

class FQL2TimeSpec extends FQL2StdlibHelperSpec("Time", stdlib.TimePrototype) {
  val auth = newDB

  def testStaticSig(sig: String*)(implicit pos: Position) =
    s"has signature $sig" inWithTest { test =>
      test.signatures = sig
      lookupSig(stdlib.TimeCompanion, test.function) shouldBe sig
    }

  "toString" - {
    testSig("toString() => String")

    "works" in {
      checkOk(
        auth,
        "Time('2023-02-10T12:01:19.000Z').toString()",
        Value.Str("2023-02-10T12:01:19Z"))
    }
  }

  "dayOfWeek" - {
    testSig("Number")

    "works" in {
      checkOk(auth, "Time('2023-02-10T12:01:19.000Z').dayOfWeek", Value.Number(5))
    }
  }

  "dayOfMonth" - {
    testSig("Number")

    "works" in {
      checkOk(auth, "Time('2023-02-10T12:01:19.000Z').dayOfMonth", Value.Number(10))
    }
  }

  "dayOfYear" - {
    testSig("Number")

    "works" in {
      checkOk(auth, "Time('2023-02-10T12:01:19.000Z').dayOfYear", Value.Number(41))
    }
  }

  "second" - {
    testSig("Number")

    "works" in {
      checkOk(auth, "Time('2023-02-10T12:01:19.000Z').second", Value.Number(19))
    }
  }

  "minute" - {
    testSig("Number")

    "works" in {
      checkOk(auth, "Time('2023-02-10T12:01:19.000Z').minute", Value.Number(1))
    }
  }

  "hour" - {
    testSig("Number")

    "works" in {
      checkOk(auth, "Time('2023-02-10T12:01:19.000Z').hour", Value.Number(12))
    }
  }

  "month" - {
    testSig("Number")

    "works" in {
      checkOk(auth, "Time('2023-02-10T12:01:19.000Z').month", Value.Number(2))
    }
  }

  "year" - {
    testSig("Number")

    "works" in {
      checkOk(auth, "Time('2023-02-10T12:01:19.000Z').year", Value.Number(2023))
    }
  }

  "toSeconds" - {
    testSig("toSeconds() => Number")

    "works" in {
      checkOk(
        auth,
        "Time('2023-02-10T12:01:19.000Z').toSeconds()",
        Value.Number(1676030479))
    }
  }

  "toMillis" - {
    testSig("toMillis() => Number")

    "works" in {
      checkOk(
        auth,
        "Time('2023-02-10T12:01:19.000Z').toMillis()",
        Value.Number(1676030479000L))
    }
  }

  "toMicros" - {
    testSig("toMicros() => Number")

    "works" in {
      checkOk(
        auth,
        "Time('2023-02-10T12:01:19.000Z').toMicros()",
        Value.Number(1676030479000000L))
    }
  }

  "add" - {
    testSig("add(amount: Number, unit: String) => Time")

    "works" in {
      checkOk(
        auth,
        "Time('2023-02-10T12:00:00.000Z').add(19, 'minutes')",
        Value.Time(Timestamp.parse("2023-02-10T12:19:00Z")))
    }

    "rounds doubles" in {
      evalOk(
        auth,
        """|let ts = Time("2023-02-10T12:00:00.000Z")
           |[
           |  ts.add(0.5, 'seconds'),
           |  ts.add(0.5, 'minutes'),
           |  ts.add(-0.5, 'seconds'),
           |  ts.subtract(1.5, 'hours'),
           |]
           |""".stripMargin
      ) shouldBe (
        Value.Array(
          Value.Time(Timestamp.parse("2023-02-10T12:00:00.500Z")),
          Value.Time(Timestamp.parse("2023-02-10T12:00:30Z")),
          Value.Time(Timestamp.parse("2023-02-10T11:59:59.500Z")),
          Value.Time(Timestamp.parse("2023-02-10T10:30:00Z"))
        )
      )
    }

    "rejects invalid inputs" in {
      checkErr(
        auth,
        "Time('2023-02-10T12:00:00.000Z').add(1, 'fortnight')",
        QueryRuntimeFailure.InvalidTimeUnit(
          "fortnight",
          FQLInterpreter.StackTrace(Seq(Span(36, 52, Src.Query("")))))
      )
    }

    "handle large units" in {
      checkOk(
        auth,
        "Time('1970-01-01T00:00:00Z').add(1000000000000, 'seconds')",
        Value.Time(Timestamp.parse("+33658-09-27T01:46:40Z")))

      checkOk(
        auth,
        "Time('1970-01-01T00:00:00Z').add(1000000000000.0, 'seconds')",
        Value.Time(Timestamp.parse("+33658-09-27T01:46:40Z")))

      checkOk(
        auth,
        "Time('1970-01-01T00:00:00Z').add(9223372036854775807, 'nanoseconds')",
        Value.Time(Timestamp.parse("2262-04-11T23:47:16.854775807Z")))

      checkOk(
        auth,
        "Time('1970-01-01T00:00:00Z').add(9223372036854775807.0, 'nanoseconds')",
        Value.Time(Timestamp.parse("2262-04-11T23:47:16.854776Z")))

      checkOk(
        auth,
        "Time('-999999999-01-01T00:00:00Z').add(63113903968377595000000000.0, 'nanoseconds')",
        Value.Time(Timestamp.parse("+999999999-12-31T23:59:55Z"))
      )

      checkOk(
        auth,
        "Time('-999999999-01-01T00:00:00Z').add(63113903968377590000000.0, 'microseconds')",
        Value.Time(Timestamp.parse("+999999999-12-31T23:59:50Z"))
      )

      checkOk(
        auth,
        "Time('-999999999-01-01T00:00:00Z').add(63113903968377590000.0, 'milliseconds')",
        Value.Time(Timestamp.parse("+999999999-12-31T23:59:50Z"))
      )

      checkOk(
        auth,
        "Time('-999999999-01-01T00:00:00Z').add(63113903968377592.0, 'seconds')",
        Value.Time(Timestamp.parse("+999999999-12-31T23:59:52Z"))
      )

      checkOk(
        auth,
        "Time('-999999999-01-01T00:00:00Z').add(17531639991215.998, 'hours')",
        Value.Time(Timestamp.parse("+999999999-12-31T23:59:52.968750Z"))
      )
    }

    "handle overflows" in {
      checkErr(
        auth,
        "Time('-999999999-01-01T00:00:00Z').add(9223372036854775807.0, 'hours')",
        QueryRuntimeFailure(
          "invalid_time",
          "Time exceeds the maximum value.",
          FQLInterpreter.StackTrace(Seq(Span(38, 70, Src.Query("")))))
      )

      checkErr(
        auth,
        "Time('+999999999-12-31T23:59:59.999999999Z').add(1, 'nanosecond')",
        QueryRuntimeFailure(
          "invalid_time",
          "Time exceeds the maximum value.",
          FQLInterpreter.StackTrace(Seq(Span(48, 65, Src.Query("")))))
      )

      checkErr(
        auth,
        "Time('+999999999-12-31T23:59:59.999999999Z').add(1.0, 'nanosecond')",
        QueryRuntimeFailure(
          "invalid_time",
          "Time exceeds the maximum value.",
          FQLInterpreter.StackTrace(Seq(Span(48, 67, Src.Query("")))))
      )

      checkErr(
        auth,
        "Time('2023-02-10T12:00:00.000Z').add(9223372036854775807, 'days')",
        QueryRuntimeFailure(
          "invalid_time",
          "Time exceeds the maximum value.",
          FQLInterpreter.StackTrace(Seq(Span(36, 65, Src.Query("")))))
      )

      checkErr(
        auth,
        "Time('2023-02-10T12:00:00.000Z').add(1000000000000000000000000000000.0, 'days')",
        QueryRuntimeFailure(
          "invalid_time",
          "Time exceeds the maximum value.",
          FQLInterpreter.StackTrace(Seq(Span(36, 79, Src.Query("")))))
      )
    }
  }

  "subtract" - {
    testSig("subtract(amount: Number, unit: String) => Time")

    "works" in {
      checkOk(
        auth,
        "Time('2023-02-10T12:00:00.000Z').add(19, 'minutes')",
        Value.Time(Timestamp.parse("2023-02-10T12:19:00Z")))
    }

    "rejects invalid inputs" in {
      checkErr(
        auth,
        "Time('2023-02-10T12:00:00.000Z').subtract(1, 'scores')",
        QueryRuntimeFailure.InvalidTimeUnit(
          "scores",
          FQLInterpreter.StackTrace(Seq(Span(41, 54, Src.Query("")))))
      )
    }

    "handle large units" in {
      checkOk(
        auth,
        "Time('+33658-09-27T01:46:40Z').subtract(1000000000000, 'seconds')",
        Value.Time(Timestamp.parse("1970-01-01T00:00:00Z")))

      checkOk(
        auth,
        "Time('+33658-09-27T01:46:40Z').subtract(1000000000000.0, 'seconds')",
        Value.Time(Timestamp.parse("1970-01-01T00:00:00Z")))

      checkOk(
        auth,
        "Time('2262-04-11T23:47:16.854775807Z').subtract(9223372036854775807, 'nanoseconds')",
        Value.Time(Timestamp.parse("1970-01-01T00:00:00Z")))

      checkOk(
        auth,
        "Time('2262-04-11T23:47:16.854776Z').subtract(9223372036854775807.0, 'nanoseconds')",
        Value.Time(Timestamp.parse("1970-01-01T00:00:00Z")))

      checkOk(
        auth,
        "Time('+999999999-12-31T23:59:59.999999999Z').subtract(63113903968377595000000000.0, 'nanoseconds')",
        Value.Time(Timestamp.parse("-999999999-01-01T00:00:04.999999999Z"))
      )

      checkOk(
        auth,
        "Time('+999999999-12-31T23:59:59.999999999Z').subtract(63113903968377590000000.0, 'microseconds')",
        Value.Time(Timestamp.parse("-999999999-01-01T00:00:09.999999999Z"))
      )

      checkOk(
        auth,
        "Time('+999999999-12-31T23:59:59.999999999Z').subtract(63113903968377590000.0, 'milliseconds')",
        Value.Time(Timestamp.parse("-999999999-01-01T00:00:09.999999999Z"))
      )

      checkOk(
        auth,
        "Time('+999999999-12-31T23:59:59.999999999Z').subtract(63113903968377592.0, 'seconds')",
        Value.Time(Timestamp.parse("-999999999-01-01T00:00:07.999999999Z"))
      )

      checkOk(
        auth,
        "Time('+999999999-12-31T23:59:59.999999999Z').subtract(17531639991215.998, 'hours')",
        Value.Time(Timestamp.parse("-999999999-01-01T00:00:07.031249999Z"))
      )
    }

    "handle overflows" in {
      checkErr(
        auth,
        "Time('+999999999-12-31T23:59:59.999999999Z').subtract(9223372036854775807.0, 'hours')",
        QueryRuntimeFailure(
          "invalid_time",
          "Time exceeds the minimum value.",
          FQLInterpreter.StackTrace(Seq(Span(53, 85, Src.Query("")))))
      )

      checkErr(
        auth,
        "Time('-999999999-01-01T00:00:00.000000000Z').subtract(1, 'nanosecond')",
        QueryRuntimeFailure(
          "invalid_time",
          "Time exceeds the minimum value.",
          FQLInterpreter.StackTrace(Seq(Span(53, 70, Src.Query("")))))
      )

      checkErr(
        auth,
        "Time('-999999999-01-01T00:00:00.000000000Z').subtract(1.0, 'nanosecond')",
        QueryRuntimeFailure(
          "invalid_time",
          "Time exceeds the minimum value.",
          FQLInterpreter.StackTrace(Seq(Span(53, 72, Src.Query("")))))
      )

      checkErr(
        auth,
        "Time('2023-02-10T12:00:00.000Z').subtract(9223372036854775807, 'days')",
        QueryRuntimeFailure(
          "invalid_time",
          "Time exceeds the minimum value.",
          FQLInterpreter.StackTrace(Seq(Span(41, 70, Src.Query("")))))
      )

      checkErr(
        auth,
        "Time('2023-02-10T12:00:00.000Z').subtract(1000000000000000000000000000000.0, 'days')",
        QueryRuntimeFailure(
          "invalid_time",
          "Time exceeds the minimum value.",
          FQLInterpreter.StackTrace(Seq(Span(41, 84, Src.Query("")))))
      )
    }
  }

  "difference" - {
    testSig("difference(start: Time, unit: String) => Number")

    "returns the difference in the given unit" in {
      checkOk(
        auth,
        "Time('2023-02-10T12:00:00.000Z').difference(Time('2023-02-01T12:00:00.000Z'), 'days')",
        Value.Number(9))
      checkOk(
        auth,
        "Time('2023-02-10T12:00:00.000Z').difference(Time('2023-02-10T12:00:19.000Z'), 'seconds')",
        Value.Number(-19))
    }
  }

  "now" - {
    testStaticSig("now() => Time")

    "counts as an observation" in {
      evalOk(auth, "Time.now()", effect = Effect.Read)
      evalOk(auth, "Time.now()", effect = Effect.Observation)
      val err = evalErr(auth, "Time.now()", effect = Effect.Pure)
      err.code shouldEqual "invalid_effect"
      err.errors.head.message shouldEqual "`now` performs an observation, which is not allowed in model tests."
    }
  }

  "fromString" - {
    testStaticSig("fromString(time: String) => Time")

    "works" in {
      checkOk(
        auth,
        "Time.fromString('2023-02-01T12:00:00.000Z')",
        Value.Time(Timestamp.parse("2023-02-01T12:00:00.000Z")))
    }

    "rejects invalid inputs" in {
      checkErr(
        auth,
        "Time.fromString('hey')",
        QueryRuntimeFailure.InvalidTime(
          "hey",
          FQLInterpreter.StackTrace(Seq(Span(15, 22, Src.Query(""))))))
    }
  }

  "epoch" - {
    testStaticSig("epoch(offset: Number, unit: String) => Time")

    "works" in {
      checkOk(
        auth,
        "Time.epoch(1676030400, 'seconds')",
        Value.Time(Timestamp.parse("2023-02-10T12:00:00.000Z")))
    }

    "rejects invalid inputs" in {
      checkErr(
        auth,
        "Time.epoch(1, 'years')",
        QueryRuntimeFailure.InvalidTimeUnit(
          "years",
          FQLInterpreter.StackTrace(Seq(Span(10, 22, Src.Query(""))))))
    }

    "handles doubles" in {
      checkOk(
        auth,
        "Time.epoch(19398.25, 'days')",
        Value.Time(Timestamp.parse("2023-02-10T06:00:00.000Z")))
    }
  }
}
