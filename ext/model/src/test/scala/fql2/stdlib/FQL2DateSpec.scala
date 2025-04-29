package fauna.model.test

import fauna.model.runtime.fql2.{ stdlib, FQLInterpreter, QueryRuntimeFailure }
import fauna.repo.values.Value
import fql.ast.{ Span, Src }
import java.time.LocalDate

class FQL2DateSpec extends FQL2StdlibHelperSpec("Date", stdlib.DatePrototype) {
  val auth = newDB

  "toString" - {
    testSig("toString() => String")

    "works" in {
      checkOk(auth, "Date('2023-03-02').toString()", Value.Str("2023-03-02"))
    }
  }

  "dayOfWeek" - {
    testSig("Number")

    "works" in {
      checkOk(auth, "Date('2023-02-10').dayOfWeek", Value.Number(5))
    }
  }

  "dayOfMonth" - {
    testSig("Number")

    "works" in {
      checkOk(auth, "Date('2023-02-10').dayOfMonth", Value.Number(10))
    }
  }

  "dayOfYear" - {
    testSig("Number")

    "works" in {
      checkOk(auth, "Date('2023-02-10').dayOfYear", Value.Number(41))
    }
  }

  "month" - {
    testSig("Number")

    "works" in {
      checkOk(auth, "Date('2023-02-10').month", Value.Number(2))
    }
  }

  "year" - {
    testSig("Number")

    "works" in {
      checkOk(auth, "Date('2023-02-10').year", Value.Number(2023))
    }
  }

  "add" - {
    testSig("add(amount: Number, unit: String) => Date")

    "works" in {
      checkOk(
        auth,
        "Date('2023-02-10').add(19, 'days')",
        Value.Date(LocalDate.parse("2023-03-01")))
    }

    "rejects invalidate inputs" in {
      checkErr(
        auth,
        "Date('2023-02-10').add(1, 'fortnight')",
        QueryRuntimeFailure.InvalidDateUnit(
          "fortnight",
          FQLInterpreter.StackTrace(Seq(Span(22, 38, Src.Query("")))))
      )
    }

    "rounds doubles" in {
      checkOk(
        auth,
        """|let dt = Date("2023-02-20")
            |[
            |  dt.add(0.5, 'days'),
            |  dt.add(-0.5, 'days'),
            |  dt.add(1.5, 'days'),
            |  dt.add(-1.5, 'days')
            |]
            |""".stripMargin,
        Value.Array(
          Value.Date(LocalDate.parse("2023-02-21")),
          Value.Date(LocalDate.parse("2023-02-20")),
          Value.Date(LocalDate.parse("2023-02-22")),
          Value.Date(LocalDate.parse("2023-02-19"))
        )
      )
    }
  }

  "subtract" - {
    testSig("subtract(amount: Number, unit: String) => Date")

    "works" in {
      checkOk(
        auth,
        "Date('2023-02-10').subtract(41, 'days')",
        Value.Date(LocalDate.parse("2022-12-31")))
    }
    "rejects invalid inputs" in {
      checkErr(
        auth,
        "Date('2023-02-10').subtract(1, 'minutes')",
        QueryRuntimeFailure.InvalidDateUnit(
          "minutes",
          FQLInterpreter.StackTrace(Seq(Span(27, 41, Src.Query("")))))
      )
    }

    "rounds doubles" in {
      checkOk(
        auth,
        """|let dt = Date("2023-02-20")
            |[
            |  dt.subtract(0.5, 'days'),
            |  dt.subtract(1.5, 'days'),
            |]
            |""".stripMargin,
        Value.Array(
          Value.Date(LocalDate.parse("2023-02-19")),
          Value.Date(LocalDate.parse("2023-02-18")))
      )
    }
  }

  "difference" - {
    testSig("difference(start: Date) => Number")

    "returns the difference in days between dates" in {
      checkOk(
        auth,
        "Date('2023-02-10').difference(Date('2023-01-01'))",
        Value.Number(40))
      checkOk(
        auth,
        "Date('2023-02-10').difference(Date('2023-12-31'))",
        Value.Number(-324))
    }
  }
}
