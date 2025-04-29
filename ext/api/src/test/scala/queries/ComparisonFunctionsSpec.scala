package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.prop.api.Database

class ComparisonFunctionsSpec extends QueryAPI3Spec {
  "comparisons with a single operand" - {
    once("are always true") {
      for {
        db <- aDatabase
      } {
        testAllOperatorsTrue(JSLong(1), db)
        testAllOperatorsTrue(JSTrue, db)
        testAllOperatorsTrue(JSDouble(1.1), db)
        testAllOperatorsTrue(JSNull, db)
        testAllOperatorsTrue(JSString("foo"), db)
        testAllOperatorsTrue(JSArray(JSLong(2), JSTrue), db)
        testAllOperatorsTrue(MkObject("a" -> JSLong(3)), db)
      }
    }
  }

  "compare a value with itself" - {
    once("works as expected") {
      for {
        db <- aDatabase
      } {
        testCompareValueWithItself(JSLong(1), db)
        testCompareValueWithItself(JSTrue, db)
        testCompareValueWithItself(JSDouble(1.1), db)
        testCompareValueWithItself(JSNull, db)
        testCompareValueWithItself(JSString("foo"), db)
        testCompareValueWithItself(JSArray(JSLong(2), JSTrue), db)
        testCompareValueWithItself(MkObject("a" -> JSLong(3)), db)
      }
    }
  }

  "compare two operands of the same type" - {
    once("works as expected") {
      for {
        db <- aDatabase
      } {
        assertLessThan(JSLong(1), JSLong(3), db)
        assertLessThan(JSLong(-3), JSLong(-2), db)
        assertLessThan(JSLong(-1), JSLong(1), db)
        assertLessThan(JSLong(Long.MinValue), JSLong(Long.MaxValue), db)
        assertLessThan(JSFalse, JSTrue, db)
        assertLessThan(JSDouble(1.1), JSDouble(3.3), db)
        assertLessThan(JSDouble(Double.MinValue), JSDouble(Double.MaxValue), db)
        assertLessThan(JSString(""), JSString("a"), db)
        assertLessThan(JSString("abcd"), JSString("abce"), db)
        assertLessThan(JSArray(JSLong(1), JSLong(2)),
                       JSArray(JSLong(1), JSLong(3)),
                       db)
        assertLessThan(MkObject("a" -> JSLong(1), "b" -> JSLong(2)),
                       MkObject("a" -> JSLong(1), "b" -> JSLong(3)),
                       db)
      }
    }
  }

  "compare many operands of the same type" - {
    once("works as expected") {
      for {
        db <- aDatabase
      } {
        assertLessThan(JSLong(Long.MinValue),
                       JSLong(3),
                       JSLong(5),
                       JSLong(7),
                       JSLong(Long.MaxValue),
                       db)
        assertLessThan(JSDouble(Double.MinValue),
                       JSDouble(-1E80),
                       JSDouble(0),
                       JSDouble(1E80),
                       JSDouble(Double.MaxValue),
                       db)
        assertLessThan(JSString(""),
                       JSString("a"),
                       JSString("aa"),
                       JSString("aaa"),
                       JSString("z"),
                       db)
        assertLessThan(JSArray(JSLong(1), JSLong(2)),
                       JSArray(JSLong(1), JSLong(3)),
                       JSArray(JSLong(2), JSLong(1)),
                       JSArray(JSLong(3), JSLong(1)),
                       JSArray(JSLong(3), JSLong(2)),
                       db)
        assertLessThan(
          MkObject("a" -> JSLong(1), "b" -> JSLong(2)),
          MkObject("a" -> JSLong(1), "b" -> JSLong(3)),
          MkObject("a" -> JSLong(2), "b" -> JSLong(1)),
          MkObject("a" -> JSLong(3), "b" -> JSLong(2)),
          MkObject("a" -> JSLong(4), "b" -> JSLong(1)),
          db
        )
      }
    }
  }

  "compare operands of different types" - {
    once("works as expected") {
      for {
        db <- aDatabase
      } {
        // Pairwise
        assertLessThan(JSLong(1), JSString("1"), db)
        assertLessThan(JSString(""), JSArray(), db)
        assertLessThan(JSArray(), MkObject(), db)
        assertLessThan(MkObject(), JSTrue, db)
        assertLessThan(JSDouble(1.0), JSTrue, db)
        assertLessThan(JSDouble(1.0), JSNull, db)

        qequals(LessThan(JSLong(1), JSDouble(1.5), JSLong(2)), JSTrue, db)
        qequals(LessThan(JSDouble(0.9), JSLong(1), JSDouble(1.1)), JSTrue, db)
        qequals(LessThanOrEquals(JSLong(1), JSDouble(1.0), JSLong(1)), JSTrue, db)

        qequals(GreaterThan(JSLong(2), JSDouble(1.5), JSLong(1)), JSTrue, db)
        qequals(GreaterThan(JSDouble(1.1), JSLong(1), JSDouble(0.9)), JSTrue, db)
        qequals(GreaterThanOrEquals(JSLong(1), JSDouble(1.0), JSLong(1)), JSTrue, db)

        // All together
        qequals(
          LessThan(
            JSLong(1),
            JSDouble(1.1),
            JSString("str"),
            JSArray(),
            MkObject(),
            JSFalse,
            JSTrue,
            JSNull
          ),
          JSTrue,
          db
        )
      }
    }
  }

  "invalid refs" - {
    once("works as expected") {
      for {
        db <- aDatabase
      } {
        testAllOperatorsFail(ClassRef("coll"), "invalid ref", "Ref refers to undefined collection 'coll'", db)
      }
    }
  }

  private def testAllOperatorsFail(arg: JSValue, code: String, desc: String, db: Database): Unit = {
    qassertErr(LessThan(arg), code, desc, JSArray("lt"), db)
    qassertErr(LessThanOrEquals(arg), code, desc, JSArray("lte"), db)
    qassertErr(GreaterThan(arg), code, desc, JSArray("gt"), db)
    qassertErr(GreaterThanOrEquals(arg), code, desc, JSArray("gte"), db)
  }

  private def testAllOperatorsTrue(arg: JSValue, db: Database): Unit = {
    qequals(LessThan(arg), JSTrue, db)
    qequals(LessThanOrEquals(arg), JSTrue, db)
    qequals(GreaterThan(arg), JSTrue, db)
    qequals(GreaterThanOrEquals(arg), JSTrue, db)
  }

  private def testCompareValueWithItself(arg: JSValue, db: Database): Unit = {
    qequals(LessThan(arg, arg), JSFalse, db)
    qequals(LessThanOrEquals(arg, arg), JSTrue, db)
    qequals(GreaterThan(arg, arg), JSFalse, db)
    qequals(GreaterThanOrEquals(arg, arg), JSTrue, db)
  }

  private def assertLessThan(
    arg1: JSValue,
    arg2: JSValue,
    arg3: JSValue,
    arg4: JSValue,
    arg5: JSValue,
    db: Database): Unit = {
    qequals(LessThan(arg1, arg2, arg3, arg4, arg5), JSTrue, db)
    qequals(LessThanOrEquals(arg1, arg2, arg3, arg4, arg5), JSTrue, db)

    qequals(GreaterThan(arg1, arg2, arg3, arg4, arg5), JSFalse, db)
    qequals(GreaterThanOrEquals(arg1, arg2, arg3, arg4, arg5), JSFalse, db)

    qequals(LessThan(arg5, arg4, arg3, arg2, arg1), JSFalse, db)
    qequals(LessThanOrEquals(arg5, arg4, arg3, arg2, arg1), JSFalse, db)

    qequals(GreaterThan(arg5, arg4, arg3, arg2, arg1), JSTrue, db)
    qequals(GreaterThanOrEquals(arg5, arg4, arg3, arg2, arg1), JSTrue, db)
  }

  private def assertLessThan(arg1: JSValue, arg2: JSValue, db: Database): Unit = {
    qequals(LessThan(arg1, arg2), JSTrue, db)
    qequals(LessThanOrEquals(arg1, arg2), JSTrue, db)

    qequals(GreaterThan(arg1, arg2), JSFalse, db)
    qequals(GreaterThanOrEquals(arg1, arg2), JSFalse, db)

    qequals(LessThan(arg2, arg1), JSFalse, db)
    qequals(LessThanOrEquals(arg2, arg1), JSFalse, db)

    qequals(GreaterThan(arg2, arg1), JSTrue, db)
    qequals(GreaterThanOrEquals(arg2, arg1), JSTrue, db)
  }
}
