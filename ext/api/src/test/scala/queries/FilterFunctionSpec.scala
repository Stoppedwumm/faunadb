package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._

class FilterFunctionSpec extends QueryAPI27Spec {

  "filter" - {
    once("selects elements from an array matching a predicate") {
      for {
        db <- aDatabase
      } {
        val even = Lambda("i" -> Equals(0, Modulo(Var("i"), 2)))
        qequals(JSArray(2), Filter(even, JSArray(1, 2, 3)), db)
      }
    }

    once("selects elements from a page or set matching a predicate") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        runQuery(
          CreateIndex(
            MkObject("name" -> "foo",
                     "source" -> cls.refObj,
                     "active" -> true,
                     "values" -> MkObject("field" -> JSArray("data", "foo")))),
          db)

        for (i <- 1 to 20) {
          runQuery(CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> i))), db)
        }

        val set = Match(IndexRef("foo"))
        val evens = Lambda("i" -> Equals(Modulo(Var("i"), 2), 0))
        val odds = Lambda("i" -> Equals(Modulo(Var("i"), 2), 1))

        // FIXME: Reenable.
        // val threes = Let("three" -> 3)(Lambda("i" -> Equals(Modulo(Var("i"), Var("three")), 0)))
        val threes = Lambda("i" -> Equals(Modulo(Var("i"), 3), 0))

        qequals(JSArray(2, 4, 6, 8, 10), Select("data", Filter(evens, Paginate(set, size = 10))), db)
        qequals(JSArray(2, 4, 6, 8, 10), Select("data", Paginate(Filter(evens, set), size = 5)), db)
        qequals(Paginate(Filter(evens, set)), Filter(evens, Paginate(set)), db)

        qequals(Paginate(set), Paginate(Union(Filter(evens, set), Filter(odds, set))), db)
        qequals(JSArray(6, 12, 18), Select("data", Paginate(Intersection(Filter(evens, set), Filter(threes, set)))), db)
        qequals(JSArray(2, 4, 8, 10, 14, 16, 20), Select("data", Paginate(Difference(Filter(evens, set), Filter(threes, set)))), db)
      }
    }

    once("returns a reusable set ref") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        runQuery(
          CreateIndex(
            MkObject("name" -> "foo",
                     "source" -> cls.refObj,
                     "active" -> true)),
          db)

        val set = Match(IndexRef("foo"))
        val filter = Filter(Lambda("i" -> true), set)
        val setJSON = runQuery(set, db)
        val filterJSON = runQuery(filter, db)

        filterJSON should equal(
          JSObject("@set" -> JSObject("filter" -> JSObject("lambda" -> "i","expr" -> true),
                                      "collection" -> setJSON)))
      }
    }

    once("convert set to irValue") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
      } {
        val doc = runQuery(
          CreateF(coll.refObj, MkObject("credentials" -> MkObject("password" -> "sekret"))),
          db
        )

        //login call .toDiff on ObjectL, which calls irValue
        val token = runRawQuery(
          Login(doc.refObj, MkObject("password" -> "sekret", "data" -> MkObject("filter" -> Filter(Lambda("ref" -> true), ClassesNativeClassRef)))),
          db.key
        )

        token should respond(200, 201)

        //write functions call .toDiff on ObjectL, which calls irValue
        val create = runRawQuery(
          CreateF(
            coll.refObj,
            MkObject("data" -> MkObject("filter" -> Filter(Lambda("ref" -> true), ClassesNativeClassRef)))
          ),
          db.key
        )

        create should respond (200, 201)
      }
    }

    once("returns an error if the array, page, or set elements do not match the predicate arity") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        runQuery(
          CreateIndex(
            MkObject("name" -> "foo",
                     "source" -> cls.refObj,
                     "active" -> true,
                     "values" -> MkObject("field" -> JSArray("data", "foo")))),
          db)

        for (i <- 1 to 20) {
          runQuery(CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> i))), db)
        }

        val set = Match(IndexRef("foo"))
        val bad = Lambda(JSArray("i", "oops") -> true)

        // array
        qassertErr(Filter(bad, JSArray(1, 2, 3, 4, 5)),
                   "invalid argument",
                   "Lambda expects an array with 2 elements. Array contains 1.",
                   JSArray("filter", "lambda"),
                   db)

        // page
        qassertErr(Filter(bad, Paginate(set)),
                   "invalid argument",
                   "Lambda expects an array with 2 elements. Array contains 1.",
                   JSArray("filter", "lambda"),
                   db)

        // set
        qassertErr(Paginate(Filter(bad, set)),
                   "invalid argument",
                   "Lambda expects an array with 2 elements. Array contains 1.",
                   JSArray("paginate", "filter", "lambda"),
                   db)
      }
    }

    once("disallows writes when called with a set") {
      for {
        db <- aDatabase
      } {
        val set = DatabasesRef
        val badWrite = Filter(Lambda("db" -> CreateClass(MkObject("name" -> "things"))), set)

        qassertErr(badWrite,
                   "invalid argument",
                   "Invalid lambda. Lambda in this context may not write or call a user function.",
                   JSArray("filter"),
                   db)
      }
    }
  }
}
