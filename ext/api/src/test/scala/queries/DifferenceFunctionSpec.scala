package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._

class DifferenceFunctionSpec extends SetSpec {

  "Difference" - {
    once("evaluates to its set identifier") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
      } {
        val setfn = Difference(Match(idx.refObj, "x"), Match(idx.refObj, "y"))
        val setref = SetRef(Difference(SetRef(Match(idx.refObj, "x")), SetRef(Match(idx.refObj, "y"))))

        qequals(setfn, setref, db)
        runQuery(setfn, db) should equal (setref)
        runQuery(setref, db) should equal (setref)
      }
    }

    prop("collection") {
      for {
        db <- aDatabase
        (_, set1, coll1) <- collP(db)
        (_, set2, coll2) <- collP(db)
      } validateCollection(db, Difference(set1, set2), coll1 -- coll2)
    }

    prop("historical") {
      for {
        db <- aDatabase
        (_, set1, es1) <- eventsP(db)
        (_, set2, es2) <- eventsP(db)
      } validateEvents(db, Difference(set1, set2), es1 -- es2)
    }

    once("works with arrays") {
      for {
        db <- aDatabase
      } {
        runQuery(Difference(JSArray(1, 2)), db) shouldBe JSArray(1, 2)
        runQuery(Difference(JSArray(1, 2), JSArray(3, 4)), db) shouldBe JSArray(1, 2)
        runQuery(Difference(JSArray(1, 2), JSArray(1, 2)), db) shouldBe JSArray.empty

        runQuery(Difference(JSArray(1, 2), JSArray(3, 4), JSArray(1, 2)), db) shouldBe JSArray.empty
        runQuery(Difference(JSArray(4, 3, 2, 1), JSArray(5, 1, 2), JSArray(1, 2)), db) shouldBe JSArray(4, 3)

        //preserve order and duplicates
        runQuery(Difference(JSArray(1, 2, 2, 3, 3), JSArray(3)), db) shouldBe JSArray(1, 2, 2, 3)
        runQuery(Difference(JSArray(3, 2, 2, 1, 3), JSArray(3)), db) shouldBe JSArray(2, 2, 1, 3)
      }
    }

    once("doesn't accept mixed types") {
      for {
        db <- aDatabase
      } {
        qassertErr(
          Difference(JSArray(1, 2), Events(db.refObj)),
          "invalid argument",
          "Arguments cannot be of different types, expected Set or Array.",
          JSArray("difference"),
          rootKey
        )
      }
    }

    once("doesn't accept pages") {
      for {
        db <- aDatabase
      } {
        qassertErr(
          Difference(Paginate(Events(db.refObj))),
          "invalid argument",
          "Set or Array expected, Page provided.",
          JSArray("difference", 0),
          rootKey
        )
      }
    }
  }
}
