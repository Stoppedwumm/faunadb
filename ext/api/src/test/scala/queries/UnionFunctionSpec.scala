package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.prop._

class UnionFunctionSpec extends SetSpec {

  "Union" - {
    prop("collection") {
      for {
        db <- aDatabase
        (_, set1, coll1) <- collP(db)
        (_, set2, coll2) <- collP(db)
      } validateCollection(db, Union(set1, set2), coll1 ++ coll2)
    }

    prop("historical") {
      for {
        db <- aDatabase
        (_, set1, es1) <- eventsP(db)
        (_, set2, es2) <- eventsP(db)
      } validateEvents(db, Union(set1, set2), es1 ++ es2)
    }

    once("evaluates to its set identifier") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
      } {
        val setfn = Union(Match(idx.refObj, "x"), Match(idx.refObj, "y"))
        val setref = SetRef(Union(SetRef(Match(idx.refObj, "x")), SetRef(Match(idx.refObj, "y"))))

        qequals(setfn, setref, db)
        runQuery(setfn, db) should equal (setref)
        runQuery(setref, db) should equal (setref)
      }
    }

    once("emits maximum element count for each source set") {
      for {
        db <- aDatabase
        cls1 <- aCollection(db)
        idx1 <- anIndex(cls1, Prop.const(Nil), Prop.const(Seq(JSArray("data", "n") -> false)))
        cls2 <- aCollection(db)
        idx2 <- anIndex(cls2, Prop.const(Nil), Prop.const(Seq(JSArray("data", "n") -> false)))
      } {
        Seq(1, 2, 2, 3, 3, 3) foreach { i =>
          runQuery(CreateF(cls1.refObj,
            MkObject("data" -> MkObject("n" -> i))), db)
        }

        Seq(1, 2, 2, 2, 3, 3) foreach { i =>
          runQuery(CreateF(cls2.refObj,
            MkObject("data" -> MkObject("n" -> i))), db)
        }

        val q = Union(Match(idx1.refObj), Match(idx2.refObj))
        collection(q, db) should equal (Seq(1, 2, 2, 2, 3, 3, 3) map { JSLong(_) })
        collection(Distinct(q), db) should equal (Seq(1, 2, 3) map { JSLong(_) })
      }
    }

    once("works with arrays") {
      for {
        db <- aDatabase
      } {
        runQuery(Union(JSArray(1, 2)), db) shouldBe JSArray(1, 2)
        runQuery(Union(JSArray(1, 2), JSArray(3, 4)), db) shouldBe JSArray(1, 2, 3, 4)

        //preserve order and duplicates
        runQuery(Union(JSArray(2, 1), JSArray(2, 1)), db) shouldBe JSArray(2, 1)
        runQuery(Union(JSArray(2, 2), JSArray(2, 2)), db) shouldBe JSArray(2, 2)
        runQuery(Union(JSArray(2, 2), JSArray(2, 2, 2)), db) shouldBe JSArray(2, 2, 2)
        runQuery(Union(JSArray(3, 2, 2, 1), JSArray(6, 5, 9, 1)), db) shouldBe JSArray(3, 2, 2, 1, 6, 5, 9)
        runQuery(Union(JSArray(6, 5, 9, 1), JSArray(3, 2, 2, 1)), db) shouldBe JSArray(6, 5, 9, 1, 3, 2, 2)

        runQuery(Union(JSArray(1, 2), JSArray(3, 4), JSArray(5, 6)), db) shouldBe JSArray(1, 2, 3, 4, 5, 6)
        runQuery(Union(JSArray(6, 5), JSArray(4, 3), JSArray(2, 1)), db) shouldBe JSArray(6, 5, 4, 3, 2, 1)
        runQuery(Union(JSArray(1, 2), JSArray(1, 2, 3, 4), JSArray(5, 6)), db) shouldBe JSArray(1, 2, 3, 4, 5, 6)

        //from set test
        runQuery(Union(JSArray(1, 2, 2, 3, 3, 3), JSArray(1, 2, 2, 2, 3, 3)), db) shouldBe JSArray(1, 2, 2, 2, 3, 3, 3)
      }
    }

    once("doesn't accept mixed types") {
      for {
        db <- aDatabase
      } {
        qassertErr(
          Union(JSArray(1, 2), Events(db.refObj)),
          "invalid argument",
          "Arguments cannot be of different types, expected Set or Array.",
          JSArray("union"),
          rootKey
        )
      }
    }

    once("doesn't accept pages") {
      for {
        db <- aDatabase
      } {
        qassertErr(
          Union(Paginate(Events(db.refObj))),
          "invalid argument",
          "Set or Array expected, Page provided.",
          JSArray("union", 0),
          rootKey
        )
      }
    }
  }
}
