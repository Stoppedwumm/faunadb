package fauna.api.test.queries

import fauna.api.test._

class MatchFunctionSpec extends SetSpec {

  "Match" - {
    once("evaluates to its set identifier") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
      } {
        val setfn = Match(idx.refObj, "x")
        val setref = SetRef(Match(idx.refObj, "x"))
        val namefn = Match(idx.name, "x")

        qequals(setfn, setref, db)
        qequals(namefn, setref, db)
        runQuery(setfn, db) should equal (setref)
        runQuery(setref, db) should equal (setref)
        runQuery(namefn, db) should equal (setref)
      }
    }

    prop("collection") {
      for {
        db <- aDatabase
        (_, set, coll) <- collP(db)
      } validateCollection(db, set, coll)
    }

    prop("historical") {
      for {
        db <- aDatabase
        (_, set, es) <- eventsP(db)
      } validateEvents(db, set, es)
    }
  }
}
