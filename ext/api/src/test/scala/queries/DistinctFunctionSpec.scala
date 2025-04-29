package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.prop.Prop

class DistinctFunctionSpec extends QueryAPI27Spec {

  "Distinct" - {
    once("removes duplicates") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        runQuery(CreateIndex(
                   MkObject("name" -> "foo",
                            "source" -> cls.refObj,
                            "active" -> true,
                            "values" -> JSArray(
                              MkObject("field" -> JSArray("data", "foo"))))),
                 db)

        for (i <- List(1, 1, 1, 2, 2, 2, 3, 4, 3, 3, 1, 1, 2)) {
          runQuery(CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> i))), db)
        }
        val q1 = Match(IndexRef("foo"))
        val q2 = Distinct(q1)
        collection(q2, db) should equal(Seq(1, 2, 3, 4) map { JSLong(_) })
        collection(q2, db) should not equal (collection(q1, db))
        // composes correctly
        collection(Union(q2, q1), db) should equal(collection(q1, db))
        // history is unaltered
        events(q2, db) should equal(events(q1, db))
      }
    }

    once("pass-through when ref is covered ") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        runQuery(
          CreateIndex(
            MkObject("name" -> "foo",
                     "source" -> cls.refObj,
                     "active" -> true,
                     "values" -> JSArray(MkObject("field" -> JSArray("data", "foo")),
                                         MkObject("field" -> "ref")))),
          db)

        for (i <- List(1, 1, 1, 2, 2, 2, 3, 4, 3, 3, 1, 1, 2)) {
          runQuery(CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> i))), db)
        }
        val q1 = Match(IndexRef("foo"))
        val q2 = Distinct(q1)
        collection(q1, db) should equal(collection(q2, db))
      }
    }

    once("works across page boundaries") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        runQuery(CreateIndex(
                   MkObject("name" -> "foo",
                            "source" -> cls.refObj,
                            "active" -> true,
                            "values" -> JSArray(
                              MkObject("field" -> JSArray("data", "foo"))))),
                 db)

        for (i <- List
               .fill(100)(List(1, 1, 1, 2, 2, 2, 3, 4, 3, 3, 1, 1, 2))
               .flatten) {
          runQuery(CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> i))), db)
        }

        val q1 = Match(IndexRef("foo"))
        val q2 = Distinct(q1)
        collection(q2, db) should not equal (collection(q1, db))
        collection(q2, db) should equal(Seq(1, 2, 3, 4) map { JSLong(_) })
      }
    }

    once("returns a resuable set ref") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        runQuery(CreateIndex(
                   MkObject("name" -> "foo",
                            "source" -> cls.refObj,
                            "active" -> true,
                            "values" -> JSArray(
                              MkObject("field" -> JSArray("data", "foo"))))),
                 db)

        val set = Match(IndexRef("foo"))
        val distinct = Distinct(set)
        val setJSON = runQuery(set, db)
        val distinctJSON = runQuery(distinct, db)

        distinctJSON should equal (
          JSObject("@set" -> JSObject("distinct" -> setJSON)))

        qequals(distinctJSON, distinct, db)
      }
    }

    once("works on arrays") {
      for {
        db <- aDatabase
      } {
        qequals(Distinct(JSArray(1, 1, 2, 2, 3, 3, 4, 4)), JSArray(1, 2, 3, 4), db)
        qequals(Distinct(JSArray(4, 4, 3, 3, 2, 2, 1, 1)), JSArray(4, 3, 2, 1), db)
        qequals(Distinct(JSArray(4, 4, 2, 2, 3, 3, 1, 1)), JSArray(4, 2, 3, 1), db)
        qequals(Distinct(JSArray(1, "str", 2, "str", 1, 2, "str", 3.14)), JSArray(1, "str", 2, 3.14), db)
      }
    }

    once("works on pages") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        asc <- anIndex(coll, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))))
        desc <- anIndex(coll, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), true))))
      } {
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> 1))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> 1))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> 3))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> 3))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> 6))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> 2))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> 2))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> 4))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> 4))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> "str"))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> "str"))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> 3.14))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> 3.14))), db)

        runQuery(Distinct(Paginate(Match(asc.refObj))), db) shouldBe JSObject("data" -> JSArray(1, 2, 3, 3.14, 4, 6, "str"))
        runQuery(Distinct(Paginate(Match(desc.refObj))), db) shouldBe JSObject("data" -> JSArray("str", 6, 4, 3.14, 3, 2, 1))
      }
    }
  }
}
