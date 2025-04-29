package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.prop.Prop

class RangeFunctionSpec extends QueryAPI27Spec {

  "Range" - {
    once("limits a set to a lower and upper bound") {
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

        for (i <- 1 to 20) {
          runQuery(CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> i))), db)
        }
        val q1 = Match(IndexRef("foo"))
        val q2 = RangeF(q1, 5, 10)
        collection(q2, db) should equal (5 to 10 map { JSLong(_) })
        collection(q2, db) should not equal (collection(q1, db))
        // composes correctly
        collection(Union(q2, q1), db) should equal(collection(q1, db))
      }
    }

    once("limits a set to a lower and upper bound with refs") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        runQuery(CreateIndex(
          MkObject("name" -> "foo",
            "source" -> cls.refObj,
            "active" -> true
          )),
          db)

        for (i <- 1 to 20) {
          runQuery(CreateF(MkRef(cls.refObj, i), MkObject()), db)
        }
        val q1 = Match(IndexRef("foo"))
        val q2 = RangeF(q1, MkRef(cls.refObj, 5), MkRef(cls.refObj, 10))
        collection(q2, db) should equal (5 to 10 map { id => RefV(id.toString, cls.refObj) })
        collection(q2, db) should not equal collection(q1, db)
        // composes correctly
        collection(Union(q2, q1), db) should equal(collection(q1, db))
      }
    }

    once("limits a set to a lower and upper bound with ref and field") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        runQuery(CreateIndex(
          MkObject("name" -> "foo",
            "source" -> cls.refObj,
            "active" -> true,
            "values" -> JSArray(
              MkObject("field" -> JSArray("ref")),
              MkObject("field" -> JSArray("data", "bar")),
              MkObject("field" -> JSArray("ref"))
            )
          )),
          db)

        for (i <- 1 to 20) {
          runQuery(CreateF(MkRef(cls.refObj, i), MkObject("data" -> MkObject("bar" -> i))), db)
        }
        val q1 = Match(IndexRef("foo"))
        val q2 = RangeF(q1, MkRef(cls.refObj, 5), MkRef(cls.refObj, 10))
        collection(q2, db) should equal (5 to 10 map { id => JSArray(RefV(id.toString, cls.refObj), id, RefV(id.toString, cls.refObj)) })
        collection(q2, db) should not equal collection(q1, db)
        // composes correctly
        collection(Union(q2, q1), db) should equal(collection(q1, db))
      }
    }

    once("limits based on lexical prefix") {
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
                                         MkObject("field" -> JSArray("data", "bar"))))),
          db)

        for (i <- 1 to 20) {
          val data = MkObject("data" -> MkObject("foo" -> i, "bar" -> -i))
          runQuery(CreateF(cls.refObj, data), db)
        }
        val q1 = Match(IndexRef("foo"))
        val q2 = RangeF(q1, 3, 6)
        collection(q2, db) should equal (3 to 6 map { i => JSArray(JSLong(i), JSLong(-i)) })
      }
    }

    once("returns a reusable set ref") {
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
        val range = RangeF(set, 3, 6)
        val setJSON = runQuery(set, db)
        val rangeJSON = runQuery(range, db)

        rangeJSON should equal (
          JSObject("@set" -> JSObject("range" -> setJSON, "from" -> 3, "to" -> 6)))

        qequals(rangeJSON, range, db)
      }
    }

    once("does not support historical events reads") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("ts"))))
      } {
        runQuery(CreateF(cls.refObj, MkObject("data" -> MkObject("firstName" -> "Bob"))), db)
        val q = Paginate(Events(RangeF(Match(idx.refObj), 0, Now())))
        qassertErr(q, "invalid argument", "Set does not support events history.", JSArray.empty, db.key)
      }
    }

    once("invalid refs") {
      for {
        db <- aDatabase
      } {
        val ret = runRawQuery(Paginate(
          RangeF(ClassesNativeClassRef, ClassRef("foo"), ClassRef("bar"))
        ), db.key)
        ret should respond (400)

        (ret.errors / 0 / "code") shouldBe JSString("invalid ref")
        (ret.errors / 0 / "description") shouldBe JSString("Ref refers to undefined collection 'foo'")
        (ret.errors / 0 / "position") shouldBe JSArray("paginate", "from")

        (ret.errors / 1 / "code") shouldBe JSString("invalid ref")
        (ret.errors / 1 / "description") shouldBe JSString("Ref refers to undefined collection 'bar'")
        (ret.errors / 1 / "position") shouldBe JSArray("paginate", "to")
      }
    }
  }
}
