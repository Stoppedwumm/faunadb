package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.prop._

class IntersectionFunctionSpec extends SetSpec {

  "Intersection" - {
    once("evaluates to its set identifier") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
      } {
        val setfn = Intersection(Match(idx.refObj, "x"), Match(idx.refObj, "y"))
        val setref = SetRef(Intersection(SetRef(Match(idx.refObj, "x")), SetRef(Match(idx.refObj, "y"))))

        qequals(setfn, setref, db)
        runQuery(setfn, db) should equal (setref)
        runQuery(setref, db) should equal (setref)
      }
    }

    prop("sparse intersection collection with covered values") {
      for {
        db <- aDatabase
        users <- aCollection(db, nameProp = Prop.const("users"))
        follows <- aCollection(db, nameProp = Prop.const("follows"))
        recs <- aCollection(db, nameProp = Prop.const("recommendations"))
        follows_by_follower <- anIndex(follows, Prop.const(Seq(JSArray("data", "follower"))), Prop.const(Seq(JSArray("data", "followee") -> false)))
        recs_by_post <- anIndex(recs, Prop.const(Seq(JSArray("data", "post"))), Prop.const(Seq(JSArray("data", "user") -> false)))

        user1 <- aDocument(users)
        user2 <- aDocument(users)
        user3 <- aDocument(users)
        user4 <- aDocument(users)
        user5 <- aDocument(users)

        instCount <- Prop.int(DefaultRange)
        yInst1 <- aDocument(follows, dataProp = Prop.const(JSObject("follower" -> user1.refObj, "followee" -> user2.refObj)))
        yInst2 <- aDocument(follows, dataProp = Prop.const(JSObject("follower" -> user1.refObj, "followee" -> user3.refObj)))
        yInst3 <- aDocument(follows, dataProp = Prop.const(JSObject("follower" -> user1.refObj, "followee" -> user4.refObj)))
        yInst4 <- aDocument(follows, dataProp = Prop.const(JSObject("follower" -> user1.refObj, "followee" -> user5.refObj)))

        post = Prop.const(JSString("post1"))
        _ <- someDocuments(instCount, recs, dataProp = jsObject("post" -> post, "user" -> jsString))
        _ <- aDocument(recs, dataProp = Prop.const(JSObject("post" -> "post1", "user" -> user2.refObj)))
        _ <- someDocuments(instCount, recs, dataProp = jsObject("post" -> post, "user" -> jsString))
        _ <- aDocument(recs, dataProp = Prop.const(JSObject("post" -> "post1", "user" -> user3.refObj)))
        _ <- someDocuments(instCount, recs, dataProp = jsObject("post" -> post, "user" -> jsString))
        _ <- aDocument(recs, dataProp = Prop.const(JSObject("post" -> "post1", "user" -> user4.refObj)))
        _ <- someDocuments(instCount, recs, dataProp = jsObject("post" -> post, "user" -> jsString))
        _ <- aDocument(recs, dataProp = Prop.const(JSObject("post" -> "post1", "user" -> user5.refObj)))
        _ <- someDocuments(instCount, recs, dataProp = jsObject("post" -> post, "user" -> jsString))
      } {
        val matchX = Match(follows_by_follower.refObj, user1.refObj)
        val matchY = Match(recs_by_post.refObj, "post1")
        val expected = Seq(yInst1, yInst2, yInst3, yInst4) map { i => Document(i.ts, (i / "data" / "followee").as[JSObject]) }
        validateCollection(db, Intersection(matchX, matchY), expected.toSet)
      }
    }

    prop("sparse intersection collection") {
      for {
        db <- aDatabase
        cls <- aCollection(db, nameProp = Prop.const("follows"))
        idxX <- anIndex(cls, Prop.const(Seq(JSArray("data", "x"))))
        idxY <- anIndex(cls, Prop.const(Seq(JSArray("data", "y"))))

        instCount <- Prop.int(DefaultRange)
        _ <- someDocuments(instCount, cls, dataProp = Prop.const(JSObject("x" -> "x")))
        yInst1 <- aDocument(cls, dataProp = Prop.const(JSObject("x" -> "x", "y" -> "y")))
        _ <- someDocuments(instCount, cls, dataProp = Prop.const(JSObject("x" -> "x")))
        yInst2 <- aDocument(cls, dataProp = Prop.const(JSObject("x" -> "x", "y" -> "y")))
        _ <- someDocuments(instCount, cls, dataProp = Prop.const(JSObject("x" -> "x")))
        yInst3 <- aDocument(cls, dataProp = Prop.const(JSObject("x" -> "x", "y" -> "y")))
        _ <- someDocuments(instCount, cls, dataProp = Prop.const(JSObject("x" -> "x")))
        yInst4 <- aDocument(cls, dataProp = Prop.const(JSObject("x" -> "x", "y" -> "y")))
      } {
        val matchX = Match(idxX.refObj, "x")
        val matchY = Match(idxY.refObj, "y")
        val expected = Seq(yInst1, yInst2, yInst3, yInst4) map { Document.fromJS(_) }
        validateCollection(db, Intersection(matchX, matchY), expected.toSet)
      }
    }

    prop("collection") {
      for {
        db <- aDatabase
        (_, set1, coll1) <- collP(db)
        (_, set2, _) <- collP(db)
      } {
        validateCollection(db, Intersection(set1, set1), coll1)
        validateCollection(db, Intersection(set1, set2), Set.empty)
      }
    }

    prop("historical") {
      for {
        db <- aDatabase
        (_, set1, es1) <- eventsP(db)
        (_, set2, _) <- eventsP(db)
      } {
        validateEvents(db, Intersection(set1, set1), es1)
        validateEvents(db, Intersection(set1, set2), Set.empty)
      }
    }

    once("emits minimum element count for each source set") {
      for {
        db <- aDatabase
        cls1 <- aCollection(db)
        cls2 <- aCollection(db)
      } {
        runQuery(CreateIndex(MkObject(
          "name" -> "test1",
          "source" -> cls1.refObj,
          "active" -> true,
          "values" -> JSArray(MkObject("field" -> JSArray("data", "n"))))), db)

        runQuery(CreateIndex(MkObject(
          "name" -> "test2",
          "source" -> cls2.refObj,
          "active" -> true,
          "values" -> JSArray(MkObject("field" -> JSArray("data", "n"))))), db)

        Seq(1, 2, 2, 3, 3, 3) foreach { i =>
          runQuery(CreateF(cls1.refObj,
            MkObject("data" -> MkObject("n" -> i))), db)
        }

        Seq(1, 2, 2, 2, 3, 3) foreach { i =>
          runQuery(CreateF(cls2.refObj,
            MkObject("data" -> MkObject("n" -> i))), db)
        }

        val q = Intersection(Match(IdxRefV("test1"), JSArray()), Match(IdxRefV("test2"), JSArray()))
        collection(q, db) should equal (Seq(1, 2, 2, 3, 3) map { JSLong(_) })
      }
    }

    once("works with arrays") {
      for {
        db <- aDatabase
      } {
        runQuery(Intersection(JSArray(1, 2)), db) shouldBe JSArray(1, 2)
        runQuery(Intersection(JSArray(1, 2), JSArray(3, 4)), db) shouldBe JSArray.empty
        runQuery(Intersection(JSArray(1, 2), JSArray(4, 2)), db) shouldBe JSArray(2)

        runQuery(Intersection(JSArray(1, 2), JSArray(3, 4), JSArray(1, 2)), db) shouldBe JSArray.empty
        runQuery(Intersection(JSArray(1, 2), JSArray(3, 4, 2, 1), JSArray(1, 2)), db) shouldBe JSArray(1, 2)

        //preserve order and duplicates
        runQuery(Intersection(JSArray(1, 2, 2, 3), JSArray(1, 2, 2, 5)), db) shouldBe JSArray(1, 2, 2)
        runQuery(Intersection(JSArray(3, 2, 2, 1), JSArray(2, 2, 1, 0)), db) shouldBe JSArray(2, 2, 1)
      }
    }

    once("doesn't accept mixed types") {
      for {
        db <- aDatabase
      } {
        qassertErr(
          Intersection(JSArray(1, 2), Events(db.refObj)),
          "invalid argument",
          "Arguments cannot be of different types, expected Set or Array.",
          JSArray("intersection"),
          rootKey
        )
      }
    }

    once("doesn't accept pages") {
      for {
        db <- aDatabase
      } {
        qassertErr(
          Intersection(Paginate(Events(db.refObj))),
          "invalid argument",
          "Set or Array expected, Page provided.",
          JSArray("intersection", 0),
          rootKey
        )
      }
    }
  }
}
