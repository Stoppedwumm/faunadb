package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.prop.Prop

class ReverseFunctionSpec extends SetSpec {
  "Reverse" - {
    prop("arrays") {
      for {
        db <- aDatabase
        size <- Prop.int(10000)
      } {
        val values = (1 to size).map { JSLong(_) }

        runQuery(Reverse(values), db) shouldBe JSArray(values.reverse: _*)
      }
    }

    once("pages") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(
          Foreach(Lambda("i" -> CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> Var("i"))))), 1 to 100),
          db
        )

        val values0 = (1 to 100).map { JSLong(_) }
        val result0 = runQuery(Reverse(Paginate(Match(idx.refObj), size = 100)), db)
        (result0 / "data") shouldBe JSArray(values0.reverse: _*)

        val values1 = (1 to 25).map { JSLong(_) }
        val result1 = runQuery(Reverse(Paginate(Match(idx.refObj), size = 25, cursor = After(1))), db)
        (result1 / "data") shouldBe JSArray(values1.reverse: _*)
        (result1 / "after" / 0) shouldBe JSLong(26)
        (result1 / "before" / 0) shouldBe JSLong(1)

        val values2 = (26 to 50).map { JSLong(_) }
        val result2 = runQuery(Reverse(Paginate(Match(idx.refObj), size = 25, cursor = After(26))), db)
        (result2 / "data") shouldBe JSArray(values2.reverse: _*)
        (result2 / "after" / 0) shouldBe JSLong(51)
        (result2 / "before" / 0) shouldBe JSLong(26)

        val values3 = (51 to 75).map { JSLong(_) }
        val result3 = runQuery(Reverse(Paginate(Match(idx.refObj), size = 25, cursor = After(51))), db)
        (result3 / "data") shouldBe JSArray(values3.reverse: _*)
        (result3 / "after" / 0) shouldBe JSLong(76)
        (result3 / "before" / 0) shouldBe JSLong(51)

        val values4 = (76 to 100).map { JSLong(_) }
        val result4 = runQuery(Reverse(Paginate(Match(idx.refObj), size = 25, cursor = After(76))), db)
        (result4 / "data") shouldBe JSArray(values4.reverse: _*)
        (result4 / "before" / 0) shouldBe JSLong(76)
      }
    }

    prop("sets") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq.empty), Prop.const(Seq.empty))
        instCount <- Prop.int(DefaultRange)
        docs <- someDocuments(instCount, cls)
      } {
        val expected = docs map { Document.fromJS(_) }
        validateCollection(db, Reverse(Match(idx.refObj)), expected, reversed = true)
      }
    }

    once("range") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(
          Foreach(Lambda("i" -> CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> Var("i"))))), 1 to 100),
          db
        )
        //range = 50 .. 60 / reverse = 60 .. 50
        val values0 = (50 to 60).map { JSLong(_) }
        val result0 = runQuery(Paginate(Reverse(RangeF(Match(idx.refObj), 50, 60))), db)
        (result0 / "data") shouldBe JSArray(values0.reverse: _*)

        //reverse = 100 .. 1 / range = 60 .. 50
        val values1 = (50 to 60).map { JSLong(_) }
        val result1 = runQuery(Paginate(RangeF(Reverse(Match(idx.refObj)), 60, 50)), db)
        (result1 / "data") shouldBe JSArray(values1.reverse: _*)

        //after

        //range = 10 .. 20 / reverse = 20 .. 10 / after(15) = 15 .. 10
        val values2 = (10 to 15).map { JSLong(_) }
        val result2 = runQuery(Paginate(Reverse(RangeF(Match(idx.refObj), 10, 20)), cursor = After(15)), db)
        (result2 / "data") shouldBe JSArray(values2.reverse: _*)

        //reverse = 100 .. 1 / range = 20 .. 10 / after(15) = 15 .. 10
        val values3 = (10 to 15).map { JSLong(_) }
        val result3 = runQuery(Paginate(RangeF(Reverse(Match(idx.refObj)), 20, 10), cursor = After(15)), db)
        (result3 / "data") shouldBe JSArray(values3.reverse: _*)

        //before

        //range = 10 .. 20 / reverse = 20 .. 10 / before(15) = 20 .. 16
        val values4 = (16 to 20).map { JSLong(_) }
        val result4 = runQuery(Paginate(Reverse(RangeF(Match(idx.refObj), 10, 20)), cursor = Before(15)), db)
        (result4 / "data") shouldBe JSArray(values4.reverse: _*)

        //reverse = 100 .. 1 / range = 20 .. 10 / before(15) = 20 .. 16
        val values5 = (16 to 20).map { JSLong(_) }
        val result5 = runQuery(Paginate(RangeF(Reverse(Match(idx.refObj)), 20, 10), cursor = Before(15)), db)
        (result5 / "data") shouldBe JSArray(values5.reverse: _*)
      }
    }

    prop("sparse intersection collection with covered values") {
      for {
        db <- aDatabase
        users <- aCollection(db, nameProp = Prop.const("users"))
        follows <- aCollection(db, nameProp = Prop.const("follows"))
        recs <- aCollection(db, nameProp = Prop.const("recommendations"))
        followsByFollower <- anIndex(follows, Prop.const(Seq(JSArray("data", "follower"))), Prop.const(Seq(JSArray("data", "followee") -> false)))
        recsByPost <- anIndex(recs, Prop.const(Seq(JSArray("data", "post"))), Prop.const(Seq(JSArray("data", "user") -> false)))

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
        val matchX = Reverse(Match(followsByFollower.refObj, user1.refObj))
        val matchY = Reverse(Match(recsByPost.refObj, "post1"))

        val expected = Seq(yInst4, yInst3, yInst2, yInst1) map { i =>
          Document(i.ts, (i / "data" / "followee").as[JSObject])
        }
        validateCollection(db, Intersection(matchX, matchY), expected, reversed = true)
      }
    }

    prop("sparse intersection collection") {
      for {
        db <- aDatabase
        cls <- aCollection(db, nameProp = Prop.const("follows"))
        idxX <- anIndex(cls, Prop.const(Seq(JSArray("data", "x"))))
        idxY <- anIndex(cls, Prop.const(Seq(JSArray("data", "y"))))

        instCount <- Prop.int(100 to 500)

        _ <- someDocuments(instCount, cls, dataProp = Prop.const(JSObject("x" -> "x")))
        yInst1 <- aDocument(cls, dataProp = Prop.const(JSObject("x" -> "x", "y" -> "y")))

        _ <- someDocuments(instCount, cls, dataProp = Prop.const(JSObject("x" -> "x")))
        yInst2 <- aDocument(cls, dataProp = Prop.const(JSObject("x" -> "x", "y" -> "y")))

        _ <- someDocuments(instCount, cls, dataProp = Prop.const(JSObject("x" -> "x")))
        yInst3 <- aDocument(cls, dataProp = Prop.const(JSObject("x" -> "x", "y" -> "y")))

        _ <- someDocuments(instCount, cls, dataProp = Prop.const(JSObject("x" -> "x")))
        yInst4 <- aDocument(cls, dataProp = Prop.const(JSObject("x" -> "x", "y" -> "y")))
      } {
        val matchX = Reverse(Match(idxX.refObj, "x"))
        val matchY = Reverse(Match(idxY.refObj, "y"))

        val expected = Seq(yInst4, yInst3, yInst2, yInst1) map { i =>
          Document.fromJS(i)
        }
        validateCollection(db, Intersection(matchX, matchY), expected, reversed = true)
      }
    }

    once("returns a reusable set ref") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll, Prop.const(Seq.empty))
        doc <- aDocument(coll)
      } {
        val set = runQuery(Reverse(Match(idx.refObj)), db)

        set shouldBe JSObject("@set" -> JSObject("reverse" -> JSObject("@set" -> JSObject("match" -> idx.refObj))))

        val refs = collection(set, db)
        refs should contain only doc.refObj
      }
    }
  }
}
