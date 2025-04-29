package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.prop._

class JoinFunctionSpec extends SetSpec {
  "Join" - {
    once("evaluates to its set identifier") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
      } {
        val lambda = Lambda("t" -> Match(idx.refObj, Var("t")))
        val setfn = Join(Match(idx.refObj, "x"), lambda)
        val setref = SetRef(Join(SetRef(Match(idx.refObj, "x")), lambda))

        runQuery(setfn, db) should equal (setref)
        runQuery(setref, db) should equal (setref)

        //FIXME: does not equal itself in the expr language. probably due to positions being saved.
        pendingUntilFixed { qequals(setfn, setref, db) }
      }
    }

    prop("retrieves a page from the set") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        cls2 <- aCollection(db)
      } {

        runQuery(CreateIndex(MkObject(
          "name" -> "elements_by_hero",
          "source" -> cls.refObj,
          "active" -> true,
          "terms" -> JSArray(MkObject("field" -> JSArray("data", "name"))),
          "values" -> JSArray(MkObject("field" -> JSArray("data", "elements"))))), db)

        runQuery(CreateIndex(MkObject(
          "name" -> "heroes_with_element",
          "source" -> cls.refObj,
          "active" -> true,
          "values" -> JSArray(MkObject("field" -> JSArray("data", "elements"))))), db)

        runQuery(CreateIndex(MkObject(
          "name" -> "elements",
          "source" -> cls2.refObj,
          "active" -> true,
          "terms" -> JSArray(MkObject("field" -> JSArray("data", "name"))))), db)

        runQuery(CreateF(cls.refObj, MkObject(
          "data" -> MkObject(
            "elements" -> JSArray("fire", "water"),
            "name" -> "Zeus"))), db)

        runQuery(CreateF(cls.refObj, MkObject(
          "data" -> MkObject(
            "elements" -> JSArray("water", "wind"),
            "name" -> "Trident"))), db)

        val fire = runQuery(CreateF(cls2.refObj, MkObject(
          "data" -> MkObject(
            "name" -> "fire"))), db)

        val water = runQuery(CreateF(cls2.refObj, MkObject(
          "data" -> MkObject(
            "name" -> "water"))), db)

        val wind = runQuery(CreateF(cls2.refObj, MkObject(
          "data" -> MkObject(
            "name" -> "wind"))), db)

        val trident = Match(Ref("indexes/elements_by_hero"), "Trident")
        val zeus = Match(Ref("indexes/elements_by_hero"), "Zeus")
        val heroes = Match(Ref("indexes/heroes_with_element"), JSArray())

        val fireSource = Match(IdxRefV("elements"), "fire")
        val waterSource = Match(IdxRefV("elements"), "water")
        val windSource = Match(IdxRefV("elements"), "wind")

        val fireEvent = Event(SetRef(fireSource), fire.refObj, fire.ts, "add")
        val waterEvent = Event(SetRef(waterSource), water.refObj, water.ts, "add")
        val windEvent = Event(SetRef(windSource), wind.refObj, wind.ts, "add")

        val fireInst = Document.fromJS(fire.as[JSObject])
        val waterInst = Document.fromJS(water.as[JSObject])
        val windInst = Document.fromJS(wind.as[JSObject])

        val target = Lambda("elem" -> Match(Ref("indexes/elements"), Var("elem")))

        validateEvents(db, Events(Join(trident, target)), List(waterEvent, windEvent))
        validateEvents(db, Events(Join(zeus, target)), List(fireEvent, waterEvent))

        validateCollection(db, Join(trident, target), List(waterInst, windInst))
        validateCollection(db, Join(zeus, target), List(fireInst, waterInst))
        validateCollection(db, Join(heroes, target), List(fireInst, waterInst, windInst))

        // target may be closure
        val q = Let("i" -> Ref("indexes/elements"))(Join(heroes, Lambda("e" -> Match(Var("i"), Var("e")))))
        validateCollection(db, q, List(fireInst, waterInst, windInst))
      }
    }

    once("target lambda function is pure") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        runQuery(CreateIndex(MkObject(
          "name" -> "elements_by_hero_name",
          "source" -> cls.refObj,
          "terms" -> JSArray(MkObject("field" -> JSArray("data", "name"))),
          "values" -> JSArray(MkObject("field" -> JSArray("data", "element"))))), db)

        runQuery(CreateF(cls.refObj, MkObject(
          "data" -> MkObject(
            "element" -> "water",
            "name" -> "Trident"))), db)

        val trident = Match(Ref("indexes/elements_by_hero_name"), "Trident")
        val create = Lambda("elem" -> CreateIndex(MkObject("name" -> "spells", "source" -> cls.refObj)))
        val get = Lambda("elem" -> Get(cls.refObj))

        qassertErr(Paginate(Join(trident, create)), "invalid argument", JSArray("paginate", "with"), db)
        qassertErr(Paginate(Join(trident, get)),  "invalid argument", JSArray("paginate", "with"), db)
      }
    }

    once("works with index ref") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, termProp = Prop.const(Seq(JSArray("data", "foo"))), valueProp = Prop.const(Seq((JSArray("data", "bar"), false))))
        idx2 <- anIndex(cls, termProp = Prop.const(Seq(JSArray("data", "bar"))))
        inst1 <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> 1, "bar" -> 2)))
        inst2 <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> 1, "bar" -> 3)))
      } {
        val set = Match(idx.refObj, 1)
        collection(set, db) should equal (List(2, 3) map { JSLong(_) })
        val join = Join(set, idx2.refObj)
        val joinRef = SetRef(Join(SetRef(set), idx2.refObj))
        runQuery(join, db) should equal (joinRef)
        collection(join, db) should equal (List(inst1.refObj, inst2.refObj))

        qassertErr(Paginate(Join(set, cls.refObj)), "invalid argument", JSArray("paginate", "with"),  db)
        qassertErr(Paginate(Join(set, IdxRefV("foodoo"))), "invalid ref", JSArray("paginate", "with"),  db)
      }
    }

    once("join range with index") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx1 <- anIndex(cls, termProp = Prop.const(Seq(JSArray("data", "foo"))), valueProp = Prop.const(Seq((JSArray("data", "bar"), false))))
        _ <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> 1, "bar" -> 1)))
        _ <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> 1, "bar" -> 2)))
        _ <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> 1, "bar" -> 3)))
        _ <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> 1, "bar" -> 4)))
        _ <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> 1, "bar" -> 5)))
        _ <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> 2, "bar" -> 6)))
        _ <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> 2, "bar" -> 7)))
        _ <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> 2, "bar" -> 8)))
        _ <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> 2, "bar" -> 9)))
        _ <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> 2, "bar" -> 10)))
      } {
        val setIdx1 = Match(idx1.refObj, 1)
        collection(setIdx1, db) should equal (List(1, 2, 3, 4, 5) map { JSLong(_) })

        val range1 = RangeF(setIdx1, 2, 4)
        collection(range1, db) should equal (List(2, 3, 4) map { JSLong(_) })

        collection(Join(range1, idx1.refObj), db) should equal (List(6, 7, 8, 9, 10) map { JSLong(_) })
      }
    }

    prop("index events") {
      // FIXME: Historical joins just plain don't work like they need to
      pending

      for {
        db <- aDatabase
        peopleCls <- aCollection(db, nameProp = Prop.const("people"))
        postsCls <- aCollection(db, nameProp = Prop.const("posts"))
        byAuthorIdx <- anIndex(postsCls,
          Prop.const(Seq(JSArray("data", "author"))),
          nameProp = Prop.const("by_author"))

        followsCls <- aCollection(db, nameProp = Prop.const("follows"))
        byFollowerIdx <- anIndex(followsCls,
          Prop.const(Seq(JSArray("data", "follower"))),
          Prop.const(Seq(JSArray("data", "followee") -> false)),
          nameProp = Prop.const("by_follower"))

        alice <- aDocument(peopleCls)
        bob <- aDocument(peopleCls)

        nposts <- Prop.int(10 to 20)
        instP = aDocument(postsCls,
          dataProp = Prop.const(JSObject("author" -> alice.refObj)))
        posts <- instP * nposts

        ntimes <- Prop.int(1 to 10)
        times <- (Prop.choose(posts) map { _.ts }) * ntimes
      } {
        val timelineQ = Match(byAuthorIdx.refObj, alice.refObj)
        val graphQ = Match(byFollowerIdx.refObj, bob.refObj)
        val feedQ = Join(graphQ, byAuthorIdx.refObj)

        val data = MkObject(
          "data" -> MkObject(
            "follower" -> bob.refObj,
            "followee" -> alice.refObj))
        val ref = runQuery(MkRef(followsCls.refObj, NewID()), db.key)

        times foreach { ts =>
          val action = anAction.sample
          runQuery(InsertVers(ref, ts, action, data), db.key)
        }

        val timelineEvts = collectEvents(db, timelineQ, None, "before")
        val followEvts = collectEvents(db, graphQ, None, "before")

        validateEvents(db, feedQ, historicalJoin(followEvts, timelineEvts))
      }
    }
  }
}
