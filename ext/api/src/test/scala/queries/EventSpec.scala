package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.prop.Prop

class EventV20Spec extends QueryAPI20Spec {
  "set events" / {
    once("representation") {
      for {
        db <- aDatabase
        cls <- aFaunaClass(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "foo"))))
      } {
        val inst = runQuery(CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "bar"))), db)
        val ref = inst.refObj
        runQuery(Update(ref, MkObject("data" -> MkObject("foo" -> "baz"))), db)

        // do not render the root ref
        val cursor = runQuery(Paginate(Match(idx.refObj, "bar"), cursor = Before(123456789), events = true), db)
        (cursor / "after" / "ts").as[Long] should be (123456789)
        (cursor / "after" / "resource").isEmpty should be(true)

        val page = runQuery(Paginate(Match(idx.refObj, "bar"), events = true), db)

        val data = (page / "data").as[JSArray]
        data.value.size should equal (2)

        (data / 0 / "action") should equal (JSString("create"))
        (data / 0 / "resource") should equal (ref)

        (data / 1 / "action") should equal (JSString("delete"))
        (data / 1 / "resource") should equal (ref)

        val afterCursor = runQuery(Paginate(Match(idx.refObj, "bar"), cursor = After(inst / "ts"), events = true), db)
        val afterData = (afterCursor / "data").as[JSArray]

        afterData.value.length should equal (2)

        (afterData / 0 / "action") should equal (JSString("create"))
        (afterData / 0 / "resource") should equal (ref)

        (afterData / 1 / "action") should equal (JSString("delete"))
        (afterData / 1 / "resource") should equal (ref)
      }
    }
  }

  "instance events" / {
    once("representation") {
      for {
        db <- aDatabase
        cls <- aFaunaClass(db)
      } {
        val inst = runQuery(CreateF(Ref(cls.ref), MkObject("data" -> MkObject("foo" -> "bar"))), db)
        val ref = inst.refObj

        runQuery(Update(ref, MkObject("data" -> MkObject("foo" -> "baz"))), db)
        runQuery(DeleteF(ref), db)

        // do not render the root ref
        val cursor = runQuery(Paginate(ref, cursor = Before(123456789), events = true), db)
        (cursor / "after" / "ts").as[Long] should be (123456789)
        (cursor / "after" / "resource").isEmpty should be(true)

        val page = runQuery(Paginate(ref, events = true), db)

        val data = (page / "data").as[JSArray]
        data.value.size should equal (3)

        (data / 0 / "action") should equal (JSString("create"))
        (data / 0 / "resource") should equal (ref)

        (data / 1 / "action") should equal (JSString("create"))
        (data / 1 / "resource") should equal (ref)

        (data / 2 / "action") should equal (JSString("delete"))
        (data / 2 / "resource") should equal (ref)

        val pageAfter = runQuery(Paginate(ref, events = true, cursor = After(inst.ts)), db)
        val dataAfter = (pageAfter / "data").as[JSArray]

        dataAfter.value.length should equal (3)

        (dataAfter / 0 / "action") should equal (JSString("create"))
        (dataAfter / 0 / "resource") should equal (ref)

        (dataAfter / 1 / "action") should equal (JSString("create"))
        (dataAfter / 1 / "resource") should equal (ref)

        (dataAfter / 2 / "action") should equal (JSString("delete"))
        (dataAfter / 2 / "resource") should equal (ref)

        // Non-paginated events representation - asking for the events of any ref should yield a set
        runQuery(Events(ref), db) should equal(SetRef(Events(ref)))
      }
    }
  }

  "singleton instance events" / {
    once("representation") {
      for {
        db <- aDatabase
        cls <- aFaunaClass(db)
      } {
        val inst = runQuery(CreateF(Ref(cls.ref), MkObject("data" -> MkObject("foo" -> "bar"))), db)
        val ref = inst.refObj

        runQuery(Update(ref, MkObject("data" -> MkObject("foo" -> "baz"))), db)
        runQuery(DeleteF(ref), db)

        // do not render the root ref
        val cursor = runQuery(Paginate(Singleton(ref), cursor = Before(123456789), events = true), db)
        (cursor / "after" / "ts").as[Long] should be (123456789)
        (cursor / "after" / "resource").isEmpty should be(true)

        val page = runQuery(Paginate(Singleton(ref), events = true), db)

        val data = (page / "data").as[JSArray]
        data.value.size should equal (2)

        (data / 0 / "action") should equal (JSString("create"))
        (data / 0 / "resource") should equal (ref)

        (data / 1 / "action") should equal (JSString("delete"))
        (data / 1 / "resource") should equal (ref)

        val pageAfter = runQuery(Paginate(Singleton(ref), events = true, cursor = After(inst.ts)), db)
        val dataAfter = (pageAfter / "data").as[JSArray]

        dataAfter.value.size should equal (2)

        (dataAfter / 0 / "action") should equal (JSString("create"))
        (dataAfter / 0 / "resource") should equal (ref)

        (dataAfter / 1 / "action") should equal (JSString("delete"))
        (dataAfter / 1 / "resource") should equal (ref)
      }
    }
  }
}

class EventV21Spec extends QueryAPI21Spec {
  "combinations" / {
    once("cannot be combined with snapshot sets") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        val ref = runQuery(CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "bar"))), db).refObj
        qassertErr(Paginate(Union(Events(ref), Singleton(ref))), "invalid argument", JSArray("paginate", "union"), db)
      }
    }

    once("cannot nest event queries") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        val ref = runQuery(CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "bar"))), db).refObj
        qassertErr(Paginate(Events(Events(ref))), "invalid argument", JSArray("paginate"), db)
      }
    }

    once("can be combined with other historical sets") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "foo"))))
      } {
        val ref = runQuery(CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "bar"))), db).refObj
        val page = runQuery(Paginate(Union(Events(Match(idx.refObj, "bar")), Events(ref))), db)

        val data = (page / "data").as[JSArray]
        data.value.size should equal (2)

        (data / 0 / "action") should equal (JSString("add"))
        (data / 0 / "instance") should equal (ref)

        (data / 1 / "action") should equal (JSString("create"))
        (data / 1 / "instance") should equal (ref)
        (data / 1 / "data") should equal (JSObject("foo" -> "bar"))
      }
    }
  }

  "set events" / {
    once("representation") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "foo"))))
      } {
        val inst = runQuery(CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "bar"))), db)
        val ref = inst.refObj
        runQuery(Update(ref, MkObject("data" -> MkObject("foo" -> "baz"))), db)

        // Events should be a @set
        val evs = Events(Match(idx.refObj, "bar"))
        runQuery(evs, db) / "@set" should matchPattern { case e if e == evs => }

        // do not render the root ref
        val cursor = runQuery(Paginate(evs, cursor = Before(123456789)), db)
        (cursor / "after" / "ts").as[Long] should be (123456789)
        (cursor / "after" / "instance").isEmpty should be(true)

        val page = runQuery(Paginate(evs), db)

        val data = (page / "data").as[JSArray]
        data.value.size should equal (2)

        (data / 0 / "action") should equal (JSString("add"))
        (data / 0 / "instance") should equal (ref)

        (data / 1 / "action") should equal (JSString("remove"))
        (data / 1 / "instance") should equal (ref)

        val afterCursor = runQuery(Paginate(Events(Match(idx.refObj, "bar")), cursor = After(inst.ts)), db)
        val afterData = (afterCursor / "data").as[JSArray]

        afterData.value.length should equal (2)

        (afterData / 0 / "action") should equal (JSString("add"))
        (afterData / 0 / "instance") should equal (ref)

        (afterData / 1 / "action") should equal (JSString("remove"))
        (afterData / 1 / "instance") should equal (ref)
      }
    }
  }

  "singleton instance events" / {
    once("representation") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        val res = runQuery(CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "bar", "buzz" -> "bing"))), db)
        val addTs = res.ts
        val ref = res.refObj

        runQuery(Update(ref, MkObject("data" -> MkObject("foo" -> "baz"))), db)
        runQuery(DeleteF(ref), db)

        // Events should be a @set
        val evs = Events(Singleton((ref)))
        runQuery(evs, db) / "@set" should matchPattern { case e if e == evs => }

        val page = runQuery(Paginate(Events(Singleton(ref))), db)

        val data = (page / "data").as[JSArray]
        data.value.size should equal (2)

        (data / 0 / "action") should equal (JSString("add"))
        (data / 0 / "instance") should equal (ref)

        // ensure the "add" we see is the create and not the update
        (data / 0 / "ts").as[Long] should equal (addTs)

        (data / 1 / "action") should equal (JSString("remove"))
        (data / 1 / "instance") should equal (ref)

        val pageAfter = runQuery(Paginate(Events(Singleton(ref)), cursor = After(addTs)), db)
        val dataAfter = (pageAfter / "data").as[JSArray]

        dataAfter.value.size should equal (2)

        (dataAfter / 0 / "action") should equal (JSString("add"))
        (dataAfter / 0 / "instance") should equal (ref)

        (dataAfter / 1 / "action") should equal (JSString("remove"))
        (dataAfter / 1 / "instance") should equal (ref)
      }
    }
  }

  "instance events" / {
    once("representation") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        val inst = runQuery(CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "bar", "buzz" -> "bing"))), db)
        val ref = inst.refObj

        runQuery(Update(ref, MkObject("data" -> MkObject("foo" -> "baz"))), db)
        runQuery(DeleteF(ref), db)

        // Events should be a @set
        val evs = Events(ref)
        runQuery(evs, db) / "@set" should matchPattern { case e if e == evs => }

        // do not render the diff in the cursor
        val smallPage = runQuery(Paginate(Events(ref), size = 1), db)
        (smallPage / "before" / "data").isEmpty should be(true)
        (smallPage / "after" / "data").isEmpty should be(true)

        // do not render the root ref
        val cursor = runQuery(Paginate(Events(ref), cursor = Before(123456789)), db)
        (cursor / "after" / "ts").as[Long] should be (123456789)
        (cursor / "after" / "instance").isEmpty should be(true)

        val page = runQuery(Paginate(Events(ref)), db)

        val data = (page / "data").as[JSArray]
        data.value.size should equal (3)

        (data / 0 / "action") should equal (JSString("create"))
        (data / 0 / "instance") should equal (ref)
        (data / 0 / "data") should equal (JSObject("foo" -> "bar", "buzz" -> "bing"))

        (data / 1 / "action") should equal (JSString("update"))
        (data / 1 / "instance") should equal (ref)
        (data / 1 / "data") should equal (JSObject("foo" -> "baz"))

        (data / 2 / "action") should equal (JSString("delete"))
        (data / 2 / "instance") should equal (ref)
        (data / 2 / "data") should equal (JSNull)

        val pageAfter = runQuery(Paginate(Events(ref), cursor = After(inst.ts)), db)
        val dataAfter = (pageAfter / "data").as[JSArray]

        dataAfter.value.size should equal (3)

        (dataAfter / 0 / "action") should equal (JSString("create"))
        (dataAfter / 0 / "instance") should equal (ref)
        (dataAfter / 0 / "data") should equal (JSObject("foo" -> "bar", "buzz" -> "bing"))

        (dataAfter / 1 / "action") should equal (JSString("update"))
        (dataAfter / 1 / "instance") should equal (ref)
        (dataAfter / 1 / "data") should equal (JSObject("foo" -> "baz"))

        (dataAfter / 2 / "action") should equal (JSString("delete"))
        (dataAfter / 2 / "instance") should equal (ref)
        (dataAfter / 2 / "data") should equal (JSNull)
      }
    }
  }
}
