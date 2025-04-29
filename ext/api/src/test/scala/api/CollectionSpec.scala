package fauna.api.test

import fauna.codex.json._
import fauna.prop.Prop

class CollectionSpec extends API20Spec {
  "classes" / {
    once("works") {
      for {
        database <- aDatabase
        user     <- aUser(database)
        cls1     <- aCollection(database)
        cls2     <- aCollection(database)
      } {
        val set = api.get("/classes", database.key)
        set should respond(OK)

        (set.resource / "data") should containElem(cls1.resource / "ref")
        (set.resource / "data") should containElem(cls2.resource / "ref")

        val events = api.get("/classes/events", database.key)
        events should respond(OK)

        (events.resource / "data") should containEvent(
          "create",
          cls1.resource / "ref",
          cls1.ts)
        (events.resource / "data") should containEvent(
          "create",
          cls2.resource / "ref",
          cls2.ts)

        api.get("/classes", user.token) should respond(Forbidden)
        api.get("/classes/events", user.token) should respond(Forbidden)
      }
    }
  }

  "classes/{name}" / {
    once("GET works") {
      for {
        database <- aDatabase
        user     <- aUser(database)
        cls      <- aCollection(database)
      } {
        api.get(cls.path, user.token) should respond(Forbidden)
        api.get(s"${cls.path}/events", user.token) should respond(Forbidden)

        val res = api.get(cls.path, database.key)
        res should respond(OK)
        (res.resource / "ref" / "@ref").as[String] should startWith("classes")
      }
    }

    once("PUT works") {
      for {
        database <- aDatabase
        user     <- aUser(database)
      } {
        api.post(
          "/classes",
          JSObject("name" -> "magical_creatures"),
          database.key) should respond(Created)

        val res = api.post(
          "/classes/magical_creatures",
          JSObject("data" -> JSObject("kinds" -> JSArray("Pigs"))),
          database.key)
        res should respond(Created)

        (res.resource / "ref" / "@ref").as[String] should startWith(
          "classes/magical_creatures")
        (res.resource / "data" / "kinds").as[Seq[String]] should equal(Seq("Pigs"))

        api.post("/classes", JSObject("name" -> "foo"), user.token) should respond(
          Forbidden)
      }
    }

    once("can rename a class via PUT") {
      for {
        database <- aDatabase
      } {
        api.post(
          "/classes",
          JSObject("name" -> "old_hermit_huts"),
          database.key) should respond(Created)
        api.get("/classes/old_hermit_huts", database.key) should respond(OK)
        val res1 = api.put(
          "/classes/old_hermit_huts",
          JSObject("name" -> "decrepit_huts"),
          database.key)
        res1 should respond(OK)
        (res1.resource / "ref") should equal(
          JSObject("@ref" -> "classes/decrepit_huts"))
        (res1.resource / "name") should equal(JSString("decrepit_huts"))

        // new name should be immediately available
        api.get("/classes/decrepit_huts", database.key) should respond(OK)

        beforeTTLCacheExpiration {
          // old name is eventually no longer available
          api.get("/classes/old_hermit_huts", database.key) should respond(NotFound)
        }

        // can rename back within cache window
        val res2 = api.patch(
          "/classes/decrepit_huts",
          JSObject("name" -> "old_hermit_huts"),
          database.key)
        res2 should respond(OK)
        (res2.resource / "ref") should equal(
          JSObject("@ref" -> "classes/old_hermit_huts"))
        (res2.resource / "name") should equal(JSString("old_hermit_huts"))

        // old name is available again
        api.get("/classes/old_hermit_huts", database.key) should respond(OK)

        beforeTTLCacheExpiration {
          // new name is eventually no longer available
          api.get("/classes/decrepit_huts", database.key) should respond(NotFound)
        }

        val res3 = api.patch(
          "/classes/old_hermit_huts",
          JSObject("name" -> "decrepit_huts"),
          database.key)
        res3 should respond(OK)
        (res3.resource / "ref") should equal(
          JSObject("@ref" -> "classes/decrepit_huts"))
        (res3.resource / "name") should equal(JSString("decrepit_huts"))

        // new-new name should be immediately available
        api.get("/classes/decrepit_huts", database.key) should respond(OK)

        beforeTTLCacheExpiration {
          // old name is eventually no longer available again
          api.get("/classes/old_hermit_huts", database.key) should respond(NotFound)
        }
      }
    }

    once("classes can be recreated within cache window") {
      for {
        database <- aDatabase
      } {
        api.post(
          "/classes",
          JSObject("name" -> "items"),
          database.key) should respond(Created)

        // make sure the class gets cached
        api.post(
          "/classes",
          JSObject("name" -> "items"),
          database.key) should respond(BadRequest)
        val res = api.delete("/classes/items", database.key)
        res should respond(OK)

        api.post(
          "/classes",
          JSObject("name" -> "items"),
          database.key) should respond(Created)
      }
    }

    once("repeat puts result in OK") {
      for {
        database <- aDatabase
      } {
        api.post(
          "/classes",
          JSObject("name" -> "magic_vessels"),
          database.key) should respond(Created)
        api.patch(
          "/classes/magic_vessels",
          JSObject("name" -> "magic_vessels"),
          database.key) should respond(OK)
      }
    }

    once("root key can create a class") {
      for {
        name <- aUniqueName
      } {
        api.post("/classes", JSObject("name" -> name), rootKey) should respond(
          Created)
      }
    }

    once("classes get the default retention policy if not specified") {
      for {
        database <- aDatabase
      } {
        val cls =
          api.post("/classes", JSObject("name" -> "foo"), database.key).resource
        (cls / "history_days").as[Long] should equal(0)
      }
    }

    once("PATCH works") {
      for {
        database <- aDatabase
        user     <- aUser(database)
      } {
        api.post(
          "/classes",
          JSObject("name" -> "magical_creatures"),
          database.key) should respond(Created)

        val res = api.patch(
          "/classes/magical_creatures",
          JSObject("data" -> JSObject("kinds" -> JSArray("Pigs"))),
          database.key)
        res should respond(OK)

        (res.resource / "ref" / "@ref").as[String] should startWith("classes")
        (res.resource / "data" / "kinds").as[Seq[String]] should equal(Seq("Pigs"))

        api.post("/classes", JSObject("name" -> "foo"), database.key) should respond(
          Created)
        api.patch("/classes/foo", JSObject.empty, user.token) should respond(
          Forbidden)
      }
    }

    once("can rename a class via PATCH") {
      for {
        database <- aDatabase
      } {
        api.post(
          "/classes",
          JSObject("name" -> "old_hermit_huts"),
          database.key) should respond(Created)
        // Ensure cache gets loaded
        api.get("/classes/old_hermit_huts", database.key) should respond(OK)

        val res1 = api.patch(
          "/classes/old_hermit_huts",
          JSObject("name" -> "decrepit_huts"),
          database.key)
        res1 should respond(OK)
        (res1.resource / "ref") should equal(
          JSObject("@ref" -> "classes/decrepit_huts"))
        (res1.resource / "name") should equal(JSString("decrepit_huts"))

        // new name should be immediately available
        api.get("/classes/decrepit_huts", database.key) should respond(OK)

        beforeTTLCacheExpiration {
          // old name is eventually no longer available
          api.get("/classes/old_hermit_huts", database.key) should respond(NotFound)
        }

        // can rename back within cache window
        val res2 = api.patch(
          "/classes/decrepit_huts",
          JSObject("name" -> "old_hermit_huts"),
          database.key)
        res2 should respond(OK)
        (res2.resource / "ref") should equal(
          JSObject("@ref" -> "classes/old_hermit_huts"))
        (res2.resource / "name") should equal(JSString("old_hermit_huts"))

        // old name is available again
        api.get("/classes/old_hermit_huts", database.key) should respond(OK)

        beforeTTLCacheExpiration {
          // new name is no longer available
          api.get("/classes/decrepit_huts", database.key) should respond(NotFound)
        }

        val res3 = api.patch(
          "/classes/old_hermit_huts",
          JSObject("name" -> "decrepit_huts"),
          database.key)
        res3 should respond(OK)
        (res3.resource / "ref") should equal(
          JSObject("@ref" -> "classes/decrepit_huts"))
        (res3.resource / "name") should equal(JSString("decrepit_huts"))

        // new-new name should be immediately available
        api.get("/classes/decrepit_huts", database.key) should respond(OK)

        beforeTTLCacheExpiration {
          // old name is eventually no longer available again
          api.get("/classes/old_hermit_huts", database.key) should respond(NotFound)
        }
      }
    }

    once("DELETE works") {
      for {
        database <- aDatabase
        cls      <- aCollection(database)
      } {
        api.delete(cls.path, database.key) should respond(OK)
        api.get(cls.path, database.key) should respond(NotFound)
      }
    }

    once("resetting a class clears all data") {
      for {
        database <- aDatabase
      } {
        val cls = JSObject("name" -> "foo")

        val idx =
          JSObject(
            "name" -> "foo_by_bar",
            "terms" -> JSArray(JSObject("field" -> List("data", "bar"))),
            "source" -> JSObject("@ref" -> "classes/foo"),
            "active" -> true)

        val inst = JSObject("data" -> JSObject("bar" -> "x"))
        var instanceRefs: List[String] = Nil

        def createClass(): Unit = {
          val res = api.query(
            JSObject(
              "create" -> JSObject("@ref" -> "classes"),
              "params" -> JSObject("quote" -> cls)),
            database.key)

          res should respond(Created)
          api
            .query(
              JSObject("get" -> JSObject("@ref" -> "classes/foo")),
              database.key)
            .body
        }

        def createInstance(): Unit = {
          val res = api.query(
            JSObject(
              "create" -> JSObject("@ref" -> "classes/foo"),
              "params" -> JSObject("quote" -> inst)),
            database.key)

          instanceRefs = (res.resource / "ref" / "@ref").as[String] :: instanceRefs
        }

        def deleteClass(): Unit = {
          val res =
            api.query(
              JSObject("delete" -> JSObject("@ref" -> "classes/foo")),
              database.key)

          res should respond(OK)
        }

        def createIndex(): Unit = {
          val res = api.query(
            JSObject(
              "create" -> JSObject("@ref" -> "indexes"),
              "params" -> JSObject("quote" -> idx)),
            database.key)

          res should respond(Created)
          api
            .query(
              JSObject("get" -> JSObject("@ref" -> "indexes/foo_by_bar")),
              database.key)
            .body
        }

        def cantDeleteIndex(): Unit = {
          val res =
            api.query(
              JSObject("delete" -> JSObject("@ref" -> "indexes/foo_by_bar")),
              database.key)

          res should respond(BadRequest, NotFound)
        }

        for (_ <- 1 to 5) {
          createClass()
          waitForTaskExecution()
          createIndex()

          for (ref <- instanceRefs)
            api.get(s"/$ref", database.key) should respond(NotFound)

          val q = JSObject(
            "paginate" -> JSObject(
              "match" -> "x",
              "index" -> JSObject("@ref" -> "indexes/foo_by_bar")))

          (api.query(q, database.key).resource / "data")
            .as[Seq[JSValue]]
            .size should equal(0)

          for (_ <- 1 to 10) createInstance()

          (api.query(q, database.key).resource / "data")
            .as[Seq[JSValue]]
            .size should equal(10)

          // kill the class
          deleteClass()
          cantDeleteIndex()
          waitForTaskExecution()
        }
      }
    }

    once(
      "CreateCollection api doesn't splode when passed additional values besides name") {
      for {
        db  <- aDatabase
        col <- aCollection(db)
        idx <- anIndex(col, Prop.const(Seq(JSArray("data", "foo"))))
      } {
        val extras = List(
          MkObject("another" -> "object"),
          JSArray(Seq(MkObject("nonempty" -> "array"))),
          Events(Match(idx.refObj, "foo"))
        )

        List
          .tabulate(extras.size) { i => "name" -> s"colName$i" }
          .zip(extras)
          .map { case (name, extra) =>
            api.query(
              CreateClass(MkObject(name, "extra" -> extra)),
              db.key) should respond(Created)
          }
      }
    }
  }

  // This behavior is still allowed before V5. See CollectionSpec5 below.
  once("can create a collection and document in same query") {
    for {
      db <- aDatabase
    } {
      val res = api.query(
        Let(
          "car" -> Select(
            JSArray("ref"),
            CreateClass(MkObject("name" -> "eng2679")))) {
          CreateF(Var("car"), MkObject("data" -> MkObject("plate" -> "ACME")))
        },
        db.key)

      res should respond(Created)
    }
  }
}

class CollectionSpec27 extends API27Spec {
  "classes" / {
    once("classes cannot be created in a container") {
      for {
        container <- aContainerDB
      } {
        api.post(
          "/classes",
          JSObject("name" -> "items"),
          container.key) should respond(BadRequest)
      }
    }
  }
}

class CollectionSpec5 extends API5Spec {
  once("can't create collection and document in same query") {
    for {
      db <- aDatabase
    } {
      api.query(
        Let(
          "car" -> Select(
            JSArray("ref"),
            CreateClass(MkObject("name" -> "eng2679")))) {
          CreateF(Var("car"), MkObject("data" -> MkObject("plate" -> "ACME")))
        },
        db.key) should respond(BadRequest)
    }
  }
}
