package fauna.api.test

import fauna.codex.json._
import fauna.prop.api.{ Membership, Privilege, RoleAction }

class IndexSpec extends API20Spec {
  "indexes" / {
    once("GET works") {
      for {
        database <- aDatabase
        user <- aUser(database)
        cls <- aCollection(database)
      } {
        val idx1 = api
          .post("/indexes",
                JSObject("name" -> "creatures_by_name",
                         "source" -> JSObject("@ref" -> cls.ref),
                         "field" -> List("data", "name"),
                         "active" -> true),
                database.key)
          .resource

        val idx2 = api
          .post("/indexes",
                JSObject("name" -> "creatures_by_kind",
                         "source" -> JSObject("@ref" -> cls.ref),
                         "field" -> List("data", "kind"),
                         "active" -> true),
                database.key)
          .resource

        val idx3 = api
          .post("/indexes",
                JSObject("name" -> "creatures_by_element",
                         "source" -> JSObject("@ref" -> cls.ref),
                         "field" -> List("data", "element"),
                         "active" -> true),
                database.key)
          .resource

        val set = api.get("/indexes", database.key)
        set should respond(OK)

        (set.resource / "data") should containElem(JSObject("@ref" -> idx1.ref))
        (set.resource / "data") should containElem(JSObject("@ref" -> idx2.ref))
        (set.resource / "data") should containElem(JSObject("@ref" -> idx3.ref))

        api.get("/indexes", user.token) should respond(Forbidden)
      }
    }
  }

  "indexes/{name}" / {
    once("GET works") {
      for {
        database <- aDatabase
        cls <- aCollection(database)
      } {
        val res = api.post("/indexes",
                           JSObject("name" -> "creatures_by_name",
                                    "source" -> JSObject("@ref" -> cls.ref),
                                    "field" -> List("data", "name"),
                                    "active" -> true),
                           database.key)

        res should respond(Created)

        api.get("/indexes/creatures_by_name", database.key).resource should equal(
          res.resource)
      }
    }

    once("validates terms / path ") {
      for {
        database <- aDatabase
        cls <- aCollection(database)
      } {
        val err1 = api.post(
          "/indexes",
          JSObject(
            "name" -> "cls_index1",
            "source" -> JSObject("@ref" -> cls.ref),
            "terms" -> JSArray(
              JSObject("object" -> JSObject("field" -> List("data", "name")))),
            "active" -> true),
          database.key
        )

        err1 should respond(BadRequest)
        (err1.json / "errors" / 0 / "code").as[String] should equal(
          "validation failed")
        (err1.json / "errors" / 0 / "failures" / 0 / "field" / 0)
          .as[String] should equal("field")
        (err1.json / "errors" / 0 / "failures" / 0 / "code").as[String] should equal(
          "value required")

        val err2 = api.post(
          "/indexes",
          JSObject(
            "name" -> "cls_index2",
            "source" -> JSObject("@ref" -> cls.ref),
            "terms" -> JSArray(JSObject("field" -> JSObject("data" -> "name"))),
            "active" -> true),
          database.key
        )

        err2 should respond(BadRequest)
        (err2.json / "errors" / 0 / "code").as[String] should equal(
          "validation failed")
        (err2.json / "errors" / 0 / "failures" / 0 / "field" / 0)
          .as[String] should equal("field")
        (err2.json / "errors" / 0 / "failures" / 0 / "code").as[String] should equal(
          "invalid type")

        // mask away extraneous data in terms and values
        val req3 = JSObject(
          "name" -> "cls_index3",
          "source" -> JSObject("@ref" -> cls.ref),
          "terms" -> JSArray(JSObject("field" -> List("data", "name"), "foo" -> 2)),
          "values" -> JSArray(
            JSObject("bar" -> "baz", "field" -> "ref", "reverse" -> true),
            JSObject("field" -> "class", "transform" -> "casefold", "foo" -> "bar")),
          "active" -> true
        )
        val res3 = api.post("/indexes", req3, database.key)

        res3 should respond(Created)
        (res3.json / "resource" / "terms" / 0) should equal(
          JSObject("field" -> JSArray("data", "name")))
        (res3.json / "resource" / "values" / 0) should equal(
          JSObject("field" -> "ref", "reverse" -> true))
        (res3.json / "resource" / "values" / 1) should equal(
          JSObject("field" -> "class", "transform" -> "casefold"))
      }
    }

    once("validates terms / field ") {
      for {
        database <- aDatabase
        cls <- aCollection(database)
      } {
        val res1 = api.post(
          "/indexes",
          JSObject("name" -> "cls_index1",
                   "source" -> JSObject("@ref" -> cls.ref),
                   "terms" -> JSArray(JSObject("field" -> JSArray("data", "name"))),
                   "active" -> true),
          database.key
        )

        res1 should respond(Created)
        (res1.resource / "terms" / 0 / "field") should equal(JSArray("data", "name"))

        val err2 = api.post(
          "/indexes",
          JSObject(
            "name" -> "cls_index2",
            "source" -> JSObject("@ref" -> cls.ref),
            "terms" -> JSArray(JSObject("field" -> false)),
            "active" -> true),
          database.key
        )

        err2 should respond(BadRequest)
        (err2.json / "errors" / 0 / "code").as[String] should equal(
          "validation failed")
        (err2.json / "errors" / 0 / "failures" / 0 / "field" / 0)
          .as[String] should equal("field")
        (err2.json / "errors" / 0 / "failures" / 0 / "code").as[String] should equal(
          "invalid type")
      }
    }

    once("Index config is not accessible by users") {
      for {
        database <- aDatabase
        user <- aUser(database)
        cls <- aCollection(database)
      } {
        val args = JSObject("name" -> "items_by_name",
                            "source" -> JSObject("@ref" -> cls.ref),
                            "field" -> List("data", "name"),
                            "active" -> true)

        api.post("/indexes", args, database.key) should respond(Created)
        api.get("/indexes/items_by_name", database.key) should respond(OK)
        api.get("/indexes/items_by_name", user.token) should respond(Forbidden)
      }
    }

    once("PUT works") {
      for {
        database <- aDatabase
        user <- aUser(database)
        cls <- aCollection(database)
      } {
        val res = api.post(
          "/indexes",
          JSObject("name" -> "creatures_by_name",
                   "source" -> JSObject("@ref" -> cls.ref),
                   "field" -> List("data", "name"),
                   "active" -> true,
                   "unique" -> false),
          database.key
        )

        res should respond(Created)
        (res.resource / "name").as[String] should equal("creatures_by_name")

        val res2 =
          api.post("/indexes", JSObject("name" -> "creatures_by_size"), user.token)
        res2 should respond(Forbidden)
      }
    }

    test("root key can create an index") {
      api.post("/indexes", JSObject("source" -> "_", "name" -> "creatures_by_size"), rootKey) should respond(
        Created)

    }

    once("index can be named `users`") {
      for {
        database <- aDatabase
        cls <- aCollection(database)
      } {
        val res1 = api.post("/indexes",
                            JSObject("name" -> "users",
                                     "source" -> JSObject("@ref" -> cls.ref),
                                     "field" -> "class",
                                     "active" -> true),
                            database.key)

        res1 should respond(Created)
        (res1.resource / "name").as[String] should equal("users")

        val res2 = api.get("/indexes/users", database.key)
        res2 should respond(OK)

        (res2.resource / "name").as[String] should equal("users")
      }
    }

    once("index can be renamed `users`") {
      for {
        database <- aDatabase
        cls <- aCollection(database)
      } {
        val res1 = api.post("/indexes",
                            JSObject("name" -> "users_idx",
                                     "source" -> JSObject("@ref" -> cls.ref),
                                     "field" -> "class",
                                     "active" -> true),
                            database.key)

        res1 should respond(Created)

        (res1.resource / "name").as[String] should equal("users_idx")

        val res2 = api.query(
          JSObject("update" -> JSObject("@ref" -> "indexes/users_idx"),
                   "params" -> JSObject("quote" -> JSObject("name" -> "users"))),
          database.key)

        res2 should respond(OK)
        (res2.resource / "name").as[String] should equal("users")
      }
    }

    once("index name cannot be duplicated") {
      for {
        database <- aDatabase
        cls <- aCollection(database)
      } {
        val res = api.post(
          "/indexes",
          JSObject("name" -> "cls_by_name",
                   "name" -> "cls_by_name",
                   "source" -> JSObject("@ref" -> cls.ref),
                   "field" -> List("data", "name"),
                   "active" -> true),
          database.key
        )

        res should respond(Created)

        val names = res.resource.value collect { case ("name", v) => v.as[String] }
        names should equal(Seq("cls_by_name"))
      }
    }

    once("can rename an index") {
      for {
        database <- aDatabase
        cls <- aCollection(database)
      } {
        api.post("/indexes",
                 JSObject("name" -> "cls_by_name",
                          "source" -> JSObject("@ref" -> cls.ref),
                          "field" -> List("data", "name"),
                          "active" -> true),
                 database.key) should respond(Created)

        api.get("/indexes/cls_by_name", database.key) should respond(OK)

        val res1 = api.patch("/indexes/cls_by_name",
                             JSObject("name" -> "creatures_by_name"),
                             database.key)
        res1 should respond(OK)
        (res1.resource / "ref") should equal(
          JSObject("@ref" -> "indexes/creatures_by_name"))
        (res1.resource / "name") should equal(JSString("creatures_by_name"))

        // new name should be immediately available
        api.get("/indexes/creatures_by_name", database.key) should respond(OK)

        beforeTTLCacheExpiration {
          // old name is no longer available
          api.get("/indexes/cls_by_name", database.key) should respond(NotFound)
        }
      }
    }

    once("indexes can be recreated within cache window") {
      for {
        database <- aDatabase
        cls <- aCollection(database)
      } {
        val args = JSObject("name" -> "items_by_name",
                            "source" -> JSObject("@ref" -> cls.ref),
                            "field" -> List("data", "name"),
                            "active" -> true)

        api.post("/indexes", args, database.key) should respond(Created)

        // make sure the index gets cached
        val q = JSObject(
          "paginate" -> JSObject(
            "match" -> "foo",
            "index" -> JSObject("@ref" -> "indexes/items_by_name")))

        api.query(q, database.key) should respond(OK)
        api.delete("/indexes/items_by_name", database.key) should respond(OK)
        api.get("/indexes/items_by_name", database.key) should respond(NotFound)
        api.post("/indexes", args, database.key) should respond(Created)
      }
    }

    once("indexes cannot be read by users without permission") {
      for {
        database <- aDatabase
        user <- aUser(database)
        cls <- aCollection(database)
      } {
        val args = JSObject("name" -> "items_by_name",
                            "source" -> JSObject("@ref" -> cls.ref),
                            "field" -> List("data", "name"),
                            "active" -> true)

        api.post("/indexes", args, database.key) should respond(Created)
        val idxRef = JSObject("@ref" -> "indexes/items_by_name")

        // make sure the index gets cached

        val query =
          JSObject("paginate" -> JSObject("match" -> "foo", "index" -> idxRef))

        api.query(query, database.key) should respond(OK)
        api.query(query, database.clientKey) should respond(Forbidden)
        api.query(query, user.token) should respond(Forbidden)

        aRole(
          database,
          Membership(user.collection),
          Privilege(idxRef, read = RoleAction.Granted)).sample

        api.query(query, user.token) should respond(OK)
      }
    }

    once("single-term indexes elide null values") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        api.query(
          JSObject(
            "create" -> JSObject("@ref" -> "indexes"),
            "params" -> JSObject(
              "object" -> JSObject(
                "name" -> "p1",
                "source" -> JSObject("@ref" -> cls.ref),
                "unique" -> true,
                "active" -> true,
                "terms" -> JSArray(
                  JSObject("object" -> JSObject("field" -> JSArray("data", "p1"))))))
          ),
          db.key
        ) should respond(Created)

        api.query(
          JSObject(
            "create" -> JSObject("@ref" -> cls.ref),
            "params" -> JSObject(
              "object" -> JSObject("data" -> JSObject("object" -> JSObject(
                "p1" -> JSObject("object" -> JSObject("x" -> "y"))))))
          ),
          db.key
        ) should respond(Created)

        val res = api.query(
          JSObject(
            "paginate" -> JSObject("match" -> JSObject("@ref" -> "indexes/p1"),
                                   "terms" -> JSNull)),
          db.key)

        res should respond(OK)
        (res.resource / "data").isEmpty should be(true)
      }
    }

    once("multi-term indexes include null values") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        api.query(
          JSObject(
            "create" -> JSObject("@ref" -> "indexes"),
            "params" -> JSObject("object" -> JSObject(
              "name" -> "compound",
              "source" -> JSObject("@ref" -> cls.ref),
              "unique" -> true,
              "active" -> true,
              "terms" -> JSArray(
                JSObject("object" -> JSObject("field" -> JSArray("data", "p1"))),
                JSObject("object" -> JSObject("field" -> JSArray("data", "p2"))))
            ))
          ),
          db.key
        ) should respond(Created)

        api.query(
          JSObject(
            "create" -> JSObject("@ref" -> cls.ref),
            "params" -> JSObject(
              "object" -> JSObject(
                "data" -> JSObject("object" -> JSObject(
                  "p1" -> JSArray(JSObject("object" -> JSObject("x" -> "y")),
                                  JSLong(1)),
                  "p2" -> JSLong(2)))))
          ),
          db.key
        ) should respond(Created)

        val res = api.query(
          JSObject(
            "paginate" -> JSObject("match" -> JSObject("@ref" -> "indexes/compound"),
                                   "terms" -> JSArray(JSNull, JSLong(2)))),
          db.key)

        res should respond(OK)
        (res.resource / "data").isEmpty should be(false)
      }
    }

    once("multi-term indexes elide entirely null values") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        api.query(
          JSObject(
            "create" -> JSObject("@ref" -> "indexes"),
            "params" -> JSObject("object" -> JSObject(
              "name" -> "compound",
              "source" -> JSObject("@ref" -> cls.ref),
              "unique" -> true,
              "active" -> true,
              "terms" -> JSArray(
                JSObject("object" -> JSObject("field" -> JSArray("data", "p1"))),
                JSObject("object" -> JSObject("field" -> JSArray("data", "p2"))))
            ))
          ),
          db.key
        ) should respond(Created)

        api.trace(
          JSObject(
            "create" -> JSObject("@ref" -> cls.ref),
            "params" -> JSObject(
              "object" -> JSObject("data" -> JSObject(
                "object" -> JSObject("p1" -> JSObject("object" -> JSObject()),
                                     "p2" -> JSObject("object" -> JSObject())))))
          ),
          db.key
        ) should respond(Created)

        val res = api.query(
          JSObject(
            "paginate" -> JSObject("match" -> JSObject("@ref" -> "indexes/compound"),
                                   "terms" -> JSArray(JSNull, JSNull))),
          db.key)

        res should respond(OK)
        (res.resource / "data").isEmpty should be(true)
      }
    }

    once("PATCH works") {
      for {
        database <- aDatabase
        cls <- aCollection(database)
        user <- aUser(database)
      } {
        api.post(
          "/indexes",
          JSObject("name" -> "creatures_by_name",
                   "source" -> JSObject("@ref" -> cls.ref),
                   "terms" -> JSArray(JSObject("field" -> List("data", "name"))),
                   "active" -> true),
          database.key
        ) should respond(Created)

        val res = api.patch(
          "/indexes/creatures_by_name",
          JSObject("data" -> JSObject("comment" -> "A very magical index.")),
          database.key)

        res should respond(OK)
        //(res.resource / "in").as[String] should equal ("indexes")
        (res.resource / "source") should equal(JSObject("@ref" -> cls.ref))
        (res.resource / "data" / "comment").as[String] should equal(
          "A very magical index.")
        val terms = (res.resource / "terms").as[Seq[JSObject]]
        terms.size should equal(1)
        (res.resource / "terms" / 0) should equal(
          JSObject("field" -> JSArray("data", "name")))

        api.patch("/indexes/creatures_by_name", JSObject.empty, user.token) should respond(
          Forbidden)
      }
    }

    once("DELETE works") {
      for {
        database <- aDatabase
        cls <- aCollection(database)
      } {
        val res = api.post("/indexes",
                           JSObject("name" -> "creatures_by_size",
                                    "source" -> JSObject("@ref" -> cls.ref),
                                    "field" -> List("data", "size")),
                           database.key)
        res should respond(Created)

        api.delete(res.path, database.key) should respond(OK)
        api.get(res.path, database.key) should respond(NotFound)
      }
    }
  }
}
