package fauna.api.test

import fauna.codex.json._

class DocumentSpec extends API20Spec {
  "classes/{name}" / {
    once("POST works") {
      for {
        database <- aDatabase
        user <- aUser(database)
        cls <- aCollection(database)
      } {
        val res = api.post(cls.path, JSObject(
          "data" -> JSObject(
          "name" -> "Henwen",
          "description" -> "Oracular pig",
          "age" -> 120,
          "keeper" -> JSObject("@ref" -> user.ref))),
          database.key)

        api.put("/indexes/creatures_by_age", JSObject(
          "source" -> cls.ref,
          "terms" -> JSArray(JSObject("field" -> List("data", "age"))),
          "active" -> true,
          "unique" -> false),
          database.key)

        res should respond (Created)
        (res.resource / "ref" / "@ref").as[String] should startWith (cls.ref)

        (res.resource / "data") should containJSON(JSObject("keeper" -> JSObject("@ref" -> user.ref)))
      }
    }

    once("database can create instances") {
      for {
        database <- aDatabase
        faunaClass <- aCollection(database)
      } {
        val res = api.post(faunaClass.path, JSObject.empty, database.key)

        res should respond(Created)
        (res.resource / "ref" / "@ref").as[String] should startWith(faunaClass.ref)
      }
    }

    once("reference liveness not checked on write") {
      for {
        database <- aDatabase
        faunaClass <- aCollection(database)
      } {
        val res = api.post(faunaClass.path, JSObject("data" -> JSObject("instance" -> JSObject("@ref" -> s"${faunaClass.ref}/1237"))), database.key)

        res should respond (Created)
      }
    }

    once("deduplicates fields") {
      for {
        database <- aDatabase
        faunaClass <- aCollection(database)
      } {
        val res = api.post(faunaClass.path, JSObject("data" -> JSObject("foo" -> "orig", "foo" -> "dup")), database.key)

        res should respond (Created)

        val foos = (res.resource / "data").as[JSObject].value collect { case ("foo", v) => v.as[String] }
        foos should equal (Seq("orig"))
      }
    }

    test("elides `class` for newer versions of DB") {
      val db = aDatabase("2.0").sample
      val cls = aFaunaClass(db).sample
      val inst = aDocument(cls).sample
      inst should containKeys("class")
      (inst / "class") should equal (JSObject("@ref" -> cls.ref))

      val db2 = aDatabase("2.1").sample
      val cls2 = aFaunaClass(db2).sample
      val inst2 = client.withVersion("2.1").api.post(cls2.path, JSObject(), db2.key)
      inst2.resource should excludeKeys("class")
    }
  }

  "classes/{name}/{id}" / {
    once("PUT works") {
      for {
        database <- aDatabase
        cls <- aCollection(database)
      } {
        val instance = api.post(cls.path, JSObject.empty, database.key).resource
        val params = instance patch JSObject("data" -> JSObject("hungry" -> true))

        val res = api.put(instance.path, params, database.key)
        res should respond (OK)

        res.resource should matchJSON (params)
      }
    }

    once("respects uniqueness constraints") {
      for {
        database <- aDatabase
        faunaClass <- aCollection(database)
      } {
        val index = JSObject(
          "name" -> "foo",
          "source" -> JSObject("@ref" -> faunaClass.ref),
          "terms" -> JSArray(JSObject("field" -> List("data", "foo"))),
          "active" -> true,
          "unique" -> true)
        api.post("/indexes", index, database.key) should respond (Created)

        val one = api.post(faunaClass.path, JSObject("data" -> JSObject("foo" -> "bar")), database.key)
        one should respond (Created)

        val two = api.post(faunaClass.path, JSObject(), database.key)
        two should respond (Created)

        val err = api.put(two.path, JSObject("data" -> JSObject("foo" -> "bar")), database.key)
        err should respond (BadRequest)

        (err.json / "errors" / 0 / "code").as[String] should equal ("instance not unique")
      }
    }

    once("PATCH works") {
      for {
        database <- aDatabase
        cls <- aCollection(database)
      } {
        val instance = api.post(cls.path, JSObject.empty, database.key)
        val params = JSObject("data" -> JSObject("hungry" -> false))

        val res = api.patch(instance.path, params, database.key)
        res should respond (OK)

        res.resource should containKeys("data")
      }
    }

    once("GET works") {
      for {
        database <- aDatabase
        cls <- aCollection(database)
      } {
        val instance = api.post(cls.path, JSObject.empty, database.key)

        val res = api.get(instance.path, database.key)
        res should respond (OK)
        (res.resource / "ref" / "@ref").as[String] should startWith (cls.ref)
      }
    }

    once("DELETE works") {
      for {
        database <- aDatabase
        cls <- aCollection(database)
      } {
        val instance = api.post(cls.path, JSObject.empty, database.key)

        api.delete(instance.path, database.key) should respond (OK)
        api.get(instance.path, database.key) should respond (NotFound)
      }
    }

    once("create followed by create (with same id) should fail") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
      } {
        val docQ = api.query(CreateF(coll.refObj, MkObject("data" -> MkObject("f" -> "v"))), db.key)
        docQ should respond (Created)
        val err = api.query(CreateF(docQ.refObj, MkObject("data" -> MkObject("f" -> "v"))), db.key)
        err should respond (BadRequest)
        (err.json / "errors" / 0 / "code") should equal (JSString("instance already exists"))
      }
    }

    once("create, delete, create works") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
      } {
        val docQ = api.query(CreateF(coll.refObj, MkObject("data" -> MkObject("f" -> "v"))), db.key)
        docQ should respond (Created)

        api.query(DeleteF(docQ.refObj), db.key) should respond (OK)

        api.query(CreateF(docQ.refObj, MkObject("data" -> MkObject("f" -> "v"))), db.key) should respond (Created)
      }
    }
  }
}
