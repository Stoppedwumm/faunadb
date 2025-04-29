package fauna.api.test

import fauna.codex.json._

class DatabaseSpec extends RESTSpec {

  "databases" / {
    once("works") {
      for {
        dbname1 <- aUniqueDBName
        dbname2 <- aUniqueDBName
        otherdb <- aDatabase
        user <- aUser(otherdb)
      } {
        val db1 = api.post("/databases", JSObject("name" -> dbname1), rootKey)
        db1 should respond (Created)
        val db2 = api.post("/databases", JSObject("name" -> dbname2), rootKey)
        db2 should respond (Created)

        val set = collection("/databases", rootKey)
        set should contain (db1.resource / "ref")
        set should contain (db2.resource / "ref")

        val events = api.get(s"/databases/events?after=${db1.ts}&size=1000", rootKey)
        events should respond (OK)

        (events.resource / "data") should containEvent ("create", db1.resource / "ref", db1.ts)
        (events.resource / "data") should containEvent ("create", db2.resource / "ref", db2.ts)

        api.get("/databases", otherdb.key) should respond (Forbidden)
        api.get("/databases", user.token) should respond (Forbidden)
      }
    }

    test("name should contain valid URL and filename chars only") {
      val err1 = api.post("/databases", JSObject("name" -> "foo/"), rootKey)
      err1 should respond (BadRequest)
      (err1.json / "errors" / 0 / "code") should equal (JSString("validation failed"))
      val err2 = api.post("/databases", JSObject("name" -> "foo:"), rootKey)
      err2 should respond (BadRequest)
      (err2.json / "errors" / 0 / "code") should equal (JSString("validation failed"))
      val err3 = api.post("/databases", JSObject("name" -> "foo\\"), rootKey)
      err3 should respond (BadRequest)
      (err3.json / "errors" / 0 / "code") should equal (JSString("validation failed"))
    }
  }

  "databases/{name}" / {
    once("GET works") {
      for {
        name <- aUniqueDBName
        otherdb <- aDatabase
        user <- aUser(otherdb)
      } {
        val db1 = api.post("/databases", JSObject("name" -> name), rootKey)

        db1 should respond (Created)

        api.get("/databases", rootKey) should respond (OK)
        api.get(s"/databases/$name/events", rootKey) should respond (OK)

        //could be a separate test
        api.get(s"/databases/Imaginary", otherdb.key) should respond (NotFound)
        api.get(s"/databases/Imaginary/events", otherdb.key) should respond (NotFound)

        api.get(s"/databases/$name", otherdb.key) should respond (NotFound)
        api.get(s"/databases/$name/events", otherdb.key) should respond (NotFound)

        api.get(s"/databases/$name", otherdb.clientKey) should respond (NotFound)
        api.get(s"/databases/$name/events", otherdb.clientKey) should respond (NotFound)

        api.get(s"/databases/$name", user.token) should respond (NotFound)
        api.get(s"/databases/$name/events", user.token) should respond (NotFound)
      }
    }

    once("PUT works") {
      for {
        name <- aUniqueDBName
        rename <- aUniqueDBName
      } {
        val res1 = api.post("/databases", JSObject("name" -> name), rootKey)

        res1 should respond (Created)
        (res1.resource / "name").as[String] should equal (name)
        (res1.resource / "ref") should equal (JSObject("@ref" -> s"databases/$name"))

        val res2 = api.put(res1.path, JSObject("name" -> rename), rootKey)

        res2 should respond (OK)
        (res2.resource / "name").as[String] should equal (rename)
        (res2.resource / "ref") should equal (JSObject("@ref" -> s"databases/$rename"))

        api.get(res2.path, rootKey) should respond (OK)
        api.get(res1.path, rootKey) should respond (NotFound)

        api.put(res2.path, JSObject("name" -> name), rootKey) should respond (OK)
        api.get(res1.path, rootKey) should respond (OK)

        api.get(res2.path, rootKey) should respond (NotFound)
      }
    }

    test("databases hide their scope field") {
      val name = aUniqueDBName.sample
      val res = api.post("/databases", JSObject("name" -> name), rootKey)
      res should respond (Created)
      (res.resource / "scope").isEmpty should be(true)
    }

    test("databases take data") {
      val name = aUniqueDBName.sample
      val res1 = api.post("/databases", JSObject("name" -> name), rootKey)
      res1 should respond (Created)

      val res2 = api.put(s"/databases/$name", JSObject("data" -> JSObject("tenant" -> "Terra")), rootKey)

      (res1.resource / "name").as[String] should equal (name)
      (res2.resource / "data" / "tenant").as[String] should equal ("Terra")
    }

    test("databases can be repeatedly created and deleted") {
      val name = aUniqueDBName.sample
      for (_ <- 1 to 5) {
        api.post("/databases", JSObject("name" -> name), rootKey) should respond (Created)

        val k = (api.post("/keys", JSObject(
          "database" -> JSObject("@ref" -> s"databases/$name"),
          "role" -> "server"), rootKey).resource / "secret").as[String]

        api.post(s"/classes", JSObject("name" -> "foo") , k) should respond (Created)

        api.delete(s"/databases/$name", rootKey) should respond (OK)
      }
    }

    once("databases cannot be read or created by a server key") {
      for {
        dbname1 <- aUniqueDBName
      } {
        val db1 = api.post("/databases", JSObject("name" -> dbname1), rootKey)
        api.get(db1.path, rootKey) should respond (OK)

        val keyres = api.post("/keys", JSObject("database" -> (db1.resource / "ref"), "role" -> "server"), rootKey)
        keyres should respond (Created)

        val secret = (keyres.resource / "secret").as[String]
        api.get(db1.path, secret) should respond (NotFound)
        api.post("/databases", JSObject("name" -> "Imaginary"), secret) should respond (Forbidden)
      }
    }

    once("PATCH works") {
      for {
        db <- aDatabase
      } {
        val res = api.patch(db.path, JSObject("data" -> JSObject("env" -> "test")), rootKey)
        res should respond (OK)
        (res.resource / "data" / "env").as[String] should equal ("test")

        val rename = aUniqueDBName.sample
        val res2 = api.patch(res.path, JSObject("name" -> rename), rootKey)

        res2 should respond (OK)

        val res3 = api.get(res2.path, rootKey)

        (res3.resource / "data" / "env").as[String] should equal ("test")
      }
    }

    once("DELETE works") {
      for {
        name <- aUniqueDBName
        otherdb <- aDatabase
        user <- aUser(otherdb)
      } {
        val db1 = api.post("/databases", JSObject("name" -> name), rootKey)

        api.get("/databases", rootKey) should respond (OK)

        api.delete(db1.path, otherdb.key) should respond (NotFound)
        api.delete(db1.path, otherdb.clientKey) should respond (NotFound)
        api.delete(db1.path, user.token) should respond (NotFound)

        api.get(db1.path, rootKey) should respond (OK)
        api.delete(db1.path, rootKey) should respond (OK)
        api.get(db1.path, rootKey) should respond (NotFound)
      }
    }

    once("keys are invalidated on database deletion") {
      for {
        database <- aDatabase
      } {
        api.get("/classes", database.key) should respond (OK)
        api.delete(database.path, rootKey) should respond (OK)
        api.get(database.path, rootKey) should respond (NotFound)

        beforeTTLCacheExpiration {
          api.get("/classes", database.key) should respond (Unauthorized)
          api.get("/classes", database.clientKey) should respond (Unauthorized)
        }
      }
    }

    once("user tokens are invalidated on database deletion") {
      for {
        database <- aDatabase
        user <- aUser(database)
      } {
        api.get("/tokens/self", user.token) should respond(OK)

        api.delete(database.path, rootKey) should respond (OK)

        beforeTTLCacheExpiration {
          api.get("/tokens/self", user.token) should respond(Unauthorized)
        }
      }
    }
  }
}

class DatabaseSpec27 extends API27Spec {
  "databases" / {
    once("can be created in containers") {
      for {
        parent <- aContainerDB
      } {
        api.post("/databases", JSObject("name" -> "child"), parent.adminKey) should respond(Created)
      }
    }
  }

  "container databases" / {
    once("can be created in other containers") {
      for {
        parent <- aContainerDB
      } {
        api.post("/databases", JSObject("name" -> "child", "container" -> true), parent.adminKey) should respond (Created)
      }
    }

    // FIXME: cannot run this test till we have real versioned response objects
    once("PATCH works") {
      for {
        container <- aContainerDB
      } pendingUntilFixed {
        {
          api.patch(container.path, JSObject("container" -> true), rootKey) should respond(OK)
        }
      }
    }
  }
}
