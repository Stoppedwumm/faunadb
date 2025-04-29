package fauna.api.test

import fauna.codex.json._
import fauna.lang.clocks.Clock
import scala.concurrent.duration._

class KeySpec extends RESTSpec {

  "keys" / {
    once("GET works") {
      for {
        db1 <- aDatabase
      } {
        val key = api.post("/keys", JSObject("database" -> (db1.resource / "ref"), "role" -> "server"), rootKey).resource
        collection("/keys", rootKey) should contain ((key / "ref").as[JSValue])
      }
    }

    once("POST works") {
      for {
        db1 <- aDatabase
      } {
        val res = api.post("/keys", JSObject("database" -> (db1.resource / "ref"), "role" -> "server"), rootKey)

        res should respond (Created)
        (res.resource / "ref" / "@ref").as[String] should startWith ("keys")
        res.resource should containKeys ("secret", "role")
        (res.resource / "role").as[String] should equal ("server")
        (res.resource / "database") should equal (db1.resource / "ref")

        val res2 = api.post("/keys", JSObject("database" -> (db1.resource/ "ref"), "role" -> "client"), rootKey)
        res2 should respond (Created)
        (res2.resource / "role").as[String] should equal ("client")

        val res3 = api.post("/keys", JSObject("database" -> (db1.resource / "ref"), "role" -> "foo"), rootKey)
        res3 should respond (BadRequest)

        val res4 = api.post("/keys", JSObject("database" -> "database/foo", "role" -> "foo"), rootKey)
        res4 should respond (BadRequest)
      }
    }

    once("keys cannot duplicate hash fields") {
      for {
        db1 <- aDatabase
        db2 <- aDatabase
      } {
        val admin1 = {
          val db = (db1.resource / "ref")
          val res = api.post("/keys", JSObject("database" -> db, "role" -> "admin"), rootKey)
          (res.resource / "secret").as[String]
        }

        val fooDB = api.post("/databases", JSObject("name" -> "foo"), admin1)
        fooDB should respond (Created)

        val admin2 = {
          val db = (db2.resource / "ref")
          val res = api.post("/keys", JSObject("database" -> db, "role" -> "admin"), rootKey)
          (res.resource / "secret").as[String]
        }
        api.post("/databases", JSObject("name" -> "foo"), admin2) should respond (Created)

        val key1 = api.post("/keys", JSObject("database" -> (fooDB.resource / "ref"), "role" -> "server"), admin1).resource
        api.post(key1.path, key1, admin2) should respond (BadRequest)
      }
    }

    once("keys can have data") {
      for {
        db1 <- aDatabase
      } {
        val res = api.post("/keys", JSObject(
          "role" -> "client",
          "database" -> (db1.resource / "ref"),
          "data" -> JSObject("app_id" -> "ios_v_3_4")), rootKey)
        res should respond (Created)

        (res.resource / "data" / "app_id").as[String] should equal ("ios_v_3_4")
      }
    }

    once("a key's scope field is hidden") {
      for {
        db1 <- aDatabase
      } {
        val res = api.post("/keys", JSObject("role" -> "client", "database" -> (db1.resource / "ref")), rootKey)
        res should respond (Created)
        (res.resource / "scope").isEmpty should be(true)
      }
    }

    once("keys cannot be read or created with other keys") {
      for {
        db1 <- aDatabase
        db2 <- aDatabase
        subDb1 <- aDatabase(apiVers, db1)
        subDb2 <- aDatabase(apiVers, db2)
      } {
        val key1 = api.post("/keys", JSObject("database" -> (db1.resource / "ref"), "role" -> "server"), rootKey)
        key1 should respond (Created)

        val key2 = api.post("/keys", JSObject("database" -> (db2.resource / "ref"), "role" -> "client"), rootKey)
        key2 should respond (Created)

        val serverSecret = (key1.resource / "secret").as[String]
        val clientSecret = (key2.resource / "secret").as[String]

        api.get("/keys/21312", serverSecret) should respond (Forbidden)
        api.get("/keys/21312", clientSecret) should respond (Forbidden)

        api.get(key1.path, serverSecret) should respond (Forbidden)
        api.get(key2.path, serverSecret) should respond (Forbidden)
        api.get(key1.path, clientSecret) should respond (Forbidden)
        api.get(key2.path, clientSecret) should respond (Forbidden)

        api.post("/keys", JSObject("database" -> (subDb1.resource / "ref"), "role" -> "server"), serverSecret) should respond (Forbidden)
        api.post("/keys", JSObject("database" -> (subDb2.resource / "ref"), "role" -> "server"), clientSecret) should respond (Forbidden)
      }
    }

    once("keys do not store a plain-text secret") {
      for {
        db1 <- aDatabase
      } {
        val key = api.post("/keys", JSObject("database" -> (db1.resource / "ref"), "role" -> "server"), rootKey)
        (key.resource / "secret").asOpt[String].isDefined should be(true)
        (api.get(key.path, rootKey).resource / "secret").asOpt[String].isDefined should be(false)
      }
    }

    once("keys can be restored using the root key") {
      for {
        db1 <- aDatabase
      } {
        val args = JSObject("database" -> (db1.resource / "ref"), "role" -> "server")
        val (key, secret) = {
          val res = api.post("/keys", args, rootKey).resource
          (api.get(res.path, rootKey).resource, (res / "secret").as[String])
        }

        api.delete(db1.path, rootKey) should respond (OK)

        api.post("/databases", JSObject("name" -> (db1.resource / "name")), rootKey) should respond (Created)
        api.get("/classes", secret) should respond (Unauthorized)

        api.put(key.path, key, rootKey) should respond (OK)
        api.get(key.path, rootKey) should respond (OK)

        api.get("/classes", secret) should respond (OK)
      }
    }

    once("keys cannot be restored using an admin key") {
      for {
        parent <- aDatabase
        db <- aDatabase(apiVers, parent)
      } {
        // Create a key in the parent database to use for all the key manipulation in the child.
        val parentArgs = JSObject("database" -> (parent.resource / "ref"), "role" -> "admin")
        val parentKey = (api.post("/keys", parentArgs, rootKey).resource / "secret").as[String]

        // Create a key for the child database.
        val args = JSObject("database" -> (db.resource / "ref"), "role" -> "server")
        val (key, secret) = {
          val res = api.post("/keys", args, parentKey).resource
          (api.get(res.path, parentKey).resource, (res / "secret").as[String])
        }

        // Delete the child database, then restore the child database (i.e. create
        // a database with the same name and parent). The key will not work.
        api.delete(db.path, parentKey) should respond (OK)
        api.post("/databases", JSObject("name" -> (db.resource / "name")), parentKey) should respond (Created)
        api.get("/classes", secret) should respond (Unauthorized)

        // Try to replace or update the key using the parent key. It's not allowed.
        api.put(key.path, key, parentKey) should respond (Forbidden)
        api.patch(key.path, key, parentKey) should respond (Forbidden)
      }
    }

    once("requests with no key return 401 unauthorized") {
      for {
        db1 <- aDatabase
        faunaClass <- aCollection(db1)
        instance <- aDocument(faunaClass)
      } {
        api.get(faunaClass.path) should respond (Unauthorized)
        api.get(instance.path) should respond (Unauthorized)
        api.get(instance.path+"/events") should respond (Unauthorized)
      }
    }

    once("requests with an invalid or nonexistent key return 401 unauthorized") {
      for {
        db1 <- aDatabase
        faunaClass <- aCollection(db1)
        instance <- aDocument(faunaClass)
      } {
        val key = api.post("/keys", JSObject("database" -> (db1.resource / "ref"), "role" -> "server"), rootKey).resource
        val secret = (key / "secret").as[String]

        api.get(faunaClass.path, secret) should respond (OK)
        api.delete(key.path, rootKey) should respond (OK)
        api.get(key.path, rootKey) should respond (NotFound)

        Seq("foo", secret) foreach { secret =>
          api.get(faunaClass.path, secret) should respond (Unauthorized)
          api.get(instance.path, secret) should respond (Unauthorized)
          api.get(instance.path+"/events", secret) should respond (Unauthorized)
        }
      }
    }

    once("requests with a valid key that does not have permissions to view a resource return 403 forbidden") {
      for {
        db1 <- aDatabase
        faunaClass <- aCollection(db1)
        instance <- aDocument(faunaClass)
        user <- aUser(db1)
      } {
        Seq(user.token, db1.clientKey) foreach { secret =>
          api.get(faunaClass.path, secret) should respond (Forbidden)
          api.get(instance.path, secret) should respond (Forbidden)
          api.get(instance.path+"/events", secret) should respond (Forbidden)
        }
      }
    }
  }

  "keys/{id}" / {
    once("GET works") {
      for {
        db1 <- aDatabase
      } {
        val key = api.post("/keys", JSObject("database" -> (db1.resource / "ref"), "role" -> "server"), rootKey).resource
        val res = api.get(key.path, rootKey)
        res should respond (OK)
      }
    }

    once("PUT works with the root key") {
      for {
        db1 <- aDatabase
      } {
        val res = api.post("/keys", JSObject("role" -> "server", "database" -> (db1.resource / "ref")), rootKey)
        res should respond (Created)
        val secret = (res.resource / "secret").as[String]

        val res1 = api.put(res.path, JSObject("secret" -> secret, "role" -> "client", "database" -> (db1.resource / "ref")), rootKey)
        res1 should respond (OK)
        (res1.resource / "hashed_secret").as[String] should not equal ((res.resource / "hashed_secret").as[String])

        val res2 = api.put(res.path, JSObject("role" -> "server", "database" -> (db1.resource / "ref")), rootKey)
        res1 should respond (OK)
        (res2.resource / "hashed_secret").as[String] should equal ((res1.resource / "hashed_secret").as[String])

        val res3 = api.put(res.path, JSObject("database" -> db1.ref, "role" -> "server", "secret" -> "badsecret"), rootKey)
        res3 should respond (BadRequest)
      }
    }

    once("PATCH works with the root key") {
      for {
        db1 <- aDatabase
      } {
        val res = api.post("/keys", JSObject("role" -> "server", "database" -> (db1.resource / "ref")), rootKey)
        res should respond (Created)
        val secret = (res.resource / "secret").as[String]

        val res1 = api.patch(res.path, JSObject("secret" -> secret), rootKey)
        res1 should respond (OK)
        (res1.resource / "hashed_secret").as[String] should not equal ((res.resource / "hashed_secret").as[String])
      }
    }

    once("DELETE works") {
      for {
        db1 <- aDatabase
      } {
        val key = api.post("/keys", JSObject("database" -> (db1.resource / "ref"), "role" -> "server"), rootKey).resource

        val res = api.delete(key.path, rootKey)
        res should respond (OK)
        api.get(key.path, rootKey) should respond (NotFound)

        api.get("/classes", (key / "secret").as[String]) should respond (Unauthorized)
      }
    }
  }

  //read the stored key
  "ttl: stored" / {
      once("respect ttl") {
        for {
          db <- aDatabase
        } {
          val ttl = Clock.time + 5.seconds

          val key = api.post("/keys", JSObject(
            "ttl" -> JSObject("@ts" -> ttl.toString),
            "database" -> (db.resource / "ref"),
            "role" -> "server"
          ), rootKey).resource

          api.get(s"/${key.ref}", rootKey) should respond (OK)

          eventually(timeout(10.seconds), interval(200.millis)) {
            api.get(s"/${key.ref}", rootKey) should respond (NotFound)
          }
        }
      }

      once("update ttl with the root key") {
        for {
          db <- aDatabase
        } {

          val key = api.post("/keys", JSObject(
            "database" -> (db.resource / "ref"),
            "role" -> "server"
          ), rootKey).resource

          api.get(s"/${key.ref}", rootKey) should respond (OK)

          val ttl = Clock.time + 5.seconds

          api.patch(s"/${key.ref}", JSObject(
            "ttl" -> JSObject("@ts" -> ttl.toString)
          ), rootKey).resource

          eventually(timeout(10.seconds), interval(200.millis)) {
            api.get(s"/${key.ref}", rootKey) should respond (NotFound)
          }
        }
      }
    }

  //authenticate using the key
  "ttl: cached" / {
    once("respect ttl") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
      } {
        val ttl = Clock.time + 5.seconds

        val key = api.post("/keys", JSObject(
          "ttl" -> JSObject("@ts" -> ttl.toString),
          "database" -> (db.resource / "ref"),
          "role" -> "server"
        ), rootKey).resource

        val secret = (key / "secret").as[String]

        api.get(s"/${coll.ref}", secret) should respond (OK)

        eventually(timeout(10.seconds), interval(200.millis)) {
          api.get(s"/${coll.ref}", secret) should respond (Unauthorized)
        }
      }
    }

    once("update ttl with the root key") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
      } {
        val key = api.post("/keys", JSObject(
          "database" -> (db.resource / "ref"),
          "role" -> "server"
        ), rootKey).resource

        val secret = (key / "secret").as[String]

        api.get(s"/${coll.ref}", secret) should respond (OK)

        val ttl = Clock.time + 5.seconds

        api.patch(s"/${key.ref}", JSObject(
          "ttl" -> JSObject("@ts" -> ttl.toString)
        ), rootKey).resource

        eventually(timeout(10.seconds), interval(200.millis)) {
          api.get(s"/${coll.ref}", secret) should respond (Unauthorized)
        }
      }
    }
  }
}
