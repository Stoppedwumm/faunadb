package fauna.api.test

import fauna.codex.json._
import fauna.lang.clocks.Clock
import fauna.prop._
import scala.concurrent.duration._

class TokenSpec extends RESTSpec {
  "tokens" / {
    once("GET works") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        doc <- aDocument(cls)
      } {
        val params = JSObject("instance" -> JSObject("@ref" -> doc.ref))

        val token0 = api.post("/tokens", params, db.key).resource
        val token1 = api.post("/tokens", params, db.key).resource

        val tokens = collection("/tokens", db.key)
        tokens should contain (token0 / "ref")
        tokens should contain (token1 / "ref")
      }
    }

    once("POST works") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        val params = JSObject("instance" -> JSObject("@ref" -> inst.ref))

        val res = api.post("/tokens", params, db.key)
        res should respond (Created)

        val res2 = api.post("/tokens", params, db.clientKey)
        res2 should respond (Forbidden)
      }
    }

    "tokens/{id}" / {
      once("GET works") {
        for {
          db <- aDatabase
          cls <- aCollection(db)
          inst <- aDocument(cls)
        } {
          val tok = api.post("/tokens", JSObject(
            "instance" -> JSObject(
              "@ref" -> inst.ref)), db.key).resource

          api.get(tok.path, db.key) should respond (OK)
        }
      }

      once("PUT can update tokens") {
        for {
          db <- aDatabase
          cls <- aCollection(db)
          inst <- aDocument(cls)
          userClass <- aCollection(db)
          user <- aDocument(userClass)
        } {
          val tok = api.post("/tokens", JSObject(
            "instance" -> JSObject(
              "@ref" -> inst.ref)), db.key).resource

          val params = tok -- Seq("instance") + ("instance" -> JSObject("@ref" -> user.ref))
          val res = api.put(s"/${tok.ref}", params, db.key)
          res should respond (OK)
        }
      }

      prop("PUT without an existing record") {
        for {
          db <- aDatabase
          cls <- aCollection(db)
          inst <- aDocument(cls)
          id <- Prop.long
        } {
          val params = JSObject(
            "password" -> "sekrit",
            "instance" -> JSObject(
              "@ref" -> inst.ref))
          api.put(s"/tokens/$id", params, db.key) should respond (NotFound)
        }
      }

      once("PATCH works") {
        for {
          db <- aDatabase
          cls <- aCollection(db)
          inst <- aDocument(cls)
          userClass <- aCollection(db)
          user <- aDocument(userClass)
        } {
          val tok = api.post("/tokens", JSObject(
            "instance" -> JSObject(
              "@ref" -> inst.ref)), db.key).resource

          val params = JSObject("user" -> user.ref)
          val res = api.patch(s"/${tok.ref}", params, db.key)
          res should respond (OK)
        }
      }

      once("DELETE works") {
        for {
          db <- aDatabase
          cls <- aCollection(db)
          inst <- aDocument(cls)
        } {
          val tok = api.post("/tokens", JSObject(
            "instance" -> JSObject(
              "@ref" -> inst.ref)), db.key).resource

          api.delete(s"/${tok.ref}", db.key) should respond (OK)
          api.get(s"/${tok.ref}", db.key) should respond (NotFound)
        }

      }
    }

    //get the stored token
    "ttl: stored" / {
      once("respect ttl") {
        for {
          db <- aDatabase
          cls <- aCollection(db)
          inst <- aDocument(cls)
        } {
          val ttl = Clock.time + 5.seconds

          val tok = api.post("/tokens", JSObject(
            "ttl" -> JSObject("@ts" -> ttl.toString),
            "instance" -> JSObject("@ref" -> inst.ref)),
            db.key).resource

          api.get(s"/${tok.ref}", db.key) should respond (OK)

          eventually(timeout(10.seconds), interval(200.millis)) {
            api.get(s"/${tok.ref}", db.key) should respond (NotFound)
          }
        }
      }

      once("update ttl") {
        for {
          db <- aDatabase
          cls <- aCollection(db)
          inst <- aDocument(cls)
        } {
          val tok = api.post("/tokens", JSObject(
            "instance" -> JSObject("@ref" -> inst.ref)),
            db.key).resource

          api.get(s"/${tok.ref}", db.key) should respond (OK)

          val ttl = Clock.time + 5.seconds
          api.patch(s"/${tok.ref}", JSObject(
            "ttl" -> JSObject("@ts" -> ttl.toString)
          ), db.key) should respond (OK)

          eventually(timeout(10.seconds), interval(200.millis)) {
            api.get(s"/${tok.ref}", db.key) should respond (NotFound)
          }
        }
      }
    }

    //authenticate with token
    "ttl: cached" / {
      once("respect ttl") {
        for {
          db <- aDatabase
          cls <- aCollection(db)
          inst <- aDocument(cls)
        } {
          val ttl = Clock.time + 5.seconds

          val tok = api.post("/tokens", JSObject(
            "ttl" -> JSObject("@ts" -> ttl.toString),
            "instance" -> JSObject("@ref" -> inst.ref)),
            db.key).resource

          val secret = (tok / "secret").as[String]

          api.get(s"/${tok.ref}", secret) should respond(OK)

          eventually(timeout(10.seconds), interval(200.millis)) {
            api.get(s"/${tok.ref}", secret) should respond(Unauthorized)
          }
        }
      }

      once("update ttl") {
        for {
          db <- aDatabase
          cls <- aCollection(db)
          inst <- aDocument(cls)
        } {
          val tok = api.post("/tokens", JSObject(
            "instance" -> JSObject("@ref" -> inst.ref)),
            db.key).resource

          val secret = (tok / "secret").as[String]

          api.get(s"/${tok.ref}", secret) should respond(OK)

          val ttl = Clock.time + 5.seconds
          api.patch(s"/${tok.ref}", JSObject(
            "ttl" -> JSObject("@ts" -> ttl.toString)
          ), db.key) should respond (OK)

          eventually(timeout(10.seconds), interval(200.millis)) {
            api.get(s"/${tok.ref}", secret) should respond(Unauthorized)
          }
        }
      }
    }

    once("tokens are invalid for a deleted database") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        doc <- aDocument(coll)
      } {
        val tok = api
          .post(
            "/tokens",
            JSObject("instance" -> JSObject("@ref" -> doc.ref)),
            db.key)
          .resource

        val secret = (tok / "secret").as[String]

        api.get(s"/${tok.ref}", secret) should respond(OK)

        api.delete(db.path, rootKey) should respond (OK)

        eventually(timeout(30.seconds), interval(100.millis)) {
          api.get(s"/${tok.ref}", secret) should respond(Unauthorized)
        }
      }
    }
  }
}
