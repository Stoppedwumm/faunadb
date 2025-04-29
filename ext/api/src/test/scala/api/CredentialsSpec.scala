package fauna.api.test

import fauna.codex.json._
import fauna.prop._

class CredentialsSpec extends RESTSpec {
  "credentials" / {
    once("GET works") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        doc0 <- aDocument(cls)
        doc1 <- aDocument(cls)
      } {
        def params(doc: JSObject) = JSObject(
          "password" -> "sekrit",
          "instance" -> JSObject("@ref" -> doc.ref))

        val cred0 = api.post("/credentials", params(doc0), db.key).resource
        val cred1 = api.post("/credentials", params(doc1), db.key).resource

        val creds = collection("/credentials", db.key)
        creds should contain (cred0 / "ref")
        creds should contain (cred1 / "ref")
      }
    }

    once("POST works") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        val params = JSObject(
          "password" -> "sekrit",
          "instance" -> JSObject("@ref" -> inst.ref))

        val res = api.post("/credentials", params, db.key)
        res should respond (Created)

        val res2 = api.post("/credentials", params, db.clientKey)
        res2 should respond (Forbidden)
      }
    }

    once("without a password") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        val params = JSObject(
          "instance" -> JSObject("@ref" -> inst.ref))
        val res = api.post("/credentials", params, db.key)
        res should respond (Created)
      }
    }

    once("without an instance") {
      for {
        db <- aDatabase
      } {
        val params = JSObject("password" -> "sekrit")
        val res = api.post("/credentials", params, db.key)
        res should respond (BadRequest)
      }
    }

    "credentials/{id}" / {
      once("GET works") {
        for {
          db <- aDatabase
          cls <- aCollection(db)
          inst <- aDocument(cls)
          creds <- mkCredentials(db, inst, "sekrit")
        } {
          api.get(creds.path, db.key) should respond (OK)
        }
      }

      once("PUT can update credentials") {
        for {
          db <- aDatabase
          cls <- aCollection(db)
          inst <- aDocument(cls)
          creds <- mkCredentials(db, inst, "sekrit")
        } {
          val params = JSObject("password" -> "str0ngpa$$",
            "instance" -> JSObject("@ref" -> inst.ref))
          val res = api.put(s"/${creds.ref}", params, db.key)
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
          api.put(s"/credentials/$id", params, db.key) should respond (NotFound)
        }
      }

      once("PATCH works") {
        for {
          db <- aDatabase
          cls <- aCollection(db)
          inst <- aDocument(cls)
          creds <- mkCredentials(db, inst, "sekrit")
        } {
          val params = JSObject("password" -> "str0ngpa$$")
          val res = api.patch(s"/${creds.ref}", params, db.key)
          res should respond (OK)
        }
      }

      once("DELETE works") {
        for {
          db <- aDatabase
          cls <- aCollection(db)
          inst <- aDocument(cls)
          creds <- mkCredentials(db, inst, "sekrit")
        } {
          api.delete(s"/${creds.ref}", db.key) should respond (OK)
          api.get(s"/${creds.ref}", db.key) should respond (NotFound)
        }

      }
    }
  }
}
