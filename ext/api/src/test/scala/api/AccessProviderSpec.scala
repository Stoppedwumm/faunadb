package fauna.api.test

import fauna.codex.json._
import fauna.prop.Prop

class AccessProviderSpec extends API4Spec {

  "access_providers" / {
    once("only admin can create") {
      for {
        db <- aDatabase
        name <- aUniqueName
        issuer <- Prop.alphaString()
        url <- Prop.aJwksUri
      } {
        val body = JSObject(
          "name" -> name,
          "issuer" -> issuer,
          "jwks_uri" -> url
        )

        api.post("/access_providers", body, db.key) should respond(Forbidden)
        api.post("/access_providers", body, db.clientKey) should respond(Forbidden)
        api.post("/access_providers", body, db.adminKey) should respond(Created)
      }
    }

    once("GET works") {
      for {
        db <- aDatabase
        accessProvider <- anAccessProvider(db)
      } {
        val res = api.get("/access_providers", db.adminKey)

        res should respond(OK)
        (res.resource / "data") should containElem(accessProvider.refObj)
      }
    }

    "access_providers/{name}" / {
      once("GET works") {
        for {
          db <- aDatabase
          accessProvider <- anAccessProvider(db)
        } {
          val res = api.get(s"/access_providers/${accessProvider.name}", db.adminKey)
          res should respond(OK)
          res.resource shouldBe accessProvider.resource
        }
      }

      once("PATCH works") {
        for {
          db <- aDatabase
          accessProvider <- anAccessProvider(db)
          otherName <- aName
        } {
          val res =
            api.patch(
              s"/access_providers/${accessProvider.name}",
              JSObject("name" -> otherName),
              db.adminKey)

          res should respond(OK)
          (res.resource / "ref" / "@ref" / "id") shouldBe JSString(otherName)
          (res.resource / "name") shouldBe JSString(otherName)
        }
      }

      once("PUT works") {
        for {
          db <- aDatabase
          accessProvider <- anAccessProvider(db)
          role <- aRole(db)
        } {
          val roles = JSArray(role.refObj)

          val res = api.put(
            s"/access_providers/${accessProvider.name}",
            JSObject(
              "roles" -> roles
            ),
            db.adminKey)

          res should respond(OK)
          (res.resource / "ref" / "@ref" / "id")
            .as[String] shouldBe accessProvider.name
          (res.resource / "name").as[String] shouldBe accessProvider.name
          (res.resource / "roles") shouldBe roles
        }
      }

      once("DELETE works") {
        for {
          db <- aDatabase
          accessProvider <- anAccessProvider(db)
        } {
          api.delete(
            s"/access_providers/${accessProvider.name}",
            db.adminKey) should respond(OK)
          api.get(
            s"/access_providers/${accessProvider.name}",
            db.adminKey) should respond(NotFound)
        }
      }
    }
  }
}
