package fauna.api.test

import fauna.codex.json._
import fauna.prop.api._

class RoleSpec extends API21Spec {

  "roles" / {
    once("GET works") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        role <- aRole(db, Privilege.open(cls.refObj))
      } {
        val res = api.get("/roles", db.adminKey)
        res should respond (OK)
        (res.resource / "data") should containElem (role.refObj)
      }
    }

    "roles/{name}" / {
      once("GET works") {
        for {
          db <- aDatabase
          cls <- aCollection(db)
          role <- aRole(db, Privilege.open(cls.refObj))
        } {
          val res = api.get(s"/roles/${role.name}", db.adminKey)
          res should respond (OK)
          res.resource should equal (role.resource)
        }
      }

      once("PATCH works") {
        for {
          db <- aDatabase
          cls <- aCollection(db)
          role <- aRole(db, Privilege.open(cls.refObj))
          otherName <- aName
        } {
          val res =
            api.patch(s"/roles/${role.name}", JSObject("name" -> otherName), db.adminKey)

          res should respond(OK)
          (res.resource / "ref" / "@ref" / "id") should equal(JSString(otherName))
          (res.resource / "name") should equal(JSString(otherName))
        }
      }

      once("PUT works") {
        for {
          db <- aDatabase
          cls <- aCollection(db)
          role <- aRole(db, Privilege.open(cls.refObj))
        } {
          val privileges = JSObject(
            "resource" -> cls.refObj,
            "actions" -> JSObject(
              "write" -> JSTrue
            )
          )

          val res = api.put(s"/roles/${role.name}", JSObject(
            "privileges" -> privileges
          ), db.adminKey)

          res should respond (OK)
          (res.resource / "ref" / "@ref" / "id").as[String] should equal (role.name)
          (res.resource / "name").as[String] should equal (role.name)
          (res.resource / "privileges") should equal (privileges)
        }
      }

      once("DELETE works") {
        for {
          db <- aDatabase
          cls <- aCollection(db)
          role <- aRole(db, Privilege.open(cls.refObj))
        } {
          api.delete(s"/roles/${role.name}", db.adminKey) should respond (OK)
          api.get(s"/roles/${role.name}", db.adminKey) should respond (NotFound)
        }
      }
    }
  }
}
