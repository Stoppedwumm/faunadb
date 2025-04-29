package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.net.http.{ BearerAuth, RawAuth }
import fauna.prop.api.{ Membership, Privilege, RoleAction }
import fauna.util.Base64

class Auth21Spec extends QueryAPI21Spec {
  val time = "X-Query-Time"

  "unauthorized" - {
    once("cannot see stats output") {
      for {
        db <- aDatabase
      } {
        val q = JSObject("add" -> JSArray(1, 1))
        val good = api.query(q, db.key)

        good.headers contains time should equal (true)

        val bad = api.query(q, null)

        bad should respond (Unauthorized)
        bad.headers contains time should equal (false)
      }
    }

    once("cannot see trace output") {
      for {
        db <- aDatabase
      } {
        val q = JSObject("add" -> JSArray(1, 1))
        val good = api.trace(q, db.key)

        (good.json / "trace").isEmpty should be (false)

        val bad = api.trace(q, null)

        bad should respond (Unauthorized)
        (bad.json / "trace").isEmpty should be (true)
      }
    }
  }
}

class Auth20Spec extends QueryAPI20Spec {

  "server key" - {
    once("can scope a request to a specific document") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
        inst2 <- aDocument(cls)
      } {
        val scopedKey0 = s"${db.key}:classes/${cls.name}/${inst.id}"
        val scopedKey1 = s"${db.key}:collections/${cls.name}/${inst.id}"
        val scopedKey2 = s"${db.key}:@doc/${cls.name}/${inst.id}"

        runQuery(Exists(inst2.refObj), db) should equal (JSTrue)
        runQuery(Exists(inst2.refObj), db.clientKey) should equal (JSFalse)
        runQuery(Exists(inst2.refObj), scopedKey0) should equal (JSFalse)
        runQuery(Exists(inst2.refObj), scopedKey1) should equal (JSFalse)
        runQuery(Exists(inst2.refObj), scopedKey2) should equal (JSFalse)

        aRole(
          db,
          Membership(cls),
          Privilege(cls.refObj, read = RoleAction.Granted)).sample

        runQuery(Exists(inst2.refObj), scopedKey0) should equal (JSTrue)
        runQuery(Exists(inst2.refObj), scopedKey1) should equal (JSTrue)
        runQuery(Exists(inst2.refObj), scopedKey2) should equal (JSTrue)
      }
    }

    once("scoped request with invalid auth results in no auth") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        val scopedKey0 = s"${db.key}:classes/${cls.name}/123"
        val scopedKey1 = s"${db.key}:collections/${cls.name}/123"
        val scopedKey2 = s"${db.key}:@doc/${cls.name}/123"
        val scopedKey3 = s"${db.key}:@role/invalidRole"
        val scopedKey4 = s"${db.key}:sub/db:admin"
        val scopedKey5 = s"${db.key}:sub/db:classes/${cls.name}/123"
        val scopedKey6 = s"${db.key}:sub/db:collections/${cls.name}/123"
        val scopedKey7 = s"${db.key}:sub/db:@doc/${cls.name}/123"
        val scopedKey8 = s"${db.key}:sub/db:@role/invalidRole"

        runRawQuery(Exists(inst.refObj), scopedKey0) should respond (Unauthorized)
        runRawQuery(Exists(inst.refObj), scopedKey1) should respond (Unauthorized)
        runRawQuery(Exists(inst.refObj), scopedKey2) should respond (Unauthorized)
        runRawQuery(Exists(inst.refObj), scopedKey3) should respond (Unauthorized)
        runRawQuery(Exists(inst.refObj), scopedKey4) should respond (Unauthorized)
        runRawQuery(Exists(inst.refObj), scopedKey5) should respond (Unauthorized)
        runRawQuery(Exists(inst.refObj), scopedKey6) should respond (Unauthorized)
        runRawQuery(Exists(inst.refObj), scopedKey7) should respond (Unauthorized)
        runRawQuery(Exists(inst.refObj), scopedKey8) should respond (Unauthorized)
      }
    }

    once("cannot override roles") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        role <- aRole(db, Privilege(cls.refObj, read = RoleAction.Granted))
        inst <- aDocument(cls)
      } {
        val scopedKey0 = s"${db.key}:@role/${role.name}"
        val scopedKey1 = s"${db.key}:admin"
        val scopedKey2 = s"${db.key}:server"
        val scopedKey3 = s"${db.key}:server-readonly"
        val scopedKey4 = s"${db.key}:client"

        runRawQuery(Get(inst.refObj), scopedKey0) should respond (Unauthorized)
        runRawQuery(Update(inst.refObj, MkObject()), scopedKey0) should respond (Unauthorized)

        runRawQuery(Get(inst.refObj), scopedKey1) should respond (Unauthorized)
        runRawQuery(Get(inst.refObj), scopedKey2) should respond (Unauthorized)
        runRawQuery(Get(inst.refObj), scopedKey3) should respond (Unauthorized)
        runRawQuery(Get(inst.refObj), scopedKey4) should respond (Unauthorized)
      }
    }
  }

  "client key" - {
    once("cannot override scope") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        role <- aRole(db, Privilege(cls.refObj, read = RoleAction.Granted))
        inst <- aDocument(cls)
      } {
        val scopedKey0 = s"${db.clientKey}:@role/${role.name}"
        val scopedKey1 = s"${db.clientKey}:admin"
        val scopedKey2 = s"${db.clientKey}:server"
        val scopedKey3 = s"${db.clientKey}:server-readonly"
        val scopedKey4 = s"${db.clientKey}:client"
        val scopedKey5 = s"${db.clientKey}:@doc/${cls.name}/${inst.id}"
        val scopedKey6 = s"${db.clientKey}:collections/${cls.name}/${inst.id}"
        val scopedKey7 = s"${db.clientKey}:classes/${cls.name}/${inst.id}"

        runRawQuery(Get(inst.refObj), scopedKey0) should respond (Unauthorized)
        runRawQuery(Update(inst.refObj, MkObject()), scopedKey0) should respond (Unauthorized)

        runRawQuery(Get(inst.refObj), scopedKey1) should respond (Unauthorized)
        runRawQuery(Get(inst.refObj), scopedKey2) should respond (Unauthorized)
        runRawQuery(Get(inst.refObj), scopedKey3) should respond (Unauthorized)
        runRawQuery(Get(inst.refObj), scopedKey4) should respond (Unauthorized)
        runRawQuery(Get(inst.refObj), scopedKey5) should respond (Unauthorized)
        runRawQuery(Get(inst.refObj), scopedKey6) should respond (Unauthorized)
        runRawQuery(Get(inst.refObj), scopedKey7) should respond (Unauthorized)
      }
    }
  }

  "admin key" - {
    once("can override scope") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        role <- aRole(
          db,
          Membership(cls),
          Privilege(cls.refObj, read = RoleAction.Granted))
        inst <- aDocument(cls)
      } {
        val scopedKey0 = s"${db.adminKey}:@role/${role.name}"
        val scopedKey1 = s"${db.adminKey}:admin"
        val scopedKey2 = s"${db.adminKey}:server"
        val scopedKey3 = s"${db.adminKey}:server-readonly"
        val scopedKey4 = s"${db.adminKey}:client"
        val scopedKey5 = s"${db.adminKey}:@doc/${cls.name}/${inst.id}"
        val scopedKey6 = s"${db.adminKey}:collections/${cls.name}/${inst.id}"
        val scopedKey7 = s"${db.adminKey}:classes/${cls.name}/${inst.id}"

        runRawQuery(Get(inst.refObj), scopedKey0) should respond (OK)
        runRawQuery(Update(inst.refObj, MkObject()), scopedKey0) should respond (Forbidden)

        runRawQuery(Get(inst.refObj), scopedKey1) should respond (OK)
        runRawQuery(Get(inst.refObj), scopedKey2) should respond (OK)
        runRawQuery(Get(inst.refObj), scopedKey3) should respond (OK)
        runRawQuery(Get(inst.refObj), scopedKey4) should respond (Forbidden)
        runRawQuery(Get(inst.refObj), scopedKey5) should respond (OK)
        runRawQuery(Get(inst.refObj), scopedKey6) should respond (OK)
        runRawQuery(Get(inst.refObj), scopedKey7) should respond (OK)
      }
    }

    // This test demonstrates that trying to update or replace the
    // hashed secret of a key has no effect.
    // It also documents a funny but useful behavior where inserting a new
    // version of a key (whether it is for an existing key document or not)
    // strips the hashed_secret instead of replacing it.
    once("update keys only in allowed ways") {
      for {
        parent <- aDatabase
        child <- aDatabase(apiVers, parent)
      } {
        val childKey = runQuery(
          CreateKey(MkObject("database" -> child.refObj, "role" -> "server")),
          parent.adminKey)
        val (ref, hs) = (childKey / "ref", childKey / "hashed_secret")

        // It's not possible to update the hashed secret.
        runQuery(Update(ref, MkObject("hashed_secret" -> "taco")), parent.adminKey)
        (runQuery(Get(ref), parent.adminKey) / "hashed_secret") should equal (hs)

        // It's not possible to replace the hashed secret.
        runQuery(Replace(ref, MkObject("role" -> "server", "hashed_secret" -> "taco")), parent.adminKey)
        (runQuery(Get(ref), parent.adminKey) / "hashed_secret") should equal (hs)

        // Insert a new version of an existing key: the hashed secret will be stripped out.
        runQuery(InsertVers(ref, Now(), "create", MkObject("role" -> "admin", "hashed_secret" -> "taco")), parent.adminKey)
        runQuery(Get(ref), parent.adminKey).as[JSObject].keys should not contain ("hashed_secret")

        // Insert a new key: there'll be no hashed secret even though one was provided.
        val newRef = JSObject("@ref" -> "keys/302853774295171584")
        runQuery(InsertVers(newRef, Now(), "create", MkObject("role" -> "admin", "hashed_secret" -> "taco")), parent.adminKey)
        runQuery(Get(newRef), parent.adminKey).as[JSObject].keys should not contain ("hashed_secret")
      }
    }
  }

  "bearer" - {
    once("allow bearer auth with fauna keys") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        role <- aRole(
          db,
          Membership(cls),
          Privilege(cls.refObj, read = RoleAction.Granted))
        inst <- aDocument(cls)
      } {
        val scopedKey0 = BearerAuth(s"${db.adminKey}:@role/${role.name}")
        val scopedKey1 = BearerAuth(s"${db.adminKey}:admin")
        val scopedKey2 = BearerAuth(s"${db.adminKey}:server")
        val scopedKey3 = BearerAuth(s"${db.adminKey}:server-readonly")
        val scopedKey4 = BearerAuth(s"${db.adminKey}:client")
        val scopedKey5 = BearerAuth(s"${db.adminKey}:@doc/${cls.name}/${inst.id}")
        val scopedKey6 = BearerAuth(s"${db.adminKey}:collections/${cls.name}/${inst.id}")
        val scopedKey7 = BearerAuth(s"${db.adminKey}:classes/${cls.name}/${inst.id}")

        runRawQuery(Get(inst.refObj), scopedKey0) should respond (OK)
        runRawQuery(Update(inst.refObj, MkObject()), scopedKey0) should respond (Forbidden)

        runRawQuery(Get(inst.refObj), scopedKey1) should respond (OK)
        runRawQuery(Get(inst.refObj), scopedKey2) should respond (OK)
        runRawQuery(Get(inst.refObj), scopedKey3) should respond (OK)
        runRawQuery(Get(inst.refObj), scopedKey4) should respond (Forbidden)
        runRawQuery(Get(inst.refObj), scopedKey5) should respond (OK)
        runRawQuery(Get(inst.refObj), scopedKey6) should respond (OK)
        runRawQuery(Get(inst.refObj), scopedKey7) should respond (OK)
      }
    }
  }

  "errors" - {
    once("support buggy go driver") {
      for {
        db <- aDatabase
      } {
        def encode(str: String) = s"Basic ${Base64.encodeStandard(str.getBytes)}:"

        runRawQuery(1, new RawAuth(encode("secret"))) should respond (OK)
        runRawQuery(1, new RawAuth(encode(s"secret:${db.name}:admin"))) should respond (OK)
      }
    }

    once("invalid base64 keys") {
      for {
        _ <- aDatabase
      } {
        runRawQuery(1, new RawAuth("Basic non-base64")) should respond (Unauthorized)
      }
    }
  }
}
