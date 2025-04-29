package fauna.api.test

import fauna.api.ReplicaTypeNames
import fauna.atoms.ScopeID
import fauna.codex.json._
import fauna.limits.AccountLimits
import fauna.model.Database
import fauna.net.http.{ Body, ContentType, HTTPHeaders, HttpResponse, NoBody }
import fauna.prop.Prop
import fauna.repo.store.{ DatabaseTree, DumpEntry }
import java.io.{ File, FileOutputStream }
import java.util.{ UUID => JUUID }
import scala.concurrent.duration._
import scala.concurrent.Future

class AdminSpec extends API27Spec {
  "ping" / {
    test("defaults to write scope") {
      eventually(timeout(10.seconds)) {
        val res = api.get("/ping")

        res should respond(OK)
        (res.json / "resource").as[String] should equal("Scope write is OK")
      }
    }

    test("uses passed timeout") {
      val res = api.get("/ping", query = "timeout=0")

      res should respond(ServiceUnavailable)
      (res.json / "errors" / 0 / "description").as[String] should equal(
        "Scope write timed out after 0 milliseconds")
    }

    test("responds to node scope") {
      val res = api.get("/ping", query = "scope=node")

      res should respond(OK)
      (res.json / "resource").as[String] should equal("Scope node is OK")
    }

    test("responds to read scope") {
      val res = api.get("/ping", query = "scope=read")

      res should respond(OK)
      (res.json / "resource").as[String] should equal("Scope read is OK")
    }

    test("responds to (deprecated) local scope") {
      val res = api.get("/ping", query = "scope=local")

      res should respond(OK)
      (res.json / "resource").as[String] should equal("Scope read is OK")
    }

    test("responds to write scope") {
      val res = api.get("/ping", query = "scope=write")

      res should respond(OK)
      (res.json / "resource").as[String] should equal("Scope write is OK")
    }

    test("responds to (deprecated) global scope") {
      val res = api.get("/ping", query = "scope=global")

      res should respond(OK)
      (res.json / "resource").as[String] should equal("Scope write is OK")
    }

    test("responds to all scope") {
      eventually(timeout(10.seconds)) {
        val res = api.get("/ping", query = "scope=all")

        res should respond(OK)
        (res.json / "resource").as[String] should equal("Scope all is OK")
      }
    }

    test("unknown scope is treated as node") {
      val res = api.get("/ping", query = "scope=foobar")

      res should respond(OK)
      (res.json / "resource").as[String] should equal("Scope node is OK")
    }
  }

  "error" / {
    test("responds with an error") {
      api.base("").get("/error") should respond(InternalServerError)
    }
  }

  "admin/status" / {
    test("returns node status") {
      val res = admin.get("/admin/status", rootKey)

      (res.json / "nodes").as[Seq[JSObject]] foreach { node =>
        (node / "status").as[String] should equal("up")
        (node / "state").as[String] should equal("live")
        ((node / "ownership").as[Float] > 0) should equal(true)
        (node / "host_id").is[String] should be(true)
        (node / "address").is[String] should be(true)
        (node / "worker_id").is[String] should be(true)
      }
    }

    once("requires root key") {
      getRequiresRootKey("/admin/replication")
    }
  }

  "admin/replication" / {
    test("returns current replication") {
      val res = admin.get("/admin/replication", rootKey)

      res should respond(OK)
      (res.resource / "replicas").as[JSArray] should equal(
        JSArray(JSObject("name" -> "NoDC", "type" -> ReplicaTypeNames.Log)))
    }

    test("updates replication") {
      val res1 = admin.get("/admin/replication", rootKey)
      res1 should respond(OK)

      val res2 = admin.put("/admin/replication", res1.json / "resource", rootKey)
      res2 should respond(NoContent)
    }

    test("Returns ETag and checks If-Match") {
      val res1 = admin.get("/admin/replication", rootKey)
      res1 should respond(OK)

      val etag = res1.headers.get(HTTPHeaders.ETag)
      etag shouldNot equal(null)

      val res2 = admin.put("/admin/replication", res1.json / "resource", rootKey, headers = Seq(HTTPHeaders.IfMatch -> (etag + "foo")))
      res2 should respond(PreconditionFailed)

      val res3 = admin.put("/admin/replication", res1.json / "resource", rootKey, headers = Seq(HTTPHeaders.IfMatch -> etag))
      res3 should respond(NoContent)
    }

    once("requires root key") {
      getRequiresRootKey("/admin/replication")
    }
  }

  "admin/storage/version" / {
    test("returns current storage version") {
      val res = admin.get("/admin/storage/version", rootKey)

      res should respond(OK)
      val ver = (res.json / "resource" / "version").as[String]

      noException should be thrownBy {
        JUUID.fromString(ver)
      }
    }

    test("updates storage version") {
      val res1 = admin.get("/admin/storage/version", rootKey)
      res1 should respond(OK)
      val ver = (res1.json / "resource" / "version").as[String]

      val res2 = admin.put("/admin/storage/version", JSObject.empty, rootKey)
      res2 should respond(NoContent)

      val res3 = admin.get("/admin/storage/version", rootKey)
      res3 should respond(OK)
      (res3.json / "resource" / "version").as[String] should not equal (ver)
    }

    once("requires root key") {
      getRequiresRootKey("/admin/storage/version")
    }
  }

  "admin/reseed" / {
    test("re-seeds sample data") {
      val res = admin.post("/admin/reseed", JSObject.empty, rootKey)
      res should respond(NoContent)
    }

    once("requires root key") {
      postRequiresRootKey("/admin/reseed")
    }
  }

  "admin/identity" / {
    test("returns an UUID") {
      val res = admin.get("/admin/identity", rootKey)

      res should respond(OK)
      res.json should matchJSONPattern(
        "identity" -> "[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}")
    }

    once("requires root key") {
      getRequiresRootKey("/admin/identity")
    }
  }

  "admin/host-id" / {
    test("returns an UUID for a known node") {
      val res = admin.post("/admin/host-id", JSObject("node" -> admin.host), rootKey)

      res should respond(OK)
      res.json should matchJSONPattern(
        "identity" -> "[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}")
    }

    test("returns unknown") {
      val res = admin.post("/admin/host-id", JSObject("node" -> "unknown"), rootKey)

      res should respond(BadRequest)
    }

    once("requires root key") {
      getRequiresRootKey("/admin/host-id")
    }
  }

  "admin/remove_node" / {
    once("requires root key") {
      postRequiresRootKey("/admin/remove_node")
    }

    test("requires a nodeID") {
      emptyPostIsBad("/admin/remove_node", "No nodeID in the request")
    }

    test("Can remove a non-existent nodeID") {
      val uuid = JUUID.nameUUIDFromBytes("No such node".getBytes).toString
      val res =
        admin.post("/admin/remove_node", JSObject("nodeID" -> JS(uuid)), rootKey)
      res should respond(NoContent)
    }

    test("Can't remove last node in a replica") {
      val res1 = admin.get("/admin/identity", rootKey)
      res1 should respond(OK)
      val id = res1.json / "identity"

      val res2 = admin.post("/admin/remove_node", JSObject("nodeID" -> id), rootKey)
      res2 should respond(BadRequest)
      (res2.json / "errors" / 0 / "description").as[String] should endWith(
        " would leave its data replica NoDC empty. Update the replica to compute type first.")
    }
  }

  "admin/movement_status" / {
    once("requires root key") {
      getRequiresRootKey("/admin/movement_status")
    }

    test("Returns meaningful status") {
      val res = admin.get("/admin/movement_status", rootKey)
      res should respond(OK)
      // NOTE: status text comes from C* StorageService
      (res.resource / "movement_status").as[String] should equal(
        "No data movement is currently in progress.")
    }
  }

  "admin/init" / {
    once("requires root key") {
      postRequiresRootKey("/admin/init")
    }

    test("requires a replica_name") {
      emptyPostIsBad("/admin/init", "Replica name is required.")
    }
  }

  "admin/join" / {
    once("requires root key") {
      postRequiresRootKey("/admin/join")
    }

    test("requires a seed host") {
      emptyPostIsBad("/admin/join", "No seed in the request.")
    }

    test("requires replica name") {
      val res = admin.post("/admin/join", """{"seed":"127.0.0.2"}""", rootKey)
      assertBadRequest(res, "Replica name is required.")
    }
  }

  "admin/repair_task" / {
    once("requires root key") {
      postRequiresRootKey("/admin/repair_task")
    }
  }

  "admin/repair_task/status" / {
    once("requires root key") {
      postRequiresRootKey("/admin/repair_task/status")
    }
  }

  "admin/repair_task/cancel" / {
    once("requires root key") {
      postRequiresRootKey("/admin/repair_task/cancel")
    }
  }

  "admin/accounts" / {
    once("requires root key") {
      getRequiresRootKey("/admin/accounts/42/settings")
    }

    once("works") {
      for {
        account1        <- Prop.long
        account2        <- Prop.long
        accountWithNoDB <- Prop.long filter { i => i != account1 && i != account2 }
        tenants         <- aDatabase(apiVers)
        _               <- aContainerDB(apiVers, tenants, account1)
        _ <- aContainerDB(
          apiVers,
          tenants,
          account2,
          AccountLimits(
            Some(1.0),
            Some(2.0),
            Some(3.0),
            Some(4.0),
            Some(5.0),
            Some(6.0)))
      } {
        val missing =
          admin.get(s"/admin/accounts/$accountWithNoDB/settings", token = rootKey)
        missing should respond(NotFound)

        val unset = admin.get(s"/admin/accounts/$account1/settings", token = rootKey)
        unset should respond(OK)

        val unsetBody = unset.json / "resource"

        (unsetBody / "id").as[String] should equal(account1.toString)
        (unsetBody / "limits" / "read_ops" / "hard").as[String] should equal("N/A")
        (unsetBody / "limits" / "write_ops" / "hard").as[String] should equal("N/A")
        (unsetBody / "limits" / "compute_ops" / "hard").as[String] should equal(
          "N/A")
        (unsetBody / "limits" / "read_ops" / "soft").as[String] should equal("N/A")
        (unsetBody / "limits" / "write_ops" / "soft").as[String] should equal("N/A")
        (unsetBody / "limits" / "compute_ops" / "soft").as[String] should equal(
          "N/A")

        val set = admin.get(s"/admin/accounts/$account2/settings", token = rootKey)
        set should respond(OK)

        val setBody = set.json / "resource"

        (setBody / "id").as[String] should equal(account2.toString)
        (setBody / "limits" / "read_ops" / "hard").as[Double] should equal(1.0)
        (setBody / "limits" / "write_ops" / "hard").as[Double] should equal(2.0)
        (setBody / "limits" / "compute_ops" / "hard").as[Double] should equal(3.0)
        (setBody / "limits" / "read_ops" / "soft").as[Double] should equal(4.0)
        (setBody / "limits" / "write_ops" / "soft").as[Double] should equal(5.0)
        (setBody / "limits" / "compute_ops" / "soft").as[Double] should equal(6.0)
      }
    }
  }

  "admin/accounts/reprioritize" / {
    once("requires root key") {
      postRequiresRootKey("/admin/accounts/reprioritize")
    }

    once("requires account and priority") {
      for {
        account <- Prop.long
        priority <- Prop.int
      } {
        val none = admin.post("/admin/accounts/reprioritize", JSObject.empty, rootKey)
        assertBadRequest(none, "account_id and priority params required.")

        val acct = admin.post("/admin/accounts/reprioritize", JSObject("account_id" -> account), rootKey)
        assertBadRequest(acct, "account_id and priority params required.")

        val prio = admin.post("/admin/accounts/reprioritize", JSObject("priority" -> priority), rootKey)
        assertBadRequest(prio, "account_id and priority params required.")
      }
    }

    once("adjusts priority") {
      for {
        account <- Prop.long
        dummy <- aDatabase(apiVers) // Just to deepen the hierarchy.
        tenants <- aContainerDB(apiVers, dummy)
        parent <- aContainerDB(apiVers, tenants, account)
        _ <- aDatabase(apiVers, parent)
      } {
        val res = admin.post(
          "/admin/accounts/reprioritize",
          JSObject("account_id" -> account, "priority" -> 1),
          rootKey)
        res should respond (NoContent)
      }
    }
  }

  "admin/scope-from-account" / {
    once("works") {
      for {
        account <- Prop.long
        accountWithNoDB <- Prop.long filter { _ != account }
        tenants <- aDatabase(apiVers)
        _ <- aContainerDB(apiVers, tenants, account)
      } {
        // TODO: Add a tool that gets the scope for a database so we can verify the response here.
        // AFAIK, there's no other way to get the scope ID through the admin or client API.
        // Note that the functionality below the API level is tested in DatabaseSpec.
        val res = admin.get(s"/admin/scope-from-account", query = s"account-id=$account", token = rootKey)
        res should respond(OK)

        val noDBRes = admin.get(s"/admin/scope-from-account", query = s"account-id=$accountWithNoDB", token = rootKey)
        noDBRes should respond(NotFound)
      }
    }
  }

  "/admin/database/global-to-scope" / {
    once("requires root key") {
      postRequiresRootKey("/admin/database/global-to-scope")
    }

    once("validates input") {
      for {
        _ <- aDatabase(apiVers)
      } {
        // missing field
        assertBadRequest(
          admin.post("/admin/database/global-to-scope", JSObject.empty, rootKey),
          "Missing 'global_id' field."
        )

        // invalid global_id
        assertBadRequest(
          admin.post(
            "/admin/database/global-to-scope",
            JSObject("global_id" -> "1234"),
            rootKey),
          "Invalid global id '1234'"
        )

        // missing database
        admin.post(
          "/admin/database/global-to-scope",
          JSObject("global_id" -> "yyyyyyyyyyyy9"),
          rootKey) should respond(NotFound)
      }
    }

    once("works") {
      for {
        db <- aDatabase(apiVers)
      } {
        val res = admin.post(
          "/admin/database/global-to-scope",
          JSObject("global_id" -> db.globalID),
          rootKey)

        (res.json / "scope_id").as[Long] should be > 0L
      }
    }
  }

  "admin/database/move" / {
    once("requires root key") {
      postRequiresRootKey("/admin/database/move")
    }

    once("validates input") {
      for {
        fromAccountID <- Prop.long
        toAccountID <- Prop.long
        name <- Prop.alphaString()

        fromDB <- aDatabase(apiVers)
        toDB <- aDatabase(apiVers)

        fromGlobalID = fromDB.globalID
        toGlobalID <- toDB.globalID
      } {
        assertBadRequest(
          admin.post("/admin/database/move", JSObject.empty, rootKey),
          "Invalid combination of 'from', 'to' and 'name' fields."
        )

        //account id
        assertBadRequest(
          admin.post("/admin/database/move", JSObject("from" -> fromAccountID), rootKey),
          "Invalid combination of 'from', 'to' and 'name' fields."
        )

        assertBadRequest(
          admin.post("/admin/database/move", JSObject("to" -> toAccountID), rootKey),
          "Invalid combination of 'from', 'to' and 'name' fields."
        )

        assertBadRequest(
          admin.post("/admin/database/move", JSObject("from" -> fromAccountID, "to" -> toAccountID), rootKey),
          "Name field is mandatory when used with Account ID."
        )

        //global id
        assertBadRequest(
          admin.post("/admin/database/move", JSObject("from" -> fromGlobalID), rootKey),
          "Invalid combination of 'from', 'to' and 'name' fields."
        )

        assertBadRequest(
          admin.post("/admin/database/move", JSObject("to" -> toGlobalID), rootKey),
          "Invalid combination of 'from', 'to' and 'name' fields."
        )

        assertBadRequest(
          admin.post("/admin/database/move", JSObject("from" -> "xyz", "to" -> toGlobalID), rootKey),
          "Invalid global id 'xyz'"
        )

        assertBadRequest(
          admin.post("/admin/database/move", JSObject("from" -> fromGlobalID, "to" -> "abc"), rootKey),
          "Invalid global id 'abc'"
        )

        assertBadRequest(
          admin.post("/admin/database/move", JSObject("from" -> fromGlobalID, "to" -> toGlobalID, "name" -> name), rootKey),
          "Name field cannot be used along with Global ID."
        )

        assertBadRequest(
          admin.post("/admin/database/move", JSObject("from" -> fromGlobalID, "to" -> fromGlobalID), rootKey),
          "Target and destination databases cannot be the same."
        )
      }
    }

    once("works with account id") {
      for {
        from <- Prop.long
        to <- Prop.long
        root <- aDatabase(apiVers)
        src <- aContainerDB(apiVers, root, from)
        dest <- aContainerDB(apiVers, root, to)
        target <- aDatabase(apiVers, src)
      } {

        api.query(Get(target.refObj), src.adminKey) should respond (OK)
        api.query(Get(target.refObj), dest.adminKey) should respond (BadRequest)

        val res = admin.post(
          "/admin/database/move",
          JSObject("from" -> from, "to" -> to, "name" -> target.name), rootKey)
        res should respond (NoContent)

        api.query(Get(target.refObj), src.adminKey) should respond (BadRequest)
        api.query(Get(target.refObj), dest.adminKey) should respond (OK)
      }
    }

    once("works with global id") {
      for {
        root <- aDatabase(apiVers)
        src <- aContainerDB(apiVers, root)
        dest <- aContainerDB(apiVers, root)
        target <- aDatabase(apiVers, src)
      } {

        api.query(Get(target.refObj), src.adminKey) should respond (OK)
        api.query(Get(target.refObj), dest.adminKey) should respond (BadRequest)

        val res = admin.post(
          "/admin/database/move",
          JSObject("from" -> target.globalID, "to" -> dest.globalID), rootKey)
        res should respond (NoContent)

        api.query(Get(target.refObj), src.adminKey) should respond (BadRequest)
        api.query(Get(target.refObj), dest.adminKey) should respond (OK)
      }
    }
  }

  "admin/database/recover" / {
    once("requires root key") {
      getRequiresRootKey("/admin/database/recover")
    }

    once("requires scope_id") {
      for {
        _ <- aDatabase
      } {
        assertBadRequest(
          admin.post("/admin/database/recover", JSObject.empty, rootKey),
          "Invalid or missing 'scope_id' fields."
        )
      }
    }

    once("requires a deleted scope_id") {
      for {
        db <- aDatabase
      } {
        // get db's scope id via admin
        val globalToScope =
          admin.post(
            "/admin/database/global-to-scope",
            JSObject("global_id" -> db.globalID),
            rootKey
          )
        globalToScope should respond(OK)
        val scopeID = (globalToScope.json / "scope_id").as[Long]

        assertBadRequest(
          admin.post(
            "/admin/database/recover",
            JSObject("scope_id" -> scopeID.toString),
            rootKey
          ),
          "Database is not deleted."
        )
      }
    }

    once("dry-run do not restore the database") {
      for {
        db <- aDatabase
        _ = api.query(DeleteF(db.refObj), rootKey) should respond(OK)
      } {
        // get db's scope id via admin
        val globalToScope =
          admin.post(
            "/admin/database/global-to-scope",
            JSObject("global_id" -> db.globalID),
            rootKey
          )
        globalToScope should respond(OK)
        val scopeID = (globalToScope.json / "scope_id").as[Long]

        // dry-run recovery
        val recover =
          admin.post(
            "/admin/database/recover",
            JSObject(
              "scope_id" -> scopeID.toString,
              "dry_run" -> true
            ),
            rootKey
          )
        recover should respond(OK)

        (recover.json / "recovered_dbs").as[Seq[String]] should
          contain.only(db.name)

        val restoredCheck = api.query(Exists(db.refObj), rootKey)
        restoredCheck should respond(OK)

        assert(
          !(restoredCheck.json / "resource").as[Boolean],
          "Dry run recovered the deleted database"
        )
      }
    }

    once("recovers a deleted database") {
      for {
        rootDB   <- aDatabase
        childDB1 <- aDatabase(apiVers, rootDB)
        childDB2 <- aDatabase(apiVers, rootDB)
        coll     <- aCollection(rootDB)
        doc      <- aDocument(coll)

        // childDB1 gets deleted normally; childDB2 is deleted with rootDB
        _ = api.query(DeleteF(childDB1.refObj), rootDB.adminKey) should respond(OK)
        _ = api.query(DeleteF(rootDB.refObj), rootKey) should respond(OK)
      } {
        // get rootDB's scope id via admin
        val globalToScope =
          admin.post(
            "/admin/database/global-to-scope",
            JSObject("global_id" -> rootDB.globalID),
            rootKey
          )
        globalToScope should respond(OK)
        val scopeID = (globalToScope.json / "scope_id").as[Long]

        // recover rootDB
        val recover =
          admin.post(
            "/admin/database/recover",
            JSObject("scope_id" -> scopeID.toString),
            rootKey
          )
        recover should respond(OK)

        (recover.json / "recovered_dbs").as[Seq[String]] should
          contain.only(
            rootDB.name,
            s"${rootDB.name}/${childDB2.name}"
          )

        val rootDBCheck = api.query(Exists(rootDB.refObj), rootKey)
        rootDBCheck should respond(OK)

        assert(
          (rootDBCheck.json / "resource").as[Boolean],
          "Restore did not bring back deleted database"
        )

        val newKey =
          api.query(
            CreateKey(
              MkObject(
                "database" -> rootDB.refObj,
                "role" -> "admin"
              )),
            rootKey
          )
        newKey should respond(Created)

        val newSecret = (newKey.resource / "secret").as[String]
        val contentsCheck =
          api.query(
            And(
              Not(Exists(childDB1.refObj)), // not deleted in the same txn
              Exists(childDB2.refObj), // deleted in the same txn
              Exists(coll.refObj), // part of the deleted db schema
              Exists(doc.refObj) // part of the deleted db data
            ),
            newSecret
          )
        contentsCheck should respond(OK)

        assert(
          (contentsCheck.json / "resource").as[Boolean],
          "Incorrect database state after recover"
        )
      }
    }
  }

  "admin/backup/dump-tree" / {
    once("requires root key") {
      getRequiresRootKey("/admin/backup/dump-tree")
    }

    once("works") {
      for {
        account <- Prop.long
        root <- aDatabase(apiVers)
        tenants <- aContainerDB(apiVers, root)
        tenantDB <- aContainerDB(apiVers, tenants, account)
        customer0 <- aContainerDB(apiVers, tenantDB)
        customer1 <- aContainerDB(apiVers, tenantDB)
      } {
        val res = admin.get(
          "/admin/backup/dump-tree",
          token = rootKey)

        res should respond(OK)

        val entries = DumpEntry.fromJSON(res.json)

        def assertDB(globalID: String): Unit = {
          val decoded = Database.decodeGlobalID(globalID).get

          //find global_id entry
          val globalIDEntry = entries find { entry =>
            entry.globalID == decoded
          }

          globalIDEntry.isDefined shouldBe true

          //make sure correspondent scope_id entry exists
          entries exists { entry =>
            entry.globalID match {
              case ScopeID(_) => entry.dbID == globalIDEntry.get.dbID
              case _          => false
            }
          } shouldBe true
        }

        assertDB(root.globalID)
        assertDB(tenants.globalID)
        assertDB(tenantDB.globalID)
        assertDB(customer0.globalID)
        assertDB(customer1.globalID)

        val file = File.createTempFile("tree", "json")
        val fis = new FileOutputStream(file)
        try {
          res.body.readBytes(fis, res.body.readableBytes)

          val check = admin.post(
            "/admin/backup/check-tree",
            body =
              Body(JSObject("path" -> file.toString).toString, ContentType.JSON),
            token = rootKey)

          check should respond(OK)
        } finally {
          fis.close()
          file.delete()
        }
      }
    }
  }

  "admin/backup/check-tree" / {
    once("requires root key") {
      postRequiresRootKey("/admin/backup/check-tree")
    }

    test("requires a path") {
      emptyPostIsBad("/admin/backup/check-tree", "Missing 'path' parameter.")
    }
  }

  "admin/backup/allocate-scopes" / {
    once("requires root key") {
      postRequiresRootKey("/admin/backup/allocate-scopes")
    }

    def decode(globalID: String) =
      Database.decodeGlobalID(globalID).get

    once("validate inputs") {
      for {
        db <- aDatabase
      } {
        //global_id is required
        val res0 = admin.post(
          "/admin/backup/allocate-scopes",
          body = JSObject(
            "entries" -> JSArray(
              JSObject("type" -> "scope_id", "id" -> "1", "parent_id" -> "0", "db_id" -> "1"),
              JSObject("type" -> "global_id", "id" -> "yyyyyyyyyyyyy", "parent_id" -> "0", "db_id" -> "1")
            )
          ),
          token = rootKey)

        res0 should respond(BadRequest)
        (res0.json / "errors" / 0).as[JSObject] shouldBe JSObject(
          "code" -> "bad request",
          "description" -> "global_id param required."
        )

        //invalid dump-tree
        val res1 = admin.post(
          "/admin/backup/allocate-scopes",
          query = s"global_id=${db.globalID}",
          body = NoBody,
          token = rootKey)

        res1 should respond(BadRequest)
        (res1.json / "errors" / 0).as[JSObject] shouldBe JSObject(
          "code" -> "bad request",
          "description" -> "invalid dump-tree."
        )
      }
    }

    once("cannot import tree on itself") {
      for {
        account <- Prop.long
        root <- aDatabase(apiVers)
        tenantDB <- aContainerDB(apiVers, root, account)
        userDB <-  aContainerDB(apiVers, tenantDB)
      } {
        val treeF = admin.get(
          "/admin/backup/dump-tree",
          token = rootKey)
        treeF should respond (OK)

        val root = DatabaseTree.fromJSON(treeF.json)
        val node = root.forGlobalID(decode(userDB.globalID)).get

        val res = admin.post(
          "/admin/backup/allocate-scopes",
          body = node.toJSON,
          query = s"global_id=${userDB.globalID}",
          token = rootKey)

        res should respond(BadRequest)
        (res.json / "errors" / 0).as[JSObject] shouldBe JSObject(
          "code" -> "bad request",
          "description" -> "cannot import a tree on itself."
        )
      }
    }

    once("works preserving global ids") {
      for {
        account <- Prop.long
        root <- aDatabase(apiVers)
        tenantDB <- aContainerDB(apiVers, root, account)
        userDB <-  aContainerDB(apiVers, tenantDB)
        _ <- aContainerDB(apiVers, userDB)
        _ <- aContainerDB(apiVers, userDB)
      } {
        val treeF = admin.get(
          "/admin/backup/dump-tree",
          token = rootKey)
        treeF should respond (OK)

        val root = DatabaseTree.fromJSON(treeF.json)
        val node = root.forGlobalID(decode(userDB.globalID)).get

        val res = admin.post(
          "/admin/backup/allocate-scopes",
          body = node.toJSON,
          query = s"global_id=${tenantDB.globalID}&preserve_global_ids=true",
          token = rootKey)

        res should respond(OK)
        val mapping = (res.json / "mapping").as[Seq[JSValue]]

      def assertMapping(db: DatabaseTree): Unit = {
          val scope = db.scopeID.toLong.toString
          val global = Database.encodeGlobalID(db.globalID)

          val allocated = mapping find { x => (x / "old_scope_id").as[String] == scope } get

          (allocated / "old_scope_id").as[String] shouldBe scope
          (allocated / "new_scope_id").as[String] shouldNot be (scope)
          (allocated / "old_global_id").as[String] shouldBe global
          (allocated / "new_global_id").as[String] shouldBe global //preserve_global_ids=true
        }

        //root mapping should be the same
        val rootMapping = mapping.head
        (rootMapping / "old_scope_id") shouldBe (rootMapping / "new_scope_id")
        (rootMapping / "old_global_id").as[String] shouldBe tenantDB.globalID
        (rootMapping / "new_global_id").as[String] shouldBe tenantDB.globalID

        assertMapping(node)
        assertMapping(node.children(0))
        assertMapping(node.children(1))
      }
    }

    once("works creating new global ids") {
      for {
        account <- Prop.long
        root <- aDatabase(apiVers)
        tenantDB <- aContainerDB(apiVers, root, account)
        userDB <- aContainerDB(apiVers, tenantDB)
        _ <- aContainerDB(apiVers, userDB)
        _ <- aContainerDB(apiVers, userDB)
      } {
        val treeF = admin.get(
          "/admin/backup/dump-tree",
          token = rootKey)
        treeF should respond (OK)

        val root = DatabaseTree.build(DumpEntry.fromJSON(treeF.json))
        val node = root.forGlobalID(decode(userDB.globalID)).get

        val res = admin.post(
          "/admin/backup/allocate-scopes",
          query = s"global_id=${tenantDB.globalID}&preserve_global_ids=false",
          body = node.toJSON,
          token = rootKey)

        res should respond(OK)
        val mapping = (res.json / "mapping").as[Seq[JSValue]]

        def assertMapping(db: DatabaseTree): Unit = {
          val scope = db.scopeID.toLong.toString
          val global = Database.encodeGlobalID(db.globalID)

          val allocated = mapping find { x => (x / "old_scope_id").as[String] == scope } get

          (allocated / "old_scope_id").as[String] shouldBe scope
          (allocated / "new_scope_id").as[String] shouldNot be (scope)
          (allocated / "old_global_id").as[String] shouldBe global
          (allocated / "new_global_id").as[String] shouldNot be (global) //preserve_global_ids=false
        }

        //root mapping should be the same
        val rootMapping = mapping.head
        (rootMapping / "old_scope_id") shouldBe (rootMapping / "new_scope_id")
        (rootMapping / "old_global_id").as[String] shouldBe tenantDB.globalID
        (rootMapping / "new_global_id").as[String] shouldBe tenantDB.globalID

        assertMapping(node)
        assertMapping(node.children(0))
        assertMapping(node.children(1))
      }
    }

    once("allocate on different hierarchy") {
      for {
        account <- Prop.long
        root <- aDatabase(apiVers)
        tenantDB <- aContainerDB(apiVers, root, account)
        userDB1 <-  aContainerDB(apiVers, tenantDB)
        _ <- aContainerDB(apiVers, userDB1)
        _ <- aContainerDB(apiVers, userDB1)
        userDB2 <-  aContainerDB(apiVers, tenantDB)
      } {
        val treeF = admin.get(
          "/admin/backup/dump-tree",
          token = rootKey)
        treeF should respond (OK)

        val root = DatabaseTree.build(DumpEntry.fromJSON(treeF.json))
        val node = root.forGlobalID(decode(userDB1.globalID)).get

        val res = admin.post(
          "/admin/backup/allocate-scopes",
          body = node.toJSON,
          query = s"global_id=${userDB2.globalID}&preserve_global_ids=false",
          token = rootKey)

        res should respond(OK)
        val mapping = (res.json / "mapping").as[Seq[JSValue]]

        def assertMapping(db: DatabaseTree): Unit = {
          val scope = db.scopeID.toLong.toString
          val global = Database.encodeGlobalID(db.globalID)

          val allocated = mapping find { x => (x / "old_scope_id").as[String] == scope } get

          (allocated / "old_scope_id").as[String] shouldBe scope
          (allocated / "new_scope_id").as[String] shouldNot be (scope)
          (allocated / "old_global_id").as[String] shouldBe global
          (allocated / "new_global_id").as[String] shouldNot be (global) //preserve_global_ids=false
        }

        //root mapping should be different
        val rootMapping = mapping.head
        (rootMapping / "old_scope_id") shouldNot be (rootMapping / "new_scope_id")
        (rootMapping / "old_global_id").as[String] shouldBe userDB2.globalID
        (rootMapping / "new_global_id").as[String] shouldBe userDB2.globalID

        assertMapping(node)
        assertMapping(node.children(0))
        assertMapping(node.children(1))
      }
    }
  }

  "admin/database/enable" / {
    once("requires root key") {
      postRequiresRootKey("/admin/database/enable")
    }

    once("works for specific database") {
      for {
        account <- Prop.long
        root <- aDatabase(apiVers)
        tenantDB <- aContainerDB(apiVers, root, account)
        userDB <-  aContainerDB(apiVers, tenantDB)
      } {
        //disable the database
        admin.post(
          "/admin/database/enable",
          body = NoBody,
          query = s"global_id=${userDB.globalID}&enable=false",
          token = rootKey
        ) should respond(OK)

        eventually(timeout(30.seconds)) {
          api.query(MkObject("foo" -> "bar"), root.adminKey) should respond (OK)
          api.query(MkObject("foo" -> "bar"), tenantDB.adminKey) should respond (OK)
          //try to execute a query on a disabled database should fail
          api.query(MkObject("foo" -> "bar"), userDB.adminKey) should respond(Unauthorized)
        }

        //enable the database
        admin.post(
          "/admin/database/enable",
          body = NoBody,
          query = s"global_id=${userDB.globalID}&enable=true",
          token = rootKey
        ) should respond(OK)

        eventually(timeout(30.seconds)) {
          api.query(MkObject("foo" -> "bar"), userDB.adminKey) should respond (OK)
        }
      }
    }

    once("works for the whole hierarchy") {
      for {
        account <- Prop.long
        root <- aDatabase(apiVers)
        tenantDB <- aContainerDB(apiVers, root, account)
        userDB <-  aContainerDB(apiVers, tenantDB)
      } {
        //disable the whole hierarchy
        admin.post(
          "/admin/database/enable",
          body = NoBody,
          query = s"global_id=${root.globalID}&enable=false",
          token = rootKey
        ) should respond(OK)

        eventually(timeout(30.seconds)) {
          api.query(MkObject("foo" -> "bar"), root.adminKey) should respond(Unauthorized)
          api.query(MkObject("foo" -> "bar"), tenantDB.adminKey) should respond(Unauthorized)
          api.query(MkObject("foo" -> "bar"), userDB.adminKey) should respond(Unauthorized)
        }

        //enable the a specific database should not work either
        admin.post(
          "/admin/database/enable",
          body = NoBody,
          query = s"global_id=${userDB.globalID}&enable=true",
          token = rootKey
        ) should respond(OK)

        eventually(timeout(30.seconds)) {
          api.query(MkObject("foo" -> "bar"), userDB.adminKey) should respond(Unauthorized)
        }

        //enable the whole hierarchy
        admin.post(
          "/admin/database/enable",
          body = NoBody,
          query = s"global_id=${root.globalID}&enable=true",
          token = rootKey
        ) should respond(OK)

        eventually(timeout(30.seconds)) {
          api.query(MkObject("foo" -> "bar"), root.adminKey) should respond(OK)
          api.query(MkObject("foo" -> "bar"), tenantDB.adminKey) should respond(OK)
          api.query(MkObject("foo" -> "bar"), userDB.adminKey) should respond(OK)
        }
      }
    }
  }

  "admin/document/location" / {
    once("requires root key") {
      getRequiresRootKey("/admin/document/location")
    }

    test("scopeID must exist") {
      val params =
        s"scope_id=1" :: s"collection_id=2" :: s"doc_id=3" :: Nil
      val res = admin.get(s"/admin/document/location", query = params.mkString("&"), token = rootKey)
      res should respond(BadRequest)
      res.errors.head.get("description") should be(JSString("scope_id 'ScopeID(1)' not found"))
    }

    test("scope_id collection_id doc_id must be present in params") {
      val bad_params = List(
        s"collection_id=2" :: s"doc_id=3" :: Nil,
        s"scope_id=1" :: s"doc_id=3" :: Nil,
        s"scope_id=1" :: s"collection_id=2" :: Nil
      )
      bad_params.foreach { params =>
        val res = admin.get(s"/admin/document/location", query = params.mkString("&"), token = rootKey)
        res should respond(BadRequest)
        res.errors.head.get("description") should be(JSString("scope_id, collection_id and doc_id params required."))
      }
    }
  }

  private def emptyPostIsBad(url: String, expectedErrorMessage: String) = {
    val res = admin.post(url, JSObject.empty, rootKey)
    assertBadRequest(res, expectedErrorMessage)
  }

  private def assertBadRequest(res: Future[HttpResponse], expectedErrorMessage: String) = {
    res should respond(BadRequest)
    // NOTE: description comes from AdminEndpoints
    (res.json / "errors" / 0 / "description").as[String] should equal(
      expectedErrorMessage)
  }

  private def getRequiresRootKey(url: String) =
    for {
      database <- aDatabase
    } {
      val res1 = admin.get(url)
      res1 should respond(Unauthorized)

      val res2 = admin.get(url, "lolwhat")
      res2 should respond(Unauthorized)

      val res3 = admin.get(url, database.key)
      res3 should respond(Unauthorized)
    }

  private def postRequiresRootKey(url: String) =
    for {
      database <- aDatabase
    } {
      val res1 = admin.post(url, JSObject.empty)
      res1 should respond(Unauthorized)

      val res2 = admin.post(url, JSObject.empty, "lolwhat")
      res2 should respond(Unauthorized)

      val res3 = admin.post(url, JSObject.empty, database.key)
      res3 should respond(Unauthorized)
    }
}
