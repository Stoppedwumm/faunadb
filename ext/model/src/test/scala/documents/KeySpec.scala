package fauna.model.test

import fauna.ast._
import fauna.atoms._
import fauna.auth._
import fauna.codex.json._
import fauna.lang.clocks.Clock
import fauna.model._
import fauna.model.runtime.Effect
import fauna.repo._
import fauna.repo.test.{ CassandraHelper, MapVOps }
import fauna.storage._
import fauna.storage.doc._
import fauna.storage.ir._
import scala.util.Random

class KeySpec extends Spec {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")

  "KeySpec" - {
    val scope = ctx ! newScope
    val scopeAdmin = Auth.adminForScope(scope)

    def patch(data: Data, auth: Auth) = {
      val prev = Data.empty
      val ev = EvalContext(
        auth,
        Clock.time,
        None,
        Effect.Limit(Effect.Write, ""),
        APIVersion.Default)

      val validator =
        Key.LiveValidator(ev) +
          Key.SecretValidator(ev.scopeID, None)

      ctx ! validator.patch(prev, prev diffTo data)
    }

    def patchError(data: Data, auth: Auth = RootAuth) = {
      patch(data, auth) match {
        case Left(v)  => v
        case Right(_) => Nil
      }
    }

    def patchData(data: Data, auth: Auth = RootAuth) = {
      patch(data, auth) match {
        case Left(errs)  => fail(s"Unexpected errors: $errs")
        case Right(data) => data
      }
    }

    "bad secret" in {
      val dbID = (ctx ! Database.forScope(scope)).get.id
      val data = MapV("secret" -> "badsecret",
                      "role" -> "server",
                      "database" -> DocIDV(dbID.toDocID)).toData
      patchError(data) should equal(List(InvalidSecret(List("secret"))))
    }

    "respect filterMask" in {
      val dbID = (ctx ! Database.forScope(scope)).get.id

      val data = MapV(
        "role" -> "admin",
        "database" -> DocIDV(dbID.toDocID),
        "should_ignore" -> "Should Ignore"
      ).toData

      val expected = MapV(
        "role" -> "admin",
        "database" -> DocIDV(dbID.toDocID)
      ).toData

      patchData(data) should equal(expected)
    }

    "accept user defined roles" in {
      ctx ! mkCollection(scopeAdmin, MkObject("name" -> "class_a"))

      def mkRole(name: String): DocIDV = {
        ctx ! runQuery(
          scopeAdmin,
          CreateRole(
            MkObject("name" -> name,
                     "privileges" -> JSArray(
                       MkObject("resource" -> ClassRef("class_a"),
                                "actions" -> MkObject("read" -> true)))))
        ) match {
          case VersionL(v, _) => DocIDV(v.id)
          case _              => fail(s"Failed to create role $name")
        }
      }

      val roleA = mkRole("role_a")
      val roleB = mkRole("role_b")

      patchError(MapV("role" -> roleA).toData, scopeAdmin).isEmpty should be(true)
      patchError(MapV("role" -> ArrayV(roleA, roleB)).toData, scopeAdmin).isEmpty should be(true)
    }

    "duplicate hash field" in {
      val scope2 = ctx ! newScope
      val scope2Admin = Auth.adminForScope(scope2)

      ctx ! runQuery(scopeAdmin,
                     CreateDatabase(MkObject("name" -> "foo")))

      ctx ! runQuery(scope2Admin,
                     CreateDatabase(MkObject("name" -> "foo")))

      val (_, key) = ctx ! mkKey("foo", "server", scopeAdmin)

      val q = CreateF(Ref(s"keys/${key.id.toLong}"),
                      Quote(
                        JSObject("database" -> Ref("databases/foo"),
                                 "role" -> "server",
                                 "hashed_secret" -> key.hashedSecret)))

      val key2 = ctx ! evalQuery(scope2Admin, q) match {
        case Left(errs) => errs
        case Right(_)   => Nil
      }

      key2 should equal(
        List(
          ValidationError(List(DuplicateValue(Key.HashField.path)), RootPosition at "create")))
    }

    "sibling keys are indexed into LookupStore" in {
      val db1 = {
        val scope = ctx ! newScope
        (ctx ! Database.forScope(scope)).get
      }

      val (_, key) = ctx ! mkKey(db1.name, "server", RootAuth)

      val create = ctx ! Store.lookups(key.globalID).flattenT
      create.size should be(1)

      val createL = create.head
      createL.globalID should equal(key.globalID)
      createL.scope should equal(db1.parentScopeID)
      createL.id should equal(key.id.toDocID)
      createL.action should equal(Add)

      val db2 = {
        val scope = ctx ! newScope
        (ctx ! Database.forScope(scope)).get
      }

      ctx ! runQuery(
        RootAuth,
        Update(
          Ref(s"keys/${key.id.toLong}"),
          MkObject("database" -> DatabaseRef(db2.name))))

      // Lookups remain unchanged across updates.
      val update = ctx ! Store.lookups(key.globalID).flattenT
      update should equal(create)

      ctx ! runQuery(RootAuth, DeleteF(Ref(s"keys/${key.id.toLong}")))
      (ctx ! Store.keys(key.globalID).flattenT).isEmpty should be(true)
    }

    "child keys are indexed into LookupStore" in {
      val scope = ctx ! newScope
      val auth = Auth.adminForScope(scope)
      val db = (ctx ! Database.forScope(scope)).get

      val (_, key) = ctx ! mkCurrentDBKey("server", auth)

      val create = ctx ! Store.lookups(key.globalID).flattenT
      create.size should be(1)

      val createL = create.head
      createL.globalID should equal(key.globalID)
      createL.scope should equal(db.scopeID)
      createL.id should equal(key.id.toDocID)
      createL.action should equal(Add)

      val subScope = ctx ! newScope(auth)
      val child = (ctx ! Database.forScope(subScope)).get

      ctx ! runQuery(
        auth,
        Update(
          Ref(s"keys/${key.id.toLong}"),
          MkObject("database" -> DatabaseRef(child.name))))

      // Lookups remain unchanged across updates.
      val update = ctx ! Store.lookups(key.globalID).flattenT
      update should equal(create)

      ctx ! runQuery(auth, DeleteF(Ref(s"keys/${key.id.toLong}")))
      (ctx ! Store.keys(key.globalID).flattenT).isEmpty should be(true)
    }
  }

  "Admin Key" - {
    val scope = ctx ! newScope
    val admin = Auth.adminForScope(scope)

    "can create databases" in {
      val q = runQuery(admin,
                       CreateDatabase(
                               MkObject("name" -> Random.nextInt().toString)))

      noException should be thrownBy { ctx ! q }
    }

    "can create keys" in {
      val subScope = ctx ! newScope(admin)
      val subDB = (ctx ! Database.forScope(subScope)).get.name
      noException should be thrownBy {
        ctx ! mkKey(subDB, "server", admin)
      }
    }

    def checkCreated(name: String, singularName: String, id: CollectionID) =
      ctx ! evalQuery(
        admin,
        CreateF(Ref(name),
                MkObject("name" -> Random.nextInt().toString,
                         "source" -> Ref("tokens")))
      ) match {
        case Right(VersionL(v, n)) if v.collID == id && n => ()
        case e => sys.error(s"Expected $singularName, got: $e")
      }

    "can create indexes" in {
      checkCreated("indexes", "an index", IndexID.collID)
    }

    "can create classes" in {
      checkCreated("classes", "a class", CollectionID.collID)
    }
  }
}
