package fauna.model.test

import fauna.atoms._
import fauna.auth._
import fauna.lang.Timestamp
import fauna.model.runtime.fql2.{
  FQLInterpreter,
  QueryCheckFailure,
  QueryRuntimeFailure
}
import fauna.model.runtime.fql2.stdlib.DatabaseDefCompanion
import fauna.model.Database
import fauna.prop._
import fauna.repo.store.CacheStore
import fauna.repo.values.Value
import fql.error.TypeError

class FQL2DatabaseSpec extends FQL2WithV4Spec {

  "FQL2DatabaseSpec" - {
    "create" - {
      "admin can create" in {
        val auth = newDB

        val db = evalOk(
          auth.withPermissions(AdminPermissions),
          """|let db = Database.create({
             |  name: "Foo",
             |  data: {
             |    foo: "bar"
             |  }
             |})
             |
             |db { name, coll, data }""".stripMargin
        )

        (db / "name") should matchPattern { case Value.Str("Foo") => }
        (db / "coll") shouldEqual DatabaseDefCompanion
        (db / "data" / "foo") should matchPattern { case Value.Str("bar") => }
      }

      "respect user role" in {
        val auth = newDB

        evalOk(
          auth.withPermissions(AdminPermissions),
          """|Role.create({
             |  name: "aRole",
             |  privileges: {
             |    resource: "Database",
             |    actions: {
             |      create: true
             |    }
             |  }
             |})""".stripMargin
        )

        val key = evalOk(
          auth.withPermissions(AdminPermissions),
          """|Key.create({
             |  role: "aRole"
             |})""".stripMargin
        )
        val secret = (getDocFields(auth, key) / "secret").as[String]

        evalOk(secret, """Database.create({name: "Foo"})""") should matchPattern {
          case Value.Doc(_, Some("Foo"), _, _, _) =>
        }

        evalOk(
          auth.withPermissions(AdminPermissions),
          """|Role.byName("aRole")!.update({
             |  privileges: {
             |    resource: "Database",
             |    actions: {
             |      create: false
             |    }
             |  }
             |})""".stripMargin
        )

        evalErr(secret, """Database.create({name: "Foo"})""") should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }
      }

      "server & client cannot create" in {
        val auth = newDB

        def assertWithPerm(perm: Permissions): Unit =
          evalErr(
            auth.withPermissions(perm),
            """Database.create({name: "Foo"})"""
          ) should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "permission_denied",
                  "Insufficient privileges to perform the action.",
                  _,
                  Seq()) =>
          }

        assertWithPerm(ServerPermissions)
        assertWithPerm(ServerReadOnlyPermissions)
        assertWithPerm(NullPermissions)
      }

      "created database works" in {
        val auth = newDB
        val db = ctx ! {
          Database.forScope(auth.scopeID)
        } value

        evalOk(
          auth.withPermissions(AdminPermissions),
          """Database.create({name: "Foo"})"""
        ) should matchPattern { case Value.Doc(_, Some("Foo"), _, _, _) => }

        val dbAuth = s"secret:${db.name}/Foo:admin"
        val key = evalOk(dbAuth, "Key.create({ role: 'admin' })")
        val secret = (getDocFields(dbAuth, key) / "secret").as[String]

        evalOk(
          secret,
          "Collection.create({ name: 'Bar' })"
        ) should matchPattern { case Value.Doc(_, Some("Bar"), _, _, _) => }
      }

      "returns global_id encoded" in {
        val auth = newDB

        val db = evalOk(
          auth.withPermissions(AdminPermissions),
          """|Database.create({name: "Foo"}) {
             | name,
             | global_id
             |}""".stripMargin
        )

        (db / "name").as[String] shouldBe "Foo"

        Database.decodeGlobalID(
          (db / "global_id").as[String]
        ) should matchPattern { case Some(GlobalDatabaseID(_)) => }
      }

      "don't accept invalid fields" in {
        val auth = newDB

        renderErr(
          auth.withPermissions(AdminPermissions),
          """|Database.create({
             |  name: "Foo",
             |  global_id: "yu3pdd5ssyyyy"
             |})""".stripMargin,
          typecheck = false
        ) shouldBe (
          """|error: Failed to create Database.
             |constraint failures:
             |  global_id: Failed to update field because it is readonly
             |at *query*:1:16
             |  |
             |1 |   Database.create({
             |  |  ________________^
             |2 | |   name: "Foo",
             |3 | |   global_id: "yu3pdd5ssyyyy"
             |4 | | })
             |  | |__^
             |  |""".stripMargin
        )

        evalErr(
          auth.withPermissions(AdminPermissions),
          """|Database.create({
             |  name: "Foo",
             |  global_id: "yu3pdd5ssyyyy"
             |})""".stripMargin
        ).errors.head.renderWithSource(Map.empty) shouldBe (
          """|error: Type `{ name: "Foo", global_id: "yu3pdd5ssyyyy" }` contains extra field `global_id`
             |at *query*:1:17
             |  |
             |1 |   Database.create({
             |  |  _________________^
             |2 | |   name: "Foo",
             |3 | |   global_id: "yu3pdd5ssyyyy"
             |4 | | })
             |  | |_^
             |  |""".stripMargin
        )

        renderErr(
          auth.withPermissions(AdminPermissions),
          """|Database.create({
             |  name: "Foo",
             |  scope: 1
             |})""".stripMargin,
          typecheck = false
        ) shouldBe (
          """|error: Failed to create Database.
             |constraint failures:
             |  scope: Unexpected field provided
             |at *query*:1:16
             |  |
             |1 |   Database.create({
             |  |  ________________^
             |2 | |   name: "Foo",
             |3 | |   scope: 1
             |4 | | })
             |  | |__^
             |  |""".stripMargin
        )

        evalErr(
          auth.withPermissions(AdminPermissions),
          """|Database.create({
             |  name: "Foo",
             |  scope: 1
             |})""".stripMargin
        ) should matchPattern {
          case QueryCheckFailure(
                Seq(TypeError(
                  "Type `{ name: \"Foo\", scope: 1 }` contains extra field `scope`",
                  _,
                  _,
                  _))) =>
        }
      }

      "account id" in {
        val auth = newDB

        val accountIDParent = ctx.nextID()
        val accountIDChild = ctx.nextID()

        // can set account.id if parent doesn't have
        //
        // account is not part of the static type, so we disable typechecking
        // for this query.
        val parent = evalOk(
          auth.withPermissions(AdminPermissions),
          s"""|let db = Database.create({
              |  name: "Foo",
              |  account: {
              |    id: $accountIDParent
              |  }
              |})
              |
              |db { name, account }""".stripMargin,
          typecheck = false
        )

        (parent / "name").as[String] shouldBe "Foo"
        (parent / "account" / "id").as[Long] shouldBe accountIDParent

        val parentDB = getDB(auth.scopeID, "Foo").value

        // cannot set account.id
        val child = evalOk(
          Auth.adminForScope(parentDB.scopeID),
          s"""|let db = Database.create({
              |  name: "Bar",
              |  account: {
              |    id: $accountIDChild
              |  }
              |})
              |
              |db { name, account }""".stripMargin,
          typecheck = false
        )

        (child / "name").as[String] shouldBe "Bar"
        (child / "account").asOpt[Long] shouldBe None
      }
    }

    "update" - {
      "name" in {
        val admin = newDB.withPermissions(AdminPermissions)

        evalOk(
          admin,
          """Database.create({name: "Foo"})"""
        )

        evalOk(
          admin,
          """Database.byName("Foo")!.update({name: "Bar"})"""
        )

        ctx ! CacheStore.invalidateScope(admin.scopeID)

        // all the existence checks
        evalOk(admin, "Database.byName('Foo').exists()") shouldBe Value.False
        evalOk(admin, "Database.byName('Foo') == null") shouldBe Value.True
        evalOk(admin, "Database.byName('Bar').exists()") shouldBe Value.True
      }

      "invalid name is left alone" in {
        val admin = newDB.withPermissions(AdminPermissions)

        evalV4Ok(
          admin,
          CreateDatabase(MkObject("name" -> "foo-bar"))
        )

        evalOk(admin, "Database.byName('foo-bar')!")
        evalErr(admin, "Database.byName('foo-bar')!.update({ name: 'foo-baz' })")
        evalOk(admin, "Database.byName('foo-bar')!.update({ data: { a: true }})")
      }

      "data" in {
        val admin = newDB.withPermissions(AdminPermissions)

        evalOk(
          admin,
          """Database.create({name: "Foo"})"""
        )

        evalOk(
          admin,
          """Database.byName("Foo")!.update({data: {foo: "bar"}})"""
        )

        evalOk(
          admin,
          """Database.byName("Foo")!.data"""
        ) shouldBe Value.Struct("foo" -> Value.Str("bar"))
      }

      "keep scope & global_id" in {
        val admin = newDB.withPermissions(AdminPermissions)

        evalOk(
          admin,
          """Database.create({name: "Foo"})"""
        )

        val dbBefore = getDB(admin.scopeID, "Foo").value

        evalOk(
          admin,
          """Database.byName("Foo")!.update({name: "Bar"})"""
        )

        val dbAfter = getDB(admin.scopeID, "Bar").value

        dbBefore.scopeID shouldBe dbAfter.scopeID
        dbBefore.globalID shouldBe dbAfter.globalID
      }

      "account.id" in {
        val auth = newDB.withPermissions(AdminPermissions)

        val accountID = ctx.nextID()

        evalOk(auth, """Database.create({name: "Foo"})""")

        // can set account.id if parent doesn't have
        val parent = evalOk(
          auth,
          s"""|let db = Database.byName("Foo")!.update({
              |  account: {
              |    id: $accountID
              |  }
              |})
              |
              |db { name, account }""".stripMargin,
          typecheck = false
        )

        (parent / "name").as[String] shouldBe "Foo"
        (parent / "account" / "id").as[Long] shouldBe accountID

        val parentDB = getDB(auth.scopeID, "Foo").value

        evalOk(
          Auth.adminForScope(parentDB.scopeID),
          """Database.create({name: "Bar"})"""
        )

        // cannot set account.id
        val child = evalOk(
          Auth.adminForScope(parentDB.scopeID),
          """|let db = Database.byName("Bar")!.update({
             |  account: {
             |    id: 1234
             |  }
             |})
             |
             |db { name, account }""".stripMargin,
          typecheck = false
        )

        (child / "name").as[String] shouldBe "Bar"
        (child / "account").asOpt[Long] shouldBe None
      }
    }

    "replace" - {
      "name" in {
        val admin = newDB.withPermissions(AdminPermissions)

        evalOk(
          admin,
          """Database.create({name: "Foo"})"""
        )

        val dbBefore = getDB(admin.scopeID, "Foo").value

        evalOk(
          admin,
          """Database.byName("Foo")!.replace({name: "Bar"})"""
        )

        val dbAfter = getDB(admin.scopeID, "Bar").value

        dbBefore.scopeID shouldBe dbAfter.scopeID
        dbBefore.globalID shouldBe dbAfter.globalID
      }

      "data" in {
        val admin = newDB.withPermissions(AdminPermissions)

        evalOk(
          admin,
          """Database.create({name: "Foo"})"""
        )

        evalOk(
          admin,
          """Database.byName("Foo")!.replace({name: "Foo", data: {foo: "bar"}})"""
        )

        val db = evalOk(admin, """Database.byName("Foo") { name, data }""")

        (db / "name").as[String] shouldBe "Foo"
        (db / "data" / "foo").as[String] shouldBe "bar"
      }

      "account.id" in {
        val auth = newDB.withPermissions(AdminPermissions)

        val accountID = ctx.nextID()

        evalOk(
          auth,
          """Database.create({name: "Foo"})"""
        )

        // can set account.id if parent doesn't have
        val parent = evalOk(
          auth,
          s"""|let db = Database.byName("Foo")!.replace({
              |  name: "Foo",
              |  account: {
              |    id: $accountID
              |  }
              |})
              |
              |db { name, account }""".stripMargin,
          typecheck = false
        )

        (parent / "name").as[String] shouldBe "Foo"
        (parent / "account" / "id").as[Long] shouldBe accountID

        val parentDB = getDB(auth.scopeID, "Foo").value

        evalOk(
          Auth.adminForScope(parentDB.scopeID),
          """Database.create({name: "Bar"})"""
        )

        // cannot set account.id
        val child = evalOk(
          Auth.adminForScope(parentDB.scopeID),
          """|let db = Database.byName("Bar")!.replace({
             |  name: "Bar",
             |  account: {
             |    id: 1234
             |  }
             |})
             |
             |db { name, account }""".stripMargin,
          typecheck = false
        )

        (child / "name").as[String] shouldBe "Bar"
        (child / "account").asOpt[Long] shouldBe None
      }
    }

    "delete" - {
      "works" in {
        val admin = newDB.withPermissions(AdminPermissions)

        evalOk(admin, "Database.create({name: 'Foo'})")
        getDB(admin.scopeID, "Foo") should not be empty

        evalOk(admin, "Database.byName('Foo')!.delete()")
        getDB(admin.scopeID, "Foo") shouldBe empty
      }

      // regression test: schema validation triggered by
      // background v4 UDF signature updates failed seemingly-unrelated
      // schema writes. v4 signatures are no longer updated.
      "can modify schema with a v4 role ref on a function in the same scope" in {
        val admin = newDB.withPermissions(AdminPermissions)

        evalOk(admin, "Database.create({ name: 'db' })")

        evalV4Ok(
          admin,
          CreateRole(MkObject("name" -> "r"))
        )

        evalV4Ok(
          admin,
          CreateFunction(
            MkObject(
              "name" -> "f",
              "body" -> QueryF(Lambda("x" -> Var("x"))),
              "role" -> RoleRef("r")
            )
          )
        )

        evalOk(admin, "Database.byName('db')!.delete()")
      }

      "deletes cascade to sibling keys" in {
        val admin = newDB.withPermissions(AdminPermissions)

        evalOk(admin, "Database.create({name: 'Foo'})")
        evalOk(admin, "Key.create({ role: 'admin', database: 'Foo' })")

        evalOk(admin, "Database.byName('Foo')!.delete()")

        evalOk(admin, "Key.all().toArray()") shouldEqual Value.Array()
      }

      "deletes cascade to pending writes of sibling keys" in {
        val admin = newDB.withPermissions(AdminPermissions)

        evalOk(admin, "Database.create({ name: 'Undead' })")

        evalOk(
          admin,
          """|Key.create({ role: 'admin', database: 'Undead' })
             |Database.byName('Undead')!.delete()
             |""".stripMargin)

        evalOk(admin, "Key.all().toArray()") shouldEqual Value.Array()
      }

      "delete cascade doesn't get tripped up by pending deletes" in {
        val admin = newDB.withPermissions(AdminPermissions)

        evalOk(admin, "Database.create({ name: 'Undead2' })")
        evalOk(admin, "Key.create({ role: 'admin', database: 'Undead2' })")

        evalOk(
          admin,
          """|Key.where(.database == 'Undead2').forEach(.delete())
             |Database.byName('Undead2')!.delete()
             |""".stripMargin)

        evalOk(admin, "Key.all().toArray()") shouldEqual Value.Array()
      }
    }

    "validate FK" - {
      "keys" - {
        "update" in {
          val admin = newDB.withPermissions(AdminPermissions)

          val key = evalOk(
            admin,
            """|Database.create({name: "Foo"})
               |
               |Key.create({
               |  role: "admin",
               |  database: "Foo"
               |})""".stripMargin
          ).as[DocID]

          evalOk(admin, """Database.byName("Foo")!.update({name: "Bar"})""")
          val updatedKeyDb =
            evalOk(admin, s"""Key.byId("${key.subID.toLong}")!.database""")
          updatedKeyDb shouldBe Value.Str("Bar")
        }
      }

      "roles cannot refer to databases" in {
        val auth = newDB.withPermissions(AdminPermissions)

        evalOk(auth, """Database.create({name: "Foo"})""")

        renderErr(
          auth,
          """|Role.create({
             | name: "aRole",
             | privileges: {
             |   resource: "Foo",
             |   actions: {}
             | }
             |})""".stripMargin
        ) shouldBe (
          """|error: Invalid database schema update.
             |    error: Resource `Foo` does not exist
             |    at main.fsl:5:14
             |      |
             |    5 |   privileges Foo {
             |      |              ^^^
             |      |
             |at *query*:1:12
             |  |
             |1 |   Role.create({
             |  |  ____________^
             |2 | |  name: "aRole",
             |3 | |  privileges: {
             |4 | |    resource: "Foo",
             |5 | |    actions: {}
             |6 | |  }
             |7 | | })
             |  | |__^
             |  |""".stripMargin
        )
      }
    }

    "updating name shows up correctly when listing databases" in {
      val auth = newDB.withPermissions(AdminPermissions)

      evalOk(auth, "Database.create({ name: 'foo' })")

      val res = evalOk(auth, "(Database.all(){ name }).paginate()")
      (res / "data" / 0 / "name").as[String] shouldEqual "foo"

      evalOk(
        auth,
        """
          |Database.byName('foo')!.update({ name: 'bar' })
          |""".stripMargin
      )

      val res2 = evalOk(auth, "(Database.all(){ name }).paginate()")
      (res2 / "data" / 0 / "name").as[String] shouldEqual "bar"
    }

    "name must be unique" in {
      val auth = newDB.withPermissions(AdminPermissions)

      evalOk(auth, "Database.create({ name: 'foo' })")

      val res = evalErr(auth, "Database.create({ name: 'foo' })")

      inside(res) { case QueryRuntimeFailure(code, _, _, _, Seq(cf), _) =>
        code shouldEqual "constraint_failure"
        cf.label shouldEqual "name"
      }
    }

    "account id must be unique" in {
      val auth = newDB.withPermissions(AdminPermissions)
      val id = Prop.long.sample

      evalOk(
        auth,
        s"Database.create({ name: 'foo', account: { id: $id } })",
        typecheck = false)

      val res =
        evalErr(
          auth,
          s"Database.create({ name: 'bar', account: { id: $id } })",
          typecheck = false)

      inside(res) { case QueryRuntimeFailure(code, _, _, _, Seq(cf), _) =>
        code shouldEqual "constraint_failure"
        cf.label shouldEqual "account.id"
      }
    }

    "disabled DBs are hidden" in {
      val auth = newDB.withPermissions(AdminPermissions)

      evalOk(auth, s"Database.create({ name: 'foo' })")
      val db = evalOk(auth, s"Database.create({ name: 'bar' })").as[DocID]

      val res1 = evalOk(auth, "Database.all().toArray().map(.name)")

      // disable
      {
        import fauna.ast._
        val ec = EvalContext.write(auth, Timestamp.MaxMicros, APIVersion.Default)
        ctx ! DatabaseWriteConfig.Default.disable(ec, db)
      }

      val res2 = evalOk(auth, "Database.all().toArray().map(.name)")

      res1 shouldEqual Value.Array(Value.Str("foo"), Value.Str("bar"))
      res2 shouldEqual Value.Array(Value.Str("foo"))
    }

    "typechecked flag" - {
      // we bypass newDB in these tests since we're messing with the typechecked
      // flag, which the helpers set.
      def dbName = s"db${ctx.nextID().toString}"

      "defaults to true (no tenant root in tree)" in {
        val doc = evalOk(RootAuth, s"Database.create({ name: '$dbName' })")
        val id = doc.as[DocID].as[DatabaseID]
        val db = (ctx ! Database.getUncached(ScopeID.RootID, id)).get

        ctx ! FQLInterpreter.isEnvTypechecked(db.scopeID) shouldBe true
      }

      "can be set" in {
        val doc = evalOk(
          RootAuth,
          s"Database.create({ name: '$dbName', typechecked: true })")
        val id = doc.as[DocID].as[DatabaseID]
        val db = (ctx ! Database.getUncached(ScopeID.RootID, id)).get

        ctx ! FQLInterpreter.isEnvTypechecked(db.scopeID) shouldBe true
      }

      "inherits from parent" in {
        val parent = {
          val doc = evalOk(
            RootAuth,
            s"Database.create({ name: '$dbName', typechecked: true })")
          val id = doc.as[DocID].as[DatabaseID]
          (ctx ! Database.getUncached(ScopeID.RootID, id)).get
        }
        val pauth = Auth.adminForScope(parent.scopeID)

        val doc = evalOk(pauth, "Database.create({ name: 'child' })")
        val id = doc.as[DocID].as[DatabaseID]
        val db = (ctx ! Database.getUncached(parent.scopeID, id)).get

        ctx ! FQLInterpreter.isEnvTypechecked(db.scopeID) shouldBe true
      }

      "can be overidden" in {
        val parent = {
          val doc = evalOk(
            RootAuth,
            s"Database.create({ name: '$dbName', typechecked: true })")
          val id = doc.as[DocID].as[DatabaseID]
          (ctx ! Database.getUncached(ScopeID.RootID, id)).get
        }
        val pauth = Auth.adminForScope(parent.scopeID)

        val doc =
          evalOk(pauth, "Database.create({ name: 'child', typechecked: false })")
        val id = doc.as[DocID].as[DatabaseID]
        val db = (ctx ! Database.getUncached(parent.scopeID, id)).get

        ctx ! FQLInterpreter.isEnvTypechecked(db.scopeID) shouldBe false
      }
    }

    "protected flag" - {
      val baseAuth = newDB

      "defaults to false" in {
        val db = (ctx ! Database.forScope(baseAuth.scopeID)).get

        // Derived as false from the root database.
        ctx ! Database.isProtected(db.scopeID) shouldBe false
      }

      "can be set" in {
        val dbName = s"db${ctx.nextID().toString}"
        val doc = evalOk(
          Auth.adminForScope(baseAuth.scopeID),
          s"Database.create({ name: '$dbName', protected: true })")
        val id = doc.as[DocID].as[DatabaseID]
        val db = (ctx ! Database.getUncached(baseAuth.scopeID, id)).get

        ctx ! Database.isProtected(db.scopeID) shouldBe true
      }

      "inherits from parent" in {
        val parent = {
          val doc = evalOk(
            Auth.adminForScope(baseAuth.scopeID),
            s"Database.create({ name: 'parent', protected: true })")
          val id = doc.as[DocID].as[DatabaseID]
          (ctx ! Database.getUncached(baseAuth.scopeID, id)).get
        }
        val pAuth = Auth.adminForScope(parent.scopeID)

        val doc = evalOk(
          Auth.adminForScope(pAuth.scopeID),
          "Database.create({ name: 'child' })")
        val id = doc.as[DocID].as[DatabaseID]
        val db = (ctx ! Database.getUncached(pAuth.scopeID, id)).get

        ctx ! Database.isProtected(db.scopeID) shouldBe true
      }
    }
  }

  "move_database works" in {
    val auth = newDB.withPermissions(AdminPermissions)

    val subDB1 = newChildDB(auth).withPermissions(AdminPermissions)
    val subDB2 = newChildDB(auth).withPermissions(AdminPermissions)

    val db1 = ctx ! Database.forScope(subDB1.scopeID).map(_.get)
    val db2 = ctx ! Database.forScope(subDB2.scopeID).map(_.get)

    updateSchemaOk(
      subDB1,
      "main.fsl" ->
        s"""|collection Foo {
            |  index byName {
            |    terms [.name]
            |  }
            |}
            |""".stripMargin
    )

    evalOk(subDB1, "Foo.create({ id: 0, name: 'bar' })")

    val db1key = evalOk(subDB1, "Key.create({ role: 'admin' }).secret").as[String]
    val db2key = evalOk(subDB2, "Key.create({ role: 'admin' }).secret").as[String]

    evalOk(auth, "Database.all().toArray().map(.name)") shouldBe Value.Array(
      Value.Str(db1.name),
      Value.Str(db2.name))
    evalOk(db1key, "Database.all().toArray().map(.name)") shouldBe Value.Array()
    evalOk(db2key, "Database.all().toArray().map(.name)") shouldBe Value.Array()
    evalOk(db1key, "Foo(0)!.name") shouldBe Value.Str("bar")
    evalV4Ok(db1key, Get(RefV(0, RefV("Foo", RefV("collections")))))

    evalV4Ok(
      auth,
      MoveDatabase(
        RefV(db1.name, RefV("databases")),
        RefV(db2.name, RefV("databases")))
    )

    evalOk(auth, "Database.all().toArray().map(.name)") shouldBe Value.Array(
      Value.Str(db2.name))
    evalOk(db1key, "Database.all().toArray().map(.name)") shouldBe Value.Array()
    evalOk(db2key, "Database.all().toArray().map(.name)") shouldBe Value.Array(
      Value.Str(db1.name))
    evalV4Ok(db1key, Get(RefV(0, RefV("Foo", RefV("collections")))))

    evalOk(db1key, "Foo.create({ id: 1, name: 'baz' })")
    evalV4Ok(
      db1key,
      CreateF(
        RefV("Foo", RefV("collections")),
        MkObject("data" -> MkObject("name" -> "baz"))))
  }
}
