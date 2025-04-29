package fauna.model.test

import fauna.atoms.DocID
import fauna.auth._
import fauna.model.{ Database, Key }
import fauna.model.runtime.fql2.{ QueryCheckFailure, QueryRuntimeFailure }
import fauna.model.runtime.fql2.stdlib.KeyCompanion
import fauna.repo.schema.ConstraintFailure._
import fauna.repo.schema.Path
import fauna.repo.values.Value
import fql.error.TypeError

class FQL2KeySpec extends FQL2WithV4Spec {

  "FQL2KeySpec" - {
    "create" - {
      "non admin cannot create" in {
        val auth = newDB

        evalErr(
          auth.withPermissions(NullPermissions),
          """Key.create({role: "admin"})"""
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }

        evalErr(
          auth.withPermissions(ServerPermissions),
          """Key.create({role: "admin"})"""
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }

        evalErr(
          auth.withPermissions(ServerReadOnlyPermissions),
          """Key.create({role: "admin"})"""
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }
      }

      "cannot pass secret" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalErr(
          admin,
          """|Key.create({
             |  role: "client",
             |  secret: "foo"
             |})""".stripMargin
        ) should matchPattern {
          case QueryCheckFailure(
                Seq(
                  TypeError(
                    """Type `{ role: "client", secret: "foo" }` contains extra field `secret`""",
                    _,
                    Nil,
                    Nil))) =>
        }
      }

      "only admin can create" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        val key = evalOk(
          admin,
          """|Key.create({
             |  role: "client",
             |  data: {
             |    foo: "bar"
             |  }
             |})""".stripMargin
        )
        val fields = getDocFields(auth, key)

        (fields / "id") should matchPattern { case Value.ID(_) => }
        (fields / "coll") shouldEqual KeyCompanion
        (fields / "secret") should matchPattern { case Value.Str(_) => }
        (fields / "role") should matchPattern { case Value.Str("client") => }
        (fields / "data" / "foo") should matchPattern { case Value.Str("bar") => }

        evalOk((fields / "secret").as[String], "1") shouldBe Value.Int(1)
      }

      "secret is ephemeral" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        val id = evalOk(
          admin,
          """|Key.create({
             |  role: "client"
             |})""".stripMargin
        ).as[DocID].subID.toLong

        evalOk(admin, s"""Key.byId('$id')!.secret""") should matchPattern {
          case Value.Null(_) =>
        }
      }

      "can set a ttl" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        val id = evalOk(
          admin,
          s"""|Key.create({
             |  role: "client",
             |  ttl: Time.now().add(1, "day")
             |})""".stripMargin
        ).as[DocID].subID.toLong

        evalOk(admin, s"""Key.byId('$id')!.ttl""") should matchPattern {
          case Value.Time(_) =>
        }
      }

      "should respect builtin role" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalErr(
          mkKey(admin, "client"),
          """Collection.create({name: "Foo"})"""
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }

        evalErr(
          mkKey(admin, "server-readonly"),
          """Collection.create({name: "Foo"})"""
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }

        evalOk(
          mkKey(admin, "server"),
          """Collection.create({name: "Foo"}).exists()"""
        ) shouldBe Value.True

        evalOk(
          mkKey(admin, "admin"),
          """Collection.create({name: "Bar"}).exists()"""
        ) shouldBe Value.True
      }

      "should respect user role" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(
          admin,
          """|Role.create({
             |  name: "OnlyFoo",
             |  privileges: {
             |    resource: "Collection",
             |    actions: {
             |      create: 'data => "Foo" == data.name'
             |    }
             |  }
             |})""".stripMargin
        )

        val secret = mkKey(admin, "OnlyFoo")

        evalErr(
          secret,
          """Collection.create({name: "Bar"})"""
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }

        evalOk(secret, "Collection.create({name: 'Foo'})") should matchPattern {
          case Value.Doc(_, Some("Foo"), _, _, _) =>
        }
      }

      "should respect database field" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)
        val child = newChildDB(admin)
        val childDB = (ctx ! Database.forScope(child.scopeID)).value

        val key = evalOk(
          admin,
          s"""|Key.create({
              |  role: "admin",
              |  database: "${childDB.name}"
              |})""".stripMargin
        )
        val fields = getDocFields(auth, key)
        val secret = (fields / "secret").as[String]

        (fields / "database").as[String] shouldBe childDB.name

        val coll = evalOk(
          secret,
          """Collection.create({name: "Foo"})"""
        )

        evalOk(admin, """Collection.all().paginate()""") shouldBe Value.Struct(
          "data" -> Value.Array.empty)
        evalOk(child, """Collection.all().paginate()""") shouldBe Value.Struct(
          "data" -> Value.Array(coll))
      }
    }

    "update" - {
      "can add data" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        val key = evalOk(
          admin,
          """|Key.create({
             |  role: "client"
             |})""".stripMargin
        )
        val fields = getDocFields(auth, key)

        val secret = (fields / "secret").as[String]
        val id = (fields / "id").as[Long]

        evalOk(
          admin,
          s"""|let key = Key.byId('$id')!
              |key.update({
              |  data: {
              |    foo: "bar"
              |  }
              |})""".stripMargin
        )

        val updated = evalOk(
          admin,
          s"""|Key.byId('$id') {
              |  data
              |}
              |""".stripMargin
        )

        (updated / "data" / "foo") should matchPattern { case Value.Str("bar") => }

        evalOk(secret, "1") shouldBe Value.Int(1)
      }

      "can change role" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        val key = evalOk(
          admin,
          """|Key.create({
             |  role: "client"
             |})""".stripMargin
        )
        val fields = getDocFields(auth, key)

        val secret = (fields / "secret").as[String]
        val id = (fields / "id").as[Long]

        evalErr(
          secret,
          """Collection.create({name: "Foo"})"""
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }

        evalOk(
          admin,
          s"""|let key = Key.byId('$id')!
              |key.update({
              |  role: "server"
              |})""".stripMargin
        )

        evalOk(
          secret,
          """Collection.create({name: "Foo"}).exists()"""
        ) shouldBe Value.True
      }

      "cannot update secret" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        val key = evalOk(
          admin,
          """|Key.create({
             |  role: "client"
             |})""".stripMargin
        ).as[DocID]

        val id = key.subID.toLong

        renderErr(
          admin,
          s"""|let key = Key.byId('$id')!
              |key.update({
              |  secret: "foo"
              |})""".stripMargin
        ) shouldBe (
          s"""|error: Failed to update Key with id $id.
              |constraint failures:
              |  secret: Failed to update field because it is readonly
              |at *query*:2:11
              |  |
              |2 |   key.update({
              |  |  ___________^
              |3 | |   secret: "foo"
              |4 | | })
              |  | |__^
              |  |""".stripMargin
        )
      }

      "can change database" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)
        val child0 = newChildDB(admin)
        val childDB0 = (ctx ! Database.forScope(child0.scopeID)).value
        val child1 = newChildDB(admin)
        val childDB1 = (ctx ! Database.forScope(child1.scopeID)).value

        val collChild0 = evalOk(
          child0,
          """|[
             |  Collection.create({name: "coll0"}),
             |  Collection.create({name: "coll1"})
             |]""".stripMargin
        )

        val collChild1 = evalOk(
          child1,
          """|[
             |  Collection.create({name: "Child1"}),
             |  Collection.create({name: "Child2"}),
             |  Collection.create({name: "Child3"}),
             |  Collection.create({name: "Child4"})
             |]""".stripMargin
        )

        val key = evalOk(
          admin,
          s"""|Key.create({
              |  role: "admin",
              |  database: "${childDB0.name}"
              |})""".stripMargin
        )
        val fields = getDocFields(auth, key)

        val secret = (fields / "secret").as[String]
        val id = (fields / "id").as[Long]

        evalOk(secret, """Collection.all().toArray()""") shouldBe collChild0

        evalOk(admin, s"""Key.byId('$id')!.update({database: "${childDB1.name}"})""")

        evalOk(secret, """Collection.all().toArray()""") shouldBe collChild1
      }
    }

    "replace" - {
      "can add data" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        val key = evalOk(
          admin,
          """|Key.create({
             |  role: "client"
             |})""".stripMargin
        )
        val fields = getDocFields(auth, key)

        val secret = (fields / "secret").as[String]
        val id = (fields / "id").as[Long]

        evalOk(
          admin,
          s"""|let key = Key.byId('$id')!
              |key.replace({
              |  role: "client",
              |  data: {
              |    foo: "bar"
              |  }
              |})""".stripMargin
        )

        val replaced = evalOk(
          admin,
          s"""|Key.byId('$id') {
              |  data
              |}
              |""".stripMargin
        )

        (replaced / "data" / "foo") should matchPattern { case Value.Str("bar") => }

        evalOk(secret, "1") shouldBe Value.Int(1)
      }

      "can change role" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        val key = evalOk(
          admin,
          """|Key.create({
             |  role: "client"
             |})""".stripMargin
        )
        val fields = getDocFields(auth, key)

        val secret = (fields / "secret").as[String]
        val id = (fields / "id").as[Long]

        evalErr(
          secret,
          """Collection.create({name: "Foo"})"""
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }

        evalOk(
          admin,
          s"""|let key = Key.byId('$id')!
              |key.replace({
              |  role: "server"
              |})""".stripMargin
        )

        evalOk(
          secret,
          """Collection.create({name: "Foo"}).exists()"""
        ) shouldBe Value.True
      }

      "cannot replace secret" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        val key = evalOk(
          admin,
          """|Key.create({
             |  role: "client"
             |})""".stripMargin
        ).as[DocID]

        val id = key.subID.toLong

        renderErr(
          admin,
          s"""|let key = Key.byId('$id')!
              |key.replace({
              |  role: "server",
              |  secret: "foo"
              |})""".stripMargin
        ) shouldBe (
          s"""|error: Failed to update Key with id $id.
              |constraint failures:
              |  secret: Failed to update field because it is readonly
              |at *query*:2:12
              |  |
              |2 |   key.replace({
              |  |  ____________^
              |3 | |   role: "server",
              |4 | |   secret: "foo"
              |5 | | })
              |  | |__^
              |  |""".stripMargin
        )
      }

      "can change database" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)
        val child0 = newChildDB(admin)
        val childDB0 = (ctx ! Database.forScope(child0.scopeID)).value
        val child1 = newChildDB(admin)
        val childDB1 = (ctx ! Database.forScope(child1.scopeID)).value

        val collChild0 = evalOk(
          child0,
          """|[
             |  Collection.create({name: "coll0"}),
             |  Collection.create({name: "coll1"})
             |]""".stripMargin
        )

        val collChild1 = evalOk(
          child1,
          """|[
             |  Collection.create({name: "Child1"}),
             |  Collection.create({name: "Child2"}),
             |  Collection.create({name: "Child3"}),
             |  Collection.create({name: "Child4"})
             |]""".stripMargin
        )

        val key = evalOk(
          admin,
          s"""|Key.create({
              |  role: "admin",
              |  database: "${childDB0.name}"
              |})""".stripMargin
        )
        val fields = getDocFields(auth, key)

        val secret = (fields / "secret").as[String]
        val id = (fields / "id").as[Long]

        evalOk(secret, """Collection.all().toArray()""") shouldBe collChild0

        evalOk(
          admin,
          s"""|Key.byId('$id')!.replace({
              |  role: "admin",
              |  database: "${childDB1.name}"
              |})""".stripMargin
        )

        evalOk(secret, """Collection.all().toArray()""") shouldBe collChild1
      }
    }

    "delete" - {
      "works" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        val key = evalOk(
          admin,
          """|Key.create({
             |  role: "client"
             |})""".stripMargin
        )
        val fields = getDocFields(auth, key)

        val secret = (fields / "secret").as[String]
        val id = (fields / "id").as[Long]

        (ctx ! Auth.lookup(secret)) should not be None

        evalOk(
          admin,
          s"""Key.byId('$id')!.delete()"""
        )

        (ctx ! Auth.lookup(secret)) shouldBe None
      }
    }

    "validate FK" - {
      "role" - {
        "create" in {
          val auth = newDB
          val admin = auth.withPermissions(AdminPermissions)

          evalErr(
            admin,
            """|Key.create({
               |  role: "foobar"
               |})""".stripMargin
          ) should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "constraint_failure",
                  "Failed to create Key.",
                  _,
                  Seq(
                    ValidatorFailure(
                      Path(Right("role")),
                      "Field refers to an unknown role name `foobar`.")
                  )) =>
          }
        }

        "delete" in {
          val auth = newDB
          val admin = auth.withPermissions(AdminPermissions)

          evalOk(
            admin,
            """|Role.create({
               |  name: "aRole",
               |  privileges: {
               |    resource: "Collection",
               |    actions: {
               |      create: true
               |    }
               |  }
               |})
               |""".stripMargin
          ).as[DocID].subID.toLong

          val key = evalOk(
            admin,
            """|Key.create({
               |  role: "aRole"
               |})""".stripMargin
          ).as[DocID].subID.toLong

          val msg0 = "Failed to delete Role `aRole`."
          val msg1 =
            s"The key `$key` refers to this document by its previous identifier `aRole`."

          evalErr(
            admin,
            """Role.byName('aRole')!.delete()"""
          ) should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "constraint_failure",
                  `msg0`,
                  _,
                  Seq(ValidatorFailure(Path(Right("name")), `msg1`))) =>
          }
        }

        "update" in {
          val auth = newDB
          val admin = auth.withPermissions(AdminPermissions)

          evalOk(
            admin,
            """|Role.create({
               |  name: "aRole",
               |  privileges: {
               |    resource: "Collection",
               |    actions: {
               |      create: true
               |    }
               |  }
               |})
               |""".stripMargin
          )

          val key = evalOk(
            admin,
            """|Key.create({
               |  role: "aRole"
               |})""".stripMargin
          ).as[DocID].subID.toLong

          evalOk(
            admin,
            """Role.byName("aRole")!.update({name: "anotherRole"})"""
          )

          evalOk(
            admin,
            s"""
              |Key.byId("$key")!.role
              |""".stripMargin
          ).as[String] shouldBe "anotherRole"
        }
      }
    }

    "legacy api" - {
      import fauna.ast.VersionL

      "read database as string" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        val childAuth = newChildDB(admin)
        val childDB = (ctx ! Database.forScope(childAuth.scopeID)).value

        val id = evalV4Ok(
          admin,
          CreateKey(
            MkObject(
              "role" -> "admin",
              "database" -> DatabaseRef(childDB.name)
            ))
        ) match {
          case VersionL(v, _) => v.id
          case _              => fail()
        }

        evalOk(
          admin,
          s"""Key.byId('${id.subID.toLong}')!.database"""
        ).as[String] shouldBe childDB.name
      }

      "write database as string" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        val childAuth = newChildDB(admin)
        val childDB = (ctx ! Database.forScope(childAuth.scopeID)).value

        val key = evalOk(
          admin,
          s"""|Key.create({
              |  role: 'admin',
              |  database: '${childDB.name}'
              |})""".stripMargin
        ).as[DocID].subID.toLong

        val id = evalV4Ok(
          admin,
          Get(MkRef(KeysRef, key))
        ) match {
          case VersionL(v, _) => v.data(Key.DatabaseField).value
          case _              => fail()
        }

        id shouldBe childDB.id
      }
    }
  }
}
