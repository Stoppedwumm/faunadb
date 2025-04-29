package fauna.model.test

import fauna.ast.{ PermissionDenied, VersionL }
import fauna.atoms.{ APIVersion, SchemaVersion }
import fauna.auth.{ AdminPermissions, Auth, DocumentAuthScope }
import fauna.codex.json.JSArray
import fauna.model.{ DynamicRoleAction, Role }
import fauna.model.runtime.fql2.{ FQLInterpreter, QueryFailure, QueryRuntimeFailure }
import fauna.model.schema.{ SchemaSource, SchemaStatus }
import fauna.repo.schema.ConstraintFailure.{
  TenantRootWriteFailure,
  ValidatorFailure
}
import fauna.repo.schema.Path
import fauna.repo.values.Value
import fauna.storage.doc.InvalidType
import fauna.storage.ir.{ DocIDV, MapV, QueryV, StringV, TrueV }
import fauna.util.BCrypt
import fql.ast.{ Span, Src }
import scala.collection.immutable.{ ArraySeq, SeqMap }

class FQL2RoleSpec extends FQL2WithV4Spec {

  "FQL2RoleSpec" - {
    "create" - {
      "works" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'Users' })")
        evalOk(auth, "Collection.create({ name: 'Books' })")

        val role = evalOk(
          admin,
          """|let aRole = Role.create({
             |  name: "aRole",
             |  membership: {
             |    resource: "Users"
             |  },
             |  privileges: [{
             |    resource: "Users",
             |    actions: {
             |      read: true
             |    }
             |  }, {
             |    resource: "Books",
             |    actions: {
             |      create: true
             |    }
             |  }],
             |  data: {
             |    custom: "some data"
             |  }
             |})
             |
             |aRole {
             |  name,
             |  membership,
             |  privileges,
             |  data
             |}
             |""".stripMargin
        )

        (role / "name").as[String] shouldBe "aRole"
        (role / "membership").asOpt[Value].nonEmpty shouldBe true
        (role / "privileges").asOpt[Value].nonEmpty shouldBe true
        (role / "data" / "custom").as[String] shouldBe "some data"
      }

      "create with predicates" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'Users' })")
        evalOk(auth, "Collection.create({ name: 'Books' })")

        val role = evalOk(
          admin,
          """|let aRole = Role.create({
             |  name: "aRole",
             |  membership: {
             |    resource: "Users",
             |    predicate: "user => true"
             |  },
             |  privileges: [{
             |    resource: "Users",
             |    actions: {
             |      read: "x => true"
             |    }
             |  }, {
             |    resource: "Books",
             |    actions: {
             |      create: "x => true"
             |    }
             |  }]
             |})
             |
             |aRole {
             |  name,
             |  membership,
             |  privileges
             |}""".stripMargin
        )

        (role / "name").as[String] shouldBe "aRole"
        (role / "membership").asOpt[Value].nonEmpty shouldBe true
        (role / "privileges").asOpt[Value].nonEmpty shouldBe true
      }

      "validate predicates" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'Users' })")
        evalOk(auth, "Collection.create({ name: 'Books' })")

        evalErr(
          admin,
          """|Role.create({
             |  name: "aRole",
             |  membership: {
             |    resource: "Users",
             |    predicate: "invalid fqlx"
             |  },
             |  privileges: [{
             |    resource: "Users",
             |    actions: {
             |      read: "invalid flqx"
             |    }
             |  }, {
             |    resource: "Books",
             |    actions: {
             |      create: "invalid fqlx"
             |    }
             |  }]
             |})
             |""".stripMargin
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "constraint_failure",
                "Failed to create Role.",
                _,
                Seq(
                  ValidatorFailure(Path(Right("membership"), Right("predicate")), _),
                  ValidatorFailure(
                    Path(
                      Right("privileges"),
                      Left(0),
                      Right("actions"),
                      Right("read")),
                    _),
                  ValidatorFailure(
                    Path(
                      Right("privileges"),
                      Left(1),
                      Right("actions"),
                      Right("create")),
                    _)
                )
              ) =>
        }

        evalOk(admin, """Role.byName("aRole") == null""") shouldBe Value.True
        evalOk(admin, """Role.byName("aRole").exists()""") shouldBe Value.False
      }

      "validate number of roles per collection" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'Users' })")
        evalOk(auth, "Collection.create({ name: 'Books' })")

        (1 to Role.MaxRolesPerCollection) foreach { i =>
          evalOk(
            admin,
            s"""|Role.create({
                |  name: "aRole$i",
                |  membership: {
                |    resource: "Users"
                |  },
                |  privileges: {
                |    resource: "Books",
                |    actions: {}
                |  }
                |})""".stripMargin
          )
        }

        evalErr(
          admin,
          s"""|Role.create({
              |  name: "aRole",
              |  membership: {
              |    resource: "Users"
              |  },
              |  privileges: {
              |    resource: "Books",
              |    actions: {}
              |  }
              |})""".stripMargin
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "constraint_failure",
                "Failed to create Role.",
                _,
                Seq(ValidatorFailure(
                  Path.Root,
                  "The maximum number of roles per resource (64) have been exceeded."))
              ) =>
        }
      }

      "can create before collection" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(
          admin,
          """|Role.create({
             |  name: "aRole",
             |  privileges: {
             |    resource: "Users",
             |    actions: {
             |      read: true
             |    }
             |  }
             |})
             |
             |Collection.create({ name: "Users" })
             |""".stripMargin
        )
      }

      "validate resource names" in {
        val auth = newAuth
        val admin = auth.withPermissions(AdminPermissions)

        renderErr(
          admin,
          """|Collection.create({ name: "Coll" })
             |Function.create({ name: "Fn", body: "_ => null" })
             |
             |Role.create({
             |  name: "aRole",
             |  membership: [
             |    { resource: "Coll" },
             |    { resource: "Fn" },
             |    { resource: "Foo" },
             |  ],
             |  privileges: {
             |    resource: "Bar",
             |    actions: {
             |      read: true
             |    }
             |  }
             |})
             |""".stripMargin
        ) shouldBe (
          """|error: Invalid database schema update.
             |    error: Resource `Fn` does not exist
             |    at main.fsl:16:14
             |       |
             |    16 |   membership Fn
             |       |              ^^
             |       |
             |    error: Resource `Foo` does not exist
             |    at main.fsl:17:14
             |       |
             |    17 |   membership Foo
             |       |              ^^^
             |       |
             |    error: Resource `Bar` does not exist
             |    at main.fsl:12:14
             |       |
             |    12 |   privileges Bar {
             |       |              ^^^
             |       |
             |at *query*:4:12
             |   |
             | 4 |   Role.create({
             |   |  ____________^
             | 5 | |   name: "aRole",
             | 6 | |   membership: [
             | 7 | |     { resource: "Coll" },
             | 8 | |     { resource: "Fn" },
             | 9 | |     { resource: "Foo" },
             |10 | |   ],
             |11 | |   privileges: {
             |12 | |     resource: "Bar",
             |13 | |     actions: {
             |14 | |       read: true
             |15 | |     }
             |16 | |   }
             |17 | | })
             |   | |__^
             |   |""".stripMargin
        )
      }

      "can create with aliases" in {
        val auth = newAuth
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(
          admin,
          """|Collection.create({
             |  name: "Foo",
             |  alias: "FooA"
             |})
             |
             |Role.create({
             |  name: "aRole",
             |  privileges: {
             |    resource: "FooA",
             |    actions: {
             |      read: true
             |    }
             |  }
             |})
             |""".stripMargin
        )
      }

      "allow omit actions" in {
        val auth = newDB.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'User' })")

        evalOk(
          auth,
          """|Role.create({
             |  name: "aRole",
             |  privileges: {
             |    resource: "User"
             |  }
             |})""".stripMargin
        )

        val key = evalOk(auth, "Key.create({ role: 'aRole' })")
        val secret = (getDocFields(auth, key) / "secret").as[String]

        evalOk(secret, "'works'").as[String] shouldBe "works"
      }

      "allow omit privileges" in {
        val auth = newDB.withPermissions(AdminPermissions)

        evalOk(
          auth,
          """|Role.create({
             |  name: "aRole"
             |})""".stripMargin
        )

        val key = evalOk(auth, """Key.create({role: "aRole"})""")
        val secret = (getDocFields(auth, key) / "secret").as[String]

        evalOk(secret, "'works'").as[String] shouldBe "works"
      }

      "fails in customer tenant root" in {
        val auth = newDB.withPermissions(AdminPermissions)

        val tenantRoot =
          newCustomerTenantRoot(auth).withPermissions(AdminPermissions)

        val err = evalErr(
          tenantRoot,
          "Role.create({ name: 'rootRole'})"
        )

        err.code shouldBe "constraint_failure"
        err.failureMessage shouldBe "Failed to create Role."
        err.asInstanceOf[QueryRuntimeFailure].constraintFailures shouldBe Seq(
          TenantRootWriteFailure("Role")
        )
      }

      "succeeds in internal tenant root" in {
        val auth = newDB.withPermissions(AdminPermissions)

        val tenantRoot =
          newInternalTenantRoot(auth).withPermissions(AdminPermissions)

        evalOk(
          tenantRoot,
          "Role.create({ name: 'rootRole'})"
        )
      }

      "allows staged deletes" in {
        val auth = newDB.withPermissions(AdminPermissions)

        evalOk(auth, "Role.create({ name: 'foo' })")

        ctx ! SchemaStatus.pinActiveSchema(auth.scopeID, SchemaVersion.Min)

        evalOk(auth, "Role.byName('foo')!.delete()")
      }
    }

    "update" - {
      "works" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'Users' })")
        evalOk(auth, "Collection.create({ name: 'Books' })")

        val res1 = evalOk(
          admin,
          """|let aRole = Role.create({
             |  name: "aRole",
             |  membership: [],
             |  privileges: {
             |    resource: "Users",
             |    actions: {
             |      read: true
             |    }
             |  }
             |})
             |
             |aRole {
             |  name,
             |  membership,
             |  privileges,
             |  data
             |}
             |""".stripMargin
        )

        (res1 / "name").as[String] shouldBe "aRole"
        (res1 / "membership").as[Vector[Value]].isEmpty shouldBe true
        (res1 / "privileges" / "resource").as[String] shouldBe "Users"
        (res1 / "privileges" / "actions" / "read").as[Boolean] shouldBe true

        val res2 = evalOk(
          admin,
          """|let aRole = Role.byName("aRole")!.update({
             |  name: "aNewName",
             |  membership: {
             |    resource: "Users"
             |  },
             |  privileges: [{
             |    resource: "Users",
             |    actions: {
             |      read: true
             |    }
             |  }, {
             |    resource: "Books",
             |    actions: {
             |      create: true
             |    }
             |  }],
             |  data: {
             |    custom: "some data"
             |  }
             |})
             |
             |aRole {
             |  name,
             |  membership,
             |  privileges,
             |  data
             |}
             |""".stripMargin
        )

        (res2 / "name").as[String] shouldBe "aNewName"
        (res2 / "membership" / "resource").as[String] shouldBe "Users"
        (res2 / "privileges" / 0 / "resource").as[String] shouldBe "Users"
        (res2 / "privileges" / 0 / "actions" / "read").as[Boolean] shouldBe true
        (res2 / "privileges" / 1 / "resource").as[String] shouldBe "Books"
        (res2 / "privileges" / 1 / "actions" / "create").as[Boolean] shouldBe true
        (res2 / "data" / "custom").as[String] shouldBe "some data"
      }

      "validate number of roles per collection" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'Users' })")
        evalOk(auth, "Collection.create({ name: 'Books' })")

        evalOk(
          admin,
          """|Role.create({
             |  name: "anotherRole",
             |  membership: {
             |    resource: "Books"
             |  },
             |  privileges: {
             |    resource: "Books",
             |    actions: {}
             |  }
             |})""".stripMargin
        ).to[Value.Doc]

        (1 to Role.MaxRolesPerCollection) foreach { i =>
          evalOk(
            admin,
            s"""|Role.create({
                |  name: "aRole$i",
                |  membership: {
                |    resource: "Users"
                |  },
                |  privileges: {
                |    resource: "Books",
                |    actions: {}
                |  }
                |})""".stripMargin
          )
        }

        val expectedMsg = s"Failed to update Role `anotherRole`."

        evalErr(
          admin,
          s"""|Role.byName("anotherRole")!.update({
              |  membership: {
              |    resource: "Users"
              |  },
              |  privileges: {
              |    resource: "Books",
              |    actions: {}
              |  }
              |})""".stripMargin
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "constraint_failure",
                `expectedMsg`,
                _,
                Seq(ValidatorFailure(
                  Path.Root,
                  "The maximum number of roles per resource (64) have been exceeded."))) =>
        }
      }

      "validates foreign keys" - {
        "functions" in {
          val auth = newDB
          val admin = auth.withPermissions(AdminPermissions)

          evalOk(
            admin,
            """|let role = Role.create({
               |  name: "aRole",
               |  privileges: {
               |    resource: "Collection",
               |    actions: {
               |      read: true
               |    }
               |  }
               |})
               |
               |Function.create({
               |  name: "aFn",
               |  body: "_ => null",
               |  role: "aRole"
               |})
               |
               |role""".stripMargin
          )

          evalOk(
            admin,
            """|Role.byName("aRole")!.update({
               |  name: "anotherName"
               |})""".stripMargin
          )
          evalOk(
            admin,
            s"""Function.byName("aFn")!.role"""
          ) shouldBe Value.Str("anotherName")
        }

        "multiple entities" in {
          val auth = newDB
          val admin = auth.withPermissions(AdminPermissions)

          evalOk(
            admin,
            s"""|
                |Role.create({
                |  name: "aRole",
                |  privileges: {
                |    resource: "Collection",
                |    actions: {
                |      read: true
                |    }
                |  }
                |})
                |Role.create({
                |  name: "aRole2",
                |  privileges: {
                |    resource: "Collection",
                |    actions: {
                |      read: true
                |    }
                |  }
                |})
                |
                |Function.create({
                |  name: "aFn",
                |  body: "_ => null",
                |  role: "aRole"
                |})
                |
                |AccessProvider.create({
                |  name: "anAccessProvider",
                |  issuer: "https://fauna.auth0.com",
                |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
                |  roles: [
                |    "aRole",
                |    {
                |      role: "aRole2",
                |      predicate: "_ => true"
                |    }
                |  ]
                |})
                |
                |Key.create({
                |  role: "aRole",
                |})
                |
                |""".stripMargin
          )
          evalOk(
            admin,
            s"Role.byName('aRole')!.update({ name: 'anotherName' })"
          )

          evalOk(
            admin,
            """
              |[
              |  Function.byName("aFn")!.role,
              |  AccessProvider.byName("anAccessProvider")!.roles[0],
              |  AccessProvider.byName("anAccessProvider")!.roles[1].role,
              |  Key.all().first()!.role
              |]
              |""".stripMargin
          ) shouldBe Value.Array(
            Value.Str("anotherName"),
            Value.Str("anotherName"),
            Value.Str("aRole2"),
            Value.Str("anotherName")
          )
        }

        "access providers" in {
          val auth = newDB
          val admin = auth.withPermissions(AdminPermissions)

          for (i <- 0 to 1) yield {
            evalOk(
              admin,
              s"""|Role.create({
                  |  name: "aRole$i",
                  |  privileges: {
                  |    resource: "Collection",
                  |    actions: {
                  |      read: true
                  |    }
                  |  }
                  |})""".stripMargin
            )
          }

          evalOk(
            admin,
            """|AccessProvider.create({
               |  name: "anAccessProvider",
               |  issuer: "https://fauna.auth0.com",
               |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
               |  roles: [
               |    "aRole0",
               |    {
               |      role: "aRole1",
               |      predicate: "_ => true"
               |    }
               |  ]
               |})
               |""".stripMargin
          )

          for (i <- 0 to 1) {
            evalOk(
              admin,
              s"Role.byName('aRole$i')!.update({ name: 'anotherName$i' })"
            )
          }
          val apRoles =
            evalOk(
              admin,
              """
                |AccessProvider.byName("anAccessProvider")!.roles
                |""".stripMargin
            )

          apRoles shouldBe Value.Array(
            ArraySeq(
              Value.Str("anotherName0"),
              Value.Struct(
                SeqMap(
                  "role" -> Value.Str("anotherName1"),
                  "predicate" -> Value.Str("_ => true")
                )
              )
            )
          )
        }
      }
    }

    "delete" - {
      "works" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'Users' })")

        evalOk(
          admin,
          """|Role.create({
             |  name: "aRole",
             |  membership: [],
             |  privileges: {
             |    resource: "Users",
             |    actions: {
             |      read: true
             |    }
             |  }
             |})
             |""".stripMargin
        )

        evalOk(admin, """Role.byName("aRole")!.delete()""")

        eventually {
          evalOk(admin, """Role.byName("aRole") == null""") shouldBe Value.True
          evalOk(admin, """Role.byName("aRole").exists()""") shouldBe Value.False
        }
      }

      "validates foreign keys" - {
        "functions" in {
          val auth = newDB
          val admin = auth.withPermissions(AdminPermissions)

          evalOk(
            admin,
            """|let role = Role.create({
               |  name: "aRole",
               |  privileges: {
               |    resource: "Collection",
               |    actions: {
               |      read: true
               |    }
               |  }
               |})
               |
               |Function.create({
               |  name: "aFn",
               |  body: "_ => null",
               |  role: "aRole"
               |})
               |
               |role""".stripMargin
          ).to[Value.Doc]

          val expectedMsg = s"Failed to delete Role `aRole`."

          evalErr(admin, "Role.byName('aRole')!.delete()") should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "constraint_failure",
                  `expectedMsg`,
                  _,
                  Seq(
                    ValidatorFailure(
                      Path(Right("name")),
                      "The function `aFn` refers to this document by its previous identifier `aRole`.")
                  )
                ) =>
          }
        }

        "access providers" in {
          val auth = newDB
          val admin = auth.withPermissions(AdminPermissions)

          for (i <- 0 to 1) yield {
            evalOk(
              admin,
              s"""|Role.create({
                  |  name: "aRole$i",
                  |  privileges: {
                  |    resource: "Collection",
                  |    actions: {
                  |      read: true
                  |    }
                  |  }
                  |})""".stripMargin
            )
          }

          evalOk(
            admin,
            """|AccessProvider.create({
               |  name: "anAccessProvider",
               |  issuer: "https://fauna.auth0.com",
               |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
               |  roles: [
               |    "aRole0",
               |    {
               |      role: "aRole1",
               |      predicate: "_ => true"
               |    }
               |  ]
               |})""".stripMargin
          )

          renderErr(
            admin,
            "Role.byName('aRole0')!.delete()"
          ) shouldBe (
            """|error: Invalid database schema update.
               |    error: Role `aRole0` does not exist
               |    at main.fsl:13:8
               |       |
               |    13 |   role aRole0
               |       |        ^^^^^^
               |       |
               |at *query*:1:30
               |  |
               |1 | Role.byName('aRole0')!.delete()
               |  |                              ^^
               |  |""".stripMargin
          )

          renderErr(
            admin,
            "Role.byName('aRole1')!.delete()"
          ) shouldBe (
            """|error: Invalid database schema update.
               |    error: Role `aRole1` does not exist
               |    at main.fsl:14:8
               |       |
               |    14 |   role aRole1 {
               |       |        ^^^^^^
               |       |
               |at *query*:1:30
               |  |
               |1 | Role.byName('aRole1')!.delete()
               |  |                              ^^
               |  |""".stripMargin
          )
        }
      }
    }

    "name must be unique" in {
      val auth = newDB.withPermissions(AdminPermissions)

      evalOk(auth, "Role.create({ name: 'foo' })")

      val res = evalErr(auth, "Role.create({ name: 'foo' })")

      inside(res) { case QueryRuntimeFailure(code, _, _, _, Seq(cf), _) =>
        code shouldEqual "constraint_failure"
        cf.label shouldEqual "name"
      }
    }

    "typechecks role predicates" in {
      val auth = newDB
      val admin = auth.withPermissions(AdminPermissions)

      val err = evalErr(
        admin,
        """|Collection.create({name: "User"})
           |Collection.create({name: "Book"})
           |
           |Role.create({
           |  name: "aRole",
           |  membership: { resource: "User", predicate: "u => Users == u" },
           |  privileges: {
           |    resource: "Book",
           |    actions: {
           |      read: "x => Books == x"
           |    }
           |  }
           |})
           |""".stripMargin
      )

      err shouldEqual QueryRuntimeFailure(
        "invalid_schema",
        """|Invalid database schema update.
           |    error: Unbound variable `Users`
           |    at main.fsl:17:23
           |       |
           |    17 |     predicate ((u) => Users == u)
           |       |                       ^^^^^
           |       |
           |    error: Unbound variable `Books`
           |    at main.fsl:13:25
           |       |
           |    13 |       predicate ((x) => Books == x)
           |       |                         ^^^^^
           |       |""".stripMargin,
        FQLInterpreter.StackTrace(Seq(Span(80, 261, Src.Query(""))))
      )
    }

    "abac" - {
      "works" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'Users' })")
        evalOk(auth, "Collection.create({ name: 'Books' })")

        evalOk(
          admin,
          """|Role.create({
             |  name: "DontCreateBooks",
             |  privileges: {
             |    resource: "Books",
             |    actions: {}
             |  }
             |})
             |""".stripMargin
        )

        val secret = mkKey(admin, role = "DontCreateBooks")

        val book = evalOk(auth, "Books.create({})").to[Value.Doc]

        // byId doesn't read
        evalOk(secret, s"Books.byId('${book.id.subID.toLong}')") shouldBe book

        // User cannot read Books
        evalErr(
          secret,
          s"Books.byId('${book.id.subID.toLong}')!") should matchPattern {
          case QueryRuntimeFailure.Simple(
                "null_value",
                "Null value, due to permission denied",
                _,
                Seq()) =>
        }

        // allow User to read Books
        evalOk(
          admin,
          """|Role.byName("DontCreateBooks")!.update({
             |  privileges: {
             |    resource: "Books",
             |    actions: {
             |      read: true
             |    }
             |  }
             |})
             |""".stripMargin
        )

        // byId doesn't read
        evalOk(secret, s"Books.byId('${book.id.subID.toLong}')") shouldBe book

        // User can read Books
        eventually {
          evalOk(
            secret,
            s"Books.byId('${book.id.subID.toLong}').exists()") shouldBe Value.True
        }

        // User cannot create Books
        evalErr(secret, """Books.create({})""") should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }

        // User cannot delete Books
        val auth1 = (ctx ! Auth.fromAuth(secret, List(BCrypt.hash("secret")))).value
        evalErr(
          auth1,
          s"Books.byId('${book.id.subID.toLong}')!.delete()") should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }

        // User cannot update Books
        evalErr(
          auth1,
          s"Books.byId('${book.id.subID.toLong}')!.update({ name: 'Dora' })") should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }

        // allow User to create Books
        evalOk(
          admin,
          """|Role.byName("DontCreateBooks")!.update({
             |  privileges: {
             |    resource: "Books",
             |    actions: {
             |      create: true
             |    }
             |  }
             |})
             |""".stripMargin
        )

        eventually {
          evalOk(secret, "Books.create({})")
        }
      }

      "create_with_id is checked" in {
        val auth = newDB.withPermissions(AdminPermissions)

        evalOk(auth, """Collection.create({ name: "Book" })""")

        Seq(
          "Create" -> "create: true",
          "CreateID" -> "create: true, create_with_id: true",
          "CreateHist" -> "create: true, history_write: true").foreach {
          case (n, acts) =>
            evalOk(
              auth,
              s"""|Role.create({
                  |  name: "$n",
                  |  privileges: {
                  |    resource: "Book",
                  |    actions: { $acts }
                  |  }
                  |})""".stripMargin
            )
        }

        val secret1 = mkKey(auth, role = "Create")
        val secret2 = mkKey(auth, role = "CreateID")
        val secret3 = mkKey(auth, role = "CreateHist")

        evalOk(secret1, "Book.create({})")

        evalErr(secret1, "Book.create({ id: '123' })") should matchPattern {
          case QueryRuntimeFailure("permission_denied", _, _, _, _, _) =>
        }

        evalOk(secret2, "Book.create({ id: '123' })")

        // assert history_write still gives the privilege as well
        evalOk(secret3, "Book.create({ id: '234' })")
      }

      "querying an index with read permissions on the collection should succeed" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(
          auth,
          """|Collection.create({
             |  name: "Books",
             |  indexes: {
             |    byTitle: { terms: [ { field: "title" } ] }
             |  }
             |})
             |""".stripMargin
        )

        evalOk(
          admin,
          """|Books.create({ title: "t1" })
             |
             |Role.create({
             |  name: "ReadBooks",
             |  privileges: {
             |    resource: "Books",
             |    actions: {
             |      read: true
             |    }
             |  }
             |})
             |""".stripMargin
        )

        val secret = mkKey(admin, role = "ReadBooks")

        evalOk(secret, "Books.byTitle('t1').toArray()") match {
          case Value.Array(elems) => elems.size shouldBe 1
          case v                  => fail(s"unexpected value type $v")
        }
      }

      "querying an index with a read predicate for a collection should succeed" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(
          auth,
          """
            |
            |Collection.create({
            |  name: "Books",
            |  indexes: {
            |    byTitle: { terms: [ { field: "title" } ] }
            |  }
            |})
            |""".stripMargin
        )

        evalOk(
          admin,
          """|Books.create({ title: "t1" })
             |
             |Role.create({
             |  name: "ReadBooks",
             |  privileges: {
             |    resource: "Books",
             |    actions: {
             |      read: 'doc => true'
             |    }
             |  }
             |})
             |""".stripMargin
        )

        val secret = mkKey(admin, role = "ReadBooks")

        evalOk(secret, "Books.byTitle('t1').toArray()") match {
          case Value.Array(elems) => elems.size shouldBe 1
          case v                  => fail(s"unexpected value type $v")
        }
      }

      "querying an index with a read predicate that returns false should succeed but filter out results" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(
          auth,
          """|Collection.create({
             |  name: "Books",
             |  indexes: {
             |    byTitle: { terms: [ { field: "title" } ] }
             |  }
             |})
             |""".stripMargin
        )

        evalOk(
          admin,
          """|Books.create({ title: "t1" })
             |
             |Role.create({
             |  name: "ReadBooks",
             |  privileges: {
             |    resource: "Books",
             |    actions: {
             |      read: 'doc => false'
             |    }
             |  }
             |})
             |""".stripMargin
        )

        val secret = mkKey(admin, role = "ReadBooks")

        evalOk(secret, "Books.byTitle('t1').toArray()") shouldBe Value
          .Array(ArraySeq.empty)
      }

      "querying an index with a read predicate should only return documents that pass the predicate" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(
          auth,
          """
            |
            |Collection.create({
            |  name: "Books",
            |  indexes: {
            |    byTitle: { values: [ { field: "title" } ] }
            |  }
            |})
            |""".stripMargin
        )

        evalOk(
          admin,
          """|Books.create({ title: "t1" })
             |Books.create({ title: "t3" })
             |
             |Role.create({
             |  name: "ReadBooks",
             |  privileges: {
             |    resource: "Books",
             |    actions: {
             |      read: 'doc => doc.title == "t3"'
             |    }
             |  }
             |})
             |""".stripMargin
        )

        val secret = mkKey(admin, role = "ReadBooks")

        evalOk(secret, "Books.byTitle().toArray().map(.title)") shouldBe Value
          .Array(ArraySeq(Value.Str("t3")))
      }

      "querying an index without any read permissions on the collection should fail with permission denied" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(
          auth,
          """
            |
            |Collection.create({
            |  name: "Books",
            |  indexes: {
            |    byTitle: { terms: [ { field: "title" } ] }
            |  }
            |})
            |""".stripMargin
        )

        evalOk(
          admin,
          """|Role.create({
             |  name: "ReadBooks",
             |  privileges: {
             |    resource: "Books",
             |    actions: {
             |    }
             |  }
             |})
             |""".stripMargin
        )

        val secret = mkKey(admin, role = "ReadBooks")

        evalErr(
          secret,
          """
            |Books.byTitle("t1")
            |""".stripMargin
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to read from collection Books.",
                _,
                _) =>
        }
      }

      "querying an index with false read permissions on the collection should fail with permission denied" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(
          auth,
          """
            |
            |Collection.create({
            |  name: "Books",
            |  indexes: {
            |    byTitle: { terms: [ { field: "title" } ] }
            |  }
            |})
            |""".stripMargin
        )

        evalOk(
          admin,
          """|Role.create({
             |  name: "ReadBooks",
             |  privileges: {
             |    resource: "Books",
             |    actions: {
             |      read: false
             |    }
             |  }
             |})
             |""".stripMargin
        )

        val secret = mkKey(admin, role = "ReadBooks")

        evalErr(
          secret,
          """
            |Books.byTitle("t1")
            |""".stripMargin
        ) match {
          case QueryRuntimeFailure.Simple(code, failureMessage, _, _) =>
            code shouldBe "permission_denied"
            failureMessage shouldBe "Insufficient privileges to read from collection Books."
          case err =>
            fail(
              s"unexpected error when evaluating query, expected QueryRuntimeFailure, got $err")
        }
      }

      "reject write operations on predicates" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'Users' })")
        evalOk(auth, "Collection.create({ name: 'Books' })")

        evalOk(
          admin,
          """|Role.create({
             |  name: "AllowCreateUsers",
             |  privileges: {
             |    resource: "Users",
             |    actions: {
             |      create: true
             |    }
             |  }
             |})
             |""".stripMargin
        )

        evalOk(
          admin,
          """|Function.create({
             |  name: "CreateUser",
             |  body: '() => Users.create({})',
             |  role: "AllowCreateUsers"
             |})""".stripMargin
        )

        // FIXME: Ideally we should reject the role creation
        // not the predicate evaluation.
        evalOk(
          admin,
          """|Role.create({
             |  name: "AllowCreateBooks",
             |  privileges: {
             |    resource: "Books",
             |    actions: {
             |      create: 'data => { CreateUser(); true }'
             |    }
             |  }
             |})
             |""".stripMargin
        )

        val secret = mkKey(admin, role = "AllowCreateBooks")

        evalErr(secret, "Books.create({})") should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }
      }

      "write predicate" - {
        "new version top level access" in {

          val auth = newDB
          val admin = auth.withPermissions(AdminPermissions)

          val role = "BooksRole"

          evalOk(admin, "Collection.create({ name: 'Books' })")

          evalOk(
            admin,
            s"""|Role.create({
                |  name: "$role",
                |  privileges: {
                |    resource: "Books",
                |    actions: {
                |      create: true,
                |      read: true,
                |      write: '(old, new) => "$role" == new.role'
                |    }
                |  }
                |})
                |""".stripMargin
          )

          val secret = mkKey(admin, role)

          evalOk(
            secret,
            s"""|let book = Books.create({})
                |book.update({role: "$role"})
                |""".stripMargin)

          evalOk(
            secret,
            s"""|let book = Books.create({ role: "$role"})
                |book.update({title: "name of the wind"})
                |""".stripMargin)

          // fql4 writes
          val res =
            evalOk(
              secret,
              s"""|Books.create({ title: "mona lisa" })
                  |""".stripMargin)

          val legacyAuth =
            (ctx ! Auth.fromAuth(secret, List(BCrypt.hash("secret")))).value
          evalV4Err(
            legacyAuth,
            Update(
              MkRef(ClassRef("Books"), res.asInstanceOf[Value.Doc].id.subID.toLong),
              MkObject(
                "data" -> MkObject(
                  "role" -> "some other name"
                )
              )
            )).head shouldBe a[PermissionDenied]

          evalV4Ok(
            legacyAuth,
            Update(
              MkRef(ClassRef("Books"), res.asInstanceOf[Value.Doc].id.subID.toLong),
              MkObject(
                "data" -> MkObject(
                  "role" -> role
                )
              )
            ))

          evalV4Ok(
            legacyAuth,
            Update(
              MkRef(ClassRef("Books"), res.asInstanceOf[Value.Doc].id.subID.toLong),
              MkObject(
                "data" -> MkObject(
                  "title" -> "the name of the wind-up bird chronicles"
                )
              )
            )
          )
        }
        "prev version top level access" in {
          val auth = newDB
          val admin = auth.withPermissions(AdminPermissions)

          evalOk(admin, "Collection.create({ name: 'Books' })")

          val roleName = "BooksOld"

          evalOk(
            admin,
            s"""|Role.create({
                |  name: "$roleName",
                |  privileges: {
                |    resource: "Books",
                |    actions: {
                |      create: true,
                |      read: true,
                |      write: '(old, new) => "$roleName" == old.role'
                |    }
                |  }
                |})
                |""".stripMargin
          )

          val secret = mkKey(admin, roleName)

          val errRes = evalErr(
            secret,
            s"""|let book = Books.create({})
                |book.update({role: "$roleName"})
                |""".stripMargin)
          validatePermissionDenied(errRes)

          evalOk(
            secret,
            s"""|let book = Books.create({ role: "$roleName"})
                |book.update({title: "name of the wind"})
                |""".stripMargin
          )

          // fql4 writes
          val bookNoRole =
            evalOk(
              secret,
              s"""|Books.create({})
                  |""".stripMargin)

          val bookRole =
            evalOk(
              secret,
              s"""|Books.create({ role: "$roleName" })
                  |""".stripMargin)

          val legacyAuth =
            (ctx ! Auth.fromAuth(secret, List(BCrypt.hash("secret")))).value
          evalV4Err(
            legacyAuth,
            Update(
              MkRef(
                ClassRef("Books"),
                bookNoRole.asInstanceOf[Value.Doc].id.subID.toLong),
              MkObject(
                "data" -> MkObject(
                  "role" -> s"$roleName"
                )
              )
            )).head shouldBe a[PermissionDenied]

          evalV4Ok(
            legacyAuth,
            Update(
              MkRef(
                ClassRef("Books"),
                bookRole.asInstanceOf[Value.Doc].id.subID.toLong),
              MkObject(
                "data" -> MkObject(
                  "title" -> "the name of the wind-up bird chronicles"
                )
              )
            )
          )
        }
        "new version .data access" in {
          val auth = newDB
          val admin = auth.withPermissions(AdminPermissions)

          evalOk(admin, "Collection.create({ name: 'Books' })")

          val roleName = "BooksOld"

          evalOk(
            admin,
            s"""|Role.create({
                |  name: "$roleName",
                |  privileges: {
                |    resource: "Books",
                |    actions: {
                |      create: true,
                |      read: true,
                |      write: '(old, new) => "$roleName" == new.data.role'
                |    }
                |  }
                |})
                |""".stripMargin
          )

          val secret = mkKey(admin, roleName)

          val errRes = evalErr(
            secret,
            s"""|let book = Books.create({})
                |book.update({role: "some other name"})
                |""".stripMargin)
          validatePermissionDenied(errRes)

          evalOk(
            secret,
            s"""|let book = Books.create({})
                |book.update({role: "$roleName"})
                |""".stripMargin)

          evalOk(
            secret,
            s"""|let book = Books.create({ role: "$roleName"})
                |book.update({title: "name of the wind"})
                |""".stripMargin
          )

          // fql4 writes
          val res =
            evalOk(
              secret,
              s"""|Books.create({})
                  |""".stripMargin)

          val legacyAuth =
            (ctx ! Auth.fromAuth(secret, List(BCrypt.hash("secret")))).value
          evalV4Err(
            legacyAuth,
            Update(
              MkRef(ClassRef("Books"), res.asInstanceOf[Value.Doc].id.subID.toLong),
              MkObject(
                "data" -> MkObject(
                  "role" -> "some other name"
                )
              )
            )).head shouldBe a[PermissionDenied]

          evalV4Ok(
            legacyAuth,
            Update(
              MkRef(ClassRef("Books"), res.asInstanceOf[Value.Doc].id.subID.toLong),
              MkObject(
                "data" -> MkObject(
                  "role" -> roleName
                )
              )
            ))

          evalV4Ok(
            legacyAuth,
            Update(
              MkRef(ClassRef("Books"), res.asInstanceOf[Value.Doc].id.subID.toLong),
              MkObject(
                "data" -> MkObject(
                  "title" -> "the name of the wind-up bird chronicles"
                )
              )
            )
          )
        }
        "prev version .data access" in {
          val auth = newDB
          val admin = auth.withPermissions(AdminPermissions)

          mkColl(admin, "Books")
          val roleName = "BooksRole"

          evalOk(
            admin,
            s"""|Role.create({
                |  name: "$roleName",
                |  privileges: {
                |    resource: "Books",
                |    actions: {
                |      create: true,
                |      read: true,
                |      write: '(old, new) => "$roleName" == old.data.role'
                |    }
                |  }
                |})
                |""".stripMargin
          )

          val secret = mkKey(admin, roleName)

          val errRes = evalErr(
            secret,
            s"""|let book = Books.create({})
                |book.update({role: "$roleName"})
                |""".stripMargin)
          validatePermissionDenied(errRes)

          evalOk(
            secret,
            s"""|let book = Books.create({ role: "$roleName"})
                |book.update({title: "name of the wind"})
                |""".stripMargin
          )

          // fql4 writes
          val bookNoRole =
            evalOk(
              secret,
              s"""|Books.create({})
                  |""".stripMargin)

          val bookRole =
            evalOk(
              secret,
              s"""|Books.create({ role: "$roleName" })
                  |""".stripMargin)

          val legacyAuth =
            (ctx ! Auth.fromAuth(secret, List(BCrypt.hash("secret")))).value
          evalV4Err(
            legacyAuth,
            Update(
              MkRef(
                ClassRef("Books"),
                bookNoRole.asInstanceOf[Value.Doc].id.subID.toLong),
              MkObject(
                "data" -> MkObject(
                  "role" -> s"$roleName"
                )
              )
            )).head shouldBe a[PermissionDenied]

          evalV4Ok(
            legacyAuth,
            Update(
              MkRef(
                ClassRef("Books"),
                bookRole.asInstanceOf[Value.Doc].id.subID.toLong),
              MkObject(
                "data" -> MkObject(
                  "title" -> "the name of the wind-up bird chronicles"
                )
              )
            )
          )
        }
        "fql4 role prev version" in {
          val auth = newDB
          val admin = auth.withPermissions(AdminPermissions)
          val roleName = "rollin"
          val collName = "Books"
          mkColl(admin, collName)
          evalV4Ok(
            admin,
            CreateRole(
              MkObject(
                "name" -> roleName,
                "membership" -> MkObject(
                  "resource" -> ClassRef(collName)
                ),
                "privileges" -> MkObject(
                  "resource" -> ClassRef(collName),
                  "actions" -> MkObject(
                    "read" -> true,
                    "write" -> QueryF(
                      Lambda(
                        JSArray("oldData", "newData") ->
                          Equals(
                            Select(JSArray("data", "role"), Var("oldData")),
                            roleName
                          ))),
                    "create" -> true
                  )
                )
              )
            )
          )

          val secret = mkKey(admin, roleName)

          val errRes = evalErr(
            secret,
            s"""|let book = Books.create({})
                |book.update({role: "$roleName"})
                |""".stripMargin)
          validatePermissionDenied(errRes)

          evalOk(
            secret,
            s"""|let book = Books.create({ role: "$roleName"})
                |book.update({title: "name of the wind"})
                |""".stripMargin
          )
        }
        "fql4 role new version" in {
          val auth = newDB
          val admin = auth.withPermissions(AdminPermissions)
          val roleName = "rollin"
          val collName = "Books"
          mkColl(admin, collName)
          evalV4Ok(
            admin,
            CreateRole(
              MkObject(
                "name" -> roleName,
                "membership" -> MkObject(
                  "resource" -> ClassRef(collName)
                ),
                "privileges" -> MkObject(
                  "resource" -> ClassRef(collName),
                  "actions" -> MkObject(
                    "read" -> true,
                    "write" -> QueryF(
                      Lambda(
                        JSArray("oldData", "newData") ->
                          Equals(
                            Select(JSArray("data", "role"), Var("newData")),
                            roleName
                          ))),
                    "create" -> true
                  )
                )
              )
            )
          )
          val secret = mkKey(admin, roleName)

          val errRes = evalErr(
            secret,
            s"""|let book = Books.create({})
                |book.update({role: "some other name"})
                |""".stripMargin)
          validatePermissionDenied(errRes)

          evalOk(
            secret,
            s"""|let book = Books.create({})
                |book.update({role: "$roleName"})
                |""".stripMargin)

          evalOk(
            secret,
            s"""|let book = Books.create({ role: "$roleName"})
                |book.update({title: "name of the wind"})
                |""".stripMargin
          )
        }
      }
      "read predicate" - {
        "top level and .data access" in {
          val auth = newDB
          val admin = auth.withPermissions(AdminPermissions)

          mkColl(admin, "Books")

          val roleName = "BooksRole"
          val roleNameDataAccess = "BooksRoleData"
          val readTitle = "name of the wind-up bird chronicles"

          evalOk(
            admin,
            s"""|Role.create({
               |  name: "$roleName",
               |  privileges: {
               |    resource: "Books",
               |    actions: {
               |      create: true,
               |      read: 'book => book.title == "$readTitle"',
               |    }
               |  }
               |})
               |Role.create({
               |  name: "$roleNameDataAccess",
               |  privileges: {
               |    resource: "Books",
               |    actions: {
               |      create: true,
               |      read: 'book => book.data.title == "$readTitle"',
               |    }
               |  }
               |})
               |""".stripMargin
          )

          def evalRole(roleName: String) = {
            val bookCanRead = evalOk(
              admin,
              s"""|Books.create({ title: "$readTitle"})
                  |""".stripMargin).asInstanceOf[Value.Doc]

            val bookNoRead = evalOk(
              admin,
              s"""|Books.create({ title: "some other title"})
                  |""".stripMargin).asInstanceOf[Value.Doc]

            val secret = mkKey(admin, roleName)

            evalOk(secret, s"Books.byId('${bookCanRead.id.subID.toLong}')!")
            val errRes =
              evalErr(secret, s"Books.byId('${bookNoRead.id.subID.toLong}')!")
            validateReadPermissionDenied(errRes)

            // fql4 reads
            val legacyAuth =
              (ctx ! Auth.fromAuth(secret, List(BCrypt.hash("secret")))).value
            evalV4Err(
              legacyAuth,
              Get(
                MkRef(ClassRef("Books"), bookNoRead.id.subID.toLong)
              )).head shouldBe a[PermissionDenied]
            evalV4Ok(
              legacyAuth,
              Get(
                MkRef(ClassRef("Books"), bookCanRead.id.subID.toLong)
              ))
          }
          evalRole(roleName)
          evalRole(roleNameDataAccess)
        }
        "fql4 role with fqlx read" in {
          val auth = newDB
          val admin = auth.withPermissions(AdminPermissions)
          val roleName = "BooksRole"
          val readTitle = "name of the wind-up bird chronicles"
          val collName = "Books"
          mkColl(admin, collName)
          evalV4Ok(
            admin,
            CreateRole(
              MkObject(
                "name" -> roleName,
                "membership" -> MkObject(
                  "resource" -> ClassRef(collName)
                ),
                "privileges" -> MkObject(
                  "resource" -> ClassRef(collName),
                  "actions" -> MkObject(
                    "read" -> QueryF(
                      Lambda(
                        JSArray("data") ->
                          Equals(
                            Select(JSArray("data", "title"), Get(Var("data"))),
                            readTitle
                          ))),
                    "create" -> true
                  )
                )
              )
            )
          )

          val bookCanRead = evalOk(
            admin,
            s"""|Books.create({ title: "$readTitle"})
                |""".stripMargin).asInstanceOf[Value.Doc]

          val bookNoRead = evalOk(
            admin,
            s"""|Books.create({ title: "some other title"})
                |""".stripMargin).asInstanceOf[Value.Doc]

          val secret = mkKey(admin, roleName)

          evalOk(secret, s"Books.byId('${bookCanRead.id.subID.toLong}')!")
          val errRes =
            evalErr(secret, s"Books.byId('${bookNoRead.id.subID.toLong}')!")
          validateReadPermissionDenied(errRes)
        }
        def validateReadPermissionDenied(error: QueryFailure) =
          error should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "null_value",
                  "Null value, due to permission denied",
                  _,
                  Seq()) =>
          }
      }
      "delete predicate" - {
        "top level and .data access" in {
          val auth = newDB
          val admin = auth.withPermissions(AdminPermissions)

          mkColl(admin, "Books")

          val roleName = "BooksRole"
          val roleNameDataAccess = "BooksRoleData"
          val readTitle = "name of the wind-up bird chronicles"

          evalOk(
            admin,
            s"""|Role.create({
                |  name: "$roleName",
                |  privileges: {
                |    resource: "Books",
                |    actions: {
                |      create: true,
                |      read: true,
                |      delete: 'book => book.title == "$readTitle"',
                |    }
                |  }
                |})
                |Role.create({
                |  name: "$roleNameDataAccess",
                |  privileges: {
                |    resource: "Books",
                |    actions: {
                |      create: true,
                |      read: true,
                |      delete: 'book => book.data.title == "$readTitle"',
                |    }
                |  }
                |})
                |""".stripMargin
          )

          def evalRole(roleName: String) = {
            val secret = mkKey(admin, roleName)

            evalOk(
              secret,
              s"""|let book = Books.create({ title: "$readTitle"})
                  |book.delete()
                  |""".stripMargin)
            val errRes = evalErr(
              secret,
              s"""|let book = Books.create({ title: "some other title"})
                  |book.delete()
                  |""".stripMargin)
            validatePermissionDenied(errRes)

            // fql4 deletes
            val bookCanDelete = evalOk(
              admin,
              s"""|Books.create({ title: "$readTitle"})
                  |""".stripMargin).asInstanceOf[Value.Doc]

            val bookNoDelete = evalOk(
              admin,
              s"""|Books.create({ title: "some other title"})
                  |""".stripMargin).asInstanceOf[Value.Doc]
            val legacyAuth =
              (ctx ! Auth.fromAuth(secret, List(BCrypt.hash("secret")))).value
            evalV4Err(
              legacyAuth,
              DeleteF(
                MkRef(ClassRef("Books"), bookNoDelete.id.subID.toLong)
              )).head shouldBe a[PermissionDenied]
            evalV4Ok(
              legacyAuth,
              DeleteF(
                MkRef(ClassRef("Books"), bookCanDelete.id.subID.toLong)
              ))
          }
          evalRole(roleName)
          evalRole(roleNameDataAccess)
        }
        "fql4 role fqlx delete" in {
          val auth = newDB
          val admin = auth.withPermissions(AdminPermissions)
          val roleName = "BooksRole"
          val readTitle = "name of the wind-up bird chronicles"
          val collName = "Books"
          mkColl(admin, collName)
          evalV4Ok(
            admin,
            CreateRole(
              MkObject(
                "name" -> roleName,
                "membership" -> MkObject(
                  "resource" -> ClassRef(collName)
                ),
                "privileges" -> MkObject(
                  "resource" -> ClassRef(collName),
                  "actions" -> MkObject(
                    "delete" -> QueryF(
                      Lambda(
                        JSArray("data") ->
                          Equals(
                            Select(JSArray("data", "title"), Get(Var("data"))),
                            readTitle
                          ))),
                    "create" -> true,
                    "read" -> true
                  )
                )
              )
            )
          )

          val secret = mkKey(admin, roleName)

          evalOk(
            secret,
            s"""|let book = Books.create({ title: "$readTitle"})
                |book.delete()
                |""".stripMargin)

          val errRes = evalErr(
            secret,
            s"""|let book = Books.create({ title: "some other title"})
                |book.delete()
                |""".stripMargin)
          validatePermissionDenied(errRes)
        }
      }

      def validatePermissionDenied(error: QueryFailure) =
        error should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }
    }

    "udf" - {
      "call" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'Users' })")

        evalOk(
          admin,
          """|Function.create({
             |  name: "Function1",
             |  body: "data => data"
             |})""".stripMargin)

        evalOk(
          admin,
          """|Function.create({
             |  name: "Function2",
             |  body: "data => data"
             |})""".stripMargin)

        evalOk(
          admin,
          """|Role.create({
             |  name: "FunctionsRole",
             |  privileges: [{
             |    resource: "Function1",
             |    actions: { call: false }
             |  }, {
             |    resource: "Function2",
             |    actions: { call: true }
             |  }]
             |})
             |""".stripMargin
        )

        val secret = mkKey(admin, role = "FunctionsRole")

        evalErr(secret, "Function1('foo')") should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }

        evalOk(secret, "Function2('bar')").to[Value.Str] shouldBe Value.Str("bar")
      }

      "works" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'Users' })")
        evalOk(auth, "Collection.create({ name: 'Books' })")

        evalOk(
          admin,
          """|Role.create({
             |  name: "AllowCreateBooks",
             |  privileges: {
             |    resource: "Books",
             |    actions: {
             |      create: true
             |    }
             |  }
             |})
             |""".stripMargin
        )

        evalOk(
          admin,
          """|Function.create({
             |  name: "CreateBook",
             |  body: "data => Books.create(data)"
             |})""".stripMargin)

        // FIXME: Change role type to support role names
        evalOk(
          admin,
          """|Function.create({
             |  name: "CreateBookWithPrivileges",
             |  body: "data => Books.create(data)",
             |  role: "AllowCreateBooks"
             |})""".stripMargin
        )

        evalOk(
          admin,
          """|Role.create({
             |  name: "DontTouchBooks",
             |  privileges: [{
             |    resource: "Books",
             |    actions: {}
             |  }, {
             |    resource: "CreateBook",
             |    actions: { call: true }
             |  }, {
             |    resource: "CreateBookWithPrivileges",
             |    actions: { call: true }
             |  }]
             |})
             |""".stripMargin
        )

        val secret = mkKey(admin, "DontTouchBooks")

        evalErr(secret, """CreateBook({title: "Hamlet"})""") should matchPattern {
          case QueryRuntimeFailure(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                FQLInterpreter.StackTrace(
                  Seq(
                    Span(_, _, Src.UserFunc("CreateBook")),
                    Span(_, _, Src.Inline("*query*", _))
                  )),
                Seq(),
                Seq(),
                None
              ) =>
        }

        evalOk(secret, """CreateBookWithPrivileges({title: "Hamlet"})""")
      }
    }

    "schemas" - {
      "works" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'Users' })")
        val books = evalOk(auth, "Collection.create({ name: 'Books' })")

        evalOk(
          admin,
          """|Role.create({
             |  name: "CollectionRole",
             |  privileges: {
             |    resource: "Collection",
             |    actions: {
             |      read: true,
             |      write: true,
             |      create: false
             |    }
             |  }
             |})
             |""".stripMargin
        )

        val secret = mkKey(admin, role = "CollectionRole")

        evalOk(secret, """Collection.byName("Books")!""") shouldBe books

        evalOk(
          secret,
          """|Collection.byName("Books")!.update({
             | name: "NewBooks"
             |})""".stripMargin) shouldBe books

        evalErr(
          secret,
          "Collection.create({ name: 'Widgets' })") should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }
      }
    }

    "arity" - {
      "call predicate" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(
          admin,
          """|Function.create({
             |  name: "Sum",
             |  body: "(arg1, arg2) => arg1 + arg2"
             |})""".stripMargin)

        // arguments must be arg1=1 & arg2=2 to allow the call
        evalOk(
          admin,
          """|Role.create({
             |  name: "SumRole",
             |  privileges: {
             |    resource: "Sum",
             |    actions: {
             |      call: '(arg1, arg2) => arg1 == 1 && arg2 == 2'
             |    }
             |  }
             |})
             |""".stripMargin
        )

        val secret = mkKey(admin, role = "SumRole")

        evalOk(secret, "Sum(1, 2)").to[Value.Int].value shouldBe 3

        evalErr(secret, "Sum(2, 1)") should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }
      }

      "create predicate" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(admin, "Collection.create({ name: 'Books' })")

        // create predicates will accept the data passed into the create() call
        //
        // role 1 shows that `id` is always present.
        // role 2 shows that we can fetch fields of the create data.
        // role 3 shows that the nested .data field will work.
        evalOk(
          admin,
          """|Role.create({
             |  name: "BooksRole1",
             |  privileges: {
             |    resource: "Books",
             |    actions: {
             |      create: 'data => data.id != null',
             |      write: true,
             |      read: true,
             |    }
             |  }
             |})
             |Role.create({
             |  name: "BooksRole2",
             |  privileges: {
             |    resource: "Books",
             |    actions: {
             |      create: 'data => data.override',
             |      write: true,
             |      read: true,
             |    }
             |  }
             |})
             |Role.create({
             |  name: "BooksRole3",
             |  privileges: {
             |    resource: "Books",
             |    actions: {
             |      create: 'data => data.data.override',
             |      write: true,
             |      read: true,
             |    }
             |  }
             |})
             |""".stripMargin
        )

        val role1 = mkKey(admin, "BooksRole1")
        evalOk(role1, "Books.create({})")

        val role2 = mkKey(admin, "BooksRole2")
        evalErr(role2, "Books.create({})") shouldBe QueryRuntimeFailure
          .PermissionDenied(
            FQLInterpreter.StackTrace(Seq(Span(12, 16, Src.Query("")))))
        evalOk(role2, "Books.create({ override: true })")

        val role3 = mkKey(admin, "BooksRole3")
        evalErr(role3, "Books.create({})") shouldBe QueryRuntimeFailure
          .PermissionDenied(
            FQLInterpreter.StackTrace(Seq(Span(12, 16, Src.Query("")))))
        evalOk(role3, "Books.create({ override: true })")
      }

      "predicates should work with short lambdas" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(admin, "Collection.create({ name: 'Books' })")

        evalOk(
          admin,
          """|Role.create({
             |  name: "BooksRole",
             |  privileges: {
             |    resource: "Books",
             |    actions: {
             |      create: '(.creatable)',
             |    }
             |  }
             |})
             |""".stripMargin
        )

        val role = mkKey(admin, "BooksRole")
        val err = evalErr(role, "Books.create({ creatable: false })")
        err.code shouldBe "permission_denied"
        err.failureMessage shouldBe "Insufficient privileges to perform the action."
        evalOk(role, "Books.create({ creatable: true })")
      }
    }
    "fql4" - {
      "create" - {
        "prevents membership resource string" in {
          val auth = newDB.withPermissions(AdminPermissions)
          mkColl(auth, "TestCol")
          validateV4ErrorResponse(
            evalV4Err(
              auth,
              CreateRole(
                MkObject(
                  "name" -> "testRole",
                  "membership" -> MkObject(
                    "resource" -> "TestCol"
                  )
                )
              )),
            InvalidType(
              path = List("membership", "resource"),
              expected = DocIDV.Type,
              actual = StringV.Type
            )
          )
          validateV4ErrorResponse(
            evalV4Err(
              auth,
              CreateRole(
                MkObject(
                  "name" -> "testRole",
                  "membership" -> JSArray(
                    MkObject(
                      "resource" -> "TestCol"
                    ))
                )
              )),
            InvalidType(
              path = List("membership", "resource"),
              expected = DocIDV.Type,
              actual = StringV.Type
            )
          )
        }
        "prevents membership predicate string" in {
          val auth = newDB.withPermissions(AdminPermissions)
          mkColl(auth, "TestCol")
          validateV4ErrorResponse(
            evalV4Err(
              auth,
              CreateRole(
                MkObject(
                  "name" -> "testRole",
                  "membership" -> MkObject(
                    "resource" -> ClassRef("TestCol"),
                    "predicate" -> "x => true"
                  )
                )
              )),
            InvalidType(
              path = List("membership", "predicate"),
              expected = QueryV.Type,
              actual = StringV.Type
            )
          )
          validateV4ErrorResponse(
            evalV4Err(
              auth,
              CreateRole(
                MkObject(
                  "name" -> "testRole",
                  "membership" -> JSArray(
                    MkObject(
                      "resource" -> ClassRef("TestCol"),
                      "predicate" -> "x => true"
                    ))
                )
              )),
            InvalidType(
              path = List("membership", "predicate"),
              expected = QueryV.Type,
              actual = StringV.Type
            )
          )
        }
        "prevents privileges resource string" in {
          val auth = newDB.withPermissions(AdminPermissions)
          mkColl(auth, "TestCol")
          validateV4ErrorResponse(
            evalV4Err(
              auth,
              CreateRole(
                MkObject(
                  "name" -> "testRole",
                  "membership" -> MkObject(
                    "resource" -> ClassRef("TestCol")
                  ),
                  "privileges" -> MkObject(
                    "resource" -> "TestCol"
                  )
                )
              )),
            InvalidType(
              path = List("privileges", "resource"),
              expected = DocIDV.Type,
              actual = StringV.Type
            )
          )
          validateV4ErrorResponse(
            evalV4Err(
              auth,
              CreateRole(
                MkObject(
                  "name" -> "testRole",
                  "membership" -> MkObject(
                    "resource" -> ClassRef("TestCol")
                  ),
                  "privileges" -> JSArray(
                    MkObject(
                      "resource" -> "TestCol"
                    ))
                )
              )
            ),
            InvalidType(
              path = List("privileges", "resource"),
              expected = DocIDV.Type,
              actual = StringV.Type
            )
          )
        }
        "prevents privileges action predicate string" in {
          val auth = newDB.withPermissions(AdminPermissions)
          mkColl(auth, "TestCol")
          validateV4ErrorResponse(
            evalV4Err(
              auth,
              CreateRole(
                MkObject(
                  "name" -> "testRole",
                  "membership" -> MkObject(
                    "resource" -> ClassRef("TestCol")
                  ),
                  "privileges" -> MkObject(
                    "resource" -> ClassRef("TestCol"),
                    "actions" -> MkObject(
                      "read" -> "x => true"
                    )
                  )
                )
              )
            ),
            InvalidType(
              path = List("privileges", "actions", "read"),
              expected = QueryV.Type,
              actual = StringV.Type
            )
          )
          validateV4ErrorResponse(
            evalV4Err(
              auth,
              CreateRole(
                MkObject(
                  "name" -> "testRole",
                  "membership" -> MkObject(
                    "resource" -> ClassRef("TestCol")
                  ),
                  "privileges" -> JSArray(
                    MkObject(
                      "resource" -> ClassRef("TestCol"),
                      "actions" -> MkObject(
                        "read" -> "x => true"
                      )
                    ))
                )
              )
            ),
            InvalidType(
              path = List("privileges", "actions", "read"),
              expected = QueryV.Type,
              actual = StringV.Type
            )
          )
        }
      }
      "update" - {
        "prevents membership resource string" in {
          val auth = newDB.withPermissions(AdminPermissions)
          mkColl(auth, "TestCol")
          mkRole("TestRole", "TestCol", auth)
          validateV4ErrorResponse(
            evalV4Err(
              auth,
              Update(
                Ref("roles/TestRole"),
                MkObject(
                  "membership" -> MkObject(
                    "resource" -> "TestCol"
                  )
                )
              )),
            InvalidType(
              path = List("membership", "resource"),
              expected = DocIDV.Type,
              actual = StringV.Type
            )
          )
          validateV4ErrorResponse(
            evalV4Err(
              auth,
              Update(
                Ref("roles/TestRole"),
                MkObject(
                  "membership" -> JSArray(
                    MkObject(
                      "resource" -> "TestCol"
                    ))
                )
              )),
            InvalidType(
              path = List("membership", "resource"),
              expected = DocIDV.Type,
              actual = StringV.Type
            )
          )
        }
        "prevents membership predicate string" in {
          val auth = newDB.withPermissions(AdminPermissions)
          mkColl(auth, "TestCol")
          mkRole("TestRole", "TestCol", auth)
          validateV4ErrorResponse(
            evalV4Err(
              auth,
              Update(
                Ref("roles/TestRole"),
                MkObject(
                  "name" -> "testRole",
                  "membership" -> MkObject(
                    "resource" -> ClassRef("TestCol"),
                    "predicate" -> "x => true"
                  )
                )
              )),
            InvalidType(
              path = List("membership", "predicate"),
              expected = QueryV.Type,
              actual = StringV.Type
            )
          )
          validateV4ErrorResponse(
            evalV4Err(
              auth,
              Update(
                Ref("roles/TestRole"),
                MkObject(
                  "name" -> "testRole",
                  "membership" -> JSArray(
                    MkObject(
                      "resource" -> ClassRef("TestCol"),
                      "predicate" -> "x => true"
                    ))
                )
              )
            ),
            InvalidType(
              path = List("membership", "predicate"),
              expected = QueryV.Type,
              actual = StringV.Type
            )
          )
        }
        "prevents privileges resource string" in {
          val auth = newDB.withPermissions(AdminPermissions)
          mkColl(auth, "TestCol")
          mkRole("TestRole", "TestCol", auth)
          validateV4ErrorResponse(
            evalV4Err(
              auth,
              Update(
                Ref("roles/TestRole"),
                MkObject(
                  "name" -> "testRole",
                  "membership" -> MkObject(
                    "resource" -> ClassRef("TestCol")
                  ),
                  "privileges" -> MkObject(
                    "resource" -> "TestCol"
                  )
                )
              )
            ),
            InvalidType(
              path = List("privileges", "resource"),
              expected = DocIDV.Type,
              actual = StringV.Type
            )
          )
          validateV4ErrorResponse(
            evalV4Err(
              auth,
              Update(
                Ref("roles/TestRole"),
                MkObject(
                  "name" -> "testRole",
                  "membership" -> MkObject(
                    "resource" -> ClassRef("TestCol")
                  ),
                  "privileges" -> JSArray(
                    MkObject(
                      "resource" -> "TestCol"
                    ))
                )
              )
            ),
            InvalidType(
              path = List("privileges", "resource"),
              expected = DocIDV.Type,
              actual = StringV.Type
            )
          )
        }
        "prevents privileges action predicate string" in {
          val auth = newDB.withPermissions(AdminPermissions)
          mkColl(auth, "TestCol")
          mkRole("TestRole", "TestCol", auth)
          validateV4ErrorResponse(
            evalV4Err(
              auth,
              Update(
                Ref("roles/TestRole"),
                MkObject(
                  "name" -> "testRole",
                  "membership" -> MkObject(
                    "resource" -> ClassRef("TestCol")
                  ),
                  "privileges" -> MkObject(
                    "resource" -> ClassRef("TestCol"),
                    "actions" -> MkObject(
                      "read" -> "x => true"
                    )
                  )
                )
              )
            ),
            InvalidType(
              path = List("privileges", "actions", "read"),
              expected = QueryV.Type,
              actual = StringV.Type
            )
          )
          validateV4ErrorResponse(
            evalV4Err(
              auth,
              Update(
                Ref("roles/TestRole"),
                MkObject(
                  "name" -> "testRole",
                  "membership" -> MkObject(
                    "resource" -> ClassRef("TestCol")
                  ),
                  "privileges" -> JSArray(
                    MkObject(
                      "resource" -> ClassRef("TestCol"),
                      "actions" -> MkObject(
                        "read" -> "x => true"
                      )
                    ))
                )
              )
            ),
            InvalidType(
              path = List("privileges", "actions", "read"),
              expected = QueryV.Type,
              actual = StringV.Type
            )
          )
        }
        "can replace an fqlx role with fql4 role" in {
          val auth = newDB.withPermissions(AdminPermissions)
          mkColl(auth, "TestCol")
          evalOk(
            auth,
            """|Role.create({
               |  name: "TestRole",
               |  membership: {
               |    resource: "TestCol",
               |    predicate: "user => true"
               |  },
               |  privileges: [{
               |    resource: "TestCol",
               |    actions: {
               |      read: "x => true",
               |      create: "x => true"
               |    }
               |  }]
               |})
               |""".stripMargin
          )
          evalV4Ok(
            auth,
            Replace(
              Ref("roles/TestRole"),
              MkObject(
                "name" -> "testRole",
                "membership" -> MkObject(
                  "resource" -> ClassRef("TestCol")
                ),
                "privileges" -> JSArray(
                  MkObject(
                    "resource" -> ClassRef("TestCol"),
                    "actions" -> MkObject(
                      "read" -> QueryF(Lambda("x" -> true))
                    )
                  ))
              )
            )
          ) match {
            case VersionL(version, _) =>
              val privileges = version.data(Role.PrivilegesField)
              privileges.size shouldBe 1
              privileges.head.actions shouldEqual Some(
                List(
                  (
                    "read",
                    DynamicRoleAction(
                      Right(
                        QueryV(
                          APIVersion.V4,
                          MapV(
                            ("lambda", "x"),
                            ("expr", TrueV)
                          ))
                      )
                    ))
                ))
            case v =>
              fail(
                s"unexpected return type, expected VersionL, got $v"
              )
          }
        }
      }
    }
    def mkRole(roleName: String, collName: String, auth: Auth) =
      evalV4Ok(
        auth,
        CreateRole(
          MkObject(
            "name" -> roleName,
            "membership" -> MkObject(
              "resource" -> ClassRef(collName),
              "predicate" -> QueryF(Lambda("x" -> true))
            )
          )
        ))
  }

  "read predicates when executing as doc don't 500 for deleted docs" in {
    val auth = newDB.withPermissions(AdminPermissions)

    evalOk(
      auth,
      s"""|Collection.create({ name: "User" })
          |Collection.create({ name: "Foo" })
          |Role.create({
          |  name: "myRole",
          |  privileges: [
          |    {
          |      resource: "Foo",
          |      actions: {
          |        read: "(doc) => Query.identity() == doc.owner",
          |        delete: true,
          |      }
          |    }
          |  ],
          |  membership: [
          |    {
          |      resource: "User"
          |    }
          |  ]
          |})
          |""".stripMargin
    )

    evalOk(auth, "User.create({ id: '0' })")
    evalOk(auth, "Foo.create({ id: '1', owner: User.byId('0') })")

    val secret =
      evalOk(auth, "Key.create({ role: 'admin' }).secret").as[String]

    val user0 =
      ctx ! Auth
        .lookup(secret, DocumentAuthScope("collections/User/0", List.empty))
        .map { _.get }

    evalOk(user0, "Foo.byId('1')!.delete().exists()") shouldBe Value.False
  }

  "v4 and v10 roles with same membership should not conflict" in {
    val auth = newDB.withPermissions(AdminPermissions)

    evalOk(
      auth,
      s"""|Collection.create({ name: "users" })
          |Function.create({name: "func_v10", body: "x => x*2"})
          |Role.create({
          |  name: "role_v10",
          |  privileges: [{
          |    resource: "func_v10",
          |    actions: {
          |      call: true,
          |    }
          |  }],
          |  membership: [{
          |    resource: "users"
          |  }]
          |})
          |""".stripMargin
    )

    evalOk(auth, "users.create({ id: '1' })")

    val secret =
      evalOk(auth, "Token.create({ document: users.byId('1')! }).secret").as[String]

    evalOk(
      ctx ! Auth.lookup(secret).map { _.get },
      "func_v10(10)"
    ) shouldBe Value.Int(20)

    evalV4Ok(
      auth,
      CreateFunction(
        MkObject(
          "name" -> "func_v4",
          "body" -> QueryF(Lambda("x" -> Multiply(Var("x"), 3)))
        )
      ))

    evalV4Ok(
      auth,
      CreateRole(
        MkObject(
          "name" -> "role_v4",
          "membership" -> MkObject(
            "resource" -> ClassRef("users")
          ),
          "privileges" -> MkObject(
            "resource" -> FunctionRef("func_v4"),
            "actions" -> MkObject("call" -> true)
          )
        )
      )
    )

    evalOk(
      ctx ! Auth.lookup(secret).map { _.get },
      "func_v10(10)"
    ) shouldBe Value.Int(20)

    evalOk(
      ctx ! Auth.lookup(secret).map { _.get },
      "func_v4(10)"
    ) shouldBe Value.Int(30)
  }

  "doesn't display privileges that are denied" in {
    val auth = newDB.withPermissions(AdminPermissions)

    evalOk(
      auth,
      """|Collection.create({
         |  name: "Foo",
         |})
         |
         |Role.create({
         |  name: "Bar",
         |  privileges: {
         |    resource: "Foo",
         |    actions: { create: false }
         |  }
         |})
         |
         |Role.create({
         |  name: "Baaz",
         |  privileges: {
         |    resource: "Foo",
         |    actions: { create: false, read: true }
         |  }
         |})""".stripMargin
    )

    ctx ! SchemaSource.get(auth.scopeID, "main.fsl") map { _.content } shouldBe (
      Some(
        """|// The following schema is auto-generated.
           |// This file contains FSL for FQL10-compatible schema items.
           |
           |collection Foo {
           |}
           |
           |role Bar {
           |  privileges Foo {
           |  }
           |}
           |
           |role Baaz {
           |  privileges Foo {
           |    read
           |  }
           |}
           |""".stripMargin
      )
    )
  }
}
