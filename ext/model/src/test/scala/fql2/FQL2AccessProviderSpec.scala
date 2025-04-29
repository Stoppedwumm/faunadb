package fauna.model.test

import fauna.atoms.SchemaVersion
import fauna.auth.{ AdminPermissions, JWTToken }
import fauna.model.runtime.fql2.QueryRuntimeFailure
import fauna.model.schema.{ SchemaCollection, SchemaStatus }
import fauna.model.Database
import fauna.repo.schema._
import fauna.repo.schema.ConstraintFailure._
import fauna.storage.doc.InvalidType
import fauna.storage.doc.ValidationFailure
import fauna.storage.ir._
import fql.ast.{ Span, Src }
import scala.concurrent.duration._

class FQL2AccessProviderSpec extends FQL2WithV4Spec {

  "FQL2AccessProviderSpec" - {
    "create" - {
      "works" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)
        val db = (ctx ! Database.forScope(auth.scopeID)).value

        evalOk(auth, "Collection.create({ name: 'Users' })")

        evalOk(
          admin,
          """|
             |Role.create({
             |  name: "aRole",
             |  privileges: {
             |    resource: "Users",
             |    actions: {}
             |  }
             |})
             |Role.create({
             |  name: "aRole2",
             |  privileges: {
             |    resource: "Users",
             |    actions: {}
             |  }
             |})
             |""".stripMargin
        )

        val ap = evalOk(
          admin,
          """|let ap = AccessProvider.create({
             |  name: "anAccessProvider",
             |  issuer: "https://fauna.auth0.com",
             |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |  roles: [
             |    "aRole",
             |    {
             |      role: "aRole2",
             |      predicate: "_ => true"
             |    }
             |  ],
             |  data: {
             |    custom: "some data"
             |  }
             |})
             |
             |ap {
             |  name,
             |  issuer,
             |  jwks_uri,
             |  roles,
             |  audience,
             |  data
             |}
             |""".stripMargin
        )

        (ap / "name").as[String] shouldBe "anAccessProvider"
        (ap / "issuer").as[String] shouldBe "https://fauna.auth0.com"
        (ap / "jwks_uri")
          .as[String] shouldBe "https://fauna.auth0.com/.well-known/jwks.json"
        (ap / "roles" / 0).as[String] shouldBe "aRole"
        (ap / "roles" / 1 / "role").as[String] shouldBe "aRole2"
        (ap / "roles" / 1 / "predicate").as[String] shouldBe "_ => true"
        (ap / "audience").as[String] shouldBe JWTToken.canonicalDBUrl(db)
        (ap / "data" / "custom").as[String] shouldBe "some data"
      }

      "accept roles names, objects or array" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'Users' })")

        evalOk(
          admin,
          """|Role.create({
             |  name: "aRole",
             |  privileges: {
             |    resource: "Users",
             |    actions: {}
             |  }
             |})
             |""".stripMargin
        )

        evalOk(
          admin,
          """|AccessProvider.create({
             |  name: "anAccessProvider0",
             |  issuer: "https://fauna0.auth0.com",
             |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |  roles: "aRole",
             |})
             |""".stripMargin
        )

        evalOk(
          admin,
          """|AccessProvider.create({
             |  name: "anAccessProvider1",
             |  issuer: "https://fauna1.auth0.com",
             |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |  roles: {
             |    role: "aRole",
             |    predicate: 'arg => true'
             |  },
             |})
             |""".stripMargin
        )

        evalOk(
          admin,
          """|AccessProvider.create({
             |  name: "anAccessProvider2",
             |  issuer: "https://fauna2.auth0.com",
             |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |  roles: [{
             |    role: "aRole",
             |    predicate: 'arg => true'
             |  }],
             |})
             |""".stripMargin
        )

        evalOk(
          admin,
          """|AccessProvider.create({
             |  name: "anAccessProvider3",
             |  issuer: "https://fauna3.auth0.com",
             |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |  roles: [ "aRole" ],
             |})
             |""".stripMargin
        )
      }

      "fail if role name doesn't exist" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'Users' })")

        evalOk(
          admin,
          """|Role.create({
             |  name: "aRole",
             |  privileges: {
             |    resource: "Users",
             |    actions: {}
             |  }
             |})
             |""".stripMargin
        )

        evalOk(
          admin,
          """|AccessProvider.create({
             |  name: "accessProvider0",
             |  issuer: "https://fauna0.auth0.com",
             |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |  roles: "aRole"
             |})
             |""".stripMargin
        )

        renderErr(
          admin,
          """|AccessProvider.create({
             |  name: "accessProvider1",
             |  issuer: "https://fauna1.auth0.com",
             |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |  roles: "anotherRole"
             |})
             |""".stripMargin
        ) shouldBe (
          """|error: Invalid database schema update.
             |    error: Role `anotherRole` does not exist
             |    at main.fsl:21:8
             |       |
             |    21 |   role anotherRole
             |       |        ^^^^^^^^^^^
             |       |
             |at *query*:1:22
             |  |
             |1 |   AccessProvider.create({
             |  |  ______________________^
             |2 | |   name: "accessProvider1",
             |3 | |   issuer: "https://fauna1.auth0.com",
             |4 | |   jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |5 | |   roles: "anotherRole"
             |6 | | })
             |  | |__^
             |  |""".stripMargin
        )
      }

      "rejects builtin roles" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalErr(
          admin,
          """|AccessProvider.create({
             |  name: "accessProvider",
             |  issuer: "https://fauna.auth0.com",
             |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |  roles: ["admin", "server", "server-readonly", "client"]
             |})
             |""".stripMargin
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "constraint_failure",
                "Failed to create AccessProvider.",
                _,
                Seq(
                  ValidatorFailure(
                    Path(Right("roles")),
                    "Builtin role `admin` not allowed."),
                  ValidatorFailure(
                    Path(Right("roles")),
                    "Builtin role `server` not allowed."),
                  ValidatorFailure(
                    Path(Right("roles")),
                    "Builtin role `server-readonly` not allowed."),
                  ValidatorFailure(
                    Path(Right("roles")),
                    "Builtin role `client` not allowed.")
                )
              ) =>
        }
      }

      "fail to create with audience" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'Users' })")

        evalOk(
          admin,
          """|Role.create({
             |  name: "aRole",
             |  privileges: {
             |    resource: "Users",
             |    actions: {}
             |  }
             |})
             |""".stripMargin
        )

        renderErr(
          admin,
          """|AccessProvider.create({
             |  name: "anAccessProvider",
             |  issuer: "https://fauna.auth0.com",
             |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |  audience: "https://db.fauna.com/db/yyyyyyyyyy",
             |  roles: "aRole"
             |})
             |""".stripMargin,
          typecheck = false
        ) shouldBe (
          """|error: Failed to create AccessProvider.
             |constraint failures:
             |  audience: Failed to update field because it is readonly
             |at *query*:1:22
             |  |
             |1 |   AccessProvider.create({
             |  |  ______________________^
             |2 | |   name: "anAccessProvider",
             |3 | |   issuer: "https://fauna.auth0.com",
             |4 | |   jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |5 | |   audience: "https://db.fauna.com/db/yyyyyyyyyy",
             |6 | |   roles: "aRole"
             |7 | | })
             |  | |__^
             |  |""".stripMargin
        )

        evalErr(
          admin,
          """|AccessProvider.create({
             |  name: "anAccessProvider",
             |  issuer: "https://fauna.auth0.com",
             |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |  audience: "https://db.fauna.com/db/yyyyyyyyyy",
             |  roles: "aRole"
             |})
             |""".stripMargin
        ).errors.head.renderWithSource(Map.empty) shouldBe (
          """|error: Type `{ name: "anAccessProvider", issuer: "https://fauna.auth0.com", jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json", audience: "https://db.fauna.com/db/yyyyyyyyyy", roles: "aRole" }` contains extra field `audience`
             |at *query*:1:23
             |  |
             |1 |   AccessProvider.create({
             |  |  _______________________^
             |2 | |   name: "anAccessProvider",
             |3 | |   issuer: "https://fauna.auth0.com",
             |4 | |   jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |5 | |   audience: "https://db.fauna.com/db/yyyyyyyyyy",
             |6 | |   roles: "aRole"
             |7 | | })
             |  | |_^
             |  |""".stripMargin
        )
      }

      "fail to create with invalid fields" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'Users' })")

        evalOk(
          admin,
          """|Role.create({
             |  name: "aRole",
             |  privileges: {
             |    resource: "Users",
             |    actions: {}
             |  }
             |})
             |""".stripMargin
        )

        // invalid jwks_uri
        evalErr(
          admin,
          """|AccessProvider.create({
             |  name: "anAccessProvider",
             |  issuer: "https://fauna.auth0.com",
             |  jwks_uri: "http://fauna.auth0.com/.well-known/jwks.json",
             |  roles: "aRole"
             |})
             |""".stripMargin
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "constraint_failure",
                "Failed to create AccessProvider.",
                _,
                Seq(
                  ValidatorFailure(
                    Path(Right("jwks_uri")),
                    "`http://fauna.auth0.com/.well-known/jwks.json` must be a valid https URI."))
              ) =>
        }

        val AccessProviderRole = SchemaCollection.AccessProviderRole

        // invalid role type
        evalErr(
          admin,
          """|AccessProvider.create({
             |  name: "anAccessProvider",
             |  issuer: "https://fauna.auth0.com",
             |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |  roles: [ Time.now() ],
             |})
             |""".stripMargin,
          typecheck = false
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "constraint_failure",
                "Failed to create AccessProvider.",
                _,
                Seq(
                  TypeMismatch(
                    Path(Right("roles"), Left(0)),
                    AccessProviderRole,
                    SchemaType.Scalar(ScalarType.Time)))
              ) =>
        }

        val err = evalErr(
          admin,
          """|AccessProvider.create({
             |  name: "anAccessProvider",
             |  issuer: "https://fauna.auth0.com",
             |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |  roles: [ Time.now() ],
             |})
             |""".stripMargin
        ).errors.head.renderWithSource(Map.empty)

        err shouldBe (
          """|error: Type `{ name: "anAccessProvider", issuer: "https://fauna.auth0.com", jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json", roles: [Time] }` is not a subtype of `{ name: String, issuer: String, jwks_uri: String, roles: String | { role: String, predicate: String } | Array<String | { role: String, predicate: String }> | Null, data: { *: Any } | Null }`
             |at *query*:1:23
             |  |
             |1 |   AccessProvider.create({
             |  |  _______________________^
             |2 | |   name: "anAccessProvider",
             |3 | |   issuer: "https://fauna.auth0.com",
             |4 | |   jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |5 | |   roles: [ Time.now() ],
             |6 | | })
             |  | |_^
             |  |
             |cause: Type `Time` is not a subtype of `String | { role: String, predicate: String }`
             |  |
             |1 |   AccessProvider.create({
             |  |  _______________________^
             |2 | |   name: "anAccessProvider",
             |3 | |   issuer: "https://fauna.auth0.com",
             |4 | |   jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |5 | |   roles: [ Time.now() ],
             |6 | | })
             |  | |_^
             |  |
             |cause: Type `[Time]` is not a subtype of `{ role: String, predicate: String } | String | Null`
             |at *query*:5:10
             |  |
             |5 |   roles: [ Time.now() ],
             |  |          ^^^^^^^^^^^^^^
             |  |""".stripMargin
        )

        renderErr(
          admin,
          """|AccessProvider.create({
             |  name: "anAccessProvider",
             |  issuer: "https://fauna.auth0.com",
             |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |  roles: [ "Foo", { role: "Bar", predicate: 'arg => true' } ],
             |})
             |""".stripMargin
        ) shouldBe (
          """|error: Invalid database schema update.
             |    error: Role `Foo` does not exist
             |    at main.fsl:15:8
             |       |
             |    15 |   role Foo
             |       |        ^^^
             |       |
             |    error: Role `Bar` does not exist
             |    at main.fsl:16:8
             |       |
             |    16 |   role Bar {
             |       |        ^^^
             |       |
             |at *query*:1:22
             |  |
             |1 |   AccessProvider.create({
             |  |  ______________________^
             |2 | |   name: "anAccessProvider",
             |3 | |   issuer: "https://fauna.auth0.com",
             |4 | |   jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |5 | |   roles: [ "Foo", { role: "Bar", predicate: 'arg => true' } ],
             |6 | | })
             |  | |__^
             |  |""".stripMargin
        )
      }
      "fail to create with two same role entries" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'Users' })")

        evalOk(
          admin,
          """|Role.create({
             |  name: "aRole",
             |  privileges: {
             |    resource: "Users",
             |    actions: {}
             |  }
             |})
             |""".stripMargin
        )

        val res = evalErr(
          admin,
          """|AccessProvider.create({
             |  name: "anAccessProvider",
             |  issuer: "https://fauna.auth0.com",
             |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |  roles: [ "aRole", { role: "aRole", predicate: "_ => true" } ],
             |})
             |""".stripMargin
        )

        res should matchPattern {
          case QueryRuntimeFailure.Simple(
                "constraint_failure",
                "Failed to create AccessProvider.",
                _,
                Seq(
                  ValidatorFailure(
                    Path(Right("roles")),
                    "The role `aRole` is present more than once, there can only be one entry per role."))
              ) =>
        }
      }
      "rejects writes to customer tenant root" in {
        val auth = newDB.withPermissions(AdminPermissions)

        val tenantRoot =
          newCustomerTenantRoot(auth).withPermissions(AdminPermissions)

        val err = evalErr(
          tenantRoot,
          "AccessProvider.create({ name: 'rootAP', issuer: 'iss', jwks_uri: 'https://test' })"
        )

        err.code shouldBe "constraint_failure"
        err.failureMessage shouldBe "Failed to create AccessProvider."
        err.asInstanceOf[QueryRuntimeFailure].constraintFailures shouldBe Seq(
          TenantRootWriteFailure("AccessProvider")
        )
      }
      "allows writes to internal tenant root" in {
        val auth = newDB.withPermissions(AdminPermissions)

        val tenantRoot =
          newInternalTenantRoot(auth).withPermissions(AdminPermissions)

        evalOk(
          tenantRoot,
          "AccessProvider.create({ name: 'rootAP', issuer: 'iss', jwks_uri: 'https://test' })"
        )
      }

      "allows staged deletes" in {
        val auth = newDB.withPermissions(AdminPermissions)

        evalOk(
          auth,
          "AccessProvider.create({ name: 'foo', issuer: 'iss', jwks_uri: 'https://test' })")

        ctx ! SchemaStatus.pinActiveSchema(auth.scopeID, SchemaVersion.Min)

        evalOk(auth, "AccessProvider.byName('foo')!.delete()")
      }
    }

    "update" - {
      "works" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'Users' })")

        evalOk(
          admin,
          """|Role.create({
             |  name: "aRole",
             |  privileges: {
             |    resource: "Users",
             |    actions: {}
             |  }
             |})
             |""".stripMargin
        )

        evalOk(
          admin,
          """|Role.create({
             |  name: "anotherRole",
             |  privileges: {
             |    resource: "Users",
             |    actions: {}
             |  }
             |})
             |""".stripMargin
        )

        evalOk(
          admin,
          """|AccessProvider.create({
             |  name: "anAccessProvider",
             |  issuer: "https://fauna.auth0.com",
             |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |  roles: "aRole"
             |})
             |""".stripMargin
        )

        val ret = evalOk(
          admin,
          """|AccessProvider.byName("anAccessProvider")!.update({
             |  name: "theAccessProvider",
             |  issuer: "https://fauna-updated.auth0.com",
             |  roles: "anotherRole"
             |}) {
             |  name,
             |  issuer,
             |  roles
             |}""".stripMargin
        )

        (ret / "name").as[String] shouldBe "theAccessProvider"
        (ret / "issuer").as[String] shouldBe "https://fauna-updated.auth0.com"
        (ret / "roles").as[String] shouldBe "anotherRole"
      }

      "cannot update audience" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'Users' })")

        evalOk(
          admin,
          """|Role.create({
             |  name: "aRole",
             |  privileges: {
             |    resource: "Users",
             |    actions: {}
             |  }
             |})
             |""".stripMargin
        )

        evalOk(
          admin,
          """|Role.create({
             |  name: "anotherRole",
             |  privileges: {
             |    resource: "Users",
             |    actions: {}
             |  }
             |})
             |""".stripMargin
        )

        evalOk(
          admin,
          """|AccessProvider.create({
             |  name: "anAccessProvider",
             |  issuer: "https://fauna.auth0.com",
             |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |  roles: "aRole"
             |})
             |""".stripMargin
        )

        renderErr(
          admin,
          """|AccessProvider.byName("anAccessProvider")!.update({
             |  audience: "https://db.fauna.com/db/yyyyyyyyyy"
             |})
             |""".stripMargin
        ) shouldBe (
          """|error: Failed to update AccessProvider `anAccessProvider`.
             |constraint failures:
             |  audience: Failed to update field because it is readonly
             |at *query*:1:50
             |  |
             |1 |   AccessProvider.byName("anAccessProvider")!.update({
             |  |  __________________________________________________^
             |2 | |   audience: "https://db.fauna.com/db/yyyyyyyyyy"
             |3 | | })
             |  | |__^
             |  |""".stripMargin
        )
      }
      "fail to update with two same role entries" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'Users' })")

        evalOk(
          admin,
          """|Role.create({
             |  name: "aRole",
             |  privileges: {
             |    resource: "Users",
             |    actions: {}
             |  }
             |})
             |""".stripMargin
        )

        evalOk(
          admin,
          """|AccessProvider.create({
             |  name: "anAccessProvider",
             |  issuer: "https://fauna.auth0.com",
             |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |})
             |""".stripMargin
        )

        val res = evalErr(
          admin,
          """|AccessProvider.byName("anAccessProvider")!.update({
             |  name: "anAccessProvider",
             |  issuer: "https://fauna.auth0.com",
             |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |  roles: [ "aRole", { role: "aRole", predicate: "_ => true" } ],
             |})
             |""".stripMargin
        )
        res should matchPattern {
          case QueryRuntimeFailure.Simple(
                "constraint_failure",
                "Failed to update AccessProvider `anAccessProvider`.",
                _,
                Seq(
                  ValidatorFailure(
                    Path(Right("roles")),
                    "The role `aRole` is present more than once, there can only be one entry per role."))
              ) =>
        }
      }
    }

    "replace" - {
      "preserves audience" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        val ret1 = evalOk(
          admin,
          """|AccessProvider.create({
             |  name: "ap",
             |  issuer: "https://fauna.auth0.com",
             |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |})
             |""".stripMargin
        )

        val aud1 = (getDocFields(admin, ret1) / "audience").as[String]

        val ret2 = evalOk(
          admin,
          """|AccessProvider.byName('ap')!.replace({
             |  name: "ap",
             |  issuer: "https://fauna.auth0.com",
             |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |})
             |""".stripMargin
        )

        val aud2 = (getDocFields(admin, ret2) / "audience").as[String]
        aud2 shouldEqual aud1
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
             |  privileges: {
             |    resource: "Users",
             |    actions: {}
             |  }
             |})
             |""".stripMargin
        )

        evalOk(
          admin,
          """|AccessProvider.create({
             |  name: "anAccessProvider",
             |  issuer: "https://fauna.auth0.com",
             |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
             |  roles: "aRole"
             |})
             |""".stripMargin
        )

        evalOk(admin, """AccessProvider.byName("anAccessProvider")!.delete()""")

        eventually(timeout(5.seconds)) {
          evalErr(
            admin,
            """AccessProvider.byName("anAccessProvider")!""") should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "document_not_found",
                  "AccessProvider `anAccessProvider` not found.",
                  _,
                  Seq()) =>
          }
        }
      }
    }

    "typechecks membership predicates" in {
      val auth = newDB
      val admin = auth.withPermissions(AdminPermissions)

      evalErr(
        admin,
        """|Collection.create({ name: "User" })
           |
           |Role.create({ name: "aRole", privileges: { resource: "User", actions: {} } })
           |
           |AccessProvider.create({
           |  name: "anAccessProvider",
           |  issuer: "https://fauna.auth0.com",
           |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
           |  roles: [
           |    {
           |      role: "aRole",
           |      predicate: "_ => Users.all == null"
           |    }
           |  ]
           |})""".stripMargin
      ) shouldEqual QueryRuntimeFailure(
        "invalid_schema",
        """|Invalid database schema update.
           |    error: Unbound variable `Users`
           |    at main.fsl:16:23
           |       |
           |    16 |     predicate ((_) => Users.all == null)
           |       |                       ^^^^^
           |       |""".stripMargin,
        Span(137, 358, Src.Query(""))
      )
    }
    "fql4" - {
      "create" - {
        "prevents role predicate resource string" in {
          val auth = newDB.withPermissions(AdminPermissions)
          evalOk(auth, "Role.create({ name: 'MyFQLXRole' })")
          validateV4ErrorResponse(
            evalV4Err(
              auth,
              CreateAccessProvider(
                MkObject(
                  "name" -> "myAP",
                  "issuer" -> "https://fauna.auth0.com",
                  "jwks_uri" -> "https://fauna.auth0.com/.well-known/jwks.json",
                  "roles" -> Seq(
                    MkObject(
                      "role" -> "MyFQLXRole",
                      "predicate" -> "_ => true"
                    )
                  )
                )
              )
            ),
            InvalidType(
              path = List("roles", "predicate"),
              expected = QueryV.Type,
              actual = StringV.Type
            )
          )
        }

        "validates schema with role name" in {
          val auth = newDB.withPermissions(AdminPermissions)
          validateV4ErrorResponse(
            evalV4Err(
              auth,
              CreateAccessProvider(
                MkObject(
                  "name" -> "myAP",
                  "issuer" -> "https://fauna.auth0.com",
                  "jwks_uri" -> "https://fauna.auth0.com/.well-known/jwks.json",
                  "roles" -> Seq(
                    MkObject(
                      "role" -> "AMissingRole",
                      "predicate" -> QueryF(Lambda("x" -> true))
                    )
                  )
                )
              )
            ),
            ValidationFailure(
              path = List("roles"),
              "Role `AMissingRole` does not exist."
            )
          )
        }
      }
      "update" - {
        "prevents predicate resource string" in {
          val auth = newDB.withPermissions(AdminPermissions)
          evalOk(auth, "Role.create({ name: 'MyFQLXRole' })")
          evalV4Ok(
            auth,
            CreateAccessProvider(
              MkObject(
                "name" -> "myAP",
                "issuer" -> "https://fauna.auth0.com",
                "jwks_uri" -> "https://fauna.auth0.com/.well-known/jwks.json",
                "roles" -> Seq(
                  MkObject(
                    "role" -> "MyFQLXRole",
                    "predicate" -> QueryF(Lambda("x" -> true))
                  )
                )
              )
            )
          )
          validateV4ErrorResponse(
            evalV4Err(
              auth,
              Update(
                Ref("access_providers/myAP"),
                MkObject(
                  "roles" -> Seq(
                    MkObject(
                      "role" -> "MyFQLXRole",
                      "predicate" -> "_ => true"
                    ))
                )
              )),
            InvalidType(
              path = List("roles", "predicate"),
              expected = QueryV.Type,
              actual = StringV.Type
            )
          )
        }

        "validates schema with role name" in {
          val auth = newDB.withPermissions(AdminPermissions)
          evalOk(auth, "Role.create({ name: 'MyFQLXRole' })")
          evalV4Ok(
            auth,
            CreateAccessProvider(
              MkObject(
                "name" -> "myAP",
                "issuer" -> "https://fauna.auth0.com",
                "jwks_uri" -> "https://fauna.auth0.com/.well-known/jwks.json",
                "roles" -> Seq(
                  MkObject(
                    "role" -> "MyFQLXRole",
                    "predicate" -> QueryF(Lambda("x" -> true))
                  )
                )
              )
            )
          )
          validateV4ErrorResponse(
            evalV4Err(
              auth,
              Update(
                Ref("access_providers/myAP"),
                MkObject(
                  "roles" -> Seq(
                    MkObject(
                      "role" -> "AMissingRole",
                      "predicate" -> QueryF(Lambda("x" -> true))
                    ))
                )
              )),
            ValidationFailure(
              path = List("roles"),
              "Role `AMissingRole` does not exist."
            )
          )
        }
      }
    }

    "name must be unique" in {
      val auth = newDB.withPermissions(AdminPermissions)

      evalOk(
        auth,
        "AccessProvider.create({ name: 'a', issuer: 'a', jwks_uri: 'https://a.com' })")

      val res = evalErr(
        auth,
        "AccessProvider.create({ name: 'a', issuer: 'b', jwks_uri: 'https://b.com' })")

      inside(res) { case QueryRuntimeFailure(code, _, _, _, Seq(cf), _) =>
        code shouldEqual "constraint_failure"
        cf.label shouldEqual "name"
      }
    }

    "issuer must be unique" in {
      val auth = newDB.withPermissions(AdminPermissions)

      evalOk(
        auth,
        "AccessProvider.create({ name: 'a', issuer: 'a', jwks_uri: 'https://a.com' })")

      val res = evalErr(
        auth,
        "AccessProvider.create({ name: 'b', issuer: 'a', jwks_uri: 'https://b.com' })")

      inside(res) { case QueryRuntimeFailure(code, _, _, _, Seq(cf), _) =>
        code shouldEqual "constraint_failure"
        cf.label shouldEqual "issuer"
      }
    }
  }

  "can refer to v4 roles" in {
    val auth = newDB.withPermissions(AdminPermissions)

    evalOk(auth, "Collection.create({ name: 'Foo' })")

    evalV4Ok(
      auth,
      CreateRole(
        MkObject(
          "name" -> "v4Role",
          "privileges" -> Seq(
            MkObject("resource" -> ClsRefV("Foo"))
          ))))

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|access provider Foo {
           |  issuer 'a'
           |  jwks_uri 'https://b.com'
           |  role v4Role
           |}""".stripMargin
    )
  }
}
