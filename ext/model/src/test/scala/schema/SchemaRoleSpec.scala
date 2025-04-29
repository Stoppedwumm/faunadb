package fauna.model.test

import fauna.auth.{ AdminPermissions, Auth, DocumentAuthScope }
import fauna.logging.ExceptionLogging
import fauna.model.Key

class SchemaRoleSpec extends FQL2Spec {
  var auth: Auth = _

  before {
    auth = newDB.withPermissions(AdminPermissions)
  }

  "privileges" - {
    "creates predicates correctly" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {}
             |role myRole {
             |  privileges User {
             |    read { predicate (doc => doc.readable) }
             |  }
             |}
             |""".stripMargin
      )
      evalOk(auth, "User.create({ id: 0, readable: true })")
      evalOk(auth, "User.create({ id: 1, readable: false })")

      val role = ctx ! Auth.changeRole(auth, Key.UserRoles("myRole"))
      evalOk(role, "User.byId('0')!.foo")
      evalErr(role, "User.byId('1')!.foo")
    }

    "creates predicates with short lambdas" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {}
             |role myRole {
             |  privileges User {
             |    read { predicate (.readable) }
             |  }
             |}
             |""".stripMargin
      )
      evalOk(auth, "User.create({ id: 0, readable: true })")
      evalOk(auth, "User.create({ id: 1, readable: false })")

      val role = ctx ! Auth.changeRole(auth, Key.UserRoles("myRole"))
      evalOk(role, "User.byId('0')!.foo")
      evalErr(role, "User.byId('1')!.foo")
    }

    "translates short lambdas from FQL to FSL" in {
      updateSchemaOk(auth, "main.fsl" -> "collection User {}")

      evalOk(
        auth,
        """|Role.create({
           |  name: "myRole",
           |  privileges: [{
           |    resource: "User",
           |    actions: {
           |      read: "(.readable)"
           |    }
           |  }]
           |})""".stripMargin
      )

      schemaContent(auth, "main.fsl") shouldBe Some(
        """|collection User {}
           |
           |role myRole {
           |  privileges User {
           |    read {
           |      predicate (.readable)
           |    }
           |  }
           |}
           |""".stripMargin
      )
    }

    "create doesn't allow duplicated items" in {
      ExceptionLogging.alwaysSquelch {
        updateSchemaOk(auth, "main.fsl" -> """collection Agents {}""")

        renderErr(
          auth,
          """|let roleDef = {
             |    name: "userRole",
             |    privileges: [
             |      { actions: { create: true }, resource: "Agents" },
             |      { actions: { create: true }, resource: "Agents" }
             |    ]
             |  }
             |
             |Role.create(roleDef)""".stripMargin
        ) shouldBe
          """|error: Invalid database schema update.
             |    error: Duplicate privileges `Agents`
             |    at main.fsl:12:14
             |       |
             |    12 |   privileges Agents {
             |       |              ^^^^^^
             |       |
             |    hint: Originally defined here
             |    at main.fsl:9:14
             |      |
             |    9 |   privileges Agents {
             |      |              ^^^^^^
             |      |
             |at *query*:9:12
             |  |
             |9 | Role.create(roleDef)
             |  |            ^^^^^^^^^
             |  |""".stripMargin
      }
    }

    "update doesn't allow duplicated items" in {
      ExceptionLogging.alwaysSquelch {
        updateSchemaOk(auth, "main.fsl" -> """collection Agents {}""")

        evalOk(
          auth,
          """|let roleDef = {
             |    name: "userRole",
             |    privileges: [
             |      { actions: { create: true }, resource: "Agents" }
             |    ]
             |  }
             |
             |Role.create(roleDef)""".stripMargin
        )

        renderErr(
          auth,
          """|let roleDef = {
             |    name: "userRole",
             |    privileges: [
             |      { actions: { create: true }, resource: "Agents" },
             |      { actions: { create: true }, resource: "Agents" }
             |    ]
             |  }
             |
             |Role.byName("userRole")!.update(roleDef)""".stripMargin
        ) shouldBe
          """|error: Invalid database schema update.
             |    error: Duplicate privileges `Agents`
             |    at main.fsl:12:14
             |       |
             |    12 |   privileges Agents {
             |       |              ^^^^^^
             |       |
             |    hint: Originally defined here
             |    at main.fsl:9:14
             |      |
             |    9 |   privileges Agents {
             |      |              ^^^^^^
             |      |
             |at *query*:9:32
             |  |
             |9 | Role.byName("userRole")!.update(roleDef)
             |  |                                ^^^^^^^^^
             |  |""".stripMargin
      }
    }
  }

  "membership" - {
    "creates predicates from lambdas" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {}
             |function MyFunction() { 3 }
             |role myRole {
             |  privileges MyFunction { call }
             |
             |  membership User {
             |    predicate ((doc) => doc.canCall)
             |  }
             |}
             |""".stripMargin
      )
      evalOk(auth, "User.create({ id: 0, canCall: true })")
      evalOk(auth, "User.create({ id: 1, canCall: false })")

      val secret =
        evalOk(auth, "Key.create({ role: 'admin' }).secret").as[String]

      val user0 =
        ctx ! Auth
          .lookup(secret, DocumentAuthScope("collections/User/0", List.empty))
          .map { _.get }
      evalOk(user0, "MyFunction()")

      val user1 =
        ctx ! Auth
          .lookup(secret, DocumentAuthScope("collections/User/1", List.empty))
          .map { _.get }
      val err = evalErr(user1, "MyFunction()")
      err.code shouldBe "permission_denied"
    }

    "creates predicates from short lambdas" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {}
             |function MyFunction() { 3 }
             |role myRole {
             |  privileges MyFunction { call }
             |
             |  membership User {
             |    predicate (.canCall)
             |  }
             |}
             |""".stripMargin
      )
      evalOk(auth, "User.create({ id: 0, canCall: true })")
      evalOk(auth, "User.create({ id: 1, canCall: false })")

      val secret =
        evalOk(auth, "Key.create({ role: 'admin' }).secret").as[String]

      val user0 =
        ctx ! Auth
          .lookup(secret, DocumentAuthScope("collections/User/0", List.empty))
          .map { _.get }
      evalOk(user0, "MyFunction()")

      val user1 =
        ctx ! Auth
          .lookup(secret, DocumentAuthScope("collections/User/1", List.empty))
          .map { _.get }
      val err = evalErr(user1, "MyFunction()")
      err.code shouldBe "permission_denied"
    }

    "translates short lambdas from FQL to FSL" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {}
             |function MyFunction() { 3 }
             |""".stripMargin
      )

      evalOk(
        auth,
        """|Role.create({
           |  name: "myRole",
           |  privileges: [{
           |    resource: "MyFunction",
           |    actions: {
           |      call: true,
           |    }
           |  }],
           |  membership: [{
           |    resource: "User",
           |    predicate: "(.canCall)"
           |  }]
           |})""".stripMargin
      )

      schemaContent(auth, "main.fsl") shouldBe Some(
        """|collection User {}
           |function MyFunction() { 3 }
           |
           |role myRole {
           |  privileges MyFunction {
           |    call
           |  }
           |  membership User {
           |    predicate (.canCall)
           |  }
           |}
           |""".stripMargin
      )
    }
  }
}
