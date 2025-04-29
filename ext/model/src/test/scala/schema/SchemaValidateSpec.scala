package fauna.model.test

import fauna.auth.AdminPermissions
import fauna.auth.Auth

class SchemaValidateSpec extends FQL2Spec {
  var auth: Auth = _

  before {
    auth = newDB.withPermissions(AdminPermissions)
  }

  "validate adding an index works" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}""".stripMargin
    )

    validateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  index byName {
           |    terms [.name]
           |    values [.name]
           |  }
           |}""".stripMargin
    ) shouldBe (
      """|* Modifying collection `User` at main.fsl:1:1:
         |  * Indexes:
         |  + add index `byName`
         |
         |""".stripMargin
    )
  }

  "validate modifying an index works" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}""".stripMargin
    )

    validateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  index byName {
           |    terms [.name]
           |    values [.name]
           |  }
           |}""".stripMargin
    ) shouldBe (
      """|* Modifying collection `User` at main.fsl:1:1:
         |  * Indexes:
         |  ~ change index `byName`
         |    + add values set to [.name]
         |
         |""".stripMargin
    )
  }

  "validate against staged schema works" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}""".stripMargin
    )

    updateSchemaPinOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}""".stripMargin
    )

    // This validates the new schema against the active schema.
    validateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  index byName {
           |    terms [.name]
           |    values [.name]
           |  }
           |}""".stripMargin
    ) shouldBe (
      """|* Modifying collection `User` at main.fsl:1:1:
         |  * Indexes:
         |  + add index `byName`
         |
         |""".stripMargin
    )

    // This validates the new schema against the staged schema.
    validateStagedSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  index byName {
           |    terms [.name]
           |    values [.name]
           |  }
           |}""".stripMargin
    ) shouldBe (
      """|* Modifying collection `User` at main.fsl:1:1:
         |  * Indexes:
         |  ~ change index `byName`
         |    + add values set to [.name]
         |
         |""".stripMargin
    )
  }
}
