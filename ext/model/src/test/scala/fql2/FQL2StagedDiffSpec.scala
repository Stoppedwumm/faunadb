package fauna.model.test

class FQL2StagedDiffSpec extends FQL2StagedSchemaBaseSpec {
  "schema diff works" in {
    updateSchemaOk(auth, "main.fsl" -> "function foo() { 1 }")

    pin()

    updateSchemaOk(auth, "main.fsl" -> "function foo() { 2 }")

    statusStr() shouldBe "ready"
    diff() shouldBe (
      """|* Modifying function `foo` at main.fsl:1:1:
         |  ~ change body
         |
         |""".stripMargin
    )

    abandon()

    diff() shouldBe ""
    statusStr() shouldBe "none"
  }

  "schema diff shows index statuses" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}""".stripMargin
    )

    evalOk(
      auth,
      """|Set.sequence(0, 129).forEach(i => {
         |  User.create({ name: "user-#{i}" })
         |})""".stripMargin)

    pin()

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

    statusStr() shouldBe "pending"
    pendingSummary() shouldBe (
      """|* Collection User:
         |  ~ index byName: building
         |""".stripMargin
    )

    diff() shouldBe (
      """|* Modifying collection `User` at main.fsl:1:1:
         |  * Indexes:
         |  + add index `byName`
         |
         |""".stripMargin
    )

    while (tasks.nonEmpty) exec.step()

    statusStr() shouldBe "ready"
    pendingSummary() shouldBe ""
    diff() shouldBe (
      """|* Modifying collection `User` at main.fsl:1:1:
         |  * Indexes:
         |  + add index `byName`
         |
         |""".stripMargin
    )

    abandon()

    statusStr() shouldBe "none"
    pendingSummary() shouldBe ""
    diff() shouldBe ""
  }

  "schema diff shows unique constraint statuses" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}""".stripMargin
    )

    evalOk(
      auth,
      """|Set.sequence(0, 129).forEach(i => {
         |  User.create({ name: "user-#{i}" })
         |})""".stripMargin)

    pin()

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  unique [.name]
           |}""".stripMargin
    )

    statusStr() shouldBe "pending"
    pendingSummary() shouldBe (
      """|* Collection User:
         |  ~ unique constraint on [.name]: building
         |""".stripMargin
    )
    diff() shouldBe (
      """|* Modifying collection `User` at main.fsl:1:1:
         |  * Constraints:
         |  + add unique constraint on [.name]
         |
         |""".stripMargin
    )

    while (tasks.nonEmpty) exec.step()

    statusStr() shouldBe "ready"
    pendingSummary() shouldBe ""
    diff() shouldBe (
      """|* Modifying collection `User` at main.fsl:1:1:
         |  * Constraints:
         |  + add unique constraint on [.name]
         |
         |""".stripMargin
    )

    abandon()

    statusStr() shouldBe "none"
    pendingSummary() shouldBe ""
    diff() shouldBe ""
  }
}
