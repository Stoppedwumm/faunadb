package fauna.api.test

class FQL2FunctionsSpec extends FQL2APISpec {

  "get by name" - {
    test("works") {
      val db = aDatabase.sample

      val res = queryErr("""Function("add")""", db)
      (res / "error" / "code").as[String] shouldBe "invalid_argument"
      (res / "error" / "message")
        .as[String] shouldBe "invalid argument `function`: No such user function `add`."

      queryOk(
        """|Function.create({
           |  name: "add",
           |  body: "(x, y) => x + y",
           |  alias: "plus"
           |})""".stripMargin,
        db)

      // Retrieve by name or alias.
      for (name <- Seq("add", "plus")) {
        val sum = queryOk(
          s"""|let name = "$name"
            |Function(name)(1, 2)""".stripMargin,
          db)

        sum.as[Int] shouldBe 3
      }
    }
  }

  "abort" - {
    test("cancels a query and its effects") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample

      val res = queryErr(
        s"""|
        |let author = $collection.create({ 'a' : 'x' })
        |abort(0)
        |""".stripMargin,
        db
      )

      (res / "error" / "code").as[String] shouldEqual "abort"
      (res / "error" / "message")
        .as[String] shouldEqual "Query aborted."
      (res / "summary")
        .as[String] shouldEqual """|error: Query aborted.
        |at *query*:3:6
        |  |
        |3 | abort(0)
        |  |      ^^^
        |  |""".stripMargin

      queryOk(
        s"""$collection.all().count()""",
        db
      ).as[Int] shouldBe (0)
    }
  }

  test("fails create in tenant root") {
    val db = anAccountDatabase.sample
    val errRes = queryErr(
      "Function.create({ name: 'rootFunc', body: 'x => true' })",
      db
    )

    (errRes / "error" / "code").as[String] shouldBe "constraint_failure"
    (errRes / "error" / "message").as[String] shouldBe "Failed to create Function."

    (errRes / "summary").as[String] shouldEqual """error: Failed to create Function.
      |constraint failures:
      |  A Function cannot be created in your account root
      |at *query*:1:16
      |  |
      |1 | Function.create({ name: 'rootFunc', body: 'x => true' })
      |  |                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      |  |""".stripMargin
  }
}
