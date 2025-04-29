package fql2

import fauna.api.test.{ FQL2APISpec, FQL2Params }

class FQL2DiagnosticRenderingSpec extends FQL2APISpec {

  test("renders performance hints") {
    val db = aDatabase.sample
    updateSchemaOk(
      db,
      "main.fsl" ->
        """|collection Books {
           |  index byAuthor {
           |    terms [.author]
           |    values [.title, .price]
           |  }
           |}
           |""".stripMargin
    )

    queryOk(
      """Books.create({ author: "test_author", category: "fantasy" })""".stripMargin,
      db
    )

    val res = queryRaw(
      """Books.byAuthor("test_author") { category }""".stripMargin,
      db,
      FQL2Params(performanceHints = Some(true))
    )

    (res.json / "summary")
      .as[String] shouldEqual """performance_hint: non_covered_document_read - .category is not covered by the Books.byAuthor index. See https://docs.fauna.com/performance_hint/non_covered_document_read.
                                 |at *query*:1:33
                                 |  |
                                 |1 | Books.byAuthor("test_author") { category }
                                 |  |                                 ^^^^^^^^
                                 |  |""".stripMargin
  }

  test("renders performance hints encountered during materialization") {
    val db = aDatabase.sample
    updateSchemaOk(
      db,
      "main.fsl" ->
        """|collection TestColl {
           |  index testIndex {
           |    terms [.test_term]
           |    values [.foo.a.test_1]
           |  }
           |}
           |""".stripMargin
    )

    queryOk(
      """|
         |TestColl.create({
         |  test_term: "t0",
         |  foo: {
         |    a: {
         |      test_1: "t1",
         |      test_2: "t2",
         |      test_3: "t3"
         |    }
         |  },
         |})
         |""".stripMargin,
      db
    )

    val queryString =
      """
        |let doc = TestColl.testIndex("t0").first()!
        |if (doc.foo.a.test_3 != null) {
        |  doc.foo.a
        |}
        |""".stripMargin

    val res = queryRaw(
      queryString,
      db,
      FQL2Params(performanceHints = Some(true))
    )

    (res.json / "summary").as[String] shouldEqual
      """|performance_hint: non_covered_document_read - .foo.a.test_3 is not covered by the TestColl.testIndex index. See https://docs.fauna.com/performance_hint/non_covered_document_read.
         |at *query*:3:15
         |  |
         |3 | if (doc.foo.a.test_3 != null) {
         |  |               ^^^^^^
         |  |
         |
         |performance_hint: non_covered_document_read - .foo.a is not covered by the TestColl.testIndex index. See https://docs.fauna.com/performance_hint/non_covered_document_read.
         |at *query*:4:11
         |  |
         |4 |   doc.foo.a
         |  |           ^
         |  |""".stripMargin
  }

  test("renders Collection method hints") {
    val db = aDatabase.sample
    mkCollection(db, "TestColl")

    {
      val queryString = "TestColl.firstWhere(.name == 'test')"

      val res = queryRaw(
        queryString,
        db,
        FQL2Params(performanceHints = Some(true))
      )

      val expected =
        """|performance_hint: collection_scan - Using firstWhere() on collection TestColl can cause a read of every document. See https://docs.fauna.com/performance_hint/collection_scan.
           |at *query*:1:20
           |  |
           |1 | TestColl.firstWhere(.name == 'test')
           |  |                    ^^^^^^^^^^^^^^^^^
           |  |""".stripMargin

      (res.json / "summary").as[String] shouldEqual expected
    }

    {
      val queryString = "TestColl.where(.name == 'test')"

      val res = queryRaw(
        queryString,
        db,
        FQL2Params(performanceHints = Some(true))
      )

      val expected =
        """|performance_hint: collection_scan - Using where() on collection TestColl can cause a read of every document. See https://docs.fauna.com/performance_hint/collection_scan.
           |at *query*:1:15
           |  |
           |1 | TestColl.where(.name == 'test')
           |  |               ^^^^^^^^^^^^^^^^^
           |  |""".stripMargin

      (res.json / "summary").as[String] shouldEqual expected
    }
  }

  test("invalid performance hints parameter fails request") {
    val db = aDatabase.sample

    val res = queryRaw(
      """1 + 1""".stripMargin,
      db,
      FQL2Params(headers = Seq("x-performance-hints" -> "invalid_entry"))
    )
    (res.json / "error" / "code").as[String] shouldBe "invalid_request"
    (res.json / "error" / "message")
      .as[String] shouldBe "Invalid header 'x-performance-hints': Must be a boolean value."
  }

  test("does not render performance hints without provided header") {
    val db = aDatabase.sample
    updateSchemaOk(
      db,
      "main.fsl" ->
        """|collection Books {
           |  index byAuthor {
           |    terms [.author]
           |    values [.title, .price]
           |  }
           |}
           |""".stripMargin
    )

    queryOk(
      """Books.create({ author: "test_author", category: "fantasy" })""".stripMargin,
      db
    )

    val res = queryRaw(
      """Books.byAuthor("test_author") { category }""".stripMargin,
      db
    )

    (res.json / "summary").as[String] shouldBe empty
  }
}
