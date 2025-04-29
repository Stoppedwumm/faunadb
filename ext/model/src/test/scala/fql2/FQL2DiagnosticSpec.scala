package fauna.model.test

import fauna.auth.{ AdminPermissions, Auth }
import fauna.model.runtime.fql2.serialization.FQL2ValueMaterializer
import fauna.model.runtime.fql2.FQLInterpreter
import fql.ast.{ Span, Src }
import fql.error.{ Hint, Log }

class FQL2DiagnosticSpec extends FQL2Spec {
  import Hint.HintType

  var auth: Auth = _

  before {
    auth = newDB.withPermissions(AdminPermissions)
  }

  "emits log output for role predicates" in {
    evalOk(auth, """Collection.create({name: "Books"})""")

    val queryString =
      """|Role.create({
         |  name: "TRole",
         |  privileges: {
         |    resource: "Books",
         |    actions: {
         |      read: <<+BODY
         |        b => {
         |          log('role pred!')
         |          true
         |        }
         |       BODY
         |    }
         |  }
         |})""".stripMargin

    evalOk(
      auth,
      queryString
    )

    evalOk(auth, """Books.create({ title: "Dawnshard" })""")

    val key = mkKey(auth, "TRole")
    val res = evalRes(
      key,
      """|log("reading books!")
         |Books.all().first()
         |""".stripMargin
    )
    res.logs.runtime shouldEqual (
      Seq(
        Log("reading books!", Span(3, 21, Src.Query(queryString)), false),
        Log("role pred!", Span(12, 26, Src.Id("*predicate:TRole:read*")), false)
      )
    )
  }
  "shows diagnostics for conflicted paths that are invalidated" in {
    mkColl(auth, "User")

    val queryString =
      """[0, 1].map(a => {
        |  if (a == 0) {
        |    log("a 0")
        |    User.create({ id: 0, a: 5 })
        |  } else {
        |    log("reading a: #{User(0)}")
        |  }
        |  a
        |})""".stripMargin

    val res = evalRes(
      auth,
      queryString
    )

    res.logs.runtime shouldEqual Seq(
      Log("a 0", Span(41, 48, Src.Query(queryString)), false),
      Log("reading a: null", Span(100, 125, Src.Query(queryString)), false),
      Log(
        """reading a: { id: ID("0"), coll: User, ts: TransactionTime(), a: 5 }""",
        Span(100, 125, Src.Query(queryString)),
        false)
    )
  }

  "query performance hints" - {
    "doesn't emit performance diagnostics when not enabled" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Books {
             |  index byAuthor {
             |    terms [.author]
             |    values [.title, .price]
             |  }
             |}
             |""".stripMargin
      )

      evalOk(
        auth,
        """|Books.create({
           |  title: "Dawnshard",
           |  author: "test",
           |  category: "fantasy",
           |  price: 100
           |})
           |""".stripMargin
      )

      val res = evalRes(
        auth,
        """
          |Books.byAuthor("test") { title, author, price, category }
          |""".stripMargin,
        performanceHintsEnabled = false
      )
      res.logs.runtime shouldBe empty
    }
    "does not emit hint when document fields are accessed without an index" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Books {
             |  index byAuthor {
             |    terms [.author]
             |    values [.title]
             |  }
             |}
             |""".stripMargin
      )

      evalOk(
        auth,
        """|Books.create({
           |  title: "Dawnshard",
           |  author: "test",
           |  category: "fantasy",
           |  price: 100
           |})
           |""".stripMargin
      )

      val queryString = """Books.all().first() { category, price }"""

      val res = evalRes(
        auth,
        queryString,
        performanceHintsEnabled = true
      )

      res.logs.runtime shouldBe empty
    }
    "emits hints for materialized documents" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Books {
             |  index byAuthor {
             |    terms [.author]
             |    values [.title]
             |  }
             |}
             |""".stripMargin
      )

      evalOk(
        auth,
        """|
           |Set.sequence(0, 4).forEach(_ => {
           |  Books.create({
           |    title: "Dawnshard",
           |    author: "test",
           |    category: "fantasy",
           |    price: 100
           |  })
           |})
           |""".stripMargin
      )

      {
        val queryString = """Books.byAuthor("test")"""

        val res = evalOk(
          auth,
          queryString
        )

        val intp = new FQLInterpreter(auth, performanceDiagnosticsEnabled = true)

        val logs = ctx ! (FQL2ValueMaterializer.materialize(intp, res).flatMap { _ =>
          intp.infoWarns
        })

        logs.runtime shouldEqual Seq(
          Hint(
            "non_covered_document_read - Full documents returned from Books.byAuthor. See https://docs.fauna.com/performance_hint/non_covered_document_read.",
            Span(14, 22, Src.Query(queryString)),
            hintType = HintType.Performance
          )
        )
      }

      {
        val queryString = """Books.byAuthor("test").first()"""

        val res = evalOk(
          auth,
          queryString
        )

        val intp = new FQLInterpreter(auth, performanceDiagnosticsEnabled = true)

        val logs = ctx ! (FQL2ValueMaterializer.materialize(intp, res).flatMap { _ =>
          intp.infoWarns
        })

        logs.runtime shouldEqual Seq(
          Hint(
            "non_covered_document_read - Full documents returned from Books.byAuthor. See https://docs.fauna.com/performance_hint/non_covered_document_read.",
            Span(14, 22, Src.Query(queryString)),
            hintType = HintType.Performance
          )
        )
      }
    }
    "emits hint for uncovered index field access" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Books {
             |  index byAuthor {
             |    terms [.author]
             |    values [.title]
             |  }
             |}
             |""".stripMargin
      )

      evalOk(
        auth,
        """|Books.create({
           |  title: "Dawnshard",
           |  author: "test",
           |  category: "fantasy",
           |  price: 100
           |})
           |""".stripMargin
      )

      {
        val queryString = """Books.byAuthor("test").first() { price, category }"""

        val res = evalRes(
          auth,
          queryString,
          performanceHintsEnabled = true
        )

        res.logs.runtime shouldEqual Seq(
          Hint(
            "non_covered_document_read - .price is not covered by the Books.byAuthor index. See https://docs.fauna.com/performance_hint/non_covered_document_read.",
            Span(33, 38, Src.Query(queryString)),
            hintType = HintType.Performance
          ),
          Hint(
            "non_covered_document_read - .category is not covered by the Books.byAuthor index. See https://docs.fauna.com/performance_hint/non_covered_document_read.",
            Span(40, 48, Src.Query(queryString)),
            hintType = HintType.Performance
          )
        )
      }

      {
        val queryString =
          """Books.byAuthor("test").first() { price: .data.price, category: .data.category }"""

        val res = eval(
          auth,
          queryString,
          performanceHintsEnabled = true
        )

        res.logs.runtime shouldEqual Seq(
          Hint(
            "non_covered_document_read - .price is not covered by the Books.byAuthor index. See https://docs.fauna.com/performance_hint/non_covered_document_read.",
            Span(46, 51, Src.Query(queryString)),
            hintType = HintType.Performance
          ),
          Hint(
            "non_covered_document_read - .category is not covered by the Books.byAuthor index. See https://docs.fauna.com/performance_hint/non_covered_document_read.",
            Span(69, 77, Src.Query(queryString)),
            hintType = HintType.Performance
          )
        )
      }
    }
    "doesn't emit hint when covered fields are defined with .data prefix" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Books {
             |  index byAuthor {
             |    terms [.data.author]
             |    values [.data.price, .data.category]
             |  }
             |}
             |""".stripMargin
      )

      evalOk(
        auth,
        """|Books.create({
           |  title: "Dawnshard",
           |  author: "test",
           |  category: "fantasy",
           |  price: 100
           |})
           |""".stripMargin
      )

      val queryString = """Books.byAuthor("test").first() { price, category }"""

      val res = eval(
        auth,
        queryString,
        performanceHintsEnabled = true
      )

      res.logs.runtime shouldBe empty
    }

    "doesn't emit hint when .data access is used for covered fields" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Books {
             |  index byAuthor {
             |    terms [.data.author]
             |    values [.data.price, .data.category]
             |  }
             |}
             |""".stripMargin
      )

      evalOk(
        auth,
        """|Books.create({
           |  title: "Dawnshard",
           |  author: "test",
           |  category: "fantasy",
           |  price: 100
           |})
           |""".stripMargin
      )

      val queryString =
        """Books.byAuthor("test").first() { price: .data.price, category: .data.category }"""

      val res = eval(
        auth,
        queryString,
        performanceHintsEnabled = true
      )

      res.logs.runtime shouldBe empty
    }
    "doesn't emit hint for natively covered meta field" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Books {
             |  index byAuthor {
             |    terms [.author]
             |    values [.price, .category]
             |  }
             |}
             |""".stripMargin
      )

      evalOk(
        auth,
        """|Books.create({
           |  title: "Dawnshard",
           |  author: "test",
           |  category: "fantasy",
           |  price: 100
           |})
           |""".stripMargin
      )

      val queryString =
        """Books.byAuthor("test").first() { id }"""

      val res = eval(
        auth,
        queryString,
        performanceHintsEnabled = true
      )

      res.logs.runtime shouldBe empty
    }

    "dedupes hints when same hint shows up for same span" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Books {
             |  index byAuthor {
             |    terms [.author]
             |    values [.title]
             |  }
             |}
             |""".stripMargin
      )

      evalOk(
        auth,
        """|
           |Set.sequence(0, 4).forEach(_ => {
           |  Books.create({
           |    title: "Dawnshard",
           |    author: "test",
           |    category: "fantasy",
           |    price: 100
           |  })
           |})
           |""".stripMargin
      )

      val queryString =
        """(Books.byAuthor("test") { category, price, author }).paginate()"""

      val res = evalRes(
        auth,
        queryString,
        performanceHintsEnabled = true
      )

      res.logs.runtime shouldEqual
        Seq(
          Hint(
            "non_covered_document_read - .category is not covered by the Books.byAuthor index. See https://docs.fauna.com/performance_hint/non_covered_document_read.",
            Span(26, 34, Src.Query(queryString)),
            hintType = HintType.Performance
          ),
          Hint(
            "non_covered_document_read - .price is not covered by the Books.byAuthor index. See https://docs.fauna.com/performance_hint/non_covered_document_read.",
            Span(36, 41, Src.Query(queryString)),
            hintType = HintType.Performance
          )
        )
    }
    "includes performance diagnostics across invalidated paths that are not re-run" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Books {
             |  index byAuthor {
             |    terms [.author]
             |    values [.title, .price]
             |  }
             |}
             |""".stripMargin
      )

      evalOk(
        auth,
        """|
           |Books.create({
           |  title: "Dawnshard",
           |  author: "test",
           |  category: "fantasy",
           |  price: 100
           |})
           |Books.create({
           |  title: "Dawnshard",
           |  author: "test",
           |  category: "fantasy",
           |  price: 100
           |})
           |""".stripMargin
      )

      val queryString =
        """[0, 1].map(a => {
          |  if (a == 0) {
          |    Books.byAuthor("test").map(.category).first()
          |    Books.create({ id: 0, a: 5 })
          |  } else {
          |    if (!Books.byId(0).exists()) {
          |      Books.byAuthor("test").map(.category).paginate()
          |    }
          |  }
          |  a
          |})""".stripMargin

      val res = evalRes(
        auth,
        queryString,
        performanceHintsEnabled = true
      )

      res.logs.runtime shouldEqual Seq(
        Hint(
          "non_covered_document_read - .category is not covered by the Books.byAuthor index. See https://docs.fauna.com/performance_hint/non_covered_document_read.",
          Span(66, 74, Src.Query(queryString)),
          hintType = HintType.Performance
        ),
        Hint(
          "non_covered_document_read - .category is not covered by the Books.byAuthor index. See https://docs.fauna.com/performance_hint/non_covered_document_read.",
          Span(198, 206, Src.Query(queryString)),
          hintType = HintType.Performance
        )
      )
    }
    "deduplicates performance diagnostics across invalidated paths that are re-run" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Books {
             |  index byAuthor {
             |    terms [.author]
             |    values [.title, .price]
             |  }
             |}
             |""".stripMargin
      )

      evalOk(
        auth,
        """|
           |Books.create({
           |  title: "Dawnshard",
           |  author: "test",
           |  category: "fantasy",
           |  price: 100
           |})
           |Books.create({
           |  title: "Dawnshard",
           |  author: "test",
           |  category: "fantasy",
           |  price: 100
           |})
           |""".stripMargin
      )

      val queryString =
        """[0, 1].map(a => {
          |  if (a == 0) {
          |    Books.byAuthor("test").map(.category).first()
          |    Books.create({ id: 0, a: 5 })
          |  } else {
          |    log("test")
          |    Books.byAuthor("test").map(.category).paginate()
          |    Books.byId(0)!
          |  }
          |  a
          |})""".stripMargin

      val res = evalRes(
        auth,
        queryString,
        performanceHintsEnabled = true
      )

      res.logs.runtime shouldEqual Seq(
        Log("test", Span(136, 144, Src.Query(queryString)), false),
        Log("test", Span(136, 144, Src.Query(queryString)), false),
        Hint(
          "non_covered_document_read - .category is not covered by the Books.byAuthor index. See https://docs.fauna.com/performance_hint/non_covered_document_read.",
          Span(66, 74, Src.Query(queryString)),
          hintType = HintType.Performance
        ),
        Hint(
          "non_covered_document_read - .category is not covered by the Books.byAuthor index. See https://docs.fauna.com/performance_hint/non_covered_document_read.",
          Span(177, 185, Src.Query(queryString)),
          hintType = HintType.Performance
        )
      )
    }
    "emits hints for nested field access with partial coverage" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection TestColl {
             |  index testIndex {
             |    terms [.test_term]
             |    values [.foo.a.test_1]
             |  }
             |}
             |""".stripMargin
      )

      evalOk(
        auth,
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
           |""".stripMargin
      )

      {
        val queryString =
          """
            |TestColl.testIndex("t0").first() {
            |  test_1: .foo.a.test_1,
            |  test_2: .foo.a.test_2,
            |  test_3: .foo.a.test_3
            |}
            |""".stripMargin

        val res = eval(
          auth,
          queryString,
          performanceHintsEnabled = true
        )

        res.logs.runtime shouldEqual Seq(
          Hint(
            "non_covered_document_read - .foo.a.test_2 is not covered by the TestColl.testIndex index. See https://docs.fauna.com/performance_hint/non_covered_document_read.",
            Span(78, 84, Src.Query(queryString)),
            hintType = Hint.HintType.Performance
          ),
          Hint(
            "non_covered_document_read - .foo.a.test_3 is not covered by the TestColl.testIndex index. See https://docs.fauna.com/performance_hint/non_covered_document_read.",
            Span(103, 109, Src.Query(queryString)),
            hintType = Hint.HintType.Performance
          )
        )
      }

      {
        val queryString =
          """
            |TestColl.testIndex("t0").first() {
            |  test_1: .data.foo.a.test_1,
            |  test_2: .data.foo.a.test_2,
            |  test_3: .data.foo.a.test_3
            |}
            |""".stripMargin

        val res = eval(
          auth,
          queryString,
          performanceHintsEnabled = true
        )

        res.logs.runtime shouldEqual Seq(
          Hint(
            "non_covered_document_read - .foo.a.test_2 is not covered by the TestColl.testIndex index. See https://docs.fauna.com/performance_hint/non_covered_document_read.",
            Span(88, 94, Src.Query(queryString)),
            hintType = Hint.HintType.Performance
          ),
          Hint(
            "non_covered_document_read - .foo.a.test_3 is not covered by the TestColl.testIndex index. See https://docs.fauna.com/performance_hint/non_covered_document_read.",
            Span(118, 124, Src.Query(queryString)),
            hintType = Hint.HintType.Performance
          )
        )
      }
    }

    "emits hint when only a partial of a struct that is returned is covered" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection TestColl {
             |  index testIndex {
             |    terms [.test_term]
             |    values [.foo.a.test_1]
             |  }
             |}
             |""".stripMargin
      )

      evalOk(
        auth,
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
           |""".stripMargin
      )

      val queryString =
        """
          |TestColl.testIndex("t0").first()!.foo.a
          |""".stripMargin

      val res = evalOk(
        auth,
        queryString
      )

      val intp = new FQLInterpreter(auth, performanceDiagnosticsEnabled = true)

      val logs = ctx ! (FQL2ValueMaterializer.materialize(intp, res).flatMap { _ =>
        intp.infoWarns
      })

      logs.runtime shouldEqual Seq(
        Hint(
          "non_covered_document_read - .foo.a is not covered by the TestColl.testIndex index. See https://docs.fauna.com/performance_hint/non_covered_document_read.",
          Span(39, 40, Src.Query(queryString)),
          hintType = Hint.HintType.Performance
        )
      )
    }
    "emits hint when a hydrated struct that started as a partial is returned from a query" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection TestColl {
             |  index testIndex {
             |    terms [.test_term]
             |    values [.foo.a.test_1]
             |  }
             |}
             |""".stripMargin
      )

      evalOk(
        auth,
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
           |""".stripMargin
      )

      val queryString =
        """
          |let doc = TestColl.testIndex("t0").first()!
          |if (doc.foo.a.test_3 != null) {
          |  doc.foo.a
          |}
          |""".stripMargin

      val res = evalRes(
        auth,
        queryString,
        performanceHintsEnabled = true
      )

      val intp = new FQLInterpreter(auth, performanceDiagnosticsEnabled = true)

      val logs =
        ctx ! (FQL2ValueMaterializer.materialize(intp, res.value).flatMap { _ =>
          intp.infoWarns
        })

      res.logs.runtime ++ logs.runtime shouldEqual
        Seq(
          Hint(
            "non_covered_document_read - .foo.a.test_3 is not covered by the TestColl.testIndex index. See https://docs.fauna.com/performance_hint/non_covered_document_read.",
            Span(59, 65, Src.Query(queryString)),
            hintType = HintType.Performance
          ),
          Hint(
            "non_covered_document_read - .foo.a is not covered by the TestColl.testIndex index. See https://docs.fauna.com/performance_hint/non_covered_document_read.",
            Span(87, 88, Src.Query(queryString)),
            hintType = HintType.Performance
          )
        )
    }
    "doesn't emit hints when a covered struct is returned from a query" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection TestColl {
             |  index testIndex {
             |    terms [.test_term]
             |    values [.foo.a]
             |  }
             |}
             |""".stripMargin
      )

      evalOk(
        auth,
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
           |""".stripMargin
      )

      val queryString =
        """
          |let doc = TestColl.testIndex("t0").first()!
          |if (doc.foo.a.test_3 != null) {
          |  doc.foo.a
          |}
          |""".stripMargin

      val res = evalRes(
        auth,
        queryString,
        performanceHintsEnabled = true
      )

      val intp = new FQLInterpreter(auth, performanceDiagnosticsEnabled = true)

      val logs =
        ctx ! (FQL2ValueMaterializer.materialize(intp, res.value).flatMap { _ =>
          intp.infoWarns
        })

      res.logs.runtime ++ logs.runtime shouldEqual Seq.empty
    }
    "emits hint when .order() is used on an index sourced set" in {
      mkColl(auth, "TestColl")

      {
        val queryString = """TestColl.all().order(.name)"""

        val res = evalRes(
          auth,
          queryString,
          performanceHintsEnabled = true
        )

        res.logs.runtime shouldEqual Seq(
          Hint(
            "full_set_read - Using order() causes the full set to be read. See https://docs.fauna.com/performance_hint/full_set_read.",
            Span(20, 27, Src.Query(queryString)),
            hintType = Hint.HintType.Performance
          )
        )
      }

      {
        val queryString =
          """TestColl.all().map(v => { name: v.name }).order(.name)"""

        val res = evalRes(
          auth,
          queryString,
          performanceHintsEnabled = true
        )

        res.logs.runtime shouldEqual Seq(
          Hint(
            "full_set_read - Using order() causes the full set to be read. See https://docs.fauna.com/performance_hint/full_set_read.",
            Span(47, 54, Src.Query(queryString)),
            hintType = Hint.HintType.Performance
          )
        )
      }

      {
        val queryString =
          """[{ name: "a" }, { name: "b" }].toSet().concat(TestColl.all()).map(v => { name: v.name }).order(.name)"""

        val res = evalRes(
          auth,
          queryString,
          performanceHintsEnabled = true
        )

        res.logs.runtime shouldEqual Seq(
          Hint(
            "full_set_read - Using order() causes the full set to be read. See https://docs.fauna.com/performance_hint/full_set_read.",
            Span(94, 101, Src.Query(queryString)),
            hintType = Hint.HintType.Performance
          )
        )
      }
    }
    "emits hint when .aggregate() is used on an index sourced set" in {
      mkColl(auth, "TestColl")

      val queryString = """TestColl.all().map(.num).aggregate(0, (a, b) => a + b)"""

      val res = evalRes(
        auth,
        queryString,
        performanceHintsEnabled = true
      )

      res.logs.runtime shouldEqual Seq(
        Hint(
          "full_set_read - Using aggregate() causes the full set to be read. See https://docs.fauna.com/performance_hint/full_set_read.",
          Span(34, 54, Src.Query(queryString)),
          hintType = Hint.HintType.Performance
        )
      )
    }
  }
  "emits hint when .distinct() is used on an index sourced set" in {
    mkColl(auth, "TestColl")

    val queryString = """TestColl.all().distinct()"""

    val res = evalRes(
      auth,
      queryString,
      performanceHintsEnabled = true
    )

    res.logs.runtime shouldEqual Seq(
      Hint(
        "full_set_read - Using distinct() causes the full set to be read. See https://docs.fauna.com/performance_hint/full_set_read.",
        Span(23, 25, Src.Query(queryString)),
        hintType = Hint.HintType.Performance
      )
    )
  }
  "emits hint when .count() is used on an index sourced set" in {
    mkColl(auth, "TestColl")

    val queryString = """TestColl.all().count()"""

    val res = evalRes(
      auth,
      queryString,
      performanceHintsEnabled = true
    )

    res.logs.runtime shouldEqual Seq(
      Hint(
        "full_set_read - Using count() causes the full set to be read. See https://docs.fauna.com/performance_hint/full_set_read.",
        Span(20, 22, Src.Query(queryString)),
        hintType = Hint.HintType.Performance
      )
    )
  }
  "emits hint when .every() is used on an index sourced set" in {
    mkColl(auth, "TestColl")

    val queryString = """TestColl.all().every(.name == "hi")"""

    val res = evalRes(
      auth,
      queryString,
      performanceHintsEnabled = true
    )

    res.logs.runtime shouldEqual Seq(
      Hint(
        "full_set_read - Using every() causes the full set to be read. See https://docs.fauna.com/performance_hint/full_set_read.",
        Span(20, 35, Src.Query(queryString)),
        hintType = Hint.HintType.Performance
      )
    )
  }
  "emits hint when .fold() is used on an index sourced set" in {
    mkColl(auth, "TestColl")

    val queryString = """TestColl.all().fold(100, (val, doc) => val + doc.num)"""

    val res = evalRes(
      auth,
      queryString,
      performanceHintsEnabled = true
    )

    res.logs.runtime shouldEqual Seq(
      Hint(
        "full_set_read - Using fold() causes the full set to be read. See https://docs.fauna.com/performance_hint/full_set_read.",
        Span(19, 53, Src.Query(queryString)),
        hintType = Hint.HintType.Performance
      )
    )
  }
  "emits hint when .foldRight() is used on an index sourced set" in {
    mkColl(auth, "TestColl")

    val queryString =
      """TestColl.all().foldRight(100, (val, doc) => val + doc.num)"""

    val res = evalRes(
      auth,
      queryString,
      performanceHintsEnabled = true
    )

    res.logs.runtime shouldEqual Seq(
      Hint(
        "full_set_read - Using foldRight() causes the full set to be read. See https://docs.fauna.com/performance_hint/full_set_read.",
        Span(24, 58, Src.Query(queryString)),
        hintType = Hint.HintType.Performance
      )
    )
  }
  "emits hint when .forEach() is used on an index sourced set" in {
    mkColl(auth, "TestColl")

    val queryString =
      """TestColl.all().forEach(d => log(d.name))"""

    val res = evalRes(
      auth,
      queryString,
      performanceHintsEnabled = true
    )

    res.logs.runtime shouldEqual Seq(
      Hint(
        "full_set_read - Using forEach() causes the full set to be read. See https://docs.fauna.com/performance_hint/full_set_read.",
        Span(22, 40, Src.Query(queryString)),
        hintType = Hint.HintType.Performance
      )
    )
  }
  "emits hint when .includes() is used on an index sourced set" in {
    mkColl(auth, "TestColl")

    val queryString =
      """TestColl.all().map(.name).includes("test_name")"""

    val res = evalRes(
      auth,
      queryString,
      performanceHintsEnabled = true
    )

    res.logs.runtime shouldEqual Seq(
      Hint(
        "full_set_read - Using includes() causes the full set to be read. See https://docs.fauna.com/performance_hint/full_set_read.",
        Span(34, 47, Src.Query(queryString)),
        hintType = Hint.HintType.Performance
      )
    )
  }
  // for last we use reverse which is optimized for index sets so we don't need
  // to materialize the full set.
  "doesn't emits hint when .last() is used on an index sourced set" in {
    mkColl(auth, "TestColl")

    val queryString =
      """TestColl.all().last()"""

    val res = evalRes(
      auth,
      queryString,
      performanceHintsEnabled = true
    )

    res.logs.runtime shouldBe empty
  }
  // for lastWhere we use reverse which is optimized for index sets so we don't need
  // to materialize the full set.
  "doesn't emit hint when .lastWhere() is used on an index sourced set" in {
    mkColl(auth, "TestColl")

    val queryString =
      """TestColl.all().lastWhere(.name == "test_name")"""

    val res = evalRes(
      auth,
      queryString,
      performanceHintsEnabled = true
    )

    res.logs.runtime shouldBe empty
  }
  "emits hint when .reduce() is used on an index sourced set" in {
    mkColl(auth, "TestColl")

    val queryString =
      """TestColl.all().map(.name).reduce((s, v) => s.name)"""

    val res = evalRes(
      auth,
      queryString,
      performanceHintsEnabled = true
    )

    res.logs.runtime shouldEqual Seq(
      Hint(
        "full_set_read - Using reduce() causes the full set to be read. See https://docs.fauna.com/performance_hint/full_set_read.",
        Span(32, 50, Src.Query(queryString)),
        hintType = Hint.HintType.Performance
      )
    )
  }
  "emits hint when .reduceRight() is used on an index sourced set" in {
    mkColl(auth, "TestColl")

    val queryString =
      """TestColl.all().map(.name).reduceRight((s, v) => s.name)"""

    val res = evalRes(
      auth,
      queryString,
      performanceHintsEnabled = true
    )

    res.logs.runtime shouldEqual Seq(
      Hint(
        "full_set_read - Using reduceRight() causes the full set to be read. See https://docs.fauna.com/performance_hint/full_set_read.",
        Span(37, 55, Src.Query(queryString)),
        hintType = Hint.HintType.Performance
      )
    )
  }
  // our index sets reverse without full set materialization
  "doesn't emits hint when .reverse() is used on an index sourced set" in {
    mkColl(auth, "TestColl")

    val queryString =
      """TestColl.all().reverse()"""

    val res = evalRes(
      auth,
      queryString,
      performanceHintsEnabled = true
    )

    res.logs.runtime shouldBe empty
  }
  "emits hint when .toArray() is used on an index sourced set" in {
    mkColl(auth, "TestColl")

    val queryString =
      """TestColl.all().toArray()"""

    val res = evalRes(
      auth,
      queryString,
      performanceHintsEnabled = true
    )

    res.logs.runtime shouldEqual Seq(
      Hint(
        "full_set_read - Using toArray() causes the full set to be read. See https://docs.fauna.com/performance_hint/full_set_read.",
        Span(22, 24, Src.Query(queryString)),
        hintType = Hint.HintType.Performance
      )
    )
  }
  "doesn't emit hints when take set limits index set size" in {
    mkColl(auth, "TestColl")
    val queryString = """TestColl.all().take(10).order(.name)"""
    val res = evalRes(auth, queryString, performanceHintsEnabled = true)
    res.logs.runtime shouldEqual Seq.empty
  }

  "doesn't emit hints when .order() is called for non index sourced sets" in {
    mkColl(auth, "TestColl")
    val queryString = """[{ name: "a" }, { name: "b" }].toSet().order(.name)"""
    val res = evalRes(auth, queryString)
    res.logs.runtime shouldEqual Seq.empty
  }
}
