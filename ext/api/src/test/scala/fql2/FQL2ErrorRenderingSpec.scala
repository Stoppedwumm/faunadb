package fauna.api.test

import scala.concurrent.duration._

class FQL2ErrorRenderingSpec extends FQL2APISpec {
  test("renders stack trace across udf calls") {
    val db = aDatabase.sample
    queryOk(
      """
        |Function.create({
        | name: "errFunc",
        | body: '() => abort("tracer")'
        |})
        |Function.create({
        | name: "eventuallyError",
        | body: '() => errFunc()'
        |})
        |""".stripMargin,
      db
    )

    val expectedSummary =
      """|error: Query aborted.
         |at *udf:errFunc*:1:12
         |  |
         |1 | () => abort("tracer")
         |  |            ^^^^^^^^^^
         |  |
         |at *udf:eventuallyError*:1:14
         |  |
         |1 | () => errFunc()
         |  |              ^^
         |  |
         |at *query*:2:16
         |  |
         |2 | eventuallyError()
         |  |                ^^
         |  |""".stripMargin

    val err = queryErr(
      """
        |eventuallyError()
        |""".stripMargin,
      db
    )
    (err / "summary").as[String] shouldEqual expectedSummary
  }

  test("renders IndexOutOfBounds stack trace across udf calls") {
    val db = aDatabase.sample
    queryOk(
      """
        |Function.create({
        | name: "errFunc",
        | body: '() => [1][2]'
        |})
        |Function.create({
        | name: "eventuallyError",
        | body: '() => errFunc()'
        |})
        |""".stripMargin,
      db
    )

    val expectedSummary =
      """|error: index 2 out of bounds for length 1
         |at *udf:errFunc*:1:10
         |  |
         |1 | () => [1][2]
         |  |          ^^^
         |  |
         |at *udf:eventuallyError*:1:14
         |  |
         |1 | () => errFunc()
         |  |              ^^
         |  |
         |at *query*:2:16
         |  |
         |2 | eventuallyError()
         |  |                ^^
         |  |""".stripMargin

    val err = queryErr(
      """
        |eventuallyError()
        |""".stripMargin,
      db
    )
    (err / "summary").as[String] shouldEqual expectedSummary
  }

  test("renders UnboundVariable stack trace across udf calls") {
    val db = aDatabase.sample
    queryOk(
      """
        |Function.create({
        | name: "errFunc",
        | body: '() => a'
        |})
        |Function.create({
        | name: "eventuallyError",
        | body: '() => errFunc()'
        |})
        |""".stripMargin,
      db
    )

    val expectedSummary =
      """|error: Unbound variable `a`
         |at *udf:errFunc*:1:7
         |  |
         |1 | () => a
         |  |       ^
         |  |
         |at *udf:eventuallyError*:1:14
         |  |
         |1 | () => errFunc()
         |  |              ^^
         |  |
         |at *query*:2:16
         |  |
         |2 | eventuallyError()
         |  |                ^^
         |  |""".stripMargin

    val err = queryErr(
      """
        |eventuallyError()
        |""".stripMargin,
      db
    )
    (err / "summary").as[String] shouldEqual expectedSummary
  }

  test("renders stack trace when invalid arguments are passed") {
    val db = aDatabase.sample
    queryOk(
      """
        |Function.create({
        | name: "errFunc",
        | body: '(a) => a',
        | signature: 'String => String'
        |})
        |Function.create({
        | name: "eventuallyError",
        | body: '() => "hi".concat({ cast: "this" })'
        |})
        |""".stripMargin,
      db
    )

    val expectedSummary =
      """|error: expected value for `other` of type String, received { *: Any }
         |at *udf:eventuallyError*:1:18
         |  |
         |1 | () => "hi".concat({ cast: "this" })
         |  |                  ^^^^^^^^^^^^^^^^^^
         |  |
         |at *query*:2:16
         |  |
         |2 | eventuallyError()
         |  |                ^^
         |  |""".stripMargin
    val err = queryErr(
      """
        |eventuallyError()
        |""".stripMargin,
      db,
      FQL2Params()
    )

    (err / "summary").as[String] shouldEqual expectedSummary
  }

  test("renders stack trace when failing to access a field") {
    val db = aDatabase.sample
    queryOk(
      """
        |Function.create({
        | name: "errFunc",
        | body: '(doc) => doc.a'
        |})
        |Function.create({
        | name: "eventuallyError",
        | body: '(doc) => errFunc(doc)'
        |})
        |Collection.create({
        |  name: "TestColl"
        |})
        |""".stripMargin,
      db
    )

    val docRes = queryOk("TestColl.create({})", db)
    val docID = (docRes / "id").as[String]

    val expectedSummary =
      s"""|error: Collection `TestColl` does not contain document with id $docID.
          |at *udf:errFunc*:1:10
          |  |
          |1 | (doc) => doc.a
          |  |          ^^^
          |  |
          |at *udf:eventuallyError*:1:17
          |  |
          |1 | (doc) => errFunc(doc)
          |  |                 ^^^^^
          |  |
          |at *query*:4:16
          |  |
          |4 | eventuallyError(doc)
          |  |                ^^^^^
          |  |""".stripMargin

    val err = queryErr(
      s"""
         |let doc = TestColl.byId('$docID')!
         |doc.delete()
         |eventuallyError(doc)
         |""".stripMargin,
      db
    )
    (err / "summary").as[String] shouldEqual expectedSummary
  }

  test("renders null hints") {
    val db = aDatabase.sample
    queryOk(
      """
        |Function.create({
        | name: "errFunc",
        | body: '(doc) => doc.a'
        |})
        |Function.create({
        | name: "eventuallyError",
        | body: '(doc) => errFunc(doc)'
        |})
        |""".stripMargin,
      db
    )

    val expectedSummary =
      """|error: Cannot access `a` on null.
         |at *udf:errFunc*:1:14
         |  |
         |1 | (doc) => doc.a
         |  |              ^
         |  |
         |hint: Null value created here
         |at *query*:2:9
         |  |
         |2 | let a = null
         |  |         ^^^^
         |  |
         |trace:
         |at *udf:eventuallyError*:1:17
         |  |
         |1 | (doc) => errFunc(doc)
         |  |                 ^^^^^
         |  |
         |at *query*:3:16
         |  |
         |3 | eventuallyError(a)
         |  |                ^^^
         |  |""".stripMargin
    val err = queryErr(
      s"""
         |let a = null
         |eventuallyError(a)
         |""".stripMargin,
      db
    )

    (err / "summary").as[String] shouldEqual expectedSummary
  }

  test("error occurs on Set.paginate() map call") {
    val db = aDatabase.sample
    queryOk(
      """
        |Collection.create({
        |  name: "TestColl"
        |})
        |""".stripMargin,
      db
    )
    val firstPage = queryOk(
      """
        |TestColl.create({
        |  name: "doc"
        |})
        |TestColl.create({
        |  name: "doc2"
        |})
        |TestColl.all().map(doc => {
        | if (doc.name == "doc2") {
        |   abort("error in paginate")
        | }
        | doc
        |}).paginate(1)
        |""".stripMargin,
      db
    )
    val after = (firstPage / "after").as[String]
    val err = queryErr(s"""Set.paginate("$after")""", db)
    val spanString = "^" * (after.length + 4)
    (err / "summary").as[String] shouldEqual
      s"""|error: Query aborted.
          |at *query*:1:13
          |  |
          |1 | Set.paginate("$after")
          |  |             $spanString
          |  |""".stripMargin
  }

  test("error occurs on Set.paginate() where call") {
    val db = aDatabase.sample
    queryOk(
      """
        |Collection.create({
        |  name: "TestColl"
        |})
        |""".stripMargin,
      db
    )
    val firstPage = queryOk(
      """
        |Set.sequence(1, 32).forEach((i) => TestColl.create({ name: "doc#{i}" }))
        |TestColl.where(doc => {
        | if (doc.name == "doc24") {
        |   abort("error in paginate")
        | }
        | true
        |}).paginate(16)
        |""".stripMargin,
      db
    )
    val after = (firstPage / "after").as[String]
    val err = queryErr(s"""Set.paginate("$after")""", db)
    val spanString = "^" * (after.length + 4)
    (err / "summary").as[String] shouldEqual
      s"""|error: Query aborted.
          |at *query*:1:13
          |  |
          |1 | Set.paginate("$after")
          |  |             $spanString
          |  |""".stripMargin
  }

  test("error occurs on Set.paginate() flatMap call") {
    val db = aDatabase.sample
    queryOk(
      """
        |Collection.create({
        |  name: "TestColl"
        |})
        |""".stripMargin,
      db
    )
    val firstPage = queryOk(
      """
        |TestColl.create({
        |  name: "doc"
        |})
        |TestColl.create({
        |  name: "doc2"
        |})
        |TestColl.all().flatMap(doc => {
        | if (doc.name == "doc2") {
        |   abort("error in paginate")
        | }
        | [doc].toSet()
        |}).paginate(1)
        |""".stripMargin,
      db
    )
    val after = (firstPage / "after").as[String]
    val err = queryErr(s"""Set.paginate("$after")""", db)
    val spanString = "^" * (after.length + 4)
    (err / "summary").as[String] shouldEqual
      s"""|error: Query aborted.
          |at *query*:1:13
          |  |
          |1 | Set.paginate("$after")
          |  |             $spanString
          |  |""".stripMargin
  }
  test("error in Set.map renders with query stack trace") {
    val db = aDatabase.sample
    queryOk(
      """
        |Function.create({
        | name: "myFunc",
        | body: '_ => [1].toSet().map(i => abort("ahhhhh"))'
        |})
        |""".stripMargin,
      db
    )

    val err = queryErr(
      """
        |myFunc(2)
        |""".stripMargin,
      db
    )
    (err / "error" / "code").as[String] shouldBe "abort"
    (err / "summary").as[String] shouldEqual
      """|error: Query aborted.
         |at *udf:myFunc*:1:32
         |  |
         |1 | _ => [1].toSet().map(i => abort("ahhhhh"))
         |  |                                ^^^^^^^^^^
         |  |
         |  |
         |1 | _ => [1].toSet().map(i => abort("ahhhhh"))
         |  |                     ^^^^^^^^^^^^^^^^^^^^^^
         |  |
         |at *query*:2:7
         |  |
         |2 | myFunc(2)
         |  |       ^^^
         |  |""".stripMargin
  }

  test("error in Set.flatMap renders with query stack trace") {
    val db = aDatabase.sample
    queryOk(
      """
        |Function.create({
        | name: "myFunc",
        | body: '_ => [1].toSet().flatMap(i => abort("ahhhhh"))'
        |})
        |""".stripMargin,
      db
    )

    val err = queryErr(
      """
        |myFunc(2)
        |""".stripMargin,
      db
    )
    (err / "error" / "code").as[String] shouldBe "abort"
    (err / "summary").as[String] shouldBe
      """|error: Query aborted.
         |at *udf:myFunc*:1:36
         |  |
         |1 | _ => [1].toSet().flatMap(i => abort("ahhhhh"))
         |  |                                    ^^^^^^^^^^
         |  |
         |  |
         |1 | _ => [1].toSet().flatMap(i => abort("ahhhhh"))
         |  |                         ^^^^^^^^^^^^^^^^^^^^^^
         |  |
         |at *query*:2:7
         |  |
         |2 | myFunc(2)
         |  |       ^^^
         |  |""".stripMargin
  }

  test("error in Set.where renders with query stack trace") {
    val db = aDatabase.sample
    queryOk(
      """
        |Function.create({
        | name: "myFunc",
        | body: '_ => [1].toSet().where(i => abort("ahhhhh"))'
        |})
        |""".stripMargin,
      db
    )

    val err = queryErr(
      """
        |myFunc(2)
        |""".stripMargin,
      db
    )
    (err / "error" / "code").as[String] shouldBe "abort"
    (err / "summary").as[String] shouldEqual
      """|error: Query aborted.
         |at *udf:myFunc*:1:34
         |  |
         |1 | _ => [1].toSet().where(i => abort("ahhhhh"))
         |  |                                  ^^^^^^^^^^
         |  |
         |  |
         |1 | _ => [1].toSet().where(i => abort("ahhhhh"))
         |  |                       ^^^^^^^^^^^^^^^^^^^^^^
         |  |
         |at *query*:2:7
         |  |
         |2 | myFunc(2)
         |  |       ^^^
         |  |""".stripMargin
  }

  test("error in ordering a set renders with query stack trace") {
    val db = aDatabase.sample
    queryOk(
      """
        |Function.create({
        | name: "myFunc",
        | body: '_ => [1].toSet().order(i => i.name.accessError)'
        |})
        |""".stripMargin,
      db
    )

    val err = queryErr(
      """
        |myFunc(2)
        |""".stripMargin,
      db
    )
    (err / "error" / "code").as[String] shouldBe "invalid_null_access"
    (err / "summary").as[String] shouldBe
      """|error: Cannot access `accessError` on null.
         |at *udf:myFunc*:1:36
         |  |
         |1 | _ => [1].toSet().order(i => i.name.accessError)
         |  |                                    ^^^^^^^^^^^
         |  |
         |hint: Null value created here
         |  |
         |1 | _ => [1].toSet().order(i => i.name.accessError)
         |  |                               ^^^^
         |  |
         |trace:
         |  |
         |1 | _ => [1].toSet().order(i => i.name.accessError)
         |  |                       ^^^^^^^^^^^^^^^^^^^^^^^^^
         |  |
         |at *query*:2:7
         |  |
         |2 | myFunc(2)
         |  |       ^^^
         |  |""".stripMargin
  }

  once("renders contention nicely") {
    for {
      db <- aDatabase
      _ = mkCollection(db, "User")
      doc <- aDocument(db, "User")
      query = s"User.byId($doc)!.update({ a: 'c' })"
      error =
        s"Transaction was aborted due to detection of concurrent modifications to User($doc)."
    } {
      @volatile var conflicted = false
      spawnPool { _ =>
        while (!conflicted) {
          val res = queryRaw(query, db)
          res.statusCode match {
            case OK => ()
            case Conflict =>
              conflicted = true
              (res.json / "error" / "message").as[String] shouldBe error

            case _ => fail(s"unexpected status code: ${res.statusCode}")
          }
        }
      }

      // Make sure we actually had a conflict
      if (!conflicted) {
        fail("expected at least one query to conflict")
      }
    }
  }

  once("renders aggregate contention nicely") {
    for {
      db <- aDatabase
    } {
      mkCollection(db, "User")

      val res = queryOk(
        """|{
           |  doc1: User.create({}).id,
           |  doc2: User.create({}).id,
           |}""".stripMargin,
        db
      )

      val doc1 = (res / "doc1").as[String]
      val doc2 = (res / "doc2").as[String]

      val queries = Seq(
        // This one should produce aggregate contention exceptions.
        s"""|User.byId($doc1)!.update({ a: 'c' })
            |User.byId($doc2)!.update({ a: 'c' })
            |""".stripMargin,
        s"User.byId($doc1)!.update({ a: 'c' })",
        s"User.byId($doc2)!.update({ a: 'c' })"
      )

      val contention1 =
        s"Transaction was aborted due to detection of concurrent modifications to User($doc1)."
      val contention2 =
        s"Transaction was aborted due to detection of concurrent modifications to User($doc2)."
      val aggregate =
        s"Transaction was aborted due to detection of concurrent modification."

      @volatile var conflicted = false
      spawnPool { i =>
        while (!conflicted) {
          val q = queries(i % 3)
          val res = queryRaw(q, db)
          res.statusCode match {
            case OK => ()
            case Conflict =>
              val msg = (res.json / "error" / "message").as[String]

              msg match {
                case `contention1` => ()
                case `contention2` => ()
                case `aggregate` =>
                  conflicted = true
                case _ => fail(s"unexpected error message: $msg")
              }

            case _ => fail(s"unexpected status code: ${res.statusCode}")
          }
        }
      }

      // Make sure we actually had a conflict
      if (!conflicted) {
        fail("expected at least one query to conflict")
      }
    }
  }

  def spawnPool(f: Int => Unit): Unit = {
    val threads = for (i <- 1 to Runtime.getRuntime().availableProcessors()) yield {
      new Thread {
        override def run(): Unit = f(i)
      }
    }

    val deadline = 1.minute.fromNow

    threads.foreach(_.start())
    threads.foreach(_.join(deadline.timeLeft.toMillis))
  }
}
