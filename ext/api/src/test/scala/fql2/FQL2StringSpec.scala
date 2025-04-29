package fauna.api.test

class FQL2StringSpec extends FQL2APISpec {
  "[]" - {
    test("gets a character from the string") {
      val db = aDatabase.sample
      queryOk(
        """|let str = 'abcd'
           |str[1]""".stripMargin,
        db).as[String] shouldEqual "b"
    }
    test("if idx is too big, throws index_out_of_bounds") {
      val db = aDatabase.sample
      val errRes = queryErr(
        """|let str = 'abcd'
           |str[4]""".stripMargin,
        db)

      val expectedCode = "index_out_of_bounds"
      val expectedMessage = "index 4 out of bounds for length 4"
      val summaryPrefix = s"error: $expectedMessage"
      val annotation = s"""|at *query*:2:4
                           |  |
                           |2 | str[4]
                           |  |    ^^^
                           |  |""".stripMargin

      val expectedSummary = Seq(summaryPrefix, annotation).mkString("\n")

      (errRes / "summary").as[String] shouldEqual expectedSummary
      (errRes / "error" / "code").as[String] shouldEqual expectedCode
      (errRes / "error" / "message").as[String] shouldEqual expectedMessage
    }
    test("if idx is negative, throws index_out_of_bounds") {
      val db = aDatabase.sample
      val errRes = queryErr(
        """|let str = 'abcd'
           |str[-1]""".stripMargin,
        db)

      val expectedCode = "index_out_of_bounds"
      val expectedMessage = "index -1 out of bounds for length 4"
      val summaryPrefix = s"error: $expectedMessage"
      val annotation = s"""|at *query*:2:4
                           |  |
                           |2 | str[-1]
                           |  |    ^^^^
                           |  |""".stripMargin

      val expectedSummary = Seq(summaryPrefix, annotation).mkString("\n")

      (errRes / "summary").as[String] shouldEqual expectedSummary
      (errRes / "error" / "code").as[String] shouldEqual expectedCode
      (errRes / "error" / "message").as[String] shouldEqual expectedMessage
    }
  }
  "at" - {
    test("gets a character from the string") {
      val db = aDatabase.sample
      queryOk(
        """|let str = 'abcd'
           |str.at(2)""".stripMargin,
        db).as[String] shouldEqual "c"
    }
    test("if idx is too big, throws index_out_of_bounds") {
      val db = aDatabase.sample
      val errRes = queryErr(
        """|let str = 'abcd'
           |str.at(4000)""".stripMargin,
        db)

      val expectedCode = "index_out_of_bounds"
      val expectedMessage = "index 4000 out of bounds for length 4"
      val summaryPrefix = s"error: $expectedMessage"
      val annotation = s"""|at *query*:2:7
                           |  |
                           |2 | str.at(4000)
                           |  |       ^^^^^^
                           |  |""".stripMargin

      val expectedSummary = Seq(summaryPrefix, annotation).mkString("\n")

      (errRes / "summary").as[String] shouldEqual expectedSummary
      (errRes / "error" / "code").as[String] shouldEqual expectedCode
      (errRes / "error" / "message").as[String] shouldEqual expectedMessage
    }
    test("if idx is negative, throws index_out_of_bounds") {
      val db = aDatabase.sample
      val errRes = queryErr(
        """|let str = 'abcd'
           |str.at(-123456)""".stripMargin,
        db)

      val expectedCode = "index_out_of_bounds"
      val expectedMessage = "index -123456 out of bounds for length 4"
      val summaryPrefix = s"error: $expectedMessage"
      val annotation = s"""|at *query*:2:7
                           |  |
                           |2 | str.at(-123456)
                           |  |       ^^^^^^^^^
                           |  |""".stripMargin

      val expectedSummary = Seq(summaryPrefix, annotation).mkString("\n")

      (errRes / "summary").as[String] shouldEqual expectedSummary
      (errRes / "error" / "code").as[String] shouldEqual expectedCode
      (errRes / "error" / "message").as[String] shouldEqual expectedMessage
    }
  }
  "concat" - {
    test("concatenates two strings") {
      val db = aDatabase.sample
      queryOk("""'foo'.concat('bar')""", db).as[String] shouldEqual "foobar"
    }
  }
  "concatenation operator (+)" - {
    test("concatenates two strings") {
      val db = aDatabase.sample
      queryOk("""'foo' + 'bar'""", db).as[String] shouldEqual "foobar"
    }
    test("is associative") {
      val db = aDatabase.sample
      queryOk("""'foo' + 'bar' + 'baz'""", db).as[String] shouldEqual "foobarbaz"
    }
    test("concatenates null values") {
      val db = aDatabase.sample
      queryOk(
        """'foo ' + null + ' bar ' + null""",
        db,
        FQL2Params(typecheck = Some(false)))
        .as[String] shouldEqual "foo null bar null"
      queryOk("""'foo ' + null.toString() + ' bar ' + null.toString()""", db)
        .as[String] shouldEqual "foo null bar null"
    }
    test("concatenates numbers") {
      val db = aDatabase.sample
      queryOk(
        """'foo ' + 10 + ' bar ' + 3.14159""",
        db,
        FQL2Params(typecheck = Some(false)))
        .as[String] shouldEqual "foo 10 bar 3.14159"
      queryOk("""'foo ' + 10.toString() + ' bar ' + 3.14159.toString()""", db)
        .as[String] shouldEqual "foo 10 bar 3.14159"
    }
    test("concatenates booleans") {
      val db = aDatabase.sample
      queryOk(
        """'foo ' + true + ' bar ' + false""",
        db,
        FQL2Params(typecheck = Some(false)))
        .as[String] shouldEqual "foo true bar false"
      queryOk("""'foo ' + true.toString() + ' bar ' + false.toString()""", db)
        .as[String] shouldEqual "foo true bar false"
    }
    test("concatenates arrays & structs") {
      val db = aDatabase.sample
      queryOk(
        """'foo ' + [1, 2, 3] + ' bar ' + {x: 10}""",
        db,
        FQL2Params(typecheck = Some(false)))
        .as[String] shouldEqual "foo [1, 2, 3] bar { x: 10 }"
      queryOk(
        """'foo ' + [1, 2, 3].toString() + ' bar ' + Object.toString({x: 10})""",
        db)
        .as[String] shouldEqual "foo [1, 2, 3] bar { x: 10 }"
    }
  }
  "endsWith" - {
    test("finds a substring at the end of this string") {
      val db = aDatabase.sample
      queryOk("""'foobar'.endsWith('bar')""", db).as[Boolean] shouldEqual true
    }
    test("does not find a string that is not at the end of this string") {
      val db = aDatabase.sample
      queryOk("""'foobar'.endsWith('foo')""", db).as[Boolean] shouldEqual false
    }
  }
  "includes" - {
    test("finds a substring contained in this string") {
      val db = aDatabase.sample
      queryOk("""'foobar'.includes('oba')""", db).as[Boolean] shouldEqual true
    }
    test("does not find a string that is not contained in this string") {
      val db = aDatabase.sample
      queryOk("""'foobar'.includes('baz')""", db).as[Boolean] shouldEqual false
    }
  }
  "length" - {
    test("is the number of chars in the string") {
      val db = aDatabase.sample
      queryOk("""'foobar'.length""", db).as[Int] shouldEqual 6
      queryOk("""'foobarbaz'.length""", db).as[Int] shouldEqual 9
      queryOk("""''.length""", db).as[Int] shouldEqual 0
    }
  }
  "startsWith" - {
    test("finds a substring at the start of this string") {
      val db = aDatabase.sample
      queryOk("""'foobar'.startsWith('foo')""", db).as[Boolean] shouldEqual true
    }
    test("does not find a string that is not at the start of this string") {
      val db = aDatabase.sample
      queryOk("""'foobar'.startsWith('bar')""", db).as[Boolean] shouldEqual false
    }
  }
  "equals operator" - {
    test("==") {
      val db = aDatabase.sample
      queryOk("""'hello' == 'hello'""", db).as[Boolean] shouldEqual true
      queryOk("""'hello' == 'chello'""", db).as[Boolean] shouldEqual false
      queryOk("""'hello' == 1""", db).as[Boolean] shouldEqual false
      queryOk("""'hello' == true""", db).as[Boolean] shouldEqual false
      queryOk("""'hello' == 1.0""", db).as[Boolean] shouldEqual false
    }
  }
  "not equals operator" - {
    test("!=") {
      val db = aDatabase.sample
      queryOk("""'hello' != 'hello'""", db).as[Boolean] shouldEqual false
      queryOk("""'hello' != 'chello'""", db).as[Boolean] shouldEqual true
      queryOk("""'hello' != 1""", db).as[Boolean] shouldEqual true
      queryOk("""'hello' != true""", db).as[Boolean] shouldEqual true
      queryOk("""'hello' != 1.0""", db).as[Boolean] shouldEqual true
    }
  }
}
