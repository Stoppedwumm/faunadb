package fauna.api.test

import fauna.codex.json._

class FQL2ArraySpec extends FQL2APISpec {
  "length" - {
    test("is the length of the array") {
      val db = aDatabase.sample
      queryOk(
        """|let arr = ['a','b','c','d']
           |arr.length""".stripMargin,
        db).as[Int] shouldEqual 4
    }
    test("is 0 for empty array") {
      val db = aDatabase.sample
      queryOk(
        """|let arr = []
           |arr.length""".stripMargin,
        db).as[Int] shouldEqual 0
    }
  }
  "[]" - {
    test("gets an element from the array") {
      val db = aDatabase.sample
      queryOk(
        """|let arr = ['a','b','c','d']
           |arr[1]""".stripMargin,
        db).as[String] shouldEqual "b"
    }
    test("if idx is too big, throws index_out_of_bounds (not typechecked)") {
      val db = aDatabase.sample
      val errRes = queryErr(
        """|let arr = ['a','b','c','d']
           |arr[4]""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false)))

      val expectedCode = "index_out_of_bounds"
      val expectedMessage = "index 4 out of bounds for length 4"
      val summaryPrefix = s"error: $expectedMessage"
      val annotation = s"""|at *query*:2:4
                           |  |
                           |2 | arr[4]
                           |  |    ^^^
                           |  |""".stripMargin

      val expectedSummary = Seq(summaryPrefix, annotation).mkString("\n")

      (errRes / "summary").as[String] shouldEqual expectedSummary
      (errRes / "error" / "code").as[String] shouldEqual expectedCode
      (errRes / "error" / "message").as[String] shouldEqual expectedMessage
    }
    test("if idx is too big, throws index_out_of_bounds (typechecked)") {
      pendingUntilFixed {
        val db = aDatabase.sample
        val errRes = queryErr(
          """|let arr = ['a','b','c','d']
           |arr[4]""".stripMargin,
          db)

        val expectedCode = "invalid_query"
        val expectedMessage = "The query failed 1 validation check"
        val expectedSummary =
          s"""|error: Index 4 out of bounds for length 4
            |at *query*:2:5
            |  |
            |2 | arr[4]
            |  |     ^
            |  |""".stripMargin

        (errRes / "summary").as[String] shouldEqual expectedSummary
        (errRes / "error" / "code").as[String] shouldEqual expectedCode
        (errRes / "error" / "message").as[String] shouldEqual expectedMessage
      }
    }

    test("negative indexes do not work (not typechecked)") {
      val db = aDatabase.sample
      val res = queryErr(
        """|let arr = ['a','b','c','d']
           |arr[-1]""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false)))

      val expectedCode = "index_out_of_bounds"
      val expectedMessage = "index -1 out of bounds for length 4"
      val summaryPrefix = s"error: $expectedMessage"
      val annotation = s"""|at *query*:2:4
                           |  |
                           |2 | arr[-1]
                           |  |    ^^^^
                           |  |""".stripMargin

      val expectedSummary = Seq(summaryPrefix, annotation).mkString("\n")

      (res / "summary").as[String] shouldEqual expectedSummary
      (res / "error" / "code").as[String] shouldEqual expectedCode
      (res / "error" / "message").as[String] shouldEqual expectedMessage
    }
    test("negative indexes do not work (typechecked)") {
      val db = aDatabase.sample
      val res = queryErr(
        """|let arr = ['a','b','c','d']
           |arr[-1]""".stripMargin,
        db)

      val expectedCode = "invalid_query"
      val expectedMessage = "The query failed 1 validation check"
      val expectedSummary =
        s"""|error: Index -1 out of bounds for length 4
            |at *query*:2:5
            |  |
            |2 | arr[-1]
            |  |     ^^
            |  |""".stripMargin

      (res / "summary").as[String] shouldEqual expectedSummary
      (res / "error" / "code").as[String] shouldEqual expectedCode
      (res / "error" / "message").as[String] shouldEqual expectedMessage
    }
  }
  "at" - {
    test("gets an element from the array") {
      val db = aDatabase.sample
      queryOk(
        """|let arr = ['a','b','c','d']
           |arr.at(2)""".stripMargin,
        db).as[String] shouldEqual "c"
    }
    test("if idx is too big, throws index_out_of_bounds") {
      val db = aDatabase.sample
      val errRes = queryErr(
        """|let arr = ['a','b','c','d']
           |arr.at(4000)""".stripMargin,
        db)

      val expectedCode = "index_out_of_bounds"
      val expectedMessage = "index 4000 out of bounds for length 4"
      val summaryPrefix = s"error: $expectedMessage"
      val annotation = s"""|at *query*:2:7
                           |  |
                           |2 | arr.at(4000)
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
        """|let arr = ['a','b','c','d']
           |arr.at(-123456)""".stripMargin,
        db)

      val expectedCode = "index_out_of_bounds"
      val expectedMessage = "index -123456 out of bounds for length 4"
      val summaryPrefix = s"error: $expectedMessage"
      val annotation = s"""|at *query*:2:7
                           |  |
                           |2 | arr.at(-123456)
                           |  |       ^^^^^^^^^
                           |  |""".stripMargin

      val expectedSummary = Seq(summaryPrefix, annotation).mkString("\n")

      (errRes / "summary").as[String] shouldEqual expectedSummary
      (errRes / "error" / "code").as[String] shouldEqual expectedCode
      (errRes / "error" / "message").as[String] shouldEqual expectedMessage
    }
  }
  "concat" - {
    test("joins two arrays") {
      val db = aDatabase.sample
      queryOk(
        """|let arr1 = ['a','b','c','d']
           |let arr2 = ['e','f','g']
           |arr1.concat(arr2)""".stripMargin,
        db).as[Seq[String]] shouldEqual Seq("a", "b", "c", "d", "e", "f", "g")
    }
  }
  "filter" - {
    test("selects only the elements that satisfy the predicate lambda") {
      val db = aDatabase.sample
      queryOk(
        """|let arr = ['Luke', 'Han', 'Leia', 'Lando', 'R2-D2', 'C3-P0', 'Ackbar']
           |arr.filter((name) => name.startsWith('L'))""".stripMargin,
        db).as[Seq[String]] shouldEqual Seq("Luke", "Leia", "Lando")
    }
    test("selects only the elements that satisfy the predicate native function") {
      val db = aDatabase.sample
      queryOk(
        """|let arr = ['Luke', 'Han', 'Leia', 'Lando', 'R2-D2', 'C3-P0', 'Ackbar']
           |let sentence = 'R2-D2 and C3-P0 are Luke\'s droids'
           |arr.filter(sentence.includes)""".stripMargin,
        db
      ).as[Seq[String]] shouldEqual Seq("Luke", "R2-D2", "C3-P0")
    }
    test("throws an error if the lambda has the wrong arity (typechecking)") {
      val db = aDatabase.sample
      val errRes = queryErr(
        """|let arr = ['Luke', 'Han', 'Leia', 'Lando', 'R2-D2', 'C3-P0', 'Ackbar']
           |arr.filter(() => true)""".stripMargin,
        db)

      val expectedCode = "invalid_query"
      val expectedMessage = "The query failed 1 validation check"

      // FIXME: Boolean <: Null is not relevant here.
      val expectedSummary =
        s"""|error: Function was called with too many arguments. Expected 0, received 1
            |at *query*:2:12
            |  |
            |2 | arr.filter(() => true)
            |  |            ^^^^^^^^^^
            |  |""".stripMargin

      (errRes / "summary").as[String] shouldEqual expectedSummary
      (errRes / "error" / "code").as[String] shouldEqual expectedCode
      (errRes / "error" / "message").as[String] shouldEqual expectedMessage
    }
    test("throws an error if the lambda has the wrong arity") {
      val db = aDatabase.sample
      val errRes = queryErr(
        """|let arr = ['Luke', 'Han', 'Leia', 'Lando', 'R2-D2', 'C3-P0', 'Ackbar']
           |arr.filter(() => true)""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      )

      val expectedCode = "invalid_argument"
      val expectedMessage =
        "`predicate` should accept 1 parameter, provided function requires no parameters."
      val summaryPrefix = s"error: $expectedMessage"
      val annotation = s"""|at *query*:2:11
                           |  |
                           |2 | arr.filter(() => true)
                           |  |           ^^^^^^^^^^^^
                           |  |""".stripMargin

      val expectedSummary = Seq(summaryPrefix, annotation).mkString("\n")

      (errRes / "summary").as[String] shouldEqual expectedSummary
      (errRes / "error" / "code").as[String] shouldEqual expectedCode
      (errRes / "error" / "message").as[String] shouldEqual expectedMessage
    }
    test("throws a type error if the lambda returns a non-boolean value") {
      val db = aDatabase.sample
      val errRes = queryErr(
        """|let arr = ['Luke', 'Han', 'Leia', 'Lando', 'R2-D2', 'C3-P0', 'Ackbar']
           |arr.filter((name) => if(name == 'Han') 'Han' else true)""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      )

      val expectedCode = "type_mismatch"
      val expectedMessage = "expected type: Boolean, received String"
      val summaryPrefix = s"error: $expectedMessage"
      val annotation = s"""|at *query*:2:11
                           |  |
                           |2 | arr.filter((name) => if(name == 'Han') 'Han' else true)
                           |  |           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                           |  |""".stripMargin

      val expectedSummary = Seq(summaryPrefix, annotation).mkString("\n")

      (errRes / "summary").as[String] shouldEqual expectedSummary
      (errRes / "error" / "code").as[String] shouldEqual expectedCode
      (errRes / "error" / "message").as[String] shouldEqual expectedMessage
    }
  }
  "flatten" - {
    test("flattens an array of arrays") {
      val db = aDatabase.sample
      queryOk(
        """|let arr = [[1],[2,3,4],[5],[],[6,7]]
           |arr.flatten()""".stripMargin,
        db).as[Seq[Int]] shouldEqual Seq(1, 2, 3, 4, 5, 6, 7)
    }
    test("throws a type error if any element is not an array") {
      val db = aDatabase.sample
      val errRes = queryErr(
        """|let arr = [[1],[2,3,4],[5],"howdy",[6,7]]
           |arr.flatten()""".stripMargin,
        db)

      val expectedCode = "type_mismatch"
      val expectedMessage = "expected type: Array<Any>, received String"
      val summaryPrefix = s"error: $expectedMessage"
      val annotation = s"""|at *query*:2:12
                           |  |
                           |2 | arr.flatten()
                           |  |            ^^
                           |  |""".stripMargin

      val expectedSummary = Seq(summaryPrefix, annotation).mkString("\n")

      (errRes / "summary").as[String] shouldEqual expectedSummary
      (errRes / "error" / "code").as[String] shouldEqual expectedCode
      (errRes / "error" / "message").as[String] shouldEqual expectedMessage
    }
  }
  "flatMap" - {
    test("applies a lambda to each element of the array and flattens the result") {
      val db = aDatabase.sample
      queryOk(
        """|let arr1 = [1, 2, 3, 4]
           |let arr2 = ['a','b','c','d','e','f','g','h','i']
           |arr1.flatMap(x => arr2.slice(0, x))""".stripMargin,
        db
      ).as[Seq[String]] shouldEqual Seq(
        "a",
        "a",
        "b",
        "a",
        "b",
        "c",
        "a",
        "b",
        "c",
        "d")
    }
    test("throws a type error if the lambda returns a value that is not an array") {
      val db = aDatabase.sample
      val errRes = queryErr(
        """|let arr = [0, 1, 2, 0]
           |arr.flatMap(x => 'foobar')""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false)))

      val expectedCode = "type_mismatch"
      val expectedMessage = "expected type: Array<Any>, received String"
      val summaryPrefix = s"error: $expectedMessage"
      val annotation = s"""|at *query*:2:12
                           |  |
                           |2 | arr.flatMap(x => 'foobar')
                           |  |            ^^^^^^^^^^^^^^^
                           |  |""".stripMargin

      val expectedSummary = Seq(summaryPrefix, annotation).mkString("\n")

      (errRes / "summary").as[String] shouldEqual expectedSummary
      (errRes / "error" / "code").as[String] shouldEqual expectedCode
      (errRes / "error" / "message").as[String] shouldEqual expectedMessage
    }
    test("rejects a function with the wrong arity") {
      val db = aDatabase.sample
      val errRes = queryErr(
        """|let arr1 = [0, 1, 2, 0]
           |let arr2 = [['a','b'],['c','d','e'],['f','g']]
           |arr1.flatMap((x, y) => x == y)""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      )

      val expectedCode = "invalid_argument"
      val expectedMessage =
        "`mapper` should accept 1 parameter, provided function requires exactly 2 parameters."
      val summaryPrefix = s"error: $expectedMessage"
      val annotation = s"""|at *query*:3:13
                           |  |
                           |3 | arr1.flatMap((x, y) => x == y)
                           |  |             ^^^^^^^^^^^^^^^^^^
                           |  |""".stripMargin

      val expectedSummary = Seq(summaryPrefix, annotation).mkString("\n")

      (errRes / "summary").as[String] shouldEqual expectedSummary
      (errRes / "error" / "code").as[String] shouldEqual expectedCode
      (errRes / "error" / "message").as[String] shouldEqual expectedMessage
    }
  }
  "forEach" - {
    test("returns null") {
      val db = aDatabase.sample
      queryOk(
        """|let arr = ['a','ab','abc','abcd']
           |arr.forEach(str => str.length)""".stripMargin,
        db) shouldEqual JSNull
    }
    test("applies a lambda to each element of the array") {
      // TODO: Can't test this until we have a function that causes side-effects.
      pending
    }
    test("applies a native function to each element of the array") {
      // TODO: Can't test this until we have a function that causes side-effects.
      pending
    }
    test("rejects a function with the wrong arity") {
      val db = aDatabase.sample
      val errRes = queryErr(
        """|let arr = ['a','ab','abc','abcd']
           |arr.forEach((a, b, c) => b.length)""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      )

      val expectedCode = "invalid_argument"
      val expectedMessage =
        "`fn` should accept 1 parameter, provided function requires exactly 3 parameters."
      val summaryPrefix = s"error: $expectedMessage"
      val annotation = s"""|at *query*:2:12
                           |  |
                           |2 | arr.forEach((a, b, c) => b.length)
                           |  |            ^^^^^^^^^^^^^^^^^^^^^^^
                           |  |""".stripMargin

      val expectedSummary = Seq(summaryPrefix, annotation).mkString("\n")

      (errRes / "summary").as[String] shouldEqual expectedSummary
      (errRes / "error" / "code").as[String] shouldEqual expectedCode
      (errRes / "error" / "message").as[String] shouldEqual expectedMessage
    }
  }
  "map" - {
    test("applies a lambda to each element of the array") {
      val db = aDatabase.sample
      queryOk(
        """|let arr = ['a','ab','abc','abcd']
           |arr.map(str => str.length)""".stripMargin,
        db).as[Seq[Int]] shouldEqual Seq(1, 2, 3, 4)
    }
    test("applies a native function to each element of the array") {
      val db = aDatabase.sample
      queryOk(
        """|let arr = ['a','ab','abc','abcd']
           |arr.map('ab'.includes)""".stripMargin,
        db).as[Seq[Boolean]] shouldEqual Seq(true, true, false, false)
    }
    test("rejects a function with the wrong arity") {
      val db = aDatabase.sample
      val errRes = queryErr(
        """|let arr = ['a','ab','abc','abcd']
           |arr.map((a, b) => a.concat(b))""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      )

      val expectedCode = "invalid_argument"
      val expectedMessage =
        "`mapper` should accept 1 parameter, provided function requires exactly 2 parameters."
      val summaryPrefix = s"error: $expectedMessage"
      val annotation = s"""|at *query*:2:8
                           |  |
                           |2 | arr.map((a, b) => a.concat(b))
                           |  |        ^^^^^^^^^^^^^^^^^^^^^^^
                           |  |""".stripMargin

      val expectedSummary = Seq(summaryPrefix, annotation).mkString("\n")

      (errRes / "summary").as[String] shouldEqual expectedSummary
      (errRes / "error" / "code").as[String] shouldEqual expectedCode
      (errRes / "error" / "message").as[String] shouldEqual expectedMessage
    }
  }
  "fold" - {
    test("iteratively applies the reducer function from left to right") {
      val db = aDatabase.sample
      queryOk(
        """|let arr = ['a','bc','def','ghij']
           |arr.fold('SEED', (acc, str) => acc.concat(str))""".stripMargin,
        db).as[String] shouldEqual "SEEDabcdefghij"
    }
    test("on an empty array, returns the seed") {
      val db = aDatabase.sample
      queryOk(
        """|let arr = []
           |arr.fold('SEED', (acc, str) => acc.concat(str))""".stripMargin,
        db).as[String] shouldEqual "SEED"
    }
    test("rejects a reducer function with the wrong arity") {
      val db = aDatabase.sample
      val errRes = queryErr(
        """|let arr = ['a','bc','def','ghij']
           |arr.fold('SEED', (a) => a.concat(a))""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      )

      val expectedCode = "invalid_argument"
      val expectedMessage =
        "`reducer` should accept 2 parameters, provided function requires exactly 1 parameter."
      val summaryPrefix = s"error: $expectedMessage"
      val annotation = s"""|at *query*:2:9
                           |  |
                           |2 | arr.fold('SEED', (a) => a.concat(a))
                           |  |         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                           |  |""".stripMargin

      val expectedSummary = Seq(summaryPrefix, annotation).mkString("\n")

      (errRes / "summary").as[String] shouldEqual expectedSummary
      (errRes / "error" / "code").as[String] shouldEqual expectedCode
      (errRes / "error" / "message").as[String] shouldEqual expectedMessage
    }
  }
  "foldRight" - {
    test("iteratively applies the reducer function from right to left") {
      val db = aDatabase.sample
      queryOk(
        """|let arr = ['a','bc','def','ghij']
           |arr.foldRight('SEED', (acc, str) => acc.concat(str))""".stripMargin,
        db).as[String] shouldEqual "SEEDghijdefbca"
    }
    test("on an empty array, returns the seed") {
      val db = aDatabase.sample
      queryOk(
        """|let arr = []
           |arr.foldRight('SEED', (acc, str) => acc.concat(str))""".stripMargin,
        db).as[String] shouldEqual "SEED"
    }
    test("rejects a reducer function with the wrong arity") {
      val db = aDatabase.sample
      val errRes = queryErr(
        """|let arr = ['a','bc','def','ghij']
           |arr.foldRight('SEED', (a) => a.concat(a))""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false)))

      val expectedCode = "invalid_argument"
      val expectedMessage =
        "`reducer` should accept 2 parameters, provided function requires exactly 1 parameter."
      val summaryPrefix = s"error: $expectedMessage"
      val annotation = s"""|at *query*:2:14
                           |  |
                           |2 | arr.foldRight('SEED', (a) => a.concat(a))
                           |  |              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                           |  |""".stripMargin

      val expectedSummary = Seq(summaryPrefix, annotation).mkString("\n")

      (errRes / "summary").as[String] shouldEqual expectedSummary
      (errRes / "error" / "code").as[String] shouldEqual expectedCode
      (errRes / "error" / "message").as[String] shouldEqual expectedMessage
    }
  }
  "slice" - {
    test("extracts a subarray") {
      val db = aDatabase.sample
      queryOk("""['a','b','c','d','e','f'].slice(2, 4)""", db)
        .as[Seq[String]] shouldEqual Seq("c", "d")
    }
    test("if until > arr.length, takes the rest of the array") {
      val db = aDatabase.sample
      queryOk("""['a','b','c','d','e','f'].slice(5, 5000)""", db)
        .as[Seq[String]] shouldEqual Seq("f")
    }
    test("if start > arr.length, returns empty array") {
      val db = aDatabase.sample
      queryOk("""['a','b','c','d','e','f'].slice(8, 10)""", db)
        .as[Seq[String]] shouldEqual Seq.empty
    }
    test("if until < start, returns empty array") {
      val db = aDatabase.sample
      queryOk("""['a','b','c','d','e','f'].slice(4, 2)""", db)
        .as[Seq[String]] shouldEqual Seq.empty
    }
    test("if start < 0, acts like passing in 0") {
      val db = aDatabase.sample
      queryOk("""['a','b','c','d','e','f'].slice(-5, 4)""", db)
        .as[Seq[String]] shouldEqual Seq("a", "b", "c", "d")
    }
    test("if end < 0, acts like passing in 0") {
      val db = aDatabase.sample
      queryOk("""['a','b','c','d','e','f'].slice(4, -1)""", db)
        .as[Seq[String]] shouldEqual Seq.empty
    }
    test("if both start and end < 0, both act like zero, and returns nothing") {
      val db = aDatabase.sample
      queryOk("""['a','b','c','d','e','f'].slice(-5, -1)""", db)
        .as[Seq[String]] shouldEqual Seq.empty
    }
  }
  "nulls" - {
    test("don't elide nulls after persist") {
      val db = aDatabase.sample
      queryOk(
        """|Collection.create({
           |  name: "Nulls",
           |  indexes: {
           |    byFoo: {
           |      terms: [{field: "foo", mva: true}]
           |    }
           |  }
           |})""".stripMargin,
        db
      )

      val expected = JSArray(JSLong(1), JSNull, JSLong(2))

      queryOk(s"""Nulls.create({foo: $expected}).foo""", db)
        .as[JSArray] shouldEqual expected

      queryOk("""Nulls.all().first()!.foo""", db)
        .as[JSArray] shouldEqual expected

      (queryOk("""Nulls.byFoo(1)""", db) / "data" / 0 / "foo")
        .as[JSArray] shouldEqual expected

      (queryOk("""Nulls.byFoo(2)""", db) / "data" / 0 / "foo")
        .as[JSArray] shouldEqual expected

      (queryOk("""Nulls.byFoo(null)""", db) / "data")
        .as[JSArray] shouldEqual JSArray.empty
    }
  }
}
