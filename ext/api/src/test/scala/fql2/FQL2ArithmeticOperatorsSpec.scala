package fauna.api.test

class FQL2ArithmeticOperatorsSpec extends FQL2APISpec {
  "addition operator (+)" - {
    test("adds Int + Int to get an Int") {
      val db = aDatabase.sample
      queryOk("""2 + 2""", db).as[Int] shouldEqual 4
      queryOk("""987 + -382""", db).as[Int] shouldEqual 605
    }
    test("adding Int + Int can overflow to a Long") {
      val db = aDatabase.sample
      queryOk("""1000000000 + 2000000000""", db).as[Long] shouldEqual 3000000000L
    }
    test("adds Int + Long to get a Long") {
      val db = aDatabase.sample
      queryOk("""5 + 3000000000""", db).as[Long] shouldEqual 3000000005L
    }
    test("adds Long + Int to get a Long") {
      val db = aDatabase.sample
      queryOk("""3000000000 + 5""", db).as[Long] shouldEqual 3000000005L
    }
    test("adds Long + Long to get a Long") {
      val db = aDatabase.sample
      queryOk("""8000000000 + 1000000000""", db).as[Long] shouldEqual 9000000000L
    }
    test("adding Long + Long can overflow to Double") {
      val db = aDatabase.sample
      queryOk("""9000000000000000000 + 9000000000000000000""", db)
        .as[Double] shouldEqual 18.0e18
    }
    test("adds Int + Double to get a Double") {
      val db = aDatabase.sample
      queryOk("""5 + 9.8""", db).as[Double] shouldEqual 14.8
    }
    test("adds Double + Int to get a Double") {
      val db = aDatabase.sample
      queryOk("""5.8 + 9""", db).as[Double] shouldEqual 14.8
    }
    test("adds Long + Double to get a Double") {
      val db = aDatabase.sample
      queryOk("""8000000000 + 12.345""", db).as[Double] shouldEqual 8000000012.345
    }
    test("adds Double + Long to get a Double") {
      val db = aDatabase.sample
      queryOk("""54.321 + 8000000000""", db).as[Double] shouldEqual 8000000054.321
    }
    test("adds Double + Double to get a Double") {
      val db = aDatabase.sample
      queryOk("""3.14159265358979323846 + 1.618033988749894""", db)
        .as[Double] shouldEqual 4.759626642339687
    }
    test("if Double + Double overflows the result is Infinity") {
      val db = aDatabase.sample
      queryOk("""1.7e308 + 1.7e308""", db).as[String] shouldEqual "Infinity"
    }
    test("is associative") {
      val db = aDatabase.sample
      queryOk("""2 + 9 + 83""", db).as[Int] shouldEqual 94
    }
  }
  "subtraction operator (-)" - {
    test("subtracts Int - Int to get an Int") {
      val db = aDatabase.sample
      queryOk("""7 - 18""", db).as[Int] shouldEqual -11
      queryOk("""389-203""", db).as[Int] shouldEqual 186
    }
    test("subtracting Int - Int can underflow to a Long") {
      val db = aDatabase.sample
      queryOk("""-1000000000 - 2000000000""", db).as[Long] shouldEqual -3000000000L
    }
    test("subtracts Int - Long to get a Long") {
      val db = aDatabase.sample
      queryOk("""5 - 3000000000""", db).as[Long] shouldEqual -2999999995L
    }
    test("subtracts Long + Int to get a Long") {
      val db = aDatabase.sample
      queryOk("""3000000000 - 5""", db).as[Long] shouldEqual 2999999995L
    }
    test("subtracts Long - Long to get a Long") {
      val db = aDatabase.sample
      queryOk("""8000000000 - 1000000000""", db).as[Long] shouldEqual 7000000000L
    }
    test("subtracting Long - Long can underflow to Double") {
      val db = aDatabase.sample
      queryOk("""-9000000000000000000 - 9000000000000000000""", db)
        .as[Double] shouldEqual -18.0e18
    }
    test("subtracts Int - Double to get a Double") {
      val db = aDatabase.sample
      queryOk("""5 - 9.8""", db).as[Double] shouldEqual -4.800000000000001
    }
    test("subtracts Double - Int to get a Double") {
      val db = aDatabase.sample
      queryOk("""5.8 - 9""", db).as[Double] shouldEqual -3.2
    }
    test("subtracts Long - Double to get a Double") {
      val db = aDatabase.sample
      queryOk("""8000000000 - 12.345""", db).as[Double] shouldEqual 7999999987.655
    }
    test("subtracts Double + Long to get a Double") {
      val db = aDatabase.sample
      queryOk("""54.321 - 8000000000""", db).as[Double] shouldEqual -7999999945.679
    }
    test("subtracts Double - Double to get a Double") {
      val db = aDatabase.sample
      queryOk("""3.14159265358979323846 - 2.71828182845904523536""", db)
        .as[Double] shouldEqual 0.423310825130748
    }
    test("if Double - Double underflows the result is -Infinity") {
      val db = aDatabase.sample
      queryOk("""-1.7e308 - 1.7e308""", db).as[String] shouldEqual "-Infinity"
    }
    test("associates left-to-right") {
      val db = aDatabase.sample
      queryOk("""2 - 9 - 83""", db).as[Int] shouldEqual -90
    }
  }
  "multiplication operator (*)" - {
    test("multiplies Int * Int to get an Int") {
      val db = aDatabase.sample
      queryOk("""2 * 7""", db).as[Int] shouldEqual 14
      queryOk("""987 * -382""", db).as[Int] shouldEqual -377034
    }
    test("multiplying Int * Int can overflow to a Long") {
      val db = aDatabase.sample
      queryOk("""2000000000 * 2""", db).as[Long] shouldEqual 4000000000L
    }
    test("multiplies Int * Long to get a Long") {
      val db = aDatabase.sample
      queryOk("""-5 * 3000000000""", db).as[Long] shouldEqual -15000000000L
    }
    test("multiplies Long * Int to get a Long") {
      val db = aDatabase.sample
      queryOk("""3000000000 * 5""", db).as[Long] shouldEqual 15000000000L
    }
    test("multiplying Int * Long can overflow to Double") {
      val db = aDatabase.sample
      queryOk("""2 * 9000000000000000000""", db)
        .as[Double] shouldEqual 18.0e18
    }
    test("multiplying Long * Int can overflow to Double") {
      val db = aDatabase.sample
      queryOk("""9000000000000000000 * 2""", db)
        .as[Double] shouldEqual 18.0e18
    }
    test("multiplies Long * Long to get a Long") {
      val db = aDatabase.sample
      queryOk("""2147483648 * 2147483648""", db)
        .as[Long] shouldEqual 4611686018427387904L
    }
    test("multiplying Long * Long can overflow to Double") {
      val db = aDatabase.sample
      queryOk("""9000000000000000000 * 9000000000000000000""", db)
        .as[Double] shouldEqual 81.0e36
    }
    test("multiplies Int * Double to get a Double") {
      val db = aDatabase.sample
      queryOk("""3 * 9.8""", db).as[Double] shouldEqual 29.400000000000002
    }
    test("multiplies Double * Int to get a Double") {
      val db = aDatabase.sample
      queryOk("""5.8 * 9""", db).as[Double] shouldEqual 52.199999999999996
    }
    test("multiplies Long * Double to get a Double") {
      val db = aDatabase.sample
      queryOk("""8000000000 * 1.2""", db).as[Double] shouldEqual 9.6e9
    }
    test("multiplies Double * Long to get a Double") {
      val db = aDatabase.sample
      queryOk("""54.321 * 8000000000""", db).as[Double] shouldEqual 4.34568e11
    }
    test("multiplies Double * Double to get a Double") {
      val db = aDatabase.sample
      queryOk("""3.14159265358979323846 * 1.618033988749894""", db)
        .as[Double] shouldEqual 5.083203692315257
    }
    test("if Double * Double overflows the result is Infinity") {
      val db = aDatabase.sample
      queryOk("""1.7e308 * 1.7e308""", db).as[String] shouldEqual "Infinity"
    }
    test("if Double * Double underflows the result is -Infinity") {
      val db = aDatabase.sample
      queryOk("""1.7e308 * -1.7e308""", db).as[String] shouldEqual "-Infinity"
    }
    test("is associative") {
      val db = aDatabase.sample
      queryOk("""2 * 9 * 83""", db).as[Int] shouldEqual 1494
    }
  }
  "division operator (/)" - {
    test("divides Int / Int to get a Int") {
      val db = aDatabase.sample
      queryOk("""18 / 2""", db).as[Int] shouldEqual 9
      queryOk("""987/-382""", db).as[Int] shouldEqual -2
    }
    test("dividing Int / 0 results in a divide_by_zero error") {
      val db = aDatabase.sample
      val errRes = queryErr("""234 / 0""", db)

      val expectedCode = "divide_by_zero"
      val expectedMessage = "Attempted integer division by zero."
      val summaryPrefix = s"error: $expectedMessage"
      val annotation = s"""|at *query*:1:7
                           |  |
                           |1 | 234 / 0
                           |  |       ^
                           |  |""".stripMargin

      val expectedSummary = Seq(summaryPrefix, annotation).mkString("\n")

      (errRes / "summary").as[String] shouldEqual expectedSummary
      (errRes / "error" / "code").as[String] shouldEqual expectedCode
      (errRes / "error" / "message").as[String] shouldEqual expectedMessage
    }
    test("dividing Int / 0.0 results in Infinity") {
      val db = aDatabase.sample
      queryOk("""234 / 0.0""", db).as[String] shouldEqual "Infinity"
    }
    test("dividing (negative Int) / 0.0 results in -Infinity") {
      val db = aDatabase.sample
      queryOk("""-234 / 0.0""", db).as[String] shouldEqual "-Infinity"
    }
    test("divides Int / Long to get a Int") {
      val db = aDatabase.sample
      queryOk("""-5 / 3000000000""", db)
        .as[Int] shouldEqual 0
    }
    test("divides Long / Int to get a Long") {
      val db = aDatabase.sample
      queryOk("""300000000000 / 5""", db).as[Long] shouldEqual 60000000000L
    }
    test("dividing Long / 0 results in Infinity") {
      val db = aDatabase.sample
      val errRes = queryErr("""70000000000 / 0""", db)

      val expectedCode = "divide_by_zero"
      val expectedMessage = "Attempted integer division by zero."
      val summaryPrefix = s"error: $expectedMessage"
      val annotation = s"""|at *query*:1:15
                           |  |
                           |1 | 70000000000 / 0
                           |  |               ^
                           |  |""".stripMargin

      val expectedSummary = Seq(summaryPrefix, annotation).mkString("\n")

      (errRes / "summary").as[String] shouldEqual expectedSummary
      (errRes / "error" / "code").as[String] shouldEqual expectedCode
      (errRes / "error" / "message").as[String] shouldEqual expectedMessage
    }
    test("dividing Long / 0.0 results in Infinity") {
      val db = aDatabase.sample
      queryOk("""70000000000 / 0.0""", db).as[String] shouldEqual "Infinity"
    }
    test("dividing (negative Long) / 0.0 results in -Infinity") {
      val db = aDatabase.sample
      queryOk("""-70000000000 / 0.0""", db).as[String] shouldEqual "-Infinity"
    }
    test("divides Long / Long to get a Int") {
      val db = aDatabase.sample
      queryOk("""2147483648 / 2147483648""", db).as[Int] shouldEqual 1
    }
    test("divides Int / Double to get a Double") {
      val db = aDatabase.sample
      queryOk("""3 / 9.8""", db).as[Double] shouldEqual 0.3061224489795918
    }
    test("divides Double / Int to get a Double") {
      val db = aDatabase.sample
      queryOk("""5.8 / 9""", db).as[Double] shouldEqual 0.6444444444444444
    }
    test("divides Long / Double to get a Double") {
      val db = aDatabase.sample
      queryOk("""8000000000 / 1.2""", db).as[Double] shouldEqual 6.666666666666667e9
    }
    test("divides Double / Long to get a Double") {
      val db = aDatabase.sample
      queryOk("""54.321 / 8000000000""", db).as[Double] shouldEqual 6.790125e-9
    }
    test("divides Double / Double to get a Double") {
      val db = aDatabase.sample
      queryOk("""2.71828182845904523536 / 3.14159265358979323846""", db)
        .as[Double] shouldEqual 0.8652559794322651
    }
    test("divides integral Doubles to get a Double") {
      val db = aDatabase.sample
      queryOk("""1.0 / 2.0""", db).as[Double] shouldEqual 0.5
    }
    test("if Double / Double overflows the result is Infinity") {
      val db = aDatabase.sample
      queryOk("""1.7e308 / 1.7e-308""", db).as[String] shouldEqual "Infinity"
    }
    test("if Double / Double underflows the result is -Infinity") {
      val db = aDatabase.sample
      queryOk("""-1.7e308 / 1.7e-308""", db).as[String] shouldEqual "-Infinity"
    }
    test("dividing Double / 0 results in Infinity") {
      val db = aDatabase.sample
      queryOk("""2.34 / 0""", db).as[String] shouldEqual "Infinity"
    }
    test("dividing (negative Double) / 0 results in -Infinity") {
      val db = aDatabase.sample
      queryOk("""-2.34 / 0""", db).as[String] shouldEqual "-Infinity"
    }
    test("dividing Double / 0.0 results in Infinity") {
      val db = aDatabase.sample
      queryOk("""234e-9 / 0.0""", db).as[String] shouldEqual "Infinity"
    }
    test("dividing (negative Double) / 0.0 results in -Infinity") {
      val db = aDatabase.sample
      queryOk("""-234e-9 / 0.0""", db).as[String] shouldEqual "-Infinity"
    }
    test("associates left-to-right") {
      val db = aDatabase.sample
      queryOk("""81 / 9 / 9""", db).as[Int] shouldEqual 1
    }
  }
}
