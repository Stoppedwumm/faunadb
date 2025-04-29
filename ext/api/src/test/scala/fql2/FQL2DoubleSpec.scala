package fauna.api.test

class FQL2DoubleSpec extends FQL2APISpec {
  test("==") {
    val db = aDatabase.sample
    queryOk("""1.33 == 1.33""", db).as[Boolean] shouldEqual true
    queryOk("""1.33 == 1.34""", db).as[Boolean] shouldEqual false
    queryOk("""1.33 == 1""", db).as[Boolean] shouldEqual false
    queryOk("""1.0 == 1""", db).as[Boolean] shouldEqual true
    queryOk("""1.33 == 'hi'""", db).as[Boolean] shouldEqual false
    queryOk("""1.33 == true""", db).as[Boolean] shouldEqual false
  }
  test("!=") {
    val db = aDatabase.sample
    queryOk("""1.33 != 1.33""", db).as[Boolean] shouldEqual false
    queryOk("""1.33 != 1.34""", db).as[Boolean] shouldEqual true
    queryOk("""1.33 != 1""", db).as[Boolean] shouldEqual true
    queryOk("""1.0 != 1""", db).as[Boolean] shouldEqual false
    queryOk("""1.33 != 'hi'""", db).as[Boolean] shouldEqual true
    queryOk("""1.33 != true""", db).as[Boolean] shouldEqual true
  }
  test("<") {
    val db = aDatabase.sample
    queryOk("""1.33 < 1.33""", db).as[Boolean] shouldEqual false
    queryOk("""1.33 < 1.34""", db).as[Boolean] shouldEqual true
    queryOk("""1.33 < 1""", db).as[Boolean] shouldEqual false
    queryOk("""1.33 < 2""", db).as[Boolean] shouldEqual true
    queryOk("""1.0 < 1""", db).as[Boolean] shouldEqual false
    queryOk("""1.0 < 'hi'""", db).as[Boolean] shouldEqual false
    queryOk("""1.0 < true""", db).as[Boolean] shouldEqual false
  }
  test(">") {
    val db = aDatabase.sample
    queryOk("""1.33 > 1.33""", db).as[Boolean] shouldEqual false
    queryOk("""1.33 > 1.34""", db).as[Boolean] shouldEqual false
    queryOk("""1.33 > 1""", db).as[Boolean] shouldEqual true
    queryOk("""1.33 > 2""", db).as[Boolean] shouldEqual false
    queryOk("""1.0 > 1""", db).as[Boolean] shouldEqual false
    queryOk("""1.0 > 'hi'""", db).as[Boolean] shouldEqual false
    queryOk("""1.0 > true""", db).as[Boolean] shouldEqual false
  }
  test(">=") {
    val db = aDatabase.sample
    queryOk("""1.33 >= 1.33""", db).as[Boolean] shouldEqual true
    queryOk("""1.33 >= 1.34""", db).as[Boolean] shouldEqual false
    queryOk("""1.33 >= 1""", db).as[Boolean] shouldEqual true
    queryOk("""1.33 >= 2""", db).as[Boolean] shouldEqual false
    queryOk("""1.0 >= 1""", db).as[Boolean] shouldEqual true
    queryOk("""1.0 >= 'hi'""", db).as[Boolean] shouldEqual false
    queryOk("""1.0 >= true""", db).as[Boolean] shouldEqual false
  }
  test("<=") {
    val db = aDatabase.sample
    queryOk("""1.33 <= 1.33""", db).as[Boolean] shouldEqual true
    queryOk("""1.33 <= 1.34""", db).as[Boolean] shouldEqual true
    queryOk("""1.33 <= 1""", db).as[Boolean] shouldEqual false
    queryOk("""1.33 <= 2""", db).as[Boolean] shouldEqual true
    queryOk("""1.0 <= 1""", db).as[Boolean] shouldEqual true
    queryOk("""1.0 <= 'hi'""", db).as[Boolean] shouldEqual false
    queryOk("""1.0 <= true""", db).as[Boolean] shouldEqual false
  }
}
