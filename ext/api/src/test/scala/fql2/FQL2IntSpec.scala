package fauna.api.test

class FQL2IntSpec extends FQL2APISpec {
  test("==") {
    val db = aDatabase.sample
    queryOk("""1 == 1""", db).as[Boolean] shouldEqual true
    queryOk("""1 == 1.0""", db).as[Boolean] shouldEqual true
    queryOk("""1 == 2""", db).as[Boolean] shouldEqual false
    queryOk("""1 == 1.5""", db).as[Boolean] shouldEqual false
    queryOk("""1 == 'hi'""", db).as[Boolean] shouldEqual false
    queryOk("""1 == true""", db).as[Boolean] shouldEqual false
  }
  test("!=") {
    val db = aDatabase.sample
    queryOk("""1 != 1""", db).as[Boolean] shouldEqual false
    queryOk("""1 != 1.0""", db).as[Boolean] shouldEqual false
    queryOk("""1 != 2""", db).as[Boolean] shouldEqual true
    queryOk("""1 != 1.5""", db).as[Boolean] shouldEqual true
    queryOk("""1 != 'hi'""", db).as[Boolean] shouldEqual true
    queryOk("""1 != true""", db).as[Boolean] shouldEqual true
  }
  test("<") {
    val db = aDatabase.sample
    queryOk("""1 < 1""", db).as[Boolean] shouldEqual false
    queryOk("""1 < 1.0""", db).as[Boolean] shouldEqual false
    queryOk("""1 < 2""", db).as[Boolean] shouldEqual true
    queryOk("""1 < 1.5""", db).as[Boolean] shouldEqual true
    queryOk("""2 < 'hi'""", db).as[Boolean] shouldEqual false
    queryOk("""2 < true""", db).as[Boolean] shouldEqual false
  }
  test(">") {
    val db = aDatabase.sample
    queryOk("""1 > 1""", db).as[Boolean] shouldEqual false
    queryOk("""1 > 1.0""", db).as[Boolean] shouldEqual false
    queryOk("""1 > 2""", db).as[Boolean] shouldEqual false
    queryOk("""2 > 1""", db).as[Boolean] shouldEqual true
    queryOk("""2 > 1.5""", db).as[Boolean] shouldEqual true
    queryOk("""2 > 'hi'""", db).as[Boolean] shouldEqual false
    queryOk("""2 > true""", db).as[Boolean] shouldEqual false
  }
  test("<=") {
    val db = aDatabase.sample
    queryOk("""1 <= 1""", db).as[Boolean] shouldEqual true
    queryOk("""1 <= 1.0""", db).as[Boolean] shouldEqual true
    queryOk("""1 <= 2""", db).as[Boolean] shouldEqual true
    queryOk("""2 <= 1""", db).as[Boolean] shouldEqual false
    queryOk("""1 <= 1.5""", db).as[Boolean] shouldEqual true
    queryOk("""1 <= 'hi'""", db).as[Boolean] shouldEqual false
    queryOk("""1 <= true""", db).as[Boolean] shouldEqual false
  }
  test(">=") {
    val db = aDatabase.sample
    queryOk("""1 >= 1""", db).as[Boolean] shouldEqual true
    queryOk("""1 >= 1.0""", db).as[Boolean] shouldEqual true
    queryOk("""1 >= 2""", db).as[Boolean] shouldEqual false
    queryOk("""2 >= 1""", db).as[Boolean] shouldEqual true
    queryOk("""2 >= 1.5""", db).as[Boolean] shouldEqual true
    queryOk("""1 >= 'hi'""", db).as[Boolean] shouldEqual false
    queryOk("""1 >= true""", db).as[Boolean] shouldEqual false
  }
}
