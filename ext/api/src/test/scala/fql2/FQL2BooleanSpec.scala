package fauna.api.test

class FQL2BooleanSpec extends FQL2APISpec {
  test("==") {
    val db = aDatabase.sample
    queryOk("""true == true""", db).as[Boolean] shouldEqual true
    queryOk("""true == false""", db).as[Boolean] shouldEqual false
    queryOk("""false == false""", db).as[Boolean] shouldEqual true
    queryOk("""true == 1""", db).as[Boolean] shouldEqual false
    queryOk("""true == 1.0""", db).as[Boolean] shouldEqual false
    queryOk("""true == 'hi'""", db).as[Boolean] shouldEqual false
  }
  test("!=") {
    val db = aDatabase.sample
    queryOk("""true != true""", db).as[Boolean] shouldEqual false
    queryOk("""true != false""", db).as[Boolean] shouldEqual true
    queryOk("""false != false""", db).as[Boolean] shouldEqual false
    queryOk("""true != 1""", db).as[Boolean] shouldEqual true
    queryOk("""true != 1.0""", db).as[Boolean] shouldEqual true
    queryOk("""true != 'hi'""", db).as[Boolean] shouldEqual true
  }
  test("!") {
    val db = aDatabase.sample
    queryOk("""!true""", db).as[Boolean] shouldEqual false
    queryOk("""!false""", db).as[Boolean] shouldEqual true
  }
  test("||") {
    val db = aDatabase.sample
    queryOk("""true || true""", db).as[Boolean] shouldEqual true
    queryOk("""true || false""", db).as[Boolean] shouldEqual true
    queryOk("""false || true""", db).as[Boolean] shouldEqual true
    queryOk("""false || false""", db).as[Boolean] shouldEqual false
  }
  test("&&") {
    val db = aDatabase.sample
    queryOk("""true && true""", db).as[Boolean] shouldEqual true
    queryOk("""true && false""", db).as[Boolean] shouldEqual false
    queryOk("""false && true""", db).as[Boolean] shouldEqual false
    queryOk("""false && false""", db).as[Boolean] shouldEqual false
  }
}
