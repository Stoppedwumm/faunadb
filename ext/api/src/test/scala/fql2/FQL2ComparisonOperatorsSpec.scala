package fauna.api.test

class FQL2ComparisonOperatorsSpec extends FQL2APISpec {
  "== operator" - {
    // TODO: need to do longs
    // TODO: need to do bytes
    // TODO: need to do Time
    // TODO: need to do date
    // TODO: need to do UUID
    // TODO: docs, these could point to the same doc but be different if
    // read with a different set of prior writes?
    test("== works for doubles") {
      val db = aDatabase.sample
      queryOk("""1.33 == 1.33""", db).as[Boolean] shouldEqual true
      queryOk("""1.33 == 1.34""", db).as[Boolean] shouldEqual false
      queryOk("""1.33 == 1""", db).as[Boolean] shouldEqual false
      queryOk("""1.0 == 1""", db).as[Boolean] shouldEqual true
      queryOk("""1.33 == 'hi'""", db).as[Boolean] shouldEqual false
      queryOk("""1.33 == true""", db).as[Boolean] shouldEqual false
    }
    test("== works for null") {
      val db = aDatabase.sample
      queryOk("""null == null""", db).as[Boolean] shouldEqual true
      queryOk("""null == 5""", db).as[Boolean] shouldEqual false
      queryOk("""null == 'hi'""", db).as[Boolean] shouldEqual false
      queryOk("""null == true""", db).as[Boolean] shouldEqual false
      queryOk("""null == 5.4""", db).as[Boolean] shouldEqual false
    }
  }
}
