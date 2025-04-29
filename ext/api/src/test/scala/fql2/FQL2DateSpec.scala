package fauna.api.test

import fauna.codex.json
import java.time.temporal.ChronoUnit
import java.time.LocalDate

class FQL2DateSpec extends FQL2APISpec {
  implicit object LocalDateDecoder extends json.JsonDecoder[LocalDate] {
    def decode(stream: json.JsonCodec.In): LocalDate =
      LocalDate.parse(stream.as[String])
  }

  test("today") {
    val db = aDatabase.sample

    val t0 = queryOk("Date.today()", db).as[LocalDate]
    val t1 = queryOk("Date.today()", db).as[LocalDate]

    t1 should be >= t0
  }

  test("Date()") {
    val db = aDatabase.sample

    queryOk("""Date("1970-01-01")""", db)
      .as[LocalDate] shouldBe LocalDate.parse("1970-01-01")
  }

  test("fromString") {
    val db = aDatabase.sample

    queryOk("""Date.fromString("1970-01-01")""", db)
      .as[LocalDate] shouldBe LocalDate.parse("1970-01-01")
  }

  test("==") {
    val db = aDatabase.sample

    queryOk(
      """Date("1970-01-01") == Date("1970-01-01")""",
      db
    ).as[Boolean] shouldBe true

    queryOk(
      """Date("1970-01-01") == Date("1970-01-02")""",
      db
    ).as[Boolean] shouldBe false
  }

  test("!=") {
    val db = aDatabase.sample

    queryOk(
      """Date("1970-01-01") != Date("1970-01-01")""",
      db
    ).as[Boolean] shouldBe false

    queryOk(
      """Date("1970-01-01") != Date("1970-01-02")""",
      db
    ).as[Boolean] shouldBe true
  }

  test(">") {
    val db = aDatabase.sample

    queryOk(
      """Date("1970-01-02") > Date("1970-01-01")""",
      db
    ).as[Boolean] shouldBe true

    queryOk(
      """Date("1970-01-01") > Date("1970-01-02")""",
      db
    ).as[Boolean] shouldBe false
  }

  test(">=") {
    val db = aDatabase.sample

    queryOk(
      """Date("1970-01-02") >= Date("1970-01-01")""",
      db
    ).as[Boolean] shouldBe true

    queryOk(
      """Date("1970-01-01") >= Date("1970-01-01")""",
      db
    ).as[Boolean] shouldBe true

    queryOk(
      """Date("1970-01-01") >= Date("1970-01-02")""",
      db
    ).as[Boolean] shouldBe false
  }

  test("<") {
    val db = aDatabase.sample

    queryOk(
      """Date("1970-01-01") < Date("1970-01-02")""",
      db
    ).as[Boolean] shouldBe true

    queryOk(
      """Date("1970-01-02") < Date("1970-01-01")""",
      db
    ).as[Boolean] shouldBe false
  }

  test("<=") {
    val db = aDatabase.sample

    queryOk(
      """Date("1970-01-01") <= Date("1970-01-02")""",
      db
    ).as[Boolean] shouldBe true

    queryOk(
      """Date("1970-01-01") <= Date("1970-01-01")""",
      db
    ).as[Boolean] shouldBe true

    queryOk(
      """Date("1970-01-02") <= Date("1970-01-01")""",
      db
    ).as[Boolean] shouldBe false
  }

  test("add") {
    val db = aDatabase.sample

    queryOk(
      """|let dt = Date("1970-01-01")
         |[
         |  dt.add(1, "day"),
         |  dt.add(1, "days")
         |]
         |""".stripMargin,
      db
    ).as[Seq[LocalDate]] shouldBe Seq(
      LocalDate.EPOCH.plus(1, ChronoUnit.DAYS),
      LocalDate.EPOCH.plus(1, ChronoUnit.DAYS)
    )
  }

  test("subtract") {
    val db = aDatabase.sample

    queryOk(
      """|let dt = Date("1970-01-01")
         |[
         |  dt.subtract(1, "day"),
         |  dt.subtract(1, "days")
         |]
         |""".stripMargin,
      db
    ).as[Seq[LocalDate]] shouldBe Seq(
      LocalDate.EPOCH.minus(1, ChronoUnit.DAYS),
      LocalDate.EPOCH.minus(1, ChronoUnit.DAYS)
    )
  }

  test("error on invalid date") {
    val db = aDatabase.sample

    val err = queryErr("""Date("01-01-2020")""", db)
    (err / "error" / "code").as[String] shouldBe "invalid_date"
    (err / "error" / "message")
      .as[String] shouldBe "`01-01-2020` is not a valid date."
  }

  test("error on invalid unit") {
    val db = aDatabase.sample

    val err =
      queryErr("""Date("1970-01-01").add(1, "milliseconds")""", db)
    (err / "error" / "code").as[String] shouldBe "invalid_unit"
    (err / "error" / "message")
      .as[String] shouldBe "`milliseconds` is not a valid date unit."
  }
}
