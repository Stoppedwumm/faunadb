package fauna.api.test

import fauna.codex.json
import fauna.lang.Timestamp
import scala.concurrent.duration._

class FQL2TimeSpec extends FQL2APISpec {
  implicit object TimestampDecoder extends json.JsonDecoder[Timestamp] {
    def decode(stream: json.JsonCodec.In): Timestamp =
      Timestamp.parse(stream.as[String])
  }

  test("now") {
    val db = aDatabase.sample

    val t0 = queryOk("Time.now()", db).as[Timestamp]
    val t1 = queryOk("Time.now()", db).as[Timestamp]

    t1 should be >= t0
  }

  test("Time()") {
    val db = aDatabase.sample

    queryOk("""Time("1970-01-01T00:00:00.123Z")""", db)
      .as[Timestamp] shouldBe Timestamp.parse("1970-01-01T00:00:00.123Z")
  }

  test("fromString") {
    val db = aDatabase.sample

    queryOk("""Time.fromString("1970-01-01T00:00:00.123Z")""", db)
      .as[Timestamp] shouldBe Timestamp.parse("1970-01-01T00:00:00.123Z")
  }

  test("==") {
    val db = aDatabase.sample

    queryOk(
      """Time("1970-01-01T00:00:00Z") == Time("1970-01-01T00:00:00Z")""",
      db
    ).as[Boolean] shouldBe true

    queryOk(
      """Time("1970-01-01T00:00:00Z") == Time("1970-01-01T00:00:01Z")""",
      db
    ).as[Boolean] shouldBe false
  }

  test("!=") {
    val db = aDatabase.sample

    queryOk(
      """Time("1970-01-01T00:00:00Z") != Time("1970-01-01T00:00:00Z")""",
      db
    ).as[Boolean] shouldBe false

    queryOk(
      """Time("1970-01-01T00:00:00Z") != Time("1970-01-01T00:00:01Z")""",
      db
    ).as[Boolean] shouldBe true
  }

  test(">") {
    val db = aDatabase.sample

    queryOk(
      """Time("1970-01-01T00:00:01Z") > Time("1970-01-01T00:00:00Z")""",
      db
    ).as[Boolean] shouldBe true

    queryOk(
      """Time("1970-01-01T00:00:00Z") > Time("1970-01-01T00:00:01Z")""",
      db
    ).as[Boolean] shouldBe false
  }

  test(">=") {
    val db = aDatabase.sample

    queryOk(
      """Time("1970-01-01T00:00:01Z") >= Time("1970-01-01T00:00:00Z")""",
      db
    ).as[Boolean] shouldBe true

    queryOk(
      """Time("1970-01-01T00:00:00Z") >= Time("1970-01-01T00:00:00Z")""",
      db
    ).as[Boolean] shouldBe true

    queryOk(
      """Time("1970-01-01T00:00:00Z") >= Time("1970-01-01T00:00:01Z")""",
      db
    ).as[Boolean] shouldBe false
  }

  test("<") {
    val db = aDatabase.sample

    queryOk(
      """Time("1970-01-01T00:00:00Z") < Time("1970-01-01T00:00:01Z")""",
      db
    ).as[Boolean] shouldBe true

    queryOk(
      """Time("1970-01-01T00:00:01Z") < Time("1970-01-01T00:00:00Z")""",
      db
    ).as[Boolean] shouldBe false
  }

  test("<=") {
    val db = aDatabase.sample

    queryOk(
      """Time("1970-01-01T00:00:00Z") <= Time("1970-01-01T00:00:01Z")""",
      db
    ).as[Boolean] shouldBe true

    queryOk(
      """Time("1970-01-01T00:00:00Z") <= Time("1970-01-01T00:00:00Z")""",
      db
    ).as[Boolean] shouldBe true

    queryOk(
      """Time("1970-01-01T00:00:01Z") <= Time("1970-01-01T00:00:00Z")""",
      db
    ).as[Boolean] shouldBe false
  }

  test("add") {
    val db = aDatabase.sample

    queryOk(
      """|let ts = Time("1970-01-01T00:00:00Z")
         |[
         |  ts.add(1, "nanosecond"),
         |  ts.add(1, "nanoseconds"),
         |  ts.add(1, "microsecond"),
         |  ts.add(1, "microseconds"),
         |  ts.add(1, "millisecond"),
         |  ts.add(1, "milliseconds"),
         |  ts.add(1, "second"),
         |  ts.add(1, "seconds"),
         |  ts.add(1, "minute"),
         |  ts.add(1, "minutes"),
         |  ts.add(1, "hour"),
         |  ts.add(1, "hours"),
         |  ts.add(1, "half day"),
         |  ts.add(1, "half days"),
         |  ts.add(1, "day"),
         |  ts.add(1, "days")
         |]
         |""".stripMargin,
      db
    ).as[Seq[Timestamp]] shouldBe Seq(
      Timestamp.Epoch + 1.nanos,
      Timestamp.Epoch + 1.nanos,
      Timestamp.Epoch + 1.micros,
      Timestamp.Epoch + 1.micros,
      Timestamp.Epoch + 1.millis,
      Timestamp.Epoch + 1.millis,
      Timestamp.Epoch + 1.seconds,
      Timestamp.Epoch + 1.seconds,
      Timestamp.Epoch + 1.minutes,
      Timestamp.Epoch + 1.minutes,
      Timestamp.Epoch + 1.hours,
      Timestamp.Epoch + 1.hours,
      Timestamp.Epoch + 12.hours,
      Timestamp.Epoch + 12.hours,
      Timestamp.Epoch + 1.days,
      Timestamp.Epoch + 1.days
    )
  }

  test("subtract") {
    val db = aDatabase.sample

    queryOk(
      """|let ts = Time("1970-01-01T00:00:00Z")
         |[
         |  ts.subtract(1, "nanosecond"),
         |  ts.subtract(1, "nanoseconds"),
         |  ts.subtract(1, "microsecond"),
         |  ts.subtract(1, "microseconds"),
         |  ts.subtract(1, "millisecond"),
         |  ts.subtract(1, "milliseconds"),
         |  ts.subtract(1, "second"),
         |  ts.subtract(1, "seconds"),
         |  ts.subtract(1, "minute"),
         |  ts.subtract(1, "minutes"),
         |  ts.subtract(1, "hour"),
         |  ts.subtract(1, "hours"),
         |  ts.subtract(1, "half day"),
         |  ts.subtract(1, "half days"),
         |  ts.subtract(1, "day"),
         |  ts.subtract(1, "days")
         |]
         |""".stripMargin,
      db
    ).as[Seq[Timestamp]] shouldBe Seq(
      Timestamp.Epoch - 1.nanos,
      Timestamp.Epoch - 1.nanos,
      Timestamp.Epoch - 1.micros,
      Timestamp.Epoch - 1.micros,
      Timestamp.Epoch - 1.millis,
      Timestamp.Epoch - 1.millis,
      Timestamp.Epoch - 1.seconds,
      Timestamp.Epoch - 1.seconds,
      Timestamp.Epoch - 1.minutes,
      Timestamp.Epoch - 1.minutes,
      Timestamp.Epoch - 1.hours,
      Timestamp.Epoch - 1.hours,
      Timestamp.Epoch - 12.hours,
      Timestamp.Epoch - 12.hours,
      Timestamp.Epoch - 1.days,
      Timestamp.Epoch - 1.days
    )
  }

  test("error on invalid time") {
    val db = aDatabase.sample

    val err = queryErr("""Time("2020-01-01 01:01:01")""", db)
    (err / "error" / "code").as[String] shouldBe "invalid_time"
    (err / "error" / "message")
      .as[String] shouldBe "`2020-01-01 01:01:01` is not a valid time."
  }

  test("error on invalid unit") {
    val db = aDatabase.sample

    val err =
      queryErr("""Time("2020-01-01T01:01:01Z").add(1, "invalid")""", db)
    (err / "error" / "code").as[String] shouldBe "invalid_unit"
    (err / "error" / "message")
      .as[String] shouldBe "`invalid` is not a valid time unit."
  }
}
