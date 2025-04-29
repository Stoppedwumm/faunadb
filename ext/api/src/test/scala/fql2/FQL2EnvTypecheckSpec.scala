package fauna.api.test

import fauna.codex.json._
import scala.concurrent.duration._

class FQL2EnvTypecheckSpec extends FQL2APISpec {
  "db typecheck flag" - {
    test("checks env types if db typechecking is enabled") {
      val db = aDatabase.sample

      queryOk(
        """|Function.create({
           |  name: "aFn",
           |  body: "x => 'foo' / x"
           |})""".stripMargin,
        db)

      // turn on typechecking
      queryOk(
        s"""Database.byName("${db.name}")!.update({typechecked: true})""",
        RootDB)

      // update triggers schema validation
      val res = queryErr("""Function.byName("aFn")!.update({})""", db)
      (res / "error" / "code") shouldEqual JSString("invalid_schema")

      // can "fix" the validation error by deleting.
      queryOk("""Function.byName("aFn")!.delete()""", db)
    }

    test("db typecheck flag controls default query behavior") {
      val db = aDatabase.sample

      // this will typecheck
      val res1 = queryRaw("x => 'foo' / x", db, FQL2Params(typecheck = None))
      (res1.json / "data").as[String] shouldEqual "[function <lambda>]"

      // turn on typechecking
      queryOk(
        s"""Database.byName("${db.name}")!.update({typechecked: true})""",
        RootDB)

      // this won't typecheck
      eventually(timeout(10.seconds), interval(200.millis)) {
        val res2 = queryRaw("x => 'foo' / x", db, FQL2Params(typecheck = None))
        (res2.json / "error" / "code").as[String] shouldEqual "invalid_query"
      }
    }
  }
}
