package fauna.api.test

import fauna.prop.api.Database
import fauna.prop.Prop

class FQL2LoggingSpec extends FQL2APISpec {

  def evalWithKey(db: Database, fn: String)(secret: => String) = {
    // 2 log items will be produced:
    // - one in *query* with message "foo"
    // - one in *udf:MyUserFunction* with message "bar"
    queryOk(
      s"""|Function.create({
          |  name: "MyUserFunction",
          |  body: "() => $fn('bar')"
          |})""".stripMargin,
      db
    )

    queryRaw(
      s"""|$fn('foo')
          |MyUserFunction()
          |""".stripMargin,
      secret,
      FQL2Params()
    ).json
  }

  "FQL2LoggingSpec" - {
    "log" - {
      once("admin role") {
        for {
          db  <- aDatabase
          key <- aKey(db, role = Prop.const("admin"))
        } yield {
          val res = evalWithKey(db, "log") {
            (key / "secret").as[String]
          }

          (res / "summary").as[String] shouldBe
            """|info at *query*:1: foo
               |
               |info at *udf:MyUserFunction*:1: bar""".stripMargin
        }
      }

      once("server role") {
        for {
          db  <- aDatabase
          key <- aKey(db, role = Prop.const("server"))
        } yield {
          val res = evalWithKey(db, "log") {
            (key / "secret").as[String]
          }

          (res / "summary").as[String] shouldBe
            """|info at *query*:1: foo
               |
               |info at *udf:MyUserFunction*:1: bar""".stripMargin
        }
      }

      once("server-readonly role") {
        for {
          db  <- aDatabase
          key <- aKey(db, role = Prop.const("server-readonly"))
        } yield {
          // server-readonly cannot even call the UDF, so no "bar" log item
          val res = evalWithKey(db, "log") {
            (key / "secret").as[String]
          }

          (res / "summary").as[String] shouldBe
            """|error: Insufficient privileges to perform the action.
               |at *query*:2:15
               |  |
               |2 | MyUserFunction()
               |  |               ^^
               |  |
               |
               |info at *query*:1: foo""".stripMargin
        }
      }

      once("client role") {
        for {
          db  <- aDatabase
          key <- aKey(db, role = Prop.const("client"))
        } yield {
          // client cannot even call the UDF, so no "bar" log item
          val res = evalWithKey(db, "log") {
            (key / "secret").as[String]
          }

          (res / "summary").as[String] shouldBe
            """|error: Insufficient privileges to perform the action.
               |at *query*:2:15
               |  |
               |2 | MyUserFunction()
               |  |               ^^
               |  |
               |
               |info at *query*:1: foo""".stripMargin
        }
      }

      once("custom role") {
        for {
          db <- aDatabase
        } yield {
          // custom roles cannot yet see log items
          // other than *query*
          val res = evalWithKey(db, "log") {
            val role = aRole(
              db,
              Seq.empty,
              Seq(Privilege("MyUserFunction", call = RoleAction.Granted))).sample
            val key = aKey(db, Prop.const(role)).sample

            (key / "secret").as[String]
          }

          (res / "summary").as[String] shouldBe """info at *query*:1: foo"""
        }
      }
    }

    "dbg" - {
      once("admin role") {
        for {
          db  <- aDatabase
          key <- aKey(db, role = Prop.const("admin"))
        } yield {
          val res = evalWithKey(db, "dbg") {
            (key / "secret").as[String]
          }

          (res / "summary").as[String] shouldBe
            """|info: "foo"
               |at *query*:1:4
               |  |
               |1 | dbg('foo')
               |  |    ^^^^^^^
               |  |
               |
               |info: "bar"
               |at *udf:MyUserFunction*:1:10
               |  |
               |1 | () => dbg('bar')
               |  |          ^^^^^^^
               |  |""".stripMargin
        }
      }

      once("server role") {
        for {
          db  <- aDatabase
          key <- aKey(db, role = Prop.const("server"))
        } yield {
          val res = evalWithKey(db, "dbg") {
            (key / "secret").as[String]
          }

          (res / "summary").as[String] shouldBe
            """|info: "foo"
               |at *query*:1:4
               |  |
               |1 | dbg('foo')
               |  |    ^^^^^^^
               |  |
               |
               |info: "bar"
               |at *udf:MyUserFunction*:1:10
               |  |
               |1 | () => dbg('bar')
               |  |          ^^^^^^^
               |  |""".stripMargin
        }
      }

      once("server-readonly role") {
        for {
          db  <- aDatabase
          key <- aKey(db, role = Prop.const("server-readonly"))
        } yield {
          // server-readonly cannot even call the UDF, so no "bar" log item
          val res = evalWithKey(db, "dbg") {
            (key / "secret").as[String]
          }

          (res / "summary").as[String] shouldBe
            """|error: Insufficient privileges to perform the action.
               |at *query*:2:15
               |  |
               |2 | MyUserFunction()
               |  |               ^^
               |  |
               |
               |info: "foo"
               |at *query*:1:4
               |  |
               |1 | dbg('foo')
               |  |    ^^^^^^^
               |  |""".stripMargin
        }
      }

      once("client role") {
        for {
          db  <- aDatabase
          key <- aKey(db, role = Prop.const("client"))
        } yield {
          // client cannot even call the UDF, so no "bar" log item
          val res = evalWithKey(db, "dbg") {
            (key / "secret").as[String]
          }

          (res / "summary").as[String] shouldBe
            """|error: Insufficient privileges to perform the action.
               |at *query*:2:15
               |  |
               |2 | MyUserFunction()
               |  |               ^^
               |  |
               |
               |info: "foo"
               |at *query*:1:4
               |  |
               |1 | dbg('foo')
               |  |    ^^^^^^^
               |  |""".stripMargin
        }
      }

      once("custom role") {
        for {
          db <- aDatabase
        } yield {
          // custom roles cannot yet see log items
          // other than *query*
          val res = evalWithKey(db, "dbg") {
            val role = aRole(
              db,
              Seq.empty,
              Seq(Privilege("MyUserFunction", call = RoleAction.Granted))).sample
            val key = aKey(db, Prop.const(role)).sample

            (key / "secret").as[String]
          }

          (res / "summary").as[String] shouldBe
            """|info: "foo"
               |at *query*:1:4
               |  |
               |1 | dbg('foo')
               |  |    ^^^^^^^
               |  |""".stripMargin
        }
      }
    }
  }
}
