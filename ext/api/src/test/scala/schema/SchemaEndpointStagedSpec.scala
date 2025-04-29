package fauna.api.test

import fauna.prop.api.Database
import scala.concurrent.duration._

class SchemaEndpointStagedSpec extends FQL2APISpec {

  def status(db: Database): String = {
    val status0 = api.get("/schema/1/status", db.adminKey)
    status0 should respond(OK)
    val s0 = (status0.json / "status").as[String]

    val status1 = api.get("/schema/1/staged/status", db.adminKey)
    status1 should respond(OK)
    val s1 = (status1.json / "status").as[String]

    s0 shouldBe s1
    s0
  }

  def pendingSummary(db: Database): String = {
    val status0 = api.get("/schema/1/staged/status?format=semantic", db.adminKey)
    status0 should respond(OK)
    (status0.json / "pending_summary").as[String]
  }

  def stagedDiff(db: Database, mode: String = "semantic"): String = {
    val status0 = api.get(s"/schema/1/status?diff=$mode", db.adminKey)
    status0 should respond(OK)
    val s0 = (status0.json / "diff").as[String]

    val status1 = api.get(s"/schema/1/staged/status?format=$mode", db.adminKey)
    status1 should respond(OK)
    val s1 = (status1.json / "diff").as[String]

    s0 shouldBe s1
    s0
  }

  test("staged schema is disallowed in the root scope") {
    val stagedFiles = Seq(
      "foo.fsl" -> "function foo() { 2 }"
    )

    val res =
      api.upload(
        "/schema/1/update?force=true&staged=true",
        stagedFiles,
        RootDB.adminKey)
    res should respond(BadRequest)
    (res.json / "error" / "message").as[String] shouldBe (
      "Schema cannot be staged in the root database"
    )
  }

  "schema/1/update?staged=true" / {
    once("collections can be deleted in a staged update") {
      for {
        db <- aDatabase
      } {
        val files = Seq(
          "foo.fsl" -> "collection Foo {}"
        )

        val res0 = api.upload("/schema/1/update?force=true", files, db.adminKey)
        res0 should respond(OK)

        val stagedFiles = Seq(
          "foo.fsl" -> ""
        )

        val res1 = api.upload(
          "/schema/1/update?force=true&staged=true",
          stagedFiles,
          db.adminKey)
        res1 should respond(OK)
      }
    }

    once("can rename a schema item in a staged update") {
      for {
        db <- aDatabase
      } {
        val files = Seq(
          "foo.fsl" -> "collection Foo {}"
        )

        val res0 = api.upload("/schema/1/update?force=true", files, db.adminKey)
        res0 should respond(OK)

        // No-op update to enter staged mode.
        val res1 =
          api.upload("/schema/1/update?force=true&staged=true", files, db.adminKey)
        res1 should respond(OK)

        val res2 = api.post(
          "/schema/1/items/collections/Foo?force=true",
          "collection Bar {}",
          db.adminKey)
        res2 should respond(OK)
      }
    }
  }

  "schema/1/validate?staged=true" / {
    once("can validate against staged schema") {
      for {
        db <- aDatabase
      } {
        val files = Seq(
          "foo.fsl" ->
            """|collection Foo {
               |  name: String
               |}""".stripMargin
        )
        val stagedFiles = Seq(
          "foo.fsl" ->
            """|collection Foo {
               |  name: String
               |
               |  index byName {
               |    terms [.name]
               |  }
               |}""".stripMargin
        )
        val moreFiles = Seq(
          "foo.fsl" ->
            """|collection Foo {
               |  name: String
               |
               |  index byName {
               |    terms [.name]
               |    values [.name]
               |  }
               |}""".stripMargin
        )

        val res0 = api.upload("/schema/1/update?force=true", files, db.adminKey)
        res0 should respond(OK)

        api.upload(
          "/schema/1/update?force=true&staged=true",
          stagedFiles,
          db.adminKey) should respond(OK)

        // No headers: compare the given files to the active schema.
        val res1 =
          api.upload("/schema/1/validate?force=true", stagedFiles, db.adminKey)
        res1 should respond(OK)
        (res1.json / "diff").as[String] shouldBe (
          """|* Modifying collection `Foo` at foo.fsl:1:1:
             |  * Indexes:
             |  + add index `byName`
             |
             |""".stripMargin
        )

        // `&staged=true`: compare the given files to the staged schema.
        val res2 = api.upload(
          "/schema/1/validate?force=true&staged=true",
          stagedFiles,
          db.adminKey)
        res2 should respond(OK)
        (res2.json / "diff").as[String] shouldBe ""

        // Same comparison, but now with an additional update included.
        val res3 = api.upload(
          "/schema/1/validate?force=true&staged=true",
          moreFiles,
          db.adminKey)
        res3 should respond(OK)
        (res3.json / "diff").as[String] shouldBe (
          """|* Modifying collection `Foo` at foo.fsl:1:1:
             |  * Indexes:
             |  ~ change index `byName`
             |    + add values set to [.name]
             |
             |""".stripMargin
        )
      }
    }

    once("&diff=summary works") {
      for {
        db <- aDatabase
      } {
        val activeFiles = Seq(
          "foo.fsl" ->
            """|collection Foo {
               |  name: String
               |}""".stripMargin
        )
        val stagedFiles = Seq(
          "foo.fsl" ->
            """|collection Foo {
               |  name: String
               |
               |  index byName {
               |    terms [.name]
               |  }
               |}""".stripMargin
        )

        val res0 =
          api.upload("/schema/1/update?force=true", activeFiles, db.adminKey)
        res0 should respond(OK)

        api.upload(
          "/schema/1/update?force=true&staged=true",
          stagedFiles,
          db.adminKey) should respond(OK)

        // No headers: compare the given files to the active schema.
        val res1 =
          api.upload("/schema/1/validate?diff=summary", activeFiles, db.adminKey)
        res1 should respond(OK)
        (res1.json / "diff").as[String] shouldBe ""

        val res2 =
          api.upload("/schema/1/validate?diff=summary", stagedFiles, db.adminKey)
        res2 should respond(OK)
        (res2.json / "diff").as[String] shouldBe (
          """|* Modifying collection `Foo` at foo.fsl:1:1
             |""".stripMargin
        )

        // &staged=true: compare against the staged schema.
        val res3 =
          api.upload(
            "/schema/1/validate?staged=true&diff=summary",
            activeFiles,
            db.adminKey)
        res3 should respond(OK)
        // This is for removing the index (comparing staged -> active).
        (res3.json / "diff").as[String] shouldBe (
          """|* Modifying collection `Foo` at foo.fsl:1:1
             |""".stripMargin
        )

        val res4 =
          api.upload(
            "/schema/1/validate?staged=true&diff=summary",
            stagedFiles,
            db.adminKey)
        res4 should respond(OK)
        (res4.json / "diff").as[String] shouldBe ""
      }
    }
  }

  "schema/1/staged/commit" / {
    once("commits a staged schema") {
      for {
        db   <- aDatabase
        path <- Seq("commit", "staged/commit")
      } {
        val files = Seq(
          "foo.fsl" -> "function foo() { 1 }"
        )

        val res0 = api.upload("/schema/1/update?force=true", files, db.adminKey)
        res0 should respond(OK)

        val stagedFiles = Seq(
          "foo.fsl" -> "function foo() { 2 }"
        )

        val res1 = api.upload(
          "/schema/1/update?force=true&staged=true",
          stagedFiles,
          db.adminKey)
        res1 should respond(OK)

        // Active schema should still be active.
        queryOk("if (foo() != 1) abort(foo())", db.adminKey)

        // We didn't build any indexes, so this should be ready immediately.
        status(db) shouldBe "ready"

        val res2 = api.post(s"/schema/1/$path?force=true", "", db.adminKey)
        res2 should respond(OK)

        // Staged schema should now be active.
        eventually(timeout(5.seconds)) {
          queryOk("if (foo() != 2) abort(foo())", db.adminKey)
        }
      }
    }

    once("pretty error when there is no staged schema") {
      for {
        db <- aDatabase
      } {
        val res2 = api.post("/schema/1/staged/commit?force=true", "", db.adminKey)
        res2 should respond(BadRequest)
        (res2.json / "error" / "message").as[String] shouldBe (
          "There is no staged schema to commit"
        )
      }
    }
  }

  "schema/1/staged/abandon" / {
    Seq("abandon", "staged/abandon").foreach { path =>
      once(s"abandons a staged schema with $path") {
        for {
          db <- aDatabase
        } {
          val files = Seq(
            "foo.fsl" -> "function foo() { 1 }"
          )

          val res0 = api.upload("/schema/1/update?force=true", files, db.adminKey)
          res0 should respond(OK)

          val stagedFiles = Seq(
            "foo.fsl" -> "function foo() { 2 }"
          )

          val res1 = api.upload(
            "/schema/1/update?force=true&staged=true",
            stagedFiles,
            db.adminKey)
          res1 should respond(OK)

          // Active schema should still be active.
          queryOk("if (foo() != 1) abort(foo())", db.adminKey)

          // We didn't build any indexes, so this should be ready immediately.
          status(db) shouldBe "ready"

          val res2 = api.post(s"/schema/1/$path?force=true", "", db.adminKey)
          res2 should respond(OK)

          // Active schema should still be active, and there should no longer be a
          // staged schema set.
          eventually(timeout(5.seconds)) {
            status(db) shouldBe "none"
          }
          queryOk("if (foo() != 1) abort(foo())", db.adminKey)
        }
      }
    }

    once("pretty error when there is no staged schema") {
      for {
        db <- aDatabase
      } {
        val res2 = api.post(s"/schema/1/abandon?force=true", "", db.adminKey)
        res2 should respond(BadRequest)
        (res2.json / "error" / "message").as[String] shouldBe (
          "There is no staged schema to abandon"
        )
      }
    }
  }

  "schema/1/staged/status" / {
    once("disallowed in the tenant root") {
      for {
        db <- anAccountDatabase
      } {
        val diff0 = api.get("/schema/1/status", db.adminKey)
        diff0 should respond(BadRequest)
        (diff0.json / "error" / "message").as[String] shouldBe (
          "Schema can not be created in your account root."
        )

        val diff1 = api.get("/schema/1/status?diff=semantic", db.adminKey)
        diff1 should respond(BadRequest)
        (diff1.json / "error" / "message").as[String] shouldBe (
          "Schema can not be created in your account root."
        )
      }
    }

    once("builds an index async") {
      for {
        db <- aDatabase
      } {
        val files = Seq(
          "foo.fsl" ->
            """|collection User {
               |  name: String
               |}""".stripMargin
        )

        val res0 = api.upload("/schema/1/update?force=true", files, db.adminKey)
        res0 should respond(OK)

        // Nothing has happened yet.
        status(db) shouldBe "none"

        queryOk(
          """|Set.sequence(0, 129).forEach(i => {
             |  User.create({ name: "user-#{i}" })
             |})""".stripMargin,
          db.adminKey)

        val stagedFiles = Seq(
          "foo.fsl" ->
            """|collection User {
               |  name: String
               |
               |  index byName {
               |    terms [.name]
               |  }
               |}""".stripMargin
        )

        val res1 = api.upload(
          "/schema/1/update?force=true&staged=true",
          stagedFiles,
          db.adminKey)
        res1 should respond(OK)

        // Wait for the schema cache.
        eventually(timeout(5.seconds)) {
          status(db) shouldBe "pending"
        }

        // A staged index is built in the background, so the status should be pending
        // initially.
        pendingSummary(db) shouldBe (
          """|* Collection User:
             |  ~ index byName: building
             |""".stripMargin
        )
        stagedDiff(db, "summary") shouldBe (
          """|* Modifying collection `User` at foo.fsl:1:1
             |""".stripMargin
        )

        // Wait for it to be ready.
        eventually(timeout(1.minute)) {
          status(db) shouldBe "ready"
        }

        pendingSummary(db) shouldBe ""
        stagedDiff(db, "summary") shouldBe (
          """|* Modifying collection `User` at foo.fsl:1:1
             |""".stripMargin
        )

        val res2 =
          api.upload(s"/schema/1/commit?force=true", files, db.adminKey)
        res2 should respond(OK)

        // Staged schema should now be active, and the new index should be usable.
        eventually(timeout(5.seconds)) {
          status(db) shouldBe "none"
        }
        queryOk("if (User.byName('user-0').count() != 1) abort(0)", db.adminKey)
      }
    }

    once("disallows committing during an async build") {
      for {
        db <- aDatabase
      } {
        val files = Seq(
          "foo.fsl" ->
            """|collection User {
               |  name: String
               |}""".stripMargin
        )

        val res0 = api.upload("/schema/1/update?force=true", files, db.adminKey)
        res0 should respond(OK)

        // Nothing has happened yet.
        status(db) shouldBe "none"

        queryOk(
          """|Set.sequence(0, 129).forEach(i => {
             |  User.create({ name: "user-#{i}" })
             |})""".stripMargin,
          db.adminKey)

        val stagedFiles = Seq(
          "foo.fsl" ->
            """|collection User {
               |  name: String
               |
               |  index byName {
               |    terms [.name]
               |  }
               |}""".stripMargin
        )

        val res1 = api.upload(
          "/schema/1/update?force=true&staged=true",
          stagedFiles,
          db.adminKey)
        res1 should respond(OK)

        // Wait for the schema cache.
        eventually(timeout(5.seconds)) {
          status(db) shouldBe "pending"
        }

        val res2 =
          api.upload(s"/schema/1/commit?force=true", files, db.adminKey)
        res2 should respond(BadRequest)
        (res2.json / "error" / "message").as[String] shouldBe (
          "Cannot commit schema before it is in the `ready` state. Check schema status for details."
        )
      }
    }

    once("colors work") {
      for {
        db <- aDatabase
      } {
        val files = Seq(
          "foo.fsl" ->
            """|collection User {
               |  name: String
               |}""".stripMargin
        )

        val res0 = api.upload("/schema/1/update?force=true", files, db.adminKey)
        res0 should respond(OK)

        // Nothing has happened yet.
        status(db) shouldBe "none"

        queryOk("User.create({ name: 'foo' })", db.adminKey)

        val stagedFiles = Seq(
          "foo.fsl" ->
            """|collection User {
               |  name: String
               |
               |  index byName {
               |    terms [.name]
               |  }
               |}""".stripMargin
        )

        val res1 = api.upload(
          "/schema/1/update?force=true&staged=true",
          stagedFiles,
          db.adminKey)
        res1 should respond(OK)

        val reset = "\u001b[0m"
        val green = "\u001b[32m"
        val boldBlue = "\u001b[1;34m"

        val status0 =
          api.get(s"/schema/1/status?diff=semantic&color=ansi", db.adminKey)
        status0 should respond(OK)
        (status0.json / "diff").as[String] shouldBe (
          s"""|${boldBlue}* Modifying collection `User`${reset} at foo.fsl:1:1:
              |  * Indexes:
              |${green}  + add index `byName`${reset}
              |
              |""".stripMargin
        )
      }
    }

    once("returns an error for an invalid staged schema") {
      for {
        db <- aDatabase
      } {
        val res0 = api.upload(
          "/schema/1/update",
          Seq("main.fsl" -> "collection Foo {}"),
          db.adminKey)
        res0 should respond(OK)

        val res1 = api.upload(
          "/schema/1/update?staged=true",
          Seq("main.fsl" -> "collection Foo { name: String }"),
          db.adminKey)
        res1 should respond(OK)

        queryOk("Foo.create({ id: 0, foo: 3 })", db.adminKey)

        val status0 = api.get(s"/schema/1/status?diff=semantic", db.adminKey)
        status0 should respond(BadRequest)

        (status0.json / "error" / "message").as[String] shouldBe (
          """|error: Field `.name` is not present in the live schema
             |at main.fsl:1:18
             |  |
             |1 | collection Foo { name: String }
             |  |                  ^^^^
             |  |
             |hint: Provide an `add` migration for this field
             |  |
             |1 |   collection Foo { migrations {
             |  |  __________________+
             |2 | |     add .name
             |3 | |     backfill .name = <expr>
             |4 | |   }
             |  | |____^
             |  |
             |hint: Add a default value to this field
             |  |
             |1 | collection Foo { name: String = <expr> }
             |  |                              +++++++++
             |  |
             |hint: Make the field nullable
             |  |
             |1 | collection Foo { name: String? }
             |  |                              +
             |  |
             |error: Missing wildcard definition
             |at main.fsl:1:1
             |  |
             |1 | collection Foo { name: String }
             |  | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
             |  |
             |hint: Collections with no fields defined allow arbitrary fields by default. To keep this behavior, add a wildcard declaration to the schema definition
             |  |
             |1 | collection Foo { *: Any
             |  |                  +++++++
             |  |""".stripMargin
        )
      }
    }
  }

  "schema/1/files" / {
    once("shows active and staged filenames") {
      for {
        db <- aDatabase
      } {
        val files = Seq(
          "foo.fsl" -> "function foo() { 1 }"
        )

        val res0 = api.upload("/schema/1/update?force=true", files, db.adminKey)
        res0 should respond(OK)

        // Note that the file got renamed.
        val stagedFiles = Seq(
          "bar.fsl" -> "function foo() { 2 }"
        )

        val res1 = api.upload(
          "/schema/1/update?force=true&staged=true",
          stagedFiles,
          db.adminKey)
        res1 should respond(OK)

        val res2 = api.get("/schema/1/files", db.adminKey)
        (res2.json / "files" / 0 / "filename").as[String] shouldBe "foo.fsl"

        val res3 = api.get("/schema/1/files?staged=true", db.adminKey)
        (res3.json / "files" / 0 / "filename").as[String] shouldBe "bar.fsl"
      }
    }
  }

  "schema/1/files/{filename}" / {
    once("shows active and staged file content") {
      for {
        db <- aDatabase
      } {
        val files = Seq(
          "foo.fsl" -> "function foo() { 1 }"
        )

        val res0 = api.upload("/schema/1/update?force=true", files, db.adminKey)
        res0 should respond(OK)

        val stagedFiles = Seq(
          "bar.fsl" -> "function foo() { 2 }"
        )

        val res1 = api.upload(
          "/schema/1/update?force=true&staged=true",
          stagedFiles,
          db.adminKey)
        res1 should respond(OK)

        val res2 = api.get("/schema/1/files/foo.fsl", db.adminKey)
        (res2.json / "content").as[String] shouldBe "function foo() { 1 }"

        val res3 = api.get("/schema/1/files/bar.fsl?staged=true", db.adminKey)
        (res3.json / "content").as[String] shouldBe "function foo() { 2 }"
      }
    }
  }
}
