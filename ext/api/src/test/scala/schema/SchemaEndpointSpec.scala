package fauna.api.test

import fauna.codex.json.{ JSObject, JSValue }
import fauna.model.schema.fsl.SourceGenerator
import fauna.net.http.{ AuthType, Body, ContentType, HttpResponse }
import fauna.prop.api.Database
import fauna.prop.Prop
import io.netty.util.AsciiString
import java.util.UUID
import scala.concurrent.Future

class SchemaEndpointSpec extends FQL2APISpec {

  // FSL File API

  "schema/1/files" / {
    once("initializes and returns source files") {
      for {
        db <- aDatabase
        _  <- aCollection(db)
      } {
        val res = api.get("/schema/1/files", db.adminKey)
        res should respond(OK)

        val version = (res.json / "version").as[Long]
        version should be >= 0L

        val files = (res.json / "files").as[Seq[JSObject]]
        files should have size 1
        files.head should matchJSON(
          JSObject(
            "filename" -> "main.fsl"
          ))
      }
    }
  }

  "schema/1/files/{filename}" / {
    once("returns a single source file") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
      } {
        val res = api.get("/schema/1/files/main.fsl", db.adminKey)
        res should respond(OK)
        (res.json / "version").as[Long] should be >= 0L
        (res.json / "content").as[String] should (
          include(SourceGenerator.Preamble) and include(s"${coll}")
        )
      }
    }

    once("returns 404 on unknown file") {
      for {
        db <- aDatabase
      } {
        val res = api.get("/schema/1/files/foo.fsl", db.adminKey)
        res should respond(NotFound)
      }
    }
  }

  "schema/1/update" / {

    def files =
      Seq(
        "foo.fsl" -> "function double(x) { x * 2 }",
        "bar.fsl" -> "function id(x) { x }"
      )

    def assertUpdated(db: Database, res: Future[HttpResponse]) = {
      val newVS = (res.json / "version").as[Long]
      val files = api.get(s"/schema/1/files?version=$newVS", db.adminKey)
      files should respond(OK)
      files.json should matchJSON(
        JSObject(
          "version" -> newVS,
          "files" -> Seq(
            JSObject("filename" -> "foo.fsl"),
            JSObject("filename" -> "bar.fsl")
          )))

      val foo = api.get("/schema/1/files/foo.fsl", db.adminKey)
      foo should respond(OK)
      foo.json should matchJSON(
        JSObject(
          "version" -> res.json / "version",
          "content" -> "function double(x) { x * 2 }"
        ))

      val bar = api.get("/schema/1/files/bar.fsl", db.adminKey)
      bar should respond(OK)
      bar.json should matchJSON(
        JSObject(
          "version" -> res.json / "version",
          "content" -> "function id(x) { x }"
        ))
    }

    once("invalid projection") {
      for {
        db <- aTypecheckedDatabase
      } {
        val files = Seq(
          "foo.fsl" ->
            """|function getRoomFieldsForRoom(room) {
               |  room {
               |    _id: .id,
               |    roomTitle,
               |    status
               |  }
               |}
               |""".stripMargin
        )
        val res = api.upload("/schema/1/update", files, db.adminKey)
        res should respond(BadRequest)
        (res.json / "error" / "message")
          .as[String] shouldEqual
          """|error: Invalid projection on a unknown type. Use function signatures to fix the issue.
             |at foo.fsl:2:3
             |  |
             |2 |     room {
             |  |  ___^
             |3 | |     _id: .id,
             |4 | |     roomTitle,
             |5 | |     status
             |6 | |   }
             |  | |___^
             |  |""".stripMargin
      }
    }

    once("invalid projection (multiple errors)") {
      for {
        db <- aTypecheckedDatabase
      } {
        val files = Seq(
          "foo.fsl" ->
            """|function getRoomFieldsForRoom(room) {
               |  let room2 = room { id, roomTitle, status }
               |  room2 {
               |    _id: .id,
               |    roomTitle,
               |    status
               |  }
               |}
               |""".stripMargin
        )
        val res = api.upload("/schema/1/update", files, db.adminKey)
        res should respond(BadRequest)
        (res.json / "error" / "message")
          .as[String] shouldEqual
          """|error: Invalid projection on a unknown type. Use function signatures to fix the issue.
             |at foo.fsl:2:15
             |  |
             |2 |   let room2 = room { id, roomTitle, status }
             |  |               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
             |  |
             |error: Invalid projection on a unknown type. Use function signatures to fix the issue.
             |at foo.fsl:3:3
             |  |
             |3 |     room2 {
             |  |  ___^
             |4 | |     _id: .id,
             |5 | |     roomTitle,
             |6 | |     status
             |7 | |   }
             |  | |___^
             |  |""".stripMargin
      }
    }

    once("force update the db schema") {
      for {
        db <- aDatabase
      } {
        val res = api.upload("/schema/1/update", files, db.adminKey)
        res should respond(OK)
        assertUpdated(db, res)
      }
    }

    once("validate schema version") {
      for {
        db <- aDatabase
        cur = api.get("/schema/1/files", db.adminKey)
        version = (cur.json / "version").as[Long]
        wrongVs <- Prop.long
        if wrongVs != version
      } {
        val wrong =
          api.upload(s"/schema/1/update?version=$wrongVs", files, db.adminKey)
        wrong should respond(Conflict)

        val right =
          api.upload(s"/schema/1/update?version=$version", files, db.adminKey)
        right should respond(OK)
        assertUpdated(db, right)
      }
    }

    once("validate schema consistency") {
      for {
        db <- aDatabase
      } {
        val res =
          api.upload(
            "/schema/1/update",
            Seq("main.fsl" -> "@role('a_role') function foo() { 42 }"),
            db.adminKey
          )
        res should respond(BadRequest)
        (res.json / "error" / "message").as[String] shouldEqual
          """|Failed to create Function.
             |constraint failures:
             |  role: Field refers to an unknown role name `a_role`.""".stripMargin
      }
    }

    once("validate the number of files") {
      for {
        db <- aDatabase
      } {
        val files = (0 until 1025).map(i => s"foo$i.fsl" -> "function foo() { 42 }")

        val res = api.upload("/schema/1/update", files, db.adminKey)
        res should respond(BadRequest)
        (res.json / "error" / "message").as[String] shouldEqual "Too many files"
      }
    }

    once("validate the number of bytes in a file") {
      for {
        db <- aDatabase
      } {
        // 8 bytes per line, so repeat 1 million times to get 8mb.
        val files = Seq("main.fsl" -> "// 1234\n".repeat(1024 * 1024))

        val res = api.upload("/schema/1/update", files, db.adminKey)
        // FIXME: This should be valid.
        res should respond(RequestEntityTooLarge)
        (res.json / "error" / "message")
          .as[String] shouldEqual "Request entity is too large"
      }
    }

    once("allow exactly the right number of files") {
      for {
        db <- aDatabase
      } {
        // 1023 normal files + 1 main.fsl file.
        val allowedOtherFiles =
          (0 until 1023).map(i => s"foo$i.fsl" -> "// hi")
        val tooManyOtherFiles =
          (0 until 1024).map(i => s"foo$i.fsl" -> "// hi")
        val allowedWithMainFiles =
          allowedOtherFiles ++ Seq("main.fsl" -> "// hi")
        val tooManyWithMainFiles =
          tooManyOtherFiles ++ Seq("main.fsl" -> "// hi")

        val res1 = api.upload("/schema/1/update", allowedOtherFiles, db.adminKey)
        res1 should respond(OK)

        val res2 = api.upload("/schema/1/update", tooManyOtherFiles, db.adminKey)
        res2 should respond(BadRequest)
        (res2.json / "error" / "message").as[String] shouldEqual "Too many files"

        val res3 = api.upload("/schema/1/update", allowedWithMainFiles, db.adminKey)
        res3 should respond(OK)

        val res4 = api.upload("/schema/1/update", tooManyWithMainFiles, db.adminKey)
        res4 should respond(BadRequest)
        (res4.json / "error" / "message").as[String] shouldEqual "Too many files"
      }
    }

    once("return errors") {
      for {
        db <- aDatabase
      } {
        val res =
          api.upload(
            "/schema/1/update",
            Seq(
              "main.fsl" -> "function foo {}",
              "foo.fsl" -> "bar?"
            ),
            db.adminKey
          )
        res should respond(BadRequest)
        (res.json / "error" / "message").as[String] shouldEqual
          """|error: Expected `(`
             |at main.fsl:1:14
             |  |
             |1 | function foo {}
             |  |              ^
             |  |
             |error: Invalid schema item `bar`
             |at foo.fsl:1:1
             |  |
             |1 | bar?
             |  | ^^^
             |  |""".stripMargin
      }
    }

    once("rejects duplicates") {
      for {
        db <- aDatabase
      } {
        val res0 =
          api.upload(
            "/schema/1/update",
            Seq(
              "main.fsl" -> "role foo { }",
              "foo.fsl" -> "role foo { }"
            ),
            db.adminKey
          )
        res0 should respond(BadRequest)
        (res0.json / "error" / "message")
          .as[String] shouldEqual "Duplicate role `foo` in foo.fsl (originally seen in main.fsl)"

        val res1 =
          api.upload(
            "/schema/1/update",
            Seq(
              "main.fsl" -> "function foo(x) { x }",
              "foo.fsl" -> "collection foo { }"
            ),
            db.adminKey
          )
        res1 should respond(BadRequest)
        (res1.json / "error" / "message")
          .as[String] shouldEqual "Duplicate collection or function `foo` in foo.fsl (originally seen in main.fsl)"
      }
    }

    once("allow collections when computed fields flag is disabled") {
      for {
        accountDb <- anAccountDatabase
      } {
        val key = queryOk(
          s"""|Database.create({
              |  name: "Foo",
              |})
              |
              |Key.create({ role: "admin", database: "Foo" })
              |""".stripMargin,
          accountDb.adminKey
        )

        val adminKey = (key / "secret").as[String]

        val res0 =
          api.upload(
            "/schema/1/update",
            Seq("main.fsl" -> "collection Foo {}"),
            adminKey
          )
        res0 should respond(OK)
      }
    }

    once("unique constraints work when check constraints flag is disabled") {
      for {
        // Flag disabled by default except for account 0.
        accountDb <- anAccountDatabase
      } {
        val key = queryOk(
          s"""|Database.create({
              |  name: "Foo",
              |})
              |
              |Key.create({ role: "admin", database: "Foo" })
              |""".stripMargin,
          accountDb.adminKey
        )

        api.upload(
          "/schema/1/update",
          Seq("main.fsl" -> "collection Foo { unique [.foo] }"),
          (key / "secret").as[String]
        ) should respond(OK)
      }
    }

    once("handle invalid filenames") {
      for {
        db <- aDatabase
      } {
        val res1 = api.upload("/schema/1/update", Seq("foo" -> "bar"), db.adminKey)
        res1 should respond(BadRequest)
        (res1.json / "error" / "message")
          .as[String] shouldBe "Invalid filename `foo`"

        val res2 =
          api.upload("/schema/1/update", Seq("*foo.fsl" -> "bar"), db.adminKey)
        res2 should respond(BadRequest)
        (res2.json / "error" / "message")
          .as[String] shouldBe "Invalid filename `*foo.fsl`"
      }
    }
  }

  "schema/1/validate" / {

    val proposed =
      Seq(
        "foo.fsl" -> "function double(x) { x + x }",
        "bar.fsl" -> "function incr(x) { x + 1 }"
      )

    val existing =
      Seq(
        "foo.fsl" -> "function double(x) { x * 2 }",
        "baaz.fsl" -> "function id(x) { x }"
      )

    once("returns diffs") {
      for {
        db       <- aDatabase
        endpoint <- Seq("validate", "diff")
      } {
        // Setup schema.
        val eRes = api.upload("/schema/1/update", existing, db.adminKey)
        eRes should respond(OK)

        // Get the right diff without override (non-default behavior).
        val diff =
          """|* Adding function `incr` to bar.fsl:1:1
             |
             |* Modifying function `double` at foo.fsl:1:1:
             |  ~ change body
             |
             |""".stripMargin

        val pRes =
          api.upload(s"/schema/1/$endpoint?override=false", proposed, db.adminKey)
        pRes should respond(OK)
        (pRes.json / "diff").as[String] shouldEqual diff

        // Get the right diff with override (default behavior).
        val ovDiff =
          """|* Adding function `incr` to bar.fsl:1:1
             |
             |* Removing function `id` from baaz.fsl:1:1
             |
             |* Modifying function `double` at foo.fsl:1:1:
             |  ~ change body
             |
             |""".stripMargin

        val ovRes =
          api.upload(s"/schema/1/$endpoint", proposed, db.adminKey)
        ovRes should respond(OK)
        (ovRes.json / "diff").as[String] shouldEqual ovDiff
      }
    }

    once("returns text diffs") {
      for {
        db <- aDatabase
      } {
        // Setup schema.
        val eRes = api.upload("/schema/1/update", existing, db.adminKey)
        eRes should respond(OK)

        val pRes =
          api.upload(
            s"/schema/1/validate?override=false&diff=textual",
            proposed,
            db.adminKey)
        pRes should respond(OK)
        (pRes.json / "diff").as[String] shouldEqual (
          """|bar.fsl
             |@ line 1 to 1
             |+ function incr(x) { x + 1 }
             |
             |foo.fsl
             |@ line 1 to 1
             |- function double(x) { x * 2 }
             |+ function double(x) { x + x }
             |""".stripMargin
        )

        val ovRes =
          api.upload(s"/schema/1/validate?diff=textual", proposed, db.adminKey)
        ovRes should respond(OK)
        (ovRes.json / "diff").as[String] shouldEqual (
          """|baaz.fsl
             |@ line 1 to 1
             |- function id(x) { x }
             |
             |bar.fsl
             |@ line 1 to 1
             |+ function incr(x) { x + 1 }
             |
             |foo.fsl
             |@ line 1 to 1
             |- function double(x) { x * 2 }
             |+ function double(x) { x + x }
             |""".stripMargin
        )
      }
    }

    once("errors for stale schema versions") {
      for {
        db <- aDatabase
      } {
        // Setup a schema, so that the expected `version` is set correctly.
        val eRes = api.upload("/schema/1/update", existing, db.adminKey)
        eRes should respond(OK)

        val res = api.upload(
          "/schema/1/diff?version=3",
          Seq("main.fsl" -> "function foo() { 0 }"),
          db.adminKey)
        res should respond(Conflict)
        (res.json / "error" / "message").as[String] shouldBe (
          "Stale schema version provided"
        )
      }
    }

    once("return errors") {
      for {
        db <- aDatabase
      } {
        val res =
          api.upload(
            "/schema/1/validate",
            Seq(
              "main.fsl" -> "function foo {}",
              "foo.fsl" -> "bar?"
            ),
            db.adminKey
          )
        res should respond(BadRequest)
        (res.json / "error" / "message").as[String] shouldEqual
          """|error: Expected `(`
             |at main.fsl:1:14
             |  |
             |1 | function foo {}
             |  |              ^
             |  |
             |error: Invalid schema item `bar`
             |at foo.fsl:1:1
             |  |
             |1 | bar?
             |  | ^^^
             |  |""".stripMargin
      }
    }

    once("rejects duplicates") {
      for {
        db <- aDatabase
      } {
        val res0 =
          api.upload(
            "/schema/1/validate",
            Seq(
              "main.fsl" -> "role foo { }",
              "foo.fsl" -> "role foo { }"
            ),
            db.adminKey
          )
        res0 should respond(BadRequest)
        (res0.json / "error" / "message")
          .as[String] shouldEqual "Duplicate role `foo` in foo.fsl (originally seen in main.fsl)"

        val res1 =
          api.upload(
            "/schema/1/validate",
            Seq(
              "main.fsl" -> "collection foo { }",
              "foo.fsl" -> "function foo(x) { x + 1 }"
            ),
            db.adminKey
          )
        res1 should respond(BadRequest)
        (res1.json / "error" / "message")
          .as[String] shouldEqual "Duplicate collection or function `foo` in foo.fsl (originally seen in main.fsl)"
      }
    }

    once("colors work") {
      for {
        db <- aDatabase
      } {
        val res = api.upload(
          "/schema/1/validate?color=ansi",
          Seq("main.fsl" -> "collection Foo { index byName { terms [.name] } }"),
          db.adminKey)
        res should respond(OK)

        val reset = "\u001b[0m"
        val boldBlue = "\u001b[1;34m"
        val green = "\u001b[32m"

        (res.json / "diff").as[String] shouldEqual (
          s"""|${boldBlue}* Adding collection `Foo`${reset} to main.fsl:1:1:
              |  * Indexes:
              |${green}  + add index `byName`${reset}
              |
              |""".stripMargin
        )
      }
    }
  }

  "schema/1/diff" / {
    once("handles invalid request body") {
      def uploadMalformedMultipart(
        path: String,
        files: Seq[(String, String)],
        token: AuthType,
        query: String = null,
        headers: Seq[(AsciiString, AnyRef)] = Seq.empty): Future[HttpResponse] = {

        val boundary = UUID.randomUUID.toString
        val content = new StringBuilder

        files foreach { case (name, fsl) =>
          content.append(
            s"""|
              |--$boundary
                |Content-Disposition: form-data; name="$name"
                |Content-Type: application/octet-stream
                |
                |$fsl""".stripMargin
          )
        }

        content.append(
          s"""|
        |--${boundary}--""".stripMargin
        )

        /** We omit the boundary here to test how we handle malformed requests in this way.
          */
        val contentType = s"${ContentType.FormData};"
        val body = Body(content.result(), contentType)
        api.post(path, body, token, query, headers)
      }

      for {
        db <- aDatabase
      } {
        val res = uploadMalformedMultipart(
          "/schema/1/diff",
          Seq("main.fsl" -> "collection Foo { index byName { terms [.name] } }"),
          db.adminKey)
        res should respond(BadRequest)
        (res.json / "error" / "message")
          .as[String] shouldEqual "Request is not a valid multipart request"
      }
    }
  }

  // Single Schema Item API

  val aResource =
    for {
      tpe <- Prop.choose("collections", "functions", "roles", "access_providers")
      _ = info(s"Testing $tpe")
    } yield tpe

  def anFSLFor(db: Database, tpe: String, nameP: Prop[String] = aUniqueIdentifier) =
    tpe match {
      case "collections" =>
        for {
          name <- nameP
        } yield {
          s"""|collection $name {
              |  index byFoo {
              |    terms [.foo]
              |  }
              |  unique [.bar]
              |  compute a = ((doc) => doc.x + 1)
              |  compute b = (_ => 0)
              |}""".stripMargin
        }

      case "functions" =>
        for {
          name <- nameP
        } yield {
          s"function $name() { 42 }"
        }

      case "roles" =>
        for {
          coll <- aCollection(db)
          name <- nameP
        } yield {
          s"""|role $name {
              |  privileges ${coll} {
              |    read {
              |      predicate ((x) => true)
              |    }
              |  }
              |}""".stripMargin
        }

      case "access_providers" =>
        for {
          name <- nameP
        } yield {
          s"""|access provider $name {
              |  issuer "https://fauna0.auth0.com"
              |  jwks_uri "https://fauna.auth0.com/.well-known/jwks.json"
              |}""".stripMargin
        }

      case other =>
        fail(s"unsupported schema item: $other")
    }

  def anInvalidFSLFor(tpe: String) = {
    for {
      name <- anIdentifier
    } yield {
      tpe match {
        case "collections"      => s"collection \\$name {}"
        case "functions"        => s"function $name {}"
        case "roles"            => s"role \\$name {}"
        case "access_providers" => s"access provider $name {}"
      }
    }
  }

  def assertDefinition(tpe: String, json: JSValue) = {
    val name = (json / "definition" / "name").asOpt[String]
    val coll = (json / "definition" / "coll").as[String]
    name shouldNot be(empty)

    tpe match {
      case "collections"      => coll shouldEqual "Collection"
      case "functions"        => coll shouldEqual "Function"
      case "roles"            => coll shouldEqual "Role"
      case "access_providers" => coll shouldEqual "AccessProvider"
    }
  }

  "schema/1/items/{resource}" / {
    prop("POST: creates a single schema item") {
      for {
        db  <- aDatabase
        tpe <- aResource
        fsl <- anFSLFor(db, tpe)
      } withClue(fsl) {
        val res0 =
          api.post(s"/schema/1/items/$tpe?validate=true", fsl, db.adminKey)
        res0 should respond(OK)
        (res0.json / "version").as[Long] should be >= 0L
        (res0.json / "diff").as[String].length should be > 0

        val res1 = api.post(s"/schema/1/items/$tpe", fsl, db.adminKey)
        res1 should respond(OK)
        (res1.json / "version").as[Long] should be >= 0L
        (res1.json / "filename").as[String] shouldEqual "main.fsl"
        (res1.json / "content").as[String] shouldEqual fsl
        (res1.json / "span") should matchJSONPattern(
          "start" -> "\\d+",
          "end" -> "\\d+"
        )
        assertDefinition(tpe, res1.json)
      }
    }

    prop("rejects builtin roles") {
      for {
        db <- aDatabase
      } {
        val tpe = "access_providers"
        val fsl = (
          """|access provider Foo {
             |  issuer "https://fauna0.auth0.com"
             |  jwks_uri "https://fauna.auth0.com/.well-known/jwks.json"
             |  role admin
             |  role server
             |  role "server-readonly"
             |  role client
             |}""".stripMargin
        )

        val res = api.post(s"/schema/1/items/$tpe", fsl, db.adminKey)
        res should respond(BadRequest)
        (res.json / "error" / "message").as[String] shouldEqual
          """|error: Builtin role `admin` is not allowed
             |at main.fsl:6:8
             |  |
             |6 |   role admin
             |  |        ^^^^^
             |  |
             |error: Builtin role `server` is not allowed
             |at main.fsl:7:8
             |  |
             |7 |   role server
             |  |        ^^^^^^
             |  |
             |error: Builtin role `server-readonly` is not allowed
             |at main.fsl:8:8
             |  |
             |8 |   role "server-readonly"
             |  |        ^^^^^^^^^^^^^^^^^
             |  |
             |error: Builtin role `client` is not allowed
             |at main.fsl:9:8
             |  |
             |9 |   role client
             |  |        ^^^^^^
             |  |""".stripMargin
      }
    }

    prop("POST: validates schema version") {
      for {
        db  <- aDatabase
        tpe <- aResource
        fsl <- anFSLFor(db, tpe)
        cur = api.get("/schema/1/files", db.adminKey)
        version = (cur.json / "version").as[Long]
        wrongVs <- Prop.long
        if wrongVs != version
      } withClue(fsl) {
        val wrong = api.post(
          s"/schema/1/items/$tpe?version=$wrongVs",
          fsl,
          db.adminKey
        )
        wrong should respond(Conflict)

        val rightValidate =
          api.post(s"/schema/1/items/$tpe?validate=true", fsl, db.adminKey)
        rightValidate should respond(OK)

        val right = api.post(
          s"/schema/1/items/$tpe?version=$version",
          fsl,
          db.adminKey
        )
        right should respond(OK)
      }
    }

    prop("POST: requires a single fsl snippet") {
      for {
        db   <- aDatabase
        tpe  <- aResource
        fsl0 <- anFSLFor(db, tpe)
        fsl1 <- anFSLFor(db, tpe)
      } withClue(s"fsl0=$fsl0 fsl1=$fsl1") {
        def checkErr(body: String, err: String) = {
          val res0 =
            api.post(s"/schema/1/items/$tpe?validate=true", body, db.adminKey)
          res0 should respond(BadRequest)
          (res0.json / "error" / "message").as[String] shouldEqual err

          val res1 = api.post(s"/schema/1/items/$tpe", body, db.adminKey)
          res1 should respond(BadRequest)
          (res1.json / "error" / "message").as[String] shouldEqual err
        }

        checkErr("", "No FSL snippet provided")
        checkErr("  ", "No FSL snippet provided")
        checkErr(
          s"""|$fsl0
              |$fsl1
              |""".stripMargin,
          "Expected a single schema item, got: 2"
        )
      }
    }

    prop("POST: rejects wrong schema type") {
      for {
        db   <- aDatabase
        tpe0 <- aResource
        tpe1 <- aResource
        if tpe0 != tpe1
        fsl <- anFSLFor(db, tpe1)
      } withClue(fsl) {
        val res0 =
          api.post(s"/schema/1/items/$tpe0?validate=true", fsl, db.adminKey)
        res0 should respond(BadRequest)
        (res0.json / "error" / "message").as[String] should
          include("Wrong FSL snippet.")

        val res1 = api.post(s"/schema/1/items/$tpe0", fsl, db.adminKey)
        res1 should respond(BadRequest)
        (res1.json / "error" / "message").as[String] should
          include("Wrong FSL snippet.")
      }
    }

    prop("POST: reject duplicates") {
      for {
        db  <- aDatabase
        tpe <- aResource
        fsl <- anFSLFor(db, tpe)
      } withClue(fsl) {
        api.post(s"/schema/1/items/$tpe?validate=true", fsl, db.adminKey) should
          respond(OK)

        api.post(s"/schema/1/items/$tpe", fsl, db.adminKey) should
          respond(OK)

        val res0 =
          api.post(s"/schema/1/items/$tpe?validate=true", fsl, db.adminKey)
        res0 should respond(BadRequest)
        (res0.json / "error" / "message").as[String] should
          include("error: Duplicate")

        val res1 = api.post(s"/schema/1/items/$tpe", fsl, db.adminKey)
        res1 should respond(BadRequest)
        (res1.json / "error" / "message").as[String] should
          include("error: Duplicate")
      }
    }

    prop("POST: return errors") {
      for {
        db  <- aDatabase
        tpe <- aResource
        fsl <- anInvalidFSLFor(tpe)
      } withClue(fsl) {
        val res0 =
          api.post(s"/schema/1/items/$tpe?validate=true", fsl, db.adminKey)
        res0 should respond(BadRequest)
        (res0.json / "error" / "message").as[String] should include("error:")

        val res1 = api.post(s"/schema/1/items/$tpe", fsl, db.adminKey)
        res1 should respond(BadRequest)
        (res1.json / "error" / "message").as[String] should include("error:")
      }
    }
  }

  "schema/1/items/{resource}/{name}" / {
    prop("GET: returns a single function fsl") {
      for {
        db   <- aDatabase
        tpe  <- aResource
        name <- anIdentifier
        fsl  <- anFSLFor(db, tpe, Prop.const(name))
        // setup
        files = Seq("test.fsl" -> fsl)
        setup = api.upload(
          "/schema/1/update?override=false",
          files,
          db.adminKey
        )
        _ = setup should respond(OK)
        vs = (setup.json / "version").as[Long]
      } withClue(fsl) {
        val res = api.get(s"/schema/1/items/$tpe/$name?version=$vs", db.adminKey)
        res should respond(OK)
        (res.json / "version").as[Long] shouldBe vs
        (res.json / "filename").as[String] shouldEqual "test.fsl"
        (res.json / "content").as[String] shouldEqual fsl
        (res.json / "span") should matchJSONPattern(
          "start" -> "\\d+",
          "end" -> "\\d+"
        )
        assertDefinition(tpe, res.json)
      }
    }

    prop("GET: returns 404 on unknown item") {
      for {
        db   <- aDatabase
        tpe  <- aResource
        name <- anIdentifier
      } {
        val get = api.get(s"/schema/1/items/$tpe/$name", db.adminKey)
        get should respond(NotFound)
      }
    }

    prop("GET: validates schema version if provided") {
      for {
        db   <- aDatabase
        tpe  <- aResource
        name <- anIdentifier
        fsl  <- anFSLFor(db, tpe, Prop.const(name))
        setup = api.upload(
          "/schema/1/update?override=false",
          Seq("test.fsl" -> fsl),
          db.adminKey
        )
        _ = setup should respond(OK)
        version = (setup.json / "version").as[Long]
        wrongVs <- Prop.long
        if wrongVs != version
      } withClue(fsl) {
        val wrong = api.get(
          s"/schema/1/items/$tpe/$name?version=$wrongVs",
          db.adminKey
        )
        wrong should respond(Conflict)

        val right = api.get(
          s"/schema/1/items/$tpe/$name?version=$version",
          db.adminKey
        )
        right should respond(OK)
      }
    }

    prop("POST: updates a single schema item") {
      for {
        db   <- aDatabase
        tpe  <- aResource
        name <- anIdentifier
        fsl0 <- anFSLFor(db, tpe, Prop.const(name))
        fsl1 <- anFSLFor(db, tpe, Prop.const(name))
        if fsl0 != fsl1
        setup = api.upload(
          "/schema/1/update?override=false",
          Seq("test.fsl" -> fsl0),
          db.adminKey
        )
        _ = setup should respond(OK)
        vs = (setup.json / "version").as[Long]
      } withClue(s"fsl0=$fsl0 fsl1=$fsl1") {
        val res0 =
          api.post(s"/schema/1/items/$tpe/$name?validate=true", fsl1, db.adminKey)
        res0 should respond(OK)
        (res0.json / "version").as[Long] should be >= vs
        (res0.json / "diff").as[String].length should be > 0

        val res1 =
          api.post(s"/schema/1/items/$tpe/$name", fsl1, db.adminKey)
        res1 should respond(OK)
        (res1.json / "version").as[Long] should be >= vs
        (res1.json / "filename").as[String] shouldEqual "test.fsl"
        (res1.json / "content").as[String] shouldEqual fsl1
        (res1.json / "span") should matchJSONPattern(
          "start" -> "\\d+",
          "end" -> "\\d+"
        )
        assertDefinition(tpe, res1.json)
      }
    }

    prop("POST: updates a single schema item when there're multiple files") {
      for {
        db           <- aDatabase
        existing     <- aResource
        existingName <- anIdentifier
        existingFSL  <- anFSLFor(db, existing, Prop.const(existingName))
        tpe          <- aResource
        name         <- anIdentifier
        origFSL      <- anFSLFor(db, tpe, Prop.const(name))
        newFSL       <- anFSLFor(db, tpe, Prop.const(name))
        if origFSL != newFSL
        setup = api.upload(
          "/schema/1/update?override=false",
          Seq("existing.fsl" -> existingFSL, "test.fsl" -> origFSL),
          db.adminKey
        )
        _ = setup should respond(OK)
        vs = (setup.json / "version").as[Long]
      } withClue(s"existing=$existingFSL origFSL=$origFSL newFSL=$newFSL") {
        val res0 =
          api.post(s"/schema/1/items/$tpe/$name?validate=true", newFSL, db.adminKey)
        res0 should respond(OK)
        (res0.json / "version").as[Long] should be >= vs
        (res0.json / "diff").as[String].length should be > 0

        val res1 =
          api.post(s"/schema/1/items/$tpe/$name", newFSL, db.adminKey)
        res1 should respond(OK)
        (res1.json / "version").as[Long] should be >= vs
        (res1.json / "filename").as[String] shouldEqual "test.fsl"
        (res1.json / "content").as[String] shouldEqual newFSL
        (res1.json / "span") should matchJSONPattern(
          "start" -> "\\d+",
          "end" -> "\\d+"
        )
        assertDefinition(tpe, res1.json)
      }
    }

    prop("POST: renames a single schema item") {
      for {
        db    <- aDatabase
        tpe   <- aResource
        name0 <- anIdentifier
        name1 <- anIdentifier
        fsl0  <- anFSLFor(db, tpe, Prop.const(name0))
        fsl1  <- anFSLFor(db, tpe, Prop.const(name1))
        setup = api.upload(
          "/schema/1/update?override=false",
          Seq("test.fsl" -> fsl0),
          db.adminKey
        )
        _ = setup should respond(OK)
        vs = (setup.json / "version").as[Long]
      } withClue(s"fsl0=$fsl0 fsl1=$fsl1") {
        val res0 =
          api.post(s"/schema/1/items/$tpe/$name0?validate=true", fsl1, db.adminKey)
        res0 should respond(OK)
        (res0.json / "version").as[Long] should be >= vs
        (res0.json / "diff").as[String].length should be > 0

        val res1 =
          api.post(s"/schema/1/items/$tpe/$name0", fsl1, db.adminKey)
        res1 should respond(OK)
        (res1.json / "definition" / "name").as[String] shouldEqual name1
      }
    }

    prop("POST: requires an existing schema item") {
      for {
        db   <- aDatabase
        tpe  <- aResource
        name <- anIdentifier
        fsl  <- anFSLFor(db, tpe)
      } withClue(fsl) {
        val res0 =
          api.post(s"/schema/1/items/$tpe/$name?validate=true", fsl, db.adminKey)
        res0 should respond(NotFound)

        val res1 =
          api.post(s"/schema/1/items/$tpe/$name", fsl, db.adminKey)
        res1 should respond(NotFound)
      }
    }

    prop("POST: rejects wrong schema type") {
      for {
        db   <- aDatabase
        name <- anIdentifier
        tpe0 <- aResource
        fsl0 <- anFSLFor(db, tpe0, Prop.const(name))
        setup = api.upload(
          "/schema/1/update?override=false",
          Seq("test.fsl" -> fsl0),
          db.adminKey
        )
        _ = setup should respond(OK)
        tpe1 <- aResource
        if tpe0 != tpe1
        fsl1 <- anFSLFor(db, tpe1, Prop.const(name))
      } withClue(s"fsl0=$fsl0 fsl1=$fsl1") {
        val res0 = api.post(
          s"/schema/1/items/$tpe0/$name?validate=true",
          fsl1,
          db.adminKey
        )
        res0 should respond(BadRequest)
        (res0.json / "error" / "message").as[String] should
          include("Wrong FSL snippet.")

        val res1 = api.post(
          s"/schema/1/items/$tpe0/$name",
          fsl1,
          db.adminKey
        )
        res1 should respond(BadRequest)
        (res1.json / "error" / "message").as[String] should
          include("Wrong FSL snippet.")
      }
    }

    prop("POST: requires a single fsl snippet") {
      for {
        db    <- aDatabase
        tpe   <- aResource
        name0 <- anIdentifier
        fsl0  <- anFSLFor(db, tpe, Prop.const(name0))
        name1 <- anIdentifier
        if name0 != name1
        fsl1 <- anFSLFor(db, tpe, Prop.const(name1))
      } withClue(s"fsl0=$fsl0 fsl1=$fsl1") {
        def checkErr(body: String, err: String) = {
          val res0 =
            api.post(s"/schema/1/items/$tpe/$name0?validate=true", body, db.adminKey)
          res0 should respond(BadRequest)
          (res0.json / "error" / "message").as[String] shouldEqual err

          val res1 =
            api.post(s"/schema/1/items/$tpe/$name0", body, db.adminKey)
          res1 should respond(BadRequest)
          (res1.json / "error" / "message").as[String] shouldEqual err
        }

        checkErr("", "No FSL snippet provided")
        checkErr("  ", "No FSL snippet provided")
        checkErr(
          s"""|$fsl0
              |$fsl1
              |""".stripMargin,
          "Expected a single schema item, got: 2"
        )
      }
    }

    prop("POST: return fsl errors") {
      for {
        db   <- aDatabase
        tpe  <- aResource
        name <- anIdentifier
        fsl0 <- anFSLFor(db, tpe, Prop.const(name))
        fsl1 <- anInvalidFSLFor(tpe)
        setup = api.upload(
          "/schema/1/update?override=false",
          Seq("foo.fsl" -> fsl0),
          db.adminKey
        )
        _ = setup should respond(OK)
      } withClue(s"fsl0=$fsl0 fsl1=$fsl1") {
        val res0 = api.post(
          s"/schema/1/items/$tpe/$name?validate=true",
          fsl1,
          db.adminKey
        )
        res0 should respond(BadRequest)
        (res0.json / "error" / "message").as[String] should include("error:")

        val res1 = api.post(
          s"/schema/1/items/$tpe/$name",
          fsl1,
          db.adminKey
        )
        res1 should respond(BadRequest)
        (res1.json / "error" / "message").as[String] should include("error:")
      }
    }

    prop("POST: return validation errors") {
      for {
        db <- aDatabase
      } yield {
        api.upload(
          "/schema/1/update?override=false",
          Seq(
            "foo.fsl" ->
              """|collection Foo {
                 |  foo: String
                 |}""".stripMargin),
          db.adminKey
        ) should respond(OK)

        val invalid = """|collection Foo {
                         |  migrations {
                         |    drop .foo
                         |    drop .foo
                         |  }
                         |}""".stripMargin

        val res0 = api.post(
          s"/schema/1/items/collections/Foo?validate=true",
          invalid,
          db.adminKey
        )
        res0 should respond(BadRequest)
        (res0.json / "error" / "message").as[String] shouldBe (
          """|error: Cannot drop field `.foo`. Dropped fields must be removed from schema
             |at foo.fsl:3:10
             |  |
             |3 |     drop .foo
             |  |          ^^^^
             |  |
             |hint: Field defined here
             |at foo.fsl:4:11
             |  |
             |4 |     drop .foo
             |  |           ^^^
             |  |""".stripMargin
        )

        val res1 = api.post(
          s"/schema/1/items/collections/Foo",
          invalid,
          db.adminKey
        )
        res1 should respond(BadRequest)
        (res1.json / "error" / "message").as[String] shouldBe (
          """|error: Cannot drop field `.foo`. Dropped fields must be removed from schema
             |at foo.fsl:3:10
             |  |
             |3 |     drop .foo
             |  |          ^^^^
             |  |
             |hint: Field defined here
             |at foo.fsl:4:11
             |  |
             |4 |     drop .foo
             |  |           ^^^
             |  |""".stripMargin
        )
      }
    }

    prop("POST: validates the schema version") {
      for {
        db   <- aDatabase
        tpe  <- aResource
        name <- anIdentifier
        fsl  <- anFSLFor(db, tpe, Prop.const(name))
        setup = api.upload(
          "/schema/1/update?override=false",
          Seq("test.fsl" -> fsl),
          db.adminKey
        )
        _ = setup should respond(OK)
        version = (setup.json / "version").as[Long]
        wrongVs <- Prop.long
        if wrongVs != version
      } withClue(fsl) {
        val wrong = api.post(
          s"/schema/1/items/$tpe/$name?version=$wrongVs",
          fsl,
          db.adminKey
        )
        wrong should respond(Conflict)

        val rightValidate = api.post(
          s"/schema/1/items/$tpe/$name?validate=true",
          fsl,
          db.adminKey
        )
        rightValidate should respond(OK)

        val right = api.post(
          s"/schema/1/items/$tpe/$name?version=$version",
          fsl,
          db.adminKey
        )
        right should respond(OK)
      }
    }

    prop("DELETE: removes a single schema item") {
      for {
        db   <- aDatabase
        tpe  <- aResource
        name <- anIdentifier
        fsl  <- anFSLFor(db, tpe, Prop.const(name))
        setup = api.upload(
          "/schema/1/update?override=false",
          Seq("test.fsl" -> fsl),
          db.adminKey
        )
        _ = setup should respond(OK)
      } withClue(fsl) {
        val res0 =
          api.delete(s"/schema/1/items/$tpe/$name?validate=true", db.adminKey)
        res0 should respond(OK)

        val res1 = api.delete(s"/schema/1/items/$tpe/$name", db.adminKey)
        res1 should respond(OK)

        val res2 =
          api.delete(s"/schema/1/items/$tpe/$name?validate=true", db.adminKey)
        res2 should respond(NotFound)

        val res3 = api.delete(s"/schema/1/items/$tpe/$name", db.adminKey)
        res3 should respond(NotFound)

        val vs = (res1.json / "version").as[Long]
        api.get(s"/schema/1/items/$tpe/$name?version=$vs", db.adminKey) should
          respond(NotFound)
      }
    }
  }

  "schema/1/validate/items/{resource}/{name}" / {
    prop("POST: produces an empty diff after updating an item to itself") {
      for {
        db   <- aDatabase
        tpe  <- aResource
        name <- anIdentifier
        fsl  <- anFSLFor(db, tpe, Prop.const(name))
        setup = api.upload(
          "/schema/1/update?override=false",
          Seq("test.fsl" -> fsl),
          db.adminKey
        )
        _ = setup should respond(OK)
        vs = (setup.json / "version").as[Long]
      } withClue(fsl) {
        val res =
          api.post(s"/schema/1/items/$tpe/$name?validate=true", fsl, db.adminKey)
        res should respond(OK)
        (res.json / "version").as[Long] shouldBe vs
        (res.json / "diff").as[String] shouldBe ""
      }
    }
  }
}
