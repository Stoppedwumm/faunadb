package fauna.api.test

import fauna.api.test.FQL2APISpec
import fauna.codex.json.JSObject
import fauna.net.http.NoBody
import scala.concurrent.duration._

class FQL2AdminApplicationSpec extends FQL2APISpec {
  "admin/reset-migrations" / {
    once("works") {
      for {
        db <- aDatabase
      } {
        api.upload(
          "/schema/1/update?force=true",
          Seq(
            "main.fsl" ->
              """|collection User {
                 |  name: String
                 |
                 |  index byName {
                 |    terms [.name]
                 |  }
                 |}
                 |""".stripMargin
          ),
          db.adminKey
        ) should respond(OK)

        queryOk(
          """|User.create({ name: 'Bob' })
             |Set.sequence(0, 256).forEach((i) => {
             |  User.create({ name: "User #{i}" })
             |})
             |""".stripMargin,
          db.adminKey
        )

        api.upload(
          "/schema/1/update?force=true",
          Seq(
            "main.fsl" ->
              """|collection User {
                 |  foo: String
                 |
                 |  index byName {
                 |    terms [.foo]
                 |  }
                 |
                 |  migrations {
                 |    move .name -> .foo
                 |  }
                 |}
                 |""".stripMargin
          ),
          db.adminKey
        ) should respond(OK)

        queryOk(
          """|Set.sequence(0, 256).forEach((i) => {
             |  User.create({ foo: "Another user #{i}" })
             |})""".stripMargin,
          db.adminKey
        )

        // TODO: Once index field provenance is here, we shouldn't need to wait for a
        // build.
        eventually(timeout(1.minute)) {
          queryOk("User.byName('User 3')", db.adminKey)
        }

        // Sanity check
        queryOk("if (User.byName('User 3').count() != 1) abort(0)", db.adminKey)
        queryOk(
          "if (User.byName('Another user 3').count() != 1) abort(0)",
          db.adminKey)

        // Get the scope ID
        val res = admin.post(
          "/admin/database/global-to-scope",
          JSObject("global_id" -> db.globalID),
          rootKey)
        val scope = (res.json / "scope_id").as[Long]

        // Now, kill the migrations
        admin.post(
          "/admin/reset-migrations",
          NoBody,
          rootKey,
          query = s"scope=$scope&collection=1024&rebuild=true") should respond(204)

        // The index should be rebuilt.
        queryOk(
          "if (Collection.byName('User')!.indexes.byName!.queryable) abort(0)",
          db.adminKey)
        queryOk(
          "if (Collection.byName('User')!.indexes.byName!.status != 'building') abort(0)",
          db.adminKey)

        // Wait for it to finish.
        eventually(timeout(1.minute)) {
          queryOk("User.byName('User 3')", db.adminKey)
        }

        // The index points to the new field, so any users that were created with the
        // old field should not get indexed.
        queryOk("if (User.byName('User 3').count() != 0) abort(0)", db.adminKey)
        queryOk(
          "if (User.byName('Another user 3').count() != 1) abort(0)",
          db.adminKey)

        // Schema should be updated to no longer include migrations or defined
        // fields.
        val schema = api.get("/schema/1/files/main.fsl", db.adminKey)
        schema should respond(OK)
        (schema.json / "content").as[String] shouldBe (
          """|collection User {
             |
             |  index byName {
             |    terms [.foo]
             |  }
             |  *: Any
             |}
             |""".stripMargin
        )
      }
    }

    once("works without rebuilding") {
      for {
        db <- aDatabase
      } {
        api.upload(
          "/schema/1/update?force=true",
          Seq(
            "main.fsl" ->
              """|collection User {
                 |  name: String
                 |
                 |  index byName {
                 |    terms [.name]
                 |  }
                 |}
                 |""".stripMargin
          ),
          db.adminKey
        ) should respond(OK)

        queryOk(
          """|User.create({ name: 'Bob' })
             |Set.sequence(0, 256).forEach((i) => {
             |  User.create({ name: "User #{i}" })
             |})
             |""".stripMargin,
          db.adminKey
        )

        api.upload(
          "/schema/1/update?force=true",
          Seq(
            "main.fsl" ->
              """|collection User {
                 |  foo: String
                 |
                 |  index byName {
                 |    terms [.foo]
                 |  }
                 |
                 |  migrations {
                 |    move .name -> .foo
                 |  }
                 |}
                 |""".stripMargin
          ),
          db.adminKey
        ) should respond(OK)

        queryOk(
          """|Set.sequence(0, 256).forEach((i) => {
             |  User.create({ foo: "Another user #{i}" })
             |})""".stripMargin,
          db.adminKey
        )

        // TODO: Once index field provenance is here, we shouldn't need to wait for a
        // build.
        eventually(timeout(1.minute)) {
          queryOk("User.byName('User 3')", db.adminKey)
        }

        // Sanity check
        queryOk("if (User.byName('User 3').count() != 1) abort(0)", db.adminKey)
        queryOk(
          "if (User.byName('Another user 3').count() != 1) abort(0)",
          db.adminKey)

        // Get the scope ID
        val res = admin.post(
          "/admin/database/global-to-scope",
          JSObject("global_id" -> db.globalID),
          rootKey)
        val scope = (res.json / "scope_id").as[Long]

        // Now, kill the migrations
        admin.post(
          "/admin/reset-migrations",
          NoBody,
          rootKey,
          query = s"scope=$scope&collection=1024") should respond(204)

        // The index should not be rebuilt, but queryable should be false.
        queryOk(
          "if (Collection.byName('User')!.indexes.byName!.queryable) abort(0)",
          db.adminKey)
        queryOk(
          "if (Collection.byName('User')!.indexes.byName!.status != 'complete') abort(0)",
          db.adminKey)

        // Go force queryable to true, and we should get an index mismatch.
        queryOk(
          "Collection.byName('User')!.update({ indexes: { byName: { queryable: true } } })",
          db.adminKey)

        // Note that this should be zero if the index were correct, but we didn't
        // rebuild it.
        eventually(timeout(10.seconds)) {
          // Use `eventually` to wait for the schema cache to update.
          queryOk("if (User.byName('User 3').count() != 1) abort(0)", db.adminKey)
        }
        queryOk(
          "if (User.byName('Another user 3').count() != 1) abort(0)",
          db.adminKey)

        // Schema should be updated to no longer include migrations or defined
        // fields.
        val schema = api.get("/schema/1/files/main.fsl", db.adminKey)
        schema should respond(OK)
        (schema.json / "content").as[String] shouldBe (
          """|collection User {
             |
             |  index byName {
             |    terms [.foo]
             |  }
             |  *: Any
             |}
             |""".stripMargin
        )
      }
    }
  }
}
