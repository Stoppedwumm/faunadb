package fauna.api.test

import fauna.api.test._

class SchemaABACSpec extends RESTSpec {

  once("requires admin or server keys to read schema") {
    for {
      db      <- aDatabase
      role    <- aRole(db)
      roleKey <- aKey(role)

      setup = api.upload(
        "/schema/1/update?override=false&force=true",
        Seq("foo.fsl" -> "function id(x) {x}"),
        db.adminKey
      )
      _ = setup should respond(OK)
    } {
      api.get("/schema/1/files") should respond(Unauthorized)

      all {
        Seq(db.adminKey, db.key) flatMap { secret =>
          Seq(
            api.get("/schema/1/files", secret),
            api.get("/schema/1/files/foo.fsl", secret),
            api.get("/schema/1/items/functions/id", secret)
          )
        }
      } should respond(OK)

      all {
        Seq(db.clientKey, roleKey.secret) flatMap { secret =>
          Seq(
            api.get("/schema/1/files", secret),
            api.get("/schema/1/files/foo.fsl", secret),
            api.get("/schema/1/items/functions/id", secret)
          )
        }
      } should respond(Forbidden)
    }
  }

  once("requires admin keys to update schema") {
    for {
      db      <- aDatabase
      role    <- aRole(db)
      roleKey <- aKey(role)
    } {
      val idFn = "function id(x) {x}"
      val files = Seq("foo.fsl" -> idFn)

      api.upload("/schema/1/update?override=false&force=true", files) should
        respond(Unauthorized)

      api.upload(
        "/schema/1/update?override=false&force=true",
        files,
        db.adminKey) should respond(OK)

      api.post("/schema/1/items/functions/id?force=true", idFn, db.adminKey) should
        respond(OK)

      all(
        Seq(db.key, db.clientKey, roleKey.secret) flatMap { secret =>
          Seq(
            api.upload("/schema/1/update?force=true", files, secret),
            api.post("/schema/1/items/functions/id?force=true", idFn, secret)
          )
        }
      ) should respond(Forbidden)
    }
  }

  once("tokens may not read or write schema") {
    for {
      db   <- aDatabase
      user <- aUser(db)
    } {
      api.get("/schema/1/files", user.token) should respond(Forbidden)
      api.post(
        "/schema/1/update?force=true",
        "function id(x) { x }",
        user.token
      ) should respond(Forbidden)
    }
  }
}
