package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._

class WritePermissionsSpec extends QueryAPI21Spec {

  "create" - {
    once("defaults") {
      for {
        db <- aDatabase
        person <- aUser(db)
        users <- aCollection(db)
        user <- aUserOfCollection(db, users)
      } {
        qassertRuns(CreateF(users.refObj),
          Seq(db.key -> true,
            db.adminKey -> true,
            db.clientKey -> false,
            user.token -> false,
            person.token -> false),
          JSArray("create")
        )
      }
    }
  }

  "update" - {
    once("defaults") {
      for {
        db     <- aDatabase
        person <- aUser(db)
        users  <- aCollection(db)
        alice  <- aUserOfCollection(db, users)
        bob    <- aUserOfCollection(db, users)
        name   <- aName
      } {

        qassertRuns(
          Update(
            alice.refObj,
            MkObject(
              "data" ->
                MkObject("name" -> name))),
          Seq(
            db.key -> true,
            db.adminKey -> true,
            db.clientKey -> false,
            alice.token -> false,
            bob.token -> false,
            person.token -> false)
        )
      }
    }
  }

  "replace" - {
    once("defaults") {
      for {
        db     <- aDatabase
        person <- aUser(db)
        users  <- aCollection(db)
        alice  <- aUserOfCollection(db, users)
        bob    <- aUserOfCollection(db, users)
        name   <- aName
      } {
        qassertRuns(
          Replace(
            alice.refObj,
            MkObject(
              "data" ->
                MkObject("name" -> name))),
          Seq(
            db.key -> true,
            db.adminKey -> true,
            db.clientKey -> false,
            alice.token -> false,
            bob.token -> false,
            person.token -> false)
        )
      }
    }
  }

  "delete" - {
    once("defaults") {
      for {
        db      <- aDatabase
        person  <- aUser(db)
        users   <- aCollection(db)
        user    <- aUserOfCollection(db, users)
        targets <- aDocument(users).times(5)
      } {

        runQuery(DeleteF(targets(0).refObj), db.key)
        runQuery(DeleteF(targets(1).refObj), db.adminKey)
        qassertErr(
          DeleteF(targets(2).refObj),
          "permission denied",
          JSArray.empty,
          db.clientKey)
        qassertErr(
          DeleteF(targets(3).refObj),
          "permission denied",
          JSArray.empty,
          person.token)
        qassertErr(
          DeleteF(targets(4).refObj),
          "permission denied",
          JSArray.empty,
          user.token)
        qassertErr(
          DeleteF(user.refObj),
          "permission denied",
          JSArray.empty,
          user.token)
      }
    }
  }
}
