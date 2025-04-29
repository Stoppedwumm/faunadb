package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.prop.api.{ Privilege, RoleAction }
import scala.concurrent.duration._

class IssueJWTSpec extends QueryAPIUnstableSpec {

  "IssueJWTSpec" - {
    once("issue JWT for document") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        doc <- aDocument(coll)
        token <- aJWT(db, doc.refObj)
      } {
        pending

        runQuery(Identity(), token) shouldBe doc.refObj
      }
    }

    once("issue JWT for role") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        role1 <- aRole(db, Privilege(coll.refObj, create = RoleAction.Granted))
        role2 <- aRole(db, Privilege(coll.refObj, create = RoleAction.Denied))
        token1 <- aJWT(db, role1.refObj)
        token2 <- aJWT(db, role2.refObj)
      } {
        pending

        runRawQuery(CreateF(coll.refObj), token1) should respond(Created)
        runRawQuery(CreateF(coll.refObj), token2) should respond(Forbidden)
      }
    }

    once("JWT should invalidate after expireIn seconds") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        doc <- aDocument(coll)
        token <- aJWT(db, doc.refObj, 10)
      } {
        eventually(timeout(20.seconds), interval(1.second)) {
          runRawQuery(Identity(), token) should respond(Unauthorized)
        }
      }
    }

    once("error when document ref is invalid") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
      } {
        qassertErr(
          IssueAccessJWT(MkRef(coll.refObj, "1")),
          "invalid scope",
          "Issuing JWT for an invalid scope",
          JSArray.empty,
          db
        )
      }
    }
  }
}
