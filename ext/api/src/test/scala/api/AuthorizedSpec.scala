package fauna.api.test

import fauna.atoms.APIVersion
import fauna.net.http.NoBody

class AuthorizedSpec extends RESTSpec {

  "authorized" / {
    once("GET works") {
      for {
        db <- aContainerDB(APIVersion.Default.toString, accountID = 42)
      } {
        val res = api.get("/authorized", db.key)
        res should respond(OK)

        (res.json / "account").as[String] should equal("42")
        (res.json / "global_id").as[String] should equal(db.globalID)
      }
    }

    once("GET returns unauthorized with invalid key") {
      for {
        _ <- aDatabase
      } {
        api.get("/authorized", "bogus") should respond(Unauthorized)
      }
    }

    once("POST returns not allowed for valid key") {
      for {
        db <- aDatabase
      } {
        api.post("/authorized", NoBody, db.key) should respond(MethodNotAllowed)
      }
    }
  }
}
