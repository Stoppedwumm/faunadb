package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import scala.concurrent.duration._

class TokenSpec extends QueryAPI21Spec {
  "creation" - {
    once("can be created directly with a server key") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        val tok = runQuery(CreateF(Ref("tokens"), MkObject("instance" -> inst / "ref")), db)
        val secret = (tok / "secret").as[String]

        qassert(Exists(tok / "ref"), db)
        qassert(Exists(tok / "ref"), secret)

        (tok / "global_id").asOpt[JSValue] shouldBe None
      }
    }

    once("validates the instance is a ref") {
      for {
        db <- aDatabase
      } {
        val createQ = CreateF(Ref("tokens"), MkObject("instance" -> MkObject("foo" -> "bar")))
        qassertErr(createQ, "validation failed", JSArray("create"), db)
      }
    }
  }

  "update" - {
    once("updated tokens work") {
      for {
        db  <- aDatabase
        col <- aCollection(db)
        doc <- aDocument(col)
        tok = runQuery(
          CreateF(
            Ref("tokens"),
            MkObject("instance" -> doc.refObj)
          ),
          db
        )
        _ = runQuery(Update(tok.refObj), db)
      } {
        qequals(Identity(), doc.refObj, (tok / "secret").as[String])
      }
    }

    once("can set the hashed secret") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        val tok = runQuery(CreateF(Ref("tokens"), MkObject("instance" -> (inst / "ref"))), db)

        runQuery(Replace(tok / "ref", MkObject(
          "instance" -> (inst / "ref"),
          "hashed_secret" -> (tok / "secret"))), db)

        qequals(Select("hashed_secret", Get(tok / "ref")), tok / "secret", db)
      }
    }
  }

  "ttl" - {
    once("respect ttl") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        val ttl = TimeAdd(Now(), 5, "seconds")
        val tok = runQuery(CreateF(Ref("tokens"), MkObject("ttl" -> ttl, "instance" -> inst / "ref")), db)
        val secret = (tok / "secret").as[String]

        eventually(timeout(10.seconds), interval(200.millis)) {
          runRawQuery(Identity(), secret) should respond (Unauthorized)
        }
      }
    }

    once("update ttl") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        val tok = runQuery(CreateF(Ref("tokens"), MkObject("instance" -> inst / "ref")), db)
        val secret = (tok / "secret").as[String]

        runRawQuery(Identity(), secret) should respond (OK)

        val ttl = TimeAdd(Now(), 5, "seconds")
        runRawQuery(Update(tok.refObj, MkObject("ttl" -> ttl)), db.key) should respond (OK)

        eventually(timeout(10.seconds), interval(200.millis)) {
          runRawQuery(Identity(), secret) should respond (Unauthorized)
        }
      }
    }
  }
}
