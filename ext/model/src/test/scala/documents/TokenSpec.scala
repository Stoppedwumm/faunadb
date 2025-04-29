package fauna.model.test

import fauna.ast._
import fauna.atoms._
import fauna.auth._
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.model._
import fauna.repo.query.Query
import fauna.repo.test.CassandraHelper
import org.scalatest.tags.Slow
import scala.concurrent.duration._

@Slow
class TokenSpec extends Spec {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")

  "TokenSpec" - {
    val scope = ctx ! newScope
    val dbName = (ctx ! Database.forScope(scope)).get.name

    val server = Auth.forScope(scope)
    val (_, key) = ctx ! mkKey(dbName, "client")
    val client = Auth.forScope(scope, Some(key))

    socialSetup(ctx, server)

    "works" in {
      val alice = ctx ! mkPerson(client, MkObject(
        "credentials" -> MkObject("password" -> "sekrit")))
      val bob = ctx ! mkPerson(client)

      ctx ! mkFollow(server, bob, alice)

      val token = ctx ! loginAs(client, alice, "sekrit")
      val auth = (ctx ! Auth.lookup(token.secret)).get

      ctx ! mkPost(auth, alice)
      ctx ! mkPost(auth, alice)

      val posts = ctx ! collection(client, timelineFor(bob))
      posts.elems.size should equal (2)
    }

    "respect ttl" in {
      val alice = ctx ! mkPerson(client, MkObject(
        "credentials" -> MkObject("password" -> "sekrit")))

      val token = ctx ! loginAs(client, alice, "sekrit", Some(Clock.time + 10.seconds))

      eventually(timeout(20.seconds), interval(200.millis)) {
        //this will exercise values on cache
        (ctx ! Auth.lookup(token.secret)) shouldBe None
      }
    }

    "clients can read their tokens" in {
      val alice = ctx ! mkPerson(client, MkObject(
        "credentials" -> MkObject("password" -> "sekrit")))
      val token = ctx ! loginAs(client, alice, "sekrit")
      val user = (ctx ! Auth.lookup(token.secret)).get

      val self = runQuery(user, Clock.time, Get(Ref("tokens/self"))) map {
        case VersionL(v, _) =>
          v.docID should equal (token.id.toDocID)
        case r              => sys.error(s"Unexpected: $r")
      }

      ctx ! self

      val uCreds = runQuery(user, Clock.time, Get(Ref("credentials/self")))

      val check = (uCreds, token.credentials) par { (ucreds, tcreds) =>
        (ucreds, tcreds) match {
          case (VersionL(v, _), Some(creds)) =>
            v.docID.as[CredentialsID] should equal (creds.id)
            Query.unit
          case r => sys.error(s"Unexpected: $r")
        }
      }

      ctx ! check
    }

    "users cannot create tokens" in {
      val alice = ctx ! mkPerson(client, MkObject(
        "credentials" -> MkObject("password" -> "sekrit")))

      val token = ctx ! loginAs(client, alice, "sekrit")
      val auth = (ctx ! Auth.lookup(token.secret)).get

      val creds = (ctx ! token.credentials).get.docID.subID.toLong
      val query = runQuery(auth, Clock.time,
        CreateF(Ref("token"),
          MkObject(
            "credentials" -> Ref(s"credentials/$creds"),
            "secret" -> token.secret)))

      intercept[RuntimeException] {
        ctx ! query
      }
    }

    "users can delete tokens" in {
      val alice = ctx ! mkPerson(client, MkObject(
        "credentials" -> MkObject("password" -> "sekrit")))

      val token = ctx ! loginAs(client, alice, "sekrit")
      val auth = (ctx ! Auth.lookup(token.secret)).get

      val id = token.id.toLong

      val query = runQuery(auth, Clock.time, DeleteF(Ref(s"tokens/$id")))

      noException should be thrownBy {
        ctx ! query
      }
    }

    "users can logout" in {
      def self(auth: Auth) =
        runQuery(auth, Clock.time, Get(Ref("tokens/self"))) map {
          case VersionL(_, _) => ()
          case r              => sys.error(s"Unexpected: $r")
        }

      val alice = ctx ! mkPerson(client, MkObject(
        "credentials" -> MkObject("password" -> "sekrit")))

      val token1 = ctx ! loginAs(client, alice, "sekrit")
      val auth1 = (ctx ! Auth.lookup(token1.secret)).get
      val token2 = ctx ! loginAs(client, alice, "sekrit")
      val auth2 = (ctx ! Auth.lookup(token2.secret)).get
      val token3 = ctx ! loginAs(client, alice, "sekrit")
      val auth3 = (ctx ! Auth.lookup(token3.secret)).get

      ctx ! self(auth1)
      ctx ! self(auth2)
      ctx ! self(auth3)

      ctx ! logoutAs(auth1, false)
      intercept[RuntimeException] {
        ctx ! self(auth1)
      }

      ctx ! self(auth2)
      ctx ! self(auth3)

      ctx ! logoutAs(auth2, true)
      intercept[RuntimeException] {
        ctx ! self(auth2)
      }
      intercept[RuntimeException] {
        ctx ! self(auth3)
      }
    }
  }
}
