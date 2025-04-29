package fauna.model.test

import fauna.ast._
import fauna.auth._
import fauna.codex.json._
import fauna.lang.clocks.Clock
import fauna.model._
import fauna.repo.test.CassandraHelper

class CredentialsSpec extends Spec {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")

  "CredentialsSpec" - {
    val scope = ctx ! newScope
    val dbName = (ctx ! Database.forScope(scope)).get.name

    val server = Auth.forScope(scope)
    val (_, key) = ctx ! mkKey(dbName, "client")
    val client = Auth.forScope(scope, Some(key))

    socialSetup(ctx, server)

    "works" in {
      val alice = ctx ! mkPerson(client, MkObject(
        "credentials" -> MkObject("password" -> "sekrit")))

      val token = ctx ! loginAs(client, alice, "sekrit")
      val auth = (ctx ! Auth.lookup(token.secret)).get

      val creds = (ctx ! token.credentials).get.docID

      val self = runQuery(auth, Clock.time,
        Get(Ref("credentials/self"))) map {
        case VersionL(v, _) =>
          v.docID should equal (creds)
        case r              => sys.error(s"Unexpected: $r")
      }

      ctx ! self

      val admin = runQuery(server, Clock.time,
        Get(Ref(s"credentials/${creds.subID.toLong}"))) map {
        case VersionL(v, _) =>
          v.docID should equal (creds)
        case r              => sys.error(s"Unexpected: $r")
      }

      ctx ! admin
    }

    "does not invalidate tokens" in {
      val alice = ctx ! mkPerson(client, MkObject(
        "credentials" -> MkObject("password" -> "sekrit")))

      val token = ctx ! loginAs(client, alice, "sekrit")
      val auth = (ctx ! Auth.lookup(token.secret)).get

      val creds = (ctx ! token.credentials).get.docID

      val chpass = runQuery(
        server,
        Clock.time,
        Update(
          Ref(s"credentials/${creds.subID.toLong}"),
          MkObject("password" -> "str0ngpa$$")))

      ctx ! chpass

      val self = runQuery(auth, Clock.time, Get(Ref("credentials/self"))) map {
        case VersionL(v, _) =>
          v.docID should equal (creds)
        case r              => sys.error(s"Unexpected: $r")
      }

      ctx ! self
    }

    "users cannot create credentials" in {
      val alice = ctx ! mkPerson(client, MkObject(
        "credentials" -> MkObject("password" -> "sekrit")))

      val token = ctx ! loginAs(client, alice, "sekrit")
      val auth = (ctx ! Auth.lookup(token.secret)).get

      val query = runQuery(auth, Clock.time,
        CreateF(Ref("credentials"),
          MkObject(
            "instance" -> alice.refObj,
            "password" -> "sekrit")))

      intercept[RuntimeException] {
        ctx ! query
      }
    }

    "users cannot delete credentials" in {
      val alice = ctx ! mkPerson(client, MkObject(
        "credentials" -> MkObject("password" -> "sekrit")))

      val token = ctx ! loginAs(client, alice, "sekrit")
      val auth = (ctx ! Auth.lookup(token.secret)).get

      val creds = (ctx ! token.credentials).get.docID.subID.toLong

      val query = runQuery(auth, Clock.time, DeleteF(Ref(s"credentials/$creds")))

      intercept[RuntimeException] {
        ctx ! query
      }
    }

    "users cannot create credentials on others" in {
      val alice = ctx ! mkPerson(client, MkObject(
        "credentials" -> MkObject("password" -> "sekrit")))
      val bob = ctx ! mkPerson(client)

      val token = ctx ! loginAs(client, alice, "sekrit")
      val auth = (ctx ! Auth.lookup(token.secret)).get

      val query = runQuery(auth, Clock.time,
        Update(bob.refObj,
          MkObject(
            "credentials" -> JSObject(
              "instance" -> bob.refObj,
              "password" -> "sekrit"))))

      intercept[RuntimeException] {
        ctx ! query
      }

      intercept[RuntimeException] {
        ctx ! loginAs(client, bob, "sekrit")
      }
    }

    "users cannot modify others' credentials" in {
      val alice = ctx ! mkPerson(client, MkObject(
        "credentials" -> MkObject("password" -> "sekrit")))
      val bob = ctx ! mkPerson(client, MkObject(
        "credentials" -> MkObject("password" -> "str0ngpa$$")))

      noException should be thrownBy {
        ctx ! loginAs(client, bob, "str0ngpa$$")
      }

      val token = ctx ! loginAs(client, alice, "sekrit")
      val auth = (ctx ! Auth.lookup(token.secret)).get

      val update = runQuery(auth, Clock.time,
        Update(bob.refObj,
          MkObject(
            "credentials" -> JSObject(
              "password" -> "sekrit"))))

      intercept[RuntimeException] {
        ctx ! update
      }

      intercept[RuntimeException] {
        ctx ! loginAs(client, bob, "sekrit")
      }

      noException should be thrownBy {
        ctx ! loginAs(client, bob, "str0ngpa$$")
      }

      val delete = runQuery(auth, Clock.time,
        Update(bob.refObj,
          MkObject("credentials" -> JSNull)))

      intercept[RuntimeException] {
        ctx ! delete
      }

      noException should be thrownBy {
        ctx ! loginAs(client, bob, "str0ngpa$$")
      }

    }

  }
}
