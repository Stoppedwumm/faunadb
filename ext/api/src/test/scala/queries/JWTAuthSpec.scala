package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json.JSObject
import fauna.net.Network
import fauna.net.http._
import fauna.net.security._
import fauna.prop.Prop
import fauna.prop.api.{ AccessProviderRole, Privilege, RoleAction }
import java.time.Instant
import org.scalatest.BeforeAndAfterAll
import scala.concurrent.duration._

class JWTAuthSpec extends QueryAPI4Spec with BeforeAndAfterAll {

  val jwksProvider = new JWKSHTTPServer()

  override def beforeAll(): Unit = {
    super.beforeAll()
    jwksProvider.start()
  }

  override def afterAll(): Unit = {
    jwksProvider.stop()
    super.afterAll()
  }

  "JWTAuthSpec" - {
    once("works with role reference") {
      for {
        db <- aDatabase
        userName <- aName
        key <- aJWK
        issuer = s"https://auth.fauna.com/$userName"
        jwksUrl = s"${jwksProvider.address}/$userName/.well-known/jwks.json"
        role <- aRole(
          db,
          Privilege(ClassesNativeClassRef, create = RoleAction.Granted),
          Privilege(DatabaseNativeClassRef, create = RoleAction.Denied))
        _ <- anAccessProvider(
          db,
          Prop.const(issuer),
          Prop.const(jwksUrl),
          Seq(AccessProviderRole(role)))
        collName <- aName
        dbName <- aName
      } {
        jwksProvider.add(jwksUrl, key)

        val audience = s"https://db.fauna.com/db/${db.globalID}"

        val issuedAt = Instant.now()
        val expireAt = issuedAt.plusSeconds(60)

        val jwt = JWT.createToken(
          key,
          "RS256",
          issuer,
          audience,
          issuedAt.getEpochSecond,
          expireAt.getEpochSecond,
          "read:appointments")

        runRawQuery(
          CreateCollection(MkObject("name" -> collName)),
          jwt) should respond(Created)
        runRawQuery(CreateDatabase(MkObject("name" -> dbName)), jwt) should respond(
          Forbidden)
      }
    }

    once("works with predicate") {
      for {
        db <- aDatabase
        userName <- aName
        key <- aJWK
        issuer = s"https://auth.fauna.com/$userName"
        jwksUrl = s"${jwksProvider.address}/$userName/.well-known/jwks.json"
        role <- aRole(
          db,
          Privilege(ClassesNativeClassRef, create = RoleAction.Granted),
          Privilege(DatabaseNativeClassRef, create = RoleAction.Denied))
        predicate = Lambda("token" -> true)
        _ <- anAccessProvider(
          db,
          Prop.const(issuer),
          Prop.const(jwksUrl),
          Seq(AccessProviderRole(role, predicate)))
        collName <- aName
        dbName <- aName
      } {
        jwksProvider.add(jwksUrl, key)

        val audience = s"https://db.fauna.com/db/${db.globalID}"

        val issuedAt = Instant.now()
        val expireAt = issuedAt.plusSeconds(60)

        val jwt = JWT.createToken(
          key,
          "RS256",
          issuer,
          audience,
          issuedAt.getEpochSecond,
          expireAt.getEpochSecond,
          "read:appointments")

        runRawQuery(
          CreateCollection(MkObject("name" -> collName)),
          jwt) should respond(Created)
        runRawQuery(CreateDatabase(MkObject("name" -> dbName)), jwt) should respond(
          Forbidden)
      }
    }

    once("predicate can access JWT fields") {
      for {
        db <- aDatabase
        userName <- aName
        key <- aJWK
        issuer = s"https://auth.fauna.com/$userName"
        jwksUrl = s"${jwksProvider.address}/$userName/.well-known/jwks.json"
        role <- aRole(
          db,
          Privilege(ClassesNativeClassRef, create = RoleAction.Granted),
          Privilege(DatabaseNativeClassRef, create = RoleAction.Denied))
        predicate = Lambda(
          "token" -> Let(
            JWTFields.Scope -> Select(JWTFields.Scope, Var("token")),
            JWTFields.Audience -> Select(JWTFields.Audience, Var("token"))
          ) {
            And(
              ContainsStr(Var(JWTFields.Scope), "admin"),
              EndsWith(Var(JWTFields.Audience), db.globalID)
            )
          })
        _ <- anAccessProvider(
          db,
          Prop.const(issuer),
          Prop.const(jwksUrl),
          Seq(AccessProviderRole(role, predicate)))
        collName <- aName
        dbName <- aName
      } {
        jwksProvider.add(jwksUrl, key)

        val audience = s"https://db.fauna.com/db/${db.globalID}"

        val issuedAt = Instant.now()
        val expireAt = issuedAt.plusSeconds(60)

        val jwt = JWT.createToken(
          key,
          "RS256",
          issuer,
          audience,
          issuedAt.getEpochSecond,
          expireAt.getEpochSecond,
          "admin read:appointments")

        runRawQuery(
          CreateCollection(MkObject("name" -> collName)),
          jwt) should respond(Created)
        runRawQuery(CreateDatabase(MkObject("name" -> dbName)), jwt) should respond(
          Forbidden)
      }
    }

    once("forbid access if no roles passes the membership test") {
      for {
        db <- aDatabase
        userName <- aName
        key <- aJWK
        issuer = s"https://auth.fauna.com/$userName"
        jwksUrl = s"${jwksProvider.address}/$userName/.well-known/jwks.json"
        role <-
          aRole(db, Privilege(ClassesNativeClassRef, create = RoleAction.Granted))
        predicate = Lambda("token" -> false)
        _ <- anAccessProvider(
          db,
          Prop.const(issuer),
          Prop.const(jwksUrl),
          Seq(AccessProviderRole(role, predicate)))
        collName <- aName
      } {
        jwksProvider.add(jwksUrl, key)

        val audience = s"https://db.fauna.com/db/${db.globalID}"

        val issuedAt = Instant.now()
        val expireAt = issuedAt.plusSeconds(60)

        val jwt = JWT.createToken(
          key,
          "RS256",
          issuer,
          audience,
          issuedAt.getEpochSecond,
          expireAt.getEpochSecond,
          "admin")

        runRawQuery(
          CreateCollection(MkObject("name" -> collName)),
          jwt) should respond(Unauthorized)
      }
    }

    once("forbid if access provider is deleted") {
      for {
        db       <- aDatabase
        coll     <- aCollection(db)
        userName <- aName
        key      <- aJWK
        issuer = s"https://auth.fauna.com/$userName"
        jwksUrl = s"${jwksProvider.address}/$userName/.well-known/jwks.json"
        role <- aRole(db, Privilege(coll.refObj, create = RoleAction.Granted))
        ap <- anAccessProvider(
          db,
          Prop.const(issuer),
          Prop.const(jwksUrl),
          Seq(AccessProviderRole(role))
        )
      } {
        jwksProvider.add(jwksUrl, key)

        val audience = s"https://db.fauna.com/db/${db.globalID}"
        val issuedAt = Instant.now()
        val expireAt = issuedAt.plusSeconds(60)

        val jwt =
          JWT.createToken(
            key,
            "RS256",
            issuer,
            audience,
            issuedAt.getEpochSecond,
            expireAt.getEpochSecond,
            "admin"
          )

        runRawQuery(CreateF(coll.refObj), jwt) should respond(Created)
        runRawQuery(DeleteF(ap.refObj), db.adminKey) should respond(OK)
        Thread.sleep(SchemaCacheTTL) // wait for cache invalidation
        runRawQuery(CreateF(coll.refObj), jwt) should respond(Unauthorized)
      }
    }

    once("handle rotated keys") {
      for {
        db <- aDatabase
        userName <- aName
        key0 <- aJWK
        key1 <- aJWK
        issuer = s"https://auth.fauna.com/$userName"
        jwksUrl = s"${jwksProvider.address}/$userName/.well-known/jwks.json"
        role <-
          aRole(db, Privilege(ClassesNativeClassRef, create = RoleAction.Granted))
        _ <- anAccessProvider(
          db,
          Prop.const(issuer),
          Prop.const(jwksUrl),
          Seq(AccessProviderRole(role)))
        collName <- aName
        collName2 <- aName
      } {
        jwksProvider.add(jwksUrl, key0)

        val audience = s"https://db.fauna.com/db/${db.globalID}"

        val issuedAt = Instant.now()
        val expireAt = issuedAt.plusSeconds(60)

        val jwt0 = JWT.createToken(
          key0,
          "RS256",
          issuer,
          audience,
          issuedAt.getEpochSecond,
          expireAt.getEpochSecond,
          "read:appointments")

        runRawQuery(
          CreateCollection(MkObject("name" -> collName)),
          jwt0) should respond(Created)

        //rotate keys
        jwksProvider.add(jwksUrl, key1)

        val jwt1 = JWT.createToken(
          key1,
          "RS256",
          issuer,
          audience,
          issuedAt.getEpochSecond,
          expireAt.getEpochSecond,
          "read:appointments")

        //download rate mechanism will not allow download the new JKWS if we don't wait a few moments
        eventually(timeout(1.seconds), interval(250.millis)) {
          runRawQuery(
            CreateCollection(MkObject("name" -> collName2)),
            jwt1) should respond(Created)
        }
      }
    }

    once("invalid key") {
      Prop { _ =>
        runRawQuery(AddF(1, 2), BearerAuth("a.b.c")) should respond(Unauthorized)
      }
    }

    once("fail to verify signing") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        doc <- aDocument(coll)
        userName <- aName
        verificationKey <- aJWK
        signingKey <- aJWK
        issuer = s"https://auth.fauna.com/$userName"
        jwksUrl = s"${jwksProvider.address}/$userName/.well-known/jwks.json"
        role <- aRole(
          db,
          Privilege(ClassesNativeClassRef, create = RoleAction.Granted),
          Privilege(DatabaseNativeClassRef, create = RoleAction.Denied))
        _ <- anAccessProvider(
          db,
          Prop.const(issuer),
          Prop.const(jwksUrl),
          Seq(AccessProviderRole(role)))
      } {
        jwksProvider.add(jwksUrl, verificationKey)

        val audience = s"https://db.fauna.com/db/${db.globalID}"

        val issuedAt = Instant.now()
        val expireAt = issuedAt.plusSeconds(60)

        val jwt = JWT.createToken(
          signingKey,
          "RS256",
          issuer,
          audience,
          issuedAt.getEpochSecond,
          expireAt.getEpochSecond,
          s"@doc/${coll.name}/${doc.id} read:appointments")

        runRawQuery(AddF(1, 2), jwt) should respond(Unauthorized)
      }
    }

    once("fail if JWT has different algorithm") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        doc <- aDocument(coll)
        userName <- aName
        key <- aJWK
        issuer = s"https://auth.fauna.com/$userName"
        jwksUrl = s"${jwksProvider.address}/$userName/.well-known/jwks.json"
        role <- aRole(
          db,
          Privilege(ClassesNativeClassRef, create = RoleAction.Granted),
          Privilege(DatabaseNativeClassRef, create = RoleAction.Denied))
        _ <- anAccessProvider(
          db,
          Prop.const(issuer),
          Prop.const(jwksUrl),
          Seq(AccessProviderRole(role)))
      } {
        jwksProvider.add(jwksUrl, key)

        val audience = s"https://db.fauna.com/db/${db.globalID}"

        val issuedAt = Instant.now()
        val expireAt = issuedAt.plusSeconds(60)

        val header = JSObject(
          JWTFields.Algorithm -> "RS256",
          JWTFields.Type -> "JWT",
          JWTFields.KeyID -> key.kid
        )

        val payload = JSObject(
          JWTFields.Issuer -> issuer,
          JWTFields.Audience -> audience,
          JWTFields.IssuedAt -> issuedAt.getEpochSecond,
          JWTFields.ExpiresAt -> expireAt.getEpochSecond,
          JWTFields.Scope -> s"@doc/${coll.name}/${doc.id} read:appointments"
        )

        val jwt = JWT.createToken(
          header,
          payload,
          key,
          "RS384")

        runRawQuery(AddF(1, 2), jwt) should respond(Unauthorized)
      }
    }

    once("handle network failure while downloading JKWS") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        doc <- aDocument(coll)
        userName <- aName
        key <- aJWK
        role <- aRole(
          db,
          Privilege(ClassesNativeClassRef, create = RoleAction.Granted),
          Privilege(DatabaseNativeClassRef, create = RoleAction.Denied))
      } {
        val port = Network.findFreePort()

        val issuer = s"https://auth.fauna.com/$userName"
        val invalidUrl = s"https://localhost:$port/$userName/.well-known/jwks.json"

        anAccessProvider(
          db,
          Prop.const(issuer),
          Prop.const(invalidUrl),
          Seq(AccessProviderRole(role))).sample

        val audience = s"https://db.fauna.com/db/${db.globalID}"

        val issuedAt = Instant.now()
        val expireAt = issuedAt.plusSeconds(60)

        val jwt = JWT.createToken(
          key,
          "RS256",
          issuer,
          audience,
          issuedAt.getEpochSecond,
          expireAt.getEpochSecond,
          s"@doc/${coll.name}/${doc.id} read:appointments")

        runRawQuery(AddF(1, 2), jwt) should respond(Unauthorized)
      }
    }

    once("KeyFromSecret accepts a JWT token") {
      for {
        parent <- aDatabase
        db <- aDatabase(apiVers, parent)
        serverKey <- aKey(parent, Some(db))
        key <- aJWK
        userName <- aName
        issuer = s"https://auth.fauna.com/$userName"
        jwksUrl = s"${jwksProvider.address}/$userName/.well-known/jwks.json"
        role <-
          aRole(db, Privilege(ClassesNativeClassRef, create = RoleAction.Granted))
        _ <- anAccessProvider(
          db,
          Prop.const(issuer),
          Prop.const(jwksUrl),
          Seq(AccessProviderRole(role)))
      } {
        jwksProvider.add(jwksUrl, key)

        val audience = s"https://db.fauna.com/db/${db.globalID}"

        val issuedAt = Instant.now()
        val expireAt = issuedAt.plusSeconds(60)
        val jwt = JWT.createToken(
          key,
          "RS256",
          issuer,
          audience,
          issuedAt.getEpochSecond,
          expireAt.getEpochSecond,
          "read:appointments")

        val fromSecret =
          runQuery(KeyFromSecret(serverKey.secret), parent.adminKey) / "database"
        val fromJwt =
          runQuery(KeyFromSecret(jwt.getToken), parent.adminKey) / "database"
        // the references are identical
        fromSecret should be(fromJwt)
      }
    }

    once("KeyFromSecret accepts a JWT token and enforce read permission") {
      for {
        parent <- aDatabase
        db <- aDatabase(apiVers, parent)
        key <- aJWK
        userName <- aName
        issuer = s"https://auth.fauna.com/$userName"
        jwksUrl = s"${jwksProvider.address}/$userName/.well-known/jwks.json"
        role <-
          aRole(db, Privilege(ClassesNativeClassRef, create = RoleAction.Granted))
        _ <- anAccessProvider(
          db,
          Prop.const(issuer),
          Prop.const(jwksUrl),
          Seq(AccessProviderRole(role)))
      } {
        jwksProvider.add(jwksUrl, key)

        val audience = s"https://db.fauna.com/db/${db.globalID}"

        val issuedAt = Instant.now()
        val expireAt = issuedAt.plusSeconds(60)
        val jwt = JWT.createToken(
          key,
          "RS256",
          issuer,
          audience,
          issuedAt.getEpochSecond,
          expireAt.getEpochSecond,
          "read:appointments")

        runRawQuery(KeyFromSecret(jwt.getToken), db.key) should respond(Forbidden)
      }
    }
  }
}
