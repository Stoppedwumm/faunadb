package fauna.model.auth.test

import fauna.atoms.ScopeID
import fauna.auth._
import fauna.codex.json.{ JSArray, JSObject }
import fauna.model._
import fauna.model.test._
import fauna.net.security._
import fauna.prop.Generators
import fauna.repo.test.CassandraHelper
import java.net.URL
import java.time.Instant
import org.scalatest.concurrent.Eventually
import scala.concurrent.duration._
import scala.concurrent.Future

class JWTAuthSpec extends Spec with Generators with Eventually with AuthGenerators {

  import SocialHelpersV4._

  val ctx = CassandraHelper.context("model")

  val scope = ctx ! newScope
  val auth = Auth.adminForScope(scope)
  val db = (ctx ! Database.forScope(scope)).get

  def jwkProvider(jwksUri: String, key: JWK) =
    new JWKProvider {

      def getJWK(url: URL, kid: Option[String]): Future[JWK] = {
        if (jwksUri == url.toString && key.kid == kid) {
          Future.successful(key)
        } else {
          Future.failed(new JWKSError(url, "wrong uri or key"))
        }
      }
    }

  "works with role reference" in {
    val roleName = aName.sample
    val accessName = aName.sample
    val userName = aName.sample

    val roleID = ctx ! mkRole(
      auth,
      roleName,
      Seq(
        MkObject(
          "resource" -> ClassesNativeClassRef,
          "actions" -> MkObject("read" -> true)
        )))

    val key = aJWK sample
    val issuer = s"https://$userName.auth0.com"
    val jwksUri = s"$issuer/.well-known/jwks.json"
    val audience = JWTToken.canonicalDBUrl(db)

    ctx ! mkAccessProvider(auth, accessName, issuer, jwksUri, Seq(RoleRef(roleName)))

    val payload = JSObject(
      JWTFields.Issuer -> issuer,
      JWTFields.Audience -> audience,
      JWTFields.IssuedAt -> Instant.now().getEpochSecond,
      JWTFields.ExpiresAt -> Instant.now().plusSeconds(60).getEpochSecond,
      JWTFields.NotBefore -> Instant.now().plusSeconds(1).getEpochSecond,
      JWTFields.Scope -> "read:appointments"
    )

    val payloadLit = JWTToken.toLiteral(payload)

    val jwt = JWT.createToken(payload, key, "RS256")

    val roleEvalContext = ctx ! RoleEvalContext.lookup(scope, Set(roleID))

    eventually(timeout(5.seconds), interval(500.millis)) {
      (ctx ! Auth.fromJwt(jwt.getToken, jwkProvider(jwksUri, key))) shouldBe Some(
        ctx ! LoginAuth
          .get(scope, RolePermissions(roleEvalContext), JWTLogin(jwt, payloadLit)))
    }
  }
  "works with non required fields" in {
    val roleName = aName.sample
    val accessName = aName.sample
    val userName = aName.sample

    val roleID = ctx ! mkRole(
      auth,
      roleName,
      Seq(
        MkObject(
          "resource" -> ClassesNativeClassRef,
          "actions" -> MkObject("read" -> true)
        )))

    val key = aJWK sample
    val issuer = s"https://$userName.auth0.com"
    val jwksUri = s"$issuer/.well-known/jwks.json"
    val audience = JWTToken.canonicalDBUrl(db)

    ctx ! mkAccessProvider(auth, accessName, issuer, jwksUri, Seq(RoleRef(roleName)))

    val payload = JSObject(
      JWTFields.Issuer -> issuer,
      JWTFields.Audience -> audience
    )

    val payloadLit = JWTToken.toLiteral(payload)

    val jwt = JWT.createToken(payload, key, "RS256")

    val roleEvalContext = ctx ! RoleEvalContext.lookup(scope, Set(roleID))

    eventually(timeout(5.seconds), interval(500.millis)) {
      (ctx ! Auth.fromJwt(jwt.getToken, jwkProvider(jwksUri, key))) shouldBe Some(
        ctx ! LoginAuth
          .get(scope, RolePermissions(roleEvalContext), JWTLogin(jwt, payloadLit)))
    }
  }

  "works with predicate" in {
    val roleName = aName.sample
    val accessName = aName.sample
    val userName = aName.sample

    val roleID = ctx ! mkRole(
      auth,
      roleName,
      Seq(
        MkObject(
          "resource" -> ClassesNativeClassRef,
          "actions" -> MkObject("read" -> true)
        )))

    val key = aJWK sample
    val issuer = s"https://$userName.auth0.com"
    val jwksUri = s"$issuer/.well-known/jwks.json"
    val audience = JWTToken.canonicalDBUrl(db)

    val predicate = MkObject(
      "role" -> RoleRef(roleName),
      "predicate" -> QueryF(Lambda("token" -> true))
    )
    ctx ! mkAccessProvider(auth, accessName, issuer, jwksUri, Seq(predicate))

    val payload = JSObject(
      JWTFields.Issuer -> issuer,
      JWTFields.Audience -> audience,
      JWTFields.IssuedAt -> Instant.now().getEpochSecond,
      JWTFields.ExpiresAt -> Instant.now().plusSeconds(60).getEpochSecond,
      JWTFields.NotBefore -> Instant.now().plusSeconds(1).getEpochSecond,
      JWTFields.Scope -> "read:appointments"
    )

    val payloadLit = JWTToken.toLiteral(payload)

    val jwt = JWT.createToken(payload, key, "RS256")

    val roleEvalContext = ctx ! RoleEvalContext.lookup(scope, Set(roleID))

    eventually(timeout(5.seconds), interval(500.millis)) {
      (ctx ! Auth.fromJwt(jwt.getToken, jwkProvider(jwksUri, key))) shouldBe Some(
        ctx ! LoginAuth
          .get(scope, RolePermissions(roleEvalContext), JWTLogin(jwt, payloadLit)))
    }
  }

  "predicate can access JWT fields" in {
    val roleName = aName.sample
    val accessName = aName.sample
    val userName = aName.sample

    val roleID = ctx ! mkRole(
      auth,
      roleName,
      Seq(
        MkObject(
          "resource" -> ClassesNativeClassRef,
          "actions" -> MkObject("read" -> true)
        )))

    val key = aJWK sample
    val issuer = s"https://$userName.auth0.com"
    val jwksUri = s"$issuer/.well-known/jwks.json"
    val audience = JWTToken.canonicalDBUrl(db)

    val predicate = MkObject(
      "role" -> RoleRef(roleName),
      "predicate" -> QueryF(
        Lambda(
          "token" -> Let(
            JWTFields.Scope -> Select(JWTFields.Scope, Var("token")),
            JWTFields.Audience -> Select(JWTFields.Audience, Var("token"))
          ) {
            And(
              ContainsStr(Select(JWTFields.Scope, Var("token")), "admin"),
              Equals(Var(JWTFields.Audience), audience)
            )
          }))
    )
    ctx ! mkAccessProvider(auth, accessName, issuer, jwksUri, Seq(predicate))

    val payload = JSObject(
      JWTFields.Issuer -> issuer,
      JWTFields.Audience -> audience,
      JWTFields.IssuedAt -> Instant.now().getEpochSecond,
      JWTFields.ExpiresAt -> Instant.now().plusSeconds(60).getEpochSecond,
      JWTFields.NotBefore -> Instant.now().plusSeconds(1).getEpochSecond,
      JWTFields.Scope -> "admin read:appointments"
    )

    val payloadLit = JWTToken.toLiteral(payload)

    val jwt = JWT.createToken(payload, key, "RS256")

    val roleEvalContext = ctx ! RoleEvalContext.lookup(scope, Set(roleID))

    eventually(timeout(5.seconds), interval(500.millis)) {
      (ctx ! Auth.fromJwt(jwt.getToken, jwkProvider(jwksUri, key))) shouldBe Some(
        ctx ! LoginAuth
          .get(scope, RolePermissions(roleEvalContext), JWTLogin(jwt, payloadLit)))
    }
  }

  "returns None when no iss is present" in {
    val roleName = aName.sample
    val accessName = aName.sample
    val userName = aName.sample

    ctx ! mkRole(
      auth,
      roleName,
      Seq(
        MkObject(
          "resource" -> ClassesNativeClassRef,
          "actions" -> MkObject("read" -> true)
        )))

    val key = aJWK sample
    val issuer = s"https://$userName.auth0.com"
    val jwksUri = s"$issuer/.well-known/jwks.json"
    val audience = JWTToken.canonicalDBUrl(db)

    ctx ! mkAccessProvider(auth, accessName, issuer, jwksUri, Seq(RoleRef(roleName)))

    val payload = JSObject(
      JWTFields.Audience -> audience,
      JWTFields.IssuedAt -> Instant.now().getEpochSecond,
      JWTFields.ExpiresAt -> Instant.now().plusSeconds(60).getEpochSecond,
      JWTFields.Scope -> "read:appointments"
    )

    val jwt = JWT.createToken(payload, key, "RS256").getToken

    (ctx ! Auth.fromJwt(jwt, jwkProvider(jwksUri, key))) shouldBe None
  }

  "returns None when no aud is present" in {
    val roleName = aName.sample
    val accessName = aName.sample
    val userName = aName.sample

    ctx ! mkRole(
      auth,
      roleName,
      Seq(
        MkObject(
          "resource" -> ClassesNativeClassRef,
          "actions" -> MkObject("read" -> true)
        )))

    val key = aJWK sample
    val issuer = s"https://$userName.auth0.com"
    val jwksUri = s"$issuer/.well-known/jwks.json"

    ctx ! mkAccessProvider(auth, accessName, issuer, jwksUri, Seq(RoleRef(roleName)))

    val payload = JSObject(
      JWTFields.Issuer -> issuer,
      JWTFields.IssuedAt -> Instant.now().getEpochSecond,
      JWTFields.ExpiresAt -> Instant.now().plusSeconds(60).getEpochSecond,
      JWTFields.Scope -> "read:appointments"
    )

    val jwt = JWT.createToken(payload, key, "RS256").getToken

    (ctx ! Auth.fromJwt(jwt, jwkProvider(jwksUri, key))) shouldBe None
  }

  "forbid root scope" in {
    val accessName = aName.sample
    val userName = aName.sample
    val roleName = aName.sample

    ctx ! mkRole(
      RootAuth,
      roleName,
      Seq(
        MkObject(
          "resource" -> ClassesNativeClassRef,
          "actions" -> MkObject("read" -> true)
        )))

    val key = aJWK sample
    val issuer = s"https://$userName.auth0.com"
    val jwksUri = s"$issuer/.well-known/jwks.json"
    val audience =
      s"https://db.fauna.com/db/${Database.encodeGlobalID(ScopeID.RootID)}"

    ctx ! mkAccessProvider(
      RootAuth,
      accessName,
      issuer,
      jwksUri,
      Seq(RoleRef(roleName)))

    val issuedAt = Instant.now()
    val expireAt = issuedAt.plusSeconds(60)

    val token = JWT
      .createToken(
        key,
        "RS256",
        issuer,
        audience,
        issuedAt.getEpochSecond,
        expireAt.getEpochSecond,
        "read:appointments")
      .getToken

    (ctx ! Auth.fromJwt(token, jwkProvider(jwksUri, key))) shouldBe None
  }

  "forbid if could not download JWKS" in {
    val accessName = aName.sample
    val userName = aName.sample
    val roleName = aName.sample

    ctx ! mkRole(
      auth,
      roleName,
      Seq(
        MkObject(
          "resource" -> ClassesNativeClassRef,
          "actions" -> MkObject("read" -> true)
        )))

    val key = aJWK sample
    val issuer = s"https://$userName.auth0.com"
    val jwksUri = s"$issuer/.well-known/jwks.json"
    val audience = JWTToken.canonicalDBUrl(db)

    ctx ! mkAccessProvider(auth, accessName, issuer, jwksUri, Seq(RoleRef(roleName)))

    val issuedAt = Instant.now()
    val expireAt = issuedAt.plusSeconds(60)

    val token = JWT
      .createToken(
        key,
        "RS256",
        issuer,
        audience,
        issuedAt.getEpochSecond,
        expireAt.getEpochSecond,
        "read:appointments")
      .getToken

    (ctx ! Auth.fromJwt(
      token,
      jwkProvider("https://other-url.com", key))) shouldBe None
  }

  "forbid expired tokens" in {
    val accessName = aName.sample
    val userName = aName.sample
    val roleName = aName.sample

    ctx ! mkRole(
      auth,
      roleName,
      Seq(
        MkObject(
          "resource" -> ClassesNativeClassRef,
          "actions" -> MkObject("read" -> true)
        )))

    val key = aJWK sample
    val issuer = s"https://$userName.auth0.com"
    val jwksUri = s"$issuer/.well-known/jwks.json"
    val audience = JWTToken.canonicalDBUrl(db)

    ctx ! mkAccessProvider(auth, accessName, issuer, jwksUri, Seq(RoleRef(roleName)))

    val issuedAt = Instant.now().minusSeconds(60).getEpochSecond
    val expireAt = Instant.now().minusSeconds(1).getEpochSecond

    val token = JWT
      .createToken(
        key,
        "RS256",
        issuer,
        audience,
        issuedAt,
        expireAt,
        "read:appointments")
      .getToken

    (ctx ! Auth.fromJwt(token, jwkProvider(jwksUri, key))) shouldBe None
  }

  "forbid if not before" in {
    val accessName = aName.sample
    val userName = aName.sample
    val roleName = aName.sample

    ctx ! mkRole(
      auth,
      roleName,
      Seq(
        MkObject(
          "resource" -> ClassesNativeClassRef,
          "actions" -> MkObject("read" -> true)
        )))

    val key = aJWK sample
    val issuer = s"https://$userName.auth0.com"
    val jwksUri = s"$issuer/.well-known/jwks.json"
    val audience = JWTToken.canonicalDBUrl(db)

    ctx ! mkAccessProvider(auth, accessName, issuer, jwksUri, Seq(RoleRef(roleName)))

    val payload = JSObject(
      JWTFields.Issuer -> issuer,
      JWTFields.Audience -> audience,
      JWTFields.IssuedAt -> Instant.now().getEpochSecond,
      JWTFields.ExpiresAt -> Instant.now().plusSeconds(60).getEpochSecond,
      JWTFields.NotBefore -> Instant.now().plusSeconds(30).getEpochSecond,
      JWTFields.Scope -> "read:appointments"
    )

    val token = JWT.createToken(payload, key, "RS256").getToken

    (ctx ! Auth.fromJwt(token, jwkProvider(jwksUri, key))) shouldBe None
  }

  "forbid if could not find matching audience" in {
    val accessName = aName.sample
    val userName = aName.sample
    val roleName = aName.sample

    ctx ! mkRole(
      auth,
      roleName,
      Seq(
        MkObject(
          "resource" -> ClassesNativeClassRef,
          "actions" -> MkObject("read" -> true)
        )))

    val key = aJWK sample
    val issuer = s"https://$userName.auth0.com"
    val jwksUri = s"$issuer/.well-known/jwks.json"
    val audience = s"a non sense audience"

    ctx ! mkAccessProvider(auth, accessName, issuer, jwksUri, Seq(RoleRef(roleName)))

    val issuedAt = Instant.now()
    val expireAt = issuedAt.plusSeconds(60)

    val token = JWT
      .createToken(
        key,
        "RS256",
        issuer,
        audience,
        issuedAt.getEpochSecond,
        expireAt.getEpochSecond,
        "read:appointments")
      .getToken

    (ctx ! Auth.fromJwt(token, jwkProvider(jwksUri, key))) shouldBe None
  }

  "forbid if find multiple matching audiences" in {
    val accessName = aName.sample
    val userName = aName.sample
    val roleName = aName.sample

    ctx ! mkRole(
      auth,
      roleName,
      Seq(
        MkObject(
          "resource" -> ClassesNativeClassRef,
          "actions" -> MkObject("read" -> true)
        )))

    val key = aJWK sample
    val issuer = s"https://$userName.auth0.com"
    val jwksUri = s"$issuer/.well-known/jwks.json"
    val audience0 = JWTToken.canonicalDBUrl(db)
    val audience1 = JWTToken.canonicalDBUrl(Database.RootDatabase)

    ctx ! mkAccessProvider(auth, accessName, issuer, jwksUri, Seq(RoleRef(roleName)))

    val payload = JSObject(
      JWTFields.Issuer -> issuer,
      JWTFields.Audience -> JSArray(audience0, audience1),
      JWTFields.IssuedAt -> Instant.now().getEpochSecond,
      JWTFields.ExpiresAt -> Instant.now().plusSeconds(60).getEpochSecond,
      JWTFields.Scope -> "read:appointments"
    )

    val token = JWT.createToken(payload, key, "RS256").getToken

    (ctx ! Auth.fromJwt(token, jwkProvider(jwksUri, key))) shouldBe None
  }

  "forbid if could not find access provider" in {
    val userName = aName.sample

    val key = aJWK sample
    val issuer = s"https://$userName.auth0.com"
    val jwksUri = s"$issuer/.well-known/jwks.json"
    val audience = JWTToken.canonicalDBUrl(db)

    val issuedAt = Instant.now()
    val expireAt = issuedAt.plusSeconds(60)

    val token = JWT
      .createToken(
        key,
        "RS256",
        issuer,
        audience,
        issuedAt.getEpochSecond,
        expireAt.getEpochSecond,
        "read:appointments")
      .getToken

    (ctx ! Auth.fromJwt(token, jwkProvider(jwksUri, key))) shouldBe None
  }

  "forbid if no roles passes the membership test" in {
    val roleName = aName.sample
    val accessName = aName.sample
    val userName = aName.sample

    ctx ! mkRole(
      auth,
      roleName,
      Seq(
        MkObject(
          "resource" -> ClassesNativeClassRef,
          "actions" -> MkObject("read" -> true)
        )))

    val key = aJWK sample
    val issuer = s"https://$userName.auth0.com"
    val jwksUri = s"$issuer/.well-known/jwks.json"
    val audience = JWTToken.canonicalDBUrl(db)

    ctx ! mkAccessProvider(
      auth,
      accessName,
      issuer,
      jwksUri,
      Seq(
        MkObject(
          "role" -> RoleRef(roleName),
          "predicate" -> QueryF(Lambda("token" -> false)))))

    val issuedAt = Instant.now()
    val expireAt = issuedAt.plusSeconds(60)

    val token = JWT.createToken(
      key,
      "RS256",
      issuer,
      audience,
      issuedAt.getEpochSecond,
      expireAt.getEpochSecond,
      "read:appointments")

    (ctx ! Auth.fromJwt(token.getToken, jwkProvider(jwksUri, key))) shouldBe None
  }

  "forbid if token signature is invalid" in {
    val accessName = aName.sample
    val userName = aName.sample
    val roleName = aName.sample

    ctx ! mkRole(
      auth,
      roleName,
      Seq(
        MkObject(
          "resource" -> ClassesNativeClassRef,
          "actions" -> MkObject("read" -> true)
        )))

    val key = aJWK sample
    val issuer = s"https://$userName.auth0.com"
    val jwksUri = s"$issuer/.well-known/jwks.json"
    val audience = JWTToken.canonicalDBUrl(db)

    ctx ! mkAccessProvider(auth, accessName, issuer, jwksUri, Seq(RoleRef(roleName)))

    val issuedAt = Instant.now()
    val expireAt = issuedAt.plusSeconds(60)

    val token = JWT.createToken(
      key,
      "RS256",
      issuer,
      audience,
      issuedAt.getEpochSecond,
      expireAt.getEpochSecond,
      "read:appointments")

    (ctx ! Auth.fromJwt(
      token.getToken + "xyz",
      jwkProvider(jwksUri, key))) shouldBe None
  }

  "forbid if JWT alg is not the same used to sign" in {
    val roleName = aName.sample
    val accessName = aName.sample
    val userName = aName.sample

    ctx ! mkRole(
      auth,
      roleName,
      Seq(
        MkObject(
          "resource" -> ClassesNativeClassRef,
          "actions" -> MkObject("read" -> true)
        )))

    val key = aJWK sample
    val issuer = s"https://$userName.auth0.com"
    val jwksUri = s"$issuer/.well-known/jwks.json"
    val audience = JWTToken.canonicalDBUrl(db)

    ctx ! mkAccessProvider(auth, accessName, issuer, jwksUri, Seq(RoleRef(roleName)))

    // jwt specify RS256 on header
    val header = JSObject(
      JWTFields.Algorithm -> "RS256",
      JWTFields.Type -> "JWT",
      JWTFields.KeyID -> key.kid
    )

    val payload = JSObject(
      JWTFields.Issuer -> issuer,
      JWTFields.Audience -> audience,
      JWTFields.IssuedAt -> Instant.now().getEpochSecond,
      JWTFields.ExpiresAt -> Instant.now().plusSeconds(60).getEpochSecond,
      JWTFields.Scope -> "read:appointments"
    )

    // jwt is signed with RS384
    val token = JWT.createToken(header, payload, key, "RS384").getToken

    (ctx ! Auth.fromJwt(token, jwkProvider(jwksUri, key))) shouldBe None
  }
}
