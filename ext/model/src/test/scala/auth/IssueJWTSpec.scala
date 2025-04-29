package fauna.model.auth.test

import fauna.ast._
import fauna.auth._
import fauna.codex.json.{ JSNull, JSValue }
import fauna.model._
import fauna.model.test._
import fauna.net.http.BearerAuth
import fauna.net.security._
import fauna.prop.{ Generators, Prop }
import fauna.repo.test.CassandraHelper
import fauna.util.BCrypt
import java.net.URL
import java.time.Instant
import java.time.temporal.ChronoUnit
import org.scalatest.concurrent.Eventually
import scala.concurrent.duration._
import scala.concurrent.Future

class IssueJWTSpec extends Spec with Generators with Eventually {

  import SocialHelpersUnstable._

  val signingJWK = aJWK.sample

  val ctx = CassandraHelper.context("model", internalJWK = Some((signingJWK, "RS256")))

  def assertJWT(auth: Auth, issuer: String, subject: JSValue, scope: String) = {
    val now = Instant.now().truncatedTo(ChronoUnit.SECONDS)

    //with default expire_at
    val jwt1 = issueAccessJWT(auth, subject)
    jwt1.getKeyId shouldBe signingJWK.kid
    jwt1.getIssuer shouldBe Some(issuer)
    jwt1.getAudience shouldBe Seq(issuer)
    jwt1.getAlgorithm shouldBe "RS256"
    jwt1.getIssuedAt should be >= now
    jwt1.getExpiresAt shouldBe Some(jwt1.getIssuedAt.plusSeconds(1800))
    jwt1.getClaim(JWTFields.Scope) shouldBe Some(scope)

    //setting expire_at
    val jwt2 = issueAccessJWT(auth, subject, 5000)
    jwt2.getKeyId shouldBe signingJWK.kid
    jwt2.getIssuer shouldBe Some(issuer)
    jwt2.getAudience shouldBe Seq(issuer)
    jwt2.getAlgorithm shouldBe "RS256"
    jwt2.getIssuedAt should be >= now
    jwt2.getExpiresAt shouldBe Some(jwt2.getIssuedAt.plusSeconds(5000))
    jwt2.getClaim(JWTFields.Scope) shouldBe Some(scope)
  }

  "issue JWT for a role" in {
    val scope = ctx ! newScope(RootAuth)
    val serverAuth = Auth.forScope(scope)
    val adminAuth = Auth.adminForScope(scope)

    val collName = aName.sample
    val roleName = aName.sample

    ctx ! mkCollection(adminAuth, MkObject("name" -> collName))

    ctx ! mkRole(adminAuth, roleName, Seq(MkObject(
      "resource" -> ClassRef(collName),
      "actions" -> MkObject("read" -> true)
    )))

    val issuer = s"https://db.fauna.com/db/${Database.encodeGlobalID(scope)}"

    assertJWT(adminAuth, issuer, RoleRef(roleName), s"@role/$roleName")
    assertJWT(serverAuth, issuer, RoleRef(roleName), s"@role/$roleName")
  }

  "issue JWT for a document" in {
    val scope = ctx ! newScope(RootAuth)
    val serverAuth = Auth.forScope(scope)
    val adminAuth = Auth.adminForScope(scope)

    val collName = aName.sample

    ctx ! mkCollection(adminAuth, MkObject("name" -> collName))
    val doc = ctx ! mkDoc(adminAuth, collName)

    val issuer = s"https://db.fauna.com/db/${Database.encodeGlobalID(scope)}"

    assertJWT(adminAuth, issuer, doc.refObj, doc.docRef)
    assertJWT(serverAuth, issuer, doc.refObj, doc.docRef)
  }

  "doesn't allow tokens issue JWT" in {
    val scope = ctx ! newScope(RootAuth)
    val auth = Auth.adminForScope(scope)

    val collName = aName.sample

    ctx ! mkCollection(auth, MkObject("name" -> collName))
    val doc = ctx ! mkDoc(auth, collName, MkObject("credentials" -> MkObject("password" -> "secret")))

    val login = ctx ! loginAs(auth, doc, "secret")
    val tokenAuth = getAuth(login.secret)

    evalIssueAccessJWT(tokenAuth, doc.refObj) shouldBe Left(List(PermissionDenied(Right(RefL(scope, doc.id)), RootPosition)))
  }

  "doesn't allow client keys issue JWT" in {
    val scope = ctx ! newScope(RootAuth)
    val auth = Auth.adminForScope(scope)

    val collName = aName.sample

    ctx ! mkCollection(auth, MkObject("name" -> collName))
    val doc = ctx ! mkDoc(auth, collName)

    val (secret, _) = ctx ! mkCurrentDBKey("client", auth)
    val keyAuth = getAuth(secret)

    evalIssueAccessJWT(keyAuth, doc.refObj) shouldBe Left(List(PermissionDenied(Right(RefL(scope, doc.id)), RootPosition)))
  }

  "doesn't allow server-readonly keys issue JWT" in {
    val scope = ctx ! newScope(RootAuth)
    val auth = Auth.adminForScope(scope)

    val collName = aName.sample

    ctx ! mkCollection(auth, MkObject("name" -> collName))
    val doc = ctx ! mkDoc(auth, collName)

    val (secret, _) = ctx ! mkCurrentDBKey("server-readonly", auth)
    val keyAuth = getAuth(secret)

    evalIssueAccessJWT(keyAuth, doc.refObj) shouldBe Left(List(PermissionDenied(Right(RefL(scope, doc.id)), RootPosition)))
  }

  "doesn't allow JWT issue JWT" in pendingUntilFixed {
    val scope = ctx ! newScope(RootAuth)
    val adminAuth = Auth.adminForScope(scope)

    val collName = aName.sample
    val accessName = aName.sample
    val userName = aName.sample

    ctx ! mkCollection(adminAuth, MkObject("name" -> collName))
    val doc = ctx ! mkDoc(adminAuth, collName)

    val issuer = s"https://$userName.auth0.com"
    val jwksUri = s"$issuer/.well-known/jwks.json"

    ctx ! mkAccessProvider(adminAuth, accessName, issuer, jwksUri, Seq.empty)

    //issue a JWT using admin key
    val token = issueAccessJWT(adminAuth, doc.refObj).getToken
    val jwtAuth = getAuth(token)

    //issue a JWT using a JWT
    evalIssueAccessJWT(jwtAuth, doc.refObj) shouldBe Left(List(PermissionDenied(Right(RefL(scope, doc.id)), RootPosition)))
  }

  "doesn't allow roles issue JWT" in {
    val scope = ctx ! newScope(RootAuth)
    val auth = Auth.adminForScope(scope)

    val collName = aName.sample
    val roleName = aName.sample

    ctx ! mkCollection(auth, MkObject("name" -> collName))

    ctx ! mkRole(auth, roleName, Seq(MkObject(
      "resource" -> ClassRef(collName),
      "actions" -> MkObject("read" -> true)
    )))

    val doc = ctx ! mkDoc(auth, collName)

    val (secret, _) = ctx ! mkCurrentDBKey(RoleRef(roleName), auth)
    val keyAuth = getAuth(secret)

    evalIssueAccessJWT(keyAuth, doc.refObj) shouldBe Left(List(PermissionDenied(Right(RefL(scope, doc.id)), RootPosition)))
  }

  "JWT should invalidate after expireIn seconds" in {
    val scope = ctx ! newScope(RootAuth)
    val adminAuth = Auth.adminForScope(scope)

    val collName = aName.sample
    val accessName = aName.sample
    val userName = aName.sample

    ctx ! mkCollection(adminAuth, MkObject("name" -> collName))
    val doc = ctx ! mkDoc(adminAuth, collName)

    val issuer = s"https://$userName.auth0.com"
    val jwksUri = s"$issuer/.well-known/jwks.json"

    ctx ! mkAccessProvider(adminAuth, accessName, issuer, jwksUri, Seq.empty)

    val jwt = issueAccessJWT(adminAuth, doc.refObj, 10L)

    eventually(timeout(20.seconds), interval(1.second)) {
      tryGetAuth(jwt.getToken) shouldBe None
    }
  }

  "unresolved refs" in {
    val scope = ctx ! newScope(RootAuth)
    val adminAuth = Auth.adminForScope(scope)

    val collName = aName.sample

    ctx ! mkCollection(adminAuth, MkObject("name" -> collName))
    val doc = ctx ! mkDoc(adminAuth, collName)

    val otherScope = ctx ! newScope(RootAuth)
    val otherAuth = Auth.adminForScope(otherScope)

    //doc.refObj is not visible on otherAuth
    evalIssueAccessJWT(otherAuth, doc.refObj, 10L) match {
      case Right(_)          => fail("expected an error")
      case Left(List(error)) => error shouldBe a[UnresolvedRefError]
      case Left(errors)      => fail(s"Unexpected errors $errors.")
    }
  }

  "validate expire_in" in {
    val scope = ctx ! newScope(RootAuth)
    val adminAuth = Auth.adminForScope(scope)

    val collName = aName.sample

    ctx ! mkCollection(adminAuth, MkObject("name" -> collName))
    val doc = ctx ! mkDoc(adminAuth, collName)

    evalIssueAccessJWT(adminAuth, doc.refObj, 9) shouldBe Left(List(BoundsError("expire in", ">= 10 and <= 7200", RootPosition at "expire_in")))
    evalIssueAccessJWT(adminAuth, doc.refObj, 7201) shouldBe Left(List(BoundsError("expire in", ">= 10 and <= 7200", RootPosition at "expire_in")))
  }

  "validate subject" in {
    val scope = ctx ! newScope(RootAuth)
    val adminAuth = Auth.adminForScope(scope)

    evalIssueAccessJWT(adminAuth, 10L) shouldBe Left(List(InvalidArgument(List(Type.Ref), Type.Integer, RootPosition at "issue_access_jwt")))
    evalIssueAccessJWT(adminAuth, "string") shouldBe Left(List(InvalidArgument(List(Type.Ref), Type.String, RootPosition at "issue_access_jwt")))
  }

  def getAuth(secret: String): Auth =
    tryGetAuth(secret).get

  def tryGetAuth(secret: String): Option[Auth] = {
    val jwkProvider = new JWKProvider() {
      override def getJWK(jwksUrl: URL, kid: Option[String]): Future[JWK] = {
        Future.successful(signingJWK)
      }
    }

    val root = BCrypt.hash("secret")
    ctx ! Auth.fromInfo(BearerAuth(secret), List(root), jwkProvider)
  }

  def issueAccessJWT(auth: Auth, subject: JSValue, expireIn: JSValue = JSNull): JWT = {
    evalIssueAccessJWT(auth, subject, expireIn) match {
      case Right(ObjectL(List("token" -> StringL(token)))) =>
        JWT(token)

      case v =>
        fail(s"Unexpected $v")
    }
  }

  def evalIssueAccessJWT(auth: Auth, ref: JSValue, expireIn: JSValue = JSNull) = {
    val issue = if (expireIn != JSNull) {
      IssueAccessJWT(ref, expireIn)
    } else {
      IssueAccessJWT(ref)
    }

    ctx ! evalQuery(auth, issue)
  }

  def aJWK = for {
    keyPair <- Prop.aRSAKeyPair()
    kid <- Prop.hexString()
  } yield { JWK.rsa(kid, keyPair) }
}
