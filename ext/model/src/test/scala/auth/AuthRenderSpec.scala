package fauna.model.auth.test

import fauna.auth.{ Auth, AuthRender, JWTToken }
import fauna.codex.json._
import fauna.model.Database
import fauna.model.test._
import fauna.model.test.AuthGenerators
import fauna.model.test.Spec
import fauna.net.http.{ AuthType, BasicAuth, BearerAuth }
import fauna.net.security.{ JWK, JWKProvider, JWKSError, JWT }
import fauna.prop.Generators
import fauna.repo.test.CassandraHelper
import fauna.util.BCrypt
import java.net.URL
import java.time.Instant
import scala.concurrent.Future

class AuthRenderSpec extends Spec with Generators with AuthGenerators {
  import SocialHelpersV4._

  val ctx = CassandraHelper.context("model")
  val scope = ctx ! newScope
  val db = (ctx ! Database.forScope(scope)).get

  val rootKey = "rootkey"
  val rootHash = BCrypt.hash(rootKey)

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

  def authLookup(auth: AuthType, jwkProvider: JWKProvider = null): Option[Auth] =
    ctx ! Auth.fromInfo(auth, List(rootHash), jwkProvider)

  def makeKey(db: String, role: String) = {
    ctx ! mkKey(db, role)
  }

  "Render Auth" - {
    "root key" in {
      val auth = authLookup(rootKey).get

      val json = ctx ! AuthRender.render(auth)

      (json / "key") shouldBe JSString("root")
    }

    "key" in {
      val (keySecret, _) = makeKey(db.name, "server")
      val auth = authLookup(keySecret).get

      val json = ctx ! AuthRender.render(auth)

      (json / "key").asOpt[JSValue].isDefined shouldBe true
      (json / "auth_db") shouldBe JSNull
      (json / "instance") shouldBe JSNull
      (json / "role") shouldBe JSString("server")
    }

    "token" in {
      val admin = Auth.adminForScope(scope)
      ctx ! mkCollection(admin, MkObject("name" -> "coll"))
      val inst = ctx ! mkDoc(admin, "coll", MkObject("credentials" -> MkObject("password" -> "sekrit")))

      val login = ctx ! loginAs(admin, inst, "sekrit")

      val auth = authLookup(login.secret).get

      val json = ctx ! AuthRender.render(auth)

      (json / "scope_db").asOpt[JSValue].isDefined shouldBe true
      (json / "tokens").asOpt[JSValue].isDefined shouldBe true
      (json / "instance").asOpt[JSValue].isDefined shouldBe true
      (json / "role").asOpt[JSValue].isDefined shouldBe true
    }

    "jwt" in {
      val key = aJWK sample
      val issuer = s"https://test.auth0.com"
      val jwksUri = s"$issuer/.well-known/jwks.json"
      val audience = JWTToken.canonicalDBUrl(db)

      val admin = Auth.adminForScope(scope)
      ctx ! mkRole(admin, "role", Seq.empty)
      ctx ! mkAccessProvider(admin, "ap", issuer, jwksUri, Seq(RoleRef("role")))

      val jwt = JWT.createToken(
        key = key,
        algorithm = "RS256",
        issuer = issuer,
        audience = audience,
        issuedAt = Instant.now().getEpochSecond,
        expireAt = Instant.now().getEpochSecond + 1000,
        scope = ""
      )

      val auth = authLookup(BearerAuth(jwt.getToken), jwkProvider(jwksUri, key)).get

      val json = ctx ! AuthRender.render(auth)

      (json / "jwt") shouldBe JSTrue
      (json / "scope_db").asOpt[JSValue].isDefined shouldBe true
      (json / "role").asOpt[JSValue].isDefined shouldBe true
    }
  }

  "Render AuthType" - {
    "key" in {
      val key = "fnB__________wAAAAAAAAAAAAAAAAAAAAAAAAAA"
      AuthRender.render(BasicAuth(key)).value shouldBe JSObject("key" -> "9223372036854775807")
    }

    "token" in {
      val token = "fnF__________3__________AAAAAAAAAAAAAAAAAAAAAAAAAAA"
      AuthRender.render(BasicAuth(token)).value shouldBe JSObject("token" -> "9223372036854775807",  "scope" -> "9223372036854775807")
    }

    "jwt" in {
      val jwt = JWT.createToken(
        key = aJWK.sample,
        algorithm = "RS256",
        issuer = "issuer",
        audience = "audience",
        issuedAt = 0,
        expireAt = 0,
        scope = ""
      )

      AuthRender.render(BearerAuth(jwt.getToken)).value shouldBe JSObject(
        "jwt" -> true,
        "audience" -> JSArray("audience"),
        "issuer" -> "issuer"
      )
    }
  }

}
