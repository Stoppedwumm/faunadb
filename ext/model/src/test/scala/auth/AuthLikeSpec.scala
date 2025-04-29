package fauna.model.auth.test

import fauna.atoms._
import fauna.auth._
import fauna.model.test.AuthGenerators
import fauna.net.security.JWT
import fauna.prop.PropConfig
import fauna.repo.test._

class AuthLikeSpec extends Spec with AuthGenerators {
  "AuthLike" - {
    "can decode keys" - {
      val key = KeyLike(GlobalKeyID(Long.MaxValue), new Array[Byte](20))

      "current" in {
        val dec = AuthLike.fromBase64("fnB__________wAAAAAAAAAAAAAAAAAAAAAAAAAA")
        dec.isDefined should be(true)
        dec.get should equal (key)
      }

      "legacy" in {
        val dec = AuthLike.fromBase64("kqnPf_________-0AAAAAAAAAAAAAAAAAAAAAAAAAAA")
        dec.isDefined should be(true)
        dec.get should equal (key)
      }

      "enforces correct size" in {
        AuthLike.fromBase64("fnB__________wAAAAAAAAAAAAAAAAAAAAAAAAAA EXTRA").isEmpty should be(true)
        AuthLike.fromBase64("kqnPf_________-0AAAAAAAAAAAAAAAAAAAAAAAAAAA EXTRA").isEmpty should be(true)
      }
    }

    "can decode tokens" - {
      val token = TokenLike(TokenID(Long.MaxValue), GlobalDatabaseID.MaxValue, new Array[Byte](20))

      "current" in {
        val dec = AuthLike.fromBase64("fnF__________3__________AAAAAAAAAAAAAAAAAAAAAAAAAAA")
        dec.isDefined should be(true)
        dec.get should equal (token)
      }

      "legacy" in {
        val dec = AuthLike.fromBase64("k6p__________wADqc9__________7QAAAAAAAAAAAAAAAAAAAAAAAAAAA")
        dec.isDefined should be(true)
        dec.get should equal (token)
      }

      "enforces correct size" in {
        AuthLike.fromBase64("fnF__________3__________AAAAAAAAAAAAAAAAAAAAAAAAAAA EXTRA").isEmpty should be(true)
        AuthLike.fromBase64("k6p__________wADqc9__________7QAAAAAAAAAAAAAAAAAAAAAAAAAAA EXTRA").isEmpty should be(true)
      }
    }

    "fauna auth JWT" - {
      "detects non JWT token" in {
        AuthLike.isJWT(
          TokenLike(TokenID.MaxValue, GlobalDatabaseID.MaxValue).toBase64) shouldBe false
        AuthLike.isJWT(KeyLike(GlobalKeyID.MaxValue).toBase64) shouldBe false
        AuthLike.isJWT("secret") shouldBe false
        AuthLike.isJWT("ey.xx.yy") shouldBe true
      }

      "is builtin" in {
        implicit val prop = PropConfig()

        val jwt = JWT.createToken(
          key = aJWK.sample,
          "RS256",
          issuer = "issuer",
          audience = "audience",
          issuedAt = 0,
          expireAt = 0,
          scope = ""
        )

        AuthLike.isJWT(jwt.getToken) shouldBe true
      }
    }
  }
}
