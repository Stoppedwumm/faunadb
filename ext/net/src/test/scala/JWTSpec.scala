package fauna.net.test

import fauna.codex.json.JSObject
import fauna.net.security._
import fauna.util.Base64
import java.time.Instant

class JWTSpec extends Spec {

  "JWT" - {
    "fail" in {
      a [InvalidJWT] should be thrownBy {
        JWT("header.payload")
      }

      val header = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9"
      val payload = "eyJ4eCI6Inl5In0"
      val signature = "AYIUc9b09bsTJ5sighleNVIUgTL17Sa1NQFNayEA5kXkRaJuMT4KfHGAtJxCwPrRzkkeZkOP1oTkjvEUJFfgQq3" +
                             "JhYFgnp0A9SxXLuqf6DDwfpWq9qyHqVFHD4cLPyRGBu0aU80jZOQqC76Jq3j6Jxj15mBFL8Q5ww9xyadY4SmbjU" +
                             "AQFLeDGdujn9XEFzMkz2J6LCOY1RREmVQ4p4MRUmoWufUgyhYG9VsamFYKcNvOpQ2-OstiHmz2H73wewVdqoEhR" +
                             "RF75WlxsDdx-VIMS6B0_nyoWE_N-8-mDdH1OcA4zEIoWfPQM118VlpxaJAzTlSgohV9Z4sfxocYITCyzQ"

      a [InvalidJWT] should be thrownBy {
        JWT(s"$header.error.$signature") //<-- invalid payload
      }

      a [InvalidJWT] should be thrownBy {
        JWT(s"error.$payload.$signature") //<-- invalid header
      }
    }

    "parse JWT string" in {
      val header = JSObject(
        JWTFields.KeyID -> "1234",
        JWTFields.Algorithm -> "FAUNA"
      ).toBase64String
      val payload = JSObject(
        JWTFields.Issuer -> "https://fauna.com",
        JWTFields.ExpiresAt -> Instant.parse("2020-01-01T00:00:00Z").getEpochSecond,
        JWTFields.NotBefore -> Instant.parse("2020-01-01T00:00:00Z").getEpochSecond,
        JWTFields.Audience -> Seq("https://db.fauna.com/db/xxxx", "another aud"),
        JWTFields.Scope -> "some scope"
      ).toBase64String
      val signature = Base64.encodeUrlSafe("signature".getBytes)

      val jwt = JWT(s"$header.$payload.$signature")

      jwt.getKeyId shouldBe Some("1234")
      jwt.getAlgorithm shouldBe "FAUNA"
      jwt.getIssuer shouldBe Some("https://fauna.com")
      jwt.getExpiresAt shouldBe Some(Instant.parse("2020-01-01T00:00:00Z"))
      jwt.getNotBefore shouldBe Some(Instant.parse("2020-01-01T00:00:00Z"))
      jwt.getAudience shouldBe Seq("https://db.fauna.com/db/xxxx", "another aud")
      jwt.getClaim(JWTFields.Scope) shouldBe Some("some scope")
      jwt.getClaim("invalid") shouldBe None
    }
  }

}
