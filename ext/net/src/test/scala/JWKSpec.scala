package fauna.net.test

import fauna.net.security._
import fauna.util.Base64
import java.nio.charset.StandardCharsets
import java.security.KeyPairGenerator
import java.security.interfaces.{ RSAPrivateKey, RSAPublicKey }
import fauna.codex.json.JSObject

class JWKSpec extends Spec {

  "RSA" - {

    "create from public/private key" in {
      val (publicKey, privateKey) = generateRandomKey()

      val key = RSAJWK("id", publicKey, privateKey)

      key.kid shouldBe Some("id")
      key.use shouldBe Some("sig")
      key.n shouldBe Some(Base64.encodeUrlSafe(publicKey.getModulus.toByteArray))
      key.e shouldBe Some(Base64.encodeUrlSafe(publicKey.getPublicExponent.toByteArray))
      key.d shouldBe Some(Base64.encodeUrlSafe(privateKey.getPrivateExponent.toByteArray))
    }

    "create from parameters" in {
      val (publicKey, privateKey) = generateRandomKey()

      val key = RSAJWK(JSObject(
        JWKFields.KeyType -> "id",
        JWKFields.Use -> "sig",
        JWKFields.Modulus -> Base64.encodeUrlSafe(publicKey.getModulus.toByteArray),
        JWKFields.PublicExponent -> Base64.encodeUrlSafe(publicKey.getPublicExponent.toByteArray),
        JWKFields.PrivateExponent -> Base64.encodeUrlSafe(privateKey.getPrivateExponent.toByteArray)
      ))

      key.publicKey.isDefined shouldBe true
      key.privateKey.isDefined shouldBe true

      key.publicKey.get.getPublicExponent shouldBe publicKey.getPublicExponent
      key.publicKey.get.getModulus shouldBe publicKey.getModulus
      key.publicKey.get.getAlgorithm shouldBe publicKey.getAlgorithm

      key.privateKey.get.getPrivateExponent shouldBe privateKey.getPrivateExponent
      key.privateKey.get.getModulus shouldBe privateKey.getModulus
      key.privateKey.get.getAlgorithm shouldBe privateKey.getAlgorithm
    }

    val Header = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9"
    val Payload = "eyJ4eCI6Inl5In0"
    val Signature256 = "AYIUc9b09bsTJ5sighleNVIUgTL17Sa1NQFNayEA5kXkRaJuMT4KfHGAtJxCwPrRzkkeZkOP1oTkjvEUJFfgQq" +
                           "3JhYFgnp0A9SxXLuqf6DDwfpWq9qyHqVFHD4cLPyRGBu0aU80jZOQqC76Jq3j6Jxj15mBFL8Q5ww9xyadY4Smb" +
                           "jUAQFLeDGdujn9XEFzMkz2J6LCOY1RREmVQ4p4MRUmoWufUgyhYG9VsamFYKcNvOpQ2-OstiHmz2H73wewVdqo" +
                           "EhRRF75WlxsDdx-VIMS6B0_nyoWE_N-8-mDdH1OcA4zEIoWfPQM118VlpxaJAzTlSgohV9Z4sfxocYITCyzQ"
    val Signature384 = "oMkuYcTwx8SqAIMHgEP3jm021TyGa0Xafbt2lAKnmzudpg_Cb-LgOmVf5sV2fv6Qm2F28EfV4Dqbk3op3A8FhX" +
                           "wqTcoFXXZDl9yf3yqNtHXleEHqSpuS2sd5s4rzJCnT6Lwnzg5ciOzfdXaZilMENb5_Qm2_vtVrQwEqg7DluVpt" +
                           "fUOyF4Ys-8bBqvQ_g4PM_ugxpo8tQEpIYXn5wcOU8SkLfVsjAZMTSMZ1fMzIrvqj_qULghF1q3JiMcBj3uht2B" +
                           "kzihpTLxcMcISxyHnrc9aqZGdBTCxLBTJHVkGdEEb7FtosJkmy2WJQY5qntEd3u9lu_1uGm2x6o3JbNf-PEg"
    val Signature512 = "gvNxKUyXygaZsAt5hSzwkkFNKZw2Gy4uWHR5wZ0GoOFXHniVrrcW7IpOUXymqzFH-0Rr3bYPh4KJd8cmfPRcAM" +
                           "X6QCxhsRYbnM17We4n4PAywiXds1gjPWB70hgAgQT2Ru438m9W-kWt3lVMbvxNiQ0L3f72XXjn7YDED2Re_yxy" +
                           "-epT1kXm1Q8fhfczh2NQ5YyDXP1kCfRqRW4KE6rAiHyEbMzM1RzXYhbNMjO-SPNT2cG5XltJtvSPkbu_cMBjuI" +
                           "N05kTx1wTjx9Dkk1wUe0uA8b4c4-YNFSNY_gMbRt_lLrZOQ1wRpPc3rNAqOnfmoFea_Qm9pSE_fUtmHOczEg"
    val Modulus =   "AKaAsaDSsIFnz43UNIq_3uOq4uEolqr4KHQQ6jsudcQ5JlTG6JvEOar6IbploBiqeDNU-1wqTay8pFf1T_9K3h" +
                           "xvGQIBFp-63y3a1kerNSjU7VjNLjggRfvGFfDsjZe05GWbGfVkhDofYaEsM6_B5IQo62flp6aCPTs5WVZ3Kbhq" +
                           "0PlMTb8NEpsvmk_0kciouptYLBpP3IGVc0OwbJqJIFX6_H2Zg-dYicymm5Bwhi_KeQftsUmnSptmJoxCwoTDSM" +
                           "yQmXN0Y9H6n23l1_S-LHwOVFcyrikydiMGENQF02gs7VCbQxny_msUeVyFEgj8MhUzhU_MfcayFC33A3Ebq-c"
    val Private =   "MlLzXQhREMuvj85zkvwrAeTEbgk2gLZAg9QY94ozTft3-mekJnBIfcTcLNpKaSoc6mblEhY3I95gTbV3WFHoj" +
                           "--EP3bUv7JbgiCCY2b7yjuRASYCsjQO8uoL96WHpjKmVY9raR_YVzjk6THS7WxNBSTZAa4H3eu_VgcfWWLq2T" +
                           "HyqkKnfbXqS6GsreSi_TqvNJYcRxHHFF2cKDoLBVX7BS54QOsgf6CjmBw4tcPh5IDlufccwZlZSj1bP4TyGVw" +
                           "qQ_U23Wo3G8iAaDtk4bTNGTJYPesRsR7YQTytZAiG3wbPaPl8Z6sL81Yek0QAfHT0qAdSpaTgdxTXlR45198wAQ"
    val Public = "AQAB"

    "verify works" in {
      val header = Header.getBytes(StandardCharsets.UTF_8)
      val payload = Payload.getBytes(StandardCharsets.UTF_8)

      val key = RSAJWK(JSObject(
        JWKFields.KeyID -> "id",
        JWKFields.Use -> "sig",
        JWKFields.Modulus -> Modulus,
        JWKFields.PublicExponent -> Public,
        JWKFields.PrivateExponent -> Private
      ))

      noException should be thrownBy {
        key.verify("RS256", header, payload, Base64.decodeUrlSafe(Signature256)) shouldBe true
        key.verify("RS384", header, payload, Base64.decodeUrlSafe(Signature384)) shouldBe true
        key.verify("RS512", header, payload, Base64.decodeUrlSafe(Signature512)) shouldBe true
      }
    }

    "verify fail with invalid signature" in {
      val header = Header.getBytes(StandardCharsets.UTF_8)
      val payload = Payload.getBytes(StandardCharsets.UTF_8)
      val signature256 = Base64.decodeUrlSafe(Signature256)
      val signature384 = Base64.decodeUrlSafe(Signature384)
      val signature512 = Base64.decodeUrlSafe(Signature512)

      //mess with the signature
      signature256(0) = 0
      signature384(0) = 0
      signature512(0) = 0

      val key = RSAJWK(JSObject(
        JWKFields.KeyID -> "id",
        JWKFields.Use -> "sig",
        JWKFields.Modulus -> Modulus,
        JWKFields.PublicExponent -> Public,
        JWKFields.PrivateExponent -> Private
      ))

      key.verify("RS256", header, payload, signature256) shouldBe false
      key.verify("RS384", header, payload, signature384) shouldBe false
      key.verify("RS512", header, payload, signature512) shouldBe false
    }

    "verify fail with different algorithm" in {
      val header = Header.getBytes(StandardCharsets.UTF_8)
      val payload = Payload.getBytes(StandardCharsets.UTF_8)
      val signature = Base64.decodeUrlSafe(Signature256)

      val key = RSAJWK(JSObject(
        JWKFields.KeyID -> "id",
        JWKFields.Use -> "sig",
        JWKFields.Modulus -> Modulus,
        JWKFields.PublicExponent -> Public,
        JWKFields.PrivateExponent -> Private
      ))

      key.verify("RS384", header, payload, signature) shouldBe false
    }

    "verify fail with unsupported algorithm" in {
      val header = Header.getBytes(StandardCharsets.UTF_8)
      val payload = Payload.getBytes(StandardCharsets.UTF_8)
      val signature = Base64.decodeUrlSafe(Signature256)

      val key = RSAJWK(JSObject(
        JWKFields.KeyID -> "id",
        JWKFields.Use -> "sig",
        JWKFields.Modulus -> Modulus,
        JWKFields.PublicExponent -> Public,
        JWKFields.PrivateExponent -> Private
      ))

      a[AlgorithmNotSupported] should be thrownBy {
        key.verify("HS256", header, payload, signature) shouldBe false
      }
    }

    "sign" in {
      val header = Header.getBytes(StandardCharsets.UTF_8)
      val payload = Payload.getBytes(StandardCharsets.UTF_8)

      val key = RSAJWK(JSObject(
        JWKFields.KeyID -> "id",
        JWKFields.Use -> "sig",
        JWKFields.Modulus -> Modulus,
        JWKFields.PublicExponent -> Public,
        JWKFields.PrivateExponent -> Private
      ))

      key.sign("RS256", header, payload) shouldBe Base64.decodeUrlSafe(Signature256)
      key.sign("RS384", header, payload) shouldBe Base64.decodeUrlSafe(Signature384)
      key.sign("RS512", header, payload) shouldBe Base64.decodeUrlSafe(Signature512)
    }
  }

  def generateRandomKey() = {
    val kpg = KeyPairGenerator.getInstance("RSA")
    kpg.initialize(2048)
    val keyPair = kpg.generateKeyPair()

    (keyPair.getPublic.asInstanceOf[RSAPublicKey], keyPair.getPrivate.asInstanceOf[RSAPrivateKey])
  }
}
