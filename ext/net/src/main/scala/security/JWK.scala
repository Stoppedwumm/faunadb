package fauna.net.security

import fauna.codex.json._
import fauna.util.Base64
import java.math.BigInteger
import java.net.URL
import java.security.{ KeyFactory, KeyPair, PrivateKey, PublicKey, Signature, SignatureException }
import java.security.interfaces.{ RSAPrivateKey, RSAPublicKey }
import java.security.spec.{ RSAPrivateKeySpec, RSAPublicKeySpec }
import scala.util.control.{ NoStackTrace, NonFatal }

abstract class JWKException(msg: String) extends Exception(msg) with NoStackTrace

final class JWKNotFound extends JWKException("JWK not found")
final class JWKNotConfigured extends JWKException("JWK not configured")

final class JWKProviderNotConfigured
    extends JWKException("JWK provider not configured")

final class JWKSError(uri: URL, cause: String)
    extends JWKException(s"Cannot download JWKS from $uri because of $cause")

final class InvalidModulus
    extends JWKException("Public Key and Private Key has different modulus")

final class AlgorithmNotSupported(alg: String)
    extends JWKException(s"Algorithm `$alg` not supported")

object JWKFields {
  val KeyType = "kty"
  val KeyID = "kid"
  val Use = "use"

  //rsa fields
  val Modulus = "n"
  val PublicExponent = "e"
  val PrivateExponent = "d"
}

/**
  * JSON Web Key
  *
  * https://tools.ietf.org/html/rfc7517
  */
sealed trait JWK {
  val kid: Option[String] //key id
  val use: Option[String] //usage

  def verify(jwt: JWT): Boolean =
    verify(jwt.getAlgorithm, jwt.headerBytes, jwt.payloadBytes, jwt.signatureBytes)

  def verify(algorithm: String, header: Array[Byte], payload: Array[Byte], signature: Array[Byte]): Boolean
  def sign(algorithm: String, header: Array[Byte], payload: Array[Byte]): Array[Byte]

  def toJSON: JSValue
}

object JWK {
  def rsa(kid: String, publicKey: PublicKey, privateKey: PrivateKey): JWK =
    RSAJWK(kid, publicKey.asInstanceOf[RSAPublicKey], privateKey.asInstanceOf[RSAPrivateKey])

  def rsa(kid: String, keyPair: KeyPair): JWK =
    rsa(kid, keyPair.getPublic, keyPair.getPrivate)

  def verifySignature(
    algorithm: String,
    publicKey: PublicKey,
    headerBytes: Array[Byte],
    payloadBytes: Array[Byte],
    signatureBytes: Array[Byte]): Boolean = try {
    val s = Signature.getInstance(algorithm)
    s.initVerify(publicKey)
    s.update(headerBytes)
    s.update('.'.toByte)
    s.update(payloadBytes)
    s.verify(signatureBytes)
  } catch {
    case _: SignatureException =>
      false
  }

  def createSignature(
    algorithm: String,
    privateKey: PrivateKey,
    headerBytes: Array[Byte],
    payloadBytes: Array[Byte]): Array[Byte] = try {
    val s = Signature.getInstance(algorithm)
    s.initSign(privateKey)
    s.update(headerBytes)
    s.update('.'.toByte)
    s.update(payloadBytes)
    s.sign
  } catch {
    case _: SignatureException =>
      Array.empty[Byte]
  }
}

object RSAJWK {
  import JWKFields._

  /**
    * Create the key from public/private keys
    */
  def apply(kid: String, publicKey: RSAPublicKey, privateKey: RSAPrivateKey): RSAJWK = {
    if (publicKey.getModulus != privateKey.getModulus) {
      throw new InvalidModulus
    }

    val n = Base64.encodeUrlSafe(publicKey.getModulus.toByteArray)
    val e = Base64.encodeUrlSafe(publicKey.getPublicExponent.toByteArray)
    val d = Base64.encodeUrlSafe(privateKey.getPrivateExponent.toByteArray)

    RSAJWK(Some(kid), Some("sig"), Some(n), Some(e), Some(d), Some(publicKey), Some(privateKey))
  }

  /**
    * Parse the key from the JSON
    */
  def apply(key: JSValue): RSAJWK = {
    val n = (key / Modulus).asOpt[String]
    val e = (key / PublicExponent).asOpt[String]
    val d = (key / PrivateExponent).asOpt[String]

    RSAJWK(
      (key / KeyID).asOpt[String],
      (key / Use).asOpt[String],       //ie: sig | enc
      n, e, d,
      publicKey(n, e),
      privateKey(n, d)
    )
  }

  private def publicKey(n: Option[String], e: Option[String]) = (n, e) match {
    case (Some(n), Some(e)) =>
      try {
        val kf = KeyFactory.getInstance("RSA")
        val modulus = new BigInteger(1, Base64.decodeUrlSafe(n))
        val exponent = new BigInteger(1, Base64.decodeUrlSafe(e))

        Some(kf.generatePublic(new RSAPublicKeySpec(modulus, exponent)).asInstanceOf[RSAPublicKey])
      } catch {
        case NonFatal(_) =>
          None
      }
    case _ =>
      None
  }

  private def privateKey(n: Option[String], d: Option[String]) = (n, d) match {
    case (Some(n), Some(d)) =>
      try {
        val kf = KeyFactory.getInstance("RSA")
        val modulus = new BigInteger(1, Base64.decodeUrlSafe(n))
        val exponent = new BigInteger(1, Base64.decodeUrlSafe(d))

        Some(kf.generatePrivate(new RSAPrivateKeySpec(modulus, exponent)).asInstanceOf[RSAPrivateKey])
      } catch {
        case NonFatal(_) =>
          None
      }
    case _ =>
      None
  }
}

final case class RSAJWK(
  kid: Option[String], //key id
  use: Option[String], //usage
  n: Option[String],   //modulus
  e: Option[String],   //public exponent
  d: Option[String],   //private exponent
  publicKey: Option[RSAPublicKey],
  privateKey: Option[RSAPrivateKey]
) extends JWK {

  import JWKFields._

  def verify(algorithm: String, header: Array[Byte], payload: Array[Byte], signature: Array[Byte]): Boolean = {
    publicKey.fold(false) {
      JWK.verifySignature(algToRSA(algorithm), _, header, payload, signature)
    }
  }

  def sign(algorithm: String, header: Array[Byte], payload: Array[Byte]): Array[Byte] = {
    privateKey.fold(Array.empty[Byte]) {
      JWK.createSignature(algToRSA(algorithm), _, header, payload)
    }
  }

  private def algToRSA(algorithm: String): String = algorithm match {
    case "RS256" => "SHA256withRSA"
    case "RS384" => "SHA384withRSA"
    case "RS512" => "SHA512withRSA"
    case alg     => throw new AlgorithmNotSupported(alg)
  }

  def toJSON: JSValue = {
    val fields = Seq.newBuilder[(String, JSValue)]

    fields += KeyType -> "RSA"
    kid foreach { fields += KeyID -> _ }
    use foreach { fields += Use -> _ }
    n foreach { fields += Modulus -> _ }
    e foreach { fields += PublicExponent -> _ }
    d foreach { fields += PrivateExponent -> _ }

    JSObject(fields.result(): _*)
  }
}

object JWKSFields {
  val KeyType = "kty"
  val Keys = "keys"
}

/**
  * JSON Web Key Set
  */
final case class JWKS(keys: Seq[JWK]) {
  import JWKSFields._

  def findJWK(kid: Option[String]): Option[JWK] = {
    if (keys.sizeIs == 1 && kid.isEmpty) {
      keys.headOption
    } else {
      keys find { _.kid == kid }
    }
  }

  def toJSON: JSValue = {
    JSObject(Keys -> keys.map { _.toJSON })
  }
}

object JWKS {
  import JWKSFields._

  def apply(value: JSValue): JWKS = {
    val keys = (value / Keys).asOpt[Seq[JSValue]].toList.flatten flatMap { key =>
      val kty = (key / KeyType).asOpt[String]

      kty match {
        case Some("RSA") => Some(RSAJWK(key))
        case _           => None
      }
    }

    JWKS(keys)
  }
}
