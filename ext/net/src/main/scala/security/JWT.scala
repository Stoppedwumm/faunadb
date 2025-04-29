package fauna.net.security

import fauna.codex.json._
import fauna.codex.json2.JSON
import fauna.util.Base64
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util
import java.util.regex.Pattern
import scala.util.control.NonFatal
import scala.util.Success

abstract class JWTException(msg: String) extends Exception(msg)
class InvalidJWT extends JWTException("Invalid token")

object JWTFields {
  val KeyID = "kid"
  val Type = "typ"
  val Algorithm = "alg"
  val Issuer = "iss"
  val IssuedAt = "iat"
  val ExpiresAt = "exp"
  val NotBefore = "nbf"
  val Audience = "aud"
  val Scope = "scope"
  val Subject = "sub"
}

/**
  * JSON Web Token
  */
case class JWT(
  header: JSValue,
  payload: JSValue,
  headerBytes: Array[Byte],
  payloadBytes: Array[Byte],
  signatureBytes: Array[Byte]) {

  import JWTFields._

  def getKeyId: Option[String] = (header / KeyID).asOpt[String]
  def getAlgorithm: String = (header / Algorithm).as[String]

  def getIssuer: Option[String] = (payload / Issuer).asOpt[String]
  def getIssuedAt: Instant = Instant.ofEpochSecond((payload / IssuedAt).as[Long])
  def getExpiresAt: Option[Instant] =
    (payload / ExpiresAt).asOpt[Long].map { Instant.ofEpochSecond }
  def getNotBefore: Option[Instant] = (payload / NotBefore).asOpt[Long] map {
    Instant.ofEpochSecond
  }
  def getAudience: Seq[String] = (payload / Audience) match {
    case a: JSArray  => a.value map { _.as[String] }
    case s: JSString => Seq(s.value)
    case _           => Seq.empty
  }
  def getClaim(claim: String): Option[String] = (payload / claim).asOpt[String]

  def getToken: String = {
    val headerBase64 = new String(headerBytes, StandardCharsets.ISO_8859_1)
    val payloadBase64 = new String(payloadBytes, StandardCharsets.ISO_8859_1)
    val signatureBase64 = Base64.encodeUrlSafe(signatureBytes)

    s"$headerBase64.$payloadBase64.$signatureBase64"
  }

  override def equals(obj: Any): Boolean = obj match {
    case jwt: JWT =>
      util.Arrays.equals(headerBytes, jwt.headerBytes) &&
        util.Arrays.equals(payloadBytes, jwt.payloadBytes) &&
          util.Arrays.equals(signatureBytes, jwt.signatureBytes)

    case _ => false
  }
}

object JWT {
  import JWTFields._

  val TokenSplitPattern = Pattern.compile("\\.")

  def apply(token: String): JWT = {
    val parts = TokenSplitPattern.split(token)

    if (parts.lengthIs != 3) {
      throw new InvalidJWT
    }

    try {
      val header = Base64.decodeUrlSafe(parts(0))
      val payload = Base64.decodeUrlSafe(parts(1))

      val headerBytes = parts(0).getBytes(StandardCharsets.ISO_8859_1)
      val payloadBytes = parts(1).getBytes(StandardCharsets.ISO_8859_1)
      val signatureBytes = Base64.decodeUrlSafe(parts(2))

      (JSON.tryParse[JSValue](header),
        JSON.tryParse[JSValue](payload)) match {
        case (Success(header), Success(payload)) =>
          JWT(header, payload, headerBytes, payloadBytes, signatureBytes)

        case _ =>
          throw new InvalidJWT
      }
    } catch {
      case NonFatal(_) =>
        throw new InvalidJWT
    }
  }

  def createToken(
    header: JSValue,
    payload: JSValue,
    key: JWK,
    algorithm: String): JWT = {

    val headerBytes = header.toBase64Array
    val payloadBytes = payload.toBase64Array

    val signatureBytes = key.sign(
      algorithm,
      headerBytes,
      payloadBytes
    )

    JWT(header, payload, headerBytes, payloadBytes, signatureBytes)
  }

  def createToken(
    payload: JSObject,
    key: JWK,
    algorithm: String): JWT = {

    val header = JSObject(
      Algorithm -> algorithm,
      Type -> "JWT",
      KeyID -> key.kid
    )

    createToken(header, payload, key, algorithm)
  }

  def createToken(
    key: JWK,
    algorithm: String,
    issuer: String,
    audience: String,
    issuedAt: Long,
    expireAt: Long,
    scope: String): JWT = {

    val payload = JSObject(
      Issuer -> issuer,
      Audience -> audience,
      IssuedAt -> issuedAt,
      ExpiresAt -> expireAt,
      Scope -> scope
    )

    createToken(payload, key, algorithm)
  }
}
