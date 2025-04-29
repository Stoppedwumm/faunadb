package fauna.auth

import fauna.atoms._
import fauna.lang.syntax._
import fauna.util.{ BCrypt, Base64 }
import io.netty.buffer.Unpooled
import java.security.SecureRandom

/**
  * AuthLike handles decoding (and encoding) auth secrets off of the
  * wire.
  *
  * We hand-roll serialization here because secure decoding is hard.
  * This simpler decode logic should be easier to verify and audit
  * for security bugs.
  *
  * - Branches are minimized.
  * - Fixed buffer sizes are assumed and verified.
  *
  * Binary Format:
  *
  * keys
  *
  * | byte          | byte           | 8 bytes | 20 bytes |
  * | 126 (version) | 112 (key type) | key id  | secret   |
  *
  * tokens
  *
  * | byte          | byte             | 8 bytes  | 8 bytes  | 20 bytes |
  * | 126 (version) | 113 (token type) | token id | scope id | secret   |
  *
  * Legacy format is derived from MessagePack.
  */
sealed abstract class AuthLike {
  def toBase64: String

  def secret: String

  def hashedSecret: String = BCrypt.hash(secret)
}

object AuthLike {
  final val Version = 126

  object Type {
    final val Key = 112
    final val Token = 113

    final val LegacyKey = -110 // 2 elem array
    final val LegacyToken = -109 // 3 elem array
  }

  protected val rand = new SecureRandom

  def randomSecret = {
    val bytes = new Array[Byte](20)
    rand.nextBytes(bytes)
    bytes
  }

  /**
    * To make it fast to detect JWT tokens, here we are exploiting the base64 representation of it.
    * Base64 of "{" is "ey"
    */
  def isJWT(token: String) = (token startsWith "ey") && (token contains ".")

  def fromBase64(str: String): Option[AuthLike] =
    try {
      val b = Unpooled.wrappedBuffer(Base64.decodeUrlSafe(str))

      def readBoxedUInt64 =
        b.readByte match {
          case -87 => // 9-byte bytearray
            b.readByte match {
              case -49 => Some(b.readLong) // unsigned long
              case _   => None
            }
          case _ => None
        }

      b.readByte match {
        case Version =>
          b.readByte match {
            case Type.Key if b.readableBytes == 28 =>
              Some(KeyLike(GlobalKeyID(b.readLong), b.toByteArray))

            case Type.Token if b.readableBytes == 36 =>
              Some(
                TokenLike(TokenID(b.readLong), GlobalDatabaseID(b.readLong), b.toByteArray))

            case _ => None
          }

        case Type.LegacyKey if b.readableBytes == 31 =>
          readBoxedUInt64 match {
            case Some(id) =>
              b.readByte match { // 20-byte bytearray
                case -76 => Some(KeyLike(GlobalKeyID(id), b.toByteArray))
                case _   => None
              }
            case _ => None
          }

        case Type.LegacyToken if b.readableBytes == 42 =>
          b.readByte match {
            case -86 => // 10-byte bytearray
              val id = TokenID(b.readLong)

              b.readShort match {
                case TokenID.collID.value =>
                  readBoxedUInt64 match {
                    case Some(domain) =>
                      b.readByte match {
                        case -76 =>
                          Some(TokenLike(id, GlobalDatabaseID(domain), b.toByteArray))
                        case _ => None
                      }
                    case _ => None
                  }
                case _ => None
              }
            case _ => None
          }
        case _ =>
          None
      }
    } catch {
      case _: Throwable => None
    }
}

object KeyLike {
  def apply(id: GlobalKeyID): KeyLike = KeyLike(id, AuthLike.randomSecret)

  def validateSecret(id: GlobalKeyID, str: String) = AuthLike.fromBase64(str) match {
    case Some(k: KeyLike) => id == k.id
    case _                => false
  }
}

case class KeyLike(id: GlobalKeyID, random: Array[Byte]) extends AuthLike {
  def toBase64 = {
    val b = Unpooled.buffer(30)
    b.writeByte(AuthLike.Version)
    b.writeByte(AuthLike.Type.Key)
    b.writeLong(id.toLong)
    b.writeBytes(random)
    Base64.encodeUrlSafe(b.toByteArray)
  }

  def secret = Base64.encodeUrlSafe(random)

  def matchesRaw(string: String) = AuthLike.fromBase64(string) match {
    case Some(other: KeyLike) => toBase64 secureEquals other.toBase64
    case _                    => false
  }

  def matches(otherID: GlobalKeyID, hash: String) =
    id == otherID && (BCrypt.check(secret, hash) || BCrypt.check(toBase64, hash))

  override def equals(other: Any) = other match {
    case kl: KeyLike =>
      kl.id.equals(id) &
      kl.secret.secureEquals(secret)
    case _           => false
  }

  override def hashCode: Int = id.hashCode
}

object TokenLike {
  def apply(id: TokenID, domain: GlobalDatabaseID): TokenLike =
    TokenLike(id, domain, AuthLike.randomSecret)
}

case class TokenLike(id: TokenID, domain: GlobalDatabaseID, random: Array[Byte]) extends AuthLike {
  def toBase64 = {
    val b = Unpooled.buffer(38)
    b.writeByte(AuthLike.Version)
    b.writeByte(AuthLike.Type.Token)
    b.writeLong(id.toLong)
    b.writeLong(domain.toLong)
    b.writeBytes(random)
    Base64.encodeUrlSafe(b.toByteArray)
  }

  def secret = Base64.encodeUrlSafe(random)

  def matchesRaw(string: String) = AuthLike.fromBase64(string) match {
    case Some(other: TokenLike) => toBase64 secureEquals other.toBase64
    case _                      => false
  }

  def matches(otherID: TokenID, otherDomain: GlobalDatabaseID, hash: String) =
    id == otherID && (domain == otherDomain) && (BCrypt.check(secret, hash) || BCrypt.check(toBase64, hash))

  override def equals(other: Any) = other match {
    case tl: TokenLike =>
      tl.id.equals(id) &
      tl.domain.equals(domain) &
      tl.secret.secureEquals(secret)
    case _           => false
  }

  override def hashCode: Int = id.hashCode * domain.hashCode

}
