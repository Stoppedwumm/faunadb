package fauna.net

import java.nio.file.Path
import java.security.SecureRandom
import java.util.Arrays
import javax.crypto.Cipher
import javax.security.auth.Destroyable

// Destroyable wrapper for a password.

package object security {
  type ETag = Option[String]
}

package security {
  sealed abstract class PasswordException(message: String) extends Exception(message)

  class NoPasswordException extends PasswordException("No password provided")
  class IncorrectPasswordException extends PasswordException("Incorrect password")

  class DestroyedPasswordException
      extends PasswordException("Password has been destroyed")

  class Password(_chars: Array[Char]) extends Destroyable {
    var _destroyed = false
    override def isDestroyed = _destroyed

    override def destroy() = {
      if (_chars ne null) Arrays.fill(_chars, '\u0000')
      _destroyed = true
    }

    def chars = {
      if (_destroyed) throw new DestroyedPasswordException
      _chars
    }

    def string = new String(chars)
  }

  object Password {
    val empty: Password = new Password(null) { override def destroy() = () }

    private val rnd = new SecureRandom

    def apply(str: String) = new Password(str.toCharArray)

    def random(size: Int) =
      new Password(1 to size map { _ => (32 + rnd.nextInt(94)).toChar } toArray)
  }

  // Key and Trust sources

  class InvalidKeySourceException(msg: String) extends Exception(msg)

  class InvalidTrustSourceException(msg: String) extends Exception(msg)

  object KeySource {
    def apply(pem: PEMFile, pw: Password): KeySource = new KeySource(pem, pw)
    def apply(pem: PEMFile): KeySource = apply(pem, Password.empty)

    def apply(path: Path, pw: Password = Password.empty): KeySource =
      apply(PEMFile(path), pw)
  }

  class KeySource(pem: PEMFile, pw: Password) {
    def factory = pem.keyManagerFactory(pw) getOrElse throwInvalid
    def keyedCert = pem.keyedCert(pw) getOrElse throwInvalid

    def trustFactory =
      pem.trustManagerFactory(pw) getOrElse {
        throw new InvalidTrustSourceException(
          s"File ${pem.path} does not contain a valid public key and/or certificate.")
      }

    private def throwInvalid =
      throw new InvalidKeySourceException(
        s"File ${pem.path} does not contain a valid private key and/or certificate.")
  }

  object TrustSource {
    def apply(pem: PEMFile, pw: Password): TrustSource = new TrustSource(pem, pw)
    def apply(pem: PEMFile): TrustSource = apply(pem, Password.empty)

    def apply(path: Path, pw: Password = Password.empty): TrustSource =
      apply(PEMFile(path), pw)
  }

  class TrustSource(pem: PEMFile, pw: Password) {

    def factory =
      pem.trustManagerFactory(pw) getOrElse {
        throw new InvalidTrustSourceException(
          s"File ${pem.path} does not contain a valid public key and/or certificate.")
      }
  }

  object JCE {

    lazy val assertStrength: Unit = {
      // This appears to be a decent test for JCE installation.
      if (Cipher.getMaxAllowedKeyLength("AES") <= 128) {
        throw new AssertionError(
          "Max encryption key length is too small. Ensure that Java Cryptography Extension (JCE) Unlimited Strength Jurisdiction Policy is installed.")
      }
    }
  }
}
