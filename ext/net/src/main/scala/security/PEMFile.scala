package fauna.net.security

import fauna.net.security.provider._
import java.io._
import java.math.BigInteger
import java.nio.file.{ Path, Paths }
import java.security._
import java.security.cert.{ X509CRL, X509Certificate }
import java.util.Date
import javax.crypto.BadPaddingException
import javax.net.ssl.{ KeyManagerFactory, TrustManagerFactory }
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo
import org.bouncycastle.cert._
import org.bouncycastle.cert.jcajce._
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.openssl.jcajce._
import org.bouncycastle.openssl.{ PasswordException => BCPasswordException, _ }
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo
import org.bouncycastle.pkcs.jcajce.JcePKCSPBEInputDecryptorProviderBuilder
import scala.concurrent.duration._

object PEMFile {
  Security.addProvider(new BouncyCastleProvider())

  def apply(path: Path) = new PEMFile(path)
  def apply(str: String) = new PEMFile(Paths.get(str))

  def write(path: Path, pw: Password, objects: List[AnyRef]): Unit = {
    val writer = new JcaPEMWriter(new FileWriter(path.toFile))

    objects foreach {
      case key: PrivateKey if pw != Password.empty => writer.writeObject(encryptKey(key, pw))
      case o                                       => writer.writeObject(o)
    }

    writer.close()
  }

  def encryptKey(key: PrivateKey, pw: Password) = {
    val enc = new JceOpenSSLPKCS8EncryptorBuilder(PKCS8Generator.PBE_SHA1_3DES)
    enc.setPassword(pw.chars)

    new JcaPKCS8Generator(key, enc.build).generate()
  }
}

sealed abstract class PEMFileException(val message: String) extends Exception(message)

class PEMFile private (val path: Path) {
  JCE.assertStrength

  lazy val objects = readObjects(new FileReader(path.toFile)) map normalized

  def decryptedObjects(pw: Password) = objects map { o => normalized(decrypted(o, pw)) }

  def trustedCerts: List[X509Certificate] = collectCerts(objects)

  def trustedCRLs: List[X509CRL] = collectCRLs(objects)

  def trustedKeys(pw: Password): List[PublicKey] = collectPublicKeys(decryptedObjects(pw))

  def privateKeys(pw: Password): List[PrivateKey] = collectPrivateKeys(decryptedObjects(pw))

  def keyPairs(pw: Password): List[KeyPair] = collectKeyPairs(decryptedObjects(pw))

  def keyedCert(pw: Password): Option[(PrivateKey, List[X509Certificate])] = {
    val objs = decryptedObjects(pw)

    val key = collectPrivateKeys(objs).headOption
    val pub = collectPublicKeys(objs).headOption
    val certs = collectCerts(objs)
    val adhoc = if (certs.nonEmpty) None else (for (p <- pub; k <- key) yield genAdhocCert(p, k))
    val chain = if (certs.isEmpty) adhoc.toList else certs

    if (key.isEmpty || chain.isEmpty) None else Some(key.get -> chain)
  }

  def keyManagerFactory(pw: Password = Password.empty): Option[KeyManagerFactory] =
    keyedCert(pw) map {
      case (key, chain) =>
        val store = KeyStore.getInstance("PKCS12")
        store.load(null, null)
        store.setKeyEntry("key", key, pw.chars, chain.toArray)

        val m = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
        m.init(store, pw.chars)
        m
    }

  def trustManagerFactory(pw: Password = Password.empty): Option[TrustManagerFactory] = {
    val certs = trustedCerts
    val crls = trustedCRLs
    val keys = trustedKeys(pw)

    if (certs.nonEmpty) {
      val store = KeyStore.getInstance("PKCS12")
      store.load(null, null)
      certs.zipWithIndex foreach { case (c, i) => store.setCertificateEntry(s"cert$i", c) }

      val tmf = TrustManagerFactory.getInstance("TrustStoreWithCRL", PEMFileProvider.create)
      tmf.init(TMKeyStoreAndCRL(store, crls.headOption))
      Some(tmf)

    } else if (keys.nonEmpty) {
      val tmf = TrustManagerFactory.getInstance("TrustedPublicKeys", PEMFileProvider.create)
      tmf.init(TMTrustedKeys(keys))
      Some(tmf)

    } else {
      None
    }
  }

  private def collectKeyPairs(obj: List[AnyRef]) = obj collect {
    case k: KeyPair => k
  }

  private def collectPrivateKeys(objs: List[AnyRef]) = objs collect {
    case i: PrivateKey => i
    case p: KeyPair => p.getPrivate
  }

  private def collectPublicKeys(objs: List[AnyRef]) = objs collect {
    case i: PublicKey => i
    case p: KeyPair => p.getPublic
    case c: X509Certificate => c.getPublicKey
  }

  private def collectCerts(objs: List[AnyRef]) = objs collect {
    case c: X509Certificate => c
  }

  private def collectCRLs(objs: List[AnyRef]) = objs collect {
    case c: X509CRL => c
  }

  private def genAdhocCert(pub: PublicKey, priv: PrivateKey) = {
    val cn = new X500Name("CN=FaunaDB Adhoc Self-Signed")
    val seq = BigInteger.ONE
    val from = System.currentTimeMillis - 1.day.toMillis
    val until = from + (365.days * 20).toMillis
    val cert = new JcaX509v1CertificateBuilder(cn, seq, new Date(from), new Date(until), cn, pub)
    val signer = (new JcaContentSignerBuilder(s"SHA1with${priv.getAlgorithm}")).build(priv)
    certConv.getCertificate(cert.build(signer))
  }

  // Decryption and normalization

  private def decrypted(o: AnyRef, pw: Password) =
    try {
      o match {
        case kp: PEMEncryptedKeyPair if pw ne null =>
          kp.decryptKeyPair((new JcePEMDecryptorProviderBuilder).build(pw.chars))
        case info: PKCS8EncryptedPrivateKeyInfo if pw ne null =>
          reifyKeyPair(info.decryptPrivateKeyInfo((new JcePKCSPBEInputDecryptorProviderBuilder).build(pw.chars)))
        case o => o
      }
    } catch {
      case _: BCPasswordException => throw new NoPasswordException
      case e: EncryptionException =>
        e.getCause match {
          case _: BadPaddingException => throw new IncorrectPasswordException
          case _                      => throw e
        }
    }

  private def reifyKeyPair(o: PrivateKeyInfo) = {
    val out = new CharArrayWriter
    val b = new JcaPEMWriter(out)
    b.writeObject(o)
    b.flush()
    readObjects(new CharArrayReader(out.toCharArray)).head
  }

  private lazy val keyConv = new JcaPEMKeyConverter
  private lazy val certConv = new JcaX509CertificateConverter
  private lazy val crlConv = new JcaX509CRLConverter

  private def normalized(o: AnyRef) =
    o match {
      case kp: PEMKeyPair => keyConv.getKeyPair(kp)
      case k: SubjectPublicKeyInfo => keyConv.getPublicKey(k)
      case k: PrivateKeyInfo => keyConv.getPrivateKey(k)
      case c: X509CertificateHolder => certConv.getCertificate(c)
      case c: X509CRLHolder => crlConv.getCRL(c)
      case o => o
    }

  private def readObjects(reader: Reader) = {
    val pem = new PEMParser(reader)
    val b = List.newBuilder[AnyRef]

    var o = pem.readObject
    while (o ne null) {
      b += o
      o = pem.readObject
    }

    reader.close()

    b.result()
  }
}
