package fauna.net.security.provider

import fauna.lang.syntax._
import java.net.Socket
import java.security._
import java.security.cert._
import javax.net.ssl._

object PEMFileProvider {

  // this terrible magic allows us to compile on both java 8 and 9 by
  // suppressing the warning on a val, and returning it through an
  // unannotated method.
  @annotation.nowarn
  def create = {
    @deprecated("", "")
    val p = new PEMFileProvider
    p
  }
}

@deprecated("", "")
class PEMFileProvider extends Provider("PEMFile", 1.0, "FaunaDB PEM File Provider") {
  put("TrustManagerFactory.TrustedPublicKeys",
      classOf[TrustedPublicKeysTMFactorySpi].getName)
  put("TrustManagerFactory.TrustStoreWithCRL",
      classOf[TrustStoreWithCRLTMFactorySpi].getName)
}

case class TMTrustedKeys(keys: List[PublicKey]) extends ManagerFactoryParameters

case class TMKeyStoreAndCRL(ks: KeyStore, crl: Option[X509CRL])
    extends ManagerFactoryParameters

final class TrustedPublicKeysTMFactorySpi extends TrustManagerFactorySpi {
  private var trustedKeys: List[PublicKey] = Nil

  protected def engineInit(ks: KeyStore) = ()

  protected def engineInit(params: ManagerFactoryParameters) =
    trustedKeys = params.asInstanceOf[TMTrustedKeys].keys

  protected def engineGetTrustManagers(): Array[TrustManager] =
    Array(new TrustedPublicKeysTrustManager(trustedKeys.toSet))
}

final class TrustedPublicKeysTrustManager(keys: Set[PublicKey])
    extends X509TrustManager {
  private val log = getLogger

  def getAcceptedIssuers() = Array()

  def checkClientTrusted(chain: Array[X509Certificate], authType: String) = {
    log.debug("Checking client trustworthiness:")
    log.debug(s"Auth type: $authType")
    checkCertChain(chain)
  }

  def checkServerTrusted(chain: Array[X509Certificate], authType: String) = {
    log.debug("Checking server trustworthiness:")
    log.debug(s"Auth type: $authType")
    checkCertChain(chain)
  }

  private def checkCertChain(chain: Array[X509Certificate]) = {
    if ((chain eq null) || (chain.length == 0)) {
      throw new CertificateException("null or zero-length certificate chain")
    }

    chain foreach { c =>
      log.debug(c.toString)
    }

    if (!(keys contains chain.head.getPublicKey)) {
      throw new CertificateException("untrusted certificate")
    }
  }
}

final class TrustStoreWithCRLTMFactorySpi extends TrustManagerFactorySpi {

  private val factory =
    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
  private var crlOpt: Option[X509CRL] = None

  protected def engineInit(params: ManagerFactoryParameters) = {
    val TMKeyStoreAndCRL(ks, c) = params.asInstanceOf[TMKeyStoreAndCRL]
    factory.init(ks)
    crlOpt = c
  }

  protected def engineInit(ks: KeyStore) = ()

  protected def engineGetTrustManagers(): Array[TrustManager] = {
    factory.getTrustManagers() map {
      case tm: X509ExtendedTrustManager =>
        TrustStoreWithCRLExtTrustManager(tm, crlOpt)
      case tm: X509TrustManager => TrustStoreWithCRLTrustManager(tm, crlOpt)
      case tm                   => tm
    }
  }
}

sealed trait CRLTMLogic {
  val crlOpt: Option[X509CRL]
  val tm: X509TrustManager

  protected def checkCRL(chain: Array[X509Certificate]) =
    crlOpt foreach { crl =>
      chain foreach { c =>
        if (crl.getRevokedCertificate(c) ne null)
          throw new CertificateException("certificate revoked")
      }
    }

  def getAcceptedIssuers() = tm.getAcceptedIssuers()

  def checkClientTrusted(chain: Array[X509Certificate], authType: String) = {
    tm.checkClientTrusted(chain, authType)
    checkCRL(chain)
  }

  def checkServerTrusted(chain: Array[X509Certificate], authType: String) = {
    tm.checkServerTrusted(chain, authType)
    checkCRL(chain)
  }
}

final case class TrustStoreWithCRLTrustManager(
  tm: X509TrustManager,
  crlOpt: Option[X509CRL])
    extends X509TrustManager
    with CRLTMLogic

final case class TrustStoreWithCRLExtTrustManager(
  tm: X509ExtendedTrustManager,
  crlOpt: Option[X509CRL])
    extends X509ExtendedTrustManager
    with CRLTMLogic {

  def checkClientTrusted(
    chain: Array[X509Certificate],
    authType: String,
    socket: Socket): Unit = {
    tm.checkClientTrusted(chain, authType, socket)
    checkCRL(chain)
  }

  def checkClientTrusted(
    chain: Array[X509Certificate],
    authType: String,
    engine: SSLEngine) = {
    tm.checkClientTrusted(chain, authType, engine)
    checkCRL(chain)
  }

  def checkServerTrusted(
    chain: Array[X509Certificate],
    authType: String,
    socket: Socket): Unit = {
    tm.checkServerTrusted(chain, authType, socket)
    checkCRL(chain)
  }

  def checkServerTrusted(
    chain: Array[X509Certificate],
    authType: String,
    engine: SSLEngine) = {
    tm.checkServerTrusted(chain, authType, engine)
    checkCRL(chain)
  }
}
