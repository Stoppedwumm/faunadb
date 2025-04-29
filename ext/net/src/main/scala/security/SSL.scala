package fauna.net.security

import fauna.net.HostAddress
import io.netty.buffer.ByteBufAllocator
import io.netty.handler.ssl._
import javax.net.ssl.SSLContext
import scala.annotation.unused
import scala.jdk.CollectionConverters._

sealed class SSLConfig(
  val server: Option[ServerSSL],
  val client: Option[ClientSSL]) {
  def sslEnabled = server.isDefined || client.isDefined

  def getServerHandler(alloc: ByteBufAllocator): Option[SslHandler] =
    server map { _.getHandler(alloc, None) }

  def getClientHandler(
    alloc: ByteBufAllocator,
    peer: Option[(String, Int)]): Option[SslHandler] =
    client map { _.getHandler(alloc, peer) }

  def getClientHandler(
    alloc: ByteBufAllocator,
    peer: HostAddress): Option[SslHandler] =
    getClientHandler(alloc, Some((peer.hostName, peer.port)))

  def withProtoConfig(cfg: ApplicationProtocolConfig): SSLConfig =
    new SSLConfig(
      server map { _.copy(protoConfig = Some(cfg)) },
      client map { _.copy(protoConfig = Some(cfg)) }
    )
}

object SSLConfig {
  def apply(s: ServerSSL, c: ClientSSL): SSLConfig = new SSLConfig(Some(s), Some(c))

  def apply(s: ServerSSL, c: Option[ClientSSL]): SSLConfig =
    new SSLConfig(Some(s), c)

  def apply(s: Option[ServerSSL], c: ClientSSL): SSLConfig =
    new SSLConfig(s, Some(c))

  def apply(s: Option[ServerSSL], c: Option[ClientSSL]): SSLConfig =
    new SSLConfig(s, c)
}

case object NoSSL extends SSLConfig(None, None)
case object DefaultSSL extends SSLConfig(None, Some(ClientSSL(None, None, false)))

// Server context requires a KeyStore containing a cert and private
// key. If `trust` is not provided, the default trust store is used
// (standard root CAs).

object SSL {
  val HandlerName = "ssl"

  // Evaled on demand since objects are lazily initialized.
  JCE.assertStrength

  val DefaultJDKCiphers = {
    val ctx = SSLContext.getInstance("TLS")
    ctx.init(null, null, null)

    val ciphers =
      ctx.getSupportedSSLParameters.getCipherSuites.toList filterNot {
        _ contains "_GCM_" // disable GCM cause it is broken/slow.
      } filterNot {
        _ contains "_RC4_" // disable weak RC4 family.
      }

    ciphers
  }

  def apply(ks: KeySource, ts: TrustSource, priv: Boolean) =
    SSLConfig(ServerSSL(ks, Some(ts), priv), ClientSSL(Some(ks), Some(ts), priv))

  def apply(ks: KeySource, priv: Boolean) =
    SSLConfig(ServerSSL(ks, None, priv), ClientSSL(Some(ks), None, priv))

  def apply(ts: TrustSource) =
    SSLConfig(None, ClientSSL(None, Some(ts), false))
}

sealed trait SSLBuilder {
  val ciphers: Seq[String]
  def getHandler(
    alloc: ByteBufAllocator,
    @unused peer: Option[(String, Int)]): SslHandler

  protected val useOpenSSL =
    OpenSsl.isAvailable && SslProvider.isAlpnSupported(SslProvider.OPENSSL)

  protected val activeCiphers =
    if (ciphers.nonEmpty) {
      ciphers
    } else if (useOpenSSL) {
      Nil
    } else {
      SSL.DefaultJDKCiphers
    }
}

case class ServerSSL(
  key: KeySource,
  trust: Option[TrustSource],
  priv: Boolean,
  ciphers: Seq[String] = Nil,
  protoConfig: Option[ApplicationProtocolConfig] = None)
    extends SSLBuilder {

  private lazy val trustFactory =
    trust match {
      case Some(t)      => Some(t.factory)
      case None if priv => Some(key.trustFactory)
      case None         => None
    }

  def getHandler(alloc: ByteBufAllocator, @unused peer: Option[(String, Int)]) = {
    val h = context.newHandler(alloc)
    if (trustFactory.nonEmpty) {
      h.engine.setNeedClientAuth(true)
    }
    h
  }

  private lazy val context = {
    val b = if (useOpenSSL) {
      val (k, ch) = key.keyedCert
      SslContextBuilder
        .forServer(k, ch: _*)
        .sslProvider(SslProvider.OPENSSL)
    } else {
      SslContextBuilder
        .forServer(key.factory)
        .sslProvider(SslProvider.JDK)
    }

    if (activeCiphers.nonEmpty) {
      b.ciphers(activeCiphers.asJava)
    }

    trustFactory foreach b.trustManager

    protoConfig foreach b.applicationProtocolConfig

    b.build()
  }
}

case class ClientSSL(
  key: Option[KeySource],
  trust: Option[TrustSource],
  priv: Boolean,
  ciphers: Seq[String] = Nil,
  protoConfig: Option[ApplicationProtocolConfig] = None)
    extends SSLBuilder {

  def getHandler(alloc: ByteBufAllocator, peer: Option[(String, Int)]) =
    peer match {
      case None =>
        context.newHandler(alloc)
      case Some((host, port)) =>
        context.newHandler(alloc, host, port)
    }

  private lazy val trustFactory =
    trust match {
      case Some(t)      => Some(t.factory)
      case None if priv => key map { _.trustFactory }
      case None         => None
    }

  private lazy val context = {
    val b = SslContextBuilder.forClient

    if (useOpenSSL) {
      b.sslProvider(SslProvider.OPENSSL)
      key map { _.keyedCert } foreach { case (k, ch) => b.keyManager(k, ch: _*) }
    } else {
      b.sslProvider(SslProvider.JDK)
      key map { _.factory } foreach { b.keyManager(_) }
    }

    if (activeCiphers.nonEmpty) {
      b.ciphers(activeCiphers.asJava)
    }

    trustFactory foreach b.trustManager

    protoConfig foreach b.applicationProtocolConfig

    b.build()
  }
}
